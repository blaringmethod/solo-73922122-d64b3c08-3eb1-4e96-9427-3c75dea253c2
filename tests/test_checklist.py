"""Checklist (齐套核对) behaviour: upload, backfill, incremental conflicts."""

from __future__ import annotations

import io
import json

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

from app.checklist import ChecklistError, parse_checklist, run_backfill_once
from app.db import Database


# ------------------------------------------------------------------ parsing
def test_parse_two_column_csv_uses_form_batch():
    parsed = parse_checklist("样本号,项目\nS1,ALT\nS2,GLU\n".encode(), 1024, " B1 ")
    assert parsed.batch_no == "B1"
    assert parsed.pairs == [("S1", "ALT"), ("S2", "GLU")]


def test_parse_three_column_csv_uses_its_batch():
    parsed = parse_checklist("批号,样本号,项目\nB7,S1,ALT\nB7,S2,GLU\n".encode(), 1024)
    assert parsed.batch_no == "B7"
    assert parsed.pairs == [("S1", "ALT"), ("S2", "GLU")]


def test_parse_rejects_form_and_column_batch_mismatch():
    with pytest.raises(ChecklistError) as info:
        parse_checklist("批号,样本号,项目\nB7,S1,ALT\n".encode(), 1024, "B8")
    assert info.value.code == "batch_mismatch"


def test_parse_rejects_duplicate_pair():
    with pytest.raises(ChecklistError) as info:
        parse_checklist("样本号,项目\nS1,ALT\nS1,ALT\n".encode(), 1024, "B1")
    assert info.value.code == "duplicate_pair"


def test_parse_rejects_more_than_100_pairs():
    body = "".join(f"S{i},ALT\n" for i in range(101))
    with pytest.raises(ChecklistError) as info:
        parse_checklist(("样本号,项目\n" + body).encode(), 10_000, "B1")
    assert info.value.code == "too_many_pairs"


def test_parse_accepts_exactly_100_pairs():
    body = "".join(f"S{i},ALT\n" for i in range(100))
    parsed = parse_checklist(("样本号,项目\n" + body).encode(), 10_000, "B1")
    assert len(parsed.pairs) == 100


@pytest.mark.parametrize(
    "content,code",
    [
        (b"", "empty_file"),
        ("样本号,项目\nS1,ALT,S2\n".encode(), "bad_row_width"),
        (b"sample,item\nS1,ALT\n", "bad_header"),
        ("样本号,项目\nS1,\n".encode(), "blank_field"),
        (b"\xff\xfe", "not_utf8"),
    ],
)
def test_parse_rejects_malformed_content(content, code):
    with pytest.raises(ChecklistError) as info:
        parse_checklist(content, 1024, "B1")
    assert info.value.code == code


# ------------------------------------------------------------- collection
def _backfill_to_complete(db, instance="inst-a", batch_size=2):
    summary = run_backfill_once(db, instance, batch_size, 60)
    assert summary["complete"], summary
    return summary


def test_checklist_reports_missing_and_ok_after_backfill(demo):
    collector = demo.make_collector()
    demo.deliver("history", [demo.attachment("R1")])
    collector.scan()
    # A checklist accepted before the projection exists stays "preparing".
    checklist_id, error = collector.db.create_checklist(
        "B1", [("S001", "ALT"), ("S999", "GLU")]
    )
    assert error is None
    assert collector.db.get_checklist(checklist_id)["state"] == "evaluating"

    _backfill_to_complete(collector.db)
    row = collector.db.get_checklist(checklist_id)
    assert row["state"] == "not_ready"
    pairs = {p["sample_no"]: p["status"] for p in collector.db.checklist_pairs(checklist_id)}
    assert pairs == {"S001": "ok", "S999": "missing"}


def test_checklist_ready_only_when_all_pairs_have_a_value(demo):
    collector = demo.make_collector()
    demo.deliver(
        "history",
        [
            demo.attachment("R1", rows=[
                {"report_no": "R1", "sample_no": "S001", "item": "ALT", "value": "12"},
                {"report_no": "R1", "sample_no": "S002", "item": "GLU", "value": "5"},
            ])
        ],
    )
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT"), ("S002", "GLU")])
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"
    view = __import__("app.checklist", fromlist=["checklist_view"]).checklist_view(
        collector.db, checklist_id
    )
    assert view["ready"] and not view["missing"] and not view["conflicts"]


def test_same_value_twice_counts_once_and_does_not_conflict(demo):
    collector = demo.make_collector()
    demo.deliver("one", [demo.attachment("R1")])
    demo.deliver("two", [demo.attachment("R2")])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"
    sources = collector.db.checklist_sources(checklist_id)
    values = {s["value"] for s in sources if s["value"] is not None}
    # One distinct value -> no conflict, even though two reports carry it; so
    # both report numbers can be shown as sources.
    assert values == {"12"}
    assert {s["report_no"] for s in sources if s["value"] is not None} == {"R1", "R2"}


def test_different_value_conflict_lists_both_report_numbers(demo):
    collector = demo.make_collector()
    demo.deliver("a", [{"filename": "R1.csv", "content": "报告号,样本号,项目,测值\nR1,S001,ALT,12\n"}])
    demo.deliver("b", [{"filename": "R2.csv", "content": "报告号,样本号,项目,测值\nR2,S001,ALT,13\n"}])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.get_checklist(checklist_id)["state"] == "not_ready"
    pair = collector.db.checklist_pairs(checklist_id)[0]
    assert pair["status"] == "conflict"
    report_nos = {s["report_no"] for s in collector.db.checklist_sources(checklist_id)}
    assert report_nos == {"R1", "R2"}


def test_value_comparison_is_string_exact(demo):
    """12 vs 12.0 are different strings and therefore a conflict."""
    collector = demo.make_collector()
    demo.deliver("a", [{"filename": "R1.csv", "content": "报告号,样本号,项目,测值\nR1,S001,ALT,12\n"}])
    demo.deliver("b", [{"filename": "R2.csv", "content": "报告号,样本号,项目,测值\nR2,S001,ALT,12.0\n"}])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.checklist_pairs(checklist_id)[0]["status"] == "conflict"


# ------------------------------------------------------------- incremental
def test_identical_report_arriving_after_ready_is_reused_not_conflict(demo):
    collector = demo.make_collector()
    demo.deliver("first", [demo.attachment("R1")])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"

    demo.deliver("repeat", [demo.attachment("R1")])  # same original
    collector.scan()
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"


def test_conflicting_value_after_ready_becomes_conflict_and_not_ready(demo):
    collector = demo.make_collector()
    demo.deliver("first", [demo.attachment("R1")])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"

    demo.deliver(
        "later",
        [{"filename": "R2.csv", "content": "报告号,样本号,项目,测值\nR2,S001,ALT,99\n"}],
    )
    collector.scan()
    assert collector.db.get_checklist(checklist_id)["state"] == "not_ready"
    pair = collector.db.checklist_pairs(checklist_id)[0]
    assert pair["status"] == "conflict"
    sources = collector.db.checklist_sources(checklist_id)
    assert {s["report_no"] for s in sources if s["value"] is not None} == {"R1", "R2"}


def test_rejected_mail_does_not_participate(demo):
    collector = demo.make_collector()
    demo.deliver("good", [demo.attachment("R1")])
    collector.scan()
    demo.deliver("bad", [{"filename": "R9.csv", "content": "bad,header\n1,2\n"}])
    collector.scan()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"
    assert all(s["report_no"] == "R1" for s in collector.db.checklist_sources(checklist_id))


def test_missing_pair_can_later_become_ok(demo):
    collector = demo.make_collector()
    _backfill_to_complete(collector.db)
    checklist_id, _ = collector.db.create_checklist("B1", [("S001", "ALT")])
    assert collector.db.checklist_pairs(checklist_id)[0]["status"] == "missing"
    demo.deliver("late", [demo.attachment("R1")])
    collector.scan()
    assert collector.db.get_checklist(checklist_id)["state"] == "ready"


# ------------------------------------------------------------------ backfill
def test_backfill_is_incremental_and_never_double_counts(demo):
    collector = demo.make_collector()
    for index in range(5):
        demo.deliver(f"m{index}", [demo.attachment(f"R{index}")])
    collector.scan()
    db = collector.db

    first = run_backfill_once(db, "inst-a", 2, 60, max_batches=1)
    assert first["processed"] == 2 and not first["complete"]
    second = run_backfill_once(db, "inst-b", 2, 60)
    assert second["complete"]
    values = db.backfill_progress()
    assert values["status"] == "complete"
    # Re-running the backfill is a no-op and does not duplicate values.
    again = run_backfill_once(db, "inst-a", 2, 60)
    assert not again["claimed"]


def test_backfill_lease_blocks_second_instance(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.init()
    ok, token = db.claim_backfill("inst-a", 60)
    assert ok and token
    ok_b, token_b = db.claim_backfill("inst-b", 60)
    assert not ok_b and token_b is None
    assert db.backfill_batch(token, 10) is not None


def test_backfill_resumes_from_committed_cursor_after_restart(demo):
    collector = demo.make_collector()
    for index in range(4):
        demo.deliver(
            f"m{index}",
            [
                {
                    "filename": f"R{index}.csv",
                    "report_no": f"R{index}",
                    "sample_no": f"S{index}",
                    "item": "ALT",
                    "value": str(index),
                }
            ],
        )
    collector.scan()
    db = collector.db
    # Simulate a crash-owned lease by an instance that is gone.
    ok, token = db.claim_backfill("inst-a", 0.01)
    assert ok
    db.backfill_batch(token, 2)
    import time as _time

    _time.sleep(0.05)
    # A restarting instance with the same identity resumes the cursor.
    summary = run_backfill_once(db, "inst-a", 10, 60)
    assert summary["complete"]
    state = db.backfill_state()
    assert state["scanned_reports"] == 4
    # Every pair appears exactly once in the projection.
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM report_value").fetchone()["n"]
        distinct = conn.execute(
            "SELECT COUNT(*) AS n FROM (SELECT DISTINCT sample_no, item, value FROM report_value)"
        ).fetchone()["n"]
    assert total == distinct == 4


def test_failed_backfill_keeps_checklists_preparing(demo):
    collector = demo.make_collector()
    demo.deliver("history", [demo.attachment("R1")])
    collector.scan()
    db = collector.db
    checklist_id, _ = db.create_checklist("B1", [("S001", "ALT")])
    ok, token = db.claim_backfill("inst-a", 60)
    assert ok
    db.fail_backfill(token, "boom")
    assert db.get_checklist(checklist_id)["state"] == "evaluating"
    assert db.backfill_progress()["status"] == "failed"


def test_two_instances_running_backfill_concurrently_do_not_double_count(demo):
    """Both instances hammer the same database; the lease + UNIQUE key hold."""
    import threading

    collector = demo.make_collector()
    for index in range(6):
        demo.deliver(
            f"m{index}",
            [
                {
                    "filename": f"R{index}.csv",
                    "report_no": f"R{index}",
                    "sample_no": f"S{index}",
                    "item": "ALT",
                    "value": str(index),
                }
            ],
        )
    collector.scan()
    db = collector.db
    errors: list[BaseException] = []

    def worker(instance: str) -> None:
        try:
            for _ in range(3):
                run_backfill_once(db, instance, 1, 60, max_batches=2)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(name,)) for name in ("inst-a", "inst-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    # Finish with one clean run, then verify every value appears exactly once.
    run_backfill_once(db, "inst-a", 2, 60)
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM report_value").fetchone()["n"]
        distinct = conn.execute(
            "SELECT COUNT(*) AS n FROM (SELECT DISTINCT sample_no, item, value FROM report_value)"
        ).fetchone()["n"]
    assert total == distinct == 6


def test_oversized_checklist_is_rejected(demo):
    collector = demo.make_collector()
    app = create_app(collector.cfg)
    app.state.collector = collector
    app.state.db = collector.db
    app.state.store = collector.store
    client = TestClient(app)
    big = ("样本号,项目\n" + "S1,ALT\n").encode() + b"x" * (collector.cfg.max_checklist_bytes)
    response = client.post(
        "/api/checklists",
        data={"batch_no": "B1"},
        files={"file": ("big.csv", io.BytesIO(big), "text/csv")},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "too_large"
    assert client.get("/api/checklists").json() == []


def test_collection_during_backfill_is_counted_exactly_once(demo):
    """A report ingested while the cursor is behind it must not double count."""
    collector = demo.make_collector()
    for i in range(4):
        demo.deliver(f"old{i}", [demo.attachment(f"OLD{i}")])
    collector.scan()
    db = collector.db

    # Start the backfill but stop after one small batch, leaving it unfinished.
    claimed, token = db.claim_backfill("inst-a", 60)
    assert claimed
    db.backfill_batch(token, 2)
    db.release_backfill(token)

    # Collect a new report for the same pair while the projection is unfinished.
    demo.deliver(
        "during",
        [{"filename": "NEW.csv", "content": "报告号,样本号,项目,测值\nNEW,S001,ALT,99\n"}],
    )
    collector.scan()

    # A checklist submitted now must still be "preparing" only.
    checklist_id, _ = db.create_checklist("B1", [("S001", "ALT"), ("S0", "ALT")])
    assert db.get_checklist(checklist_id)["state"] == "evaluating"

    summary = run_backfill_once(db, "inst-b", 2, 60)
    assert summary["complete"]
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM report_value").fetchone()["n"]
        distinct = conn.execute(
            "SELECT COUNT(*) AS n FROM (SELECT DISTINCT sample_no, item, value FROM report_value)"
        ).fetchone()["n"]
        # The live row written during the backfill is not counted again by
        # the later backfill batch.
        live_new = conn.execute(
            "SELECT COUNT(*) AS n FROM report_value WHERE sample_no='S001' AND value='99'"
        ).fetchone()["n"]
    assert total == distinct
    assert live_new == 1
    # The old rows (value 12) and the new row (value 99) conflict.
    assert db.checklist_pairs(checklist_id)[0]["status"] == "conflict"
    assert db.get_checklist(checklist_id)["state"] == "not_ready"
