"""Checklist web page and JSON API."""

from __future__ import annotations

import io

from fastapi.testclient import TestClient

from app.checklist import run_backfill_once
from app.main import create_app


def _client(demo, instance: str = "inst-a") -> TestClient:
    collector = demo.make_collector(instance)
    app = create_app(collector.cfg)
    app.state.collector = collector
    app.state.db = collector.db
    app.state.store = collector.store
    return TestClient(app)


def _csv(text: str):
    return {"file": ("checklist.csv", io.BytesIO(text.encode("utf-8")), "text/csv")}


def test_upload_before_backfill_is_only_preparing(demo):
    demo.deliver("history", [demo.attachment("R1")])
    client = _client(demo)
    client.post("/api/scan")

    response = client.post(
        "/api/checklists",
        data={"batch_no": "B1"},
        files=_csv("样本号,项目\nS001,ALT\nS999,GLU\n"),
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["state"] == "preparing"
    # No conclusions are published while the projection is unfinished.
    assert payload["pairs"] == []
    assert payload["ready"] is False

    page = client.get("/checklists").text
    assert "准备中" in page


def test_api_lists_missing_and_conflict_after_backfill(demo):
    collector = demo.make_collector()
    demo.deliver(
        "a",
        [{"filename": "R1.csv", "content": "报告号,样本号,项目,测值\nR1,S001,ALT,12\n"}],
    )
    demo.deliver(
        "b",
        [{"filename": "R2.csv", "content": "报告号,样本号,项目,测值\nR2,S001,ALT,13\n"}],
    )
    collector.scan()
    run_backfill_once(collector.db, "inst-a", 10, 60)

    app = create_app(collector.cfg)
    app.state.collector = collector
    app.state.db = collector.db
    app.state.store = collector.store
    client = TestClient(app)
    response = client.post(
        "/api/checklists",
        data={"batch_no": "B1"},
        files=_csv("样本号,项目\nS001,ALT\nS404,XXX\n"),
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["ready"] is False
    pairs = {(p["sample_no"], p["item"]): p for p in payload["pairs"]}
    assert pairs[("S001", "ALT")]["status"] == "conflict"
    groups = pairs[("S001", "ALT")]["values"]
    assert {group["value"] for group in groups} == {"12", "13"}
    assert {no for group in groups for no in group["report_nos"]} == {"R1", "R2"}
    assert pairs[("S404", "XXX")]["status"] == "missing"
    assert pairs[("S404", "XXX")]["values"] == []


def test_duplicate_batch_is_rejected(demo):
    client = _client(demo)
    run_backfill_once(client.app.state.db, "inst-a", 10, 60)
    first = client.post(
        "/api/checklists", data={"batch_no": "B1"}, files=_csv("样本号,项目\nS1,ALT\n")
    )
    assert first.status_code == 201
    second = client.post(
        "/api/checklists", data={"batch_no": "B1"}, files=_csv("样本号,项目\nS2,GLU\n")
    )
    assert second.status_code == 409
    # The accepted checklist was not modified by the rejected duplicate.
    checklist = client.get("/api/checklists/1").json()
    assert [(p["sample_no"], p["item"]) for p in checklist["pairs"]] == [("S1", "ALT")]


def test_malformed_upload_creates_nothing(demo):
    client = _client(demo)
    run_backfill_once(client.app.state.db, "inst-a", 10, 60)
    response = client.post(
        "/api/checklists", data={"batch_no": "B1"}, files=_csv("样本号,项目\nS1\n")
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "bad_row_width"
    assert client.get("/api/checklists").json() == []


def test_form_upload_redirects_to_detail_page(demo):
    client = _client(demo)
    run_backfill_once(client.app.state.db, "inst-a", 10, 60)
    response = client.post(
        "/checklists",
        data={"batch_no": "B9"},
        files=_csv("样本号,项目\nS1,ALT\n"),
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/checklists/")
    page = client.get(response.headers["location"])
    assert page.status_code == 200
    assert "B9" in page.text and "缺项" in page.text


def test_ready_page_says_can_hand_off_then_conflict_flips_it(demo):
    collector = demo.make_collector()
    demo.deliver("first", [demo.attachment("R1")])
    collector.scan()
    run_backfill_once(collector.db, "inst-a", 10, 60)

    app = create_app(collector.cfg)
    app.state.collector = collector
    app.state.db = collector.db
    app.state.store = collector.store
    client = TestClient(app)
    created = client.post(
        "/api/checklists", data={"batch_no": "B1"}, files=_csv("样本号,项目\nS001,ALT\n")
    )
    assert created.status_code == 201
    detail_id = created.json()["id"]
    assert created.json()["ready"] is True
    assert "可交分析" in client.get(f"/checklists/{detail_id}").text

    demo.deliver(
        "later",
        [{"filename": "R2.csv", "content": "报告号,样本号,项目,测值\nR2,S001,ALT,77\n"}],
    )
    client.post("/api/scan")
    after = client.get(f"/api/checklists/{detail_id}").json()
    assert after["ready"] is False
    assert after["pairs"][0]["status"] == "conflict"
    page = client.get(f"/checklists/{detail_id}").text
    assert "不可交分析" in page
    assert "可交分析" not in page.replace("不可交分析", "")


def test_backfill_endpoint_completes_and_evaluates(demo):
    demo.deliver("history", [demo.attachment("R1")])
    client = _client(demo)
    client.post("/api/scan")
    client.post("/api/checklists", data={"batch_no": "B1"}, files=_csv("样本号,项目\nS001,ALT\n"))
    response = client.post("/api/checklists/backfill")
    assert response.status_code == 200
    assert response.json()["backfill"]["status"] == "complete"
    status = client.get("/api/backfill").json()
    assert status["status"] == "complete"
    checklist = client.get("/api/checklists/1").json()
    assert checklist["ready"] is True


def test_existing_collection_actions_still_work(demo):
    demo.deliver("keep", [demo.attachment("R1")])
    client = _client(demo)
    assert client.post("/actions/scan", follow_redirects=True).status_code == 200
    message = client.get("/api/messages").json()[0]
    assert message["decision"] == "accepted"
    assert client.get(f"/api/messages/{message['id']}/original").status_code == 200
    assert client.get(f"/api/messages/{message['id']}/receipt").status_code == 200
    report = message["reports"][0]
    assert client.get(f"/api/reports/{report['report_id']}/attachment").status_code == 200
    assert "齐套核对" in client.get("/").text
