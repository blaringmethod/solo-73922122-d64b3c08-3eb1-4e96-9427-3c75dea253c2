"""Checklist ("齐套核对") validation, backfill worker and page views.

A checklist is a batch number plus at most 100 unique ``(样本号, 项目)`` pairs,
uploaded as UTF-8 CSV.  Once accepted it is immutable: a corrected list is a
new batch, never an edit of an accepted one.

Checking never rescans the report archive.  It reads ``report_value``, a
projection of every accepted report's data rows that is

* written inside the same transaction as each newly accepted report, and
* backfilled from the existing archive in small leased batches.

Consequences the UI relies on:

* while the backfill is unfinished a checklist is reported only as
  "准备中" (``evaluating``); no partial missing/conflict conclusion leaks out;
* once complete, two instances cannot count the same archived report twice
  (leased cursor + UNIQUE projection key), and a restart resumes the cursor;
* a conflicting value that arrives later flips the checklist out of "ready"
  in the same transaction that stores the report.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .db import Database

if TYPE_CHECKING:  # pragma: no cover
    from .config import Settings

log = logging.getLogger("bridge.checklist")

EXPECTED_HEADERS = (
    ["样本号", "项目"],
    ["批号", "样本号", "项目"],
    ["批号", "样本号", "项目", "测值"],
)

MAX_PAIRS = 100

CHECKLIST_ERRORS = {
    "empty_file": "清单文件为空",
    "too_large": "清单文件超过大小上限",
    "not_utf8": "清单不是合法的 UTF-8 文本",
    "bad_csv": "清单 CSV 无法解析",
    "bad_header": "清单表头应为“样本号,项目”或“批号,样本号,项目”",
    "bad_row_width": "清单行列数不符合表头",
    "blank_field": "清单存在空字段",
    "duplicate_pair": "清单内同一（样本号,项目）出现多次",
    "too_many_pairs": "清单超过 100 行",
    "batch_missing": "缺少批号（表单或清单“批号”列）",
    "batch_inconsistent": "清单内批号不一致",
    "batch_mismatch": "表单批号与清单批号不一致",
    "batch_empty": "批号不能为空",
}


class ChecklistError(ValueError):
    def __init__(self, code: str, detail: str | None = None):
        message = CHECKLIST_ERRORS.get(code, code)
        if detail:
            message = f"{message}：{detail}"
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class ChecklistInput:
    batch_no: str
    pairs: list[tuple[str, str]]


def parse_checklist(
    content: bytes,
    max_bytes: int,
    batch_field: str | None = None,
) -> ChecklistInput:
    """Parse and fully validate an uploaded checklist CSV.

    Raises :class:`ChecklistError` on the first problem; a rejected upload
    creates no checklist row at all.
    """
    if len(content) > max_bytes:
        raise ChecklistError("too_large", f"{len(content)} 字节")
    if content.startswith(b"\xef\xbb\xbf"):
        content = content[3:]
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ChecklistError("not_utf8", f"第 {exc.start} 字节") from exc
    if "\x00" in text:
        raise ChecklistError("not_utf8", "包含 NUL 字节")
    try:
        rows = list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        raise ChecklistError("bad_csv", str(exc)) from exc
    rows = [row for row in rows if any(cell.strip() for cell in row)]
    if not rows:
        raise ChecklistError("empty_file")

    header = [cell.strip().lstrip("﻿") for cell in rows[0]]
    if header not in EXPECTED_HEADERS:
        raise ChecklistError("bad_header", ",".join(header))
    columns = {name: index for index, name in enumerate(header)}
    has_batch_column = "批号" in columns
    body = rows[1:]
    if not body:
        raise ChecklistError("empty_file")

    brand = (batch_field or "").strip() or None
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    row_batches: set[str] = set()
    for lineno, row in enumerate(body, start=2):
        if len(row) != len(header):
            raise ChecklistError(
                "bad_row_width", f"第 {lineno} 行有 {len(row)} 列，表头 {len(header)} 列"
            )
        cells = [cell.strip() for cell in row]
        if any(cell == "" for cell in cells):
            raise ChecklistError("blank_field", f"第 {lineno} 行")
        if has_batch_column:
            row_batches.add(cells[columns["批号"]])
        pair = (cells[columns["样本号"]], cells[columns["项目"]])
        if pair in seen:
            raise ChecklistError(
                "duplicate_pair", f"第 {lineno} 行 {pair[0]}/{pair[1]}"
            )
        seen.add(pair)
        pairs.append(pair)

    if len(pairs) > MAX_PAIRS:
        raise ChecklistError("too_many_pairs", f"{len(pairs)} 行，上限 {MAX_PAIRS}")
    if has_batch_column:
        if len(row_batches) > 1:
            raise ChecklistError("batch_inconsistent", ",".join(sorted(row_batches)))
        column_batch = next(iter(row_batches))
        if brand and brand != column_batch:
            raise ChecklistError("batch_mismatch", f"{brand} ≠ {column_batch}")
        brand = brand or column_batch
    if not brand:
        raise ChecklistError("batch_missing")
    return ChecklistInput(batch_no=brand, pairs=pairs)


# --------------------------------------------------------------------- backfill
def run_backfill_once(
    db: Database,
    instance: str,
    batch_size: int,
    lease_seconds: float,
    max_batches: int | None = None,
) -> dict[str, Any]:
    """Advance the archive projection by up to ``max_batches`` batches.

    ``max_batches=None`` runs until the archive is exhausted (the page/API
    action and the test helper); the background loop passes a small bound and
    calls this repeatedly, so each call yields the processor.  A bounded run
    that stops early releases the lease with its committed cursor intact.

    It never raises for "another instance owns the lease", which is a normal
    state on the instance that is not doing the backfill.
    """
    claimed, token = db.claim_backfill(instance, lease_seconds)
    if not claimed or token is None:
        return {"claimed": False, "processed": 0, "inserted": 0, "complete": False}
    processed = 0
    inserted = 0
    limit = 1_000_000 if max_batches is None else max_batches
    try:
        for _ in range(limit):
            batch = db.backfill_batch(token, batch_size)
            if batch is None:
                return {
                    "claimed": True,
                    "processed": processed,
                    "inserted": inserted,
                    "complete": False,
                    "lease_lost": True,
                }
            processed += batch["processed"]
            inserted += batch["inserted"]
            if batch["processed"] < batch_size:
                break
        done = db.try_finish_backfill(token)
        if done is False:
            # Bounded run stopped before the end: hand the lease back.
            db.release_backfill(token)
            return {
                "claimed": True,
                "processed": processed,
                "inserted": inserted,
                "complete": False,
            }
        if done is None:
            return {
                "claimed": True,
                "processed": processed,
                "inserted": inserted,
                "complete": False,
                "lease_lost": True,
            }
        return {
            "claimed": True,
            "processed": processed,
            "inserted": inserted,
            "complete": True,
        }
    except Exception as exc:  # noqa: BLE001 - recorded, then re-raised by caller
        db.fail_backfill(token, str(exc))
        log.exception("backfill failed")
        raise


def backfill_view(db: Database) -> dict[str, Any]:
    """Human-facing summary of the projection state."""
    state = db.backfill_progress()
    status = state.get("status", "pending")
    labels = {
        "pending": "准备中",
        "running": "准备中",
        "failed": "准备失败（将重试）",
        "complete": "已完成",
    }
    state["label"] = labels.get(status, status)
    return state


# ------------------------------------------------------------------ page views
def checklist_view(db: Database, checklist_id: int) -> dict[str, Any] | None:
    """Everything the detail page needs, read only from the projection."""
    row = db.get_checklist(checklist_id)
    if row is None:
        return None
    pairs = db.checklist_pairs(checklist_id)
    sources = db.checklist_sources(checklist_id)
    backfill = db.backfill_progress()
    by_pair: dict[int, list[dict[str, Any]]] = {}
    for source in sources:
        if source["value"] is None:
            continue
        by_pair.setdefault(int(source["pair_id"]), []).append(dict(source))
    pair_views = []
    for pair in pairs:
        item = dict(pair)
        item["sources"] = by_pair.get(int(pair["id"]), [])
        # "同对同测值只计一次": group source reports by value.  One group means
        # the pair is ok; several groups are the conflicting values, each with
        # every report number that carried it.
        grouped: dict[str, list[dict[str, Any]]] = {}
        for source in item["sources"]:
            grouped.setdefault(source["value"], []).append(source)
        item["value_groups"] = [
            {
                "value": value,
                "sources": entries,
                # One report number per source row; the list can contain
                # several values when they conflict, all listed for the user.
                "report_nos": [entry["report_no"] for entry in entries],
            }
            for value, entries in sorted(grouped.items())
        ]
        item["report_nos"] = sorted({v["report_no"] for v in item["sources"]})
        pair_views.append(item)
    summary = db.checklist_summary(checklist_id)
    preparing = row["state"] == "evaluating"
    return {
        "checklist": dict(row),
        "pairs": pair_views,
        "summary": summary,
        "preparing": preparing,
        "backfill": backfill,
        "ready": row["state"] == "ready",
        "missing": [p for p in pair_views if p["status"] == "missing"],
        "conflicts": [p for p in pair_views if p["status"] == "conflict"],
        "ok": [p for p in pair_views if p["status"] == "ok"],
    }
