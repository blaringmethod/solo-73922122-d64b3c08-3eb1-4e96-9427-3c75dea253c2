"""FastAPI web application and API for the laboratory inbox bridge."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .checklist import (
    ChecklistError,
    backfill_view,
    checklist_view,
    parse_checklist,
    run_backfill_once,
)
from .collector import Collector, ScanBusy
from .config import Settings, settings
from .db import Database
from .store import Store

BASE_DIR = Path(__file__).resolve().parent
log = logging.getLogger("bridge.web")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _row_to_dict(row) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _message_view(db: Database, row) -> dict[str, Any]:
    item = dict(row)
    try:
        item["detail_list"] = json.loads(item.get("detail") or "[]")
    except json.JSONDecodeError:
        item["detail_list"] = []
    try:
        item["report_nos_list"] = json.loads(item.get("report_nos") or "[]")
    except json.JSONDecodeError:
        item["report_nos_list"] = []
    item["reports"] = [dict(r) for r in db.reports_for_message(item["id"])]
    return item


def create_app(cfg: Settings | None = None) -> FastAPI:
    cfg = cfg or settings
    db = Database(cfg.database_path)
    store = Store(cfg.storage_dir)
    collector = Collector(cfg, db, store)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init()
        store.init()
        stop = asyncio.Event()
        tasks: list[asyncio.Task] = []
        if cfg.auto_scan_interval > 0:
            tasks.append(asyncio.create_task(auto_loop(stop)))
        if cfg.auto_backfill:
            tasks.append(asyncio.create_task(backfill_loop(stop)))
        log.info(
            "instance %s ready db=%s storage=%s imap=%s:%s account=%s",
            cfg.instance_id,
            cfg.database_path,
            cfg.storage_dir,
            cfg.imap_host,
            cfg.imap_port,
            cfg.account,
        )
        try:
            yield
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    async def auto_loop(stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.auto_scan_interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await asyncio.to_thread(run_scan, "auto", False)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - loop must survive
                log.exception("automatic scan failed")

    async def backfill_loop(stop: asyncio.Event) -> None:
        """Advance the offline report projection without blocking collectors.

        Exactly one instance holds the backfill lease at a time.  The loop
        touches a few hundred reports per batch and then yields the thread, so
        simultaneous scans and web requests keep working.  A restart resumes
        from the committed cursor; until completion the UI says "准备中".
        """
        while not stop.is_set():
            try:
                summary = await asyncio.to_thread(
                    run_backfill_once,
                    db,
                    cfg.instance_id,
                    cfg.backfill_batch_size,
                    cfg.backfill_lease_seconds,
                    cfg.backfill_batches_per_tick,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - loop must survive
                log.exception("backfill batch failed")
                summary = {"complete": False}
            if summary.get("complete"):
                log.info("report projection complete on %s", cfg.instance_id)
                return
            delay = (
                max(cfg.backfill_pause_seconds, 0.0)
                if summary.get("claimed")
                else max(cfg.backfill_retry_seconds, 1.0)
            )
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass

    def run_scan(trigger: str, raise_busy: bool = True):
        try:
            return collector.scan(trigger=trigger).as_dict()
        except ScanBusy:
            if raise_busy:
                raise
            log.info("scan skipped: another instance owns the lease")
            return None

    app = FastAPI(title=cfg.title, lifespan=lifespan)
    app.state.cfg = cfg
    app.state.db = db
    app.state.store = store
    app.state.collector = collector
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    # ------------------------------------------------------------------ pages
    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        mark_filter = request.query_params.get("mark") or None
        messages = [_message_view(db, row) for row in db.list_messages(100, mark_filter)]
        reports = [dict(r) for r in db.list_reports(200)]
        state = db.get_state(cfg.account, cfg.imap_mailbox)
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "cfg": cfg,
                "stats": db.stats(),
                "messages": messages,
                "reports": reports,
                "scans": [dict(r) for r in db.recent_scans(15)],
                "state": _row_to_dict(state),
                "mark_filter": mark_filter or "",
                "flash": request.query_params.get("flash", ""),
                "error": request.query_params.get("error", ""),
            },
        )

    @app.post("/actions/scan")
    async def action_scan():
        try:
            result = await asyncio.to_thread(run_scan, "web", True)
        except ScanBusy as exc:
            return RedirectResponse(f"/?flash=&error={_q(str(exc))}", status_code=303)
        except Exception as exc:  # surface the failure in the UI
            log.exception("web scan failed")
            return RedirectResponse(f"/?flash=&error={_q(str(exc))}", status_code=303)
        summary = (
            f"扫描完成：读取 {result['scanned']}，收讫 {result['accepted']}，"
            f"拒收 {result['rejected']}，跳过 {result['skipped']}，"
            f"标记 {result['marked']}，复用原件 {result['reused_reports']}"
        )
        if result["status"] != "completed":
            summary += f"；{result['error'] or result['status']}"
            return RedirectResponse(f"/?flash=&error={_q(summary)}", status_code=303)
        return RedirectResponse(f"/?flash={_q(summary)}", status_code=303)

    @app.post("/actions/retry-marks")
    async def action_retry_marks():
        try:
            marked = await asyncio.to_thread(collector.mark_pending_public)
        except ScanBusy as exc:
            return RedirectResponse(f"/?flash=&error={_q(str(exc))}", status_code=303)
        except Exception as exc:
            log.exception("retry marks failed")
            return RedirectResponse(f"/?flash=&error={_q(str(exc))}", status_code=303)
        return RedirectResponse(f"/?flash={_q(f'补标记完成：{marked} 封')}", status_code=303)

    # -------------------------------------------------------------- checklists
    @app.get("/checklists", response_class=HTMLResponse)
    def checklist_index(request: Request):
        return templates.TemplateResponse(
            request,
            "checklists.html",
            {
                "cfg": cfg,
                "checklists": [dict(r) for r in db.list_checklists()],
                "backfill": backfill_view(db),
                "flash": request.query_params.get("flash", ""),
                "error": request.query_params.get("error", ""),
            },
        )

    @app.post("/checklists")
    async def action_create_checklist(request: Request):
        form = await request.form()
        upload = form.get("file")
        batch_field = str(form.get("batch_no") or "").strip()
        try:
            if upload is None or not getattr(upload, "filename", ""):
                raise ChecklistError("empty_file", "未选择文件")
            if getattr(upload, "size", None) and upload.size > cfg.max_checklist_bytes:
                raise ChecklistError("too_large", f"{upload.size} 字节")
            # Read at most one byte past the limit, so an oversized upload is
            # rejected without loading it into memory.
            content = await upload.read(cfg.max_checklist_bytes + 1)
            parsed = parse_checklist(content, cfg.max_checklist_bytes, batch_field)
            checklist_id, error = db.create_checklist(parsed.batch_no, parsed.pairs)
        except ChecklistError as exc:
            return RedirectResponse(
                f"/checklists?flash=&error={_q(str(exc))}", status_code=303
            )
        if error or checklist_id is None:
            return RedirectResponse(
                f"/checklists?flash=&error={_q(error or '清单未受理')}", status_code=303
            )
        return RedirectResponse(f"/checklists/{checklist_id}", status_code=303)

    @app.get("/checklists/{checklist_id}", response_class=HTMLResponse)
    def checklist_detail(request: Request, checklist_id: int):
        view = checklist_view(db, checklist_id)
        if view is None:
            raise HTTPException(status_code=404, detail="清单不存在")
        return templates.TemplateResponse(
            request,
            "checklist_detail.html",
            {"cfg": cfg, "view": view, "flash": request.query_params.get("flash", "")},
        )

    @app.post("/checklists/backfill")
    async def action_backfill():
        """Run the backfill to completion from the page (demo convenience).

        The background loop does the same incrementally; this action just waits
        for it, so an operator can watch "准备中" turn into a conclusion.
        """
        try:
            await asyncio.to_thread(
                run_backfill_once,
                db,
                cfg.instance_id,
                cfg.backfill_batch_size,
                cfg.backfill_lease_seconds,
            )
        except Exception as exc:
            log.exception("manual backfill failed")
            return RedirectResponse(
                f"/checklists?flash=&error={_q(str(exc))}", status_code=303
            )
        progress = backfill_view(db)
        label = progress.get("label", "")
        return RedirectResponse(
            f"/checklists?flash={_q('补建状态：' + label)}", status_code=303
        )

    # -------------------------------------------------------------------- API
    @app.get("/healthz")
    def healthz():
        return {"ok": True, "instance": cfg.instance_id}

    @app.get("/api/status")
    async def api_status():
        state = db.get_state(cfg.account, cfg.imap_mailbox)
        probe: dict[str, Any]
        try:
            probe = await asyncio.to_thread(collector.probe)
        except Exception as exc:
            probe = {"ok": False, "error": str(exc)}
        return {
            "instance": cfg.instance_id,
            "account": cfg.account,
            "mailbox": cfg.imap_mailbox,
            "stats": db.stats(),
            "mailbox_state": _row_to_dict(state),
            "imap": probe,
            "active_scan": _row_to_dict(db.active_scan()),
        }

    @app.post("/api/scan")
    async def api_scan():
        try:
            result = await asyncio.to_thread(run_scan, "api", True)
        except ScanBusy as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"扫描失败：{exc}") from exc
        # A stale/interrupted scan is not a failure of the request: it reports
        # what was committed, plus why it stopped.
        return result

    @app.post("/api/retry-marks")
    async def api_retry_marks():
        try:
            marked = await asyncio.to_thread(collector.mark_pending_public)
        except ScanBusy as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"补标记失败：{exc}") from exc
        return {"marked": marked}

    @app.get("/api/messages")
    def api_messages(limit: int = 100, mark_state: str | None = None):
        return [_message_view(db, row) for row in db.list_messages(limit, mark_state)]

    @app.post("/api/checklists")
    async def api_create_checklist(request: Request):
        """Accept a checklist (multipart ``file``, optional ``batch_no``).

        Validation is all-or-nothing: a malformed file or an already accepted
        batch number creates nothing and returns 400/409.
        """
        form = await request.form()
        upload = form.get("file")
        batch_field = str(form.get("batch_no") or "").strip()
        try:
            if upload is None or not getattr(upload, "filename", ""):
                raise ChecklistError("empty_file", "未选择文件")
            if getattr(upload, "size", None) and upload.size > cfg.max_checklist_bytes:
                raise ChecklistError("too_large", f"{upload.size} 字节")
            content = await upload.read(cfg.max_checklist_bytes + 1)
            parsed = parse_checklist(content, cfg.max_checklist_bytes, batch_field)
        except ChecklistError as exc:
            raise HTTPException(
                status_code=400, detail={"code": exc.code, "message": exc.message}
            ) from exc
        checklist_id, error = db.create_checklist(parsed.batch_no, parsed.pairs)
        if checklist_id is None:
            raise HTTPException(
                status_code=409, detail={"code": "batch_exists", "message": error}
            )
        return JSONResponse(_checklist_api_view(db, checklist_id), status_code=201)

    @app.get("/api/checklists")
    def api_checklists(limit: int = 100):
        return [_checklist_row_api(row) for row in db.list_checklists(limit)]

    @app.post("/api/checklists/backfill")
    async def api_backfill():
        try:
            summary = await asyncio.to_thread(
                run_backfill_once,
                db,
                cfg.instance_id,
                cfg.backfill_batch_size,
                cfg.backfill_lease_seconds,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"补建失败：{exc}") from exc
        return {"summary": summary, "backfill": backfill_view(db)}

    @app.get("/api/checklists/{checklist_id}")
    def api_checklist(checklist_id: int):
        view = _checklist_api_view(db, checklist_id)
        if view is None:
            raise HTTPException(status_code=404, detail="清单不存在")
        return view

    @app.get("/api/backfill")
    def api_backfill_status():
        return backfill_view(db)

    @app.get("/api/messages/{message_id}")
    def api_message(message_id: int):
        row = db.get_message(message_id)
        if row is None:
            raise HTTPException(status_code=404, detail="message not found")
        return _message_view(db, row)

    @app.get("/api/messages/{message_id}/original")
    def download_original(message_id: int):
        row = db.get_message(message_id)
        if row is None:
            raise HTTPException(status_code=404, detail="message not found")
        path = store.absolute(row["original_path"])
        if not path.is_file():
            raise HTTPException(status_code=410, detail="原件文件缺失")
        return FileResponse(
            path,
            media_type="message/rfc822",
            filename=f"{row['raw_sha256'][:16]}.eml",
        )

    @app.get("/api/messages/{message_id}/receipt")
    def download_receipt(message_id: int):
        row = db.get_message(message_id)
        if row is None:
            raise HTTPException(status_code=404, detail="message not found")
        path = store.absolute(row["receipt_path"])
        if not path.is_file():
            raise HTTPException(status_code=410, detail="回执文件缺失")
        return FileResponse(
            path,
            media_type="application/json",
            filename=f"receipt-{row['uidvalidity']}-{row['uid']}.json",
        )

    @app.get("/api/reports/{report_id}/attachment")
    def download_attachment(report_id: int):
        row = db.get_report(report_id)
        if row is None:
            raise HTTPException(status_code=404, detail="report not found")
        path = store.absolute(row["attachment_path"])
        if not path.is_file():
            raise HTTPException(status_code=410, detail="附件文件缺失")
        return FileResponse(
            path,
            media_type="text/csv; charset=utf-8",
            filename=row["attachment_name"],
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log.exception("unhandled error on %s", request.url.path)
        return JSONResponse({"detail": str(exc)}, status_code=500)

    return app


def _q(value: str) -> str:
    from urllib.parse import quote_plus

    return quote_plus(value)


def _checklist_row_api(row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    for key in ("ok_count", "missing_count", "conflict_count", "pending_count"):
        item[key] = int(item.get(key) or 0)
    return item


def _checklist_api_view(db: Database, checklist_id: int) -> dict[str, Any] | None:
    view = checklist_view(db, checklist_id)
    if view is None:
        return None
    checklist = view["checklist"]
    return {
        "id": checklist["id"],
        "batch_no": checklist["batch_no"],
        "created_at": checklist["created_at"],
        "pair_count": checklist["pair_count"],
        # While the archive projection is unfinished only "preparing" is
        # exposed; no per-pair conclusion is published.
        "state": "preparing" if view["preparing"] else checklist["state"],
        "ready": view["ready"],
        "summary": view["summary"],
        "backfill": view["backfill"],
        "pairs": [] if view["preparing"] else [
            {
                "sample_no": pair["sample_no"],
                "item": pair["item"],
                "status": pair["status"],
                "values": [
                    {
                        "value": group["value"],
                        "report_nos": group["report_nos"],
                        "sources": [
                            {
                                "report_no": source["report_no"],
                                "report_id": source["report_id"],
                                "source": source["source"],
                                "attachment_path": source["attachment_path"],
                            }
                            for source in group["sources"]
                        ],
                    }
                    for group in pair["value_groups"]
                ],
            }
            for pair in view["pairs"]
        ],
    }


app = create_app()
