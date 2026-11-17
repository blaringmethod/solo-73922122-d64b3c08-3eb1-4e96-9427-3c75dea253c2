"""SQLite persistence and cross-instance coordination.

The same database file is shared by two collector instances and the web UI.
SQLite in WAL mode with a busy timeout supports that; the scan lease stored in
``mailbox_state`` keeps only one collector writing a given mailbox at a time.

Two guards make stale scans harmless:

* every scan owns a random ``scan_token``; progress updates, marks and lease
  clearing are conditional on that token still being current;
* the token is bound to the UIDVALIDITY captured by SELECT.  A mailbox rebuild
  resets the generation, clears the lease and marks old pending deliveries as
  stale, so a scan from before the rebuild can neither advance progress nor
  mark a message in the rebuilt mailbox.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 3


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = _connect(self.path)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mailbox_state (
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    uidvalidity INTEGER NOT NULL DEFAULT 0,
                    generation INTEGER NOT NULL DEFAULT 0,
                    last_uid INTEGER NOT NULL DEFAULT 0,
                    scan_token TEXT,
                    scan_instance TEXT,
                    scan_uidvalidity INTEGER,
                    scan_started_at REAL,
                    scan_heartbeat_at REAL,
                    scan_lease_seconds REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (account, mailbox)
                );

                CREATE TABLE IF NOT EXISTS scan_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT NOT NULL UNIQUE,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    instance TEXT NOT NULL,
                    uidvalidity INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    scanned INTEGER NOT NULL DEFAULT 0,
                    accepted INTEGER NOT NULL DEFAULT 0,
                    rejected INTEGER NOT NULL DEFAULT 0,
                    skipped INTEGER NOT NULL DEFAULT 0,
                    reused_reports INTEGER NOT NULL DEFAULT 0,
                    marked INTEGER NOT NULL DEFAULT 0,
                    last_uid INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS message_record (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    uidvalidity INTEGER NOT NULL,
                    uid INTEGER NOT NULL,
                    generation INTEGER NOT NULL,
                    raw_sha256 TEXT NOT NULL,
                    subject TEXT,
                    from_addr TEXT,
                    date_header TEXT,
                    decision TEXT NOT NULL CHECK (decision IN ('accepted', 'rejected')),
                    reason_code TEXT,
                    reason TEXT,
                    mark_state TEXT NOT NULL DEFAULT 'pending'
                        CHECK (mark_state IN ('pending', 'marked', 'stale')),
                    mark_attempts INTEGER NOT NULL DEFAULT 0,
                    marked_at TEXT,
                    receipt_sha256 TEXT NOT NULL,
                    receipt_path TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    report_nos TEXT NOT NULL DEFAULT '[]',
                    detail TEXT,
                    scan_instance TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (account, mailbox, uidvalidity, uid)
                );

                CREATE TABLE IF NOT EXISTS report (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    report_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    item TEXT NOT NULL,
                    value TEXT NOT NULL,
                    rows_json TEXT NOT NULL,
                    attachment_sha256 TEXT NOT NULL,
                    attachment_name TEXT NOT NULL,
                    attachment_path TEXT NOT NULL,
                    source_message_id INTEGER NOT NULL REFERENCES message_record(id),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (account, mailbox, report_no)
                );

                CREATE TABLE IF NOT EXISTS delivery (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES message_record(id),
                    report_no TEXT NOT NULL,
                    attachment_sha256 TEXT NOT NULL,
                    attachment_name TEXT NOT NULL,
                    attachment_path TEXT NOT NULL,
                    disposition TEXT NOT NULL CHECK (disposition IN ('new', 'reused')),
                    created_at TEXT NOT NULL,
                    UNIQUE (message_id, report_no)
                );

                CREATE INDEX IF NOT EXISTS idx_message_created
                    ON message_record(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_message_mark
                    ON message_record(account, mailbox, mark_state);
                CREATE INDEX IF NOT EXISTS idx_report_no
                    ON report(account, mailbox, report_no);
                CREATE INDEX IF NOT EXISTS idx_delivery_message
                    ON delivery(message_id);
                CREATE INDEX IF NOT EXISTS idx_scan_started
                    ON scan_run(started_at DESC);

                -- ---------------------------------------------------- checklist
                -- One row per distinct (sample_no, item, value) in the data
                -- rows of accepted reports.  It is a projection of
                -- report.rows_json, maintained by the ingestion transaction
                -- as well as by the online backfill, so checking a checklist
                -- never rescans report history.  ``report_id`` is the first
                -- report that contributed the value; ``source`` is 'live' for
                -- rows written at ingestion and 'backfill' for archived rows.
                CREATE TABLE IF NOT EXISTS report_value (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_no TEXT NOT NULL,
                    item TEXT NOT NULL,
                    value TEXT NOT NULL,
                    report_no TEXT NOT NULL,
                    report_id INTEGER,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    source TEXT NOT NULL CHECK (source IN ('live', 'backfill')),
                    created_at TEXT NOT NULL,
                    UNIQUE (sample_no, item, value)
                );
                CREATE INDEX IF NOT EXISTS idx_report_value_pair
                    ON report_value(sample_no, item);

                -- Every report that contributed a given (pair, value).  The
                -- value itself is counted once above ("同对同测值只计一次");
                -- this table lets the page list *all* source report numbers,
                -- including several reports carrying the same value.
                CREATE TABLE IF NOT EXISTS report_value_source (
                    sample_no TEXT NOT NULL,
                    item TEXT NOT NULL,
                    value TEXT NOT NULL,
                    report_no TEXT NOT NULL,
                    report_id INTEGER,
                    account TEXT NOT NULL,
                    mailbox TEXT NOT NULL,
                    source TEXT NOT NULL CHECK (source IN ('live', 'backfill')),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (sample_no, item, value, report_no)
                );
                CREATE INDEX IF NOT EXISTS idx_value_source_pair
                    ON report_value_source(sample_no, item);

                -- A checklist is user input: batch number plus up to 100
                -- unique (sample, item) pairs.  Accepted pairs are immutable;
                -- a corrected list is a new checklist with a new batch number.
                CREATE TABLE IF NOT EXISTS checklist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    pair_count INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'evaluating'
                        CHECK (state IN ('evaluating', 'ready', 'not_ready')),
                    UNIQUE (batch_no)
                );

                CREATE TABLE IF NOT EXISTS checklist_pair (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checklist_id INTEGER NOT NULL
                        REFERENCES checklist(id) ON DELETE CASCADE,
                    line_no INTEGER NOT NULL,
                    sample_no TEXT NOT NULL,
                    item TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'missing', 'ok', 'conflict')),
                    checked_at TEXT,
                    UNIQUE (checklist_id, sample_no, item)
                );
                CREATE INDEX IF NOT EXISTS idx_pair_checklist
                    ON checklist_pair(checklist_id);
                CREATE INDEX IF NOT EXISTS idx_pair_key
                    ON checklist_pair(sample_no, item);

                -- Online backfill cursor over report history.  The singleton
                -- row is leased exactly like a mailbox scan: two instances can
                -- never count the same archived report twice, and a restart
                -- resumes from the last committed cursor.
                CREATE TABLE IF NOT EXISTS backfill_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cursor_report_id INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'complete', 'failed')),
                    lease_token TEXT,
                    lease_instance TEXT,
                    lease_heartbeat_at REAL,
                    lease_seconds REAL NOT NULL DEFAULT 0,
                    started_at TEXT,
                    finished_at TEXT,
                    scanned_reports INTEGER NOT NULL DEFAULT 0,
                    inserted_values INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    updated_at TEXT NOT NULL
                );
                INSERT OR IGNORE INTO backfill_state
                    (id, cursor_report_id, status, updated_at)
                    VALUES (1, 0, 'pending', '2026-01-01T00:00:00+00:00');
                """
            )
            # Small forward-only migrations for databases created by an
            # earlier build of this service.
            scan_cols = {row[1] for row in conn.execute("PRAGMA table_info(scan_run)")}
            if "marked" not in scan_cols:
                conn.execute(
                    "ALTER TABLE scan_run ADD COLUMN marked INTEGER NOT NULL DEFAULT 0"
                )
            state_cols = {row[1] for row in conn.execute("PRAGMA table_info(mailbox_state)")}
            if "scan_lease_seconds" not in state_cols:
                conn.execute(
                    "ALTER TABLE mailbox_state"
                    " ADD COLUMN scan_lease_seconds REAL NOT NULL DEFAULT 0"
                )
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        finally:
            conn.close()

    @contextmanager
    def connect(self):
        conn = _connect(self.path)
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ state
    def get_state(self, account: str, mailbox: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM mailbox_state WHERE account=? AND mailbox=?",
                (account, mailbox),
            ).fetchone()

    def claim_scan(
        self,
        account: str,
        mailbox: str,
        server_uidvalidity: int,
        instance: str,
        lease_seconds: float,
    ) -> tuple[bool, str | None, int]:
        """Atomically claim the mailbox scan lease.

        Returns ``(claimed, token, generation)``.  If the server's UIDVALIDITY
        differs from the stored one, the mailbox generation is reset first and
        every live lease from the old generation is revoked.  Pending marks
        from the old generation become ``stale`` and will never be applied to
        messages that reuse their UIDs in the rebuilt mailbox.
        """
        token = uuid.uuid4().hex
        now = time.time()
        with self.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT * FROM mailbox_state WHERE account=? AND mailbox=?",
                    (account, mailbox),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """INSERT INTO mailbox_state
                           (account, mailbox, uidvalidity, generation, last_uid,
                            updated_at)
                           VALUES (?, ?, ?, 1, 0, ?)""",
                        (account, mailbox, server_uidvalidity, utcnow()),
                    )
                    generation = 1
                else:
                    generation = row["generation"]
                    if row["uidvalidity"] != server_uidvalidity:
                        generation += 1
                        # Invalidate any scan belonging to the old mailbox and
                        # quarantine its pending marks.
                        conn.execute(
                            """UPDATE message_record SET mark_state='stale',
                                   updated_at=?
                               WHERE account=? AND mailbox=? AND uidvalidity<>?
                                 AND mark_state='pending'""",
                            (utcnow(), account, mailbox, server_uidvalidity),
                        )
                        conn.execute(
                            """UPDATE mailbox_state
                               SET uidvalidity=?, generation=?, last_uid=0,
                                   scan_token=NULL, scan_instance=NULL,
                                   scan_uidvalidity=NULL, scan_started_at=NULL,
                                   scan_heartbeat_at=NULL, updated_at=?
                               WHERE account=? AND mailbox=?""",
                            (
                                server_uidvalidity,
                                generation,
                                utcnow(),
                                account,
                                mailbox,
                            ),
                        )
                        row = conn.execute(
                            "SELECT * FROM mailbox_state WHERE account=? AND mailbox=?",
                            (account, mailbox),
                        ).fetchone()

                row = conn.execute(
                    "SELECT * FROM mailbox_state WHERE account=? AND mailbox=?",
                    (account, mailbox),
                ).fetchone()
                # Lease expiry uses the duration stored when the lease was
                # granted: the instance asking now does not know how long the
                # other instance's scan was supposed to live.
                active = (
                    row["scan_token"] is not None
                    and row["scan_heartbeat_at"] is not None
                    and now - row["scan_heartbeat_at"] < (row["scan_lease_seconds"] or 0)
                    and row["scan_uidvalidity"] == server_uidvalidity
                )
                # An unexpired lease always belongs to a live scan, whether it
                # runs in this process or the other instance.  Taking it over
                # would let two scans interleave, so both are refused.
                if active:
                    return False, None, int(row["generation"])

                conn.execute(
                    """UPDATE mailbox_state
                       SET scan_token=?, scan_instance=?, scan_uidvalidity=?,
                           scan_started_at=?, scan_heartbeat_at=?,
                           scan_lease_seconds=?, updated_at=?
                       WHERE account=? AND mailbox=?""",
                    (
                        token,
                        instance,
                        server_uidvalidity,
                        now,
                        now,
                        lease_seconds,
                        utcnow(),
                        account,
                        mailbox,
                    ),
                )
                conn.execute(
                    """INSERT INTO scan_run
                       (token, account, mailbox, instance, uidvalidity, status,
                        started_at)
                       VALUES (?, ?, ?, ?, ?, 'running', ?)""",
                    (token, account, mailbox, instance, server_uidvalidity, utcnow()),
                )
                return True, token, generation

    def token_is_current(self, token: str, uidvalidity: int) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT 1 FROM mailbox_state
                   WHERE scan_token=? AND uidvalidity=? AND scan_uidvalidity=?""",
                (token, uidvalidity, uidvalidity),
            ).fetchone()
            return row is not None

    def set_progress(self, token: str, uidvalidity: int, last_uid: int) -> bool:
        """Advance progress only if this scan still owns the current mailbox."""
        with self.connect() as conn:
            with transaction(conn):
                cur = conn.execute(
                    """UPDATE mailbox_state
                       SET last_uid=CASE WHEN last_uid > ? THEN last_uid ELSE ? END,
                           scan_heartbeat_at=?, updated_at=?
                       WHERE scan_token=? AND uidvalidity=? AND scan_uidvalidity=?""",
                    (
                        last_uid,
                        last_uid,
                        time.time(),
                        utcnow(),
                        token,
                        uidvalidity,
                        uidvalidity,
                    ),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        "UPDATE scan_run SET last_uid=? WHERE token=?",
                        (last_uid, token),
                    )
                return cur.rowcount == 1

    def finish_scan(
        self,
        token: str,
        status: str,
        counts: dict[str, int] | None = None,
        error: str | None = None,
    ) -> None:
        counts = counts or {}
        with self.connect() as conn:
            with transaction(conn):
                conn.execute(
                    """UPDATE scan_run
                       SET status=?, finished_at=?, scanned=?, accepted=?,
                           rejected=?, skipped=?, reused_reports=?, marked=?,
                           error=?
                       WHERE token=?""",
                    (
                        status,
                        utcnow(),
                        counts.get("scanned", 0),
                        counts.get("accepted", 0),
                        counts.get("rejected", 0),
                        counts.get("skipped", 0),
                        counts.get("reused_reports", 0),
                        counts.get("marked", 0),
                        error,
                        token,
                    ),
                )
                conn.execute(
                    """UPDATE mailbox_state
                       SET scan_token=NULL, scan_instance=NULL,
                           scan_uidvalidity=NULL, scan_started_at=NULL,
                           scan_heartbeat_at=NULL, updated_at=?
                       WHERE scan_token=?""",
                    (utcnow(), token),
                )

    def adopt_changed_uidvalidity(
        self,
        account: str,
        mailbox: str,
        expected_uidvalidity: int,
        observed_uidvalidity: int,
        token: str,
    ) -> bool:
        """Record a UIDVALIDITY change observed by a running scan.

        Applies only while the caller's scan still owns the lease for the
        mailbox generation it thought it was scanning.  If any other scan has
        since claimed the mailbox, or a newer generation was already adopted,
        nothing happens: a late observer can never roll back progress that a
        newer scan made.
        """
        with self.connect() as conn:
            with transaction(conn):
                row = conn.execute(
                    "SELECT * FROM mailbox_state WHERE account=? AND mailbox=?",
                    (account, mailbox),
                ).fetchone()
                if (
                    row is None
                    or row["uidvalidity"] != expected_uidvalidity
                    or row["scan_token"] != token
                    or row["scan_uidvalidity"] != expected_uidvalidity
                    or observed_uidvalidity == expected_uidvalidity
                ):
                    return False
                conn.execute(
                    """UPDATE message_record SET mark_state='stale', updated_at=?
                       WHERE account=? AND mailbox=? AND uidvalidity<>?
                         AND mark_state='pending'""",
                    (utcnow(), account, mailbox, observed_uidvalidity),
                )
                conn.execute(
                    """UPDATE mailbox_state
                       SET uidvalidity=?, generation=generation+1, last_uid=0,
                           scan_token=NULL, scan_instance=NULL,
                           scan_uidvalidity=NULL, scan_started_at=NULL,
                           scan_heartbeat_at=NULL, updated_at=?
                       WHERE account=? AND mailbox=? AND uidvalidity=?
                         AND scan_token=?""",
                    (
                        observed_uidvalidity,
                        utcnow(),
                        account,
                        mailbox,
                        expected_uidvalidity,
                        token,
                    ),
                )
                return True

    # --------------------------------------------------------------- messages
    def message_for_uid(self, account: str, mailbox: str, uidvalidity: int, uid: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """SELECT * FROM message_record
                   WHERE account=? AND mailbox=? AND uidvalidity=? AND uid=?""",
                (account, mailbox, uidvalidity, uid),
            ).fetchone()

    def get_message(self, message_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM message_record WHERE id=?", (message_id,)
            ).fetchone()

    def list_messages(self, limit: int = 100, mark_state: str | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM message_record"
        params: list[Any] = []
        if mark_state:
            sql += " WHERE mark_state=?"
            params.append(mark_state)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            return conn.execute(sql, params).fetchall()

    def pending_messages(
        self,
        account: str,
        mailbox: str,
        uidvalidity: int | None = None,
    ) -> list[sqlite3.Row]:
        """Rows still waiting for the \\Seen acknowledgement.

        Newest first: a fresh message is the most likely one to be pending, and
        its UID is the least likely to have been reused.
        """
        sql = (
            "SELECT * FROM message_record WHERE mark_state='pending'"
            " AND account=? AND mailbox=?"
        )
        params: list[Any] = [account, mailbox]
        if uidvalidity is not None:
            sql += " AND uidvalidity=?"
            params.append(uidvalidity)
        sql += " ORDER BY id DESC"
        with self.connect() as conn:
            return conn.execute(sql, params).fetchall()

    def reports_for_message(self, message_id: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """SELECT d.report_no, d.attachment_sha256, d.attachment_name,
                          d.attachment_path, d.disposition, d.created_at,
                          r.id AS report_id, r.sample_no, r.item, r.value
                   FROM delivery d
                   LEFT JOIN report r
                     ON r.account=(SELECT account FROM message_record WHERE id=d.message_id)
                    AND r.mailbox=(SELECT mailbox FROM message_record WHERE id=d.message_id)
                    AND r.report_no=d.report_no
                   WHERE d.message_id=?
                   ORDER BY d.report_no""",
                (message_id,),
            ).fetchall()

    def get_report(self, report_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM report WHERE id=?", (report_id,)).fetchone()

    def list_reports(self, limit: int = 200) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM report ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    # ----------------------------------------------------------- checklist data
    def create_checklist(
        self,
        batch_no: str,
        pairs: list[tuple[str, str]],
        line_offset: int = 2,
    ) -> tuple[int | None, str | None]:
        """Insert an immutable checklist, evaluating it if the archive is ready.

        Everything happens in one transaction: a checklist is never observed
        half-inserted, and against a ready archive it is evaluated before the
        commit that makes it visible.  If the backfill is still running the
        checklist stays in ``evaluating`` and is evaluated by
        :meth:`try_finish_backfill`.
        """
        with self.connect() as conn:
            with transaction(conn):
                existing = conn.execute(
                    "SELECT id FROM checklist WHERE batch_no=?", (batch_no,)
                ).fetchone()
                if existing is not None:
                    return None, "该批号已受理，清单不可修改"
                cur = conn.execute(
                    """INSERT INTO checklist(batch_no, created_at, pair_count, state)
                       VALUES (?, ?, ?, 'evaluating')""",
                    (batch_no, utcnow(), len(pairs)),
                )
                checklist_id = int(cur.lastrowid)
                for index, (sample_no, item) in enumerate(pairs):
                    conn.execute(
                        """INSERT INTO checklist_pair
                           (checklist_id, line_no, sample_no, item, status)
                           VALUES (?, ?, ?, ?, 'pending')""",
                        (checklist_id, index + line_offset, sample_no, item),
                    )
                if _backfill_complete_tx(conn):
                    _evaluate_checklist_tx(conn, checklist_id)
        return checklist_id, None

    def list_checklists(self, limit: int = 100) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """SELECT c.*,
                          SUM(CASE WHEN p.status='ok' THEN 1 ELSE 0 END) AS ok_count,
                          SUM(CASE WHEN p.status='missing' THEN 1 ELSE 0 END) AS missing_count,
                          SUM(CASE WHEN p.status='conflict' THEN 1 ELSE 0 END) AS conflict_count,
                          SUM(CASE WHEN p.status='pending' THEN 1 ELSE 0 END) AS pending_count
                   FROM checklist c
                   LEFT JOIN checklist_pair p ON p.checklist_id=c.id
                   GROUP BY c.id
                   ORDER BY c.id DESC LIMIT ?""",
                (limit,),
            ).fetchall()

    def get_checklist(self, checklist_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM checklist WHERE id=?", (checklist_id,)
            ).fetchone()

    def checklist_pairs(self, checklist_id: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """SELECT * FROM checklist_pair
                   WHERE checklist_id=? ORDER BY line_no, id""",
                (checklist_id,),
            ).fetchall()

    def checklist_sources(self, checklist_id: int) -> list[sqlite3.Row]:
        """Every distinct value contributing to a checklist, with its source.

        Joins only the projection and the report row that first contributed
        each value.  It never scans report history; per pair there are only as
        many rows as there are distinct values.
        """
        with self.connect() as conn:
            return conn.execute(
                """SELECT p.id AS pair_id, p.line_no, p.sample_no, p.item,
                          s.value, s.report_no, s.report_id, s.source,
                          s.account, s.mailbox, s.created_at AS value_seen_at,
                          r.attachment_path, r.attachment_name,
                          r.source_message_id, r.created_at AS report_created_at
                   FROM checklist_pair p
                   LEFT JOIN report_value_source s
                     ON s.sample_no=p.sample_no AND s.item=p.item
                   LEFT JOIN report r ON r.id=s.report_id
                   WHERE p.checklist_id=?
                   ORDER BY p.line_no, s.value, s.report_no""",
                (checklist_id,),
            ).fetchall()

    def checklist_summary(self, checklist_id: int) -> dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT COUNT(*) AS pairs,
                          SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok,
                          SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END) AS missing,
                          SUM(CASE WHEN status='conflict' THEN 1 ELSE 0 END) AS conflict,
                          SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending
                   FROM checklist_pair WHERE checklist_id=?""",
                (checklist_id,),
            ).fetchone()
            return {key: int(row[key] or 0) for key in row.keys()}

    # --------------------------------------------------------------- backfill
    def backfill_state(self) -> sqlite3.Row:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM backfill_state WHERE id=1").fetchone()
            assert row is not None
            return row

    def backfill_progress(self) -> dict[str, Any]:
        """Cheap status for the UI: never scans report history.

        Counting every report row on each page view would itself be a history
        scan, so progress is derived from the committed cursor and the current
        maximum report id (both indexed).  Exact counters live in
        ``backfill_state`` and are reported as ``reports_projected``.
        """
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM backfill_state WHERE id=1").fetchone()
            max_id = conn.execute("SELECT MAX(id) AS m FROM report").fetchone()["m"]
        state = dict(row) if row is not None else {}
        cursor = int(state.get("cursor_report_id") or 0)
        max_id = int(max_id or 0)
        state["report_total"] = max_id
        if state.get("status") == "complete":
            # Live ingestions project their values in their own transaction,
            # so a report with id > cursor is already covered.
            state["reports_projected"] = max_id
            state["percent"] = 100
        else:
            state["reports_projected"] = cursor
            state["percent"] = min(99, int(cursor * 100 / max_id)) if max_id else 0
        return state

    def claim_backfill(
        self,
        instance: str,
        lease_seconds: float,
    ) -> tuple[bool, str | None]:
        """Take the exclusive backfill lease.

        An unexpired lease owned by a *different* instance is never stolen.
        A lease owned by the same ``instance`` is taken over, because that
        identity can only be running again after a restart (a misconfigured
        duplicate would still be stopped by the token check on every batch).
        """
        token = uuid.uuid4().hex
        now = time.time()
        with self.connect() as conn:
            with transaction(conn):
                row = conn.execute("SELECT * FROM backfill_state WHERE id=1").fetchone()
                assert row is not None
                if row["status"] == "complete":
                    return False, None
                active = (
                    row["lease_token"] is not None
                    and row["lease_heartbeat_at"] is not None
                    and now - row["lease_heartbeat_at"] < (row["lease_seconds"] or 0)
                )
                if active and row["lease_instance"] != instance:
                    return False, None
                # A run that stopped between batches (crash, restart, or a
                # bounded run that released the lease) is resumable by anyone.
                resumable = row["status"] in {"pending", "failed", "running"}
                if not resumable:
                    return False, None
                conn.execute(
                    """UPDATE backfill_state
                       SET status='running', lease_token=?, lease_instance=?,
                           lease_heartbeat_at=?, lease_seconds=?,
                           started_at=COALESCE(started_at, ?), error=NULL,
                           updated_at=?
                       WHERE id=1""",
                    (token, instance, now, lease_seconds, utcnow(), utcnow()),
                )
        return True, token

    def backfill_batch(self, token: str, batch_size: int) -> dict[str, Any] | None:
        """Project one batch of archived reports into ``report_value``.

        The cursor, the projected rows and the heartbeat move together in one
        transaction.  A crash either commits the whole batch or none of it, so
        a resumed backfill can neither skip reports nor count one twice; the
        projection's UNIQUE key makes the writes idempotent as well.
        """
        with self.connect() as conn:
            with transaction(conn):
                state = conn.execute(
                    "SELECT * FROM backfill_state WHERE id=1"
                ).fetchone()
                if state is None or state["lease_token"] != token:
                    return None
                cursor = int(state["cursor_report_id"])
                reports = conn.execute(
                    """SELECT id, account, mailbox, report_no, rows_json
                       FROM report WHERE id > ? ORDER BY id LIMIT ?""",
                    (cursor, batch_size),
                ).fetchall()
                inserted = 0
                for report in reports:
                    for row in json.loads(report["rows_json"]):
                        cur = conn.execute(
                            """INSERT OR IGNORE INTO report_value
                               (sample_no, item, value, report_no, report_id,
                                account, mailbox, source, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, 'backfill', ?)""",
                            (
                                row[1],
                                row[2],
                                row[3],
                                report["report_no"],
                                int(report["id"]),
                                report["account"],
                                report["mailbox"],
                                utcnow(),
                            ),
                        )
                        inserted += cur.rowcount
                        _insert_value_source_tx(
                            conn,
                            row[1],
                            row[2],
                            row[3],
                            report["report_no"],
                            int(report["id"]),
                            report["account"],
                            report["mailbox"],
                            "backfill",
                        )
                last_id = int(reports[-1]["id"]) if reports else cursor
                conn.execute(
                    """UPDATE backfill_state
                       SET cursor_report_id=?,
                           scanned_reports=scanned_reports+?,
                           inserted_values=inserted_values+?,
                           lease_heartbeat_at=?, updated_at=?
                       WHERE id=1""",
                    (last_id, len(reports), inserted, time.time(), utcnow()),
                )
                remaining = conn.execute(
                    "SELECT COUNT(*) AS n FROM report WHERE id > ?", (last_id,)
                ).fetchone()["n"]
                return {
                    "processed": len(reports),
                    "inserted": inserted,
                    "cursor": last_id,
                    "remaining": int(remaining),
                }

    def try_finish_backfill(self, token: str) -> bool | None:
        """Mark the projection complete and evaluate waiting checklists.

        Completion and checklist evaluation share one transaction.  A checklist
        therefore never becomes ready against a half-built projection, and the
        "preparing" label disappears only when every pair has been evaluated.
        Returns ``False`` when reports remain (the caller runs another batch);
        ``None`` when the lease was lost.
        """
        with self.connect() as conn:
            with transaction(conn):
                state = conn.execute(
                    "SELECT * FROM backfill_state WHERE id=1"
                ).fetchone()
                if state is None or state["lease_token"] != token:
                    return None
                remaining = conn.execute(
                    "SELECT COUNT(*) AS n FROM report WHERE id > ?",
                    (int(state["cursor_report_id"]),),
                ).fetchone()["n"]
                if remaining:
                    return False
                conn.execute(
                    """UPDATE backfill_state
                       SET status='complete', lease_token=NULL, lease_instance=NULL,
                           lease_heartbeat_at=NULL, finished_at=?, error=NULL,
                           updated_at=?
                       WHERE id=1""",
                    (utcnow(), utcnow()),
                )
                for row in conn.execute(
                    "SELECT id FROM checklist WHERE state='evaluating'"
                ).fetchall():
                    _evaluate_checklist_tx(conn, int(row["id"]))
        return True

    def fail_backfill(self, token: str, error: str) -> bool:
        """Release the lease after an error; checklists stay in "preparing"."""
        with self.connect() as conn:
            with transaction(conn):
                cur = conn.execute(
                    """UPDATE backfill_state
                       SET status='failed', lease_token=NULL, lease_instance=NULL,
                           lease_heartbeat_at=?, error=?, updated_at=?
                       WHERE id=1 AND lease_token=?""",
                    (time.time(), error, utcnow(), token),
                )
                return cur.rowcount == 1

    def release_backfill(self, token: str) -> bool:
        """Give the lease back between bounded batches.

        The cursor is committed, so another instance (or this one on its next
        tick) continues exactly where this run stopped.  No conclusion is
        published: status remains not-complete until ``try_finish_backfill``.
        """
        with self.connect() as conn:
            with transaction(conn):
                cur = conn.execute(
                    """UPDATE backfill_state
                       SET status='pending', lease_token=NULL, lease_instance=NULL,
                           lease_heartbeat_at=NULL, updated_at=?
                       WHERE id=1 AND lease_token=? AND status='running'""",
                    (utcnow(), token),
                )
                return cur.rowcount == 1

    def set_mark_state(self, message_id: int, state: str) -> None:
        with self.connect() as conn:
            with transaction(conn):
                conn.execute(
                    """UPDATE message_record
                       SET mark_state=?, mark_attempts=mark_attempts+1,
                           marked_at=CASE WHEN ?='marked' THEN ? ELSE marked_at END,
                           updated_at=?
                       WHERE id=?""",
                    (state, state, utcnow(), utcnow(), message_id),
                )

    def increment_mark_attempts(self, message_id: int) -> None:
        with self.connect() as conn:
            with transaction(conn):
                conn.execute(
                    "UPDATE message_record SET mark_attempts=mark_attempts+1, updated_at=? WHERE id=?",
                    (utcnow(), message_id),
                )

    # --------------------------------------------------------------- snapshot
    def stats(self) -> dict[str, Any]:
        with self.connect() as conn:
            def one(sql: str, params: Iterable[Any] = ()) -> int:
                row = conn.execute(sql, tuple(params)).fetchone()
                return int(row[0]) if row else 0

            return {
                "messages": one("SELECT COUNT(*) FROM message_record"),
                "accepted": one("SELECT COUNT(*) FROM message_record WHERE decision='accepted'"),
                "rejected": one("SELECT COUNT(*) FROM message_record WHERE decision='rejected'"),
                "pending_marks": one("SELECT COUNT(*) FROM message_record WHERE mark_state='pending'"),
                "marked": one("SELECT COUNT(*) FROM message_record WHERE mark_state='marked'"),
                "stale_marks": one("SELECT COUNT(*) FROM message_record WHERE mark_state='stale'"),
                "reports": one("SELECT COUNT(*) FROM report"),
                "scans": one("SELECT COUNT(*) FROM scan_run"),
            }

    def recent_scans(self, limit: int = 20) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM scan_run ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def active_scan(self) -> sqlite3.Row | None:
        """The lease holder, regardless of which instance owns it."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM mailbox_state WHERE scan_token IS NOT NULL LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return conn.execute(
                """SELECT * FROM scan_run
                   WHERE token=? ORDER BY id DESC LIMIT 1""",
                (row["scan_token"],),
            ).fetchone()


__all__ = [
    "Database",
    "transaction",
    "utcnow",
    "record_live_values_tx",
    "evaluate_open_checklist_tx",
]


def _backfill_complete_tx(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT status FROM backfill_state WHERE id=1").fetchone()
    return row is not None and row["status"] == "complete"


def record_live_values_tx(
    conn: sqlite3.Connection,
    report: sqlite3.Row,
) -> None:
    """Project one newly ingested report's data rows inside its transaction.

    The insert is the same write the online backfill performs, against the
    same UNIQUE key, so a report ingested while the backfill is behind can
    never be counted twice (the backfill's later ``INSERT OR IGNORE`` is a
    no-op), and a report ingested after completion is counted immediately.
    Ingestion and the checklist projection cannot diverge: either both commit
    or neither does.
    """
    for row in json.loads(report["rows_json"]):
        conn.execute(
            """INSERT OR IGNORE INTO report_value
               (sample_no, item, value, report_no, report_id,
                account, mailbox, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'live', ?)""",
            (
                row[1],
                row[2],
                row[3],
                report["report_no"],
                int(report["id"]),
                report["account"],
                report["mailbox"],
                utcnow(),
            ),
        )
        _insert_value_source_tx(
            conn,
            row[1],
            row[2],
            row[3],
            report["report_no"],
            int(report["id"]),
            report["account"],
            report["mailbox"],
            "live",
        )


def _insert_value_source_tx(
    conn: sqlite3.Connection,
    sample_no: str,
    item: str,
    value: str,
    report_no: str,
    report_id: int,
    account: str,
    mailbox: str,
    source: str,
) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO report_value_source
           (sample_no, item, value, report_no, report_id,
            account, mailbox, source, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            sample_no,
            item,
            value,
            report_no,
            report_id,
            account,
            mailbox,
            source,
            utcnow(),
        ),
    )


def evaluate_open_checklist_tx(
    conn: sqlite3.Connection,
    keys: set[tuple[str, str]],
) -> None:
    """Re-evaluate checklists affected by newly accepted data rows.

    Called from the same transaction that commits a newly accepted report, so
    a conflicting value and the checklist's ``conflict`` status become visible
    together: a checklist that was ready to hand off stops being ready at
    exactly the moment the conflicting report enters the archive, and it cannot
    be observed as ready afterwards.
    """
    if not _backfill_complete_tx(conn):
        # During the backfill the checklist is still labelled "preparing"; the
        # final evaluation in try_finish_backfill covers everything.
        return
    affected: set[int] = set()
    for sample_no, item in sorted(keys):
        pairs = conn.execute(
            """SELECT id, checklist_id, sample_no, item, status
               FROM checklist_pair WHERE sample_no=? AND item=?""",
            (sample_no, item),
        ).fetchall()
        for pair in pairs:
            _refresh_pair_tx(conn, pair)
            affected.add(int(pair["checklist_id"]))
    for checklist_id in sorted(affected):
        # ``_refresh_checklist_state_tx`` leaves an ``evaluating`` checklist
        # alone, so a live value cannot publish a conclusion early.
        _refresh_checklist_state_tx(conn, checklist_id)


def _refresh_pair_tx(conn: sqlite3.Connection, pair: sqlite3.Row) -> None:
    """Recompute one checklist pair from the projection.

    Status is decided by the number of *distinct* values for the pair:
    none -> missing, one -> ok, two or more -> conflict.  A pair that was
    ready can therefore only leave ``ok`` when a genuinely different value
    appears; a repeated identical value keeps it ``ok``.
    """
    values = conn.execute(
        """SELECT DISTINCT value FROM report_value
           WHERE sample_no=? AND item=?""",
        (pair["sample_no"], pair["item"]),
    ).fetchall()
    distinct = {row["value"] for row in values}
    if not distinct:
        status = "missing"
    elif len(distinct) == 1:
        status = "ok"
    else:
        status = "conflict"
    conn.execute(
        "UPDATE checklist_pair SET status=?, checked_at=? WHERE id=?",
        (status, utcnow(), pair["id"]),
    )


def _evaluate_checklist_tx(conn: sqlite3.Connection, checklist_id: int) -> None:
    """Evaluate every pair of a checklist and finalise its state.

    Called only once the archive projection is complete.  No conclusion is
    shown before this point: every pair receives its ``missing`` / ``ok`` /
    ``conflict`` status and only then does the checklist leave ``evaluating``.
    """
    pairs = conn.execute(
        "SELECT * FROM checklist_pair WHERE checklist_id=?", (checklist_id,)
    ).fetchall()
    for pair in pairs:
        _refresh_pair_tx(conn, pair)
    counts = conn.execute(
        """SELECT COUNT(*) AS pairs,
                  SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok
           FROM checklist_pair WHERE checklist_id=?""",
        (checklist_id,),
    ).fetchone()
    ready = int(counts["pairs"]) > 0 and int(counts["ok"] or 0) == int(counts["pairs"])
    conn.execute(
        "UPDATE checklist SET state=? WHERE id=?",
        ("ready" if ready else "not_ready", checklist_id),
    )


def _refresh_checklist_state_tx(conn: sqlite3.Connection, checklist_id: int) -> None:
    """Derive the headline: ready only when every pair is ok.

    Any missing or conflicting pair means "not ready".  A checklist in
    ``evaluating`` (backfill still running) is left alone: it has no
    conclusion yet, and the final pass will set it.
    """
    row = conn.execute(
        "SELECT state FROM checklist WHERE id=?", (checklist_id,)
    ).fetchone()
    if row is None or row["state"] == "evaluating":
        return
    counts = conn.execute(
        """SELECT COUNT(*) AS pairs,
                  SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok
           FROM checklist_pair WHERE checklist_id=?""",
        (checklist_id,),
    ).fetchone()
    ready = int(counts["pairs"]) > 0 and int(counts["ok"] or 0) == int(counts["pairs"])
    conn.execute(
        "UPDATE checklist SET state=? WHERE id=?",
        ("ready" if ready else "not_ready", checklist_id),
    )

