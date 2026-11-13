"""SQLite 持久化层。

所有任务状态都落库：进程关闭再打开后，后台工作线程可以接着轮询已创建的任务。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key            TEXT NOT NULL UNIQUE,
    biz_date            TEXT NOT NULL,
    remote_task_id      TEXT,
    status              TEXT NOT NULL,
    phase               TEXT NOT NULL,
    attempt             INTEGER NOT NULL DEFAULT 1,
    cancel_requested    INTEGER NOT NULL DEFAULT 0,
    fail_stage          TEXT,
    fail_reason         TEXT,
    fault_config        TEXT NOT NULL DEFAULT '{}',
    link_expires_at     TEXT,
    next_run_at         TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_biz_date ON tasks(biz_date);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_pk     INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY(task_pk) REFERENCES tasks(id)
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_pk, id);

CREATE TABLE IF NOT EXISTS request_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_pk         INTEGER,
    method          TEXT NOT NULL,
    url             TEXT NOT NULL,
    request_headers TEXT,
    request_body    TEXT,
    status_code     INTEGER,
    response_headers TEXT,
    response_body   TEXT,
    error           TEXT,
    retry_after     REAL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reqlogs_task ON request_logs(task_pk, id);

CREATE TABLE IF NOT EXISTS file_records (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_pk             INTEGER NOT NULL,
    local_path          TEXT,
    manifest            TEXT,
    expected_size       INTEGER,
    actual_size         INTEGER,
    expected_sha256     TEXT,
    actual_sha256       TEXT,
    expected_time_start TEXT,
    expected_time_end   TEXT,
    actual_time_start   TEXT,
    actual_time_end     TEXT,
    check_ok            INTEGER NOT NULL DEFAULT 0,
    check_detail        TEXT,
    location            TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_filerecords_task ON file_records(task_pk, id);

CREATE TABLE IF NOT EXISTS ingested_rows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_pk     INTEGER NOT NULL,
    biz_date    TEXT NOT NULL,
    row_no      INTEGER NOT NULL,
    biz_time    TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ingested_date ON ingested_rows(biz_date);
"""

# 终态：不再被工作线程调度
TERMINAL_STATUSES = {"INGESTED", "FAILED", "CANCELLED"}
# 进程重启后需要回退到 GENERATING 的瞬态（远端任务仍然存在，从轮询继续）
RECOVERABLE_TRANSIENT = {"DOWNLOADING", "VERIFYING", "INGESTING", "POLLING"}

_lock = threading.RLock()
_local = threading.local()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def connect() -> sqlite3.Connection:
    """每个线程一个独立连接（HTTP 处理线程与工作线程不会交叉使用 cursor）。"""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        return conn
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    with _lock:
        conn.executescript(_SCHEMA)
        conn.commit()
    _local.conn = conn
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out = dict(row)
    if out.get("fault_config"):
        try:
            out["fault_config"] = json.loads(out["fault_config"])
        except (TypeError, ValueError):
            pass
    return out


# ---------------- 任务 CRUD ----------------
def create_task(task_key: str, biz_date: str, fault_config: dict) -> dict:
    with _lock:
        conn = connect()
        ts = now_iso()
        cur = conn.execute(
            """INSERT INTO tasks(task_key, biz_date, status, phase, fault_config,
                                 cancel_requested, created_at, updated_at)
               VALUES(?, ?, 'PENDING', 'WAITING', ?, 0, ?, ?)""",
            (task_key, biz_date, json.dumps(fault_config, ensure_ascii=False), ts, ts),
        )
        task_pk = cur.lastrowid
        conn.execute(
            "INSERT INTO task_events(task_pk, event_type, detail, created_at) VALUES(?,?,?,?)",
            (task_pk, "TASK_CREATED", json.dumps({"biz_date": biz_date}, ensure_ascii=False), ts),
        )
        conn.commit()
        return get_task_by_pk(task_pk)  # type: ignore[return-value]


def get_task_by_key(task_key: str) -> dict | None:
    return _row_to_dict(connect().execute("SELECT * FROM tasks WHERE task_key=?", (task_key,)).fetchone())


def get_task_by_pk(task_pk: int) -> dict | None:
    return _row_to_dict(connect().execute("SELECT * FROM tasks WHERE id=?", (task_pk,)).fetchone())


def list_tasks(limit: int = 100) -> list[dict]:
    rows = connect().execute("SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [_row_to_dict(r) for r in rows]  # type: ignore[list-item]


def list_due_tasks() -> list[dict]:
    """返回所有未到终态且到达 next_run_at（或无退避）的任务。"""
    ts = now_iso()
    rows = connect().execute(
        """SELECT * FROM tasks
           WHERE status NOT IN ('INGESTED','FAILED','CANCELLED')
             AND (next_run_at IS NULL OR next_run_at <= ?)
           ORDER BY id""",
        (ts,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]  # type: ignore[list-item]


def update_task(task_pk: int, **fields: Any) -> None:
    if not fields:
        return
    if "fault_config" in fields and isinstance(fields["fault_config"], (dict, list)):
        fields["fault_config"] = json.dumps(fields["fault_config"], ensure_ascii=False)
    fields["updated_at"] = now_iso()
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [task_pk]
    with _lock:
        conn = connect()
        conn.execute(f"UPDATE tasks SET {sets} WHERE id=?", vals)
        conn.commit()


def add_event(task_pk: int, event_type: str, detail: dict | str | None = None) -> None:
    if isinstance(detail, dict):
        detail = json.dumps(detail, ensure_ascii=False)
    with _lock:
        conn = connect()
        conn.execute(
            "INSERT INTO task_events(task_pk, event_type, detail, created_at) VALUES(?,?,?,?)",
            (task_pk, event_type, detail, now_iso()),
        )
        conn.commit()


def add_request_log(task_pk: int | None, *, method: str, url: str, request_headers: dict | None,
                    request_body: Any, status_code: int | None, response_headers: dict | None,
                    response_body: Any, error: str | None, retry_after: float | None) -> None:
    def _dump(v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return str(v)

    with _lock:
        conn = connect()
        conn.execute(
            """INSERT INTO request_logs(task_pk, method, url, request_headers, request_body,
                                        status_code, response_headers, response_body, error,
                                        retry_after, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (task_pk, method, url, _dump(request_headers), _dump(request_body), status_code,
             _dump(response_headers), _dump(response_body), error, retry_after, now_iso()),
        )
        conn.commit()


def list_request_logs(task_pk: int) -> list[dict]:
    rows = connect().execute(
        "SELECT * FROM request_logs WHERE task_pk=? ORDER BY id", (task_pk,)
    ).fetchall()
    return [dict(r) for r in rows]


def add_file_record(task_pk: int, **fields: Any) -> int:
    fields.setdefault("created_at", now_iso())
    cols = ", ".join(fields)
    marks = ", ".join("?" for _ in fields)
    with _lock:
        conn = connect()
        cur = conn.execute(
            f"INSERT INTO file_records(task_pk, {cols}) VALUES(?, {marks})",
            [task_pk, *fields.values()],
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]


def list_file_records(task_pk: int) -> list[dict]:
    rows = connect().execute(
        "SELECT * FROM file_records WHERE task_pk=? ORDER BY id", (task_pk,)
    ).fetchall()
    return [dict(r) for r in rows]


def list_events(task_pk: int) -> list[dict]:
    rows = connect().execute(
        "SELECT * FROM task_events WHERE task_pk=? ORDER BY id", (task_pk,)
    ).fetchall()
    return [dict(r) for r in rows]


def query_ingested(biz_date: str | None, limit: int, offset: int) -> tuple[list[dict], int]:
    conn = connect()
    if biz_date:
        total = conn.execute(
            "SELECT COUNT(*) c FROM ingested_rows WHERE biz_date=?", (biz_date,)
        ).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM ingested_rows WHERE biz_date=? ORDER BY id LIMIT ? OFFSET ?",
            (biz_date, limit, offset),
        ).fetchall()
    else:
        total = conn.execute("SELECT COUNT(*) c FROM ingested_rows").fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM ingested_rows ORDER BY id LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()
    return [dict(r) for r in rows], total


def recover_on_startup() -> list[dict]:
    """重启恢复。

    - 瞬态（DOWNLOADING/VERIFYING/INGESTING/POLLING）回退到 GENERATING，重新轮询/下载
    - WAITING/GENERATING 等活动状态保持不变，但同样记录恢复事件并清除退避，
      使「关闭再打开后接着轮询」在时间线上可观察
    """
    conn = connect()
    ts = now_iso()
    rows = conn.execute(
        "SELECT * FROM tasks WHERE status NOT IN ('INGESTED','FAILED','CANCELLED')"
    ).fetchall()
    recovered = [_row_to_dict(r) for r in rows]
    if not rows:
        return recovered  # type: ignore[return-value]
    placeholders = ",".join("?" for _ in RECOVERABLE_TRANSIENT)
    conn.execute(
        f"UPDATE tasks SET status='GENERATING', phase='GENERATING', next_run_at=NULL, "
        f"link_expires_at=NULL, updated_at=? WHERE status IN ({placeholders})",
        [ts, *RECOVERABLE_TRANSIENT],
    )
    conn.execute(
        "UPDATE tasks SET next_run_at=NULL, updated_at=? "
        "WHERE status NOT IN ('INGESTED','FAILED','CANCELLED')",
        (ts,),
    )
    for r in rows:
        conn.execute(
            "INSERT INTO task_events(task_pk, event_type, detail, created_at) VALUES(?,?,?,?)",
            (r["id"], "RECOVERED_ON_STARTUP",
             json.dumps({"from_status": r["status"], "from_phase": r["phase"]},
                        ensure_ascii=False), ts),
        )
    conn.commit()
    return recovered  # type: ignore[return-value]


def reset_for_retry(task_pk: int) -> None:
    """同一业务日期重试：复用任务行，attempt+1，回到 PENDING。"""
    ts = now_iso()
    with _lock:
        conn = connect()
        conn.execute(
            """UPDATE tasks SET status='PENDING', phase='WAITING', remote_task_id=NULL,
                               cancel_requested=0, fail_stage=NULL, fail_reason=NULL,
                               link_expires_at=NULL, next_run_at=NULL,
                               attempt=attempt+1, updated_at=?
               WHERE id=?""",
            (ts, task_pk),
        )
        conn.execute(
            "INSERT INTO task_events(task_pk, event_type, detail, created_at) VALUES(?,?,?,?)",
            (task_pk, "TASK_RETRY", None, ts),
        )
        conn.commit()
