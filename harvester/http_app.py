"""采集服务 HTTP 入口：REST API + 采集页面静态托管。"""
from __future__ import annotations

import json
import mimetypes
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# 支持 `python3 -I harvester/http_app.py` 直接运行（-I 不含 cwd 且屏蔽 PYTHONPATH）
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from harvester import config, db
from harvester.engine import Engine

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
ENGINE = Engine()


def make_task_key(biz_date: str) -> str:
    # 同一组条件（当前为业务日期维度）只允许一个有效导出任务
    return f"biz:{biz_date}"


class HarvesterHandler(BaseHTTPRequestHandler):
    server_version = "Harvester/1.0"

    # ---------- 工具 ----------
    def _json(self, code: int, payload: dict | list) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def log_message(self, fmt: str, *args) -> None:
        pass

    # ---------- 路由 ----------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path, query = parsed.path, parse_qs(parsed.query)
        if path == "/health":
            self._json(200, {"status": "ok", "service": "harvester",
                             "time": datetime.now(timezone.utc).isoformat()})
        elif path == "/api/tasks":
            self._json(200, db.list_tasks())
        elif path.startswith("/api/tasks/") and path.endswith("/detail"):
            self._task_detail(int(path.split("/")[3]))
        elif path == "/api/ingested/data":
            self._query_ingested(query)
        elif path == "/api/overview":
            self._overview()
        elif path == "/" or path.startswith("/static/"):
            self._serve_static(path)
        else:
            self._json(404, {"error": {"code": "NOT_FOUND", "message": f"未知接口: {path}"}})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/exports":
            self._create_export()
        elif path.startswith("/api/tasks/") and path.endswith("/cancel"):
            code, body = ENGINE.request_cancel(int(path.split("/")[3]))
            self._json(code, body)
        elif path.startswith("/api/tasks/") and path.endswith("/retry"):
            req_body = self._body()
            fault = req_body.get("fault") if isinstance(req_body.get("fault"), dict) else None
            code, payload = ENGINE.retry_task(int(path.split("/")[3]), fault_override=fault)
            self._json(code, payload)
        else:
            self._json(404, {"error": {"code": "NOT_FOUND", "message": f"未知接口: {path}"}})

    # ---------- API ----------
    def _create_export(self) -> None:
        body = self._body()
        biz_date = (body.get("biz_date") or "").strip()
        try:
            datetime.strptime(biz_date, "%Y-%m-%d")
        except ValueError:
            self._json(400, {"error": {"code": "INVALID_DATE",
                                        "message": "biz_date 必须为 YYYY-MM-DD 格式"}})
            return
        fault = body.get("fault") or {}
        if not isinstance(fault, dict):
            fault = {}
        key = make_task_key(biz_date)
        existing = db.get_task_by_key(key)
        if existing and existing["status"] not in db.TERMINAL_STATUSES:
            # 同条件重复点击：直接返回当前唯一有效任务，不再建立新任务
            self._json(200, {"deduplicated": True, "task": existing})
            return
        if existing:
            # 终态任务复用同一行做重试（保留全部历史，attempt+1）
            code, payload = ENGINE.retry_task(existing["id"])
            if code != 200:
                self._json(code, payload)
                return
            db.update_task(existing["id"], fault_config=fault)
            db.add_event(existing["id"], "FAULT_CONFIG_UPDATED", fault)
            self._json(201, {"deduplicated": False, "reused": True,
                             "task": db.get_task_by_pk(existing["id"])})
            return
        task = db.create_task(key, biz_date, fault)
        self._json(201, {"deduplicated": False, "reused": False, "task": task})

    def _task_detail(self, task_pk: int) -> None:
        task = db.get_task_by_pk(task_pk)
        if not task:
            self._json(404, {"error": {"code": "TASK_NOT_FOUND", "message": "任务不存在"}})
            return
        self._json(200, {
            "task": task,
            "events": db.list_events(task_pk),
            "request_logs": db.list_request_logs(task_pk),
            "file_records": db.list_file_records(task_pk),
        })

    def _query_ingested(self, query: dict) -> None:
        biz_date = (query.get("biz_date") or [None])[0]
        try:
            limit = min(500, int((query.get("limit") or [100])[0]))
            offset = max(0, int((query.get("offset") or [0])[0]))
        except ValueError:
            self._json(400, {"error": {"code": "INVALID_PARAM", "message": "limit/offset 必须为整数"}})
            return
        rows, total = db.query_ingested(biz_date, limit, offset)
        self._json(200, {"total": total, "limit": limit, "offset": offset, "rows": rows})

    def _overview(self) -> None:
        tasks = db.list_tasks(500)
        counts: dict[str, int] = {}
        for t in tasks:
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        self._json(200, {"tasks_total": len(tasks), "status_counts": counts})

    # ---------- 静态资源 ----------
    def _serve_static(self, path: str) -> None:
        rel = "index.html" if path == "/" else path.removeprefix("/static/")
        target = (STATIC_DIR / rel).resolve()
        if not str(target).startswith(str(STATIC_DIR.resolve())) or not target.is_file():
            self._json(404, {"error": {"code": "NOT_FOUND", "message": "静态资源不存在"}})
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if ctype.startswith("text/") else ""))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    config.ensure_dirs()
    ENGINE.start()
    server = ThreadingHTTPServer(("0.0.0.0", config.HTTP_PORT), HarvesterHandler)
    print(f"[harvester] 采集页面: http://127.0.0.1:{config.HTTP_PORT}/")
    print(f"[harvester] 数据目录: {config.BASE_DIR}  上游: {config.MOCK_BASE_URL}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[harvester] 正在停止...")
    finally:
        ENGINE.stop()
        server.server_close()


if __name__ == "__main__":
    main()
