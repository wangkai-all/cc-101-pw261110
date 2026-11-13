#!/usr/bin/env python3
"""本地导出服务（模拟异步导出接口）。

模拟真实「创建任务 -> 轮询进度 -> 领取下载链接 -> 下载文件」的异步导出协议，
并通过环境变量 / 创建任务时的 fault 参数注入各类故障：

  - 慢任务：任务需要若干秒才能生成完成
  - 限流：前 N 次轮询返回 429 + Retry-After
  - 过期链接：下载 token 有短 TTL，过期后返回 410，需重新领取
  - 下载中断：前 N 次下载在传输中途断连（Content-Length 不满足）
  - 内容不完整：实际文件被截断，但清单仍记录完整文件的大小与校验值
  - 远端失败：任务最终置为 FAILED

仅使用标准库，建议以 `python3 -I mock_server.py` 运行。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


# 全局默认故障配置（可被单个任务的 fault 参数覆盖）
DEFAULT_TASK_SECONDS = _env_int("MOCK_TASK_SECONDS", 12)
DEFAULT_LINK_TTL = _env_int("MOCK_LINK_TTL", 15)
DEFAULT_INTERRUPTS = _env_int("MOCK_INTERRUPTS", 1)
DEFAULT_RATE_HITS = _env_int("MOCK_RATE_LIMIT_HITS", 2)
DEFAULT_RETRY_AFTER = _env_int("MOCK_RETRY_AFTER", 3)
DEFAULT_INCOMPLETE = _env_bool("MOCK_INCOMPLETE", False)
DEFAULT_FAIL = _env_bool("MOCK_FAIL", False)
SERVER_PORT = _env_int("MOCK_PORT", 8080)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_dataset(biz_date: str) -> tuple[bytes, dict]:
    """生成某业务日期的模拟数据，返回 (文件字节, 清单)。"""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["row_no", "biz_time", "metric", "value"])
    rows = 24
    for i in range(rows):
        writer.writerow([
            i + 1,
            f"{biz_date}T{i:02d}:00:00Z",
            "orders",
            1000 + i * 7,
        ])
    data = buf.getvalue().encode("utf-8")
    digest = hashlib.sha256(data).hexdigest()
    manifest = {
        "biz_date": biz_date,
        "rows": rows,
        "size_bytes": len(data),
        "sha256": digest,
        "data_time_start": f"{biz_date}T00:00:00Z",
        "data_time_end": f"{biz_date}T23:00:00Z",
        "content_type": "text/csv",
    }
    return data, manifest


class ExportTask:
    def __init__(self, biz_date: str, fault: dict):
        self.task_id = "exp_" + uuid.uuid4().hex[:12]
        self.biz_date = biz_date
        self.status = "PENDING"  # PENDING / PROCESSING / READY / FAILED / CANCELLED
        self.progress = 0
        self.error = None
        self.created_at = utc_now_iso()
        self.ready_at = None

        self.task_seconds = max(1, int(fault.get("task_seconds", DEFAULT_TASK_SECONDS)))
        self.link_ttl = max(1, int(fault.get("link_ttl", DEFAULT_LINK_TTL)))
        self.interrupts = max(0, int(fault.get("interrupts", DEFAULT_INTERRUPTS)))
        self.rate_hits_left = max(0, int(fault.get("rate_limit_hits", DEFAULT_RATE_HITS)))
        self.retry_after = max(1, int(fault.get("retry_after", DEFAULT_RETRY_AFTER)))
        self.incomplete = bool(fault.get("incomplete", DEFAULT_INCOMPLETE))
        self.fail = bool(fault.get("fail", DEFAULT_FAIL))
        # 下载响应前的延迟（秒）：配合短 link_ttl 可稳定制造「下载时链接已过期(410)」
        self.download_delay = max(0, float(fault.get("download_delay", 0)))
        # 延迟仅作用于前 N 次下载（模拟偶发排队），之后恢复正常，便于客户端重领后成功
        self.delay_attempts = max(0, int(fault.get("delay_attempts", 1)))

        self.full_data, self.manifest = _build_dataset(biz_date)
        if self.incomplete:
            # 文件实际被截断，但清单保留完整文件的大小/校验值 -> 客户端校验必失败
            self.served_data = self.full_data[: max(1, len(self.full_data) * 6 // 10)]
        else:
            self.served_data = self.full_data
        self.manifest["task_id"] = self.task_id
        self.manifest["generated_at"] = utc_now_iso()

        # token: {token: expires_at_epoch}
        self.tokens: dict[str, float] = {}
        self.download_attempts = 0
        self.cancel_requested = False
        # RLock：_handle_status 等路径会在持锁时调用 public() 等再次取锁的方法
        self.lock = threading.RLock()

    def public(self) -> dict:
        with self.lock:
            return {
                "task_id": self.task_id,
                "biz_date": self.biz_date,
                "status": self.status,
                "progress": self.progress,
                "error": self.error,
                "created_at": self.created_at,
                "ready_at": self.ready_at,
            }


class TaskStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, ExportTask] = {}

    def add(self, task: ExportTask) -> None:
        with self._lock:
            self._tasks[task.task_id] = task

    def get(self, task_id: str) -> ExportTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def all(self) -> list[ExportTask]:
        with self._lock:
            return list(self._tasks.values())


STORE = TaskStore()


def _run_task(task: ExportTask) -> None:
    """后台线程：模拟慢任务，到点后物化结果。"""
    start = time.monotonic()
    while True:
        time.sleep(0.2)
        with task.lock:
            if task.cancel_requested or task.status == "CANCELLED":
                task.status = "CANCELLED"
                return
            elapsed = time.monotonic() - start
            if task.fail:
                # 失败任务也走一段时间的 PROCESSING，便于观察轮询过程
                task.status = "PROCESSING"
                task.progress = min(90, int(elapsed / task.task_seconds * 100))
                if elapsed >= task.task_seconds:
                    task.status = "FAILED"
                    task.error = {"code": "UPSTREAM_FAILED", "message": "上游数据源返回错误，导出失败"}
                    return
            else:
                task.status = "PROCESSING"
                task.progress = min(99, int(elapsed / task.task_seconds * 100))
                if elapsed >= task.task_seconds:
                    task.status = "READY"
                    task.progress = 100
                    task.ready_at = utc_now_iso()
                    return


class MockHandler(BaseHTTPRequestHandler):
    server_version = "MockExport/1.0"

    # ---------- 基础工具 ----------
    def _json(self, code: int, payload: dict, headers: dict | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, code: int, err_code: str, message: str, **extra) -> None:
        err = {"code": err_code, "message": message}
        err.update(extra)
        headers = {}
        if "retry_after" in extra:
            headers["Retry-After"] = str(extra["retry_after"])
        self._json(code, {"error": err}, headers=headers)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def log_message(self, fmt: str, *args) -> None:  # 静默默认访问日志
        pass

    # ---------- 路由 ----------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path == "/health":
            self._json(200, {"status": "ok", "service": "mock-export", "time": utc_now_iso()})
            return
        if path.startswith("/api/export-tasks/") and path.endswith("/status"):
            self._handle_status(path)
            return
        if path == "/api/download":
            self._handle_download(query)
            return
        self._error(404, "NOT_FOUND", f"未知接口: {path}")

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/export-tasks":
            self._handle_create()
            return
        if path.startswith("/api/export-tasks/") and path.endswith("/claim"):
            self._handle_claim(path)
            return
        if path.startswith("/api/export-tasks/") and path.endswith("/cancel"):
            self._handle_cancel(path)
            return
        self._error(404, "NOT_FOUND", f"未知接口: {path}")

    # ---------- 接口实现 ----------
    def _handle_create(self) -> None:
        body = self._read_body()
        biz_date = (body.get("biz_date") or "").strip()
        if not biz_date:
            self._error(400, "INVALID_PARAM", "biz_date 不能为空")
            return
        fault = body.get("fault") or {}
        if not isinstance(fault, dict):
            fault = {}
        task = ExportTask(biz_date, fault)
        STORE.add(task)
        threading.Thread(target=_run_task, args=(task,), daemon=True).start()
        self._json(201, {"task_id": task.task_id, "status": task.status, "created_at": task.created_at})

    def _handle_status(self, path: str) -> None:
        task_id = path.split("/")[3]
        task = STORE.get(task_id)
        if task is None:
            self._error(404, "TASK_NOT_FOUND", f"任务 {task_id} 不存在（导出服务可能已重启）")
            return
        with task.lock:
            # 前 N 次轮询触发限流，客户端必须按 Retry-After 等待
            if task.rate_hits_left > 0 and task.status in ("PENDING", "PROCESSING"):
                task.rate_hits_left -= 1
                self._error(
                    429,
                    "RATE_LIMITED",
                    "请求过于频繁，请按 Retry-After 等待后再轮询",
                    retry_after=task.retry_after,
                )
                return
            payload = task.public()
        self._json(200, payload)

    def _handle_claim(self, path: str) -> None:
        task_id = path.split("/")[3]
        task = STORE.get(task_id)
        if task is None:
            self._error(404, "TASK_NOT_FOUND", f"任务 {task_id} 不存在")
            return
        with task.lock:
            if task.status == "CANCELLED":
                self._error(409, "TASK_CANCELLED", "任务已取消，无法领取链接")
                return
            if task.status != "READY":
                self._error(409, "TASK_NOT_READY", f"任务当前状态 {task.status}，尚不能领取下载链接")
                return
            # 每次领取都签发新 token；过期后允许重复领取（不能从头再导出）
            token = uuid.uuid4().hex
            expires_at = time.time() + task.link_ttl
            task.tokens[token] = expires_at
            payload = {
                "task_id": task.task_id,
                "download_url": f"/api/download?token={token}",
                "expires_at": datetime.fromtimestamp(expires_at, timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "expires_in": task.link_ttl,
                "manifest": dict(task.manifest),
            }
        self._json(200, payload)

    def _handle_cancel(self, path: str) -> None:
        task_id = path.split("/")[3]
        task = STORE.get(task_id)
        if task is None:
            self._error(404, "TASK_NOT_FOUND", f"任务 {task_id} 不存在")
            return
        with task.lock:
            if task.status in ("READY", "FAILED", "CANCELLED"):
                self._error(409, "TASK_NOT_CANCELLABLE", f"任务状态 {task.status}，无法取消")
                return
            task.cancel_requested = True
            task.status = "CANCELLED"
        self._json(200, {"task_id": task.task_id, "status": "CANCELLED"})

    def _handle_download(self, query: dict) -> None:
        token = (query.get("token") or [""])[0]
        task = None
        for cand in STORE.all():
            with cand.lock:
                if token in cand.tokens:
                    task = cand
                    break
        if task is None:
            self._error(404, "INVALID_TOKEN", "下载链接无效或已被回收")
            return
        with task.lock:
            expires_at = task.tokens.get(token)
            if expires_at is None or time.time() > expires_at:
                # 过期不删 token 记录也可以，这里明确返回 410，客户端应重新领取
                self._error(410, "TOKEN_EXPIRED", "下载链接已过期，请调用 claim 重新领取")
                return
            if task.status == "CANCELLED":
                self._error(409, "TASK_CANCELLED", "任务已取消")
                return
            # 计数必须在延迟判定之前：若延迟后返回 410，本次尝试也要计数，
            # 否则 delay_attempts 永远消耗不掉，会导致客户端每次重领都再次延迟
            task.download_attempts += 1
            attempt = task.download_attempts
            if task.download_delay and attempt <= task.delay_attempts:
                # 在 token 校验通过后、实际传输前等待，用于模拟排队/网络延迟导致的过期
                time.sleep(task.download_delay)
                if time.time() > expires_at:
                    self._error(410, "TOKEN_EXPIRED",
                                "下载排队期间链接已过期，请调用 claim 重新领取")
                    return
            data = task.served_data
            full_size = len(data)
            interrupt = attempt <= task.interrupts

        self.send_response(200)
        self.send_header("Content-Type", task.manifest.get("content_type", "application/octet-stream"))
        self.send_header("Content-Length", str(full_size))
        self.send_header("X-Download-Attempt", str(attempt))
        self.end_headers()
        try:
            if interrupt:
                # 只发送约 40% 后直接断连，模拟下载中断（客户端读到不完整响应）
                cut = max(1, full_size * 4 // 10)
                self.wfile.write(data[:cut])
                self.wfile.flush()
                self.close_connection = True
                return
            # 分块发送，尽量贴近真实下载
            step = 4096
            for i in range(0, full_size, step):
                self.wfile.write(data[i : i + step])
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", SERVER_PORT), MockHandler)
    print(f"[mock-export] 监听 http://0.0.0.0:{SERVER_PORT}")
    print(
        "[mock-export] 默认故障: "
        f"task_seconds={DEFAULT_TASK_SECONDS} link_ttl={DEFAULT_LINK_TTL} "
        f"interrupts={DEFAULT_INTERRUPTS} rate_hits={DEFAULT_RATE_HITS} "
        f"retry_after={DEFAULT_RETRY_AFTER} incomplete={DEFAULT_INCOMPLETE} fail={DEFAULT_FAIL}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[mock-export] 已停止")


if __name__ == "__main__":
    main()
