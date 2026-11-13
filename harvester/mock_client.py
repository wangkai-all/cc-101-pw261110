"""本地导出服务 HTTP 客户端。

封装异步导出协议：创建 / 轮询 / 取消 / 领取链接 / 下载。
每次实际请求都写入 request_logs，页面上可以逐条查看。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from . import config, db


class ApiError(Exception):
    def __init__(self, code: str, message: str, http_status: int | None, retry_after: float | None = None,
                 body: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.retry_after = retry_after
        self.body = body


class RateLimited(ApiError):
    pass


class TokenExpired(ApiError):
    pass


class TaskGone(ApiError):
    pass


class DownloadInterrupted(Exception):
    """下载在传输中途断连 / 响应体短于 Content-Length。"""

    def __init__(self, message: str, expected: int, got: int):
        super().__init__(message)
        self.expected = expected
        self.got = got


class MockClient:
    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or config.MOCK_BASE_URL).rstrip("/")

    # ---------- 底层请求 ----------
    def _request(self, task_pk: int | None, method: str, path: str, body: dict | None = None,
                 extra_headers: dict | None = None) -> tuple[int, dict, Any, dict]:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Accept": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        status: int | None = None
        resp_headers: dict = {}
        parsed: Any = None
        err_text: str | None = None
        retry_after: float | None = None
        try:
            with urllib.request.urlopen(req, timeout=config.HTTP_TIMEOUT) as resp:
                status = resp.status
                raw = resp.read()
                resp_headers = dict(resp.headers.items())
                try:
                    parsed = json.loads(raw.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    parsed = raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            status = e.code
            resp_headers = dict(e.headers.items()) if e.headers else {}
            raw = e.read()
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                parsed = raw.decode("utf-8", errors="replace")
            if "Retry-After" in resp_headers:
                try:
                    retry_after = float(resp_headers["Retry-After"])
                except (TypeError, ValueError):
                    retry_after = None
            err_text = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            err_text = f"{type(e).__name__}: {e}"
        finally:
            db.add_request_log(
                task_pk,
                method=method,
                url=url,
                request_headers=headers,
                request_body=body,
                status_code=status,
                response_headers=resp_headers,
                response_body=parsed,
                error=err_text,
                retry_after=retry_after,
            )
        return status or 0, resp_headers, parsed, {"retry_after": retry_after}

    def _raise_for_status(self, status: int, parsed: Any, meta: dict) -> dict:
        if 200 <= status < 300 and isinstance(parsed, dict):
            return parsed
        err = parsed.get("error", {}) if isinstance(parsed, dict) else {}
        code = err.get("code", "UPSTREAM_ERROR")
        message = err.get("message", f"HTTP {status}")
        retry_after = meta.get("retry_after")
        if status == 429:
            raise RateLimited(code, message, status, retry_after or 5.0, parsed)
        if status == 410:
            raise TokenExpired(code, message, status, retry_after, parsed)
        if status == 404:
            raise TaskGone(code, message, status, retry_after, parsed)
        raise ApiError(code, message, status, retry_after, parsed)

    # ---------- 协议接口 ----------
    def create_task(self, task_pk: int, biz_date: str, fault: dict) -> dict:
        status, _, parsed, meta = self._request(
            task_pk, "POST", "/api/export-tasks", {"biz_date": biz_date, "fault": fault}
        )
        return self._raise_for_status(status, parsed, meta)

    def get_status(self, task_pk: int, remote_task_id: str) -> dict:
        status, _, parsed, meta = self._request(
            task_pk, "GET", f"/api/export-tasks/{remote_task_id}/status"
        )
        return self._raise_for_status(status, parsed, meta)

    def cancel_remote(self, task_pk: int, remote_task_id: str) -> dict:
        status, _, parsed, meta = self._request(
            task_pk, "POST", f"/api/export-tasks/{remote_task_id}/cancel"
        )
        # 远端已结束导致的 409 不致命：本地按当前真实状态收敛
        if status == 409:
            return {"status": "CONFLICT", "body": parsed}
        return self._raise_for_status(status, parsed, meta)

    def claim_link(self, task_pk: int, remote_task_id: str) -> dict:
        status, _, parsed, meta = self._request(
            task_pk, "POST", f"/api/export-tasks/{remote_task_id}/claim"
        )
        return self._raise_for_status(status, parsed, meta)

    def download(self, task_pk: int, url: str, dest_path: str) -> int:
        """流式下载到 dest_path；中断抛 DownloadInterrupted（可能已写入部分字节）。"""
        full_url = url if url.startswith("http") else f"{self.base_url}{url}"
        req = urllib.request.Request(full_url, headers={"Accept": "*/*"}, method="GET")
        status: int | None = None
        resp_headers: dict = {}
        err_text: str | None = None
        expected_len = -1
        try:
            with urllib.request.urlopen(req, timeout=config.HTTP_TIMEOUT) as resp:
                status = resp.status
                resp_headers = dict(resp.headers.items())
                expected_len = int(resp.headers.get("Content-Length") or -1)
                got = 0
                with open(dest_path, "wb") as fh:
                    while True:
                        chunk = resp.read(4096)
                        if not chunk:
                            break
                        fh.write(chunk)
                        got += len(chunk)
                if expected_len >= 0 and got != expected_len:
                    raise DownloadInterrupted(
                        f"响应提前结束：期望 {expected_len} 字节，实际 {got} 字节", expected_len, got
                    )
                return got
        except urllib.error.HTTPError as e:
            status = e.code
            resp_headers = dict(e.headers.items()) if e.headers else {}
            raw = e.read()
            try:
                parsed: Any = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                parsed = raw.decode("utf-8", errors="replace")
            retry_after = None
            if e.headers and "Retry-After" in e.headers:
                try:
                    retry_after = float(e.headers["Retry-After"])
                except (TypeError, ValueError):
                    retry_after = None
            db.add_request_log(task_pk, method="GET", url=full_url, request_headers={"Accept": "*/*"},
                               request_body=None, status_code=status, response_headers=resp_headers,
                               response_body=parsed, error=f"HTTP {e.code}", retry_after=retry_after)
            if e.code == 410:
                err = parsed.get("error", {}) if isinstance(parsed, dict) else {}
                raise TokenExpired(err.get("code", "TOKEN_EXPIRED"),
                                   err.get("message", "下载链接已过期"), e.code, retry_after, parsed)
            err = parsed.get("error", {}) if isinstance(parsed, dict) else {}
            raise ApiError(err.get("code", "DOWNLOAD_FAILED"),
                           err.get("message", f"HTTP {e.code}"), e.code, retry_after, parsed)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            err_text = f"{type(e).__name__}: {e}"
            raise DownloadInterrupted(f"网络中断：{e}", expected_len, -1)
        finally:
            # 成功路径与部分中断路径都留痕；HTTPError 路径已单独记录
            if err_text is not None or (status is not None and 200 <= status < 300):
                db.add_request_log(
                    task_pk, method="GET", url=full_url, request_headers={"Accept": "*/*"},
                    request_body=None, status_code=status, response_headers=resp_headers,
                    response_body=f"<binary saved to {dest_path}>" if status and status < 300 else None,
                    error=err_text, retry_after=None,
                )
