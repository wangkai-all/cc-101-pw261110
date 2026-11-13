"""采集工作引擎：单线程后台状态机。

阶段流水线：
  WAITING(等待) -> GENERATING(生成) -> DOWNLOADING(下载)
                -> VERIFYING(校验) -> INGESTING(入库) -> INGESTED
失败分支：FAILED（失败文件隔离保留，不进入查询结果）
取消分支：CANCELLED（入库事务一旦开始即不可取消，保证界面与实际入库一致）
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, db
from .mock_client import (
    ApiError,
    DownloadInterrupted,
    MockClient,
    RateLimited,
    TaskGone,
    TokenExpired,
)

# 状态 -> 阶段（UI 流水线直接使用）
STATUS_PHASE = {
    "PENDING": "WAITING",
    "GENERATING": "GENERATING",
    "DOWNLOADING": "DOWNLOADING",
    "VERIFYING": "VERIFYING",
    "INGESTING": "INGESTING",
    "INGESTED": "INGESTED",
    "FAILED": "FAILED",
    "CANCELLED": "CANCELLED",
}
# 入库事务开始后，取消请求不再被接受（取消与完成竞争时以入库为准）
CANCEL_BLOCKED = {"INGESTING", "INGESTED", "FAILED", "CANCELLED"}


def _iso_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0.1, seconds))).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"


class Engine:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.client = MockClient()
        # task_pk -> 下载/领取计数（瞬态，重启后任务回退到 GENERATING 重新计数）
        self._dl_attempts: dict[int, int] = {}
        self._claim_attempts: dict[int, int] = {}
        # task_pk -> 当前持有的下载链接（仅在过期/无效时才重新领取）
        self._claims: dict[int, dict] = {}

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        config.ensure_dirs()
        db.connect()
        recovered = db.recover_on_startup()
        for t in recovered:
            print(f"[engine] 重启恢复：任务#{t['id']} {t['biz_date']} 从 {t['status']} 回退到 GENERATING")
        self._thread = threading.Thread(target=self._loop, name="harvester-worker", daemon=True)
        self._thread.start()
        print("[engine] 后台工作线程已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                for task in db.list_due_tasks():
                    if self._stop.is_set():
                        break
                    try:
                        self._dispatch(task)
                    except Exception as exc:  # 防御：单任务异常不能拖垮工作线程
                        self._fail(task, task.get("status") or "PENDING",
                                   "WORKER_EXCEPTION", f"{type(exc).__name__}: {exc}")
            except Exception as exc:
                print(f"[engine] 调度异常: {type(exc).__name__}: {exc}")
            self._stop.wait(config.WORKER_TICK_SECONDS)

    # ---------------- 公共操作 ----------------
    def request_cancel(self, task_pk: int) -> dict:
        """请求取消。返回 (http_code, body)。"""
        task = db.get_task_by_pk(task_pk)
        if task is None:
            return 404, {"error": {"code": "TASK_NOT_FOUND", "message": "任务不存在"}}
        if task["status"] in CANCEL_BLOCKED:
            return 409, {"error": {
                "code": "NOT_CANCELLABLE",
                "message": f"任务处于 {task['status']}，不可取消（入库开始后取消与完成竞争以入库为准）",
            }}
        db.update_task(task_pk, cancel_requested=1)
        db.add_event(task_pk, "CANCEL_REQUESTED", {"status": task["status"]})
        # 立即唤醒调度，尽快在阶段边界收敛
        db.update_task(task_pk, next_run_at=None)
        return 202, {"task_id": task_pk, "cancel_requested": True, "status": task["status"]}

    def retry_task(self, task_pk: int, fault_override: dict | None = None) -> dict:
        task = db.get_task_by_pk(task_pk)
        if task is None:
            return 404, {"error": {"code": "TASK_NOT_FOUND", "message": "任务不存在"}}
        if task["status"] not in ("FAILED", "CANCELLED"):
            return 409, {"error": {
                "code": "NOT_RETRYABLE",
                "message": f"仅失败或已取消的任务可重试，当前状态 {task['status']}",
            }}
        db.reset_for_retry(task_pk)
        self._dl_attempts.pop(task_pk, None)
        self._claim_attempts.pop(task_pk, None)
        self._claims.pop(task_pk, None)
        if fault_override is not None:
            db.update_task(task_pk, fault_config=fault_override)
            db.add_event(task_pk, "FAULT_CONFIG_UPDATED", fault_override)
        return 200, db.get_task_by_pk(task_pk)

    # ---------------- 调度 ----------------
    def _dispatch(self, task: dict) -> None:
        if self._cancel_if_requested(task):
            return
        handler = {
            "PENDING": self._handle_pending,
            "GENERATING": self._handle_generating,
            "DOWNLOADING": self._handle_downloading,
            "VERIFYING": self._handle_verifying,
            "INGESTING": self._handle_ingesting,
        }.get(task["status"])
        if handler:
            handler(task)

    def _cancel_if_requested(self, task: dict) -> bool:
        """阶段边界的取消检查。返回 True 表示任务已被收敛为 CANCELLED。"""
        if not task.get("cancel_requested") or task["status"] in db.TERMINAL_STATUSES:
            return False
        if task["status"] == "INGESTING":
            # 理论上 request_cancel 已拦截；这里兜底，保证入库不被打断
            return False
        remote_id = task.get("remote_task_id")
        if remote_id:
            try:
                self.client.cancel_remote(task["id"], remote_id)
            except ApiError as exc:
                db.add_event(task["id"], "REMOTE_CANCEL_FAILED", {"message": str(exc)})
        # 清理下载中断遗留的临时文件与缓存链接，避免垃圾堆积
        part = self._dest_path(task).with_suffix(".part")
        if part.exists():
            part.unlink(missing_ok=True)
            db.add_event(task["id"], "PART_FILE_REMOVED", {"path": str(part)})
        self._claims.pop(task["id"], None)
        db.update_task(task["id"], status="CANCELLED", phase="CANCELLED", next_run_at=None)
        db.add_event(task["id"], "TASK_CANCELLED", {"from_status": task["status"]})
        task["status"] = "CANCELLED"
        return True

    def _backoff(self, task_pk: int, seconds: float) -> None:
        db.update_task(task_pk, next_run_at=_iso_after(seconds))

    def _fail(self, task: dict, stage: str, code: str, message: str, detail: dict | None = None) -> None:
        db.update_task(task["id"], status="FAILED", phase="FAILED", fail_stage=stage,
                       fail_reason=f"[{code}] {message}", next_run_at=None)
        db.add_event(task["id"], "TASK_FAILED", {"stage": stage, "code": code,
                                                  "message": message, "detail": detail or {}})

    # ---------------- 阶段 1：等待 -> 创建远端导出 ----------------
    def _handle_pending(self, task: dict) -> None:
        try:
            resp = self.client.create_task(task["id"], task["biz_date"], task.get("fault_config") or {})
        except RateLimited as exc:
            db.add_event(task["id"], "RATE_LIMITED", {"stage": "CREATE", "retry_after": exc.retry_after})
            self._backoff(task["id"], exc.retry_after)
            return
        except ApiError as exc:
            # 创建失败属于瞬时错误，退避后重试，绝不重复堆积任务（本行任务仍是唯一有效任务）
            db.add_event(task["id"], "CREATE_RETRY", {"message": str(exc)})
            self._backoff(task["id"], min(5, config.MAX_BACKOFF_SECONDS))
            return
        remote_id = resp["task_id"]
        db.update_task(task["id"], remote_task_id=remote_id, status="GENERATING",
                       phase="GENERATING", next_run_at=None)
        db.add_event(task["id"], "EXPORT_CREATED", {"remote_task_id": remote_id})

    # ---------------- 阶段 2：轮询生成进度 ----------------
    def _handle_generating(self, task: dict) -> None:
        try:
            resp = self.client.get_status(task["id"], task["remote_task_id"])
        except RateLimited as exc:
            # 严格按接口给出的 Retry-After 等待，期间不创建任何新任务
            db.add_event(task["id"], "RATE_LIMITED",
                         {"stage": "POLL", "retry_after": exc.retry_after})
            self._backoff(task["id"], exc.retry_after)
            return
        except TaskGone as exc:
            self._fail(task, "GENERATING", "REMOTE_TASK_LOST", str(exc))
            return
        except ApiError as exc:
            db.add_event(task["id"], "POLL_RETRY", {"message": str(exc)})
            self._backoff(task["id"], min(5, config.MAX_BACKOFF_SECONDS))
            return

        remote_status = resp.get("status")
        if remote_status in ("PENDING", "PROCESSING"):
            db.add_event(task["id"], "POLL_PROGRESS", {"progress": resp.get("progress", 0),
                                                        "remote_status": remote_status})
            # 正常轮询节奏 1s 一次
            self._backoff(task["id"], 1)
        elif remote_status == "READY":
            db.update_task(task["id"], status="DOWNLOADING", phase="DOWNLOADING", next_run_at=None)
            self._dl_attempts[task["id"]] = 0
            self._claim_attempts[task["id"]] = 0
            db.add_event(task["id"], "EXPORT_READY", {"remote_task_id": task["remote_task_id"]})
        elif remote_status == "FAILED":
            err = resp.get("error") or {}
            self._fail(task, "GENERATING", err.get("code", "REMOTE_FAILED"),
                       err.get("message", "远端导出任务失败"), err)
        elif remote_status == "CANCELLED":
            db.update_task(task["id"], status="CANCELLED", phase="CANCELLED", next_run_at=None)
            db.add_event(task["id"], "TASK_CANCELLED", {"by": "remote"})
        else:
            db.add_event(task["id"], "POLL_RETRY", {"message": f"未知远端状态 {remote_status}"})
            self._backoff(task["id"], 2)

    # ---------------- 阶段 3：领取链接 + 下载 ----------------
    def _dest_path(self, task: dict) -> Path:
        return config.DOWNLOAD_DIR / f"{task['biz_date']}_task{task['id']}.csv"

    def _claim(self, task: dict) -> dict | None:
        try:
            claim = self.client.claim_link(task["id"], task["remote_task_id"])
        except RateLimited as exc:
            db.add_event(task["id"], "RATE_LIMITED", {"stage": "CLAIM", "retry_after": exc.retry_after})
            self._backoff(task["id"], exc.retry_after)
            return None
        except ApiError as exc:
            self._fail(task, "DOWNLOADING", "CLAIM_FAILED", str(exc))
            return None
        db.update_task(task["id"], link_expires_at=claim.get("expires_at"))
        db.add_event(task["id"], "LINK_CLAIMED",
                      {"expires_at": claim.get("expires_at"), "download_url": claim.get("download_url")})
        return claim

    def _handle_downloading(self, task: dict) -> None:
        dest = self._dest_path(task)
        part = dest.with_suffix(".part")
        # 复用当前持有的链接；只有没有链接或链接过期(410)时才重新领取
        claim = self._claims.get(task["id"])
        if claim is None:
            claim_attempts = self._claim_attempts.get(task["id"], 0)
            if claim_attempts >= config.MAX_CLAIM_ATTEMPTS:
                self._fail(task, "DOWNLOADING", "CLAIM_LIMIT_EXCEEDED",
                           f"链接重新领取已达上限 {config.MAX_CLAIM_ATTEMPTS} 次")
                return
            self._claim_attempts[task["id"]] = claim_attempts + 1
            claim = self._claim(task)
            if claim is None:
                return  # 限流退避或已失败，等待下次调度
            self._claims[task["id"]] = claim

        dl_attempts = self._dl_attempts.get(task["id"], 0)
        if dl_attempts >= config.MAX_DOWNLOAD_ATTEMPTS:
            self._fail(task, "DOWNLOADING", "DOWNLOAD_LIMIT_EXCEEDED",
                       f"下载尝试已达上限 {config.MAX_DOWNLOAD_ATTEMPTS} 次")
            return
        self._dl_attempts[task["id"]] = dl_attempts + 1
        try:
            size = self.client.download(task["id"], claim["download_url"], str(part))
        except TokenExpired as exc:
            # 链接过期：丢弃旧链接，重新领取「当前任务」的链接，绝不从头再导出
            self._claims.pop(task["id"], None)
            db.add_event(task["id"], "LINK_EXPIRED", {"message": str(exc)})
            self._backoff(task["id"], 1)
            return
        except DownloadInterrupted as exc:
            # 传输中断：保留仍有效的链接，退避后用同一链接重试
            db.add_event(task["id"], "DOWNLOAD_INTERRUPTED",
                          {"expected": exc.expected, "got": exc.got,
                           "attempt": self._dl_attempts[task["id"]]})
            self._backoff(task["id"], 2)
            return
        except ApiError as exc:
            self._fail(task, "DOWNLOADING", exc.code, str(exc))
            return
        part.replace(dest)
        self._claims.pop(task["id"], None)
        db.add_event(task["id"], "DOWNLOAD_COMPLETED", {"path": str(dest), "size": size})
        db.update_task(task["id"], status="VERIFYING", phase="VERIFYING", next_run_at=None)

    # ---------------- 阶段 4：校验清单 ----------------
    def _handle_verifying(self, task: dict) -> None:
        dest = self._dest_path(task)
        if not dest.exists():
            self._fail(task, "VERIFYING", "FILE_MISSING", f"下载文件不存在: {dest}")
            return
        # 清单来自最近一次 claim 的响应，已随请求日志落库；这里从日志取回
        manifest = self._latest_manifest(task["id"])
        if not manifest:
            self._fail(task, "VERIFYING", "MANIFEST_MISSING", "未找到该任务的下载清单")
            return

        actual_size = dest.stat().st_size
        actual_sha = hashlib.sha256(dest.read_bytes()).hexdigest()
        actual_start, actual_end, row_count, parse_error = self._csv_time_range(dest)

        checks: list[dict] = []

        def check(name: str, ok: bool, expected: object, actual: object) -> None:
            checks.append({"name": name, "ok": bool(ok), "expected": expected, "actual": actual})

        check("文件大小", actual_size == manifest.get("size_bytes"),
              manifest.get("size_bytes"), actual_size)
        check("SHA256 校验值", actual_sha == manifest.get("sha256"),
              manifest.get("sha256"), actual_sha)
        check("数据起始时间", actual_start == manifest.get("data_time_start"),
              manifest.get("data_time_start"), actual_start)
        check("数据结束时间", actual_end == manifest.get("data_time_end"),
              manifest.get("data_time_end"), actual_end)
        check("数据行数", row_count == manifest.get("rows"), manifest.get("rows"), row_count)
        check("业务日期归属", bool(actual_start and actual_start.startswith(task["biz_date"])),
              task["biz_date"], actual_start)
        if parse_error:
            check("CSV 可解析", False, "无解析错误", parse_error)

        all_ok = all(c["ok"] for c in checks)
        record_id = db.add_file_record(
            task["id"],
            local_path=str(dest),
            manifest=json.dumps(manifest, ensure_ascii=False),
            expected_size=manifest.get("size_bytes"),
            actual_size=actual_size,
            expected_sha256=manifest.get("sha256"),
            actual_sha256=actual_sha,
            expected_time_start=manifest.get("data_time_start"),
            expected_time_end=manifest.get("data_time_end"),
            actual_time_start=actual_start,
            actual_time_end=actual_end,
            check_ok=1 if all_ok else 0,
            check_detail=json.dumps(checks, ensure_ascii=False),
            location="downloads",
        )
        if not all_ok:
            # 失败文件单独保留到隔离区，不入库、不出现在查询结果
            qdest = config.QUARANTINE_DIR / dest.name
            shutil.move(str(dest), str(qdest))
            # 同步更新文件记录的位置与路径，保证页面/查询与磁盘实际一致
            conn = db.connect()
            conn.execute("UPDATE file_records SET local_path=?, location='quarantine' WHERE id=?",
                         (str(qdest), record_id))
            conn.commit()
            db.add_event(task["id"], "VERIFICATION_FAILED",
                          {"file_record": record_id, "checks": checks, "quarantine": str(qdest)})
            self._fail(task, "VERIFYING", "VERIFY_FAILED",
                       "清单核对未通过（大小/校验值/数据时间范围不一致）", {"checks": checks})
            return
        db.add_event(task["id"], "VERIFICATION_PASSED", {"file_record": record_id, "checks": checks})
        db.update_task(task["id"], status="INGESTING", phase="INGESTING", next_run_at=None)

    # ---------------- 阶段 5：入库 ----------------
    def _handle_ingesting(self, task: dict) -> None:
        # 入库前最后一个取消边界：标志位仍生效；事务一开始，取消请求将被 API 拒绝
        if self._cancel_if_requested(task):
            return
        dest = self._dest_path(task)
        if not dest.exists():
            self._fail(task, "INGESTING", "FILE_MISSING", f"待入库文件不存在: {dest}")
            return
        try:
            rows = self._read_csv(dest)
        except Exception as exc:
            self._fail(task, "INGESTING", "CSV_PARSE_ERROR", f"{type(exc).__name__}: {exc}")
            return
        idest = config.INGEST_DIR / dest.name
        try:
            # DB 侧用单事务提交：写行 + 文件记录定位 + 终态要么全成要么全回滚，
            # 避免进程在步骤之间崩溃导致「数据已入库但任务未完成」的不一致
            conn = db.connect()
            with conn:
                conn.executemany(
                    """INSERT INTO ingested_rows(task_pk, biz_date, row_no, biz_time, metric,
                                                  value, ingested_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    [(task["id"], task["biz_date"], r["row_no"], r["biz_time"], r["metric"],
                      r["value"], db.now_iso()) for r in rows],
                )
                conn.execute(
                    "UPDATE file_records SET local_path=?, location='ingested' WHERE task_pk=? "
                    "AND id=(SELECT MAX(id) FROM file_records WHERE task_pk=?)",
                    (str(idest), task["id"], task["id"]),
                )
                conn.execute(
                    "UPDATE tasks SET status='INGESTED', phase='INGESTED', next_run_at=NULL, "
                    "updated_at=? WHERE id=?",
                    (db.now_iso(), task["id"]),
                )
            # 事务提交后再移动文件；move 在同盘是原子 rename，且目标已存在时保持幂等
            if dest.exists():
                shutil.move(str(dest), str(idest))
            db.add_event(task["id"], "INGESTED", {"rows": len(rows), "path": str(idest)})
        except Exception as exc:
            self._fail(task, "INGESTING", "INGEST_ERROR", f"{type(exc).__name__}: {exc}")

    # ---------------- 工具 ----------------
    def _latest_manifest(self, task_pk: int) -> dict | None:
        for log in reversed(db.list_request_logs(task_pk)):
            body = log.get("response_body")
            if not body or not isinstance(body, str) or "manifest" not in body:
                continue
            try:
                parsed = json.loads(body)
            except ValueError:
                continue
            if isinstance(parsed, dict) and isinstance(parsed.get("manifest"), dict):
                return parsed["manifest"]
        return None

    @staticmethod
    def _csv_time_range(path: Path) -> tuple[str | None, str | None, int, str | None]:
        times: list[str] = []
        rows = 0
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rows += 1
                    if row.get("biz_time"):
                        times.append(row["biz_time"])
        except Exception as exc:
            return None, None, rows, f"{type(exc).__name__}: {exc}"
        if not times:
            return None, None, 0, "缺少 biz_time 列或无数据行"
        return min(times), max(times), rows, None

    @staticmethod
    def _read_csv(path: Path) -> list[dict]:
        out: list[dict] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                out.append({
                    "row_no": int(row["row_no"]),
                    "biz_time": row["biz_time"],
                    "metric": row["metric"],
                    "value": row["value"],
                })
        return out
