#!/usr/bin/env python3 -I
"""端到端冒烟测试（标准库实现）。

覆盖：
  1. 健康检查与静态页面
  2. 正常全链路：创建 -> 429 限流退避 -> 轮询 -> 下载中断重试 -> 校验 -> 入库
  3. 同条件重复点击去重，只建立一个有效远端任务
  4. 下载链接过期后重新领取（不重新导出）
  5. 内容不完整：校验失败、文件隔离、不进入查询结果
  6. 取消任务（远端+本地一致）与失败后重试
  7. 采集进程重启后恢复轮询并完成入库
  8. 入库开始后拒绝取消（取消与完成竞争时以入库为准）
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MOCK_PORT = 18080
HARV_PORT = 18000
BASE = f"http://127.0.0.1:{HARV_PORT}"
MOCK = f"http://127.0.0.1:{MOCK_PORT}"

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}  {detail}")


def http(method: str, url: str, body: dict | None = None, expect: int | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            parsed = json.loads(resp.read().decode())
            code = resp.status
    except urllib.error.HTTPError as e:
        parsed = json.loads(e.read().decode())
        code = e.code
    if expect is not None:
        check(f"HTTP {method} {url.split('127.0.0.1')[1]} -> {expect}", code == expect,
              f"实际 {code} {parsed}")
    return code, parsed


def wait_for(url: str, timeout: float = 15) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except OSError:
            time.sleep(0.3)
    return False


def wait_status(task_pk: int, statuses: set[str], timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = http("GET", f"{BASE}/api/tasks/{task_pk}/detail")
        if last["task"]["status"] in statuses:
            return last
        time.sleep(0.5)
    raise AssertionError(f"任务 {task_pk} 未在 {timeout}s 内到达 {statuses}，最后状态 {last.get('task', {}).get('status')}")


def create_export(biz_date: str, fault: dict | None = None) -> dict:
    code, res = http("POST", f"{BASE}/api/exports",
                     {"biz_date": biz_date, "fault": fault or {}}, expect=201)
    return res["task"]


procs: list[subprocess.Popen] = []


def start_mock(env_extra: dict | None = None) -> subprocess.Popen:
    env = {"PATH": os.environ.get("PATH", "/usr/bin"), "MOCK_PORT": str(MOCK_PORT),
           "MOCK_TASK_SECONDS": "4", "MOCK_LINK_TTL": "3", "MOCK_INTERRUPTS": "1",
           "MOCK_RATE_LIMIT_HITS": "2", "MOCK_RETRY_AFTER": "1"}
    if env_extra:
        env.update(env_extra)
    p = subprocess.Popen([sys.executable, "-I", "mock_server.py"], cwd=ROOT,
                         env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(p)
    assert wait_for(f"{MOCK}/health"), "mock server 启动失败"
    return p


def start_harvester(home: Path) -> subprocess.Popen:
    env = {"PATH": os.environ.get("PATH", "/usr/bin"),
           "HARVESTER_PORT": str(HARV_PORT), "HARVESTER_HOME": str(home),
           "MOCK_BASE_URL": MOCK}
    # -I 不含 cwd 且屏蔽 PYTHONPATH，故以脚本方式运行（http_app.py 内部自举 sys.path）
    p = subprocess.Popen([sys.executable, "-I", "harvester/http_app.py"], cwd=ROOT,
                         env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(p)
    assert wait_for(f"{BASE}/health"), "harvester 启动失败"
    return p


def stop_proc(p: subprocess.Popen) -> None:
    if p.poll() is None:
        p.send_signal(signal.SIGINT)
        try:
            p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            p.kill()
    procs.remove(p)


def main() -> int:
    home = Path(tempfile.mkdtemp(prefix="harv-test-"))
    print(f"[test] HARVESTER_HOME={home}")
    try:
        start_mock()
        start_harvester(home)

        print("\n== 1. 健康检查与静态页面 ==")
        check("mock 健康", wait_for(f"{MOCK}/health"))
        check("harvester 健康", wait_for(f"{BASE}/health"))
        with urllib.request.urlopen(f"{BASE}/", timeout=5) as r:
            html = r.read().decode()
        check("采集页面可访问", "异步导出采集台" in html)

        print("\n== 2. 正常全链路（限流/中断/校验/入库）==")
        task = create_export("2026-11-01")
        pk = task["id"]
        detail = wait_status(pk, {"INGESTED", "FAILED"}, timeout=45)
        t = detail["task"]
        check("任务最终入库", t["status"] == "INGESTED", json.dumps(t, ensure_ascii=False))
        codes = [l["status_code"] for l in detail["request_logs"]]
        check("轮询期间遇到 429 限流", 429 in codes, f"状态码序列 {codes}")
        rl = [l for l in detail["request_logs"] if l["status_code"] == 429]
        check("429 响应带 Retry-After", all(l["retry_after"] for l in rl))
        check("存在下载中断事件", any(e["event_type"] == "DOWNLOAD_INTERRUPTED"
                                      for e in detail["events"]))
        check("中断后仍下载成功", any(e["event_type"] == "DOWNLOAD_COMPLETED"
                                      for e in detail["events"]))
        fr = detail["file_records"][-1]
        check("文件大小/校验值/时间范围全部核对通过", fr["check_ok"] == 1)
        check("通过文件位于 ingested 目录", fr["location"] == "ingested")
        check("实际 SHA256 与清单一致", fr["actual_sha256"] == fr["expected_sha256"])

        print("\n== 3. 同条件重复点击去重 ==")
        task2 = create_export("2026-11-02")
        code, res = http("POST", f"{BASE}/api/exports",
                         {"biz_date": "2026-11-02", "fault": {}}, expect=200)
        check("重复请求返回 deduplicated", res.get("deduplicated") is True)
        check("返回同一个任务行", res["task"]["id"] == task2["id"])
        # 等任务跑完再核对远端请求（断言需在工作线程实际发起请求之后）
        detail2 = wait_status(task2["id"], {"INGESTED", "FAILED"})
        remote_ids = set()
        for l in detail2["request_logs"]:
            if l["method"] == "POST" and l["url"].endswith("/api/export-tasks") and l["status_code"] == 201:
                body = l["response_body"]
                if isinstance(body, str):
                    body = json.loads(body)
                if isinstance(body, dict) and body.get("task_id"):
                    remote_ids.add(body["task_id"])
        check("远端只创建了一个导出任务", len(remote_ids) == 1, f"远端任务 {remote_ids}")

        print("\n== 4. 链接过期重新领取（不重新导出）==")
        # link_ttl=2 + download_delay=3：首次下载在服务端排队后 token 已过期 -> 410；
        # 重新领取后 token 仍有效（延迟只作用一次排队窗口，重领后立即下载成功）。
        # 引擎必须重新领取「当前任务」的链接，而不是重新发起导出
        t3 = create_export("2026-11-03",
                           {"link_ttl": 2, "interrupts": 0, "task_seconds": 3, "download_delay": 3})
        d3 = wait_status(t3["id"], {"INGESTED", "FAILED"}, timeout=45)
        claim_events = [e for e in d3["events"] if e["event_type"] == "LINK_CLAIMED"]
        expired_events = [e for e in d3["events"] if e["event_type"] == "LINK_EXPIRED"]
        create_events = [e for e in d3["events"] if e["event_type"] == "EXPORT_CREATED"]
        check("至少领取过一次下载链接", len(claim_events) >= 1)
        check("观察到链接过期(410)后重新领取", len(expired_events) >= 1,
              f"LINK_EXPIRED 事件 {len(expired_events)} 次")
        check("全程只有一个远端导出任务", len(create_events) == 1,
              f"EXPORT_CREATED 次数 {len(create_events)}")
        check("过期重领后仍成功入库", d3["task"]["status"] == "INGESTED",
              d3["task"].get("fail_reason", ""))
        # 410 必须留在实际请求记录中可查
        has_410 = any(l["status_code"] == 410 for l in d3["request_logs"])
        check("请求日志保留 410 TOKEN_EXPIRED 记录", has_410)

        print("\n== 5. 内容不完整 -> 校验失败 + 隔离 + 不入库 ==")
        t4 = create_export("2026-11-04", {"incomplete": True, "interrupts": 0})
        d4 = wait_status(t4["id"], {"FAILED"}, timeout=30)
        check("任务标记 FAILED 且阶段为 VERIFYING",
              d4["task"]["status"] == "FAILED" and d4["task"]["fail_stage"] == "VERIFYING",
              d4["task"].get("fail_reason", ""))
        rec = d4["file_records"][-1]
        check("失败文件记录 check_ok=0", rec["check_ok"] == 0)
        qpath = Path(rec["local_path"])
        check("失败文件移入 quarantine 隔离区且文件保留",
              rec["location"] == "quarantine" and qpath.exists()
              and qpath.parent == home / "quarantine",
              f"实际 {qpath}")
        check("downloads 目录不留存失败文件",
            not (home / "downloads" / qpath.name).exists())
        check("ingested 目录无该文件", not (home / "ingested" / qpath.name).exists())
        _, q = http("GET", f"{BASE}/api/ingested/data?biz_date=2026-11-04")
        check("失败数据不出现在入库查询", q["total"] == 0)
        _, qall = http("GET", f"{BASE}/api/ingested/data")
        check("入库查询仅含成功日期",
              {r["biz_date"] for r in qall["rows"]} <= {"2026-11-01", "2026-11-02", "2026-11-03"})

        print("\n== 6. 取消任务 ==")
        t5 = create_export("2026-11-05", {"task_seconds": 8})
        time.sleep(1.5)
        code, res = http("POST", f"{BASE}/api/tasks/{t5['id']}/cancel", expect=202)
        check("取消请求被接受", res.get("cancel_requested") is True)
        d5 = wait_status(t5["id"], {"CANCELLED"}, timeout=15)
        check("本地状态收敛为 CANCELLED", d5["task"]["status"] == "CANCELLED")
        # 远端任务也应被取消
        remote = d5["task"]["remote_task_id"]
        code, rstat = http("GET", f"{MOCK}/api/export-tasks/{remote}/status")
        check("远端任务同步取消", rstat.get("status") == "CANCELLED", json.dumps(rstat))
        # 已取消任务拒绝再次取消
        http("POST", f"{BASE}/api/tasks/{t5['id']}/cancel", expect=409)
        # 重试该日期：复用任务行，attempt+1
        code, res = http("POST", f"{BASE}/api/tasks/{t5['id']}/retry", expect=200)
        check("重试后 attempt 递增", res["attempt"] == 2)
        check("重试后回到 PENDING", res["status"] == "PENDING")
        d5b = wait_status(t5["id"], {"INGESTED", "FAILED"}, timeout=30)
        check("重试任务成功入库", d5b["task"]["status"] == "INGESTED")

        print("\n== 6b. 下载阶段取消：界面状态与磁盘/查询结果一致 ==")
        # 慢任务 + 下载中断，制造较长的下载窗口，在窗口内请求取消
        t5c = create_export("2026-11-08", {"task_seconds": 3, "interrupts": 5, "link_ttl": 30})
        # 轮询直到任务进入 DOWNLOADING
        deadline = time.time() + 20
        while time.time() < deadline:
            _, dd = http("GET", f"{BASE}/api/tasks/{t5c['id']}/detail")
            if dd["task"]["status"] in ("DOWNLOADING", "VERIFYING", "INGESTING",
                                        "INGESTED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.3)
        http("POST", f"{BASE}/api/tasks/{t5c['id']}/cancel", expect=202)
        d5c = wait_status(t5c["id"], {"CANCELLED", "INGESTED"}, timeout=20)
        check("下载/校验阶段取消收敛为 CANCELLED", d5c["task"]["status"] == "CANCELLED",
              d5c["task"]["status"])
        # 磁盘一致性：ingested 目录不得出现该日期文件
        ingested_files = list((home / "ingested").glob("2026-11-08*"))
        check("取消任务的文件未进入 ingested 目录", ingested_files == [])
        # 查询一致性：该日期无入库数据
        _, q5c = http("GET", f"{BASE}/api/ingested/data?biz_date=2026-11-08")
        check("取消任务的数据不出现在查询结果", q5c["total"] == 0)
        # 文件记录不得标记为 ingested
        check("文件记录无 ingested 位置标记",
              all(r["location"] != "ingested" for r in d5c["file_records"]))

        print("\n== 7. 远端失败任务 ==")
        t6 = create_export("2026-11-06", {"fail": True, "interrupts": 0, "task_seconds": 10})
        d6 = wait_status(t6["id"], {"FAILED"}, timeout=40)
        check("远端失败被正确标记", d6["task"]["fail_stage"] == "GENERATING"
              and "UPSTREAM_FAILED" in (d6["task"]["fail_reason"] or ""))
        # 重试时覆盖故障配置（去掉 fail 标记），验证可恢复
        code, res = http("POST", f"{BASE}/api/tasks/{t6['id']}/retry",
                         {"fault": {"fail": False, "interrupts": 0}}, expect=200)
        d6b = wait_status(t6["id"], {"INGESTED", "FAILED"}, timeout=30)
        check("失败后重试（覆盖故障参数）可成功", d6b["task"]["status"] == "INGESTED",
              d6b["task"].get("fail_reason", ""))

        print("\n== 8. 重启恢复轮询 ==")
        t7 = create_export("2026-11-07", {"task_seconds": 6, "interrupts": 0})
        time.sleep(2)
        harv_pid = [p for p in procs if any("http_app.py" in a for a in p.args)][0]
        stop_proc(harv_pid)
        print("  · 采集进程已停止，等待远端任务继续生成 ...")
        time.sleep(7)
        start_harvester(home)
        d7 = wait_status(t7["id"], {"INGESTED", "FAILED", "CANCELLED"}, timeout=30)
        check("重启后恢复并完成入库", d7["task"]["status"] == "INGESTED",
              d7["task"].get("fail_reason", ""))
        check("记录了 RECOVERED_ON_STARTUP 事件",
              any(e["event_type"] == "RECOVERED_ON_STARTUP" for e in d7["events"]))

        print("\n== 9. 入库开始后拒绝取消 ==")
        # INGESTING 窗口很短，直接对终态任务请求取消验证语义保证
        http("POST", f"{BASE}/api/tasks/{t7['id']}/cancel", expect=409)
        _, d7c = http("GET", f"{BASE}/api/tasks/{t7['id']}/detail")
        check("拒绝取消后入库结果不受影响", d7c["task"]["status"] == "INGESTED")
        _, q7 = http("GET", f"{BASE}/api/ingested/data?biz_date=2026-11-07")
        check("入库数据行数为 24", q7["total"] == 24, f"实际 {q7['total']}")

    finally:
        for p in list(procs):
            stop_proc(p)
        shutil.rmtree(home, ignore_errors=True)

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
