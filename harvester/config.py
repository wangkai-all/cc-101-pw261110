"""采集服务配置（环境变量可覆盖）。"""
from __future__ import annotations

import os
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


BASE_DIR = Path(os.environ.get("HARVESTER_HOME", Path(__file__).resolve().parent.parent / "data"))
DB_PATH = BASE_DIR / "harvester.db"
DOWNLOAD_DIR = BASE_DIR / "downloads"
QUARANTINE_DIR = BASE_DIR / "quarantine"
INGEST_DIR = BASE_DIR / "ingested"

HTTP_PORT = _env_int("HARVESTER_PORT", 8000)
MOCK_BASE_URL = os.environ.get("MOCK_BASE_URL", "http://127.0.0.1:8080")

HTTP_TIMEOUT = _env_int("HARVESTER_HTTP_TIMEOUT", 15)
WORKER_TICK_SECONDS = float(os.environ.get("HARVESTER_TICK", "0.5"))
# 下载类瞬时错误（中断）的自动重试次数
MAX_DOWNLOAD_ATTEMPTS = _env_int("HARVESTER_MAX_DOWNLOAD_ATTEMPTS", 5)
# 链接过期重新领取次数
MAX_CLAIM_ATTEMPTS = _env_int("HARVESTER_MAX_CLAIM_ATTEMPTS", 5)
# 可重试错误的退避上限（秒）
MAX_BACKOFF_SECONDS = _env_int("HARVESTER_MAX_BACKOFF", 60)


def ensure_dirs() -> None:
    for d in (BASE_DIR, DOWNLOAD_DIR, QUARANTINE_DIR, INGEST_DIR):
        d.mkdir(parents=True, exist_ok=True)
