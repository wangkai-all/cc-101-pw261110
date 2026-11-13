#!/usr/bin/env bash
# 停止两个服务
set -uo pipefail
cd "$(dirname "$0")"
for name in mock harvester; do
  f="data/logs/$name.pid"
  if [[ -f "$f" ]]; then
    pid="$(cat "$f")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" && echo "[stop] $name ($pid) 已停止"
    fi
    rm -f "$f"
  fi
done
# 兜底：按模块名清理
pkill -f "mock_server.py" 2>/dev/null || true
pkill -f "harvester/http_app.py" 2>/dev/null || true
