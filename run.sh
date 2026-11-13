#!/usr/bin/env bash
# 一键启动：本地导出服务(8080) + 采集服务(8000)
# 使用 python3 -I（隔离模式），避免工作目录中的同名模块被意外导入
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p data/logs
echo "[run] 启动本地导出服务 http://127.0.0.1:8080 ..."
python3 -I mock_server.py > data/logs/mock.log 2>&1 &
MOCK_PID=$!
echo "[run] 启动采集服务 http://127.0.0.1:8000 ..."
python3 -I harvester/http_app.py > data/logs/harvester.log 2>&1 &
HARV_PID=$!

echo "$MOCK_PID" > data/logs/mock.pid
echo "$HARV_PID" > data/logs/harvester.pid
echo "[run] PID: mock=$MOCK_PID harvester=$HARV_PID"
echo "[run] 浏览器打开 http://127.0.0.1:8000/"
echo "[run] 停止: ./stop.sh"

trap 'echo; ./stop.sh' INT TERM
wait
