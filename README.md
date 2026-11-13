# 异步导出采集系统

模拟真实「**创建任务 → 轮询进度 → 领取下载链接 → 下载 → 清单校验 → 确认入库**」的异步导出采集流程。
纯 Python 标准库实现，零第三方依赖，包含一个可注入故障的本地导出服务和一个带可视化采集页面的采集服务。

## 架构

```
┌────────────────────┐        REST/JSON         ┌─────────────────────────────┐
│  采集页面 (8000)    │  ───────────────────▶   │  采集服务 harvester          │
│  static/ 原生 JS    │ ◀───────────────────    │  ├─ 后台状态机引擎（单线程）  │
└────────────────────┘   任务/请求/文件/数据     │  ├─ SQLite 持久化（WAL）      │
                                                │  └─ downloads/quarantine/    │
                                                │     ingested 三类文件目录     │
                                                └──────────────┬──────────────┘
                                                               │ 异步导出协议
                                                               ▼
                                                ┌─────────────────────────────┐
                                                │  本地导出服务 mock (8080)     │
                                                │  创建/轮询/取消/领取/下载      │
                                                │  可注入：慢任务/限流/过期/     │
                                                │  中断/不完整/远端失败          │
                                                └─────────────────────────────┘
```

## 快速开始

```bash
./run.sh                 # 同时启动 mock(8080) 与 harvester(8000)
# 浏览器打开 http://127.0.0.1:8000/
./stop.sh                # 停止
```

也可分别前台启动（观察日志）：

```bash
python3 -I mock_server.py
HARVESTER_HOME=./data python3 -I harvester/http_app.py
```

运行端到端测试（自动启停两套服务，使用独立临时端口与数据目录）：

```bash
python3 -I tests/smoke_test.py
```

## 五阶段状态机

| 阶段 | 状态 | 含义 |
| --- | --- | --- |
| 等待 | `PENDING/WAITING` | 任务已落库，等待工作线程创建远端导出 |
| 生成 | `GENERATING` | 远端导出中，按节奏轮询；遇 429 按 `Retry-After` 退避 |
| 下载 | `DOWNLOADING` | 领取有时效的下载链接并流式下载，中断可重试 |
| 校验 | `VERIFYING` | 核对清单中的大小、SHA256、数据时间范围、行数、业务日期 |
| 入库 | `INGESTING → INGESTED` | 校验通过才写库并移入 `ingested/`；失败则进 `quarantine/` |

失败分支 `FAILED`、取消分支 `CANCELLED` 为终态；二者都可对该业务日期「重试」（复用任务行、`attempt+1`、保留全部历史）。

## 关键保证（对应需求）

1. **同条件去重**：以业务日期为条件键 `biz:<date>` 建唯一约束。重复点击只返回当前唯一有效任务，
   不会重复建单（测试断言远端 `export-tasks` 只被创建一次）。
2. **限流不新建任务**：轮询/领取收到 429 时，严格按响应头 `Retry-After` 写入 `next_run_at` 退避，
   期间不创建任何新任务。
3. **过期链接重领而非重新导出**：下载遇到 410 `TOKEN_EXPIRED` 时丢弃旧 token、对**当前远端任务**
   重新 `claim`；缓存有效链接，正常重试不消耗领取次数。全程只有一个远端导出任务。
4. **下载中断重试**：响应短于 `Content-Length` 或网络异常记为 `DOWNLOAD_INTERRUPTED`，
   用同一链接重试，超过上限才失败。
5. **先校验后入库**：逐项核对清单（大小 / SHA256 / 数据起止时间 / 行数 / 业务日期归属 / CSV 可解析）。
   任一不符：文件移入 `quarantine/` 隔离保留、任务置 `FAILED`、**不写入查询结果**。
6. **取消与完成竞争一致性**：取消只在阶段边界生效；`INGESTING` 事务开始后拒绝取消（API 返回 409），
   保证界面状态、磁盘文件位置、入库查询三者严格一致。取消会清理 `.part` 临时文件并通知远端取消。
7. **重启恢复**：所有状态落 SQLite。重启时对非终态任务记录 `RECOVERED_ON_STARTUP`，
   下载/校验/入库等瞬态回退到 `GENERATING` 重新轮询，接着完成已有任务，不重复导出。

## 故障注入（创建表单可配 / 环境变量设默认值）

| 参数 | 环境变量 | 作用 |
| --- | --- | --- |
| `task_seconds` | `MOCK_TASK_SECONDS` | 慢任务生成耗时 |
| `link_ttl` | `MOCK_LINK_TTL` | 下载链接有效期（秒），过期返回 410 |
| `interrupts` | `MOCK_INTERRUPTS` | 前 N 次下载传输中途断连 |
| `rate_limit_hits` | `MOCK_RATE_LIMIT_HITS` | 前 N 次轮询返回 429 |
| `retry_after` | `MOCK_RETRY_AFTER` | 429 的 `Retry-After` 秒数 |
| `download_delay` / `delay_attempts` | — | 下载排队延迟，配合短 TTL 稳定制造过期 |
| `incomplete` | `MOCK_INCOMPLETE` | 文件截断但清单仍为完整值 → 校验失败隔离 |
| `fail` | `MOCK_FAIL` | 远端任务最终 `FAILED` |

## 数据目录（`HARVESTER_HOME`，默认 `./data`）

```
harvester.db            # 任务/事件/请求日志/文件记录/入库数据（WAL 模式）
downloads/              # 下载中的文件（.part 临时文件与待校验文件）
quarantine/             # 校验失败、单独保留的文件，不入库
ingested/               # 校验通过并完成入库的文件
```

## 主要 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/exports` | 按业务日期创建导出（同条件自动去重/终态复用重试），body 可带 `fault` |
| GET | `/api/tasks` | 任务列表 |
| GET | `/api/tasks/{id}/detail` | 任务详情 + 事件时间线 + 实际请求日志 + 文件记录 |
| POST | `/api/tasks/{id}/cancel` | 请求取消（入库开始后 409） |
| POST | `/api/tasks/{id}/retry` | 失败/取消后重试，body 可选 `fault` 覆盖故障参数 |
| GET | `/api/ingested/data?biz_date=&limit=&offset=` | 入库数据查询（仅含校验通过文件） |
| GET | `/api/overview` | 任务状态统计 |

Mock 侧协议：`POST /api/export-tasks`、`GET /api/export-tasks/{id}/status`、
`POST /api/export-tasks/{id}/claim`、`GET /api/download?token=`、`POST /api/export-tasks/{id}/cancel`。

## 测试覆盖（tests/smoke_test.py，57 项断言）

健康检查与页面、全链路（限流/中断/校验/入库）、去重只建一个远端任务、410 过期重领不重新导出、
内容不完整隔离且不入查询、取消（远端同步 + 状态/磁盘/查询一致 + `.part` 清理）、
远端失败与重试（可覆盖故障参数）、重启恢复轮询、入库开始后拒绝取消。
