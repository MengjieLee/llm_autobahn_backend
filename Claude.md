# LLM Autobahn Backend — 项目手册

> 目标：开新会话时直接加载此文件，避免重复探索代码库，节省 token。

## 一句话定位

**LLM 推理日志 OLAP 分析平台**——从线上 ES 日志中提取 KV Cache 请求，经 tokenize → C++ LRU 模拟 → 分钟级命中率趋势计算，为 Dashboard 提供多模型、多场景的 KV Cache 命中率数据。同时附带数据集管理、SQL 查询代理、流程调度等辅助功能。

---

## 技术栈速查

| 层 | 技术 | 版本 |
|----|------|------|
| Web 框架 | FastAPI + Uvicorn | 0.128 / 0.40 |
| Python | 3.12 | (Dockerfile) |
| 数据校验 | Pydantic + pydantic-settings | 2.12 |
| 搜索引擎 | Elasticsearch | 7.10 |
| OLAP DB | Apache Doris (SQLAlchemy + asyncmy) | - |
| Tokenize | HuggingFace Transformers ≥5.2 + tiktoken 0.12 | - |
| Cache 模拟 | C++ 自编 `cache_calc` (LRU) | g++ -O2 -std=c++11 |
| 容器编排 | Kubernetes (python-client ≥31) | - |
| 对象存储 | S3/BOS (smart_open) | - |
| 前端 | Vue 3.5 + Vite 6 + Element Plus 2.13 + ECharts 6 | - |
| 认证 | JWT（百度零信任网关）+ 文件存储 token | - |

---

## 目录结构

```
llm_autobahn_backend/
├── app/                           # FastAPI 应用
│   ├── main.py                    # 入口：create_app(), lifespan, 端口 8739
│   ├── api/
│   │   ├── router.py              # 路由聚合（6 个子路由）
│   │   └── v1/
│   │       ├── account.py         # 登录（零信任网关 JWT）
│   │       ├── dashboard.py       # 仪表盘指标
│   │       ├── datasets.py        # 数据集 CRUD
│   │       ├── olap.py            # ★ 核心模块 (~2400行)：KV Cache pipeline API + 实时 + 日报
│   │       ├── process_scheduler.py # 流程调度代理
│   │       └── sql.py             # Doris SQL 查询代理
│   ├── conf/
│   │   ├── config.py              # Pydantic Settings（.env 驱动）
│   │   ├── logging_config.py      # 日志配置（app/ES/usage 三路）
│   │   ├── olap_config.json       # ★ 热加载：pipeline 参数 + K8s 资源 + IM bot
│   │   ├── realtime_config.json   # ★ 热加载：实时 Worker 参数
│   │   ├── olap_deployment.yml    # K8s Job 模板（${VAR} 替换）
│   │   ├── realtime_deployment.yml# K8s Deployment 模板
│   │   ├── inner_cluster.kubeconfig # K8s 凭证（gitignored）
│   │   └── kube.config            # K8s 凭证（gitignored）
│   └── core/
│       ├── api_schema.py          # StandardResponse / ErrorResponse
│       ├── exceptions.py          # BizException + 统一异常处理
│       ├── middleware.py          # Request-ID + Bearer token 认证
│       ├── request_context.py     # contextvars：username / trace_id
│       └── k8s_client.py         # K8s Job CRUD
│
├── src/                           # 领域逻辑层
│   ├── domains/
│   │   ├── datasets/              # 数据集（代理外部元数据服务）
│   │   │   ├── impl.py            # DatasetsClient (httpx)
│   │   │   └── svc.py             # DatasetsService
│   │   ├── kv/                    # ★ KV Cache 命中率分析
│   │   │   ├── impl.py            # ESIndexClient：ES scroll 底层
│   │   │   ├── svc.py             # ESIndexService：并行窗口 + 自适应重试
│   │   │   └── cache_hit_rate/    # C++ 模拟引擎
│   │   │       ├── cache_calc.cpp # LRU 模拟主程序 (~19KB)
│   │   │       ├── lru.h          # LRU 数据结构 + checkpoint 序列化
│   │   │       ├── hash_util.h    # FNV-1a hash
│   │   │       ├── Makefile       # g++ -O2 编译
│   │   │       └── tokenizer_*.json # 各模型 tokenizer 词表
│   │   └── process_scheduler/     # 流程调度（代理外部服务）
│   │       ├── impl.py
│   │       └── svc.py
│   └── serializers/
│       └── data_serializer.py     # 数据集预览序列化
│
├── context/                       # 基础设施层
│   ├── auth_client.py             # 文件认证（credentials.txt）
│   ├── doris_connector.py         # Doris 连接器（SQLAlchemy）
│   └── file_system/               # S3/BOS 文件系统抽象
│
├── scripts/                       # 独立脚本（K8s Job / CLI / cron）
│   ├── run_pipeline.py            # ★ 主 Pipeline 入口（K8s Job）~62KB
│   ├── kv_pipeline.py             # TokenizeDaemonClient（管理 N 个 daemon）
│   ├── tokenize_daemon.py         # 常驻 tokenize daemon（stdin/stdout JSON 协议）
│   ├── tokenize_script.py         # tokenize 核心逻辑（HF tokenizer）
│   ├── compute_trend.py           # 分钟级 hit_rate 趋势计算
│   ├── cache_simulation.py        # cache_calc 调用封装
│   ├── cache_pipeline.py          # cache 模拟 pipeline
│   ├── realtime_worker.py         # ★ 实时 Worker（K8s Deployment 常驻）
│   ├── realtime_scheduler.py      # ★ 实时调度器（cron 每分钟）
│   ├── deploy_realtime.py         # 部署实时 Worker 到 K8s
│   ├── daily_report.py            # 日报生成 + IM 推送
│   ├── daily_cache_plan.py        # 每日定时任务提交
│   ├── cleanup_deleted.py         # 清理已删除任务数据
│   └── migrate_daily_reports.py   # 日报数据迁移
│
├── olap_database/                 # 运行时数据（gitignored）
│   ├── data/{user}/{task_id}/     # Pipeline 任务数据
│   ├── status/{user}/*.json       # 任务状态文件
│   ├── daily_reports/             # 日报聚合数据
│   └── realtime/                  # 实时数据
│       ├── queue/                 # CFS 文件队列（pending/running/done/failed）
│       ├── _cache_state/          # per-model checkpoint + merged.txt
│       ├── 全场景_各模型/*.json    # 每日趋势数据
│       └── worker_heartbeat       # Worker 心跳文件
│
├── tests/                         # 测试（gitignored，本地存在）
├── logs/, es_logs/                # 运行日志（gitignored）
├── docs/                          # 设计文档
├── Dockerfile                     # 多阶段构建（builder 编译 C++ + 安装依赖 → runtime）
├── requirements.txt               # Python 依赖
├── run.sh                         # 快速启动脚本
├── build_image.sh                 # Docker 镜像构建
├── OLAP_Dashboard.md              # ★ OLAP Dashboard 详细设计文档（含实时流程+踩坑）
└── .env                           # 环境变量（Doris/ES/S3/HF 凭证）
```

---

## API 端点清单

所有路由前缀 `/api/v1`。

### 系统
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |

### Account (`/account`)
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/account/login` | 零信任网关 JWT 登录 |

### Dashboard (`/dashboard`)
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/dashboard/datasets/metrics` | 数据集统计 |
| GET | `/dashboard/users/metrics` | 用户数 |
| GET | `/dashboard/usage/metrics` | 使用量指标（时间段过滤） |
| GET | `/dashboard/usage/users` | 用户活跃详情 |

### Datasets (`/datasets`)
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/datasets/list` | 数据集列表（标签/阶段/关键词过滤） |
| POST | `/datasets/name-map` | 名称→Iceberg表映射 |
| GET | `/datasets/detail` | 数据集详情 |
| POST | `/datasets/preview` | 数据集预览 |

### SQL (`/sql`)
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/sql/sql_query` | Doris SQL 查询（强制 LIMIT 1000） |

### Process Scheduler (`/process_scheduler`)
| 方法 | 路径 | 说明 |
|------|------|------|
| GET/POST | `/process_scheduler/jobs` | 列表/启动任务 |
| POST | `/process_scheduler/jobs/{id}/stop` | 停止任务 |
| DELETE | `/process_scheduler/jobs/{id}` | 删除任务 |
| GET/POST | `/process_scheduler/pipelines` | 列表/创建 Pipeline |
| GET/DELETE | `/process_scheduler/pipelines/{id}` | 详情/删除 Pipeline |

### OLAP (`/olap`) — 核心模块
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/olap/kv/tasks` | 任务列表 |
| GET | `/olap/kv/fetch` | ★ 提交 Pipeline 任务（异步 K8s Job） |
| GET | `/olap/kv/status/{task_id}` | 任务状态（含 K8s OOM 检测） |
| DELETE | `/olap/kv/tasks/{task_id}` | 软删除任务 |
| GET | `/olap/kv/hit-rate-trend/{task_id}` | 任务级分钟趋势 |
| GET | `/olap/kv/dashboard` | ★ Dashboard 8 场景趋势（1h/6h/1d/7d/30d） |
| GET | `/olap/kv/realtime` | ★ 实时全场景命中率 |
| GET | `/olap/kv/realtime/status` | Worker 心跳/队列状态 |
| GET | `/olap/kv/models` | 可用模型列表（热加载） |
| GET | `/olap/kv/qpd` | QPD 配额查询 |
| GET | `/olap/kv/file-tree` | 任务数据目录树 |

---

## 核心业务流程

### Pipeline（4 阶段）

```
用户提交 → GET /kv/fetch
  ↓
K8s Job Pod 内运行 scripts/run_pipeline.py
  ↓
Stage 1: Fetch
  ESIndexService.query_to_dir()
  时间窗口切分 → 并行 ES scroll → JSONL 文件
  ↓
Stage 2: Tokenize（与 Fetch 流式重叠）
  TokenizeDaemonClient → N 个 tokenize_daemon.py
  HF tokenizer apply_chat_template → per-model _input_ids.txt
  ↓
Stage 3: Simulate
  cache_calc -L <file_list> -c <checkpoint>
  C++ LRU 模拟 → per-section hit_rate
  ↓
Stage 4: Trend
  聚合为 report/hit_rate_trend.json
  ↓
通知 → IM bot + 状态文件更新
```

### 实时 Pipeline

```
cron 每分钟 → realtime_scheduler.py
  计算 T = now - 5min → 写 queue/pending/*.json
  ↓
realtime_worker.py（K8s Deployment 常驻）
  扫描 queue → 文件锁
  → Stage 1: fetch（ES scroll）
  → Stage 1.5: _prefilter_and_split（regex 过滤目标模型 + 切 N 片）
  → Stage 2: N 个切片并行 tokenize（N daemon × 20 workers）
  → Stage 3: cache_calc(checkpoint+stdin)
  → 写每日 JSON
  ↓
GET /kv/realtime 读取 → 前端渲染
```

### Dashboard 数据流

```
前端 dashboard.vue
  ├─ GET /kv/realtime/status → Worker 存活？
  │   ├─ YES → GET /kv/realtime（实时数据）
  │   └─ NO  → GET /kv/dashboard（日报 fallback）
  └─ 其他 7 场景 → GET /kv/dashboard
       → 聚合 hit_rate_trend.json → hr×100 → 百分比
```

---

## 关键组件详解

### cache_calc (C++)

- **源码**: `src/domains/kv/cache_hit_rate/cache_calc.cpp`
- **编译**: `make -C src/domains/kv/cache_hit_rate/`
- **核心算法**: token 序列 → 按 block_size 切块 → FNV-1a 哈希（支持前缀链式哈希） → LRU 查询
- **关键参数**:
  - `-f <file>` 输入文件（`-` 为 stdin）
  - `-L <list>` 文件列表（自动按文件插入 `__SECTION__`）
  - `-s <size>` LRU 缓存 block 数量（实时配置 200000000）
  - `-b <size>` block 大小（实时配置 16）
  - `-p true` 前缀哈希
  - `-c <path>` checkpoint 持久化（二进制格式，支持增量计算）
- **输出**: `section: <name>\tsection_hit_rate: 0.xxxx`
- **Checkpoint 格式**: `[magic:4B][version:4B][capacity:8B][total_adds:8B][hit_count:8B][num_keys:8B][keys...]`

### TokenizeDaemonClient

- **定义**: `scripts/kv_pipeline.py`
- **架构**: 管理 N 个 `tokenize_daemon.py` 子进程，Round-Robin 分发
- **协议**: JSON over stdin/stdout
  - parent→daemon: `{"type": "task", "id": ..., "input_file": ..., "model_filter": [...]}`
  - daemon→parent: `{"type": "result", "id": ..., "status": "completed", "models": {...}}`
- **daemon 内部**: `multiprocessing.Pool(workers)` × `apply_chat_template`
- **注意**: 1 个 jsonl 文件只会分配给 1 个 daemon，其余 daemon 闲置

### ESIndexService

- **定义**: `src/domains/kv/svc.py`
- **ES 索引格式**: `as-qianfan-online_YYYY-MM-DD`
- **特性**: 时间窗口自动切分、并行 scroll、自适应重试（2GB buffer 溢出 → 缩短窗口；scroll 上下文溢出 → 降低并发）、跨天索引、流式写入 JSONL
- **两种输出模式**: `query_to_file`（合并单文件）/ `query_to_dir`（每窗口独立文件）

### 数据转换链路

```
cache_calc section_hit_rate (0~1)
  ├→ 实时 Worker → daily JSON (0~1) → /kv/realtime API → hr ≤ 1 ? hr×100 : hr → 百分比
  └→ 日报 Pipeline → hit_rate_trend.json (0~1) → /kv/dashboard API → hr × 100 → 百分比
前端统一接收 0~100 百分比，直接拼接 % 展示。
```

---

## 配置体系

### 三层配置

| 层 | 文件 | 修改后 |
|----|------|--------|
| 静态配置 | `app/conf/config.py` + `.env` | 需重启 |
| 热加载 | `olap_config.json` / `realtime_config.json` | 下次请求/任务生效 |
| K8s 模板 | `olap_deployment.yml` / `realtime_deployment.yml` | 下次部署生效 |

### 关键环境变量 (.env)

| 变量 | 用途 |
|------|------|
| `ES_HOST` / `ES_USER` / `ES_PASSWORD` | Elasticsearch 连接 |
| `DEFAULT_DORIS_*` (HOST/PORT/USER/PASSWORD/CATALOG/DATABASE) | Apache Doris 连接 |
| `access_key` / `secret_key` / `endpoint` / `region` | S3/BOS 对象存储 |
| `HF_TOKEN` | HuggingFace tokenizer 下载 |
| `OLAP_BASE_DIR` | OLAP 数据根目录 |

### realtime_config.json 关键参数

| 参数 | 当前值 | 说明 |
|------|--------|------|
| `models` | glm-5,glm-5.1,kimi-k2.5,minimax-m2.5,deepseek-v3.2 | 实时监控模型 |
| `delay_minutes` | 5 | ES 数据延迟 |
| `pipeline_tokenize_workers` | 3 | 每 daemon Pool 进程数 |
| `pipeline_tokenize_concurrency` | 10 | daemon 总数 |
| `pipeline_tokenize_batch_size` | 10000 | 每批记录数 |
| `pipeline_fetch_concurrency` | 2 | 最大并行任务数 |
| `pipeline_fetch_window_concurrency` | 12 | 并行 ES 窗口数 |
| `pipeline_es_scroll_size` | 10000 | ES scroll 页大小 |
| `pipeline_cache_size` | 200000000 | LRU 缓存 block 数 |
| `pipeline_block_size` | 16 | 每 block token 数 |
| `checkpoint_tmpfs_dir` | /mnt/checkpoint_tmpfs | tmpfs 路径（内存盘，16Gi） |
| `k8s_cpu_request/limit` | 28 | CPU 核数 |
| `k8s_memory_request/limit` | 110Gi | 内存 |
| `k8s_image` | ccr-...baidu.../llm_autobahn_backend:0.3.6 | 镜像 |

### olap_config.json 要点

- `pipeline_default_model`: 默认 tokenizer 模型
- `pipeline_models`: 可选模型列表
- `qpd_limit`: 每日查询配额
- `k8s_*`: Job 资源配置
- `im_bot_*`: IM 通知 webhook
- `daily_report_*`: 日报配置

---

## 进程间通信模式

| 模式 | 场景 | 机制 |
|------|------|------|
| CFS 文件队列 | scheduler → worker | pending/running/done/failed + fcntl 文件锁 |
| JSON stdin/stdout | worker → tokenize daemon | submit/wait + Round-Robin |
| stdin 管道 | worker → cache_calc | `subprocess.run(input=section_data)` |
| 状态 JSON 文件 | pipeline → API | 原子写入(.tmp → os.replace) |
| 心跳文件 | worker → API | 后台线程每 15s 写时间戳，>120s 视为失活 |
| checkpoint 二进制 | cache_calc ↔ 文件 | tmpfs 主路径 + CFS 异步备份 |

---

## 存储结构

```
olap_database/
  data/{user}/{task_id}/           # Pipeline 任务
    {HH}/kv_*.jsonl                # Stage 1: ES 拉取的原始日志
    tokenized/{slice}/*_input_ids.txt  # Stage 2: tokenize 输出
    report/
      cache_report.json            # Stage 3: 聚合报告
      hit_rate_trend.json          # Stage 4: 分钟级趋势
  status/{user}/{task_id}.json     # 任务状态（原子写入）
  daily_reports/{MM-DD}.json       # 日报数据
  realtime/
    queue/{pending,running,done,failed}/*.json  # 任务队列
    _cache_state/{model}/
      cache_checkpoint.bin         # LRU 状态 checkpoint（CFS 备份）
      merged.txt                   # Fallback 全量数据
    全场景_各模型/YYYY-MM-DD.json   # 实时趋势数据
    worker_heartbeat               # Worker 心跳
```

### 每日 JSON 格式（实时）

```json
{
  "date": "2026-04-10",
  "data": {
    "14:30": {
      "glm-5": 0.8485,
      "kimi-k2.5": 0.9012,
      "整体": 0.8749
    }
  },
  "updated_at": "2026-04-10 15:31:00"
}
```

### hit_rate_trend.json 格式（日报）

```json
{
  "series": [
    {"model": "整体", "data": [{"time": "04-08 10:00", "hit_rate": 0.8485}], "stats": {"mean": 0.84, "max": 0.92, "min": 0.78}},
    {"model": "glm-5", "data": [...], "stats": {...}}
  ]
}
```

---

## 部署与运维

### K8s 集群访问

```bash
# 设置 KUBECONFIG 环境变量后，kubectl 命令可省略 --kubeconfig 参数
export KUBECONFIG=/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/app/conf/inner_cluster.kubeconfig
```

设置后所有 kubectl 命令可简写：

```bash
kubectl -n cache-hit-rate-tokenizer get pods
kubectl -n cache-hit-rate-tokenizer logs -f deployment/realtime-worker
```

### 启动命令

```bash
# API 服务
uvicorn app.main:app --host 0.0.0.0 --port 8739
# 或
./run.sh

# 实时 Worker 部署
python scripts/deploy_realtime.py           # 首次部署
python scripts/deploy_realtime.py --dry-run # 预览 YAML
python scripts/deploy_realtime.py --delete  # 删除

# 重启 Worker（设置 KUBECONFIG 后可省略 --kubeconfig）
kubectl -n cache-hit-rate-tokenizer rollout restart deployment/realtime-worker

# 查看日志
kubectl -n cache-hit-rate-tokenizer logs -f deployment/realtime-worker --tail=100

# 实时调度器（crontab）
* * * * * python3 scripts/realtime_scheduler.py >> logs/realtime/realtime_scheduler.log 2>&1

# Docker 构建
./build_image.sh 0.3.6

# C++ 编译
make -C src/domains/kv/cache_hit_rate/
```

### K8s 资源

| 组件 | 类型 | Namespace | CPU | 内存 |
|------|------|-----------|-----|------|
| Pipeline Job | batch/v1 Job | cache-hit-rate-tokenizer | 28 | 110Gi |
| Realtime Worker | apps/v1 Deployment | cache-hit-rate-tokenizer | 28 | 110Gi |
| API Server | 宿主机 / Docker | - | - | - |

---

## 中间件与认证

- **CORS**: 允许 `https://vortex.n.baidu-int.com`
- **Request-ID**: `X-Trace-Id` 注入 + `X-Response-Time-ms`
- **认证**: `Authorization: Bearer <token>` → `credentials.txt` 文件校验
  - 免认证路径: `/health`, `/docs`, `/redoc`, `/openapi.json`, `/account/*`, `/test/*`
  - Token = SHA-256(username)，7 天过期
- **异常处理**: HTTPException / BizException / ValidationError → 统一 `ErrorResponse{code, message, detail, trace_id}`

---

## 前端概要

- **仓库**: `../llm_autobahn_frontend/`
- **栈**: Vue 3 + Vite 6 + Element Plus + ECharts + Axios
- **端口**: 8735 (dev)，域名 `vortex.n.baidu-int.com`
- **应用名**: Data Vortex
- **核心页面**:
  - `/olap/dashboard` — 8 场景 KV Cache 趋势仪表盘
  - `/olap/discovery` — KV Cache 分析任务提交与结果查看
  - `/datasets/catalog` — 数据集目录
  - `/sqlStudio/sqlViewer` — SQL 查询
  - `/dashboard` — 首页概览
- **API 模块**: `src/api/{dashboard,olap,datasetMetadata,processScheduler,SQLAdaptor}/index.js`
- **权限控制**: `src/permission.js`（路由守卫 + group 访问控制）

---

## 已知问题与踩坑

| 问题 | 根因 | 修复/状态 |
|------|------|-----------|
| CFS flock 需写权限 | `open("r")` 无法加排他锁 | 改为 `open("r+")` ✅ |
| Worker heartbeat 失效 | `pool.map()` 阻塞事件循环 | 独立心跳线程 + run_in_executor ✅ |
| merged.txt OOM | glm-5 膨胀至 45GB，readlines() 爆内存 | 流式扫描 + seek/copyfileobj ✅ |
| cache_calc O(N) | 每分钟全量重算 | checkpoint 增量模式 O(1) ✅ |
| checkpoint CFS I/O 慢 | 1.6GB × 4 模型 = 3.2GB/min | tmpfs + 异步 CFS 备份 ✅ |
| section 文件 CFS 往返 | 写 CFS + 读 CFS ≈ 1-3s | stdin 管道模式 `-f -` ✅ |
| **tokenize 100% 超时** | **~150 万条/min 全量拉取，仅 1 daemon 处理** | **已修复：Stage 1.5 预过滤+切片并行（_prefilter_and_split），150万→~30-50万，N 个 daemon 并行** ✅ |
| **ESIndexService 不支持 model_filter** | `qianfan_model` 不是 ES 独立字段，嵌在 `@raw` 文本中（`qianfan_model:xxx`），`terms` / `match_phrase` 均无法可靠过滤 | **已确认不可行，禁止再走 ES 层预过滤路线** |

---

## 常用操作速查

```bash
# 测试 API
curl http://localhost:8739/health
curl "http://localhost:8739/api/v1/olap/kv/realtime?time_range=1h"
curl "http://localhost:8739/api/v1/olap/kv/realtime/status"
curl "http://localhost:8739/api/v1/olap/kv/dashboard?time_range=7d"
curl "http://localhost:8739/api/v1/olap/kv/models"

# 调度器 dry-run
python scripts/realtime_scheduler.py --dry-run

# Worker 单次测试
python scripts/realtime_worker.py --once

# Pipeline 手动运行
python scripts/run_pipeline.py --task-id test-001 --username test \
  --start-datetime "2026-04-13 10:00:00" --end-datetime "2026-04-13 11:00:00"

# C++ 编译
make -C src/domains/kv/cache_hit_rate/

# 测试
pytest tests/test_e2e.py -v --tb=short
```

---

## 文件大小提示（辅助 context 预算）

| 文件 | 行数 | 说明 |
|------|------|------|
| `app/api/v1/olap.py` | ~2400 | 最大文件，Pipeline API + 实时 + 日报 |
| `scripts/run_pipeline.py` | ~1200 | 主 Pipeline 脚本 |
| `scripts/realtime_worker.py` | ~875 | 实时 Worker |
| `scripts/compute_trend.py` | ~650 | 趋势计算 |
| `scripts/tokenize_script.py` | ~500 | tokenize 核心 |
| `scripts/kv_pipeline.py` | ~350 | TokenizeDaemonClient |
| `scripts/tokenize_daemon.py` | ~280 | daemon 进程 |
| `src/domains/kv/svc.py` | ~400 | ES 查询服务 |
| `src/domains/kv/impl.py` | ~300 | ES 底层客户端 |
| `src/domains/kv/cache_hit_rate/cache_calc.cpp` | ~480 | C++ LRU 模拟 |

> 优先读取标记 ★ 的文件。对于大文件（olap.py、run_pipeline.py），建议先读目标函数周围的行而非全文。

---

## Git 规范

- **提交格式**: `type(scope): 中文描述`
- **常见 type**: `feat`, `fix`
- **常见 scope**: `olap`, `olap_dashboard`, `olap_config`, `olap_script`, `deployment`, `es`
- **分支**: 主分支 `main`，直接提交（无 feature branch 工作流）
- **无 CI/CD**: 手动构建镜像 + 手动 kubectl 部署

---

## 延伸文档

| 文档 | 路径 | 内容 |
|------|------|------|
| OLAP Dashboard 设计 | `OLAP_Dashboard.md` | 实时流程详图、踩坑记录、tokenize 超时分析与解决方案 |
| Pipeline 优化方案 | `docs/pipeline_optimization_plan.md` | Pipeline 性能优化设计 |
| OOM 分析 | `docs/OOM_issue_analysis.md` | OOM 调试记录 |
| K8s 迁移设计 | `docs/prd.md` | K8s Job 迁移产品需求 |
| 架构方案 | `docs/solution.md` | 整体架构设计 |
| Pipeline README | `scripts/README.md` | 脚本使用说明 |

---

*最后更新: 2026-04-14*
