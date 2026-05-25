# OLAP Dashboard 数据流方案

## 背景

OLAP Dashboard 展示 8 个固定场景的 KV Cache 命中率趋势。实时场景（全场景_各模型）通过常驻 Worker 产出分钟级数据；其余 7 个场景通过日报 Pipeline 产出分钟级 trend 数据。Dashboard API 统一从 `hit_rate_trend.json` 读取并聚合，前端直接渲染分钟级趋势图。

## 业务需求

| 需求 | 方案 |
|------|------|
| 8 个固定场景 | 实时场景 + 7 个非实时场景，统一分钟级趋势图 |
| 实时场景时间窗口 | 每分钟提交 1 分钟任务，目标区间 `now-5min ~ now-4min` |
| 实时场景模型范围 | glm-5, kimi-k2.5, minimax-m2.5, deepseek-v3.2 |
| 非实时场景数据源 | 日报 Pipeline 产出的 `hit_rate_trend.json`（分钟级） |
| 独立 pipeline | 实时 Worker Pod + 独立存储目录 + 独立配置文件 |
| 持久化到 CFS | `olap_database/realtime/` 目录 |
| 跨天查询 | 按 time_range 加载对应天数文件/任务，支持 1h/6h/1d/7d/30d |
| JSON 存储压力 | 每天 ~200KB，30 天 ~6MB，无压力 |
| mean/max/min | 后端计算，前端直接展示 |

## 架构总览

```
                          线上 ES 日志 (KV Cache 请求)
                               │
                 ┌─────────────┴─────────────┐
                 │                           │
           日报 Pipeline                 实时 Worker (常驻)
           (手动/定时触发)               (K8s Deployment)
                 │                           │
                 ▼                           ▼
          ┌───────────┐              ┌────────────────┐
          │ fetch (ES)│              │  fetch (ES)    │
          │ tokenize  │              │  tokenize      │
          │ simulate  │              │  trend (增量)   │
          │ trend     │              └───────┬────────┘
          └─────┬─────┘                      │
                │                             ▼
                ▼               realtime/全场景_各模型/
  olap_database/data/{user}/     └─ YYYY-MM-DD.json
    └─ {task}/                         {"data": {"10:00": {"glm-5": 0.85, ...}}}
       ├─ tokenized/*.txt
       └─ report/
          ├─ cache_report.json         ← Stage 3 (task 级聚合)
          └─ hit_rate_trend.json       ← Stage 4 (分钟级趋势)
```

### 数据流详图

```
dashboard.vue 初始化
       │
       ├─ fetchRealtimeStatus()  ─→ GET /kv/realtime/status
       │   └─ 返回 {alive, ...}
       │
       └─ 对每个场景 fetchScenarioData()
              │
              ├───── 全场景_各模型 (实时) ─────────────────────────┐
              │                                                  │
              │   realtimeStatus.alive?                           │
              │     ├─ YES → kvRealtime(timeRange)                │
              │     │         GET /kv/realtime?time_range=7d      │
              │     │         ┌─────────────────────────────┐     │
              │     │         │ 读取 realtime/全场景_各模型/  │     │
              │     │         │ YYYY-MM-DD.json (跨天合并)   │     │
              │     │         │ → hr*100 → stats             │     │
              │     │         └─────────────────────────────┘     │
              │     │                                            │
              │     └─ NO → kvDashboard(timeRange)  (fallback)   │
              │              GET /kv/dashboard?time_range=7d      │
              │              ┌─────────────────────────────┐      │
              │              │ 找最新1个全场景任务            │      │
              │              │ 读 hit_rate_trend.json       │      │
              │              │ → hr*100 → stats             │      │
              │              └─────────────────────────────┘      │
              │                                                  │
              └───── 其他7个场景 (非实时) ────────────────────────┘
                    (coding_plan, 讯飞, 无问芯穹, 得物, 金山, 腾讯, 智谱)
                                          │
                                          ▼
                    kvDashboard(timeRange)
                    GET /kv/dashboard?time_range=7d
                    ┌────────────────────────────────────────────┐
                    │ _find_tasks_for_scenario_in_range()        │
                    │   → 找到时间范围内所有已完成任务              │
                    │                                            │
                    │ _aggregate_task_trends_to_points()         │
                    │   → 对每个 task:                           │
                    │     读取 hit_rate_trend.json               │
                    │     提取整体 + 模型级分钟数据               │
                    │     hr(0~1) × 100 → 百分比                 │
                    │   → 多 task 同时间点去重合并                │
                    │   → 无 trend 的 task fallback 单点         │
                    │                                            │
                    │ _compute_stats(overall_points)             │
                    │   → {mean, max, min}                       │
                    └────────────────────────────────────────────┘
```

### 三个 API 的分工

| API | 数据源 | 场景 | 时间粒度 |
|-----|--------|------|---------|
| `GET /kv/realtime` | `realtime/` 目录 (Worker 写入) | 仅 全场景_各模型 | 分钟级 |
| `GET /kv/realtime/status` | Worker 心跳文件 | 仅 全场景_各模型 | - |
| `GET /kv/dashboard` | `status/*.json` + `hit_rate_trend.json` | **全部 8 个场景** | 分钟级 |

### 统一返回格式（前端消费）

```json
{
  "scenarios": {
    "讯飞_全场景_glm-5": {
      "task_id": "xxx-kv_20260412_080000_...",
      "points": [
        {"time": "04-12 08:00", "hit_rate": 84.85},
        {"time": "04-12 08:01", "hit_rate": 85.12}
      ],
      "stats": {"mean": 85.2, "max": 92.1, "min": 78.3},
      "models": {
        "glm-5": {
          "points": [{"time": "04-12 08:00", "hit_rate": 84.85}],
          "stats": {"mean": 84.8, "max": 91.0, "min": 77.5}
        }
      }
    }
  },
  "time_range": "7d"
}
```

### 数据转换链路

```
cache_calc (C++ LRU 模拟)
  → section_hit_rate: 0~1 小数 (如 0.8485)
      │
      ├─→ 实时 Worker: 直接写入 daily JSON (0~1)
      │     → /kv/realtime API: hr ≤ 1 ? hr×100 : hr → 百分比
      │
      └─→ 日报 Pipeline: 写入 hit_rate_trend.json (0~1)
            → /kv/dashboard API:
               ├─ 实时分支: hr × 100 → 百分比
               └─ 非实时分支: _aggregate_task_trends_to_points() → hr × 100 → 百分比
```

所有路径最终输出到前端时，`hit_rate` 统一为 **0~100 百分比**，前端直接拼接 `%` 展示。

## 实时场景架构

采用**常驻 Worker Pod + cron 调度器**方案：

```
调度器 (cron, 每分钟)              Worker Pod (常驻, K8s Deployment)
┌───────────────────┐            ┌─────────────────────────────────┐
│ realtime_         │  CFS 文件  │                                 │
│ scheduler.py      │  队列      │  1. 启动时加载 tokenizer (一次)  │
│                   │ ────────→  │  2. 循环扫描 queue/ 目录         │
│ • 计算 T=now-5min │            │  3. 取任务: fetch→tokenize→     │
│ • 幂等检查        │            │    simulate→trend               │
│ • 写任务文件到     │            │  4. 直接写结果到每日 JSON        │
│   queue/          │            │  5. 清理任务中间数据              │
│                   │            │  6. 回到步骤 2                   │
└───────────────────┘            └─────────────────────────────────┘
                                        │
                                        ▼
                               olap_database/realtime/
                                 全场景_各模型/
                                   YYYY-MM-DD.json

API: GET /api/v1/olap/kv/realtime?time_range=1h
状态: GET /api/v1/olap/kv/realtime/status
```

### 为什么选择常驻 Worker Pod

每分钟提交 K8s Job 存在 Pod 启停开销（调度 5-60s + tokenizer 加载 5-15s），
而 Worker Pod 常驻运行，tokenizer 只加载一次，彻底消除启动开销。

## 命中率计算核心逻辑

### cache_calc (C++)

入口：`src/domains/kv/cache_hit_rate/cache_calc.cpp`

1. **分块**：将每条对话的 token 序列按 `block_size` 切成多个 block
2. **哈希**：对每个 block 计算哈希值（默认开启前缀哈希，block N 的哈希包含 block 0~N-1 的信息）
3. **LRU 查询**：将每个 block hash 放入 LRU Cache 查询
   - 命中 → `hit_count++`
   - 未命中 → 插入缓存，满了则淘汰最久未访问的 block
4. **命中率**：
   ```
   hit_rate = hit_count / total_adds           （范围 0.0 ~ 1.0，全局统计）
   section_hit_rate = section_hits / section_adds （范围 0.0 ~ 1.0，段级统计）
   ```

### 实时场景 vs 非实时场景的命中率计算

| 维度 | 实时场景 | 非实时场景 |
|------|---------|----------|
| 数据来源 | `realtime/` 目录 (Worker 写入) | `hit_rate_trend.json` (Pipeline Stage 4 产出) |
| 整体命中率 | 各模型算术平均 | 各模型加权平均 (hit_count / total_queries) |
| 模型级命中率 | section_hit_rate (0~1) | section_hit_rate (0~1) |
| API 转换 | hr ≤ 1 ? hr×100 : hr | hr × 100 |

### cache_calc 参数含义

| 参数 | 含义 | 默认值 | 实时配置 |
|------|------|--------|---------|
| `-s` (cache_size) | LRU 缓存最大 block 数量 | 无 (必填) | 200000000 |
| `-b` (block_size) | 每个 block 包含的 token 数量 | 64 | 16 |
| `-p` (prefix_hash) | 使用前缀哈希 | true | true |
| `-c` (checkpoint) | checkpoint 文件路径 | 无 | 有 (增量模式) |
| `-f -` | stdin 管道模式 | 无 | 有 |

## 配置

实时 pipeline 使用独立配置文件 `app/conf/realtime_config.json`，不与日报 pipeline 共用 `olap_config.json`。

```json
{
  "enabled": true,
  "scenario": "全场景_各模型",
  "models": "glm-5,kimi-k2.5,minimax-m2.5,deepseek-v3.2",
  "delay_minutes": 5,
  "retention_days": 30,
  "pipeline_fetch_concurrency": 5,
  "pipeline_fetch_window_concurrency": 12,
  "pipeline_es_scroll_workers": 60,
  "pipeline_tokenize_concurrency": 5,
  "pipeline_tokenize_workers": 6,
  "pipeline_tokenize_batch_size": 10000,
  "pipeline_es_scroll_size": 10000,
  "pipeline_block_size": 16,
  "pipeline_cache_size": 200000000,
  "k8s_image": "ccr-2663zxft-vpc.cnc.bj.baidubce.com/qianfan-data/llm_autobahn_backend:0.5.4",
  "k8s_cpu_request": "28",
  "k8s_cpu_limit": "28",
  "k8s_memory_request": "110Gi",
  "k8s_memory_limit": "110Gi",
  "k8s_namespace": "cache-hit-rate-tokenizer",
  "k8s_working_dir": "/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend",
  "k8s_cfs_host_path": "/mnt/cfs_bj_mt",
  "k8s_cfs_mount_path": "/mnt/cfs_bj_mt"
}
```

### 配置说明

| 字段 | 说明 |
|------|------|
| `enabled` | 是否启用实时 pipeline |
| `scenario` | 场景名称，对应存储目录名 |
| `models` | 逗号分隔的模型列表 |
| `delay_minutes` | ES 数据延迟（分钟），确保数据完整 |
| `retention_days` | 每日 JSON 保留天数 |
| `pipeline_*` | 实时独立并发配置，不与日报共用 |
| `k8s_*` | K8s Deployment 资源配置（独立于日报 Job） |

## 存储结构

```
olap_database/
  realtime/
    queue/
      pending/                    # 调度器写入，Worker 消费
        2026-04-10_14-30.json
      running/                    # Worker 正在处理
      done/                       # 已完成
      failed/                     # 失败
    _cache_state/                 # per-model LRU cache 持久化
      glm-5/
        cache_checkpoint.bin      # LRU cache 状态 (checkpoint 模式主路径)
        merged.txt                # Fallback 全量数据 (checkpoint 不可用时)
      kimi-k2.5/
        cache_checkpoint.bin
        merged.txt
      minimax-m2.5/
        cache_checkpoint.bin
        merged.txt
    全场景_各模型/
      2026-04-10.json            # 每日趋势数据
      2026-04-09.json
    worker_heartbeat              # Worker 心跳文件（后台线程每 15s 更新）

  data/{user}/{task}/             # 日报 Pipeline 任务数据
    tokenized/                    # tokenize 产出
    report/
      cache_report.json           # Stage 3 产出 (task 级聚合)
      hit_rate_trend.json         # Stage 4 产出 (分钟级趋势，Dashboard 消费)
```

### 每日文件格式 (实时场景)

```json
{
  "date": "2026-04-10",
  "data": {
    "14:30": {
      "glm-5": 0.8485,
      "kimi-k2.5": 0.9012,
      "minimax-m2.5": 0.9534,
      "deepseek-v3.2": 0.8765,
      "整体": 0.8949
    }
  },
  "updated_at": "2026-04-10 15:31:00"
}
```

### hit_rate_trend.json 格式 (日报场景)

```json
{
  "series": [
    {
      "model": "整体",
      "data": [{"time": "04-08 10:00", "hit_rate": 0.8485}, ...],
      "stats": {"mean": 0.84, "max": 0.92, "min": 0.78}
    },
    {
      "model": "glm-5",
      "data": [{"time": "04-08 10:00", "hit_rate": 0.8512}, ...],
      "stats": {"mean": 0.85, "max": 0.91, "min": 0.77}
    }
  ]
}
```

## API

### 获取实时趋势数据

**端点**: `GET /api/v1/olap/kv/realtime?time_range=1h`

**响应**:
```json
{
  "scenarios": {
    "全场景_各模型": {
      "points": [{"time": "04-10 14:30", "hit_rate": 89.49}, ...],
      "stats": {"mean": 89.52, "max": 89.55, "min": 89.49},
      "models": {
        "glm-5": {"points": [...], "stats": {"mean": 84.85, "max": 85.01, "min": 84.70}},
        "kimi-k2.5": {"points": [...], "stats": {...}},
        "minimax-m2.5": {"points": [...], "stats": {...}},
        "deepseek-v3.2": {"points": [...], "stats": {...}}
      },
      "data_status": {
        "total_minutes": 60,
        "filled_minutes": 58,
        "coverage_pct": 96.7,
        "latest_minute": "04-10 14:30"
      }
    }
  },
  "time_range": "1h"
}
```

### 获取 Dashboard 趋势数据（全部 8 个场景）

**端点**: `GET /api/v1/olap/kv/dashboard?time_range=7d`

**响应**:
```json
{
  "scenarios": {
    "全场景_各模型": {
      "task_id": "xxx",
      "points": [{"time": "04-12 08:00", "hit_rate": 89.49}],
      "stats": {"mean": 89.52, "max": 89.55, "min": 89.49},
      "models": {"glm-5": {"points": [...], "stats": {...}}}
    },
    "讯飞_全场景_glm-5": {
      "task_id": "yyy",
      "points": [{"time": "04-12 08:00", "hit_rate": 84.85}],
      "stats": {"mean": 85.2, "max": 92.1, "min": 78.3},
      "models": {"glm-5": {"points": [...], "stats": {...}}}
    }
  },
  "time_range": "7d"
}
```

### 获取 Worker 状态

**端点**: `GET /api/v1/olap/kv/realtime/status`

**响应**:
```json
{
  "alive": true,
  "last_heartbeat": "2026-04-10 15:30:00",
  "queue": {
    "pending": 2,
    "running": 1,
    "failed": 0
  },
  "latest_minute": "2026-04-10 15:25"
}
```

心跳超过 120 秒视为失活。

## 部署

### 1. Worker Pod（K8s Deployment）

```bash
# 部署（通过 deploy_realtime.py 脚本，自动替换模板变量）
python scripts/deploy_realtime.py

# 预览替换后的 YAML
python scripts/deploy_realtime.py --dry-run

# 删除
python scripts/deploy_realtime.py --delete

# 重启（更新代码/配置后）
kubectl --kubeconfig app/conf/inner_cluster.kubeconfig -n cache-hit-rate-tokenizer rollout restart deployment/realtime-worker
```

注意：不能直接 `kubectl apply -f realtime_deployment.yml`，模板中的 `${VAR}` 需要先由 `deploy_realtime.py` 替换。

### 2. 调度器（cron）

```bash
# crontab -e 添加
* * * * * /usr/bin/python3 /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/scripts/realtime_scheduler.py >> /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/logs/realtime_scheduler.log 2>&1
```

### 3. 前端

Dashboard 对"全场景_各模型"场景：
- Worker 存活时显示绿色"实时"标签，自动使用实时数据
- Worker 离线时显示红色"离线"标签，回退到日报数据
- mean/max/min 统计由后端计算，前端直接展示

Dashboard 对其他 7 个场景：
- 通过 `/kv/dashboard` API 获取分钟级趋势数据（从各任务的 `hit_rate_trend.json` 聚合）
- 支持 1d / 7d 时间范围
- 图例支持整体 + 各模型的 Mean/Max/Min 统计

## 文件清单

| 操作 | 文件 | 说明 |
|------|------|------|
| 新建 | `scripts/realtime_worker.py` | 常驻 Worker 主进程 |
| 新建 | `scripts/realtime_scheduler.py` | cron 调度器 |
| 新建 | `scripts/deploy_realtime.py` | K8s Deployment 部署脚本 |
| 新建 | `app/conf/realtime_config.json` | 实时 pipeline 独立配置 |
| 新建 | `app/conf/realtime_deployment.yml` | K8s Deployment 模板 |
| 修改 | `app/api/v1/olap.py` | 新增 `/kv/realtime` 和 `/kv/realtime/status` 端点；非实时场景改为 `_aggregate_task_trends_to_points()` 读取分钟级 trend 数据 |
| 修改 | `src/api/olap/index.js` | 新增前端 API 调用 |
| 修改 | `src/views/olap/dashboard.vue` | Dashboard 适配实时数据 + 存活标签 |
| 修改 | `scripts/realtime_worker.py` | 心跳独立线程；pool.map→run_in_executor；_trim_merged_file 流式改造+大小限制；checkpoint 增量模式；tmpfs checkpoint + stdin 管道 |
| 修改 | `src/domains/kv/cache_hit_rate/lru.h` | 新增 saveCheckpoint / loadCheckpoint 二进制持久化 |
| 修改 | `src/domains/kv/cache_hit_rate/cache_calc.cpp` | 新增 `-c` checkpoint 参数；新增 `-f -` stdin 管道模式 |

## 复用的现有组件

| 组件 | 来源 | 用途 |
|------|------|------|
| `TokenizeDaemonClient` | `scripts/kv_pipeline.py` | tokenizer 常驻复用 |
| `ESIndexService` | `src/domains/kv/svc.py` | fetch 阶段 ES 查询 |
| `_calc_single_file` | `scripts/compute_trend.py` | 单文件 cache_calc |
| `_compute_stats` | `app/api/v1/olap.py` | mean/max/min 后端计算 |
| `_read_trend_for_task` | `app/api/v1/olap.py` | 读取任务的 hit_rate_trend.json |
| `_parse_trend_time` | `app/api/v1/olap.py` | 解析 trend 时间格式 |

## 验证

1. Scheduler dry-run：`python scripts/realtime_scheduler.py --dry-run`
2. Worker 本地测试：`python scripts/realtime_worker.py --once`
3. API 测试：`curl "http://localhost:8739/api/v1/olap/kv/realtime?time_range=1h"`
4. 状态测试：`curl "http://localhost:8739/api/v1/olap/kv/realtime/status"`
5. Dashboard 测试：`curl "http://localhost:8739/api/v1/olap/kv/dashboard?time_range=7d"`
6. 跨天验证：构造跨天数据，验证 `time_range=1d`
7. 部署：`python scripts/deploy_realtime.py`
8. 重启：`kubectl --kubeconfig app/conf/inner_cluster.kubeconfig -n cache-hit-rate-tokenizer rollout restart deployment/realtime-worker`
9. 查看日志：`kubectl --kubeconfig app/conf/inner_cluster.kubeconfig -n cache-hit-rate-tokenizer logs -f deployment/realtime-worker`

## 踩坑记录

### CFS 文件锁要求写权限

`fcntl.flock(fd, LOCK_EX)` 在 CFS 上要求文件描述符有写权限，否则抛 `OSError: [Errno 9] Bad file descriptor`。

- 错误写法：`open(task_file, "r")` — 只读打开，加排他锁失败
- 正确写法：`open(task_file, "r+")` — 读写打开，加排他锁成功

该 bug 表现为 Worker 心跳正常更新、pending 队列有任务但不消费（锁失败被静默捕获，无日志输出）。

### 心跳线程与事件循环阻塞

**现象**：Worker Pod 频繁因 liveness probe `heartbeat stale` 重启（15 小时 105 次）。

**根因**：`_process_task` 中 `ThreadPoolExecutor.map()` 是同步阻塞调用，在 async 函数中直接调用会阻塞 asyncio 事件循环，导致主循环中的 `_touch_heartbeat()` 无法被调度。

**修复**：

1. **后台心跳线程**：启动独立 daemon 线程每 15 秒写心跳，不依赖事件循环。即使事件循环被阻塞，liveness probe 也不会误判。
2. **`pool.map()` → `run_in_executor`**：将同步阻塞的 `pool.map()` 改为 `await loop.run_in_executor(None, _calc_all_models, ...)`，事件循环在 trend 计算期间可以继续调度。

### merged.txt 全量读入内存

**现象**：glm-5 模型的 `merged.txt` 膨胀至 45GB（每 section ~937MB，是其他模型的 20+ 倍），`_trim_merged_file` 中 `f.readlines()` 将全文加载到内存导致 OOM 或极慢。

**修复**：

1. **流式扫描**：`f.readlines()` → `for line in f`（流式迭代，内存恒定）+ `seek + copyfileobj` 替代 Python list 切片。
2. **按大小裁剪**：新增 `max_size_mb=2048` 参数，文件超过 2GB 也触发裁剪，防止无限增长。

### cache_calc 增量计算（Checkpoint）

**问题**：每分钟计算 hit_rate 时，`cache_calc` 需要从头处理 `merged.txt` 的所有历史 section，计算量随时间线性增长。48 个 section × 937MB = 45GB，在 CFS 上读取需要 30~75 分钟。

**方案**：为 `cache_calc` 添加 `-c <checkpoint_path>` 参数，支持 LRU cache 状态的持久化（`lru.h` 的 `saveCheckpoint` / `loadCheckpoint`）。

**流程对比**：

```
旧模式（全量，O(N)）：
  每分钟: cache_calc -f merged.txt (45GB, 全部 section)
  → 重跑所有历史数据以重建 LRU cache 状态

新模式（增量，O(1)）：
  首次: cache_calc -f section_0847.txt -c cache_checkpoint.bin
  → 生成 checkpoint (LRU cache 状态二进制文件)
  每分钟: cache_calc -f section_0848.txt -c cache_checkpoint.bin
  → 加载 checkpoint → 处理当前 section → 保存 checkpoint
  → 只处理当前分钟的新数据
```

**Checkpoint 文件格式**：

```
[magic: 4B] [version: 4B] [capacity: 8B] [total_adds: 8B] [hit_count: 8B]
[num_keys: 8B] [key_0: 8B] [key_1: 8B] ...
```

- `capacity` 必须与当前 `-s` 参数一致，否则跳过 checkpoint
- 原子写入：先写 `.tmp` 再 `rename`
- 对 200M block cache，checkpoint 文件 ~1.6GB

**语义等价性**：增量 checkpoint 的 `section_hit_rate` 与全量 baseline **完全一致**（已验证）。

**Fallback**：如果 checkpoint 不可用，自动回退到 `merged.txt` 全量模式。

**存储结构更新**：

```
olap_database/
  realtime/
    _cache_state/
      glm-5/
        cache_checkpoint.bin    # ← CFS 备份（异步写入，持久化）
        merged.txt              # ← Fallback 用，checkpoint 模式下不再依赖
      kimi-k2.5/
        cache_checkpoint.bin
        merged.txt
      minimax-m2.5/
        cache_checkpoint.bin
        merged.txt
```

### Checkpoint tmpfs 加速

**问题**：checkpoint 模式下，每分钟 cache_calc 需从 CFS 读写 ~1.6GB checkpoint 文件（4 模型合计 ~3.2GB/分钟 I/O）。CFS 吞吐 ~200-500 MB/s，单次 I/O 延迟 3-7.5s，是 pipeline 最大瓶颈。

**方案**：将 checkpoint 主路径放在 K8s emptyDir tmpfs（内存盘）上，CFS 仅作异步备份。

```
/mnt/checkpoint_tmpfs/           ← tmpfs (emptyDir, medium=Memory, 16Gi)
  glm-5/cache_checkpoint.bin     ← 主路径（内存盘，读写 <0.2s）
  kimi-k2.5/cache_checkpoint.bin
  minimax-m2.5/cache_checkpoint.bin
  deepseek-v3.2/cache_checkpoint.bin

olap_database/realtime/_cache_state/
  glm-5/cache_checkpoint.bin     ← CFS 备份（异步，持久化）
  glm-5/merged.txt               ← Fallback
  ...
```

**tmpfs 大小计算**（16Gi 依据）：

| 项目 | 计算 | 大小 |
|------|------|------|
| 单模型 checkpoint | 40B header + 200M × 8B keys | ~1.49 GiB |
| 4 模型稳态 | 4 × 1.49 GiB | ~5.96 GiB |
| 4 模型峰值（save 中旧文件 + .tmp 并存） | 4 × 2 × 1.49 GiB | ~11.92 GiB |
| **16Gi** | 峰值 + 34% 余量 | 覆盖所有极端场景 |

**Pod 重启恢复**：
1. Worker 启动时调用 `_restore_checkpoints_from_cfs()`，将 CFS 备份复制到 tmpfs
2. 如果 CFS 备份不存在，cache_calc 从空 cache 开始，后续自动建立 checkpoint
3. 每分钟计算完成后，后台线程异步备份 tmpfs → CFS（`_backup_checkpoint_to_cfs()`），不阻塞主流程

**本地开发兼容**：`checkpoint_tmpfs_dir` 为空或目录不存在时，checkpoint 路径自动回退到 CFS，无需 tmpfs。

**收益**：checkpoint 读写 3-7.5s → <0.2s，每分钟省 ~10-30s。

### cache_calc stdin 管道模式

**问题**：checkpoint 模式下，Worker 需将 section 数据写入临时文件到 CFS，cache_calc 再从 CFS 读取，每次 CFS 文件往返 ~1-3s。

**方案**：`cache_calc` 新增 `-f -` 参数，从 stdin 读取数据。Worker 通过管道直接传入 section 数据，省去 CFS 写+读。

```
旧: Python 写 section_file → CFS → cache_calc -f section_file → 删除 section_file
新: Python 构建 section_data → cache_calc -f - (stdin 管道) → 零 CFS I/O
```

**实现**：
- `cache_calc.cpp`：提取 `stream_process_istream(std::istream&, ...)` 通用函数，`stream_process_file` 改为打开文件后调用它；`-f -` 时直接传 `std::cin`
- `realtime_worker.py`：`subprocess.run(cmd, input=section_data, ...)` 替代写文件+运行

**收益**：省去 section 文件的 CFS 写+读，约 1-3s/分钟。

## 实时 Pipeline 完整流程（端到端）

### 整体时序

```
cron (每分钟)                    Worker Pod (常驻)
    │                                │
    ├─ realtime_scheduler.py         │
    │  ├─ T = now - 5min             │
    │  ├─ 幂等检查(queue+daily)      │
    │  └─ 写 queue/pending/*.json    │
    │                                │
    │                           ┌────┴─────────────────────────────────────────┐
    │                           │ _run_worker() 主循环 (每 5s 扫描)             │
    │                           │                                              │
    │                           │ 1. _scan_pending_tasks() → 取任务文件         │
    │                           │ 2. _try_lock_task() → CFS 排他锁 (fcntl)     │
    │                           │ 3. 幂等检查 → daily JSON 中已有则跳过         │
    │                           │ 4. _move_task(pending → running)              │
    │                           │                                              │
    │                           │ 5. _process_task():                           │
    │                           │    ┌──────────────────────────────────┐       │
    │                           │    │ Stage 1: Fetch (ES)              │       │
    │                           │    │  ESIndexService.query_to_dir()   │       │
    │                           │    │  scroll_size=10000               │       │
    │                           │    │  → 1 个 .jsonl 文件              │       │
    │                           │    │  耗时: ~6-7 min (150万条)        │       │
    │                           │    └──────────┬───────────────────────┘       │
    │                           │               ▼                               │
    │                           │    ┌──────────────────────────────────┐       │
    │                           │    │ Stage 2: Tokenize               │       │
    │                           │    │  daemon_client.submit() → RR    │       │
    │                           │    │  daemon_client.wait(timeout=3600)│       │
    │                           │    │  10 daemons × 3 workers each    │       │
    │                           │    │  但只有 1 个 jsonl → 只用 1 daemon│       │
    │                           │    │  150万条 × apply_chat_template  │       │
    │                           │    │  ★ 瓶颈: >1h 超时 ★             │       │
    │                           │    └──────────┬───────────────────────┘       │
    │                           │               ▼                               │
    │                           │    ┌──────────────────────────────────┐       │
    │                           │    │ Stage 3: Trend (cache_calc)     │       │
    │                           │    │  per-model 并行 (ThreadPool)     │       │
    │                           │    │  checkpoint + stdin 管道         │       │
    │                           │    │  耗时: <30s (checkpoint模式)     │       │
    │                           │    └──────────┬───────────────────────┘       │
    │                           │               ▼                               │
    │                           │    ┌──────────────────────────────────┐       │
    │                           │    │ Stage 4: Write daily JSON        │       │
    │                           │    │  _write_to_daily() 文件锁+原子写 │       │
    │                           │    │  耗时: <1s                       │       │
    │                           │    └──────────────────────────────────┘       │
    │                           │                                              │
    │                           │ 6. _move_task(running → done/failed)          │
    │                           │ 7. gc.collect()                               │
    │                           └──────────────────────────────────────────────┘
```

### 各阶段耗时实测 (基于 2026-04-13~14 日志)

| 阶段 | 耗时 | 数据量 | 状态 |
|------|------|--------|------|
| Fetch (ES scroll) | **6-7 min** | 1 分钟全流量 ~150 万条 | 正常 |
| Tokenize | **>60 min → 超时** | 150 万条 × 5 模型 tokenize | **100% 失败** |
| Trend (cache_calc) | <30s | checkpoint 增量模式 | 未能到达 |
| Write daily JSON | <1s | 追加 1 条记录 | 未能到达 |

### TokenizeDaemonClient 架构

```
realtime_worker.py
    │
    ├─ daemon_client = TokenizeDaemonClient(
    │      workers=3,           ← 每个 daemon 的 multiprocessing.Pool 大小
    │      batch_size=10000,    ← 每批提交给 Pool 的记录数
    │      num_daemons=10,      ← daemon 子进程总数 (Round-Robin)
    │  )
    │
    ├─ daemon_client.start(timeout=300)  ← 并行启动 10 个 daemon，加载 tokenizer
    │
    └─ _process_task():
         └─ for jsonl_file in jsonl_files:     ← 只有 1 个文件
              └─ _tokenize_via_daemon():
                   ├─ daemon_client.submit()    ← RR 分配到 1 个 daemon
                   └─ daemon_client.wait(3600)  ← 阻塞等待结果，超时1h

                         daemon-0 (tokenize_daemon.py)
                         ┌─────────────────────────────────┐
                         │ _process_file_with_pool():       │
                         │  ├─ 逐行读 jsonl                 │
                         │  ├─ 每 10000 行 → pool.apply_async│
                         │  ├─ max_pending = 3×2 = 6 批     │
                         │  ├─ _flush_results() 阻塞等待全部│
                         │  └─ 写 per-model _input_ids.txt  │
                         │                                  │
                         │  Pool workers (×3):              │
                         │   _worker_process_batch():       │
                         │    ├─ regex 预过滤 model          │
                         │    ├─ json.loads()               │
                         │    ├─ convert_record()           │
                         │    └─ apply_chat_template() ★慢★ │
                         └─────────────────────────────────┘

问题：
  10 个 daemon 只有 1 个在工作 → 9 个空闲
  1 个 daemon 只有 3 个 Pool worker → 只有 3 并发 tokenize
  150万条 × ~1-5ms/条 = 估算 25-125 分钟
  实际: 100% 超过 60 分钟超时
```

## "等待 tokenize 结果超时" 瓶颈分析

### 现象

从 2026-04-13 日志看，**全天 84 个任务，零成功**。每个任务均在 tokenize 阶段等待 1 小时后超时：

```
[rt-20260413-193200] tokenize failed: 等待 tokenize 结果超时: task_23
[rt-20260413-193400] tokenize failed: 等待 tokenize 结果超时: task_25
... (全部 84 个任务均如此)
```

### 根因

| 维度 | 现状 | 问题 |
|------|------|------|
| **数据量** | ~150 万条/分钟 (全流量) | ES 全场景无 app_id/path 过滤，数据量巨大 |
| **有效数据** | 5 模型可能只占全流量 10-30% | 150 万条中大量为无关模型，但仍需逐条 regex 过滤 |
| **daemon 利用率** | 10 个 daemon，1 个 jsonl 文件 | Round-Robin 只分配给 1 个 daemon，其余 9 个闲置 |
| **Worker 并发** | 单 daemon 3 个 Pool worker | 只有 3 个 apply_chat_template 并发 |
| **有效并发** | 28 CPU × 0 利用 | 总 28 核，实际只用 3 核 tokenize |
| **单记录耗时** | apply_chat_template ~1-5ms | 对长对话可达 10-50ms |
| **理论总耗时** | 150万 × 2ms / 3 worker = 1000s ≈ 17min | 含 regex 过滤 + I/O，实际更久 |
| **wait 方式** | time.sleep(0.05) 轮询 | 非关键瓶颈，但浪费 CPU |

### 瓶颈拆解

```
150 万条 JSONL 记录 (1 分钟全流量)
    │
    ├─ [快速] regex 预过滤: 跳过不在 model_filter 中的记录
    │   ├─ 命中: ~15-45 万条 (5 模型的流量)
    │   └─ 未命中: ~105-135 万条 (跳过，但仍需逐行读+regex)
    │
    ├─ [慢] json.loads() + convert_record() + apply_chat_template()
    │   ├─ 每条 ~1-50ms (取决于对话长度)
    │   ├─ 3 worker 并发
    │   └─ 30万条 × 3ms / 3 worker ≈ 300s = 5min (理想)
    │       45万条 × 5ms / 3 worker ≈ 750s = 12.5min (中等)
    │       45万条 × 10ms / 3 worker ≈ 1500s = 25min (偶有长对话)
    │
    └─ [I/O] 写 per-model _input_ids.txt 到 CFS
        └─ 占比较小，但 CFS 延迟累加
```

实际上 150 万条逐行读取 + regex 匹配本身也需要数分钟。加上 tokenize 处理，单 daemon 很难在 60 分钟内完成。

## 解决方案对比

### 方案 A: 提升 tokenize 并发度（改配置，不改代码）

**原理**：增大单 daemon 的 Pool worker 数，充分利用 28 核 CPU。

| 参数 | 当前值 | 调整值 | 说明 |
|------|--------|--------|------|
| `pipeline_tokenize_workers` | 3 | **20** | 单 daemon Pool 大小 |
| `pipeline_tokenize_concurrency` | 10 | **2** | daemon 数（减少内存浪费） |
| `pipeline_tokenize_batch_size` | 10000 | **2000** | 减小批次，降低尾延迟 |

**预期效果**：
- 有效并发从 3 → 20，理论速度提升 ~6.7x
- 45 万条 × 5ms / 20 worker ≈ 112s ≈ **2 分钟**（理想）
- 含 IO + regex 开销，估计 **5-10 分钟**

**风险**：内存增加（20 个 fork 子进程 × tokenizer 内存），但 COW 机制下增量有限，110Gi 足够。

**操作**：修改 `app/conf/realtime_config.json`，重启 Worker Pod。

### 方案 B: 在 ES 层按 model 预过滤（改 ES 查询，不改 tokenize 代码）

**原理**：在 ES 查询中增加 `qianfan_model` 过滤条件，只拉取 5 个目标模型的数据。

**预期效果**：
- 数据量从 150 万 → 15-45 万条（减少 70-90%）
- fetch 耗时从 7min → 1-2min
- tokenize 无需 regex 过滤无关记录
- 总时间 **3-8 分钟**

**风险**：
- 需确认 ES 索引有 `qianfan_model` 字段且已建索引
- 需改 `svc.py` 的 `query_to_dir()` 或传 model 过滤参数

### 方案 C: 将单 JSONL 切分为 N 片，分发到 N 个 daemon 并行处理

**原理**：fetch 完成后，将大 jsonl 按行数切分成 N 个小文件，每个文件分发到不同 daemon。

**预期效果**：
- 充分利用 10 个 daemon × 3 worker = 30 并发
- 45 万条 / 10 daemon × 5ms / 3 worker ≈ **75s ≈ 1.3 分钟**

**风险**：
- 需要额外的 CFS I/O 写切片文件（或用内存分片）
- 代码改动量较大

### 方案对比总结

| 维度 | 方案 A (加 Worker) | 方案 B (ES 预过滤) | 方案 C (预过滤+切片并行) |
|------|-------|-------|-------|
| 代码改动 | **零** (仅改配置) | ❌ **不可行** | 中等 (改 worker) |
| 预期效果 | 5-10 min/任务 | N/A | **1-3 min/任务** |
| 是否能追上 1 分钟/任务 | 不一定 | N/A | **可以** |
| 实施难度 | 低 | N/A | 中 |
| 状态 | ✅ 已实施 (workers=20) | ❌ 已排除 | ✅ **已实施** |

**方案 B 排除原因**：`qianfan_model` 不是 ES 独立字段，而是嵌在 `@raw` 文本中（格式 `qianfan_model:xxx`），ES 层的 `terms` / `match_phrase` 查询均无法可靠过滤。

**最终方案**：A + C 叠加。

### 方案 C 实施详情（已完成）

在 `realtime_worker.py` 的 Stage 1 (Fetch) 和 Stage 2 (Tokenize) 之间新增 **Stage 1.5: Pre-filter & Split**：

```
Stage 1: Fetch → ~150万条 JSONL files
    ↓
Stage 1.5: Pre-filter + Split（新增函数 _prefilter_and_split）
    → 纯 Python regex 扫描每个 JSONL（无需 JSON 解析，~10秒）
    → 仅保留匹配 5 个目标模型的行（qianfan_model:xxx）
    → 过滤后的行 Round-Robin 写入 N 个切片文件（N = daemon 数量）
    → 150万 → ~30-50万条，拆成 N 个文件
    ↓
Stage 2: Tokenize → N 个切片文件由 ThreadPoolExecutor 并发提交到 N 个 daemon
    → 每个 daemon 20 workers 并行 tokenize
    ↓
清理 filtered/ 临时目录
```

**核心改动**：
- 新增 `_prefilter_and_split()` 函数（~50行）
- Stage 2 改为 `ThreadPoolExecutor` 并行提交 N 个切片文件
- tokenize 完成后 `shutil.rmtree(filtered_dir)` 清理临时文件

## 线上容器测试方案

### 前置：进入 Worker Pod

```bash
# 找到 worker pod
kubectl --kubeconfig app/conf/inner_cluster.kubeconfig \
  -n cache-hit-rate-tokenizer get pods

# 进入 pod
kubectl --kubeconfig app/conf/inner_cluster.kubeconfig \
  -n cache-hit-rate-tokenizer exec -it <pod-name> -- bash
```

### 测试 1: 验证当前瓶颈（观察 tokenize 耗时）

在 Pod 内运行单次任务，观察各阶段耗时：

```bash
cd /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend

# 单次执行（--once 处理 1 个任务后退出），先确保 queue/pending/ 中有任务
python scripts/realtime_scheduler.py
python scripts/realtime_worker.py --once 2>&1 | tee /tmp/test_baseline.log

# 观察关键时间戳
grep -E "fetch done|tokenize:|tokenize done|trend done|完成" /tmp/test_baseline.log
```

### 测试 2: 方案 A — 调整 tokenize 并发参数

修改配置后重启 Worker：

```bash
# 备份原配置
cp app/conf/realtime_config.json app/conf/realtime_config.json.bak

# 修改参数（可用 python 或 sed）
python3 -c "
import json
cfg = json.load(open('app/conf/realtime_config.json'))
cfg['pipeline_tokenize_workers'] = 20      # 3 → 20
cfg['pipeline_tokenize_concurrency'] = 2    # 10 → 2
cfg['pipeline_tokenize_batch_size'] = 2000  # 10000 → 2000
json.dump(cfg, open('app/conf/realtime_config.json', 'w'), indent=2, ensure_ascii=False)
print('done:', {k:v for k,v in cfg.items() if 'tokenize' in k})
"

# 单次测试
python scripts/realtime_scheduler.py
python scripts/realtime_worker.py --once 2>&1 | tee /tmp/test_plan_a.log

# 对比
grep -E "fetch done|tokenize:|tokenize done|trend done|完成" /tmp/test_plan_a.log
```

**预期**：tokenize 从 >60min 超时降到 5-10 分钟完成。

### 测试 3: 方案 A 激进版 — 进一步加大并发

```bash
python3 -c "
import json
cfg = json.load(open('app/conf/realtime_config.json'))
cfg['pipeline_tokenize_workers'] = 24      # 接近 CPU 核数
cfg['pipeline_tokenize_concurrency'] = 1    # 只用 1 个 daemon
cfg['pipeline_tokenize_batch_size'] = 1000  # 更小的批次
json.dump(cfg, open('app/conf/realtime_config.json', 'w'), indent=2, ensure_ascii=False)
print('done:', {k:v for k,v in cfg.items() if 'tokenize' in k})
"

python scripts/realtime_scheduler.py
python scripts/realtime_worker.py --once 2>&1 | tee /tmp/test_plan_a2.log

grep -E "fetch done|tokenize:|tokenize done|trend done|完成" /tmp/test_plan_a2.log
```

### 测试 4: 监控资源使用

在测试期间，在另一个终端监控 CPU 和内存：

```bash
# CPU 使用（每 5 秒采样）
while true; do date; top -bn1 | head -5; sleep 5; done > /tmp/cpu_monitor.log &

# 或用 mpstat 看 per-core
mpstat -P ALL 5 > /tmp/mpstat.log &

# 内存
free -h && echo "---" && ps aux --sort=-rss | head -20
```

### 测试 5: 确认成功后的持久化部署

```bash
# 恢复或保留配置
# 如果方案 A 验证通过，直接重启 deployment
kubectl --kubeconfig app/conf/inner_cluster.kubeconfig \
  -n cache-hit-rate-tokenizer rollout restart deployment/realtime-worker

# 观察新 Pod 启动和处理情况
kubectl --kubeconfig app/conf/inner_cluster.kubeconfig \
  -n cache-hit-rate-tokenizer logs -f deployment/realtime-worker --tail=100
```

### 参数速查表

| 参数 | 当前值 | 方案 A 保守 | 方案 A 激进 | 含义 |
|------|--------|-------------|-------------|------|
| `pipeline_tokenize_workers` | 3 | 20 | 24 | 每 daemon 的 Pool 进程数 |
| `pipeline_tokenize_concurrency` | 10 | 2 | 1 | daemon 子进程总数 |
| `pipeline_tokenize_batch_size` | 10000 | 2000 | 1000 | 每批记录数 |
| **有效 tokenize 并发** | **3** | **20** | **24** | workers × 活跃 daemon |
| **内存估算(fork COW)** | ~30G | ~35G | ~40G | 110Gi 充足 |
