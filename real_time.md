# 全场景_各模型 实时化 Pipeline 方案

## 背景

当前"全场景_各模型"是纯离线批处理（每天 cron 触发，只计算前一天 20:00-21:00 的 1 小时数据）。
需改为实时数据，支持 Dashboard 查看分钟级命中率趋势。

## 业务需求

| 需求 | 方案 |
|------|------|
| 时间窗口 | 每分钟提交 1 分钟任务，目标区间 `now-5min ~ now-4min` |
| 模型范围 | glm-5, kimi-k2.5, minimax-m2.5, deepseek-v3.2 |
| 独立 pipeline | 独立 Worker Pod + 独立存储目录 + 独立配置文件 |
| 持久化到 CFS | `olap_database/realtime/` 目录 |
| 跨天查询 | 按 time_range 加载对应天数文件，支持 1h/6h/1d/7d/30d |
| JSON 存储压力 | 每天 ~200KB，30 天 ~6MB，无压力 |
| mean/max/min | 后端计算，前端只展示 |

## 架构

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

## 配置

实时 pipeline 使用独立配置文件 `app/conf/realtime_config.json`，不与日报 pipeline 共用 `olap_config.json`。

```json
{
  "enabled": true,
  "scenario": "全场景_各模型",
  "models": "glm-5,kimi-k2.5,minimax-m2.5,deepseek-v3.2",
  "delay_minutes": 5,
  "retention_days": 30,
  "pipeline_fetch_concurrency": 2,
  "pipeline_fetch_window_concurrency": 4,
  "pipeline_es_scroll_workers": 12,
  "pipeline_tokenize_concurrency": 2,
  "pipeline_tokenize_workers": 4,
  "pipeline_tokenize_batch_size": 1000,
  "pipeline_es_scroll_size": 5000,
  "pipeline_block_size": 16,
  "pipeline_cache_size": 200000000,
  "k8s_image": "ccr-2663zxft-vpc.cnc.bj.baidubce.com/qianfan-data/llm_autobahn_backend:0.2.5",
  "k8s_cpu_request": "14",
  "k8s_cpu_limit": "14",
  "k8s_memory_request": "55Gi",
  "k8s_memory_limit": "55Gi",
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
    全场景_各模型/
      2026-04-10.json            # 每日趋势数据
      2026-04-09.json
    worker_heartbeat              # Worker 心跳文件
```

### 每日文件格式

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

每天 ~200KB，30 天 ~6MB。mean/max/min 由 API 后端 O(n) 计算。

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

## 文件清单

| 操作 | 文件 | 说明 |
|------|------|------|
| 新建 | `scripts/realtime_worker.py` | 常驻 Worker 主进程 |
| 新建 | `scripts/realtime_scheduler.py` | cron 调度器 |
| 新建 | `scripts/deploy_realtime.py` | K8s Deployment 部署脚本 |
| 新建 | `app/conf/realtime_config.json` | 实时 pipeline 独立配置 |
| 新建 | `app/conf/realtime_deployment.yml` | K8s Deployment 模板 |
| 修改 | `app/api/v1/olap.py` | 新增 `/kv/realtime` 和 `/kv/realtime/status` 端点 |
| 修改 | `src/api/olap/index.js` | 新增前端 API 调用 |
| 修改 | `src/views/olap/dashboard.vue` | Dashboard 适配实时数据 + 存活标签 |

## 复用的现有组件

| 组件 | 来源 | 用途 |
|------|------|------|
| `TokenizeDaemonClient` | `scripts/kv_pipeline.py` | tokenizer 常驻复用 |
| `ESIndexService` | `src/domains/kv/svc.py` | fetch 阶段 ES 查询 |
| `_calc_single_file` | `scripts/compute_trend.py` | 单文件 cache_calc |
| `_compute_stats` | `app/api/v1/olap.py` | mean/max/min 后端计算 |

## 验证

1. Scheduler dry-run：`python scripts/realtime_scheduler.py --dry-run`
2. Worker 本地测试：`python scripts/realtime_worker.py --once`
3. API 测试：`curl "http://localhost:8739/api/v1/olap/kv/realtime?time_range=1h"`
4. 状态测试：`curl "http://localhost:8739/api/v1/olap/kv/realtime/status"`
5. 跨天验证：构造跨天数据，验证 `time_range=1d`
6. 部署：`python scripts/deploy_realtime.py`
7. 重启：`kubectl --kubeconfig app/conf/inner_cluster.kubeconfig -n cache-hit-rate-tokenizer rollout restart deployment/realtime-worker`
8. 查看日志：`kubectl --kubeconfig app/conf/inner_cluster.kubeconfig -n cache-hit-rate-tokenizer logs -f deployment/realtime-worker`

## 踩坑记录

### CFS 文件锁要求写权限

`fcntl.flock(fd, LOCK_EX)` 在 CFS 上要求文件描述符有写权限，否则抛 `OSError: [Errno 9] Bad file descriptor`。

- 错误写法：`open(task_file, "r")` — 只读打开，加排他锁失败
- 正确写法：`open(task_file, "r+")` — 读写打开，加排他锁成功

该 bug 表现为 Worker 心跳正常更新、pending 队列有任务但不消费（锁失败被静默捕获，无日志输出）。
