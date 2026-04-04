# OLAP KV Cache 分析 Pipeline — 脚本说明

基于线上 ES 日志数据，模拟分析 KV Cache 的前缀缓存命中率，为缓存容量规划提供数据支撑。

## 整体流程

```
                          ┌──────────────┐
                          │ 用户提交任务    │
                          │(时间范围/模型) │
                          └──────┬───────┘
                                 │
                          ┌──────▼───────┐
                          │   olap.py    │  FastAPI 后端，自动编排全流程
                          │  按小时拆切片  │  每小时一个 .jsonl 文件
                          └──────┬───────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
   ┌─────▼─────┐          ┌─────▼─────┐          ┌─────▼─────┐
   │  切片 0    │          │  切片 1    │   ...    │  切片 23   │
   └─────┬─────┘          └─────┬─────┘          └─────┬─────┘
         │                       │                       │
═════════╪═══ Stage 1: Fetch ════╪═══════════════════════╪══════════
         │                       │                       │
         ▼                       ▼                       ▼
   ES scroll 查询          ES scroll 查询          ES scroll 查询
   (最多 12 路并发)
         │                       │                       │
         ▼                       ▼                       ▼
   kv_0.jsonl              kv_1.jsonl              kv_23.jsonl
         │                       │                       │
═════════╪═══ Stage 2: Tokenize ═╪═══════════════════════╪══════════
         │   (fetch 完成一个即触发，流式重叠)              │
         ▼                       ▼                       ▼
   kv_pipeline.py          kv_pipeline.py          kv_pipeline.py
   (最多 4 路并发)
         │                       │                       │
         ▼                       ▼                       ▼
   tokenize_script.py      tokenize_script.py      tokenize_script.py
   (内部 Pool=4 多进程)
         │                       │                       │
         ▼                       ▼                       ▼
   per-model 分桶:          per-model 分桶:          per-model 分桶:
   ├ glm-5.txt             ├ glm-5.txt             ├ glm-5.txt
   ├ deepseek-v3.2.txt     ├ deepseek-v3.2.txt     ├ deepseek-v3.2.txt
   └ ...                   └ ...                   └ ...
         │                       │                       │
═════════╪═══ Stage 3: Simulate ═╪═══════════════════════╪══════════
         │   (全部 tokenize 完成后，按 model 聚合)        │
         └───────────────────────┼───────────────────────┘
                                 │
                  ┌──────────────▼──────────────┐
                  │  cache_pipeline.py (per model)│
                  │                              │
                  │  Step 1: 合并所有切片的 txt    │
                  │  (二进制读写+缓存拷贝，快速 I/O)│
                  │                              │
                  │  Step 2: cache_simulation.py  │
                  │    └→ cache_calc (C++ LRU)    │
                  │                              │
                  │  产出: cache_report.json      │
                  └──────────────┬───────────────┘
                                 │
═════════════════════════════════╪══ Stage 4: Trend ══════════════
                                 │
                  ┌──────────────▼──────────────┐
                  │  compute_trend.py            │
                  │                              │
                  │  对每个 model 的每个时间片     │
                  │  独立调用 cache_calc          │
                  │  提取 per-slice hit_rate      │
                  │  → 按时间排序 + 整体聚合       │
                  │                              │
                  │  产出: hit_rate_trend.json    │
                  └──────────────────────────────┘
```

### 调用链路

```
olap.py (_run_pipeline) / run_pipeline.py
  │
  ├─ Stage 1: ESIndexService.query_to_file()
  │     └→ ES scroll → kv_xxx.jsonl
  │
  ├─ Stage 2: kv_pipeline.py (TokenizeDaemonClient)
  │     └→ tokenize_script.py (multiprocessing.Pool)
  │           └→ per-model _input_ids.txt
  │
  ├─ Stage 3: subprocess cache_pipeline.py
  │     ├→ merge_input_files() → merged_input_ids.txt
  │     └→ subprocess cache_simulation.py
  │           └→ subprocess cache_calc (C++) → cache_report.json
  │
  └─ Stage 4: compute_trend.py (ThreadPoolExecutor)
        └→ 对每个 model 的每个时间片调用 cache_calc
              └→ hit_rate_trend.json
```

---

## 三层并发模型

| 层级 | 配置项 | 作用 | 默认值 |
|------|--------|------|--------|
| **fetch 并发** | `pipeline_fetch_concurrency` | 同时拉取 ES 的切片数（asyncio Semaphore） | 24 |
| **tokenize 并发** | `pipeline_tokenize_concurrency` | 同时做序列化的切片数（asyncio Semaphore） | 4 |
| **tokenize 多进程** | `pipeline_tokenize_workers` | 单切片内的 CPU 并行 worker 数（multiprocessing Pool） | 7 |

系统 CPU 峰值负载 ≈ `tokenize_concurrency × tokenize_workers`（如 4 × 7 = 28 核）。

所有配置通过 `app/conf/olap_config.json` **热更新**，修改即生效，无需重启服务。

### 并发调优注意事项

K8s Job Pod 的工作目录挂载在 **CFS（云文件存储）**上，每次 `read()`/`write()` 都经过网络往返（延迟 1-5ms，带宽约 200-500 MB/s）。
tokenize 阶段的 I/O 模式是**主进程串行读写 CFS** + worker 并行 CPU 计算：

```
主进程: open(jsonl, CFS) → 逐行 read → 攒 batch → 分发 worker
worker: 纯 CPU（tokenize）→ 返回 result 给主进程
主进程: 收集结果 → write(txt, CFS)
```

因此：
- **`tokenize_concurrency`（文件级并发）是主要调速旋钮**：决定同时有多少个切片在做 I/O + 计算
- **`tokenize_workers`（单文件 worker 数）加到超过 CPU 核 / concurrency 后收益递减**：worker 产出快于主进程串行收集+写盘的速度
- 不建议 `workers` 设得过大（如 12+），会导致 worker 空等主进程、内存上涨，反而变慢

---

## 脚本详解

### 1. tokenize_script.py — Token 序列化（核心计算）

将 ES 导出的 JSONL 日志转换为 `input_ids` 序列，按模型自动分桶输出。

**输入**：`.jsonl` 文件，每行是 ES 的一条完整记录（含 `_source.@raw`、`_source.@timestamp`）。

**输出**：per-model 的 `{prefix}_{model}_input_ids.txt`，每行格式：
```
'input_ids': [101, 2003, 5567, 8899, 1234, 5678]
```

**核心流程**：

```
       JSONL 输入（按 @timestamp 升序）
              │
    ┌─────────▼─────────┐
    │  主进程逐行读取     │
    │  攒满 batch (200条) │
    └─────────┬─────────┘
              │  apply_async
    ┌─────────┼─────────┐─────────┐
    ▼         ▼         ▼         ▼
 Worker-0  Worker-1  Worker-2  Worker-3   ← multiprocessing.Pool
 (batch0)  (batch1)  (batch2)  (batch3)
    │         │         │         │
    │  对每条记录:
    │  1. 提取 model：qianfan_model > body.model > default
    │  2. 查 MODEL_TOKENIZER_MAPPING → HuggingFace tokenizer
    │  3. apply_chat_template(messages, tools) → input_ids
    │         │         │         │
    └─────────┼─────────┘─────────┘
              │
    ┌─────────▼─────────┐
    │  主进程按提交顺序   │  ← 保证时间序不乱
    │  收集 → 分桶写 txt  │
    └───────────────────┘
```

**模型到 Tokenizer 映射**：

| 模型名 | HuggingFace Tokenizer |
|--------|----------------------|
| `glm-5` | `zai-org/GLM-5` |
| `glm-4.7` | `zai-org/GLM-4.7` |
| `deepseek-v3.2` | `deepseek-ai/DeepSeek-V3.2` |
| `kimi-k2.5` | `moonshotai/Kimi-K2.5` |
| `minimax-m2.5` | `MiniMaxAI/MiniMax-M2.5` |
| `minimax-m2.1` | `MiniMaxAI/MiniMax-M2.1` |
| 兜底 | `zai-org/GLM-5` |

**模型选择优先级**：`--override-tokenizer` > `@raw` 中的 `qianfan_model` > `body.model` > `--default-model`

**参数说明**：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --input` | 输入 JSONL 文件 | 必填 |
| `-o, --output-dir` | 输出目录 | 必填 |
| `-p, --file-prefix` | 输出文件名前缀 | 从输入文件名推导 |
| `-d, --default-model` | 兜底模型 | `glm-5` |
| `-t, --override-tokenizer` | 强制指定 tokenizer | 无 |
| `-W, --workers` | 多进程 worker 数（0=自动） | 0 |
| `-B, --batch-size` | 每 batch 记录数 | 200 |
| `-l, --limit` | 限制处理记录数 | 0（不限） |
| `-v, --verbose` | 详细错误输出 | 关闭 |

**用法示例**：
```bash
python scripts/tokenize_script.py \
    -i kv_20260328_000000_20260328_010000.jsonl \
    -o ./output/ -p kv_20260328_000000_20260328_010000 \
    -d glm-5 -W 4 -B 200
```

**关键设计**：
- **多进程并行 (方案A)**：tokenize 是 CPU 密集操作，由 `multiprocessing.Pool` 分发到多核并行。
- **直接输出 txt (方案B)**：跳过中间 JSON 格式，直接输出 `cache_calc` 需要的 `'input_ids': [...]` 格式。
- **顺序保证**：主进程按 batch 提交顺序收集结果，batch 内按原始行号排序写出，确保 ES `@timestamp asc` 时间序不变。

---

### 2. kv_pipeline.py — 序列化调度层

**被谁调用**：`olap.py` 的 `_run_tokenize_single_file()` (subprocess)

**职责**：
1. 对每个 `.jsonl` 调用 `tokenize_script.py` 子进程
2. 按模型过滤（可选 `-m glm-5,deepseek-v3.2`）
3. 统计 per-model 行数
4. 生成 `pipeline_summary.json`

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --input` | 输入 JSONL（支持多个） | 必填 |
| `-o, --output-dir` | 输出目录 | 必填 |
| `-d, --default-model` | 默认模型 | `glm-5` |
| `-m, --models` | 模型过滤列表，逗号分隔 | 无（全部保留） |
| `--tokenize-workers` | 透传给 tokenize_script.py `-W` | 0 |
| `--tokenize-batch-size` | 透传给 tokenize_script.py `-B` | 200 |

**产出**：
```
output_dir/
├── kv_xxx_glm-5_input_ids.txt
├── kv_xxx_deepseek-v3.2_input_ids.txt
└── pipeline_summary.json
```

---

### 3. cache_pipeline.py — 缓存模拟流水线

**被谁调用**：`olap.py` 的 `_run_simulate_stage()` → `_simulate_single_model()` (subprocess)

**两个步骤**：
1. **合并** (`merge_input_files`)：将同一 model 下所有切片的 txt 按文件名排序（含时间戳），逐行合并为 `merged_input_ids.txt`。二进制模式读写 + 缓存拷贝。
2. **模拟** (`run_simulation`)：调用 `cache_simulation.py` → `cache_calc` 执行 LRU 命中计算。

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --input` | input_ids.txt（支持多个） | 必填 |
| `-o, --output-dir` | 输出目录 | 必填 |
| `-s, --cache-sizes` | 缓存大小（block 数量） | `200000000` |
| `-b, --block-size` | block 大小（token/block） | `16` |

**用法示例**：
```bash
python scripts/cache_pipeline.py \
    -i tokenized/kv_0/kv_0_glm-5_input_ids.txt \
       tokenized/kv_1/kv_1_glm-5_input_ids.txt \
    -o report/glm-5/ \
    -s 200000000 -b 16
```

---

### 4. cache_simulation.py — 调用 cache_calc + 报告生成

**被谁调用**：`cache_pipeline.py` 的 `run_simulation()` (subprocess)

调用 C++ 编译的 `cache_calc` 可执行文件，解析其标准输出，生成结构化 JSON 报告。

**报告结构** (`cache_report.json`)：
```json
{
  "summary": {
    "total_entries": 799851,
    "total_tokens": 1234567890,
    "avg_tokens_per_entry": 1543.2
  },
  "results": [{
    "cache_size": 200000000,
    "cache_size_readable": "200.0M",
    "total_queries": 799851,
    "hit_count": 312345,
    "hit_rate": 0.3905,
    "hit_rate_percent": 39.05
  }],
  "analysis": {
    "recommendation": "命中率中等 (39.05%)，缓存效果明显"
  }
}
```

---

### 5. es_model_stats.py — ES 模型分布统计（独立工具）

独立工具脚本，快速查看某时间段内各模型的请求分布，可选过滤导出。

```bash
# 统计分布
python scripts/es_model_stats.py -s "2026-03-28 00:00:00" -e "2026-03-28 01:00:00"

# 按模型过滤导出
python scripts/es_model_stats.py -s "2026-03-28 00:00:00" -e "2026-03-28 01:00:00" \
    -m glm-5,deepseek-v3.2 -o /tmp/filtered.jsonl

# 快速预览（限量 1000 条）
python scripts/es_model_stats.py -s "2026-03-28 00:00:00" -e "2026-03-29 00:00:00" -l 1000
```

---

### 6. compute_trend.py — 分钟级命中率趋势（Stage 4）

**被谁调用**：`run_pipeline.py` 的 `_run_trend_stage()` (asyncio.to_thread)，也可 CLI 独立回填。

**职责**：对每个模型的每个时间片 `input_ids.txt` 独立调用 `cache_calc`，提取 per-slice `hit_rate`，按时间排序后计算整体维度的 mean/max/min 统计，生成趋势曲线数据。

**核心优化**：
- **单一共享 ThreadPoolExecutor**：所有模型的文件统一提交到一个线程池并行处理（默认 max_workers=8）
- **去重复用**：加载已有 `hit_rate_trend.json`，对 `(model, time_label)` 已存在结果跳过 cache_calc 调用，重试/恢复场景只计算增量
- **O(T×M) 聚合**：用 dict 替代嵌套循环计算"整体"维度的平均命中率
- **关键节点日志**：开始/去重统计/每 20 文件进度/完成耗时，通过 `logging.getLogger("compute_trend")` 输出

**两种使用方式**：

```python
# 1) 作为库函数被 pipeline 调用
from scripts.compute_trend import compute_trend
result = compute_trend(task_data_dir, cache_calc_path, cache_size, block_size, model_outputs)

# 2) 命令行回填已完成任务
python scripts/compute_trend.py --status-dir olap_database/status --data-dir olap_database/data
```

| 参数 (CLI) | 说明 | 默认值 |
|------|------|--------|
| `--status-dir` | status 目录 | `olap_database/status` |
| `--data-dir` | data 目录 | `olap_database/data` |
| `--force` | 强制覆盖已有的 trend 文件 | 关闭 |

**产出** (`hit_rate_trend.json`)：
```json
{
  "series": [
    {
      "model": "整体",
      "data": [{"time": "03-28 00:00", "hit_rate": 0.3521}, ...],
      "stats": {"mean": 0.3450, "max": 0.4102, "min": 0.2801}
    },
    {
      "model": "glm-5",
      "data": [{"time": "03-28 00:00", "hit_rate": 0.3805}, ...],
      "stats": {"mean": 0.3750, "max": 0.4200, "min": 0.3100}
    }
  ]
}
```

**日志输出示例**：
```
[INFO] [trend] 开始计算: 6 模型, 144 文件, max_workers=8
[INFO] [trend] 去重: 0 已有结果复用, 144 文件需计算
[INFO] [trend] 进度: 20/144 文件完成
[INFO] [trend] 完成: 6 模型, 144 数据点, 耗时 149.0s (复用 0, 计算 144)
```

---

### 7. run_pipeline.py — K8s Job 独立 Pipeline 入口

**被谁调用**：K8s Job Pod 的 entrypoint，由 `olap.py` 通过 K8s client 提交。

**职责**：在独立 Pod 中运行完整的 4 阶段 Pipeline（fetch → tokenize → simulate → trend），通过 CFS 上的 `status.json` 文件上报进度，FastAPI 端轮询读取。

**支持断点恢复**：
- fetch 已完成 → 跳过 fetch，从 tokenize 恢复
- tokenize 已完成 → 跳过 fetch + tokenize，直接 simulate
- simulate 完成后自动执行 trend 阶段

```bash
python scripts/run_pipeline.py \
    --task-id {task_id} \
    --username {username} \
    --start-datetime "2026-03-28 00:00:00" \
    --end-datetime "2026-03-29 00:00:00" \
    --app-id app-3Lut8O2E \
    --path "/v2/coding/chat/completions" \
    --models "glm-5,deepseek-v3.2"
```

---

### 8. daily_report.py — 每日命中率日报推送

**被谁调用**：crontab 定时执行（每天 10:00）

**职责**：
1. 查找前一天的 `{mm-dd}_全场景_各模型` 任务
2. 汇总各模型命中率数据，生成 Markdown 格式报告
3. 推送到 IM 机器人群

**配置**（`olap_config.json`）：
| 字段 | 说明 |
|------|------|
| `daily_report_im_bot_url` | 日报推送 IM bot URL |
| `daily_report_im_bot_toid` | 日报推送目标群 ID |
| `daily_report_detail_url` | 报告中"查看明细"链接地址 |

```bash
# 手动执行
python scripts/daily_report.py

# crontab（每天上午 10 点）
0 10 * * * cd /path/to/backend && python scripts/daily_report.py
```

---

### 9. convert_to_cache_input.py — 格式转换（已废弃）

旧版步骤：将 tokenize 的 JSON 数组输出转为 txt 格式。当前 `tokenize_script.py` 已直接输出 txt，此脚本不再被 pipeline 调用。

---

## cache_calc 算法说明

### 工作原理

```
对每条请求的 token 序列 [t1, t2, t3, ..., tN]:
  1. 按 block_size 切分: [block_0, block_1, ..., block_M]
  2. 计算每个 block 的前缀哈希:
     hash_0 = FNV-1a(tokens_0)
     hash_1 = FNV-1a(hash_0, tokens_1)     ← 包含前序信息
     hash_2 = FNV-1a(hash_1, tokens_2)
     ...
  3. 查询 LRU 缓存:
     hash 命中 → hit_count++（复用缓存）
     hash 未命中 → 插入 LRU，超 capacity 时淘汰最久未访问的
```

### 前缀哈希

默认开启。每个 block 的 hash 包含前序所有 block 信息，模拟真实 KV Cache 的 attention key 依赖于全部前序 token 的行为。

### block_size 对分析结果的影响

| block_size | 行为 | 适用场景 |
|------------|------|----------|
| 很大 (如 2亿) | 每条请求 ≤1 个 block，仅完全相同的请求命中 | 请求级去重 |
| `64` | 每条请求约数十个 block，公共前缀可命中 | 粗粒度前缀缓存 |
| **`16`** (默认) | 每条请求上百个 block，细粒度前缀匹配 | **前缀缓存模拟** |

### cache_calc 命令行

```bash
# 直接调用（脚本一般不需要手动执行）
./src/domains/kv/cache_hit_rate/cache_calc \
    -f merged_input_ids.txt \
    -s 200000000 \
    -b 16

# 输出格式:
# entries: 799851, tokens: 1234567890
# cache_size: 200000000  total_adds: 799851  hit_count: 312345  hit_rate: 0.3905
```

---

## 数据目录结构

```
olap_database/
├── data/{username}/{task_id}/
│   ├── kv_20260328_000000_20260328_010000.jsonl         ← Stage 1 产出
│   ├── kv_20260328_010000_20260328_020000.jsonl
│   ├── ...
│   ├── tokenized/                                        ← Stage 2 产出
│   │   ├── kv_20260328_000000_20260328_010000/
│   │   │   ├── kv_..._glm-5_input_ids.txt
│   │   │   ├── kv_..._deepseek-v3.2_input_ids.txt
│   │   │   └── pipeline_summary.json
│   │   └── ...
│   └── report/                                           ← Stage 3+4 产出
│       ├── glm-5/
│       │   ├── merged_input_ids.txt
│       │   └── cache_report.json
│       ├── deepseek-v3.2/
│       │   ├── merged_input_ids.txt
│       │   └── cache_report.json
│       └── hit_rate_trend.json                            ← Stage 4 趋势数据
│
└── status/{username}/{task_id}.json                      ← 任务进度状态
```

---

## 配置文件

`app/conf/olap_config.json`（热更新，修改即生效，无需重启）：

```json
{
  "pipeline_default_model": "glm-5",
  "pipeline_block_size": 16,
  "pipeline_cache_size": 200000000,
  "pipeline_tokenize_concurrency": 4,
  "pipeline_fetch_concurrency": 24,
  "pipeline_es_scroll_workers": 20,
  "pipeline_tokenize_workers": 7,
  "pipeline_tokenize_batch_size": 1000,
  "pipeline_es_scroll_size": 10000,
  "pipeline_default_path": "/v2/coding/chat/completions",
  "olap_qpd_limit": 100,
  "models": ["glm-5", "glm-4.7", "deepseek-v3.2", "kimi-k2.5", "minimax-m2.5", "minimax-m2.1"],
  "k8s_enabled": true,
  "k8s_image": "ccr-xxx.baidubce.com/qianfan-data/llm_autobahn_backend:0.2.4",
  "k8s_job_cpu_request": "28",
  "k8s_job_memory_request": "110Gi"
}
```

| 字段 | 说明 |
|------|------|
| `pipeline_default_model` | tokenize 兜底模型 |
| `pipeline_block_size` | cache_calc block 大小 (token/block) |
| `pipeline_cache_size` | cache_calc 缓存容量 (block 数量) |
| `pipeline_tokenize_concurrency` | 同时序列化的切片数 |
| `pipeline_fetch_concurrency` | 同时拉取 ES 的切片数 |
| `pipeline_es_scroll_workers` | ES scroll 线程池大小 |
| `pipeline_es_scroll_size` | ES scroll 单次拉取条数 |
| `pipeline_tokenize_workers` | 单切片的多进程 worker 数 |
| `pipeline_tokenize_batch_size` | worker 每批处理记录数 |
| `pipeline_default_path` | ES 查询的 path 过滤默认值 |
| `olap_qpd_limit` | 非 official 用户每日提交限额 |
| `models` | 前端展示的可选模型列表 |
| `k8s_enabled` | 是否启用 K8s Job 模式 |
| `k8s_image` | K8s Job Pod 镜像地址 |
| `k8s_job_cpu_request` | Pod CPU 资源请求 |
| `k8s_job_memory_request` | Pod 内存资源请求 |
| `k8s_job_ttl_seconds` | Job 自动清理时间（秒） |
| `k8s_cfs_host_path` | CFS 在宿主机上的挂载路径 |
| `k8s_cfs_mount_path` | CFS 在 Pod 内的挂载路径 |
| `k8s_working_dir` | Pod 内工作目录 |
| `namespace` | K8s Job 运行的命名空间 |
| `notify_im_bot_url` | 任务完成通知 IM bot URL |
| `notify_im_bot_toid` | 任务完成通知目标群 ID |
| `daily_report_im_bot_url` | 日报推送 IM bot URL |
| `daily_report_im_bot_toid` | 日报推送目标群 ID |
| `daily_report_detail_url` | 日报中"查看明细"链接地址 |

---

## 脚本一览

| 脚本 | 角色 | 被谁调用 |
|------|------|---------|
| `tokenize_script.py` | Token 序列化（核心 CPU 计算） | `kv_pipeline.py` |
| `kv_pipeline.py` | 序列化调度 + 模型过滤 + 汇总 | `run_pipeline.py` Stage 2 |
| `cache_pipeline.py` | 合并 txt + 调度模拟 | `run_pipeline.py` Stage 3 |
| `cache_simulation.py` | 调用 cache_calc + 生成 JSON 报告 | `cache_pipeline.py` |
| `compute_trend.py` | 分钟级命中率趋势计算 | `run_pipeline.py` Stage 4 / CLI 回填 |
| `run_pipeline.py` | K8s Job 完整 Pipeline 入口 | K8s Job Pod entrypoint |
| `daily_report.py` | 每日命中率日报推送 | crontab 定时执行 |
| `es_model_stats.py` | ES 模型分布统计 | 手动运行（独立工具） |
| `convert_to_cache_input.py` | JSON → txt 格式转换 | 已废弃，不再使用 |

---

## 快速开始

### 方式 A：API 驱动（推荐）

```bash
# 提交任务（全自动 fetch → tokenize → simulate → trend）
GET /api/v1/olap/kv/fetch?start_datetime=2026-03-28 00:00:00&end_datetime=2026-03-29 00:00:00

# 查询任务列表
GET /api/v1/olap/kv/tasks?username=v_limengjie03

# 查询任务状态和结果
GET /api/v1/olap/kv/status/{task_id}
```

### 方式 B：CLI 手动执行

```bash
# Stage 2: 序列化
python scripts/kv_pipeline.py \
    -i olap_database/data/v_xxx/task_xxx/kv_*.jsonl \
    -o olap_database/data/v_xxx/task_xxx/tokenized \
    -d glm-5 --tokenize-workers 4

# Stage 3: 缓存模拟
python scripts/cache_pipeline.py \
    -i olap_database/data/v_xxx/task_xxx/tokenized/kv_*/*_glm-5_input_ids.txt \
    -o olap_database/data/v_xxx/task_xxx/report/glm-5 \
    -s 200000000 -b 16

# 独立工具: 查看 ES 中模型分布
python scripts/es_model_stats.py -s "2026-03-28 00:00:00" -e "2026-03-28 01:00:00"

# Stage 4: 趋势回填（已完成任务批量生成 trend）
python scripts/compute_trend.py --status-dir olap_database/status --data-dir olap_database/data
```

---

## 环境依赖

```bash
pip install transformers    # HuggingFace tokenizer (tokenize_script.py)
pip install elasticsearch   # ES 客户端 (impl.py, es_model_stats.py)

# 编译 C++ 模拟引擎
cd src/domains/kv/cache_hit_rate && make clean && make
```

---

## 前端趋势图交互

命中率趋势图（ECharts 折线图）展示 Stage 4 `hit_rate_trend.json` 的数据。

**视觉规范**：
- **整体线**：紫色 `#7B1FA2`、加粗 3.5px、底层绘制（z=0）
- **模型线**：8 色高对比度色板（蓝/绿/橙/玫红/青/红橙/黄绿/棕），1.5px，带圆点标记
- **阴影面积**：所有线均带渐变阴影填充（整体 22% 透明度，模型 12%）
- **Y 轴**：固定 0-100% 范围

**交互**：
- **点击图例行**：显示/隐藏对应模型的折线（通过 ECharts `legendToggleSelect`）
- **Hover tooltip**：显示该时间点所有可见模型的命中率百分比
- **底部统计表**：Grafana 风格，每行显示 Mean / Max / Min

---

## 常见问题

**Q: 命中率为 0？**
`block_size` 过大（如 2 亿），每条请求只有 1 个 block，不同请求 hash 不同。改用小 `block_size`（16 或 64）。

**Q: tokenize 全部失败？**
检查 `transformers` 是否安装，HuggingFace 模型是否可访问（需 `HF_TOKEN` 环境变量）。

**Q: ES 查询报 2GB 错误？**
系统已内置自适应降档：60s → 30s → 15s → 5s 窗口自动重试。

**Q: hit_rate 是百分比还是比率？**
`cache_calc` 输出 0~1 比率，报告中 `hit_rate_percent` = `hit_rate × 100`。

**Q: 趋势图空白但底部统计有数据？**
已修复。原因是 `loadTrendData` 中 `trendLoading` 在 `renderTrendChart` 之后才设为 false，导致 ECharts canvas 容器尚未渲染到 DOM（被 `v-if="trendLoading"` 遮挡）。修复：在 `nextTick` 之前先关 `trendLoading`。

**Q: tokenize_workers 加大后反而变慢？**
tokenize 阶段 I/O 在主进程串行执行（读 jsonl + 写 txt 到 CFS），worker 只做纯 CPU 计算。workers 过多会导致产出远超主进程消费速度，worker 空等浪费资源。建议 `workers ≈ CPU 核数 / tokenize_concurrency`。
