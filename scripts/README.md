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
                  └──────────────────────────────┘
```

### 调用链路

```
olap.py (_run_pipeline)
  │
  ├─ Stage 1: ESIndexService.query_to_file()
  │     └→ ES scroll → kv_xxx.jsonl
  │
  ├─ Stage 2: subprocess kv_pipeline.py
  │     └→ subprocess tokenize_script.py
  │           └→ multiprocessing.Pool → per-model _input_ids.txt
  │
  └─ Stage 3: subprocess cache_pipeline.py
        ├→ merge_input_files() → merged_input_ids.txt
        └→ subprocess cache_simulation.py
              └→ subprocess cache_calc (C++) → cache_report.json
```

---

## 三层并发模型

| 层级 | 配置项 | 作用 | 默认值 |
|------|--------|------|--------|
| **fetch 并发** | `pipeline_fetch_concurrency` | 同时拉取 ES 的切片数（asyncio Semaphore） | 12 |
| **tokenize 并发** | `pipeline_tokenize_concurrency` | 同时做序列化的切片数（asyncio Semaphore） | 4 |
| **tokenize 多进程** | `pipeline_tokenize_workers` | 单切片内的 CPU 并行 worker 数（multiprocessing Pool） | 4 |

系统 CPU 峰值负载 ≈ `tokenize_concurrency × tokenize_workers`（如 4 × 4 = 16 核）。

所有配置通过 `app/conf/olap_config.json` **热更新**，修改即生效，无需重启服务。

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

### 6. convert_to_cache_input.py — 格式转换（已废弃）

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
│   └── report/                                           ← Stage 3 产出
│       ├── glm-5/
│       │   ├── merged_input_ids.txt
│       │   └── cache_report.json
│       └── deepseek-v3.2/
│           ├── merged_input_ids.txt
│           └── cache_report.json
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
  "pipeline_fetch_concurrency": 12,
  "pipeline_es_scroll_workers": 60,
  "pipeline_tokenize_workers": 4,
  "pipeline_tokenize_batch_size": 200,
  "pipeline_default_path": "/v2/coding/chat/completions",
  "olap_qpd_limit": 3,
  "models": ["glm-5", "glm-4.7", "deepseek-v3.2", "kimi-k2.5", "minimax-m2.5", "minimax-m2.1"]
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
| `pipeline_tokenize_workers` | 单切片的多进程 worker 数 |
| `pipeline_tokenize_batch_size` | worker 每批处理记录数 |
| `pipeline_default_path` | ES 查询的 path 过滤默认值 |
| `olap_qpd_limit` | 非 official 用户每日提交限额 |
| `models` | 前端展示的可选模型列表 |

---

## 脚本一览

| 脚本 | 角色 | 被谁调用 |
|------|------|---------|
| `tokenize_script.py` | Token 序列化（核心 CPU 计算） | `kv_pipeline.py` |
| `kv_pipeline.py` | 序列化调度 + 模型过滤 + 汇总 | `olap.py` Stage 2 |
| `cache_pipeline.py` | 合并 txt + 调度模拟 | `olap.py` Stage 3 |
| `cache_simulation.py` | 调用 cache_calc + 生成 JSON 报告 | `cache_pipeline.py` |
| `es_model_stats.py` | ES 模型分布统计 | 手动运行（独立工具） |
| `convert_to_cache_input.py` | JSON → txt 格式转换 | 已废弃，不再使用 |

---

## 快速开始

### 方式 A：API 驱动（推荐）

```bash
# 提交任务（全自动 fetch → tokenize → simulate）
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

## 常见问题

**Q: 命中率为 0？**
`block_size` 过大（如 2 亿），每条请求只有 1 个 block，不同请求 hash 不同。改用小 `block_size`（16 或 64）。

**Q: tokenize 全部失败？**
检查 `transformers` 是否安装，HuggingFace 模型是否可访问（需 `HF_TOKEN` 环境变量）。

**Q: ES 查询报 2GB 错误？**
系统已内置自适应降档：60s → 30s → 15s → 5s 窗口自动重试。

**Q: hit_rate 是百分比还是比率？**
`cache_calc` 输出 0~1 比率，报告中 `hit_rate_percent` = `hit_rate × 100`。
