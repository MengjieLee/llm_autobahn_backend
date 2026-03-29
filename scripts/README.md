# KV Cache 命中率分析 — Pipeline 脚本

基于线上 ES 日志数据，模拟分析 KV Cache 的前缀缓存命中率。

## 数据流

```
ES 日志
  │
  ▼  Stage 1: 数据采集 (API 自动触发)
olap_database/data/{username}/{task_id}/kv_*.jsonl   ← 原始 ES 记录（按小时分片）
  │
  ▼  Stage 2: Token 序列化 (kv_pipeline.py → tokenize_script.py + convert_to_cache_input.py)
olap_database/data/{username}/{task_id}/tokenized/*_input_ids.txt
  │
  ▼  Stage 3: 缓存模拟 (cache_pipeline.py → cache_simulation.py → cache_calc)
olap_database/data/{username}/{task_id}/report/cache_report.json
```

全流程由 `app/api/v1/olap.py` 的 `_run_pipeline` 自动编排：fetch → tokenize → simulate。

---

## 目录结构

```
scripts/                           ← 被 API 引用的 pipeline 脚本
├── kv_pipeline.py                 # 入口: tokenize + convert（多文件并发）
├── cache_pipeline.py              # 入口: merge + simulate
├── tokenize_script.py             # ES 记录 → input_ids（HuggingFace chat_template）
├── convert_to_cache_input.py      # input_ids JSON → TXT (cache_calc 输入格式)
└── cache_simulation.py            # 调用 cache_calc 二进制并生成报告

local_workspace/                   ← 未被 API 引用的独立工具脚本
├── legacy_tokenize_script.py      # tokenize_script.py 重写前的备份
├── raw_to_inputids.py             # 原型调试脚本
└── filter_by_model.py             # 按模型筛选原始数据

src/domains/kv/
├── svc.py                         # ES 查询服务 (时间窗口拆分、跨日期)
├── impl.py                        # ES 底层客户端 (scroll + clear_scroll)
└── cache_hit_rate/
    ├── cache_calc.cpp              # C++ LRU 缓存模拟器
    ├── lru.h                       # LRU 模板实现
    ├── hash_util.h                 # FNV-1a block 哈希 (支持前缀链式哈希)
    └── Makefile                    # make clean && make

olap_database/
├── data/{username}/{task_id}/     # 任务数据（fetch/tokenized/report）
└── status/{username}/{task_id}.json  # 任务状态文件
```

---

## 配置

所有 pipeline 参数集中在 `app/conf/config.py` 的 `Settings` 类中，支持 `.env` 或环境变量覆盖：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `ES_HOST` | `http://10.178.220.13:8200` | ES 数据源地址 |
| `ES_INDEX_PREFIX` | `as-qianfan-online_` | ES 索引前缀 |
| `ES_WINDOW_MINUTES` | `1` | 单次 scroll 查询窗口 (分钟) |
| `ES_DEFAULT_APP_ID` | `app-3Lut8O2E` | 默认应用 ID |
| `PIPELINE_DEFAULT_MODEL` | `glm-5` | 默认 tokenizer 模型 |
| `PIPELINE_BLOCK_SIZE` | `16` | cache_calc block 大小 |
| `PIPELINE_CACHE_SIZE` | `200000000` | cache_calc 缓存容量 |
| `PIPELINE_TOKENIZE_CONCURRENCY` | `4` | tokenize 阶段最大并发数 |
| `PIPELINE_DEFAULT_PATH` | `/v2/coding/chat/completions` | 默认场景过滤路径 |
| `OLAP_QPD_LIMIT` | `3` | 非 official 用户每日提交限额 |

---

## 快速开始

### 方式 A: API 驱动（推荐）

```bash
# 1. 提交任务（自动执行 fetch → tokenize → simulate 全流程）
GET /api/v1/olap/kv/fetch?start_datetime=2026-03-25 12:00:00&end_datetime=2026-03-25 13:00:00&app_id=app-3Lut8O2E

# 2. 查询任务列表
GET /api/v1/olap/kv/tasks?username=v_limengjie03

# 3. 查询单个任务状态
GET /api/v1/olap/kv/status/{task_id}
```

### 方式 B: CLI 驱动

```bash
# Stage 2: tokenize + convert（多文件并发）
python scripts/kv_pipeline.py \
    -i olap_database/data/{username}/{task_id}/kv_*.jsonl \
    -o olap_database/data/{username}/{task_id}/tokenized \
    -d glm-5 \
    -w 4

# Stage 3: 缓存模拟
python scripts/cache_pipeline.py \
    -i olap_database/data/{username}/{task_id}/tokenized/*_input_ids.txt \
    -o olap_database/data/{username}/{task_id}/report \
    -s 200000000 \
    -b 16
```

---

## 脚本详情

### kv_pipeline.py — Token 序列化流水线

整合 tokenize + convert，支持多文件并发。被 `olap.py` 的 `_run_tokenize_single_file` 调用。

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --input` | 输入文件 (支持多个) | 必填 |
| `-o, --output-dir` | 输出目录 | 必填 |
| `-d, --default-model` | 默认模型 | `glm-5` |
| `-t, --override-tokenizer` | 强制指定 tokenizer | 无 |
| `-w, --workers` | 并发度 | `4` |

### cache_pipeline.py — 缓存模拟流水线

合并多个 input_ids.txt 后执行缓存模拟。被 `olap.py` 的 `_run_simulate_stage` 调用。

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --input` | input_ids.txt 文件 (支持多个) | 必填 |
| `-o, --output-dir` | 输出目录 | 必填 |
| `-s, --cache-sizes` | 缓存大小 | `200000000` |
| `-b, --block-size` | Block 大小 (token 数) | `16` |

### tokenize_script.py — Token 序列化

将 ES 记录中的 messages 通过 `apply_chat_template` 转为 input_ids。

**模型选择优先级**: `--override-tokenizer` > `@raw.qianfan_model` > `body.model` > `--default-model`

**模型映射**:

| 模型 | Tokenizer |
|------|-----------|
| `kimi-k2.5` | `moonshotai/Kimi-K2.5` |
| `glm-5` | `zai-org/GLM-5` |
| `minimax-m2.5` | `MiniMaxAI/MiniMax-M2.5` |
| `deepseek-v3.2` | `deepseek-ai/DeepSeek-V3.2` |
| `glm-4.7` | `zai-org/GLM-4.7` |
| `minimax-m2.1` | `MiniMaxAI/MiniMax-M2.1` |
| 默认 fallback | `zai-org/GLM-5` |

**关键特性**:
- 使用 HuggingFace `transformers` 的 `apply_chat_template` (不依赖 tiktoken)
- 深度清洗 messages: 修复 `tool_calls`/`arguments` 被序列化为字符串的问题
- 兼容多种返回类型 (list/Tensor/BatchEncoding/Encoding)

---

## cache_calc 算法

### Block 哈希

1. 将 token 序列按 `block_size` 切分为多个 block
2. 每个 block 用 FNV-1a 计算 hash
3. **前缀哈希 (默认开启)**: `hash(block_i) = FNV(hash(block_{i-1}), tokens_i)`

### LRU 缓存模拟

```
对每条请求:
  拆分为 N 个 block → 计算 N 个 block_hash
  对每个 hash:
    如果在 LRU 中 → hit_count++
    否则 → 插入 LRU，超 capacity 时淘汰最久未使用的
```

**命中率 = hit_count / total_adds** (0~1 比率)

### block_size 的影响

| block_size | 行为 | 适用场景 |
|------------|------|----------|
| `200000000` | 每条请求 1 个 block，仅完全相同的请求命中 | 请求级去重分析 |
| `64` | 每条请求 ~1500 个 block，公共前缀可命中 | 前缀缓存模拟 |
| `16` | 更细粒度的前缀匹配 | **当前默认配置** |

---

## 环境依赖

```bash
pip install transformers   # HuggingFace tokenizer
pip install elasticsearch  # ES 客户端

cd src/domains/kv/cache_hit_rate && make clean && make
```

---

## 常见问题

**Q: 命中率为 0？**
`block_size` 太大，每条请求只有 1 个 block → 不同请求 hash 不同。改用小 `block_size` (16/64)。

**Q: tokenize 全部失败？**
环境缺少 `transformers`: `pip install transformers`

**Q: ES 查询报 2GB 错误？**
调小 `ES_WINDOW_MINUTES` (默认已为 1 分钟)。

**Q: ES 查询报 max_open_scroll_context 500？**
scroll 上下文堆积，`impl.py` 已在 `finally` 中自动 `clear_scroll`。

**Q: hit_rate 是百分比还是比率？**
`cache_calc` 输出 0~1 比率，报告中 `hit_rate_percent` = `hit_rate * 100`。
