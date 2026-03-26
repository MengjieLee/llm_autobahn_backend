# 缓存命中率模拟统计工具

该工具用于模拟统计线上 KV-cache 的缓存命中率，支持不同缓存大小、block 大小和哈希策略的对比评估。

## 整体流程

```
原始数据集 (JSONL)
       │
       ▼
 preprocess_jsonl.py  ──▶  input_ids.txt
       │                        │
       │(tokenizer.json)        │
       ▼                        ▼
  分词编码              cache_calc (C++程序)
                                │
                                ▼
                        缓存命中率统计结果
```

## 1. 数据预处理：从数据集解析为 input_ids

### 前置依赖

```bash
pip install tokenizers
```

### 输入数据格式

输入文件为 JSONL 格式，每行一个 JSON 对象，包含 `messages` 数组（标准对话格式）：

```json
{"messages": [{"role": "user", "content": "你好"}, {"role": "assistant", "content": "你好！有什么可以帮你的？"}]}
```

### 运行预处理脚本

```bash
python3 preprocess_jsonl.py \
    --input <输入的JSONL文件> \
    --tokenizer <tokenizer文件> \
    --output input_ids.txt
```

参数说明：
- `--input, -i` 输入的 JSONL 文件路径，默认值 `last_agentic_10_split.jsonl`
- `--tokenizer, -t` tokenizer 词表文件路径，默认值 `tokenizer.json`（目录下已提供 `tokenizer_kimi25.json` 和 `tokenizer_v32.json` 两个词表）
- `--output, -o` 输出文件路径，默认值 `input_ids.txt`

示例：

```bash
python3 preprocess_jsonl.py \
    --input last_agentic_10_split.jsonl \
    --tokenizer tokenizer_kimi25.json \
    --output input_ids.txt
```

### 输出格式

每行一条记录，格式如下：

```
'input_ids': [101, 2003, 5567, 8899, 1234, 5678]
```

如果数据中带有时间戳信息，格式为：

```
2025-02-26 14:30:00 'input_ids': [101, 2003, 5567, 8899]
```

## 2. 编译 C++ 模拟程序

在当前目录执行：

```bash
make
```

编译成功后生成 `cache_calc` 可执行文件。清理构建产物：

```bash
make clean
```

编译要求：`g++`，支持 C++11 标准。

## 3. 运行缓存命中率模拟

### 命令示例

```bash
./cache_calc -f input_ids.txt -b 16 -s 200000000
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-f` | 需要处理的文件，支持指定多个 `-f`，多文件请求会归一后排序一起处理 | 无（必填） |
| `-s` | 最大 block 数量，支持指定多个 `-s`，一次性统计不同缓存大小下的命中率。**`-s 0` 表示无限 block 数量** | 无（必填） |
| `-b` | block 大小（每个 block 包含的 token 数量） | 64 |
| `-t` | 是否处理时间戳信息，`-t 1` 或 `-t true` 启用（多文件按时间戳排序） | 不启用 |
| `-p` | 是否使用前缀哈希算法，`-p 0` 或 `-p false` 关闭 | 启用 |
| `-h` | 显示帮助信息 | - |

### 输出结果

程序在终端打印统计结果，每个缓存大小输出一行：

```
cache_size: 111    total_adds: 50000    hit_count: 25000    hit_rate: 0.5
cache_size: 222    total_adds: 50000    hit_count: 30000    hit_rate: 0.6
cache_size: 0      total_adds: 50000    hit_count: 35000    hit_rate: 0.7
```

字段含义：
- `cache_size` — LRU 缓存最大 block 数量（0 表示无限）
- `total_adds` — 总的 block 查询次数
- `hit_count` — 缓存命中次数
- `hit_rate` — 命中率（hit_count / total_adds）

可重定向到文件保存：

```bash
./cache_calc -f input_ids.txt -s 1000 -s 5000 -s 0 -b 64 > result.txt
```

## 4. 完整使用示例

```bash
# Step 1: 安装 Python 依赖
pip install tokenizers

# Step 2: 预处理数据集，生成 input_ids 文件
python3 preprocess_jsonl.py \
    --input last_agentic_10_split.jsonl \
    --tokenizer tokenizer_kimi25.json \
    --output input_ids.txt

# Step 3: 编译 C++ 程序
make

# Step 4: 运行缓存命中率模拟（对比不同缓存大小）
./cache_calc -f input_ids.txt -s 1000 -s 5000 -s 10000 -s 0 -b 64

# 多文件 + 时间戳排序
./cache_calc -f file1.txt -f file2.txt -s 1000 -s 0 -b 64 -t true

# 关闭前缀哈希
./cache_calc -f input_ids.txt -s 1000 -s 0 -b 64 -p false
```

## 5. 关键算法说明

- **前缀哈希（默认启用）**：每个 block 的哈希值会包含前序所有 block 的信息，即相同 token 序列在不同上下文位置会产生不同的哈希值。这更贴近真实 KV-cache 的行为（attention 的 key 依赖于所有前序 token）。
- **非前缀哈希**：每个 block 独立哈希，仅基于自身 token 内容。
- **LRU 淘汰策略**：当缓存满时，淘汰最久未访问的 block。

## 目录文件说明

| 文件 | 说明 |
|------|------|
| `preprocess_jsonl.py` | 数据预处理脚本，JSONL → input_ids |
| `tokenizer_kimi25.json` | kimi25 模型词表 |
| `tokenizer_v32.json` | v32 模型词表 |
| `cache_calc.cpp` | 缓存命中率模拟主程序 |
| `lru.h` | LRU 缓存实现 |
| `hash_util.h` | FNV-1a block 哈希工具 |
| `Makefile` | 编译配置 |
