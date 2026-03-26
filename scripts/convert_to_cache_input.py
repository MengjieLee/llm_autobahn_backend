#!/usr/bin/env python3
"""
将 s2_result (tokenize_script.py 的输出) 转换为 cache_calc 需要的 input_ids.txt 格式

输入格式 (s2_result JSON):
[
  {"as_id": "...", "input_ids": [123, 456, ...], "timestamp": 1234567890, ...},
  ...
]

输出格式 (input_ids.txt):
'input_ids': [123, 456, 789]
或带时间戳:
2025-02-26 14:30:00 'input_ids': [123, 456, 789]

用法:
    python convert_to_cache_input.py \
        --input /path/to/s2_result.json \
        --output /path/to/input_ids.txt

示例:
    python convert_to_cache_input.py \
        --input /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/olap_database/s2_result/manual_extra_schema_input_ids.json \
        --output /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/src/domains/kv/cache_hit_rate/input_ids.txt
"""

import json
import argparse
import os
from datetime import datetime


def timestamp_to_str(ts):
    """将时间戳转换为字符串格式"""
    if ts is None or ts == 0:
        return None
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        return None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="将 s2_result 转换为 cache_calc 输入格式")
    parser.add_argument("--input", "-i", required=True, help="输入文件路径 (s2_result JSON)")
    parser.add_argument("--output", "-o", required=True, help="输出文件路径 (input_ids.txt)")
    parser.add_argument("--with-timestamp", "-t", action="store_true", help="是否包含时间戳")
    parser.add_argument("--sort-by-timestamp", "-s", action="store_true", help="按时间戳排序")
    parser.add_argument("--limit", "-l", type=int, default=0, help="限制输出记录数 (0=不限制)")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] 输入文件不存在: {args.input}")
        return 1

    print(f"[INFO] 输入文件: {args.input}")
    print(f"[INFO] 输出文件: {args.output}")

    # 读取输入
    print(f"[INFO] 正在读取输入文件...")
    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 如果是我们的 ES 结果格式，data 可能在 "data" 字段里
    if isinstance(data, dict) and "data" in data:
        records = data["data"]
    elif isinstance(data, list):
        records = data
    else:
        print(f"[ERROR] 无法识别的输入格式")
        return 1

    print(f"[INFO] 共 {len(records)} 条记录")

    # 提取 input_ids 和时间戳
    items = []
    for r in records:
        input_ids = r.get("input_ids", [])
        if not input_ids:
            continue
        ts = r.get("timestamp", 0)
        items.append((ts, input_ids))

    print(f"[INFO] 有效记录: {len(items)} 条")

    # 按时间戳排序
    if args.sort_by_timestamp:
        items.sort(key=lambda x: x[0] if x[0] else 0)
        print(f"[INFO] 已按时间戳排序")

    # 限制数量
    if args.limit > 0:
        items = items[:args.limit]
        print(f"[INFO] 限制输出前 {args.limit} 条")

    # 创建输出目录
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 写入输出
    print(f"[INFO] 正在写入输出文件...")
    with open(args.output, 'w', encoding='utf-8') as f:
        for i, (ts, input_ids) in enumerate(items):
            ids_str = ", ".join(str(tid) for tid in input_ids)

            if args.with_timestamp:
                ts_str = timestamp_to_str(ts)
                if ts_str:
                    f.write(f"{ts_str} 'input_ids': [{ids_str}]\n")
                else:
                    f.write(f"'input_ids': [{ids_str}]\n")
            else:
                f.write(f"'input_ids': [{ids_str}]\n")

            if (i + 1) % 100000 == 0:
                print(f"[INFO] 已写入 {i + 1} 条...")

    print(f"[INFO] 完成! 共写入 {len(items)} 条记录")
    print(f"[INFO] 输出文件: {args.output}")

    return 0


if __name__ == "__main__":
    exit(main())
