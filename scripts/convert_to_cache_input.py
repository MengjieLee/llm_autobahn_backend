#!/usr/bin/env python3
"""
将 s2_result (tokenize_script.py 的输出) 转换为 cache_calc 需要的 input_ids.txt 格式

输入格式 (tokenize_script.py 产出的 JSON 数组，每行一个对象):
[
{"as_id": "...", "input_ids": [123, 456, ...], "timestamp": 1234567890, ...},
{"as_id": "...", "input_ids": [789, ...], "timestamp": 1234567891, ...}
]

输出格式 (input_ids.txt):
'input_ids': [123, 456, 789]

流式处理：逐行读取 JSON 数组中的对象，避免全量加载到内存。
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
    parser.add_argument("--limit", "-l", type=int, default=0, help="限制输出记录数 (0=不限制)")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] 输入文件不存在: {args.input}")
        return 1

    print(f"[INFO] 输入文件: {args.input}")
    print(f"[INFO] 输出文件: {args.output}")

    # 创建输出目录
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 流式读取 JSON 数组：逐行解析，跳过数组括号
    # tokenize_script.py 产出格式: [\n{...},\n{...}\n]
    written = 0
    skipped = 0
    print(f"[INFO] 流式处理中...")

    with open(args.input, 'r', encoding='utf-8') as fin, \
         open(args.output, 'w', encoding='utf-8') as fout:
        for line in fin:
            line = line.strip().rstrip(',')
            if not line or line in ('[', ']'):
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            input_ids = record.get("input_ids", [])
            if not input_ids:
                skipped += 1
                continue

            ids_str = ", ".join(str(tid) for tid in input_ids)

            if args.with_timestamp:
                ts = record.get("timestamp", 0)
                ts_str = timestamp_to_str(ts)
                if ts_str:
                    fout.write(f"{ts_str} 'input_ids': [{ids_str}]\n")
                else:
                    fout.write(f"'input_ids': [{ids_str}]\n")
            else:
                fout.write(f"'input_ids': [{ids_str}]\n")

            written += 1

            if args.limit > 0 and written >= args.limit:
                break

            if written % 100000 == 0:
                print(f"[INFO] 已写入 {written} 条...")

    print(f"[INFO] 完成! 共写入 {written} 条记录，跳过 {skipped} 条")
    print(f"[INFO] 输出文件: {args.output}")

    return 0


if __name__ == "__main__":
    exit(main())
