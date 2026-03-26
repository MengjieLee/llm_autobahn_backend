#!/usr/bin/env python3
"""
KV Cache 缓存模拟流水线

整合两个步骤：
1. 合并多个 input_ids.txt 为一个
2. 运行缓存模拟并生成报告 (cache_simulation.py)

支持多个 input_ids.txt 文件输入，按 timestamp 排序后合并，
再执行 cache_calc 模拟并生成结构化报告。

用法:
    python cache_pipeline.py -i file1.txt file2.txt -o /path/to/output_dir

示例:
    python cache_pipeline.py \
        -i olap_database/task_xxx/output/kv_20260323_180000_20260323_190000_input_ids.txt \
           olap_database/task_xxx/output/kv_20260323_190000_20260323_200000_input_ids.txt \
        -o olap_database/task_xxx/report \
        --cache-sizes 16,100,500,1000,0 \
        --block-size 200000000
"""

import argparse
import json
import os
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Any


# ============================================================
# 路径配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
CACHE_CALC_PATH = os.path.join(BASE_DIR, "src/domains/kv/cache_hit_rate/cache_calc")


# ============================================================
# Step 1: 合并多个 input_ids.txt
# ============================================================

def merge_input_files(input_files: List[str], merged_file: str) -> int:
    """
    合并多个 input_ids.txt 到一个文件，保持各文件内部顺序，按文件名排序拼接。

    Returns: 合并后的总行数
    """
    # 按文件名排序，确保时间顺序（文件名含时间戳）
    sorted_files = sorted(input_files, key=lambda f: os.path.basename(f))

    total_lines = 0
    with open(merged_file, 'w', encoding='utf-8') as f_out:
        for input_file in sorted_files:
            print(f"  合并: {os.path.basename(input_file)}")
            with open(input_file, 'r', encoding='utf-8') as f_in:
                for line in f_in:
                    line = line.rstrip('\n')
                    if line:
                        f_out.write(line + '\n')
                        total_lines += 1

    return total_lines


# ============================================================
# Step 2: 运行缓存模拟 (复用 cache_simulation.py 的逻辑)
# ============================================================

def run_simulation(
    input_file: str,
    output_file: str,
    cache_sizes: List[int],
    block_size: int,
    cache_calc_path: str = CACHE_CALC_PATH
) -> Dict[str, Any]:
    """
    调用 cache_simulation.py 运行模拟并生成报告
    """
    cache_sizes_str = ",".join(str(s) for s in cache_sizes)
    cmd = [
        "python", os.path.join(SCRIPTS_DIR, "cache_simulation.py"),
        "-i", input_file,
        "-o", output_file,
        "-s", cache_sizes_str,
        "-b", str(block_size),
        "--cache-calc", cache_calc_path
    ]

    print(f"  执行: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        line = line.strip()
        if line:
            print(f"  {line}")

    process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"cache_simulation.py 返回码: {process.returncode}")

    # 读取生成的报告
    with open(output_file, 'r', encoding='utf-8') as f:
        return json.load(f)


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="KV Cache 缓存模拟流水线 (合并 + 模拟 + 报告)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 单文件
    python cache_pipeline.py -i input_ids.txt -o report_dir/

    # 多文件合并后模拟
    python cache_pipeline.py \
        -i file1_input_ids.txt file2_input_ids.txt \
        -o report_dir/ \
        --cache-sizes 16,100,500,1000,0
        """
    )

    parser.add_argument("--input", "-i", nargs="+", required=True,
                        help="input_ids.txt 文件路径，支持多个文件")
    parser.add_argument("--output-dir", "-o", required=True,
                        help="输出目录")
    parser.add_argument("--cache-sizes", "-s", default="16",
                        help="缓存大小列表，逗号分隔 (默认: 16)")
    parser.add_argument("--block-size", "-b", type=int, default=200000000,
                        help="Block 大小 (token 数，默认: 200000000)")
    parser.add_argument("--cache-calc", default=CACHE_CALC_PATH,
                        help="cache_calc 可执行文件路径")

    args = parser.parse_args()

    # 校验输入文件
    input_files = []
    for f in args.input:
        abs_path = os.path.abspath(f)
        if not os.path.exists(abs_path):
            print(f"[ERROR] 输入文件不存在: {abs_path}")
            return 1
        input_files.append(abs_path)

    # 校验 cache_calc
    if not os.path.exists(args.cache_calc):
        print(f"[ERROR] cache_calc 不存在: {args.cache_calc}")
        return 1

    # 解析参数
    cache_sizes = [int(s.strip()) for s in args.cache_sizes.split(",")]
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    pipeline_start = datetime.now()

    print(f"\n{'='*60}")
    print(f"KV Cache 缓存模拟流水线")
    print(f"{'='*60}")
    print(f"输入文件: {len(input_files)} 个")
    for f in input_files:
        print(f"  - {os.path.basename(f)}")
    print(f"输出目录: {output_dir}")
    print(f"缓存大小: {cache_sizes}")
    print(f"Block 大小: {args.block_size}")
    print(f"{'='*60}")

    # ---- Step 1: 合并 ----
    need_merge = len(input_files) > 1
    if need_merge:
        merged_file = os.path.join(output_dir, "merged_input_ids.txt")
        print(f"\nStep 1/2: 合并 {len(input_files)} 个文件...")
        step1_start = datetime.now()

        total_lines = merge_input_files(input_files, merged_file)
        step1_duration = round((datetime.now() - step1_start).total_seconds(), 2)

        print(f"Step 1/2 完成 ({step1_duration}s): {total_lines} 行 -> {merged_file}")
        simulation_input = merged_file
    else:
        print(f"\n单文件输入，跳过合并步骤")
        simulation_input = input_files[0]
        total_lines = sum(1 for _ in open(simulation_input, 'r'))

    # ---- Step 2: 缓存模拟 + 报告 ----
    step_label = "Step 2/2" if need_merge else "Step 1/1"
    report_file = os.path.join(output_dir, "cache_report.json")
    print(f"\n{step_label}: 缓存模拟...")
    step2_start = datetime.now()

    try:
        report = run_simulation(
            input_file=simulation_input,
            output_file=report_file,
            cache_sizes=cache_sizes,
            block_size=args.block_size,
            cache_calc_path=args.cache_calc
        )
        step2_duration = round((datetime.now() - step2_start).total_seconds(), 2)
        print(f"{step_label} 完成 ({step2_duration}s) -> {report_file}")

    except Exception as e:
        print(f"[ERROR] 缓存模拟失败: {e}")
        return 1

    # ---- 汇总 ----
    total_duration = round((datetime.now() - pipeline_start).total_seconds(), 2)

    print(f"\n{'='*60}")
    print(f"流水线执行完成 ({total_duration}s)")
    print(f"{'='*60}")
    print(f"总请求数: {report['summary']['total_entries']}")
    print(f"总 Token 数: {report['summary']['total_tokens']}")
    print(f"平均 Token/请求: {report['summary']['avg_tokens_per_entry']}")
    print(f"{'-'*60}")
    print(f"{'Cache Size':<15} {'命中数':<12} {'命中率':<12}")
    print(f"{'-'*60}")
    for r in report.get("results", []):
        print(f"{r['cache_size_readable']:<15} {r['hit_count']:<12} {r['hit_rate_percent']}%")
    print(f"{'-'*60}")
    print(f"建议: {report.get('analysis', {}).get('recommendation', 'N/A')}")
    print(f"{'='*60}")
    print(f"\n输出文件:")
    if need_merge:
        print(f"  - 合并文件: {merged_file}")
    print(f"  - 报告: {report_file}")

    return 0


if __name__ == "__main__":
    exit(main())
