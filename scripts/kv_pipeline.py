#!/usr/bin/env python3
"""
KV Cache Token 序列化流水线

整合两个步骤：
1. Token 序列化 (tokenize_script.py)
2. 转换为 cache_calc 格式 (convert_to_cache_input.py)

特性：
- 支持多个输入文件
- 按各自文件名流式保存到 output_dir
- 实时进度和状态更新

用法:
    python kv_pipeline.py -i file1.json file2.json -o /path/to/output_dir

示例:
    python kv_pipeline.py \
        -i olap_database/task_xxx/kv_20260323_180000_20260323_190000.json \
           olap_database/task_xxx/kv_20260323_190000_20260323_200000.json \
        -o olap_database/task_xxx/output
"""

import json
import os
import re
import argparse
import subprocess
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, Optional, List
import uuid


# ============================================================
# 路径配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")


# ============================================================
# 单文件处理器
# ============================================================

def process_single_file(
    input_file: str,
    output_dir: str,
    default_model: str = "glm-5",
    override_tokenizer: str = None,
    file_index: int = 1,
    total_files: int = 1
) -> Dict[str, Any]:
    """
    处理单个输入文件：tokenize -> convert

    输出文件保存到 output_dir，文件名基于输入文件名：
      input: kv_20260323_180000_20260323_190000.json
      step1: kv_20260323_180000_20260323_190000_input_ids.json
      step2: kv_20260323_180000_20260323_190000_input_ids.txt

    Returns: 处理结果 dict
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    tokenized_file = os.path.join(output_dir, f"{base_name}_input_ids.json")
    cache_input_file = os.path.join(output_dir, f"{base_name}_input_ids.txt")

    file_tag = f"[{file_index}/{total_files}]"
    result = {
        "input_file": input_file,
        "tokenized_file": tokenized_file,
        "cache_input_file": cache_input_file,
        "status": "pending",
        "steps": []
    }

    # ---- Step 1: Token 序列化 ----
    print(f"\n{file_tag} Step 1/2: Token 序列化 - {base_name}")
    step1_start = datetime.now()

    try:
        cmd = [
            "python", os.path.join(SCRIPTS_DIR, "tokenize_script.py"),
            "-i", input_file,
            "-o", tokenized_file,
            "-d", default_model
        ]
        if override_tokenizer:
            cmd.extend(["-t", override_tokenizer])

        print(f"  执行: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        success_count = 0
        failed_count = 0

        for line in process.stdout:
            line = line.strip()
            if line:
                print(f"  {line}")

            if "成功:" in line and "失败:" in line:
                match = re.search(r'成功:\s*(\d+).*失败:\s*(\d+)', line)
                if match:
                    success_count = int(match.group(1))
                    failed_count = int(match.group(2))

        process.wait()

        step1_duration = round((datetime.now() - step1_start).total_seconds(), 2)

        if process.returncode != 0:
            raise RuntimeError(f"tokenize_script.py 返回码: {process.returncode}")

        result["steps"].append({
            "name": "tokenize",
            "status": "completed",
            "duration_seconds": step1_duration,
            "output_file": tokenized_file,
            "success_count": success_count,
            "failed_count": failed_count
        })
        print(f"{file_tag} Step 1/2 完成 ({step1_duration}s) -> {tokenized_file}")

    except Exception as e:
        result["steps"].append({
            "name": "tokenize",
            "status": "failed",
            "error": str(e)
        })
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"{file_tag} Step 1/2 失败: {e}")
        return result

    # ---- Step 2: 格式转换 ----
    print(f"\n{file_tag} Step 2/2: 格式转换 - {base_name}")
    step2_start = datetime.now()

    try:
        cmd = [
            "python", os.path.join(SCRIPTS_DIR, "convert_to_cache_input.py"),
            "-i", tokenized_file,
            "-o", cache_input_file
        ]

        print(f"  执行: {' '.join(cmd)}")

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.stdout:
            for line in proc.stdout.strip().split('\n'):
                print(f"  {line}")

        if proc.returncode != 0:
            raise RuntimeError(f"convert_to_cache_input.py 失败: {proc.stderr}")

        # 统计输出行数
        line_count = 0
        if os.path.exists(cache_input_file):
            with open(cache_input_file, 'r') as f:
                line_count = sum(1 for _ in f)

        step2_duration = round((datetime.now() - step2_start).total_seconds(), 2)

        result["steps"].append({
            "name": "convert",
            "status": "completed",
            "duration_seconds": step2_duration,
            "output_file": cache_input_file,
            "output_lines": line_count
        })
        print(f"{file_tag} Step 2/2 完成 ({step2_duration}s) -> {cache_input_file}")

    except Exception as e:
        result["steps"].append({
            "name": "convert",
            "status": "failed",
            "error": str(e)
        })
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"{file_tag} Step 2/2 失败: {e}")
        return result

    result["status"] = "completed"
    return result


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="KV Cache Token 序列化流水线 (tokenize + convert)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 单文件
    python kv_pipeline.py -i input.json -o output_dir/

    # 多文件
    python kv_pipeline.py -i file1.json file2.json file3.json -o output_dir/

    # 指定模型
    python kv_pipeline.py -i input.json -o output_dir/ -d glm-5
        """
    )

    parser.add_argument("--input", "-i", nargs="+", required=True,
                        help="输入文件路径，支持多个文件")
    parser.add_argument("--output-dir", "-o", required=True,
                        help="输出目录，各文件按原文件名保存")
    parser.add_argument("--default-model", "-d", default="glm-5",
                        help="默认模型 (默认: glm-5)")
    parser.add_argument("--override-tokenizer", "-t", default=None,
                        help="强制使用指定 tokenizer")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="并发处理文件数 (默认: 4)")

    args = parser.parse_args()

    # 校验输入文件
    input_files = []
    for f in args.input:
        abs_path = os.path.abspath(f)
        if not os.path.exists(abs_path):
            print(f"[ERROR] 输入文件不存在: {abs_path}")
            return 1
        input_files.append(abs_path)

    # 创建输出目录
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    total_files = len(input_files)
    pipeline_start = datetime.now()

    print(f"\n{'='*60}")
    print(f"KV Cache Token 序列化流水线")
    print(f"{'='*60}")
    print(f"输入文件: {total_files} 个")
    for f in input_files:
        print(f"  - {f}")
    print(f"输出目录: {output_dir}")
    print(f"默认模型: {args.default_model}")
    if args.override_tokenizer:
        print(f"强制 Tokenizer: {args.override_tokenizer}")
    workers = min(args.workers, total_files)
    print(f"并发度: {workers}")
    print(f"{'='*60}")

    # 并发处理文件
    all_results = [None] * total_files
    success_files = 0
    failed_files = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_idx = {}
        for idx, input_file in enumerate(input_files):
            future = executor.submit(
                process_single_file,
                input_file=input_file,
                output_dir=output_dir,
                default_model=args.default_model,
                override_tokenizer=args.override_tokenizer,
                file_index=idx + 1,
                total_files=total_files
            )
            future_to_idx[future] = idx

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                result = future.result()
            except Exception as e:
                result = {
                    "input_file": input_files[idx],
                    "status": "failed",
                    "error": str(e)
                }
            all_results[idx] = result
            if result["status"] == "completed":
                success_files += 1
            else:
                failed_files += 1

    # 汇总
    total_duration = round((datetime.now() - pipeline_start).total_seconds(), 2)

    print(f"\n{'='*60}")
    print(f"流水线执行完成")
    print(f"{'='*60}")
    print(f"总耗时: {total_duration}s")
    print(f"文件: {success_files} 成功, {failed_files} 失败 / 共 {total_files} 个")
    print(f"{'='*60}")
    print(f"输出文件:")
    for r in all_results:
        status_icon = "ok" if r["status"] == "completed" else "FAIL"
        print(f"  [{status_icon}] {os.path.basename(r['input_file'])}")
        if r["status"] == "completed":
            print(f"        -> {r['tokenized_file']}")
            print(f"        -> {r['cache_input_file']}")
        else:
            print(f"        Error: {r.get('error', 'unknown')}")
    print(f"{'='*60}")

    # 保存汇总状态
    summary_file = os.path.join(output_dir, "pipeline_summary.json")
    summary = {
        "status": "completed" if failed_files == 0 else "partial",
        "created_at": pipeline_start.isoformat(),
        "duration_seconds": total_duration,
        "workers": workers,
        "total_files": total_files,
        "success_files": success_files,
        "failed_files": failed_files,
        "default_model": args.default_model,
        "override_tokenizer": args.override_tokenizer,
        "files": all_results
    }
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"汇总状态: {summary_file}")

    return 0 if failed_files == 0 else 1


if __name__ == "__main__":
    exit(main())
