#!/usr/bin/env python3
"""
KV Cache Token 序列化流水线

整合步骤：
1. Token 序列化 (tokenize_script.py) — 按 model 分桶，直接输出 cache_calc 格式 txt

特性：
- 支持多个输入文件
- tokenize 按 model 自动分桶，每个切片产出 per-model 的 _input_ids.txt
- 多进程并行 tokenize（CPU 密集部分加速）
- 实时进度和状态更新

用法:
    python kv_pipeline.py -i file1.jsonl file2.jsonl -o /path/to/output_dir

示例:
    python kv_pipeline.py \
        -i olap_database/data/user/task_xxx/kv_20260323_180000_20260323_190000.jsonl \
        -o olap_database/data/user/task_xxx/tokenized
"""

import json
import os
import re
import glob
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, List


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
    total_files: int = 1,
    model_filter: set = None,
    tokenize_workers: int = 0,
    tokenize_batch_size: int = 200
) -> Dict[str, Any]:
    """
    处理单个输入文件：tokenize (per-model 分桶，直接输出 txt)

    产出:
      {file_prefix}_{model}_input_ids.txt (每个 model 一个)
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    file_tag = f"[{file_index}/{total_files}]"

    result = {
        "input_file": input_file,
        "status": "pending",
        "steps": [],
        "model_files": {},  # model -> {"txt": ..., "lines": ...}
    }

    # ---- Token 序列化 (per-model 分桶，直接输出 txt) ----
    print(f"\n{file_tag} 序列化 - {base_name}")
    step_start = datetime.now()

    try:
        cmd = [
            "python", "-u", os.path.join(SCRIPTS_DIR, "tokenize_script.py"),
            "-i", input_file,
            "-o", output_dir,
            "-p", base_name,
            "-d", default_model,
            "-W", str(tokenize_workers),
            "-B", str(tokenize_batch_size)
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
        summary_json = None

        for line in process.stdout:
            line = line.strip()
            if line:
                print(f"  {line}", flush=True)

            if "成功:" in line and "失败:" in line:
                match = re.search(r'成功:\s*(\d+).*失败:\s*(\d+)', line)
                if match:
                    success_count = int(match.group(1))
                    failed_count = int(match.group(2))

            # 解析 [SUMMARY] 行
            if line.startswith("[SUMMARY] "):
                try:
                    summary_json = json.loads(line[len("[SUMMARY] "):])
                except json.JSONDecodeError:
                    pass

        process.wait()
        step_duration = round((datetime.now() - step_start).total_seconds(), 2)

        if process.returncode != 0:
            raise RuntimeError(f"tokenize_script.py 返回码: {process.returncode}")

        # 从 summary 中提取各 model 的 txt 文件
        model_txt_files = {}
        if summary_json and "models" in summary_json:
            for model, info in summary_json["models"].items():
                model_txt_files[model] = info["file"]
        else:
            # 兜底：扫描产出文件
            pattern = os.path.join(output_dir, f"{base_name}_*_input_ids.txt")
            for f in sorted(glob.glob(pattern)):
                fname = os.path.basename(f)
                model = fname[len(base_name) + 1:].replace("_input_ids.txt", "")
                if model:
                    model_txt_files[model] = f

        # 如果指定了模型过滤，标记跳过的模型
        if model_filter:
            skipped = set(model_txt_files.keys()) - model_filter
            if skipped:
                print(f"{file_tag} 模型过滤: 跳过 {sorted(skipped)}，仅保留 {sorted(model_filter & set(model_txt_files.keys()))}")
            model_txt_files = {m: f for m, f in model_txt_files.items() if m in model_filter}

        # 统计每个 txt 文件的行数
        for model, txt_file in model_txt_files.items():
            line_count = 0
            if os.path.exists(txt_file):
                with open(txt_file, 'r') as f:
                    line_count = sum(1 for _ in f)
            result["model_files"][model] = {
                "txt": txt_file,
                "lines": line_count
            }

        result["steps"].append({
            "name": "tokenize",
            "status": "completed",
            "duration_seconds": step_duration,
            "success_count": success_count,
            "failed_count": failed_count,
            "models": list(model_txt_files.keys()),
        })
        print(f"{file_tag} 完成 ({step_duration}s), 模型: {list(model_txt_files.keys())}")

    except Exception as e:
        result["steps"].append({
            "name": "tokenize",
            "status": "failed",
            "error": str(e)
        })
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"{file_tag} 失败: {e}")
        return result

    result["status"] = "completed"
    return result


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="KV Cache Token 序列化流水线 (tokenize per-model → txt)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 单文件
    python kv_pipeline.py -i input.jsonl -o output_dir/

    # 多文件
    python kv_pipeline.py -i file1.jsonl file2.jsonl -o output_dir/

    # 指定模型
    python kv_pipeline.py -i input.jsonl -o output_dir/ -d glm-5
        """
    )

    parser.add_argument("--input", "-i", nargs="+", required=True,
                        help="输入文件路径，支持多个文件")
    parser.add_argument("--output-dir", "-o", required=True,
                        help="输出目录")
    parser.add_argument("--default-model", "-d", default="glm-5",
                        help="默认模型 (默认: glm-5)")
    parser.add_argument("--override-tokenizer", "-t", default=None,
                        help="强制使用指定 tokenizer")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="并发处理文件数 (默认: 4)")
    parser.add_argument("--models", "-m", default=None,
                        help="模型过滤，逗号分隔（如 glm-5,deepseek-v3.2），仅保留指定模型的输出")
    parser.add_argument("--tokenize-workers", type=int, default=0,
                        help="tokenize 多进程 worker 数 (0=自动，透传给 tokenize_script.py -W)")
    parser.add_argument("--tokenize-batch-size", type=int, default=200,
                        help="tokenize batch 大小 (透传给 tokenize_script.py -B)")

    args = parser.parse_args()

    # 解析模型过滤列表
    model_filter = set()
    if args.models:
        model_filter = {m.strip() for m in args.models.split(",") if m.strip()}

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
    print(f"KV Cache Token 序列化流水线 (per-model)")
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
    if model_filter:
        print(f"模型过滤: {sorted(model_filter)}")
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
                total_files=total_files,
                model_filter=model_filter or None,
                tokenize_workers=args.tokenize_workers,
                tokenize_batch_size=args.tokenize_batch_size
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
            if result["status"] in ("completed", "partial"):
                success_files += 1
            else:
                failed_files += 1

    # 汇总：按 model 聚合所有 txt 文件
    model_outputs = {}  # model -> [txt_file, ...]
    for r in all_results:
        if r and "model_files" in r:
            for model, files in r["model_files"].items():
                model_outputs.setdefault(model, []).append(files["txt"])

    total_duration = round((datetime.now() - pipeline_start).total_seconds(), 2)

    print(f"\n{'='*60}")
    print(f"流水线执行完成")
    print(f"{'='*60}")
    print(f"总耗时: {total_duration}s")
    print(f"文件: {success_files} 成功, {failed_files} 失败 / 共 {total_files} 个")
    print(f"模型: {list(model_outputs.keys())}")
    print(f"{'='*60}")
    print(f"输出文件 (按模型):")
    for model, txt_files in sorted(model_outputs.items()):
        print(f"  [{model}] {len(txt_files)} 个文件:")
        for tf in txt_files:
            print(f"    -> {tf}")
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
        "model_outputs": {m: fs for m, fs in model_outputs.items()},
        "files": all_results
    }
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"汇总状态: {summary_file}")

    return 0 if failed_files == 0 else 1


if __name__ == "__main__":
    exit(main())
