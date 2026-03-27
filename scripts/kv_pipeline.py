#!/usr/bin/env python3
"""
KV Cache Token 序列化流水线

整合两个步骤：
1. Token 序列化 (tokenize_script.py) — 按 model 分桶输出
2. 转换为 cache_calc 格式 (convert_to_cache_input.py) — 每个 model 文件分别转换

特性：
- 支持多个输入文件
- tokenize 按 model 自动分桶，每个切片产出 per-model 的 _input_ids.json
- convert 对每个 per-model JSON 分别转 _input_ids.txt
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
    model_filter: set = None
) -> Dict[str, Any]:
    """
    处理单个输入文件：tokenize (per-model 分桶) -> convert (每个 model 文件)

    tokenize 产出:
      {file_prefix}_{model}_input_ids.json (每个 model 一个)
    convert 产出:
      {file_prefix}_{model}_input_ids.txt  (每个 model 一个)
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    file_tag = f"[{file_index}/{total_files}]"

    result = {
        "input_file": input_file,
        "status": "pending",
        "steps": [],
        "model_files": {},  # model -> {"json": ..., "txt": ..., "lines": ...}
    }

    # ---- Step 1: Token 序列化 (per-model 分桶) ----
    print(f"\n{file_tag} Step 1/2: Token 序列化 - {base_name}")
    step1_start = datetime.now()

    try:
        cmd = [
            "python", "-u", os.path.join(SCRIPTS_DIR, "tokenize_script.py"),
            "-i", input_file,
            "-o", output_dir,
            "-p", base_name,
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
        step1_duration = round((datetime.now() - step1_start).total_seconds(), 2)

        if process.returncode != 0:
            raise RuntimeError(f"tokenize_script.py 返回码: {process.returncode}")

        # 从 summary 中提取各 model 的 JSON 文件
        model_json_files = {}
        if summary_json and "models" in summary_json:
            for model, info in summary_json["models"].items():
                model_json_files[model] = info["file"]
        else:
            # 兜底：扫描产出文件
            pattern = os.path.join(output_dir, f"{base_name}_*_input_ids.json")
            for f in sorted(glob.glob(pattern)):
                # 从文件名提取 model: {base_name}_{model}_input_ids.json
                fname = os.path.basename(f)
                model = fname[len(base_name) + 1:].replace("_input_ids.json", "")
                if model:
                    model_json_files[model] = f

        result["steps"].append({
            "name": "tokenize",
            "status": "completed",
            "duration_seconds": step1_duration,
            "success_count": success_count,
            "failed_count": failed_count,
            "models": list(model_json_files.keys()),
        })
        print(f"{file_tag} Step 1/2 完成 ({step1_duration}s), 模型: {list(model_json_files.keys())}")

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

    # ---- Step 2: 格式转换 (每个 model 文件分别转换) ----
    # 如果指定了模型过滤，只转换选中的模型
    if model_filter:
        convert_models = {m: f for m, f in model_json_files.items() if m in model_filter}
        skipped = set(model_json_files.keys()) - set(convert_models.keys())
        if skipped:
            print(f"{file_tag} 模型过滤: 跳过 {sorted(skipped)}，仅转换 {sorted(convert_models.keys())}")
    else:
        convert_models = model_json_files

    print(f"\n{file_tag} Step 2/2: 格式转换 - {len(convert_models)} 个模型文件")
    step2_start = datetime.now()
    convert_errors = []

    for model, json_file in convert_models.items():
        txt_file = json_file.replace("_input_ids.json", "_input_ids.txt")
        incomplete_txt = txt_file + ".incomplete"
        try:
            cmd = [
                "python", "-u", os.path.join(SCRIPTS_DIR, "convert_to_cache_input.py"),
                "-i", json_file,
                "-o", incomplete_txt
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.stdout:
                for line in proc.stdout.strip().split('\n'):
                    print(f"  [{model}] {line}")

            if proc.returncode != 0:
                raise RuntimeError(f"convert 失败: {proc.stderr}")

            # 成功: rename .incomplete → .txt
            os.rename(incomplete_txt, txt_file)

            line_count = 0
            if os.path.exists(txt_file):
                with open(txt_file, 'r') as f:
                    line_count = sum(1 for _ in f)

            result["model_files"][model] = {
                "json": json_file,
                "txt": txt_file,
                "lines": line_count
            }
            print(f"  [{model}] -> {txt_file} ({line_count} 行)")

        except Exception as e:
            convert_errors.append({"model": model, "error": str(e)})
            print(f"  [{model}] 转换失败: {e}")

    step2_duration = round((datetime.now() - step2_start).total_seconds(), 2)

    result["steps"].append({
        "name": "convert",
        "status": "completed" if not convert_errors else "partial",
        "duration_seconds": step2_duration,
        "models_converted": len(result["model_files"]),
        "errors": convert_errors if convert_errors else None
    })
    print(f"{file_tag} Step 2/2 完成 ({step2_duration}s)")

    result["status"] = "completed" if not convert_errors else "partial"
    return result


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="KV Cache Token 序列化流水线 (tokenize per-model + convert)",
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
                        help="模型过滤，逗号分隔（如 glm-5,deepseek-v3.2），仅对指定模型执行 convert")

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
                model_filter=model_filter or None
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
