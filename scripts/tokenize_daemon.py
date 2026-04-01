#!/usr/bin/env python3
"""
Tokenizer 常驻进程 Daemon

解决的核心问题：原先每个 .jsonl 文件启动一个独立的 tokenize_script.py 子进程，
每次都要重新 AutoTokenizer.from_pretrained 加载全部模型 tokenizer，代价极高。

本 daemon 作为长期运行的服务进程：
- 启动时预加载（或首次使用时懒加载）所有 tokenizer
- 通过 multiprocessing.Queue 接收 tokenize 任务
- 常驻 worker pool 复用已加载的 tokenizer，无需重复初始化
- 任务粒度：(input_file, output_dir, file_prefix, options) → summary dict

通信协议（JSON over stdin/stdout）：
  父进程 → daemon:  {"type": "task", "id": "...", "input_file": "...", "output_dir": "...",
                      "file_prefix": "...", "default_model": "...", "override_tokenizer": null,
                      "batch_size": 500, "verbose": false}
  daemon → 父进程:  {"type": "result", "id": "...", "status": "completed"|"failed",
                      "models": {...}, "success_count": N, "failed_count": M,
                      "duration_seconds": X.X, "error": null}
  父进程 → daemon:  {"type": "shutdown"}
  daemon → 父进程:  {"type": "ready"}   (启动完成后)

用法（通常由 kv_pipeline.py 启动并管理）:
    python tokenize_daemon.py --workers 6 --batch-size 500
"""

import json
import os
import sys
import time
import argparse
import traceback
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 确保可以 import tokenize_script
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from tokenize_script import (
    _worker_init,
    _worker_process_batch,
    TokenizerManager,
    MODEL_TOKENIZER_MAPPING,
)

os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


def _process_file_with_pool(
    pool: Pool,
    input_file: str,
    output_dir: str,
    file_prefix: str,
    batch_size: int,
    verbose: bool,
) -> dict:
    """
    用已有的 Pool 对单个文件执行 tokenize，返回 summary dict。
    复用 tokenize_script.py 的 batch 处理逻辑，但不重启 Pool。
    """
    import glob as _glob

    if not file_prefix:
        file_prefix = os.path.splitext(os.path.basename(input_file))[0]

    os.makedirs(output_dir, exist_ok=True)

    model_files: dict = {}   # model -> {"fh", "incomplete", "final", "count"}
    model_counts: dict = {}
    success_count = 0
    failed_count = 0
    total_records = 0

    max_pending_batches = pool._processes * 2  # type: ignore[attr-defined]
    pending_futures = []
    current_batch = []
    line_idx = 0

    def _flush_results():
        nonlocal success_count, failed_count
        for future in pending_futures:
            batch_results = future.get()
            for _, model, txt_line in batch_results:
                if model is not None:
                    success_count += 1
                    if model not in model_files:
                        final_file = os.path.join(output_dir, f"{file_prefix}_{model}_input_ids.txt")
                        incomplete_file = final_file + ".incomplete"
                        fh = open(incomplete_file, "w", encoding="utf-8")
                        model_files[model] = {
                            "fh": fh, "incomplete": incomplete_file,
                            "final": final_file, "count": 0,
                        }
                    mf = model_files[model]
                    mf["fh"].write(txt_line + "\n")
                    mf["count"] += 1
                    model_counts[model] = mf["count"]
                else:
                    failed_count += 1
        pending_futures.clear()

    start_time = datetime.now()

    with open(input_file, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            total_records += 1
            current_batch.append((line_idx, line))
            line_idx += 1

            if len(current_batch) >= batch_size:
                future = pool.apply_async(_worker_process_batch, (current_batch,))
                pending_futures.append(future)
                current_batch = []

                if len(pending_futures) >= max_pending_batches:
                    _flush_results()

    if current_batch:
        future = pool.apply_async(_worker_process_batch, (current_batch,))
        pending_futures.append(future)

    _flush_results()

    # 关闭文件，rename incomplete → final
    output_files = {}
    for model, mf in model_files.items():
        mf["fh"].close()
        os.rename(mf["incomplete"], mf["final"])
        output_files[model] = {"file": mf["final"], "count": mf["count"]}

    duration = (datetime.now() - start_time).total_seconds()
    return {
        "models": output_files,
        "success_count": success_count,
        "failed_count": failed_count,
        "total_records": total_records,
        "duration_seconds": round(duration, 2),
    }


def _read_msg() -> dict | None:
    """从 stdin 读取一行 JSON 消息，EOF 返回 None"""
    try:
        line = sys.stdin.readline()
        if not line:
            return None
        return json.loads(line.strip())
    except (json.JSONDecodeError, EOFError):
        return None


def _write_msg(msg: dict):
    """向 stdout 写出一行 JSON 消息"""
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Tokenizer 常驻 Daemon")
    parser.add_argument("--workers", "-W", type=int, default=0,
                        help="multiprocessing Pool worker 数 (0=auto)")
    parser.add_argument("--batch-size", "-B", type=int, default=500,
                        help="每个 batch 的记录数 (默认 500)")
    parser.add_argument("--override-tokenizer", "-t", default=None,
                        help="强制 tokenizer")
    parser.add_argument("--default-model", "-d", default="glm-5",
                        help="默认模型")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    num_workers = args.workers if args.workers > 0 else max(1, min(cpu_count() - 1, 8))
    batch_size = args.batch_size

    # 创建常驻 Pool，worker 进程在此一次性初始化 TokenizerManager
    pool = Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(args.override_tokenizer, args.default_model, args.verbose),
    )

    # 通知父进程 daemon 已就绪
    _write_msg({"type": "ready", "workers": num_workers, "batch_size": batch_size})

    try:
        while True:
            msg = _read_msg()
            if msg is None:
                # stdin 关闭，正常退出
                break

            msg_type = msg.get("type")

            if msg_type == "shutdown":
                break

            if msg_type == "task":
                task_id = msg.get("id", "")
                input_file = msg["input_file"]
                output_dir = msg["output_dir"]
                file_prefix = msg.get("file_prefix", "")
                t_batch_size = msg.get("batch_size", batch_size)
                t_verbose = msg.get("verbose", args.verbose)

                try:
                    summary = _process_file_with_pool(
                        pool=pool,
                        input_file=input_file,
                        output_dir=output_dir,
                        file_prefix=file_prefix,
                        batch_size=t_batch_size,
                        verbose=t_verbose,
                    )
                    _write_msg({
                        "type": "result",
                        "id": task_id,
                        "status": "completed",
                        **summary,
                        "error": None,
                    })
                except Exception as e:
                    _write_msg({
                        "type": "result",
                        "id": task_id,
                        "status": "failed",
                        "models": {},
                        "success_count": 0,
                        "failed_count": 0,
                        "duration_seconds": 0.0,
                        "error": f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}",
                    })
    finally:
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()
