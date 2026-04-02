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
    model_filter: set = None,
    log_fh=None,
) -> dict:
    """
    用已有的 Pool 对单个文件执行 tokenize，返回 summary dict。
    复用 tokenize_script.py 的 batch 处理逻辑，但不重启 Pool。

    model_filter: 非空时只写入指定 model 的 input_ids，其余记录直接丢弃（不写盘）。
    log_fh: 可选的日志文件句柄，非 None 时同步写入日志（用于落盘到 /tokenize_logs）。
    """
    import glob as _glob

    def _log(msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, file=sys.stderr, flush=True)
        if log_fh is not None:
            log_fh.write(line + "\n")
            log_fh.flush()

    if not file_prefix:
        file_prefix = os.path.splitext(os.path.basename(input_file))[0]

    os.makedirs(output_dir, exist_ok=True)

    model_files: dict = {}   # model -> {"fh", "incomplete", "final", "count"}
    model_counts: dict = {}
    success_count = 0
    failed_count = 0
    too_long_count = 0
    total_records = 0

    max_pending_batches = pool._processes * 2  # type: ignore[attr-defined]
    pending_futures = []
    current_batch = []
    line_idx = 0

    def _flush_results():
        nonlocal success_count, failed_count, too_long_count
        for future in pending_futures:
            batch_results, batch_too_long = future.get()
            too_long_count += batch_too_long
            for _, model, txt_line in batch_results:
                if model is not None:
                    # model_filter 非空时跳过不在列表里的 model
                    if model_filter and model not in model_filter:
                        failed_count += 1  # 计入跳过，不写盘
                        continue
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
    _log(f"开始处理: {input_file} (model_filter={model_filter})")

    with open(input_file, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            total_records += 1
            current_batch.append((line_idx, line))
            line_idx += 1

            if len(current_batch) >= batch_size:
                future = pool.apply_async(_worker_process_batch, (current_batch, model_filter))
                pending_futures.append(future)
                current_batch = []

                if len(pending_futures) >= max_pending_batches:
                    _flush_results()
                    processed = success_count + failed_count + too_long_count
                    if processed > 0 and processed % 50000 < batch_size:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        speed = processed / elapsed if elapsed > 0 else 0
                        _log(f"进度: 已处理={processed} 成功={success_count} "
                             f"失败={failed_count} 超长跳过={too_long_count} "
                             f"速度={speed:.0f}条/秒")

    if current_batch:
        future = pool.apply_async(_worker_process_batch, (current_batch, model_filter))
        pending_futures.append(future)

    _flush_results()

    # 关闭文件，rename incomplete → final
    output_files = {}
    for model, mf in model_files.items():
        mf["fh"].close()
        os.rename(mf["incomplete"], mf["final"])
        output_files[model] = {"file": mf["final"], "count": mf["count"]}

    duration = (datetime.now() - start_time).total_seconds()
    _log(f"完成: {input_file} | 总={total_records} 成功={success_count} "
         f"失败={failed_count} 超长跳过={too_long_count} 耗时={duration:.1f}s")
    for model, info in output_files.items():
        _log(f"  输出: {model} -> {info['file']} ({info['count']} 条)")

    return {
        "models": output_files,
        "success_count": success_count,
        "failed_count": failed_count,
        "too_long_count": too_long_count,
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
    parser.add_argument("--log-dir", "-L", default=None,
                        help="tokenize 日志落盘目录（默认不落盘，仅 stderr）")
    args = parser.parse_args()

    num_workers = args.workers if args.workers > 0 else max(1, min(cpu_count() - 1, 8))
    batch_size = args.batch_size

    # 主进程预加载全部 tokenizer，Pool.fork() 后 worker 通过 Linux COW 共享，无需重复加载
    # 注意：日志写 stderr，stdout 保留给 JSON 协议；每个 tokenizer 独立 try/except，加载失败不崩 daemon
    import tokenize_script as _ts
    _ts._preloaded_tm = _ts.TokenizerManager()
    print(f"[daemon] 预加载 tokenizer，共 {len(_ts.MODEL_TOKENIZER_MAPPING)} 个模型...", file=sys.stderr, flush=True)
    for model_name in _ts.MODEL_TOKENIZER_MAPPING:
        try:
            t, cfg = _ts._preloaded_tm.get_tokenizer(model_name)
            print(f"[daemon]   {model_name} -> {cfg if t else '无匹配，跳过'}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[daemon]   {model_name} 预加载失败（将在 worker 内懒加载）: {e}", file=sys.stderr, flush=True)

    # 创建常驻 Pool，worker fork 自主进程，直接继承已加载的 tokenizer
    pool = Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(args.override_tokenizer, args.default_model, args.verbose),
    )

    # 通知父进程 daemon 已就绪
    _write_msg({"type": "ready", "workers": num_workers, "batch_size": batch_size})

    # 日志落盘目录（由 --log-dir 指定，或任务 msg 中携带）
    _default_log_dir = args.log_dir  # 可能为 None

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
                t_model_filter = set(msg["model_filter"]) if msg.get("model_filter") else None

                # 日志目录：优先取 task msg 中携带的 log_dir，其次用 --log-dir 参数
                t_log_dir = msg.get("log_dir") or _default_log_dir
                log_fh = None
                if t_log_dir:
                    os.makedirs(t_log_dir, exist_ok=True)
                    _fp = os.path.splitext(os.path.basename(input_file))[0] if not file_prefix else file_prefix
                    log_path = os.path.join(t_log_dir, f"{_fp}_tokenize.log")
                    try:
                        log_fh = open(log_path, "a", encoding="utf-8")
                    except Exception as _le:
                        print(f"[daemon] 无法打开日志文件 {log_path}: {_le}", file=sys.stderr, flush=True)

                try:
                    summary = _process_file_with_pool(
                        pool=pool,
                        input_file=input_file,
                        output_dir=output_dir,
                        file_prefix=file_prefix,
                        batch_size=t_batch_size,
                        verbose=t_verbose,
                        model_filter=t_model_filter,
                        log_fh=log_fh,
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
                        "too_long_count": 0,
                        "duration_seconds": 0.0,
                        "error": f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}",
                    })
                finally:
                    if log_fh is not None:
                        log_fh.close()
    finally:
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()
