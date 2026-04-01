#!/usr/bin/env python3
"""
KV Cache Token 序列化流水线

整合步骤：
1. Token 序列化 (tokenize_script.py / tokenize_daemon.py) — 按 model 分桶，直接输出 cache_calc 格式 txt

特性：
- 支持多个输入文件
- tokenize 按 model 自动分桶，每个切片产出 per-model 的 _input_ids.txt
- 多进程并行 tokenize（CPU 密集部分加速）
- 【优化】--daemon 模式：启动常驻 tokenize_daemon.py，tokenizer 一次加载后复用，
         避免每文件重复 AutoTokenizer.from_pretrained，显著降低延迟
- 实时进度和状态更新

用法:
    python kv_pipeline.py -i file1.jsonl file2.jsonl -o /path/to/output_dir

示例:
    python kv_pipeline.py \\
        -i olap_database/data/user/task_xxx/kv_20260323_180000_20260323_190000.jsonl \\
        -o olap_database/data/user/task_xxx/tokenized
"""

import json
import os
import re
import glob
import argparse
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, List, Optional


# ============================================================
# 路径配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")


# ============================================================
# Daemon 客户端（P1 核心：复用常驻进程池）
# ============================================================

class TokenizeDaemonClient:
    """
    管理一个 tokenize_daemon.py 子进程的生命周期。
    提供 submit(task) / wait(task_id) 接口，内部通过 stdin/stdout JSON 协议通信。

    关键优化：daemon 进程的 multiprocessing.Pool worker 在整个 pipeline 生命周期内
    只初始化一次，所有 .jsonl 文件复用同一批 worker，彻底消除重复加载 tokenizer 的开销。
    """

    def __init__(
        self,
        workers: int = 6,
        batch_size: int = 500,
        default_model: str = "glm-5",
        override_tokenizer: Optional[str] = None,
        verbose: bool = False,
    ):
        self._workers = workers
        self._batch_size = batch_size
        self._default_model = default_model
        self._override_tokenizer = override_tokenizer
        self._verbose = verbose

        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._pending: Dict[str, dict] = {}   # id -> result (filled on arrival)
        self._reader_thread: Optional[threading.Thread] = None
        self._task_counter = 0

    def start(self, timeout: float = 120.0):
        """启动 daemon 进程，阻塞直到收到 ready 信号"""
        cmd = [
            sys.executable, "-u",
            os.path.join(SCRIPTS_DIR, "tokenize_daemon.py"),
            "-W", str(self._workers),
            "-B", str(self._batch_size),
            "-d", self._default_model,
        ]
        if self._override_tokenizer:
            cmd.extend(["-t", self._override_tokenizer])
        if self._verbose:
            cmd.append("-v")

        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,   # daemon 的 stderr 直通，方便调试
            text=True,
            bufsize=1,
            cwd=BASE_DIR,
        )

        # 等待 ready 信号
        deadline = datetime.now().timestamp() + timeout
        while datetime.now().timestamp() < deadline:
            line = self._proc.stdout.readline()
            if not line:
                raise RuntimeError("tokenize_daemon 进程意外退出（未收到 ready 信号）")
            line = line.strip()
            try:
                msg = json.loads(line)
                if msg.get("type") == "ready":
                    print(f"[daemon] 就绪: workers={msg.get('workers')}, batch_size={msg.get('batch_size')}", flush=True)
                    break
            except json.JSONDecodeError:
                print(f"[daemon] init: {line}", flush=True)
        else:
            self.stop()
            raise TimeoutError("tokenize_daemon 启动超时")

        # 启动后台读取线程，持续消费 daemon 的输出
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

    def _reader_loop(self):
        """后台线程：持续读取 daemon stdout，将结果写入 _pending"""
        for line in self._proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                print(f"[daemon] {line}", flush=True)
                continue

            if msg.get("type") == "result":
                task_id = msg.get("id", "")
                with self._lock:
                    self._pending[task_id] = msg
            else:
                print(f"[daemon] {line}", flush=True)

    def _send(self, msg: dict):
        line = json.dumps(msg, ensure_ascii=False) + "\n"
        self._proc.stdin.write(line)
        self._proc.stdin.flush()

    def submit(
        self,
        input_file: str,
        output_dir: str,
        file_prefix: str = "",
        batch_size: Optional[int] = None,
        verbose: Optional[bool] = None,
    ) -> str:
        """提交一个 tokenize 任务，返回 task_id（异步，不等待结果）"""
        with self._lock:
            self._task_counter += 1
            task_id = f"task_{self._task_counter}"

        self._send({
            "type": "task",
            "id": task_id,
            "input_file": input_file,
            "output_dir": output_dir,
            "file_prefix": file_prefix,
            "batch_size": batch_size if batch_size is not None else self._batch_size,
            "verbose": verbose if verbose is not None else self._verbose,
        })
        return task_id

    def wait(self, task_id: str, timeout: float = 3600.0) -> dict:
        """阻塞等待指定 task_id 的结果，超时抛异常"""
        deadline = datetime.now().timestamp() + timeout
        while datetime.now().timestamp() < deadline:
            with self._lock:
                if task_id in self._pending:
                    return self._pending.pop(task_id)
            import time
            time.sleep(0.05)
        raise TimeoutError(f"等待 tokenize 结果超时: {task_id}")

    def stop(self):
        """优雅关闭 daemon"""
        if self._proc and self._proc.stdin:
            try:
                self._send({"type": "shutdown"})
                self._proc.stdin.close()
            except Exception:
                pass
        if self._proc:
            try:
                self._proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None


# ============================================================
# 单文件处理器（daemon 模式）
# ============================================================

def process_single_file_daemon(
    daemon: TokenizeDaemonClient,
    input_file: str,
    output_dir: str,
    default_model: str = "glm-5",
    override_tokenizer: str = None,
    file_index: int = 1,
    total_files: int = 1,
    model_filter: set = None,
    batch_size: int = 500,
) -> Dict[str, Any]:
    """
    通过常驻 daemon 处理单个文件（不重启进程，不重新加载 tokenizer）
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    file_tag = f"[{file_index}/{total_files}]"

    result = {
        "input_file": input_file,
        "status": "pending",
        "steps": [],
        "model_files": {},
    }

    print(f"\n{file_tag} 序列化 (daemon) - {base_name}")
    step_start = datetime.now()

    try:
        task_id = daemon.submit(
            input_file=input_file,
            output_dir=output_dir,
            file_prefix=base_name,
            batch_size=batch_size,
        )

        summary = daemon.wait(task_id)

        step_duration = round((datetime.now() - step_start).total_seconds(), 2)

        if summary["status"] == "failed":
            raise RuntimeError(summary.get("error", "daemon 返回 failed"))

        # 整理 model_files
        model_txt_files = {}
        for model, info in summary.get("models", {}).items():
            model_txt_files[model] = info["file"]

        if model_filter:
            skipped = set(model_txt_files.keys()) - model_filter
            if skipped:
                print(f"{file_tag} 模型过滤: 跳过 {sorted(skipped)}")
            model_txt_files = {m: f for m, f in model_txt_files.items() if m in model_filter}

        for model, txt_file in model_txt_files.items():
            line_count = summary["models"].get(model, {}).get("count", 0)
            if not line_count and os.path.exists(txt_file):
                with open(txt_file, "r") as f:
                    line_count = sum(1 for _ in f)
            result["model_files"][model] = {"txt": txt_file, "lines": line_count}

        result["steps"].append({
            "name": "tokenize",
            "status": "completed",
            "duration_seconds": step_duration,
            "success_count": summary.get("success_count", 0),
            "failed_count": summary.get("failed_count", 0),
            "models": list(model_txt_files.keys()),
        })
        print(f"{file_tag} 完成 ({step_duration}s), 模型: {list(model_txt_files.keys())}", flush=True)

    except Exception as e:
        result["steps"].append({"name": "tokenize", "status": "failed", "error": str(e)})
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"{file_tag} 失败: {e}", flush=True)
        return result

    result["status"] = "completed"
    return result


# ============================================================
# 单文件处理器（legacy 子进程模式，作为降级 fallback）
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
    tokenize_batch_size: int = 500
) -> Dict[str, Any]:
    """
    处理单个输入文件（legacy 模式：每文件启动独立子进程）
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    file_tag = f"[{file_index}/{total_files}]"

    result = {
        "input_file": input_file,
        "status": "pending",
        "steps": [],
        "model_files": {},
    }

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
            if line.startswith("[SUMMARY] "):
                try:
                    summary_json = json.loads(line[len("[SUMMARY] "):])
                except json.JSONDecodeError:
                    pass

        process.wait()
        step_duration = round((datetime.now() - step_start).total_seconds(), 2)

        if process.returncode != 0:
            raise RuntimeError(f"tokenize_script.py 返回码: {process.returncode}")

        model_txt_files = {}
        if summary_json and "models" in summary_json:
            for model, info in summary_json["models"].items():
                model_txt_files[model] = info["file"]
        else:
            pattern = os.path.join(output_dir, f"{base_name}_*_input_ids.txt")
            for f in sorted(glob.glob(pattern)):
                fname = os.path.basename(f)
                model = fname[len(base_name) + 1:].replace("_input_ids.txt", "")
                if model:
                    model_txt_files[model] = f

        if model_filter:
            skipped = set(model_txt_files.keys()) - model_filter
            if skipped:
                print(f"{file_tag} 模型过滤: 跳过 {sorted(skipped)}")
            model_txt_files = {m: f for m, f in model_txt_files.items() if m in model_filter}

        for model, txt_file in model_txt_files.items():
            line_count = 0
            if os.path.exists(txt_file):
                with open(txt_file, "r") as f:
                    line_count = sum(1 for _ in f)
            result["model_files"][model] = {"txt": txt_file, "lines": line_count}

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
        result["steps"].append({"name": "tokenize", "status": "failed", "error": str(e)})
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
    # 单文件（daemon 模式，默认开启）
    python kv_pipeline.py -i input.jsonl -o output_dir/

    # 多文件
    python kv_pipeline.py -i file1.jsonl file2.jsonl -o output_dir/

    # 禁用 daemon，回退到旧模式
    python kv_pipeline.py -i input.jsonl -o output_dir/ --no-daemon
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
                        help="并发处理文件数（daemon 模式下即 daemon 内部的 Pool workers，默认: 4）")
    parser.add_argument("--models", "-m", default=None,
                        help="模型过滤，逗号分隔（如 glm-5,deepseek-v3.2）")
    parser.add_argument("--tokenize-workers", type=int, default=0,
                        help="[legacy] tokenize 多进程 worker 数 (0=自动)")
    parser.add_argument("--tokenize-batch-size", type=int, default=500,
                        help="tokenize batch 大小 (默认 500)")
    parser.add_argument("--no-daemon", action="store_true",
                        help="禁用 daemon 模式，回退到每文件启动子进程的旧行为")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    model_filter = set()
    if args.models:
        model_filter = {m.strip() for m in args.models.split(",") if m.strip()}

    input_files = []
    for f in args.input:
        abs_path = os.path.abspath(f)
        if not os.path.exists(abs_path):
            print(f"[ERROR] 输入文件不存在: {abs_path}")
            return 1
        input_files.append(abs_path)

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    total_files = len(input_files)
    pipeline_start = datetime.now()

    use_daemon = not args.no_daemon
    print(f"\n{'='*60}")
    print(f"KV Cache Token 序列化流水线 (per-model)")
    print(f"{'='*60}")
    print(f"输入文件: {total_files} 个")
    for f in input_files:
        print(f"  - {f}")
    print(f"输出目录: {output_dir}")
    print(f"默认模型: {args.default_model}")
    print(f"模式: {'daemon (tokenizer 常驻复用)' if use_daemon else 'legacy (每文件启动子进程)'}")
    if args.override_tokenizer:
        print(f"强制 Tokenizer: {args.override_tokenizer}")
    if model_filter:
        print(f"模型过滤: {sorted(model_filter)}")
    print(f"{'='*60}")

    all_results = [None] * total_files
    success_files = 0
    failed_files = 0

    if use_daemon:
        # ---- Daemon 模式：一个 daemon 进程服务所有文件 ----
        daemon = TokenizeDaemonClient(
            workers=args.workers,
            batch_size=args.tokenize_batch_size,
            default_model=args.default_model,
            override_tokenizer=args.override_tokenizer,
            verbose=args.verbose,
        )
        print(f"[INFO] 启动 tokenize daemon (workers={args.workers})...")
        daemon.start(timeout=180.0)

        # 用 ThreadPoolExecutor 并发提交+等待多文件（每线程负责一个文件的 submit+wait）
        # daemon 内部串行处理（Pool 保证并行），外部线程主要是 I/O 等待
        file_workers = min(total_files, max(1, args.workers // 2))
        try:
            with ThreadPoolExecutor(max_workers=file_workers) as executor:
                future_to_idx = {}
                for idx, input_file in enumerate(input_files):
                    future = executor.submit(
                        process_single_file_daemon,
                        daemon=daemon,
                        input_file=input_file,
                        output_dir=output_dir,
                        default_model=args.default_model,
                        override_tokenizer=args.override_tokenizer,
                        file_index=idx + 1,
                        total_files=total_files,
                        model_filter=model_filter or None,
                        batch_size=args.tokenize_batch_size,
                    )
                    future_to_idx[future] = idx

                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        result = {"input_file": input_files[idx], "status": "failed", "error": str(e)}
                    all_results[idx] = result
                    if result["status"] in ("completed", "partial"):
                        success_files += 1
                    else:
                        failed_files += 1
        finally:
            daemon.stop()

    else:
        # ---- Legacy 模式：每文件独立子进程 ----
        workers = min(args.workers, total_files)
        print(f"并发度: {workers}")

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
                    result = {"input_file": input_files[idx], "status": "failed", "error": str(e)}
                all_results[idx] = result
                if result["status"] in ("completed", "partial"):
                    success_files += 1
                else:
                    failed_files += 1

    # 汇总
    model_outputs = {}
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

    summary_file = os.path.join(output_dir, "pipeline_summary.json")
    summary = {
        "status": "completed" if failed_files == 0 else "partial",
        "created_at": pipeline_start.isoformat(),
        "duration_seconds": total_duration,
        "mode": "daemon" if use_daemon else "legacy",
        "workers": args.workers,
        "total_files": total_files,
        "success_files": success_files,
        "failed_files": failed_files,
        "default_model": args.default_model,
        "override_tokenizer": args.override_tokenizer,
        "model_outputs": {m: fs for m, fs in model_outputs.items()},
        "files": all_results
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"汇总状态: {summary_file}")

    return 0 if failed_files == 0 else 1


if __name__ == "__main__":
    exit(main())
