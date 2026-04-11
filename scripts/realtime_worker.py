#!/usr/bin/env python3
"""
实时 KV Cache 分析 Worker（常驻进程）

在 K8s Pod 内作为长驻进程运行，通过 CFS 文件队列接收任务，
执行完整的 pipeline（fetch → tokenize → simulate → trend），
将分钟级命中率结果写入每日 JSON 文件。

关键优化：
- TokenizeDaemonClient 常驻，tokenizer 只加载一次
- ES 连接按需创建，单次任务完成后关闭释放

通信方式：CFS 文件队列
  queue/pending/   ← 调度器写入待处理任务
  queue/running/   ← Worker 正在处理
  queue/done/      ← 处理完成
  queue/failed/    ← 处理失败

用法:
    python scripts/realtime_worker.py                     # 正常运行
    python scripts/realtime_worker.py --once               # 处理一个任务后退出
    python scripts/realtime_worker.py --dry-run            # 仅打印任务，不处理

crontab（无需 cron，由 K8s Deployment 管理 Pod 生命周期）:
    Worker 作为 K8s Deployment 常驻运行，无需 cron。
"""

import argparse
import asyncio
import fcntl
import gc
import glob
import json
import logging
import logging.handlers
import os
import re
import shutil
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# ============================================================
# 路径 & 配置
# ============================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(_SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

OLAP_DATABASE_DIR = os.path.join(BASE_DIR, "olap_database")
REALTIME_DIR = os.path.join(OLAP_DATABASE_DIR, "realtime")
QUEUE_DIR = os.path.join(REALTIME_DIR, "queue")
QUEUE_PENDING_DIR = os.path.join(QUEUE_DIR, "pending")
QUEUE_RUNNING_DIR = os.path.join(QUEUE_DIR, "running")
QUEUE_DONE_DIR = os.path.join(QUEUE_DIR, "done")
QUEUE_FAILED_DIR = os.path.join(QUEUE_DIR, "failed")
HEARTBEAT_FILE = os.path.join(REALTIME_DIR, "worker_heartbeat")
REALTIME_CONFIG_JSON = os.path.join(BASE_DIR, "app", "conf", "realtime_config.json")

BJT = timezone(timedelta(hours=8))
POLL_INTERVAL = 5  # 队列扫描间隔（秒）

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 文件日志：按天轮转，写入 logs/realtime/
_LOG_DIR = os.path.join(BASE_DIR, "logs", "realtime")
os.makedirs(_LOG_DIR, exist_ok=True)
_file_handler = logging.handlers.TimedRotatingFileHandler(
    os.path.join(_LOG_DIR, "realtime_worker.log"),
    when="midnight", backupCount=30, encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
))
logging.getLogger().addHandler(_file_handler)
logger = logging.getLogger("realtime_worker")

# Graceful shutdown flag
_shutdown_event = threading.Event()


def _now_bjt() -> str:
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")


def _load_realtime_config() -> dict:
    """加载实时 pipeline 独立配置"""
    try:
        with open(REALTIME_CONFIG_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.error(f"无法读取 {REALTIME_CONFIG_JSON}")
        return {}


# ============================================================
# 目录初始化
# ============================================================
def _ensure_dirs():
    for d in [
        REALTIME_DIR, QUEUE_DIR,
        QUEUE_PENDING_DIR, QUEUE_RUNNING_DIR,
        QUEUE_DONE_DIR, QUEUE_FAILED_DIR,
    ]:
        os.makedirs(d, exist_ok=True)


# ============================================================
# 心跳
# ============================================================
def _touch_heartbeat():
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(_now_bjt())
    except Exception:
        pass


# ============================================================
# 文件锁（防止多 Worker 竞争）
# ============================================================
def _try_lock_task(task_file: str) -> Optional[int]:
    """
    尝试对任务文件加排他锁，成功返回 fd，失败（已被其他 Worker 锁定）返回 None。
    CFS 要求文件描述符有写权限才能加排他锁，因此用 "r+" 模式打开。
    """
    try:
        fd = open(task_file, "r+")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (IOError, OSError):
        return None


def _unlock_task(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    except Exception:
        pass


# ============================================================
# 任务文件操作
# ============================================================
def _scan_pending_tasks() -> List[str]:
    """扫描 queue/pending/ 中的任务文件，按文件名（时间戳）排序"""
    tasks = []
    for f in sorted(glob.glob(os.path.join(QUEUE_PENDING_DIR, "*.json"))):
        tasks.append(f)
    return tasks


def _move_task(src_path: str, dest_dir: str) -> str:
    """移动任务文件到目标目录，返回新路径"""
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(src_path))
    try:
        shutil.move(src_path, dest_path)
    except shutil.Error:
        # 跨设备移动失败时，用 copy + delete
        shutil.copy2(src_path, dest_path)
        os.remove(src_path)
    return dest_path


def _load_task(task_path: str) -> dict:
    with open(task_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _migrate_legacy_cache_state(model_state_dir: str):
    """
    将旧的按天文件（如 2026-04-10.txt）合并迁移到 merged.txt。
    仅在 merged.txt 不存在且有旧按天文件时执行。
    """
    merged_file = os.path.join(model_state_dir, "merged.txt")
    if os.path.exists(merged_file):
        return

    legacy_files = sorted(
        f for f in os.listdir(model_state_dir)
        if re.match(r'\d{4}-\d{2}-\d{2}\.txt$', f)
    )
    if not legacy_files:
        return

    try:
        with open(merged_file, "w", encoding="utf-8") as out:
            for fname in legacy_files:
                fpath = os.path.join(model_state_dir, fname)
                with open(fpath, "r", encoding="utf-8") as inp:
                    shutil.copyfileobj(inp, out)
                logger.info(f"[migrate] 合并 {fname} -> merged.txt")
        # 删除旧文件
        for fname in legacy_files:
            os.remove(os.path.join(model_state_dir, fname))
        logger.info(f"[migrate] {model_state_dir}: {len(legacy_files)} 个旧文件已迁移到 merged.txt")
    except Exception as e:
        logger.warning(f"[migrate] {model_state_dir}: 迁移失败: {e}")


def _trim_merged_file(merged_file: str, max_sections: int = 720):
    """
    裁剪 merged 文件中的旧 section，保留最近 max_sections 个 section。

    按行扫描找到所有 __SECTION__ 标记的位置，丢弃超过 max_sections 的旧 section。
    原子写入替换原文件。
    """
    try:
        with open(merged_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception:
        return

    section_starts = []  # (line_index, section_name)
    for i, line in enumerate(lines):
        if line.startswith("__SECTION__:"):
            section_starts.append(i)

    if len(section_starts) <= max_sections:
        return  # 无需裁剪

    # 保留最后 max_sections 个 section（从 cut_index 开始）
    cut_index = section_starts[-max_sections]
    trimmed = lines[cut_index:]
    removed = len(section_starts) - max_sections

    # 原子写入
    tmp_path = f"{merged_file}.{os.getpid()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.writelines(trimmed)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, merged_file)
        logger.debug(f"[trim] {merged_file}: 移除 {removed} 个旧 section，保留 {max_sections}")
    except Exception as e:
        logger.warning(f"[trim] {merged_file}: 裁剪失败: {e}")
        try:
            os.remove(tmp_path)
        except OSError:
            pass


# ============================================================
# Pipeline: fetch → tokenize → simulate → trend → 写入每日文件
# ============================================================
async def _process_task(task: dict, daemon_client) -> dict:
    """
    执行完整 pipeline，返回 per-model hit_rate dict。
    返回格式: {"glm-5": 0.8485, "kimi-k2.5": 0.9012, ..., "整体": 0.8949}
    """
    task_id = task["task_id"]
    date_str = task["date"]
    minute_str = task["minute"]
    start_datetime = task["start_datetime"]
    end_datetime = task["end_datetime"]
    app_id = task.get("app_id", "")
    path = task.get("path", "")
    models = task.get("models", [])

    # 任务数据临时目录
    task_data_dir = os.path.join(REALTIME_DIR, "_task_data", task_id)
    os.makedirs(task_data_dir, exist_ok=True)
    hour_dir = os.path.join(task_data_dir, "00")
    os.makedirs(hour_dir, exist_ok=True)
    tokenized_dir = os.path.join(task_data_dir, "tokenized")
    os.makedirs(tokenized_dir, exist_ok=True)

    try:
        # ---- Stage 1: Fetch (ES) ----
        logger.info(f"[{task_id}] fetch: {start_datetime} ~ {end_datetime}")
        from src.domains.kv.svc import ESIndexService

        es_date = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        es = ESIndexService(es_date, app_id=app_id, path=path)

        cfg = _load_realtime_config()

        def _fetch_cb(count, msg):
            logger.info(f"[{task_id}] fetch: {msg}")

        try:
            result = await es.query_to_dir(
                start_datetime, end_datetime, hour_dir,
                status_callback=_fetch_cb,
                window_concurrency=cfg.get("pipeline_fetch_window_concurrency", 4),
                scroll_size=cfg.get("pipeline_es_scroll_size", 5000),
            )
            total_count = result["total_count"]
            jsonl_files = [fi["file"] for fi in result.get("files", []) if fi.get("count", 0) > 0]
            logger.info(f"[{task_id}] fetch done: {total_count} records, {len(jsonl_files)} files")
        finally:
            es.close()

        if not jsonl_files:
            logger.warning(f"[{task_id}] fetch: 无数据，跳过后续阶段")
            return None

        # ---- Stage 2: Tokenize ----
        logger.info(f"[{task_id}] tokenize: {len(jsonl_files)} files, models={models}")
        model_txt_files: Dict[str, List[str]] = {}

        for jsonl_file in jsonl_files:
            if not os.path.exists(jsonl_file) or os.path.getsize(jsonl_file) == 0:
                continue

            base_name = os.path.splitext(os.path.basename(jsonl_file))[0]
            slice_output_dir = os.path.join(tokenized_dir, base_name)
            os.makedirs(slice_output_dir, exist_ok=True)

            try:
                loop = asyncio.get_event_loop()
                summary = await loop.run_in_executor(
                    None,
                    lambda: _tokenize_via_daemon(
                        daemon_client, jsonl_file, slice_output_dir,
                        base_name, models,
                    )
                )
                if summary.get("status") == "completed":
                    for model, info in summary.get("models", {}).items():
                        txt_file = info.get("file", "")
                        if txt_file and os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                            model_txt_files.setdefault(model, []).append(txt_file)
            except Exception as e:
                logger.error(f"[{task_id}] tokenize failed for {jsonl_file}: {e}")

        if not model_txt_files:
            logger.warning(f"[{task_id}] tokenize: 无有效输出")
            return None

        logger.info(f"[{task_id}] tokenize done: {list(model_txt_files.keys())}")

        # ---- Stage 3: Trend (shared cache → hit_rate per model) ----
        logger.info(f"[{task_id}] trend: computing per-model hit rates (shared cache)")
        from concurrent.futures import ThreadPoolExecutor

        cache_calc_path = os.path.join(BASE_DIR, "src/domains/kv/cache_hit_rate/cache_calc")
        cache_size = cfg.get("pipeline_cache_size", 200000000)
        block_size = cfg.get("pipeline_block_size", 16)

        model_hit_rates: Dict[str, float] = {}

        def _calc_model_shared(item):
            model, txt_files = item
            try:
                # 1. 迁移旧按天文件（如果存在）并追加到运行时 merged 文件
                model_state_dir = os.path.join(REALTIME_DIR, "_cache_state", model)
                os.makedirs(model_state_dir, exist_ok=True)
                _migrate_legacy_cache_state(model_state_dir)
                merged_file = os.path.join(model_state_dir, "merged.txt")

                # 2. 先裁剪旧 section（在追加新数据前执行，防止文件无限增长）
                _trim_merged_file(merged_file, max_sections=720)

                with open(merged_file, "a", encoding="utf-8") as mf:
                    mf.write(f"__SECTION__:{minute_str}\n")
                    for txt_file in txt_files:
                        if os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                            with open(txt_file, "r", encoding="utf-8") as src:
                                shutil.copyfileobj(src, mf)

                # 2. 运行 cache_calc（共享 cache 状态跨分钟/跨天保持）
                cmd = [
                    cache_calc_path, "-f", merged_file,
                    "-s", str(cache_size),
                    "-b", str(block_size),
                    "-p", "true",
                ]
                import subprocess
                total_size_mb = os.path.getsize(merged_file) / (1024 * 1024)
                timeout = min(600, max(120, int(total_size_mb / 100) + 120))
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

                if result.returncode != 0:
                    logger.error(f"[{task_id}] trend cache_calc failed for {model}: {result.stderr[:200]}")
                    return model, None

                # 3. 解析最后一个 section 的 hit_rate
                last_hit_rate = None
                for line in result.stdout.strip().split("\n"):
                    if line.startswith("section:"):
                        import re as _re
                        m = _re.search(r'section_hit_rate:\s*([\d.]+)', line)
                        if m:
                            last_hit_rate = float(m.group(1))

                return model, last_hit_rate
            except Exception as e:
                logger.error(f"[{task_id}] trend cache_calc failed for {model}: {e}")
                return model, None

        with ThreadPoolExecutor(max_workers=len(model_txt_files)) as pool:
            for model, hr in pool.map(_calc_model_shared, model_txt_files.items()):
                if hr is not None:
                    model_hit_rates[model] = hr

        if not model_hit_rates:
            logger.warning(f"[{task_id}] trend: 无有效 hit_rate")
            return None

        # 计算"整体" = 各模型 hit_rate 算术平均
        rates = list(model_hit_rates.values())
        model_hit_rates["整体"] = round(sum(rates) / len(rates), 4)

        # 保留 4 位小数
        for k in model_hit_rates:
            model_hit_rates[k] = round(model_hit_rates[k], 4)

        logger.info(f"[{task_id}] trend done: {model_hit_rates}")
        return model_hit_rates

    finally:
        # 清理任务中间数据
        try:
            if os.path.exists(task_data_dir):
                shutil.rmtree(task_data_dir, ignore_errors=True)
        except Exception:
            pass
        gc.collect()


def _tokenize_via_daemon(daemon_client, input_file, output_dir, file_prefix, models) -> dict:
    """通过常驻 daemon 执行 tokenize（同步阻塞，由 executor 调用）"""
    dt_id = daemon_client.submit(
        input_file=input_file,
        output_dir=output_dir,
        file_prefix=file_prefix,
        batch_size=_load_realtime_config().get("pipeline_tokenize_batch_size", 1000),
        model_filter=models if models else None,
    )
    return daemon_client.wait(dt_id, timeout=3600.0)


# ============================================================
# 结果写入每日文件
# ============================================================
def _write_to_daily(date_str: str, minute_str: str, model_rates: Dict[str, float]):
    """将一个分钟的结果追加到每日 JSON 文件"""
    scenario = _load_realtime_config().get("scenario", "全场景_各模型")
    scenario_dir = os.path.join(REALTIME_DIR, scenario)
    os.makedirs(scenario_dir, exist_ok=True)
    daily_file = os.path.join(scenario_dir, f"{date_str}.json")

    # 文件锁 + 原子写入（CFS 要求 fd 有写权限才能加排他锁）
    lock_path = f"{daily_file}.lock"
    fd = open(lock_path, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)

        # 读取已有数据
        if os.path.exists(daily_file):
            try:
                with open(daily_file, "r", encoding="utf-8") as f:
                    daily_data = json.load(f)
            except (json.JSONDecodeError, Exception):
                daily_data = {"date": date_str, "data": {}}
        else:
            daily_data = {"date": date_str, "data": {}}

        daily_data.setdefault("data", {})

        # 追加新分钟数据
        daily_data["data"][minute_str] = model_rates
        daily_data["updated_at"] = _now_bjt()

        # 原子写入
        tmp_path = f"{daily_file}.{os.getpid()}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(daily_data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, daily_file)

        logger.info(f"[daily] 写入 {daily_file} {minute_str}: {model_rates}")
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
        try:
            os.remove(lock_path)
        except OSError:
            pass


# ============================================================
# 主循环
# ============================================================
async def _run_worker(once: bool = False, dry_run: bool = False):
    """Worker 主循环"""
    _ensure_dirs()

    # 启动 tokenize daemon
    cfg = _load_realtime_config()
    logger.info("正在启动 tokenize daemon...")

    if _SCRIPT_DIR not in sys.path:
        sys.path.insert(0, _SCRIPT_DIR)
    from kv_pipeline import TokenizeDaemonClient

    num_daemons = cfg.get("pipeline_tokenize_concurrency", 2)
    daemon_client = TokenizeDaemonClient(
        workers=cfg.get("pipeline_tokenize_workers", 7),
        batch_size=cfg.get("pipeline_tokenize_batch_size", 500),
        default_model=cfg.get("pipeline_default_model", "glm-5"),
        num_daemons=num_daemons,
    )
    daemon_client.start(timeout=300.0)
    logger.info(f"tokenize daemon 就绪 (daemons={num_daemons})")

    # 恢复 running/ 中的中断任务
    for f in glob.glob(os.path.join(QUEUE_RUNNING_DIR, "*.json")):
        logger.info(f"[recovery] 发现中断任务: {os.path.basename(f)}")
        _move_task(f, QUEUE_PENDING_DIR)

    # 并发度
    max_concurrency = cfg.get("pipeline_fetch_concurrency", 2)
    semaphore = asyncio.Semaphore(max_concurrency)
    active_tasks: set = set()

    processed = 0

    async def _handle_task(task_path: str, task: dict, task_fd):
        """处理单个任务的完整生命周期"""
        task_id = task.get("task_id", "unknown")
        minute_label = task.get("date", "") + " " + task.get("minute", "")

        # 幂等检查：该分钟已有结果则跳过
        minute_str = task.get("minute", "")
        date_str = task.get("date", "")
        if minute_str and date_str:
            scenario = _load_realtime_config().get("scenario", "全场景_各模型")
            daily_file = os.path.join(REALTIME_DIR, scenario, f"{date_str}.json")
            if os.path.exists(daily_file):
                try:
                    with open(daily_file, "r", encoding="utf-8") as f:
                        daily_data = json.load(f)
                    if minute_str in daily_data.get("data", {}):
                        logger.info(f"[{task_id}] 跳过（{minute_label} 已有结果）")
                        _move_task(task_path, QUEUE_DONE_DIR)
                        _unlock_task(task_fd)
                        return
                except Exception:
                    pass

        logger.info(f"[{task_id}] 开始处理: {minute_label}")

        if dry_run:
            logger.info(f"[{task_id}] [DRY-RUN] 跳过处理")
            _move_task(task_path, QUEUE_DONE_DIR)
            _unlock_task(task_fd)
            return

        # 移到 running
        running_path = _move_task(task_path, QUEUE_RUNNING_DIR)
        _unlock_task(task_fd)

        try:
            async with semaphore:
                model_rates = await _process_task(task, daemon_client)

            if model_rates:
                _write_to_daily(task["date"], task["minute"], model_rates)
                _move_task(running_path, QUEUE_DONE_DIR)
                logger.info(f"[{task_id}] 完成")
            else:
                _move_task(running_path, QUEUE_DONE_DIR)
                logger.info(f"[{task_id}] 完成（无数据）")

        except Exception as e:
            logger.error(f"[{task_id}] 失败: {e}", exc_info=True)
            try:
                task_data = _load_task(running_path)
                task_data["error"] = str(e)[:500]
                task_data["failed_at"] = _now_bjt()
                with open(running_path, "w", encoding="utf-8") as f:
                    json.dump(task_data, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            _move_task(running_path, QUEUE_FAILED_DIR)

        nonlocal processed
        processed += 1
        gc.collect()

    while not _shutdown_event.is_set():
        _touch_heartbeat()

        # 清理已完成的异步任务
        done = {t for t in active_tasks if t.done()}
        # 检查子任务异常
        for t in done:
            if t.exception():
                logger.error(f"子任务异常: {t.exception()}", exc_info=t.exception())
        active_tasks.difference_update(done)

        # 扫描 pending 任务
        pending_tasks = _scan_pending_tasks()

        # 启动新任务（受并发度限制）
        started = 0
        for t_path in pending_tasks:
            if _shutdown_event.is_set():
                break
            if len(active_tasks) >= max_concurrency:
                break
            task_fd = _try_lock_task(t_path)
            if task_fd is None:
                continue
            try:
                task = _load_task(t_path)
            except Exception:
                _unlock_task(task_fd)
                continue

            # 幂等检查：该分钟已有结果则跳过
            minute_s = task.get("minute", "")
            date_s = task.get("date", "")
            if minute_s and date_s:
                scenario = _load_realtime_config().get("scenario", "全场景_各模型")
                daily_file = os.path.join(REALTIME_DIR, scenario, f"{date_s}.json")
                if os.path.exists(daily_file):
                    try:
                        with open(daily_file, "r", encoding="utf-8") as f:
                            if minute_s in json.load(f).get("data", {}):
                                logger.info(f"[{task.get('task_id')}] 跳过（已有结果）")
                                _move_task(t_path, QUEUE_DONE_DIR)
                                _unlock_task(task_fd)
                                continue
                    except Exception:
                        pass

            at = asyncio.create_task(_handle_task(t_path, task, task_fd))
            active_tasks.add(at)
            started += 1

        if started > 0:
            logger.info(f"启动 {started} 个任务，活跃 {len(active_tasks)}/{max_concurrency}")

        # 让 event loop 执行子任务
        if active_tasks:
            await asyncio.sleep(POLL_INTERVAL)
        elif not once:
            _shutdown_event.wait(POLL_INTERVAL)

        if once and processed > 0:
            break

    # 关闭 daemon
    logger.info("正在关闭 tokenize daemon...")
    try:
        daemon_client.stop()
    except Exception:
        pass
    logger.info(f"Worker 退出，共处理 {processed} 个任务")


def _signal_handler(signum, frame):
    logger.info(f"收到信号 {signum}，将在当前任务完成后退出...")
    _shutdown_event.set()


def main():
    parser = argparse.ArgumentParser(description="实时 KV Cache 分析 Worker")
    parser.add_argument("--once", action="store_true",
                        help="处理一个任务后退出（用于调试）")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印任务信息，不执行 pipeline")
    args = parser.parse_args()

    logger.info(f"实时 Worker 启动 (once={args.once}, dry_run={args.dry_run})")
    logger.info(f"BASE_DIR: {BASE_DIR}")
    logger.info(f"REALTIME_DIR: {REALTIME_DIR}")

    # 注册信号处理
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    try:
        asyncio.run(_run_worker(once=args.once, dry_run=args.dry_run))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt，退出")


if __name__ == "__main__":
    main()
