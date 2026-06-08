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


def _bjt_time(timestamp=None):
    """将 UNIX 时间戳转换为北京时间 time.struct_time"""
    import time as _time
    dt = datetime.fromtimestamp(timestamp or _time.time(), tz=BJT)
    return dt.timetuple()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.Formatter.converter = staticmethod(_bjt_time)  # 全局强制北京时间
# 文件日志：按天轮转，写入 logs/realtime/
_LOG_DIR = os.path.join(BASE_DIR, "logs", "realtime")
os.makedirs(_LOG_DIR, exist_ok=True)
_file_handler = logging.handlers.TimedRotatingFileHandler(
    os.path.join(_LOG_DIR, "realtime_worker.log"),
    when="midnight", backupCount=30, encoding="utf-8",
)
_file_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
)
_file_fmt.converter = _bjt_time
_file_handler.setFormatter(_file_fmt)
logging.getLogger().addHandler(_file_handler)
logger = logging.getLogger("realtime_worker")

# Graceful shutdown flag
_shutdown_event = threading.Event()

# Heartbeat thread control
_heartbeat_stop = threading.Event()

# Per-model simulate lock（防止并发 cache_calc 写坏同一 checkpoint）
_simulate_locks: Dict[str, threading.Lock] = {}

# Prefetch pipeline：overlap fetch+tokenize of next task with simulate of current
_prefetch_cache: Dict[str, dict] = {}     # task_id → {"model_txt_files", "task_data_dir"}
_active_prefetch = None                   # Optional[asyncio.Task]


def _heartbeat_loop():
    """后台心跳线程：独立于事件循环定期更新心跳文件，防止 liveness probe 误判"""
    while not _heartbeat_stop.is_set():
        _touch_heartbeat()
        _heartbeat_stop.wait(15)


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


def _trim_merged_file(merged_file: str, max_sections: int = 720, max_size_mb: int = 2048):
    """
    裁剪 merged 文件中的旧 section，保留最近 max_sections 个 section。

    使用流式扫描定位 __SECTION__ 标记（不将全文加载到内存）。
    当 section 数超过 max_sections 或文件大小超过 max_size_mb 时触发裁剪。
    原子写入替换原文件。
    """
    try:
        file_size = os.path.getsize(merged_file)
    except OSError:
        return

    # 流式扫描 section 起始位置（不加载全文到内存）
    section_offsets = []  # byte offset of each __SECTION__ line
    try:
        with open(merged_file, "rb") as f:
            offset = 0
            for line in f:
                if line.startswith(b"__SECTION__:"):
                    section_offsets.append(offset)
                offset += len(line)
    except Exception:
        return

    need_trim = len(section_offsets) > max_sections or file_size > max_size_mb * 1024 * 1024
    if not need_trim:
        return  # 无需裁剪

    # 确定裁剪的起始字节偏移
    if len(section_offsets) > max_sections:
        # 按 section 数量裁剪：保留最后 max_sections 个
        cut_offset = section_offsets[-max_sections]
    else:
        # 按文件大小裁剪：逐步丢弃旧 section 直到满足大小限制
        # 目标：删除最旧的 section，使剩余大小 <= max_size_mb
        target_size = max_size_mb * 1024 * 1024
        # 从最新 section 往前数，找到满足大小限制的最早 section
        cut_idx = 0
        for i in range(len(section_offsets)):
            remaining = file_size - section_offsets[i]
            if remaining <= target_size:
                cut_idx = i
                break
        else:
            cut_idx = len(section_offsets) - 1  # 至少保留最后一个 section
        cut_offset = section_offsets[cut_idx]

    removed = sum(1 for o in section_offsets if o < cut_offset)

    # 从 cut_offset 开始复制到新文件（流式，不加载全文）
    tmp_path = f"{merged_file}.{os.getpid()}.tmp"
    try:
        with open(merged_file, "rb") as src, open(tmp_path, "wb") as dst:
            src.seek(cut_offset)
            shutil.copyfileobj(src, dst, length=64 * 1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp_path, merged_file)
        logger.info(f"[trim] {merged_file}: 移除 {removed} 个旧 section，"
                     f"大小 {file_size / (1024*1024):.0f}MB → {os.path.getsize(merged_file) / (1024*1024):.0f}MB")
    except Exception as e:
        logger.warning(f"[trim] {merged_file}: 裁剪失败: {e}")
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def _get_checkpoint_path(model: str, cfg: dict) -> tuple:
    """
    返回 (primary_checkpoint_path, cfs_backup_path)。

    当 tmpfs 可用时，主路径指向 tmpfs（快速），CFS 作为备份（持久）。
    当 tmpfs 不可用时（本地开发），主路径直接指向 CFS，无需备份。
    """
    tmpfs_dir = cfg.get("checkpoint_tmpfs_dir", "")
    cfs_dir = os.path.join(REALTIME_DIR, "_cache_state", model)
    cfs_path = os.path.join(cfs_dir, "cache_checkpoint.bin")

    if tmpfs_dir and os.path.isdir(tmpfs_dir):
        primary = os.path.join(tmpfs_dir, model, "cache_checkpoint.bin")
        return primary, cfs_path
    else:
        return cfs_path, None


def _restore_checkpoints_from_cfs(models: list, cfg: dict):
    """
    Pod 重启后，从 CFS 备份恢复 checkpoint 到 tmpfs。
    仅在 tmpfs 可用时执行；否则 checkpoint 已在 CFS 上，无需恢复。
    """
    tmpfs_dir = cfg.get("checkpoint_tmpfs_dir", "")
    if not tmpfs_dir or not os.path.isdir(tmpfs_dir):
        return

    for model in models:
        tmpfs_cp = os.path.join(tmpfs_dir, model, "cache_checkpoint.bin")
        if os.path.exists(tmpfs_cp) and os.path.getsize(tmpfs_cp) > 0:
            size_mb = os.path.getsize(tmpfs_cp) / (1024 * 1024)
            logger.info(f"[restore] {model}: tmpfs checkpoint 已存在 ({size_mb:.0f}MB)，跳过")
            continue

        cfs_cp = os.path.join(REALTIME_DIR, "_cache_state", model, "cache_checkpoint.bin")
        if os.path.exists(cfs_cp) and os.path.getsize(cfs_cp) > 0:
            os.makedirs(os.path.dirname(tmpfs_cp), exist_ok=True)
            try:
                shutil.copy2(cfs_cp, tmpfs_cp)
                size_mb = os.path.getsize(tmpfs_cp) / (1024 * 1024)
                logger.info(f"[restore] {model}: 从 CFS 恢复 checkpoint ({size_mb:.0f}MB)")
            except Exception as e:
                logger.warning(f"[restore] {model}: 从 CFS 恢复失败: {e}")
        else:
            logger.info(f"[restore] {model}: 无 CFS 备份，将从空 cache 开始")


def _backup_checkpoint_to_cfs(tmpfs_checkpoint_path: str, model: str):
    """
    备份 tmpfs checkpoint 到 CFS（后台线程调用）。
    使用原子写入（.tmp → rename），防止备份期间读到不完整文件。
    """
    cfs_checkpoint_path = os.path.join(REALTIME_DIR, "_cache_state", model, "cache_checkpoint.bin")
    try:
        if not os.path.exists(tmpfs_checkpoint_path) or os.path.getsize(tmpfs_checkpoint_path) == 0:
            return
        os.makedirs(os.path.dirname(cfs_checkpoint_path), exist_ok=True)
        tmp_path = f"{cfs_checkpoint_path}.{os.getpid()}.tmp"
        shutil.copy2(tmpfs_checkpoint_path, tmp_path)
        os.replace(tmp_path, cfs_checkpoint_path)
        size_mb = os.path.getsize(cfs_checkpoint_path) / (1024 * 1024)
        logger.info(f"[backup] {model}: checkpoint 已备份到 CFS ({size_mb:.0f}MB)")
    except Exception as e:
        logger.warning(f"[backup] {model}: 备份失败: {e}")


# ============================================================
# Stage 1.5: 切片（Fetch → Tokenize 之间）
# ============================================================
def _split_jsonl_files(jsonl_files, num_splits, output_dir, task_id):
    """
    按时序连续切片，源头 ES 已按 api_name 过滤，不再按 qianfan_model 二次过滤。

    ★ 保序性：cache_calc LRU 模拟要求输入严格按时间递增。
    采用两遍扫描 + 连续分块（非 Round-Robin），确保：
    - 每个切片内部保持源 JSONL 的时序
    - 切片 0 的所有行 < 切片 1 的所有行（时间上严格连续）
    - 合并切片结果时按编号顺序拼接即可还原完整时序

    ★ 流式读写：全程逐行 for line in f，无整文件加载，无 OOM 风险。
    """
    valid_files = [f for f in jsonl_files if os.path.exists(f) and os.path.getsize(f) > 0]
    if not valid_files:
        return []

    total_count = 0
    for jf in valid_files:
        with open(jf, "r", encoding="utf-8") as f:
            for _ in f:
                total_count += 1

    if total_count == 0:
        logger.info(f"[{task_id}] split: 0 行")
        return []

    chunk_size = -(-total_count // num_splits)  # ceil division
    split_files = []
    chunk_idx = 0
    written_in_chunk = 0
    fh = None

    try:
        for jf in valid_files:
            with open(jf, "r", encoding="utf-8") as f:
                for line in f:
                    if fh is None or (written_in_chunk >= chunk_size and len(split_files) < num_splits):
                        if fh is not None:
                            fh.close()
                        path = os.path.join(output_dir, f"_split_{chunk_idx}.jsonl")
                        split_files.append(path)
                        fh = open(path, "w", encoding="utf-8")
                        written_in_chunk = 0
                        chunk_idx += 1
                    fh.write(line)
                    written_in_chunk += 1
    finally:
        if fh is not None:
            fh.close()

    logger.info(f"[{task_id}] split: {total_count} 条连续切为 {len(split_files)} 个文件 (chunk_size={chunk_size})")

    return [f for f in split_files if os.path.exists(f) and os.path.getsize(f) > 0]


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
        es = ESIndexService(es_date, app_id=app_id, path=path, models=models)

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

        # ---- Stage 1.5: Split ----
        # ★ 通过 run_in_executor 让出事件循环，避免阻塞其他任务的回调
        num_daemons = cfg.get("pipeline_tokenize_concurrency", 2)
        split_dir = os.path.join(task_data_dir, "split")
        os.makedirs(split_dir, exist_ok=True)

        loop = asyncio.get_event_loop()
        split_files = await loop.run_in_executor(
            None, _split_jsonl_files, jsonl_files, num_daemons, split_dir, task_id
        )

        if not split_files:
            logger.warning(f"[{task_id}] split: 切片后无数据，跳过 tokenize")
            return None

        # ---- Stage 2: Tokenize (并行 N 个 daemon) ----
        logger.info(f"[{task_id}] tokenize: {len(split_files)} split files, models={models}")

        from concurrent.futures import ThreadPoolExecutor as _TPool

        def _tokenize_one_split(split_file):
            base_name = os.path.splitext(os.path.basename(split_file))[0]
            out_dir = os.path.join(tokenized_dir, base_name)
            os.makedirs(out_dir, exist_ok=True)
            return _tokenize_via_daemon(daemon_client, split_file, out_dir, base_name)

        def _run_tokenize_all():
            """在线程池中并行 tokenize 所有 split 文件，返回 model_txt_files dict"""
            result_map: Dict[str, List[str]] = {}
            with _TPool(max_workers=len(split_files)) as pool:
                future_map = {pool.submit(_tokenize_one_split, f): f for f in split_files}
                for future in future_map:
                    split_file = future_map[future]
                    try:
                        summary = future.result()
                        if summary.get("status") == "completed":
                            for model, info in summary.get("models", {}).items():
                                txt_file = info.get("file", "")
                                if txt_file and os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                                    result_map.setdefault(model, []).append(txt_file)
                    except Exception as e:
                        logger.error(f"[{task_id}] tokenize failed for {split_file}: {e}")
            return result_map

        # ★ 通过 run_in_executor 让出事件循环，避免阻塞其他任务
        model_txt_files = await loop.run_in_executor(None, _run_tokenize_all)

        # 清理切片临时文件
        import shutil
        shutil.rmtree(split_dir, ignore_errors=True)

        if not model_txt_files:
            logger.warning(f"[{task_id}] tokenize: 无有效输出")
            return None

        logger.info(f"[{task_id}] tokenize done: {list(model_txt_files.keys())}")

        # ---- Stage 3: Trend (shared cache → hit_rate per model) ----
        logger.info(f"[{task_id}] trend: computing per-model hit rates (shared cache)")
        from concurrent.futures import ThreadPoolExecutor

        cache_calc_path = os.path.join(BASE_DIR, "src/domains/kv/cache_hit_rate/cache_calc")
        cache_size = cfg.get("pipeline_cache_size", 200000000)
        block_size = cfg.get("pipeline_block_size", 64)

        model_hit_rates: Dict[str, float] = {}

        def _calc_model_shared(item):
            model, txt_files = item
            try:
                model_state_dir = os.path.join(REALTIME_DIR, "_cache_state", model)
                os.makedirs(model_state_dir, exist_ok=True)
                _migrate_legacy_cache_state(model_state_dir)
                checkpoint_path, cfs_backup_path = _get_checkpoint_path(model, cfg)

                # 确保 checkpoint 目录存在
                os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)

                # 优先使用 checkpoint 模式（增量计算，O(1) 每分钟）
                use_checkpoint = os.path.exists(cache_calc_path) and os.path.getsize(cache_calc_path) > 0

                if use_checkpoint:
                    # 构造 section 数据（header + txt 文件内容）
                    section_data = f"__SECTION__:{minute_str}\n"
                    for txt_file in txt_files:
                        if os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                            with open(txt_file, "r", encoding="utf-8") as src:
                                section_data += src.read()

                    # stdin 管道模式：数据通过管道传给 cache_calc，不写 CFS section 文件
                    cmd = [
                        cache_calc_path, "-f", "-",
                        "-s", str(cache_size),
                        "-b", str(block_size),
                        "-p", "true",
                        "-c", checkpoint_path,
                    ]
                    import subprocess
                    timeout = 600  # checkpoint 模式下数据量小，固定 10 分钟超时
                    result = subprocess.run(
                        cmd, input=section_data,
                        capture_output=True, text=True, timeout=timeout,
                    )

                    if result.returncode != 0:
                        logger.error(f"[{task_id}] trend cache_calc failed for {model}: {result.stderr[:200]}")
                        return model, None

                    # 解析 section_hit_rate
                    last_hit_rate = None
                    for line in result.stdout.strip().split("\n"):
                        if line.startswith("section:"):
                            import re as _re
                            m = _re.search(r'section_hit_rate:\s*([\d.]+)', line)
                            if m:
                                last_hit_rate = float(m.group(1))

                    logger.info(f"[{task_id}] trend {model} (checkpoint+stdin): hit_rate={last_hit_rate}")

                    # 异步备份 checkpoint 到 CFS（不阻塞主流程）
                    if cfs_backup_path:
                        threading.Thread(
                            target=_backup_checkpoint_to_cfs,
                            args=(checkpoint_path, model),
                            daemon=True,
                        ).start()

                    return model, last_hit_rate

                # Fallback: 无 checkpoint 支持，回退到 merged.txt 全量模式
                merged_file = os.path.join(model_state_dir, "merged.txt")
                _trim_merged_file(merged_file, max_sections=720)

                with open(merged_file, "a", encoding="utf-8") as mf:
                    mf.write(f"__SECTION__:{minute_str}\n")
                    for txt_file in txt_files:
                        if os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                            with open(txt_file, "r", encoding="utf-8") as src:
                                shutil.copyfileobj(src, mf)

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

                # 解析最后一个 section 的 hit_rate
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

        def _calc_all_models(items):
            """在线程池中并行计算所有模型的 hit_rate（不阻塞事件循环）"""
            with ThreadPoolExecutor(max_workers=len(items)) as pool:
                return list(pool.map(_calc_model_shared, items))

        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, _calc_all_models, list(model_txt_files.items()))
        for model, hr in results:
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


def _tokenize_via_daemon(daemon_client, input_file, output_dir, file_prefix) -> dict:
    """通过常驻 daemon 执行 tokenize（同步阻塞，由 executor 调用）"""
    dt_id = daemon_client.submit(
        input_file=input_file,
        output_dir=output_dir,
        file_prefix=file_prefix,
        batch_size=_load_realtime_config().get("pipeline_tokenize_batch_size", 1000),
        model_filter=None,
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

    # 启动后台心跳线程（独立于事件循环，防止阻塞导致 liveness probe 失败）
    _heartbeat_stop.clear()
    _heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat")
    _heartbeat_thread.start()
    logger.info("心跳线程已启动 (间隔 15s)")

    # 启动 tokenize daemon
    cfg = _load_realtime_config()
    logger.info("正在启动 tokenize daemon...")

    # 从 CFS 恢复 checkpoint 到 tmpfs（Pod 重启后）
    models = [m.strip() for m in cfg.get("models", "").split(",") if m.strip()]
    _restore_checkpoints_from_cfs(models, cfg)

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

    # 停止心跳线程
    _heartbeat_stop.set()
    _heartbeat_thread.join(timeout=5)

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
