#!/usr/bin/env python3
"""
K8s Job 入口脚本：独立运行完整 OLAP pipeline (fetch → tokenize → simulate)。

从 olap.py 抽取 _run_pipeline 全流程逻辑，作为 K8s Job Pod 的 entrypoint。
所有进度通过 CFS 上的 status.json 文件上报，FastAPI 端轮询读取，前端零改动。

用法:
    python scripts/run_pipeline.py \
        --task-id {task_id} \
        --username {username} \
        --start-datetime "2026-03-28 00:00:00" \
        --end-datetime "2026-03-29 00:00:00" \
        --app-id app-3Lut8O2E \
        --path "/v2/coding/chat/completions" \
        --models "glm-5,deepseek-v3.2"
"""

import argparse
import asyncio
import gc
import glob
import json
import logging
import os
import re
import sys
import threading

from collections import deque
from datetime import datetime, timedelta, timezone

# ============================================================
# 路径 & 配置
# ============================================================
# 优先从脚本位置推导项目根目录（scripts/ 的父目录），兜底用环境变量
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(_SCRIPT_DIR)  # scripts/../ = 项目根目录
# 确保项目根目录在 sys.path 中，以便 import src.* / context.* 等模块
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
OLAP_DATABASE_DIR = os.path.join(BASE_DIR, "olap_database")
KV_DATA_DIR = os.path.join(OLAP_DATABASE_DIR, "data")
KV_STATUS_DIR = os.path.join(OLAP_DATABASE_DIR, "status")
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
OLAP_CONFIG_JSON = os.path.join(BASE_DIR, "app", "conf", "olap_config.json")

# cache_calc 二进制路径：优先使用 Docker 镜像内的版本（与容器 GLIBC 兼容），
# fallback 到 CFS 上的版本（本地/CLI 使用）
_DOCKER_CACHE_CALC = "/workspace/src/domains/kv/cache_hit_rate/cache_calc"
_CFS_CACHE_CALC = os.path.join(BASE_DIR, "src/domains/kv/cache_hit_rate/cache_calc")
CACHE_CALC_PATH = _DOCKER_CACHE_CALC if os.path.isfile(_DOCKER_CACHE_CALC) and os.access(_DOCKER_CACHE_CALC, os.X_OK) else _CFS_CACHE_CALC

BJT = timezone(timedelta(hours=8))


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
logger = logging.getLogger("run_pipeline")

# es_query logger 在 logging_config.py 中设置了 propagate=False，
# K8s Job 环境不加载该配置，需手动配置 handler
_es_logger = logging.getLogger("es_query")
_es_logger.setLevel(logging.INFO)
_es_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_es_fmt.converter = _bjt_time
if not _es_logger.handlers:
    # stderr — kubectl logs 可见
    _es_stderr = logging.StreamHandler()
    _es_stderr.setFormatter(_es_fmt)
    _es_logger.addHandler(_es_stderr)


def _setup_es_log_file(task_id: str):
    """在 main() 解析出 task_id 后调用，添加文件 handler。"""
    _es_log_dir = os.path.join(BASE_DIR, "es_logs")
    os.makedirs(_es_log_dir, exist_ok=True)
    _es_log_file = os.path.join(
        _es_log_dir,
        f"{task_id}_es_query.log",
    )
    _es_file = logging.FileHandler(_es_log_file, encoding="utf-8")
    _es_file.setFormatter(_es_fmt)
    _es_logger.addHandler(_es_file)


# ============================================================
# 工具函数（与 olap.py 保持一致）
# ============================================================
def _now_bjt() -> str:
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")


_OLAP_DEFAULTS = {
    "pipeline_default_model": "glm-5",
    "pipeline_block_size": 16,
    "pipeline_cache_size": 200000000,
    "pipeline_tokenize_concurrency": 4,
    "pipeline_fetch_concurrency": 2,
    "pipeline_fetch_window_concurrency": 24,
    "pipeline_es_scroll_workers": 30,
    "pipeline_tokenize_workers": 4,
    "pipeline_tokenize_batch_size": 200,
    "pipeline_slice_minutes": 60,
    "pipeline_default_path": "/v2/coding/chat/completions",
    "olap_qpd_limit": 1,
    "models": [],
}


def _load_olap_config() -> dict:
    try:
        with open(OLAP_CONFIG_JSON, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        for k, v in _OLAP_DEFAULTS.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception:
        return dict(_OLAP_DEFAULTS)


def _extract_username(task_id: str) -> str:
    if "-kv_" in task_id:
        return task_id.split("-kv_", 1)[0]
    return "unknown"


def _status_file(task_id: str) -> str:
    username = _extract_username(task_id)
    d = os.path.join(KV_STATUS_DIR, username)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{task_id}.json")


def _task_dir(task_id: str) -> str:
    username = _extract_username(task_id)
    d = os.path.join(KV_DATA_DIR, username, task_id)
    os.makedirs(d, exist_ok=True)
    return d


def _read_status(task_id: str) -> dict | None:
    path = _status_file(task_id)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_status(data: dict):
    data["updated_at"] = _now_bjt()
    path = _status_file(data["task_id"])
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


# Status 更新 — 同步版（Job Pod 内单进程，无需 asyncio.Lock）
def _update_stage(task_id: str, stage: str, stage_data: dict, current_stage: str = None):
    status = _read_status(task_id)
    if not status:
        return
    if current_stage:
        status["pipeline"]["current_stage"] = current_stage

    now = _now_bjt()
    existing = status["pipeline"]["stages"].get(stage, {})
    if stage_data.get("status") == "running" and "started_at" not in existing:
        stage_data["started_at"] = now
    elif "started_at" in existing:
        stage_data.setdefault("started_at", existing["started_at"])
    if stage_data.get("status") in ("completed", "failed"):
        stage_data["completed_at"] = now

    status["pipeline"]["stages"][stage] = stage_data
    _write_status(status)


def _set_result(task_id: str, result: dict):
    status = _read_status(task_id)
    if not status:
        return
    status["pipeline"]["current_stage"] = "done"
    status["result"] = result
    _write_status(status)


def _set_failed(task_id: str, stage: str, error_msg: str):
    status = _read_status(task_id)
    if not status:
        return
    status["pipeline"]["current_stage"] = "failed"
    status["pipeline"]["stages"][stage] = {"status": "failed", "message": error_msg}
    _write_status(status)


# ============================================================
# Stage 1.5: 逐文件原地预过滤（Fetch → Tokenize 之间）
# ============================================================
def _prefilter_inplace(jsonl_files, model_filter):
    """
    逐文件原地预过滤：只保留目标模型行，直接覆写原文件。

    保留原始 kv_YYYYMMDD_HHMMSS 文件名 → trend 时间轴正常。
    全程逐行流式处理，无整文件加载，无 OOM 风险。

    :param jsonl_files: 原始 per-minute jsonl 文件路径列表
    :param model_filter: 模型名集合，如 {"glm-5", "kimi-k2.5"}；为空则不过滤
    :return: 过滤后非空文件路径列表（保留原始文件名）
    """
    import re as _re

    if not model_filter:
        return [f for f in jsonl_files if os.path.exists(f) and os.path.getsize(f) > 0]

    escaped = [_re.escape(m) for m in model_filter]
    pattern = _re.compile(r'qianfan_model:(' + '|'.join(escaped) + r')(?:[^a-zA-Z0-9._-]|$)')

    total_count = 0
    matched_count = 0
    result_files = []

    for jf in jsonl_files:
        if not os.path.exists(jf) or os.path.getsize(jf) == 0:
            continue
        tmp_path = jf + ".tmp"
        file_matched = 0
        try:
            with open(jf, "r", encoding="utf-8") as fin, \
                 open(tmp_path, "w", encoding="utf-8", buffering=4 * 1024 * 1024) as fout:
                for line in fin:
                    total_count += 1
                    if pattern.search(line):
                        fout.write(line)
                        file_matched += 1
            matched_count += file_matched
            if file_matched > 0:
                os.replace(tmp_path, jf)
                result_files.append(jf)
            else:
                os.remove(tmp_path)
                os.remove(jf)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    filter_pct = matched_count * 100 // max(total_count, 1)
    logger.info(f"[prefilter] {total_count} → {matched_count} 条 ({filter_pct}%), "
                f"保留 {len(result_files)} 个 per-minute 文件")

    return result_files


# ============================================================
# Stage 1+2: fetch → prefilter → tokenize (streaming pipeline)
# ============================================================
async def _run_streaming_fetch_tokenize(
    task_id: str, start_datetime: str, end_datetime: str, app_id: str, path: str = ""
):
    _update_stage(task_id, "fetch", {"status": "running", "message": "正在查询 ES 数据..."}, "fetch")
    _update_stage(task_id, "tokenize", {"status": "pending", "message": "等待数据..."})

    # 延迟导入（仅在 Job Pod 中需要，且避免 FastAPI 启动时加载）
    sys.path.insert(0, BASE_DIR)
    from src.domains.kv.svc import ESIndexService

    start_dt = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    task_data_dir = _task_dir(task_id)
    output_dir = os.path.join(task_data_dir, "tokenized")
    os.makedirs(output_dir, exist_ok=True)

    # 按小时拆分
    hours = []
    current = start_dt.replace(minute=0, second=0)
    if current < start_dt:
        current = start_dt
    while current < end_dt:
        hour_end = min(current.replace(minute=0, second=0) + timedelta(hours=1), end_dt)
        hours.append((current, hour_end))
        current = hour_end

    total_slices = len(hours)

    # 共享状态
    fetch_results = []
    fetch_incomplete = []
    tokenize_results = []
    fetch_total_count = [0]
    fetch_done_count = [0]
    tokenize_done_count = [0]
    tokenize_total_lines = [0]
    tokenize_total_seconds = [0.0]
    tokenize_total_submitted = [0]
    _cb_msg = [None]
    _progress_dirty = [False]

    cfg = _load_olap_config()
    fetch_sem = asyncio.Semaphore(cfg["pipeline_fetch_concurrency"])
    tokenize_sem = asyncio.Semaphore(cfg["pipeline_tokenize_concurrency"])
    tokenize_tasks = []

    # simulate 不再流式逐文件执行（1440 次 -f -c 调用太慢），
    # 改为 tokenize 全部完成后由 _run_simulate_stage 统一走 -L 批量模式

    # 预过滤使用的模型列表（与 tokenize daemon 一致）
    _status_data = _read_status(task_id)
    selected_models = (_status_data.get("query", {}).get("models", []) if _status_data else [])
    if not selected_models:
        selected_models = cfg.get("models", [])
    prefilter_model_set = set(selected_models) if selected_models else set()

    # 启动共享 tokenize daemon —— 整个 pipeline 生命周期内 tokenizer 只加载一次
    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    from kv_pipeline import TokenizeDaemonClient  # noqa: E402
    num_daemons = cfg["pipeline_tokenize_concurrency"]
    tokenize_daemon = TokenizeDaemonClient(
        workers=cfg["pipeline_tokenize_workers"],
        batch_size=cfg["pipeline_tokenize_batch_size"],
        default_model=cfg["pipeline_default_model"],
        num_daemons=num_daemons,
    )
    logger.info(f"[daemon] 启动 {num_daemons} 个 tokenize daemon (workers_each={cfg['pipeline_tokenize_workers']})...")
    tokenize_daemon.start(timeout=300.0)
    logger.info(f"[daemon] {num_daemons} 个 daemon 就绪，切片级真并行")

    def _update_fetch_progress():
        msg_parts = [f"拉取进度 {fetch_done_count[0]}/{total_slices}，已获取 {fetch_total_count[0]} 条"]
        if fetch_incomplete:
            msg_parts.append(f"，{len(fetch_incomplete)} 个失败")
        _update_stage(task_id, "fetch", {
            "status": "running",
            "message": "".join(msg_parts),
            "processed_count": fetch_total_count[0],
            "progress": f"{fetch_done_count[0]}/{total_slices}",
        })

    def _update_tokenize_progress():
        if tokenize_done_count[0] == 0:
            return
        total_tokenize_files = max(tokenize_total_submitted[0], 1)
        stage_data = {
            "status": "running",
            "message": f"序列化进度 {tokenize_done_count[0]}/{total_tokenize_files}",
            "progress": f"{tokenize_done_count[0]}/{total_tokenize_files}",
            "total_lines": tokenize_total_lines[0],
        }
        if tokenize_total_seconds[0] > 0 and tokenize_total_lines[0] > 0:
            speed = tokenize_total_lines[0] / tokenize_total_seconds[0]
            stage_data["tokenize_speed"] = round(speed, 2)
        _update_stage(task_id, "tokenize", stage_data)

    async def _progress_flusher():
        while True:
            await asyncio.sleep(2)
            if _progress_dirty[0] and _cb_msg[0]:
                _progress_dirty[0] = False
                _update_stage(task_id, "fetch", {
                    "status": "running",
                    "message": _cb_msg[0],
                    "processed_count": fetch_total_count[0],
                })

    flusher_task = asyncio.create_task(_progress_flusher())

    async def _fetch_slice(idx: int, h_start: datetime, h_end: datetime):
        h_start_str = h_start.strftime("%Y-%m-%d %H:%M:%S")
        h_end_str = h_end.strftime("%Y-%m-%d %H:%M:%S")
        hour_dir_name = h_start.strftime("%H")
        hour_dir = os.path.join(task_data_dir, hour_dir_name)
        os.makedirs(hour_dir, exist_ok=True)

        max_scroll_retries = 8
        for scroll_attempt in range(max_scroll_retries + 1):
            es = ESIndexService(h_start.strftime("%Y-%m-%d"), app_id=app_id, path=path)

            async with fetch_sem:
                _update_fetch_progress()

                def _cb(count, msg):
                    _cb_msg[0] = f"[{idx + 1}/{total_slices}] {msg}"
                    _progress_dirty[0] = True

                try:
                    result = await es.query_to_dir(
                        h_start_str, h_end_str, hour_dir, status_callback=_cb,
                        window_concurrency=cfg.get("pipeline_fetch_window_concurrency", 24)
                    )
                    hour_count = result["total_count"]
                    fetch_total_count[0] += hour_count
                    fetch_done_count[0] += 1

                    # 收集本 hour 的 per-minute 文件
                    hour_jsonl_files = []
                    for fi in result["files"]:
                        fetch_results.append({
                            "file": fi["file"],
                            "hour": f"{h_start_str}~{h_end_str}",
                            "minute": fi["minute"],
                            "count": fi["count"]
                        })
                        if fi["count"] > 0 and os.path.exists(fi["file"]):
                            hour_jsonl_files.append(fi["file"])

                    # ---- Stage 1.5: 逐文件原地预过滤 ----
                    # 用 regex 过滤目标模型，去除 99%+ 无效记录，
                    # 保留原始 per-minute 文件名（trend 时间轴依赖）
                    if hour_jsonl_files:
                        loop = asyncio.get_event_loop()
                        filtered_files = await loop.run_in_executor(
                            None, _prefilter_inplace,
                            hour_jsonl_files, prefilter_model_set
                        )
                        for sf in filtered_files:
                            tokenize_total_submitted[0] += 1
                            t = asyncio.create_task(
                                _tokenize_single_with_tracking(sf, output_dir, task_id)
                            )
                            tokenize_tasks.append(t)

                    _update_fetch_progress()
                    es.close()
                    return  # 成功，退出重试循环

                except Exception as e:
                    es.close()
                    err_msg = str(e)
                    is_scroll_err = "too many scroll contexts" in err_msg.lower() or "Trying to create too many scroll contexts" in err_msg
                    if is_scroll_err and scroll_attempt < max_scroll_retries:
                        wait_secs = min(60 * (scroll_attempt + 1), 300)
                        logger.warning(
                            f"[fetch] slice {h_start_str}~{h_end_str} scroll limit hit "
                            f"(attempt {scroll_attempt + 1}/{max_scroll_retries}), "
                            f"waiting {wait_secs}s before retry..."
                        )
                        _cb_msg[0] = f"[{idx + 1}/{total_slices}] scroll 超限，等待 {wait_secs}s 后重试 ({scroll_attempt + 1}/{max_scroll_retries})..."
                        _progress_dirty[0] = True
                        break

                    fetch_done_count[0] += 1
                    fetch_incomplete.append({
                        "hour": f"{h_start_str}~{h_end_str}",
                        "error": str(e)[:200]
                    })
                    logger.warning(f"[fetch] slice {h_start_str}~{h_end_str} failed: {e}")
                    _update_fetch_progress()
                    return
            # end async with fetch_sem

            if scroll_attempt < max_scroll_retries:
                wait_secs = min(60 * (scroll_attempt + 1), 300)
                await asyncio.sleep(wait_secs)
                continue

        # 所有重试用尽仍失败
        fetch_done_count[0] += 1
        fetch_incomplete.append({
            "hour": f"{h_start_str}~{h_end_str}",
            "error": f"scroll context limit exceeded after {max_scroll_retries} retries"
        })
        logger.warning(f"[fetch] slice {h_start_str}~{h_end_str} scroll limit retries exhausted")
        _update_fetch_progress()

    async def _tokenize_single_with_tracking(input_file: str, out_dir: str, tid: str):
        async with tokenize_sem:
                _update_tokenize_progress()
                result = await _run_tokenize_via_daemon(tokenize_daemon, input_file, out_dir, tid)
                tokenize_results.append(result)
                tokenize_done_count[0] += 1
                if result["status"] == "completed":
                    tokenize_total_lines[0] += result.get("lines", 0)
                    tokenize_total_seconds[0] += result.get("duration_seconds", 0.0)
                _update_tokenize_progress()

    # 启动所有 fetch（并行，受信号量控制）
    fetch_tasks_list = [
        asyncio.create_task(_fetch_slice(idx, h_start, h_end))
        for idx, (h_start, h_end) in enumerate(hours)
    ]
    try:
        await asyncio.gather(*fetch_tasks_list)
    finally:
        flusher_task.cancel()
        fetch_tasks_list.clear()

    # 更新 fetch 最终状态
    fetch_status = {
        "status": "completed",
        "message": f"查询完成，共 {fetch_total_count[0]} 条",
        "total_count": fetch_total_count[0],
        "total_files": len(fetch_results),
    }
    if fetch_incomplete:
        fetch_status["incomplete_count"] = len(fetch_incomplete)
        fetch_status["incomplete_files"] = fetch_incomplete
        fetch_status["message"] = (
            f"查询完成，共 {fetch_total_count[0]} 条，"
            f"{len(fetch_incomplete)} 个切片失败"
        )
    _update_stage(task_id, "fetch", fetch_status)

    # 空数据校验
    if fetch_total_count[0] == 0:
        _update_stage(task_id, "tokenize", {"status": "skipped", "message": "无数据，跳过"})
        _update_stage(task_id, "simulate", {"status": "skipped", "message": "无数据，跳过"})
        status = _read_status(task_id)
        if status:
            status["pipeline"]["current_stage"] = "done"
            status["result"] = {
                "hit_rate": 0, "hit_rate_percent": 0,
                "hit_count": 0, "total_queries": 0,
                "total_tokens": 0, "total_entries": 0,
                "message": "数据提取阶段无匹配数据"
            }
            if fetch_incomplete:
                status["result"]["incomplete_count"] = len(fetch_incomplete)
                status["result"]["incomplete_files"] = fetch_incomplete
            _write_status(status)
        tokenize_daemon.stop()
        return  # 不抛异常，正常结束

    # 等待所有 tokenize
    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"等待序列化完成 ({tokenize_done_count[0]}/{tokenize_total_submitted[0]})"
    }, "tokenize")

    if tokenize_tasks:
        try:
            await asyncio.gather(*tokenize_tasks)
        finally:
            tokenize_tasks.clear()
            try:
                tokenize_daemon.stop()
                logger.info("[daemon] 已关闭")
            except Exception:
                pass

    # tokenize 最终状态
    success_files = [r for r in tokenize_results if r["status"] == "completed"]
    failed_files = [r for r in tokenize_results if r["status"] == "failed"]

    model_txt_files = {}
    for r in success_files:
        for model, txt_file in r.get("outputs", {}).items():
            model_txt_files.setdefault(model, []).append(txt_file)

    # 模型过滤
    status = _read_status(task_id)
    selected_models = status.get("query", {}).get("models", []) if status else []
    all_detected_models = list(model_txt_files.keys())
    if selected_models:
        skipped_models = [m for m in model_txt_files if m not in selected_models]
        model_txt_files = {m: fs for m, fs in model_txt_files.items() if m in selected_models}
        if skipped_models:
            logger.info(f"[tokenize] 按模型过滤: 保留 {list(model_txt_files.keys())}，跳过 {skipped_models}")

    if success_files and selected_models and not model_txt_files:
        msg = (
            f"序列化完成，但所选模型 {selected_models} 在数据中未检测到。"
            f"实际检测到的模型: {all_detected_models}"
        )
        _update_stage(task_id, "tokenize", {
            "status": "completed", "message": msg,
            "model_outputs": {}, "total_lines": 0,
            "success_count": len(success_files), "failed_count": len(failed_files),
            "models": [], "all_detected_models": all_detected_models,
            "files": tokenize_results
        })
        _update_stage(task_id, "simulate", {"status": "skipped", "message": "所选模型未匹配，跳过模拟"})
        zero_result = {}
        for m in selected_models:
            zero_result[m] = {
                "hit_rate": 0, "hit_rate_percent": 0,
                "hit_count": 0, "total_queries": 0,
                "total_tokens": 0, "total_entries": 0,
                "input_files_count": 0
            }
        zero_result["all_detected_models"] = all_detected_models
        zero_result["message"] = msg
        _set_result(task_id, zero_result)
        return

    if not success_files:
        _update_stage(task_id, "tokenize", {
            "status": "failed",
            "message": f"序列化全部失败，{len(failed_files)} 个文件",
            "files": tokenize_results, "model_outputs": {}, "total_lines": 0
        })
        _set_failed(task_id, "tokenize", f"全部 {len(failed_files)} 个文件序列化失败")
        return

    status_msg = (
        f"序列化完成，{len(success_files)}/{len(tokenize_results)} 成功，"
        f"{tokenize_total_lines[0]} 条，{len(model_txt_files)} 个模型"
    )
    if selected_models:
        status_msg += f"（已过滤，检测到 {len(all_detected_models)} 个模型）"
    _update_stage(task_id, "tokenize", {
        "status": "completed", "message": status_msg,
        "model_outputs": {m: fs for m, fs in model_txt_files.items()},
        "total_lines": tokenize_total_lines[0],
        "success_count": len(success_files), "failed_count": len(failed_files),
        "models": list(model_txt_files.keys()),
        "all_detected_models": all_detected_models,
        "files": tokenize_results,
    })

    # 释放中间数据
    fetch_results.clear()
    fetch_incomplete.clear()
    tokenize_results.clear()
    del success_files, failed_files
    gc.collect()

    if tokenize_total_lines[0] == 0:
        _update_stage(task_id, "simulate", {"status": "skipped", "message": "序列化结果为空，跳过模拟"})
        status = _read_status(task_id)
        if status:
            status["pipeline"]["current_stage"] = "done"
            status["result"] = {
                "hit_rate": 0, "hit_rate_percent": 0,
                "hit_count": 0, "total_queries": 0,
                "total_tokens": 0, "total_entries": 0,
                "message": "序列化阶段无有效数据"
            }
            _write_status(status)
        return


async def _run_tokenize_via_daemon(
    daemon,
    input_file: str,
    output_dir: str,
    task_id: str,
) -> dict:
    """
    通过共享 daemon 对单个文件执行 tokenize。
    tokenizer 只在 daemon 启动时加载一次，所有切片复用同一批 worker。
    由于 daemon.wait() 是阻塞调用，用 run_in_executor 避免阻塞事件循环。
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    short_name = os.path.basename(input_file)
    slice_output_dir = os.path.join(output_dir, base_name)
    os.makedirs(slice_output_dir, exist_ok=True)

    cfg = _load_olap_config()

    # 读取用户指定的模型过滤列表，透传给 daemon，只写入目标 model 的文件
    # 用户未指定时，回退到 olap_config.json 的 models 作为默认白名单，
    # 列表外的模型 tokenizer 未知，跳过以避免乱算浪费时间和磁盘
    status = _read_status(task_id)
    selected_models = status.get("query", {}).get("models", []) if status else []
    if not selected_models:
        selected_models = cfg.get("models", [])

    # 日志落盘目录：task_data_dir/tokenize_logs
    task_data_dir = _task_dir(task_id)
    log_dir = os.path.join(task_data_dir, "tokenize_logs")

    def _submit_and_wait():
        dt_id = daemon.submit(
            input_file=input_file,
            output_dir=slice_output_dir,
            file_prefix=base_name,
            batch_size=cfg["pipeline_tokenize_batch_size"],
            model_filter=selected_models if selected_models else None,
            log_dir=log_dir,
        )
        return daemon.wait(dt_id, timeout=86400.0)

    try:
        loop = asyncio.get_event_loop()
        summary = await loop.run_in_executor(None, _submit_and_wait)
    except Exception as e:
        logger.warning(f"[daemon] {short_name} 失败: {e}")
        return {"file": short_name, "status": "failed", "error": str(e), "outputs": {}}

    if summary.get("status") != "completed":
        err = summary.get("error", "daemon tokenize failed")
        logger.warning(f"[daemon] {short_name} 返回 failed: {err[:200]}")
        return {"file": short_name, "status": "failed", "error": err[:500], "outputs": {}}

    # 将 daemon 返回格式转换为 _run_tokenize_single_file 的格式
    model_outputs = {}
    total_lines = 0
    for model, info in summary.get("models", {}).items():
        txt_file = info.get("file", "")
        count = info.get("count", 0)
        if txt_file and os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
            model_outputs[model] = txt_file
            total_lines += count

    if not model_outputs:
        return {"file": short_name, "status": "failed", "error": "daemon 无有效输出", "outputs": {}}

    return {
        "file": short_name,
        "status": "completed",
        "outputs": model_outputs,
        "lines": total_lines,
        "duration_seconds": summary.get("duration_seconds", 0.0),
        "error": None,
    }


async def _run_tokenize_single_file(
    input_file: str, output_dir: str, task_id: str, file_index: int, total_files: int,
) -> dict:
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    short_name = os.path.basename(input_file)
    slice_output_dir = os.path.join(output_dir, base_name)
    os.makedirs(slice_output_dir, exist_ok=True)

    cfg = _load_olap_config()
    cmd = [
        "python", "-u", os.path.join(SCRIPTS_DIR, "kv_pipeline.py"),
        "-i", input_file,
        "-o", slice_output_dir,
        "-d", cfg["pipeline_default_model"],
        "-w", str(cfg["pipeline_tokenize_workers"]),
        "--tokenize-batch-size", str(cfg["pipeline_tokenize_batch_size"]),
        # daemon 模式由 kv_pipeline.py 默认开启（tokenizer 常驻复用），
        # 如需回退旧模式可在 olap_config.json 中添加 "pipeline_tokenize_no_daemon": true
        *( ["--no-daemon"] if cfg.get("pipeline_tokenize_no_daemon") else [] ),
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=BASE_DIR
    )

    output_tail = deque(maxlen=30)
    last_progress_update = 0
    async for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace").strip()
        output_tail.append(line)

        now = asyncio.get_event_loop().time()
        if now - last_progress_update >= 2:
            progress_msg = None
            m = re.search(r'\[INFO\]\s*进度:\s*(\d+)/(\d+)\S*.*成功:\s*(\d+).*失败:\s*(\d+).*速度:\s*([\d.]+)', line)
            if m:
                done, total = int(m.group(1)), int(m.group(2))
                ok, fail = int(m.group(3)), int(m.group(4))
                speed = m.group(5)
                progress_msg = f"{short_name}: {done}/{total} 条 (成功 {ok}, 失败 {fail}, {speed} 条/秒)"
            elif "序列化" in line and base_name in line:
                progress_msg = f"{short_name}: 序列化中..."
            elif "[INFO] 流式处理中" in line:
                progress_msg = f"{short_name}: 流式序列化中..."

            if progress_msg:
                last_progress_update = now
                _update_stage(task_id, "tokenize", {
                    "status": "running",
                    "message": progress_msg,
                })

    await proc.wait()
    output_text = "\n".join(output_tail)

    if proc.returncode != 0:
        return {
            "file": short_name, "status": "failed",
            "error": output_text[-500:], "outputs": {}
        }

    # 读取 pipeline_summary.json
    summary_file = os.path.join(slice_output_dir, "pipeline_summary.json")
    model_outputs = {}
    total_lines = 0
    duration_seconds = 0.0

    if os.path.exists(summary_file):
        with open(summary_file, "r", encoding="utf-8") as f:
            summary = json.load(f)
        duration_seconds = summary.get("duration_seconds", 0.0)
        for file_result in summary.get("files", []):
            for model, mf in file_result.get("model_files", {}).items():
                txt_file = mf.get("txt", "")
                lines = mf.get("lines", 0)
                if txt_file and os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                    model_outputs[model] = txt_file
                    total_lines += lines
    else:
        pattern = os.path.join(slice_output_dir, f"{base_name}_*_input_ids.txt")
        for txt_file in sorted(glob.glob(pattern)):
            fname = os.path.basename(txt_file)
            model = fname[len(base_name) + 1:].replace("_input_ids.txt", "")
            if model and os.path.getsize(txt_file) > 0:
                with open(txt_file, "r") as f:
                    lines = sum(1 for _ in f)
                model_outputs[model] = txt_file
                total_lines += lines

    if not model_outputs:
        return {
            "file": short_name, "status": "failed",
            "error": "tokenize 无有效输出文件", "outputs": {}
        }

    return {
        "file": short_name, "status": "completed",
        "outputs": model_outputs, "lines": total_lines,
        "duration_seconds": duration_seconds, "error": None
    }


# ============================================================
# Stage 3: simulate
# ============================================================

def _parse_section_time(section_name: str) -> str:
    """从 cache_calc section 名提取时间标签。
    section 名格式: kv_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS_model
    输出: MM-DD HH:mm
    """
    m = re.match(r'kv_(\d{8})_(\d{6})_', section_name)
    if m:
        d, t = m.group(1), m.group(2)
        return f"{d[4:6]}-{d[6:8]} {t[0:2]}:{t[2:4]}"
    return section_name[:16]


async def _simulate_single_model_fallback(
    model, sorted_files, cache_calc_path, cache_size, block_size,
    checkpoint_path, report_file_final,
    sim_done_count, model_outputs, task_id, report_dir, cfg
) -> dict:
    """Fallback: 逐文件调用 cache_calc -c（兼容旧版 cache_calc 或 -L 失败时）"""
    total_entries = 0
    total_tokens = 0
    total_adds = 0
    hit_count = 0
    file_idx = 0
    section_points = []

    for fpath in sorted_files:
        file_idx += 1
        cmd = [
            cache_calc_path, "-f", fpath,
            "-s", str(cache_size),
            "-b", str(block_size),
            "-p", "true",
            "-c", checkpoint_path,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=BASE_DIR,
            )
            output = await proc.stdout.read()
            await proc.wait()

            if proc.returncode != 0:
                continue

            output_text = output.decode("utf-8", errors="replace")
            time_label = _parse_section_time(os.path.basename(fpath))
            for line in output_text.strip().split("\n"):
                line = line.strip()
                if line.startswith("entries:"):
                    m = re.match(r'entries:\s*(\d+),\s*tokens:\s*(\d+)', line)
                    if m:
                        total_entries += int(m.group(1))
                        total_tokens += int(m.group(2))
                elif line.startswith("cache_size:"):
                    parts = re.findall(r'(\w+):\s*([\d.]+)', line)
                    section_hr = None
                    for key, val in parts:
                        if key == "total_adds":
                            adds = int(val)
                            total_adds += adds
                        elif key == "hit_count":
                            hits = int(val)
                            hit_count += hits
                            if adds > 0:
                                section_hr = hits / adds
                    if section_hr is not None:
                        section_points.append({"time": time_label, "hit_rate": section_hr})

        except Exception as e:
            logger.warning("[simulate-fallback] cache_calc exception for %s: %s",
                           os.path.basename(fpath), e)

    # 保存 section_points
    if section_points:
        model_report_dir = os.path.join(report_dir, model)
        section_file = os.path.join(model_report_dir, "_section_points.json")
        with open(section_file, "w", encoding="utf-8") as f:
            json.dump(section_points, f, ensure_ascii=False)

    hit_rate = hit_count / total_adds if total_adds > 0 else 0

    # 写 report
    report = {
        "meta": {
            "generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S"),
            "algorithm": "LRU checkpoint incremental (fallback per-file)",
        },
        "config": {
            "cache_sizes": [cache_size],
            "block_size": block_size,
            "use_prefix_hash": True,
            "algorithm": "LRU",
        },
        "summary": {
            "total_entries": total_entries,
            "total_tokens": total_tokens,
            "avg_tokens_per_entry": round(total_tokens / total_entries, 2) if total_entries > 0 else 0,
        },
        "results": [{
            "cache_size": cache_size,
            "cache_size_readable": f"{cache_size / 1e6:.1f}M" if cache_size >= 1_000_000 else str(cache_size),
            "total_queries": total_adds,
            "hit_count": hit_count,
            "miss_count": total_adds - hit_count,
            "hit_rate": hit_rate,
            "hit_rate_percent": round(hit_rate * 100, 2),
            "miss_rate": 1 - hit_rate,
            "miss_rate_percent": round((1 - hit_rate) * 100, 2),
        }],
        "analysis": {
            "recommendation": (
                f"命中率较高 ({hit_rate * 100:.2f}%)，缓存效果显著，推荐配置: cache_size={cache_size}"
                if hit_rate >= 0.5 else
                f"命中率一般 ({hit_rate * 100:.2f}%)，建议缓存大小设置为 {cache_size}"
            ),
        },
    }
    with open(report_file_final, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    sim_done_count[0] += 1
    return {
        "model": model, "status": "completed",
        "hit_rate": hit_rate,
        "hit_rate_percent": round(hit_rate * 100, 2),
        "hit_count": hit_count,
        "total_queries": total_adds,
        "total_tokens": total_tokens,
        "total_entries": total_entries,
        "input_files_count": len(sorted_files),
        "report_file": report_file_final,
    }


async def _run_simulate_stage(task_id: str):
    _update_stage(task_id, "simulate", {"status": "running", "message": "正在模拟缓存命中..."}, "simulate")

    task_data_dir = _task_dir(task_id)
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)

    status = _read_status(task_id) or {}
    tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
    model_outputs = tokenize_stage.get("model_outputs", {})

    # ---- file 模式 ----
    if not model_outputs:
        tokenized_dir = os.path.join(task_data_dir, "tokenized")
        for txt_file in sorted(glob.glob(os.path.join(tokenized_dir, "**", "*_input_ids.txt"), recursive=True)):
            fname = os.path.basename(txt_file)
            m = re.match(r'kv_\d{8}_\d{6}_\d{8}_\d{6}_(.+)_input_ids\.txt$', fname)
            if m:
                model = m.group(1)
            else:
                model = fname.replace("_input_ids.txt", "").split("_")[-1]
            if os.path.getsize(txt_file) > 0:
                model_outputs.setdefault(model, []).append(txt_file)

    for model in list(model_outputs.keys()):
        model_outputs[model] = [
            f for f in model_outputs[model]
            if os.path.exists(f) and os.path.getsize(f) > 0
        ]
        if not model_outputs[model]:
            del model_outputs[model]

    if not model_outputs:
        _set_failed(task_id, "simulate", "无有效 input_ids 文件（全部为空）")
        return

    _update_stage(task_id, "simulate", {
        "status": "running",
        "message": f"正在模拟 {len(model_outputs)} 个模型..."
    })

    sim_done_count = [0]
    cfg = _load_olap_config()
    cache_calc_path = CACHE_CALC_PATH

    async def _simulate_single_model(model: str, txt_files: list) -> dict:
        model_report_dir = os.path.join(report_dir, model)
        os.makedirs(model_report_dir, exist_ok=True)
        checkpoint_dir = os.path.join(model_report_dir, "_checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)
        checkpoint_path = os.path.join(checkpoint_dir, "cache_checkpoint.bin")

        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"[{model}] 正在模拟 ({sim_done_count[0]}/{len(model_outputs)} 已完成)..."
        })

        report_file_final = os.path.join(model_report_dir, "cache_report.json")
        cache_size = cfg["pipeline_cache_size"]
        block_size = cfg["pipeline_block_size"]

        sorted_files = sorted(txt_files, key=lambda f: os.path.basename(f))

        # ---- 单进程流式模式 ----
        # 生成文件列表，一次性调用 cache_calc -L <list> -c <checkpoint>
        # LRU cache 只需 load 1 次 + save 1 次，避免 1440 次子进程 fork + CFS 往返
        filelist_path = os.path.join(checkpoint_dir, "_filelist.txt")
        with open(filelist_path, "w", encoding="utf-8") as f:
            for fpath in sorted_files:
                f.write(fpath + "\n")

        cmd = [
            cache_calc_path,
            "-L", filelist_path,
            "-s", str(cache_size),
            "-b", str(block_size),
            "-p", "true",
            "-c", checkpoint_path,
        ]

        total_entries = 0
        total_tokens = 0
        total_adds = 0
        hit_count = 0
        section_points = []  # per-minute hit_rate，供 trend 使用

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=BASE_DIR,
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                err_msg = (stderr or b"").decode("utf-8", errors="replace")[:500]
                logger.warning("[simulate] cache_calc failed for model %s (rc=%d): %s",
                               model, proc.returncode, err_msg)
                # fallback: 逐文件模式
                return await _simulate_single_model_fallback(
                    model, sorted_files, cache_calc_path, cache_size,
                    block_size, checkpoint_path, report_file_final,
                    sim_done_count, model_outputs, task_id, report_dir, cfg)

            output_text = stdout.decode("utf-8", errors="replace")
            for line in output_text.strip().split("\n"):
                line = line.strip()
                if line.startswith("section:"):
                    # section: kv_20260411_020300_20260411_020400_glm-5  section_hit_rate: 0.623813
                    m = re.match(
                        r'section:\s*([^\t]+).*section_hit_rate:\s*([\d.]+)',
                        line,
                    )
                    if m:
                        section_name = m.group(1).strip()
                        section_hr = float(m.group(2))
                        # 从 section 名提取时间标签: kv_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS_model -> MM-DD HH:mm
                        time_label = _parse_section_time(section_name)
                        section_points.append({"time": time_label, "hit_rate": section_hr})
                elif line.startswith("entries:"):
                    m = re.match(r'entries:\s*(\d+),\s*tokens:\s*(\d+)', line)
                    if m:
                        total_entries = int(m.group(1))
                        total_tokens = int(m.group(2))
                elif line.startswith("cache_size:"):
                    parts = re.findall(r'(\w+):\s*([\d.]+)', line)
                    for key, val in parts:
                        if key == "total_adds":
                            total_adds = int(val)
                        elif key == "hit_count":
                            hit_count = int(val)

            # 保存 section_points 供 trend 阶段直接使用
            if section_points:
                section_file = os.path.join(model_report_dir, "_section_points.json")
                with open(section_file, "w", encoding="utf-8") as f:
                    json.dump(section_points, f, ensure_ascii=False)
                logger.info("[simulate] %s: %d section points saved", model, len(section_points))

        except Exception as e:
            logger.warning("[simulate] cache_calc exception for model %s: %s", model, e)
            return await _simulate_single_model_fallback(
                model, sorted_files, cache_calc_path, cache_size,
                block_size, checkpoint_path, report_file_final,
                sim_done_count, model_outputs, task_id, report_dir, cfg)

        # 清理文件列表
        try:
            os.remove(filelist_path)
        except OSError:
            pass

        # 计算整体 hit_rate
        hit_rate = hit_count / total_adds if total_adds > 0 else 0

        # 生成与 cache_pipeline.py 兼容的 cache_report.json
        report = {
            "meta": {
                "generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S"),
                "algorithm": "LRU checkpoint incremental (single-process streaming)",
            },
            "config": {
                "cache_sizes": [cache_size],
                "block_size": block_size,
                "use_prefix_hash": True,
                "algorithm": "LRU",
            },
            "summary": {
                "total_entries": total_entries,
                "total_tokens": total_tokens,
                "avg_tokens_per_entry": round(total_tokens / total_entries, 2) if total_entries > 0 else 0,
            },
            "results": [{
                "cache_size": cache_size,
                "cache_size_readable": f"{cache_size / 1e6:.1f}M" if cache_size >= 1_000_000 else str(cache_size),
                "total_queries": total_adds,
                "hit_count": hit_count,
                "miss_count": total_adds - hit_count,
                "hit_rate": hit_rate,
                "hit_rate_percent": round(hit_rate * 100, 2),
                "miss_rate": 1 - hit_rate,
                "miss_rate_percent": round((1 - hit_rate) * 100, 2),
            }],
            "analysis": {
                "recommendation": (
                    f"命中率较高 ({hit_rate * 100:.2f}%)，缓存效果显著，推荐配置: cache_size={cache_size}"
                    if hit_rate >= 0.5 else
                    f"命中率一般 ({hit_rate * 100:.2f}%)，建议缓存大小设置为 {cache_size}"
                ),
            },
        }

        with open(report_file_final, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        # 保留 checkpoint 供 trend 阶段复用（trend 会从该状态继续计算 per-minute hit_rate）

        sim_done_count[0] += 1
        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"模拟进度 {sim_done_count[0]}/{len(model_outputs)} 模型完成"
        })

        return {
            "model": model, "status": "completed",
            "hit_rate": hit_rate,
            "hit_rate_percent": round(hit_rate * 100, 2),
            "hit_count": hit_count,
            "total_queries": total_adds,
            "total_tokens": total_tokens,
            "total_entries": total_entries,
            "input_files_count": len(txt_files),
            "report_file": report_file_final
        }

    sim_tasks = [
        _simulate_single_model(model, txt_files)
        for model, txt_files in model_outputs.items()
    ]
    sim_results = await asyncio.gather(*sim_tasks)
    sim_tasks.clear()
    model_outputs.clear()

    result = {}
    all_ok = True
    for sr in sim_results:
        model = sr["model"]
        if sr["status"] == "completed":
            result[model] = {
                "hit_rate": sr["hit_rate"],
                "hit_rate_percent": sr["hit_rate_percent"],
                "hit_count": sr["hit_count"],
                "total_queries": sr["total_queries"],
                "total_tokens": sr["total_tokens"],
                "total_entries": sr["total_entries"],
                "input_files_count": sr["input_files_count"],
            }
        else:
            all_ok = False
            result[model] = {"status": "failed", "error": sr.get("error", "")}

    if not any(sr["status"] == "completed" for sr in sim_results):
        errors = "; ".join(f"{sr['model']}: {sr.get('error', '')[:100]}" for sr in sim_results)
        _set_failed(task_id, "simulate", f"全部模型模拟失败: {errors}")
        return

    completed_models = [sr for sr in sim_results if sr["status"] == "completed"]
    msg_parts = []
    for sr in completed_models:
        msg_parts.append(f"{sr['model']} {sr['hit_rate'] * 100:.2f}%")
    sim_msg = f"模拟完成 ({len(completed_models)}/{len(sim_results)} 模型): " + ", ".join(msg_parts)

    _update_stage(task_id, "simulate", {
        "status": "completed" if all_ok else "partial",
        "message": sim_msg,
        "models": [sr["model"] for sr in sim_results],
    })

    # 将 result 暂存到 status 但不设 done（由 pipeline 在 trend 之后统一标记）
    status = _read_status(task_id)
    if status:
        status["result"] = result
        _write_status(status)


# ============================================================
# Resume: 当 fetch 已完成时，仅重跑 tokenize（跳过重新拉取 ES 数据）
# ============================================================
async def _run_tokenize_only(task_id: str):
    """
    fetch 已完成时的 tokenize 恢复入口。
    从磁盘扫描 {task_data_dir}/{HH}/*.jsonl 获取待 tokenize 文件列表。
    用于任务中断后无需重新拉取数据的快速恢复。
    """
    task_data_dir = _task_dir(task_id)

    # 从磁盘扫描 per-minute jsonl 文件（新格式: {HH}/kv_*.jsonl）
    # 同时兼容旧格式: task_data_dir/kv_*.jsonl
    result_files = []
    for pattern in [os.path.join(task_data_dir, "*", "kv_*.jsonl"),
                    os.path.join(task_data_dir, "kv_*.jsonl")]:
        for f in sorted(glob.glob(pattern)):
            if f.endswith(".incomplete"):
                continue
            result_files.append({"file": f})

    if not result_files:
        logger.error("[resume] fetch 已完成但未找到任何 .jsonl 文件，无法恢复 tokenize")
        _set_failed(task_id, "tokenize", "resume 失败：未找到数据文件")
        return

    cfg = _load_olap_config()
    output_dir_base = os.path.join(_task_dir(task_id), "tokenized")
    os.makedirs(output_dir_base, exist_ok=True)

    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    from kv_pipeline import TokenizeDaemonClient  # noqa: E402

    num_daemons = cfg["pipeline_tokenize_concurrency"]
    tokenize_daemon = TokenizeDaemonClient(
        workers=cfg["pipeline_tokenize_workers"],
        batch_size=cfg["pipeline_tokenize_batch_size"],
        default_model=cfg["pipeline_default_model"],
        num_daemons=num_daemons,
    )
    logger.info(f"[resume] 启动 {num_daemons} 个 daemon (workers_each={cfg['pipeline_tokenize_workers']})...")
    tokenize_daemon.start(timeout=300.0)
    logger.info(f"[resume] {num_daemons} 个 daemon 就绪，共 {len(result_files)} 个文件待 tokenize")

    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"恢复 tokenize，共 {len(result_files)} 个文件...",
    }, "tokenize")
    _update_stage(task_id, "simulate", {"status": "pending", "message": "等待 tokenize..."})

    # 新版：result_files 已是 per-minute 文件列表，无需拆分
    # 旧版兼容：如果是整小时文件（没有 minute 字段），直接 tokenize
    tokenize_file_list = []
    for fi in result_files:
        input_file = fi.get("file") if isinstance(fi, dict) else fi
        if input_file and os.path.exists(input_file):
            tokenize_file_list.append({"file": input_file})

    logger.info(f"[resume] 共 {len(tokenize_file_list)} 个文件待 tokenize")

    total_tokenize_files = len(tokenize_file_list)

    tokenize_results = []
    total_lines = 0
    total_seconds = 0.0
    done_count = [0]
    tokenize_sem = asyncio.Semaphore(cfg["pipeline_tokenize_concurrency"])

    async def _tokenize_one(file_info):
        input_file = file_info["file"]
        if not os.path.exists(input_file):
            logger.warning(f"[resume] 文件不存在，跳过: {input_file}")
            return {
                "file": os.path.basename(input_file),
                "status": "failed", "error": "文件不存在", "outputs": {},
            }
        logger.info(f"[resume] tokenize: {os.path.basename(input_file)}")
        async with tokenize_sem:
            result = await _run_tokenize_via_daemon(tokenize_daemon, input_file, output_dir_base, task_id)
        done_count[0] += 1
        _update_stage(task_id, "tokenize", {
            "status": "running",
            "message": f"tokenize 进度 {done_count[0]}/{total_tokenize_files}",
        })
        return result

    try:
        tasks = [asyncio.create_task(_tokenize_one(fi)) for fi in tokenize_file_list]
        tokenize_results = await asyncio.gather(*tasks)
        tokenize_results = list(tokenize_results)
        for r in tokenize_results:
            if r["status"] == "completed":
                total_lines += r.get("lines", 0)
                total_seconds += r.get("duration_seconds", 0.0)
    finally:
        try:
            tokenize_daemon.stop()
            logger.info("[resume] daemon 已关闭")
        except Exception:
            pass

    success_files = [r for r in tokenize_results if r["status"] == "completed"]
    failed_files  = [r for r in tokenize_results if r["status"] == "failed"]

    model_txt_files: dict = {}
    for r in success_files:
        for model, txt_file in r.get("outputs", {}).items():
            model_txt_files.setdefault(model, []).append(txt_file)

    status = _read_status(task_id)
    selected_models = status.get("query", {}).get("models", []) if status else []
    all_detected_models = list(model_txt_files.keys())
    if selected_models:
        model_txt_files = {m: fs for m, fs in model_txt_files.items() if m in selected_models}

    if not success_files:
        _set_failed(task_id, "tokenize", f"全部 {len(failed_files)} 个文件序列化失败")
        return

    _update_stage(task_id, "tokenize", {
        "status": "completed",
        "message": f"恢复 tokenize 完成，{len(success_files)}/{len(tokenize_results)} 成功，{total_lines} 条",
        "model_outputs": {m: fs for m, fs in model_txt_files.items()},
        "total_lines": total_lines,
        "success_count": len(success_files),
        "failed_count": len(failed_files),
        "models": list(model_txt_files.keys()),
        "all_detected_models": all_detected_models,
        "files": tokenize_results,
    })


# ============================================================
# Stage 4: 分钟级命中率趋势
# ============================================================
async def _run_trend_stage(task_id: str):
    """计算分钟级命中率趋势并保存到 report/hit_rate_trend.json

    优先从 simulate 阶段保存的 _section_points.json 读取 per-minute hit_rate，
    避免重复运行 cache_calc（从 ~30min 降到 <1s）。
    如果没有 section_points（旧版任务或 fallback），回退到 compute_trend 重算。
    """
    _update_stage(task_id, "trend", {"status": "running", "message": "正在计算分钟级命中率趋势..."}, "trend")
    logger.info("[trend] 开始趋势计算: %s", task_id)

    task_data_dir = _task_dir(task_id)

    # ---- 优先: 从 simulate 的 _section_points.json 直接读取 ----
    model_series = []
    report_dir = os.path.join(task_data_dir, "report")

    if os.path.isdir(report_dir):
        for model_dir in sorted(os.listdir(report_dir)):
            model_path = os.path.join(report_dir, model_dir)
            if not os.path.isdir(model_path):
                continue
            section_file = os.path.join(model_path, "_section_points.json")
            if not os.path.exists(section_file):
                continue
            try:
                with open(section_file, "r", encoding="utf-8") as f:
                    points = json.load(f)
                if points:
                    model_series.append({
                        "model": model_dir,
                        "data": points,
                        "stats": _calc_trend_stats(points),
                    })
                    logger.info("[trend] 从 section_points 读取 %s: %d points",
                                model_dir, len(points))
            except Exception as e:
                logger.warning("[trend] 读取 section_points 失败 %s: %s", model_dir, e)

    if model_series:
        logger.info("[trend] 使用 simulate 阶段的 section_points，%d 模型", len(model_series))
        # 计算整体维度
        series = _build_trend_series(model_series)
        output_file = os.path.join(report_dir, "hit_rate_trend.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(series, f, ensure_ascii=False, indent=2)
        n_series = len(series.get("series", []))
        _update_stage(task_id, "trend", {
            "status": "completed",
            "message": f"趋势计算完成 ({len(model_series)} 模型，from section_points)",
            "output_file": output_file,
        })
        logger.info("[trend] 已保存: %s (%d series)", output_file, n_series)
        return

    # ---- Fallback: 调用 compute_trend 重算 ----
    logger.info("[trend] 未找到 section_points，回退到 compute_trend 重算")
    try:
        from compute_trend import compute_and_save, _collect_model_outputs

        status = _read_status(task_id)
        tokenize_stage = (status or {}).get("pipeline", {}).get("stages", {}).get("tokenize", {})
        status_model_outputs = tokenize_stage.get("model_outputs")

        model_outputs = _collect_model_outputs(task_data_dir, status_model_outputs)

        if not model_outputs:
            _update_stage(task_id, "trend", {
                "status": "completed",
                "message": "无 input_ids 文件，跳过趋势计算",
            })
            return

        cfg = _load_olap_config()
        cache_size = cfg.get("pipeline_cache_size", 200000000)
        block_size = cfg.get("pipeline_block_size", 16)

        output_file = await asyncio.to_thread(
            compute_and_save,
            task_data_dir=task_data_dir,
            cache_size=cache_size,
            block_size=block_size,
            model_outputs=model_outputs,
        )

        _update_stage(task_id, "trend", {
            "status": "completed",
            "message": f"趋势计算完成 ({len(model_outputs)} 模型，fallback 重算)",
            "output_file": output_file,
        })
        logger.info("[trend] 趋势计算完成: %s -> %s", task_id, output_file)
    except Exception as e:
        logger.warning(f"[trend] 趋势计算失败: {task_id}: {e}", exc_info=True)
        _update_stage(task_id, "trend", {
            "status": "failed",
            "message": f"趋势计算失败: {str(e)[:200]}",
        })


def _calc_trend_stats(data_points: list) -> dict:
    """计算 mean / max / min"""
    rates = [d["hit_rate"] for d in data_points if d.get("hit_rate") is not None]
    if not rates:
        return {"mean": 0, "max": 0, "min": 0}
    return {
        "mean": round(sum(rates) / len(rates), 4),
        "max": round(max(rates), 4),
        "min": round(min(rates), 4),
    }


def _build_trend_series(model_series: list) -> dict:
    """从 per-model series 构建 trend 输出（含整体维度）"""
    series = list(model_series)

    if model_series:
        time_rates: dict = {}
        for ms in model_series:
            for d in ms["data"]:
                if d.get("hit_rate") is not None:
                    time_rates.setdefault(d["time"], []).append(d["hit_rate"])

        overall_data = []
        for t in sorted(time_rates.keys()):
            rates = time_rates[t]
            overall_data.append({"time": t, "hit_rate": round(sum(rates) / len(rates), 4)})

        if overall_data:
            series.insert(0, {
                "model": "整体",
                "data": overall_data,
                "stats": _calc_trend_stats(overall_data),
            })

    return {"series": series}


# ============================================================
# 主入口
# ============================================================
async def run_pipeline(args):
    task_id = args.task_id
    logger.info(f"Pipeline started: {task_id}")

    try:
        # CLI --slice-minutes 覆盖写入 status config
        if getattr(args, "slice_minutes", None) is not None:
            status = _read_status(task_id)
            if status:
                status.setdefault("config", {})["slice_minutes"] = args.slice_minutes
                _write_status(status)
                logger.info(f"[pipeline] CLI 覆盖 slice_minutes={args.slice_minutes}")

        # 检查是否可以从更晚的阶段恢复
        existing_status = _read_status(task_id)
        stages = (existing_status or {}).get("pipeline", {}).get("stages", {})
        tokenize_already_done = (
            existing_status is not None
            and stages.get("tokenize", {}).get("status") == "completed"
        )
        fetch_already_done = (
            existing_status is not None
            and stages.get("fetch", {}).get("status") == "completed"
        )

        if tokenize_already_done:
            logger.info(f"[pipeline] tokenize 已完成，直接进入 simulate: {task_id}")
        elif fetch_already_done:
            logger.info(f"[pipeline] fetch 已完成，从 tokenize 阶段恢复: {task_id}")
            await _run_tokenize_only(task_id)
        else:
            # Stage 1+2
            await _run_streaming_fetch_tokenize(
                task_id, args.start_datetime, args.end_datetime,
                args.app_id, args.path
            )

        # 检查是否已完成（无数据时 fetch 阶段直接设置 done）
        status = _read_status(task_id)
        if status and status["pipeline"].get("current_stage") == "done":
            logger.info(f"Pipeline finished early (no data): {task_id}")
            return

        # Stage 3: simulate
        status = _read_status(task_id)
        sim_stage = (status or {}).get("pipeline", {}).get("stages", {}).get("simulate", {})
        if sim_stage.get("status") == "completed":
            logger.info(f"[pipeline] simulate 已完成，跳过: {task_id}")
        else:
            await _run_simulate_stage(task_id)

        # Stage 4: 分钟级命中率趋势
        await _run_trend_stage(task_id)

        # 标记 done
        status = _read_status(task_id)
        if status and status["pipeline"].get("current_stage") not in ("done", "failed"):
            status["pipeline"]["current_stage"] = "done"
            _write_status(status)
        logger.info(f"Pipeline completed: {task_id}")

    except Exception as e:
        logger.exception(f"Pipeline failed: {task_id}")
        status = _read_status(task_id)
        if status and status["pipeline"]["current_stage"] not in ("done", "failed"):
            _set_failed(task_id, status["pipeline"]["current_stage"], str(e))
        sys.exit(1)

    finally:
        gc.collect()


def main():
    parser = argparse.ArgumentParser(description="OLAP KV Pipeline (K8s Job entrypoint)")
    parser.add_argument("--task-id", required=True, help="任务 ID")
    parser.add_argument("--username", required=True, help="提交用户")
    parser.add_argument("--start-datetime", required=True, help="查询开始时间")
    parser.add_argument("--end-datetime", required=True, help="查询结束时间")
    parser.add_argument("--app-id", default="app-3Lut8O2E", help="ES app ID")
    parser.add_argument("--path", default="/v2/coding/chat/completions", help="场景过滤路径")
    parser.add_argument("--models", default="", help="模型过滤，逗号分隔")
    parser.add_argument("--slice-minutes", type=int, default=None,
                        help="子切片粒度（分钟），覆盖 status.json 和全局配置。60=不拆分，1=每分钟一个子切片")
    args = parser.parse_args()

    # es_query 日志文件以 task_id 命名，便于定位
    _setup_es_log_file(args.task_id)

    # 确保 HF_TOKEN 可用
    hf_token = os.environ.get("HF_TOKEN", "")
    if hf_token:
        os.environ["HF_TOKEN"] = hf_token

    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
