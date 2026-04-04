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

BJT = timezone(timedelta(hours=8))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


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
    "pipeline_fetch_concurrency": 12,
    "pipeline_es_scroll_workers": 60,
    "pipeline_tokenize_workers": 4,
    "pipeline_tokenize_batch_size": 200,
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
# Stage 1+2: fetch → tokenize (streaming pipeline)
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
    _cb_msg = [None]
    _progress_dirty = [False]

    cfg = _load_olap_config()
    fetch_sem = asyncio.Semaphore(cfg["pipeline_fetch_concurrency"])
    tokenize_sem = asyncio.Semaphore(cfg["pipeline_tokenize_concurrency"])
    tokenize_tasks = []

    # 启动共享 tokenize daemon —— 整个 pipeline 生命周期内 tokenizer 只加载一次
    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    from kv_pipeline import TokenizeDaemonClient  # noqa: E402
    tokenize_daemon = TokenizeDaemonClient(
        workers=cfg["pipeline_tokenize_workers"],
        batch_size=cfg["pipeline_tokenize_batch_size"],
        default_model=cfg["pipeline_default_model"],
    )
    logger.info(f"[daemon] 启动共享 tokenize daemon (workers={cfg['pipeline_tokenize_workers']})...")
    tokenize_daemon.start(timeout=300.0)
    logger.info("[daemon] 就绪，所有切片共享同一批 worker")

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
        stage_data = {
            "status": "running",
            "message": f"序列化进度 {tokenize_done_count[0]}/{len(fetch_results)}",
            "progress": f"{tokenize_done_count[0]}/{len(fetch_results)}",
            "total_lines": tokenize_total_lines[0],
        }
        if tokenize_total_seconds[0] > 0 and tokenize_total_lines[0] > 0:
            speed = tokenize_total_lines[0] / tokenize_total_seconds[0]
            done_files = {r["file"] for r in tokenize_results if r.get("status") == "completed"}
            remaining_records = sum(
                fr["count"] for fr in fetch_results
                if os.path.basename(fr["file"]) not in done_files
            )
            stage_data["tokenize_speed"] = round(speed, 2)
            stage_data["remaining_records"] = remaining_records
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
        h_start_tag = h_start.strftime("%Y%m%d_%H%M%S")
        h_end_tag = h_end.strftime("%Y%m%d_%H%M%S")
        base_name = f"kv_{h_start_tag}_{h_end_tag}"
        incomplete_file = os.path.join(task_data_dir, f"{base_name}.jsonl.incomplete")
        final_file = os.path.join(task_data_dir, f"{base_name}.jsonl")

        es = ESIndexService(h_start.strftime("%Y-%m-%d"), app_id=app_id, path=path)

        async with fetch_sem:
            _update_fetch_progress()

            def _cb(count, msg):
                _cb_msg[0] = f"[{idx + 1}/{total_slices}] {msg}"
                _progress_dirty[0] = True

            try:
                hour_count = await es.query_to_file(
                    h_start_str, h_end_str, incomplete_file, status_callback=_cb
                )
                os.rename(incomplete_file, final_file)
                fetch_total_count[0] += hour_count
                fetch_done_count[0] += 1
                fetch_results.append({
                    "file": final_file,
                    "hour": f"{h_start_str}~{h_end_str}",
                    "count": hour_count
                })
                _update_fetch_progress()

                if hour_count > 0:
                    t = asyncio.create_task(
                        _tokenize_single_with_tracking(final_file, output_dir, task_id)
                    )
                    tokenize_tasks.append(t)

            except Exception as e:
                if not os.path.exists(incomplete_file):
                    with open(incomplete_file, 'w') as _f:
                        pass
                fetch_done_count[0] += 1
                fetch_incomplete.append({
                    "file": f"{base_name}.jsonl.incomplete",
                    "hour": f"{h_start_str}~{h_end_str}",
                    "error": str(e)[:200]
                })
                logger.warning(f"[fetch] slice {h_start_str}~{h_end_str} failed: {e}")
                _update_fetch_progress()
            finally:
                es.close()

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
        "result_files": fetch_results,
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
        "message": f"等待序列化完成 ({tokenize_done_count[0]}/{len(fetch_results)})"
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
        "files": tokenize_results
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
async def _run_simulate_stage(task_id: str):
    _update_stage(task_id, "simulate", {"status": "running", "message": "正在模拟缓存命中..."}, "simulate")

    task_data_dir = _task_dir(task_id)
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)

    status = _read_status(task_id) or {}
    tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
    model_outputs = tokenize_stage.get("model_outputs", {})

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

    async def _simulate_single_model(model: str, txt_files: list) -> dict:
        model_report_dir = os.path.join(report_dir, model)
        os.makedirs(model_report_dir, exist_ok=True)

        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"[{model}] 正在准备模拟 ({sim_done_count[0]}/{len(model_outputs)} 已完成)..."
        })

        report_file_final = os.path.join(model_report_dir, "cache_report.json")

        cfg = _load_olap_config()
        cmd = [
            "python", "-u", os.path.join(SCRIPTS_DIR, "cache_pipeline.py"),
            "-i", *sorted(txt_files),
            "-o", model_report_dir,
            "-s", str(cfg["pipeline_cache_size"]),
            "-b", str(cfg["pipeline_block_size"])
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
                if "合并" in line and "个文件" in line:
                    progress_msg = f"[{model}] 合并文件中..."
                elif "Step" in line and "缓存模拟" in line:
                    progress_msg = f"[{model}] 缓存模拟计算中..."
                elif "执行命令" in line or "cache_calc" in line:
                    progress_msg = f"[{model}] 执行 cache_calc..."
                elif "流水线执行完成" in line:
                    progress_msg = f"[{model}] 模拟完成，生成报告中..."
                if progress_msg:
                    last_progress_update = now
                    _update_stage(task_id, "simulate", {
                        "status": "running",
                        "message": progress_msg,
                    })

        await proc.wait()
        output_text = "\n".join(output_tail)

        if proc.returncode != 0:
            sim_done_count[0] += 1
            return {
                "model": model, "status": "failed",
                "error": f"模拟失败 (rc={proc.returncode}): {output_text[-500:]}"
            }

        if not os.path.exists(report_file_final):
            sim_done_count[0] += 1
            return {"model": model, "status": "failed", "error": "报告文件未生成"}

        with open(report_file_final, "r", encoding="utf-8") as f:
            report = json.load(f)

        results_list = report.get("results", [])
        summary = report.get("summary", {})
        cr = results_list[0] if results_list else {}

        sim_done_count[0] += 1
        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"模拟进度 {sim_done_count[0]}/{len(model_outputs)} 模型完成"
        })

        return {
            "model": model, "status": "completed",
            "hit_rate": cr.get("hit_rate", 0),
            "hit_rate_percent": cr.get("hit_rate_percent", 0),
            "hit_count": cr.get("hit_count", 0),
            "total_queries": cr.get("total_queries", 0),
            "total_tokens": summary.get("total_tokens", 0),
            "total_entries": summary.get("total_entries", 0),
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
    读取 status 中的 result_files，对每个已有 .jsonl 重跑 daemon tokenize。
    用于任务中断后无需重新拉取数据的快速恢复。
    """
    existing_status = _read_status(task_id)
    fetch_stage = existing_status.get("pipeline", {}).get("stages", {}).get("fetch", {})
    result_files = fetch_stage.get("result_files", [])

    if not result_files:
        logger.error("[resume] fetch 已完成但无 result_files，无法恢复 tokenize")
        _set_failed(task_id, "tokenize", "resume 失败：fetch 无 result_files")
        return

    cfg = _load_olap_config()
    output_dir_base = os.path.join(_task_dir(task_id), "tokenized")
    os.makedirs(output_dir_base, exist_ok=True)

    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    from kv_pipeline import TokenizeDaemonClient  # noqa: E402

    tokenize_daemon = TokenizeDaemonClient(
        workers=cfg["pipeline_tokenize_workers"],
        batch_size=cfg["pipeline_tokenize_batch_size"],
        default_model=cfg["pipeline_default_model"],
    )
    logger.info(f"[resume] 启动 daemon (workers={cfg['pipeline_tokenize_workers']})...")
    tokenize_daemon.start(timeout=300.0)
    logger.info(f"[resume] daemon 就绪，共 {len(result_files)} 个文件待 tokenize")

    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"恢复 tokenize，共 {len(result_files)} 个文件...",
    }, "tokenize")
    _update_stage(task_id, "simulate", {"status": "pending", "message": "等待 tokenize..."})

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
        logger.info(f"[resume] tokenize: {os.path.basename(input_file)} ({file_info.get('count', '?')} 条)")
        async with tokenize_sem:
            result = await _run_tokenize_via_daemon(tokenize_daemon, input_file, output_dir_base, task_id)
        done_count[0] += 1
        _update_stage(task_id, "tokenize", {
            "status": "running",
            "message": f"tokenize 进度 {done_count[0]}/{len(result_files)}",
        })
        return result

    try:
        tasks = [asyncio.create_task(_tokenize_one(fi)) for fi in result_files]
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
    """计算分钟级命中率趋势并保存到 report/hit_rate_trend.json"""
    _update_stage(task_id, "trend", {"status": "running", "message": "正在计算分钟级命中率趋势..."}, "trend")
    logger.info("[trend] 开始趋势计算: %s", task_id)
    try:
        from compute_trend import compute_and_save, _collect_model_outputs

        task_data_dir = _task_dir(task_id)
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
            "message": f"趋势计算完成 ({len(model_outputs)} 模型)",
            "output_file": output_file,
        })
        logger.info("[trend] 趋势计算完成: %s -> %s", task_id, output_file)
    except Exception as e:
        logger.warning(f"[trend] 趋势计算失败: {task_id}: {e}", exc_info=True)
        _update_stage(task_id, "trend", {
            "status": "failed",
            "message": f"趋势计算失败: {str(e)[:200]}",
        })


# ============================================================
# 主入口
# ============================================================
async def run_pipeline(args):
    task_id = args.task_id
    logger.info(f"Pipeline started: {task_id}")

    try:
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

        # Stage 3
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
    args = parser.parse_args()

    # 确保 HF_TOKEN 可用
    hf_token = os.environ.get("HF_TOKEN", "")
    if hf_token:
        os.environ["HF_TOKEN"] = hf_token

    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
