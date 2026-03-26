import asyncio
import glob
import json
import logging
import uuid
import os

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from app.core.api_schema import StandardResponse
from app.conf.config import settings
from app.core.request_context import log_usage
from src.domains.kv.svc import ESIndexService


# ============================================================
# 配置（从 settings 读取）
# ============================================================
BJT = timezone(timedelta(hours=8))  # 北京时间 UTC+8


def _now_bjt() -> str:
    """返回北京时间字符串 YYYY-MM-DD HH:MM:SS"""
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")
BASE_DIR = settings.OLAP_BASE_DIR
KV_RESULTS_DIR = os.path.join(BASE_DIR, settings.OLAP_DATABASE_DIR)
KV_DATA_DIR = os.path.join(KV_RESULTS_DIR, "data")
KV_STATUS_DIR = os.path.join(KV_RESULTS_DIR, "status")
SCRIPTS_DIR = os.path.join(BASE_DIR, settings.OLAP_SCRIPTS_DIR)

# Pipeline 参数
DEFAULT_MODEL = settings.PIPELINE_DEFAULT_MODEL
BLOCK_SIZE = settings.PIPELINE_BLOCK_SIZE
CACHE_SIZE = settings.PIPELINE_CACHE_SIZE
TOKENIZE_CONCURRENCY = settings.PIPELINE_TOKENIZE_CONCURRENCY
FETCH_CONCURRENCY = settings.PIPELINE_FETCH_CONCURRENCY
QPD_LIMIT = settings.OLAP_QPD_LIMIT

logger = logging.getLogger(__name__)
router = APIRouter()

os.makedirs(KV_DATA_DIR, exist_ok=True)
os.makedirs(KV_STATUS_DIR, exist_ok=True)

# 确保 HF_TOKEN 在子进程环境中可用
if settings.HF_TOKEN:
    os.environ.setdefault("HF_TOKEN", settings.HF_TOKEN)

# 运行中的 asyncio.Task 注册表，用于取消任务
_running_tasks: Dict[str, asyncio.Task] = {}


# ============================================================
# 状态管理
# ============================================================
def _extract_username(task_id: str) -> str:
    """从 task_id 中提取 username 前缀（格式: {username}-kv_...）"""
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


def _read_status(task_id: str) -> Optional[dict]:
    path = _status_file(task_id)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_status(data: dict):
    data["updated_at"] = _now_bjt()
    path = _status_file(data["task_id"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _update_stage(task_id: str, stage: str, stage_data: dict, current_stage: str = None):
    """更新某个阶段的状态，自动管理 started_at / completed_at"""
    status = _read_status(task_id)
    if not status:
        return
    if current_stage:
        status["pipeline"]["current_stage"] = current_stage

    now = _now_bjt()

    # 保留已有的 started_at，首次 running 时写入
    existing = status["pipeline"]["stages"].get(stage, {})
    if stage_data.get("status") == "running" and "started_at" not in existing:
        stage_data["started_at"] = now
    elif "started_at" in existing:
        stage_data.setdefault("started_at", existing["started_at"])

    # completed / failed 时记录 completed_at
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
# Pipeline 后台任务
# ============================================================
async def _run_pipeline(task_id: str, start_datetime: str, end_datetime: str, app_id: str, path: str = "", scheduled_at: str = ""):
    """全自动流水线: (scheduled wait →) fetch+tokenize (streaming) → simulate"""
    try:
        # ---- 定时等待 ----
        if scheduled_at:
            target = datetime.strptime(scheduled_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJT)
            delay = (target - datetime.now(BJT)).total_seconds()
            if delay > 0:
                _update_stage(task_id, "scheduled", {
                    "status": "waiting",
                    "message": f"等待定时启动 {scheduled_at}",
                    "scheduled_at": scheduled_at
                }, "scheduled")
                await asyncio.sleep(delay)

        # ---- Stage 1+2: fetch → tokenize (streaming pipeline) ----
        await _run_streaming_fetch_tokenize(task_id, start_datetime, end_datetime, app_id, path)

        # ---- Stage 3: simulate ----
        await _run_simulate_stage(task_id)

    except asyncio.CancelledError:
        logger.info(f"Pipeline cancelled for {task_id}")
        status = _read_status(task_id)
        if status:
            cur_stage = status["pipeline"].get("current_stage", "fetch")
            if cur_stage not in ("done", "failed", "cancelled"):
                status["pipeline"]["stages"].setdefault(cur_stage, {})
                status["pipeline"]["stages"][cur_stage]["status"] = "cancelled"
                status["pipeline"]["stages"][cur_stage]["message"] = "任务已被用户取消"
                status["pipeline"]["stages"][cur_stage]["completed_at"] = _now_bjt()
            status["pipeline"]["current_stage"] = "cancelled"
            status["is_deleted"] = True
            _write_status(status)

    except Exception as e:
        logger.exception(f"Pipeline failed for {task_id}")
        status = _read_status(task_id)
        if status and status["pipeline"]["current_stage"] not in ("done", "failed"):
            _set_failed(task_id, status["pipeline"]["current_stage"], str(e))


async def _run_streaming_fetch_tokenize(
    task_id: str, start_datetime: str, end_datetime: str, app_id: str, path: str = ""
):
    """
    流水线并行：fetch 切片并行拉取，每个切片完成后立即触发 tokenize。
    fetch 和 tokenize 重叠执行，总时长 ≈ max(fetch_total, tokenize_total)。

    文件状态：
      - 拉取中 / 失败: kv_xxx.jsonl.incomplete
      - 拉取成功: kv_xxx.jsonl (rename)
      - tokenize 只消费 .jsonl
    """
    _update_stage(task_id, "fetch", {"status": "running", "message": "正在查询 ES 数据..."}, "fetch")
    _update_stage(task_id, "tokenize", {"status": "pending", "message": "等待数据..."})

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

    # 共享状态（线程安全：asyncio 单线程，无需锁）
    fetch_results = []        # {"file": ..., "hour": ..., "count": ...}
    fetch_incomplete = []     # {"file": ..., "hour": ..., "error": ...}
    tokenize_results = []     # {"file": ..., "status": ..., ...}
    fetch_total_count = [0]
    fetch_done_count = [0]
    tokenize_done_count = [0]
    tokenize_total_lines = [0]

    fetch_sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    tokenize_sem = asyncio.Semaphore(TOKENIZE_CONCURRENCY)
    tokenize_tasks = []  # 收集所有 tokenize 协程

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
        _update_stage(task_id, "tokenize", {
            "status": "running",
            "message": f"序列化进度 {tokenize_done_count[0]}/{len(fetch_results)}",
            "progress": f"{tokenize_done_count[0]}/{len(fetch_results)}",
            "total_lines": tokenize_total_lines[0],
        })

    async def _fetch_slice(idx: int, h_start: datetime, h_end: datetime):
        """拉取单个切片，成功 rename 为 .jsonl，失败保留 .incomplete"""
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

            def _cb(count, msg, _tid=task_id):
                _update_stage(_tid, "fetch", {
                    "status": "running",
                    "message": f"[{idx + 1}/{total_slices}] {msg}",
                    "processed_count": fetch_total_count[0] + count
                })

            try:
                hour_count = await es.query_to_file(
                    h_start_str, h_end_str, incomplete_file, status_callback=_cb
                )
                # 成功: rename .incomplete → .jsonl
                os.rename(incomplete_file, final_file)
                fetch_total_count[0] += hour_count
                fetch_done_count[0] += 1
                fetch_results.append({
                    "file": final_file,
                    "hour": f"{h_start_str}~{h_end_str}",
                    "count": hour_count
                })
                _update_fetch_progress()

                # 立即触发 tokenize（钩子）
                if hour_count > 0:
                    t = asyncio.create_task(
                        _tokenize_single_with_tracking(final_file, output_dir, task_id)
                    )
                    tokenize_tasks.append(t)

            except Exception as e:
                # 失败: 保留 .incomplete
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

    async def _tokenize_single_with_tracking(input_file: str, out_dir: str, tid: str):
        """带信号量和进度追踪的 tokenize 单文件"""
        async with tokenize_sem:
            _update_tokenize_progress()
            result = await _run_tokenize_single_file(
                input_file, out_dir, tid, 0, 0
            )
            tokenize_results.append(result)
            tokenize_done_count[0] += 1
            if result["status"] == "completed":
                tokenize_total_lines[0] += result.get("lines", 0)
            _update_tokenize_progress()

    # ---- 启动所有 fetch 任务（并行，受信号量控制） ----
    fetch_tasks = [
        asyncio.create_task(_fetch_slice(idx, h_start, h_end))
        for idx, (h_start, h_end) in enumerate(hours)
    ]
    # 等待所有 fetch 完成
    await asyncio.gather(*fetch_tasks)

    # ---- 更新 fetch 阶段最终状态 ----
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
        raise RuntimeError("Fetch returned 0 records, pipeline stopped")

    # ---- 等待所有 tokenize 完成 ----
    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"等待序列化完成 ({tokenize_done_count[0]}/{len(fetch_results)})"
    }, "tokenize")

    if tokenize_tasks:
        await asyncio.gather(*tokenize_tasks)

    # ---- 更新 tokenize 阶段最终状态 ----
    success_files = [r for r in tokenize_results if r["status"] == "completed"]
    failed_files = [r for r in tokenize_results if r["status"] == "failed"]

    # 按 model 聚合所有 txt 文件: model -> [txt_file, ...]
    model_txt_files = {}
    for r in success_files:
        for model, txt_file in r.get("outputs", {}).items():
            model_txt_files.setdefault(model, []).append(txt_file)

    if not success_files:
        _update_stage(task_id, "tokenize", {
            "status": "failed",
            "message": f"序列化全部失败，{len(failed_files)} 个文件",
            "files": tokenize_results,
            "model_outputs": {},
            "total_lines": 0
        })
        _set_failed(task_id, "tokenize", f"全部 {len(failed_files)} 个文件序列化失败")
        raise RuntimeError("All tokenize files failed")

    status_msg = (
        f"序列化完成，{len(success_files)}/{len(tokenize_results)} 成功，"
        f"{tokenize_total_lines[0]} 条，{len(model_txt_files)} 个模型"
    )
    _update_stage(task_id, "tokenize", {
        "status": "completed",
        "message": status_msg,
        "model_outputs": {m: fs for m, fs in model_txt_files.items()},
        "total_lines": tokenize_total_lines[0],
        "success_count": len(success_files),
        "failed_count": len(failed_files),
        "models": list(model_txt_files.keys()),
        "files": tokenize_results
    })

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
        raise RuntimeError("Tokenize produced 0 lines, pipeline stopped")


async def _run_tokenize_single_file(
    input_file: str, output_dir: str, task_id: str, file_index: int, total_files: int
) -> dict:
    """对单个 jsonl 文件执行 tokenize + convert (per-model 分桶)，返回结果 dict"""
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    short_name = os.path.basename(input_file)

    cmd = [
        "python", os.path.join(SCRIPTS_DIR, "kv_pipeline.py"),
        "-i", input_file,
        "-o", output_dir,
        "-d", DEFAULT_MODEL,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=BASE_DIR
    )
    stdout, _ = await proc.communicate()
    output_text = stdout.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        return {
            "file": short_name,
            "status": "failed",
            "error": output_text[-300:],
            "outputs": {}
        }

    # 读取 pipeline_summary.json 获取 per-model 产出
    summary_file = os.path.join(output_dir, "pipeline_summary.json")
    model_outputs = {}  # model -> txt_file
    total_lines = 0

    if os.path.exists(summary_file):
        with open(summary_file, "r", encoding="utf-8") as f:
            summary = json.load(f)
        # summary.files[0].model_files: {model: {json, txt, lines}}
        for file_result in summary.get("files", []):
            for model, mf in file_result.get("model_files", {}).items():
                txt_file = mf.get("txt", "")
                lines = mf.get("lines", 0)
                if txt_file and os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
                    model_outputs[model] = txt_file
                    total_lines += lines
    else:
        # 兜底：扫描 per-model txt 文件
        pattern = os.path.join(output_dir, f"{base_name}_*_input_ids.txt")
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
            "file": short_name,
            "status": "failed",
            "error": "tokenize 无有效输出文件",
            "outputs": {}
        }

    return {
        "file": short_name,
        "status": "completed",
        "outputs": model_outputs,  # model -> txt_file
        "lines": total_lines,
        "error": None
    }


async def _run_simulate_stage(task_id: str):
    """缓存模拟: 按 model 分组并行调用 cache_pipeline.py，汇总多 model 结果"""
    _update_stage(task_id, "simulate", {"status": "running", "message": "正在模拟缓存命中..."}, "simulate")

    task_data_dir = _task_dir(task_id)
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)

    # 从 tokenize 阶段获取 per-model 文件列表
    status = _read_status(task_id)
    tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
    model_outputs = tokenize_stage.get("model_outputs", {})

    if not model_outputs:
        # 兜底: 扫描 tokenized 目录，按 model 分组
        # 文件名格式: {slice_prefix}_{model}_input_ids.txt
        # slice_prefix 格式: kv_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS
        # model 提取: 去掉 _input_ids.txt 后缀，再去掉 kv_日期_日期 前缀
        import re
        tokenized_dir = os.path.join(task_data_dir, "tokenized")
        for txt_file in sorted(glob.glob(os.path.join(tokenized_dir, "*_input_ids.txt"))):
            fname = os.path.basename(txt_file)
            # 正则: kv_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS_{model}_input_ids.txt
            m = re.match(r'kv_\d{8}_\d{6}_\d{8}_\d{6}_(.+)_input_ids\.txt$', fname)
            if m:
                model = m.group(1)
            else:
                # 兜底：取 _input_ids.txt 前面最后一段非日期部分
                model = fname.replace("_input_ids.txt", "").split("_")[-1]
            if os.path.getsize(txt_file) > 0:
                model_outputs.setdefault(model, []).append(txt_file)

    # 过滤掉空文件
    for model in list(model_outputs.keys()):
        model_outputs[model] = [
            f for f in model_outputs[model]
            if os.path.exists(f) and os.path.getsize(f) > 0
        ]
        if not model_outputs[model]:
            del model_outputs[model]

    if not model_outputs:
        _set_failed(task_id, "simulate", "无有效 input_ids 文件（全部为空）")
        raise RuntimeError("No non-empty input_ids files for simulate")

    _update_stage(task_id, "simulate", {
        "status": "running",
        "message": f"正在模拟 {len(model_outputs)} 个模型..."
    })

    # 已完成的 model 计数（用于进度更新）
    sim_done_count = [0]

    async def _simulate_single_model(model: str, txt_files: list) -> dict:
        """对单个 model 的所有 txt 文件执行 cache 模拟，产出文件使用 .incomplete 状态机"""
        model_report_dir = os.path.join(report_dir, model)
        os.makedirs(model_report_dir, exist_ok=True)

        # 更新状态：该 model 开始模拟
        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"正在模拟 {len(model_outputs)} 个模型 ({sim_done_count[0]}/{len(model_outputs)} 完成)..."
        })

        # report 产出使用 .incomplete 标记
        report_file_incomplete = os.path.join(model_report_dir, "cache_report.json.incomplete")
        report_file_final = os.path.join(model_report_dir, "cache_report.json")

        cmd = [
            "python", os.path.join(SCRIPTS_DIR, "cache_pipeline.py"),
            "-i", *sorted(txt_files),
            "-o", model_report_dir,
            "-s", str(CACHE_SIZE),
            "-b", str(BLOCK_SIZE)
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=BASE_DIR
        )
        stdout, _ = await proc.communicate()
        output_text = stdout.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            sim_done_count[0] += 1
            return {
                "model": model,
                "status": "failed",
                "error": f"模拟失败 (rc={proc.returncode}): {output_text[-300:]}"
            }

        # 读取报告（cache_pipeline.py 直接产出 cache_report.json）
        if not os.path.exists(report_file_final):
            sim_done_count[0] += 1
            return {
                "model": model,
                "status": "failed",
                "error": "报告文件未生成"
            }

        with open(report_file_final, "r", encoding="utf-8") as f:
            report = json.load(f)

        results_list = report.get("results", [])
        summary = report.get("summary", {})
        cr = results_list[0] if results_list else {}

        sim_done_count[0] += 1

        # 更新进度
        _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"正在模拟 {len(model_outputs)} 个模型 ({sim_done_count[0]}/{len(model_outputs)} 完成)..."
        })

        return {
            "model": model,
            "status": "completed",
            "hit_rate": cr.get("hit_rate", 0),
            "hit_rate_percent": cr.get("hit_rate_percent", 0),
            "hit_count": cr.get("hit_count", 0),
            "total_queries": cr.get("total_queries", 0),
            "total_tokens": summary.get("total_tokens", 0),
            "total_entries": summary.get("total_entries", 0),
            "input_files_count": len(txt_files),
            "report_file": report_file_final
        }

    # 并行执行所有 model 的 simulate
    sim_tasks = [
        _simulate_single_model(model, txt_files)
        for model, txt_files in model_outputs.items()
    ]
    sim_results = await asyncio.gather(*sim_tasks)

    # 汇总结果
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
        raise RuntimeError("All model simulations failed")

    # 构建 message
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
    _set_result(task_id, result)


# ============================================================
# QPD（每人每天请求限额）
# ============================================================


def _count_user_today(username: str) -> int:
    """统计某用户今天已提交的任务数（排除已删除）"""
    today_prefix = datetime.now(BJT).strftime("%Y-%m-%d")
    count = 0
    user_status_dir = os.path.join(KV_STATUS_DIR, username)
    if not os.path.isdir(user_status_dir):
        return 0
    for filename in os.listdir(user_status_dir):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(user_status_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                task = json.load(f)
        except Exception:
            continue
        if task.get("is_deleted"):
            continue
        if task.get("created_at", "").startswith(today_prefix):
            count += 1
    return count


def _is_official(user_info: dict) -> bool:
    """判断用户是否拥有 official 身份"""
    groups = user_info.get("groups", [])
    return "official" in groups


# ============================================================
# API 端点
# ============================================================
@router.get("/kv/qpd", summary="查询当前用户 QPD 配额")
async def kv_qpd(request: Request):
    """返回当前用户今日已用次数、配额上限、是否为 official"""
    user_info = getattr(request.state, "user", {}) or {}
    username = user_info.get("username", "unknown")
    official = _is_official(user_info)
    used = _count_user_today(username)
    return StandardResponse(
        code=0, message="success",
        data={
            "username": username,
            "used": used,
            "limit": QPD_LIMIT,
            "is_official": official,
            "remaining": max(0, QPD_LIMIT - used) if not official else -1,
        },
        trace_id=None,
    )


@router.get("/kv/tasks", summary="获取任务列表")
async def kv_task_list(
    username: Optional[str] = Query(default=None, description="按创建人 username 过滤"),
    task_name: Optional[str] = Query(default=None, description="按任务名模糊过滤"),
):
    """
    扫描 status/{username}/ 子目录，返回所有任务状态，按创建时间降序。
    支持按 username 精确过滤、按 task_name 模糊过滤。
    """
    tasks = []
    # 如果指定了 username，只扫描该用户目录；否则扫描全部
    if username:
        user_dirs = [os.path.join(KV_STATUS_DIR, username)]
    else:
        try:
            user_dirs = [
                os.path.join(KV_STATUS_DIR, d)
                for d in os.listdir(KV_STATUS_DIR)
                if os.path.isdir(os.path.join(KV_STATUS_DIR, d))
            ]
        except FileNotFoundError:
            user_dirs = []

    for user_dir in user_dirs:
        if not os.path.isdir(user_dir):
            continue
        for filename in os.listdir(user_dir):
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(user_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    task = json.load(f)
            except Exception:
                continue

            # 排除已删除任务
            if task.get("is_deleted"):
                continue
            # 按 task_name 模糊过滤
            if task_name and task_name not in task.get("task_name", ""):
                continue

            tasks.append(task)

    tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
    return StandardResponse(code=0, message="success", data=tasks, trace_id=None)


@router.get("/kv/fetch", summary="提交ES查询任务（异步）")
async def es_fetch(
    request: Request,
    start_datetime: str = Query(
        ...,
        description="开始时间（北京时间），格式 YYYY-MM-DD HH:MM:SS",
        examples=["2026-03-23 18:00:00"]
    ),
    end_datetime: str = Query(
        ...,
        description="结束时间（北京时间），格式 YYYY-MM-DD HH:MM:SS",
        examples=["2026-03-24 18:00:00"]
    ),
    app_id: str = Query(
        default=settings.ES_DEFAULT_APP_ID,
        description="应用 ID",
        examples=["app-3Lut8O2E"]
    ),
    task_name: Optional[str] = Query(
        default=None,
        description="任务名称",
    ),
    path: Optional[str] = Query(
        default=settings.PIPELINE_DEFAULT_PATH,
        description="场景过滤路径，非空添加 match_phrase 过滤，空字符串则不过滤",
    ),
    scheduled_at: Optional[str] = Query(
        default=None,
        description="定时启动时间（北京时间），格式 YYYY-MM-DD HH:MM:SS，为空则立即执行",
    )
):
    """
    提交全自动 pipeline 任务 (fetch → tokenize → simulate)。
    立即返回任务 ID，后台异步执行。
    """
    log_usage("es_fetch", scenario="OLAP")
    # 从中间件获取用户信息
    user_info = getattr(request.state, "user", {}) or {}
    username = user_info.get("username", "unknown")
    user_name = user_info.get("name", "未知用户")

    # QPD 限额检查（非 official 用户）
    if not _is_official(user_info):
        used = _count_user_today(username)
        if used >= QPD_LIMIT:
            raise HTTPException(
                status_code=429,
                detail=f"今日配额已用完（{used}/{QPD_LIMIT}），official 身份用户无此限制"
            )

    try:
        start_dt = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="时间格式错误，请使用 YYYY-MM-DD HH:MM:SS")
    if start_dt >= end_dt:
        raise HTTPException(status_code=400, detail="开始时间必须早于结束时间")
    if not path and not app_id:
        raise HTTPException(status_code=400, detail="全部场景下必须指定 App ID")

    now_bjt = datetime.now(BJT).replace(tzinfo=None)
    if scheduled_at:
        # 定时执行：启动时间必须晚于查询结束时间
        try:
            sched_dt = datetime.strptime(scheduled_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="定时启动时间格式错误")
        if sched_dt <= now_bjt:
            raise HTTPException(status_code=400, detail="定时启动时间必须在当前时间之后")
        if sched_dt < end_dt:
            raise HTTPException(status_code=400, detail="定时启动时间必须晚于查询结束时间")
    else:
        # 立即执行：查询结束时间不能超过当前时间
        if end_dt > now_bjt:
            raise HTTPException(status_code=400, detail="查询结束时间不能超过当前时间，无法查询未来数据")

    start_tag = start_dt.strftime("%Y%m%d_%H%M%S")
    end_tag = end_dt.strftime("%Y%m%d_%H%M%S")
    task_id = f"{username}-kv_{start_tag}_{end_tag}_{uuid.uuid4().hex[:8]}"

    # 场景标签映射
    scenario_label = "coding plan" if path else "all"

    # 初始化状态文件
    status_data = {
        "task_id": task_id,
        "task_name": task_name or task_id,
        "created_by": {
            "username": username,
            "name": user_name
        },
        "created_at": _now_bjt(),
        "updated_at": _now_bjt(),
        "query": {
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "app_id": app_id
        },
        "scenario": {
            "path": path or "",
            "label": scenario_label
        },
        "pipeline": {
            "current_stage": "scheduled" if scheduled_at else "fetch",
            "stages": {}
        },
        "config": {
            "default_model": DEFAULT_MODEL,
            "block_size": BLOCK_SIZE,
            "cache_size": CACHE_SIZE
        },
        "result": None,
        "is_deleted": False,
        "scheduled_at": scheduled_at or None
    }
    _write_status(status_data)

    task = asyncio.create_task(_run_pipeline(task_id, start_datetime, end_datetime, app_id, path or "", scheduled_at or ""))
    _running_tasks[task_id] = task
    task.add_done_callback(lambda t: _running_tasks.pop(task_id, None))

    return StandardResponse(
        code=0,
        message="任务已提交",
        data={"task_id": task_id},
        trace_id=None
    )


@router.get("/kv/status/{task_id}", summary="查询任务状态")
async def es_fetch_status(task_id: str):
    status = _read_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    return StandardResponse(code=0, message="success", data=status, trace_id=None)


@router.delete("/kv/tasks/{task_id}", summary="软删除任务")
async def delete_task(task_id: str):
    """
    软删除任务：
    - 已完成/失败: 直接标记 is_deleted
    - 运行中: 取消 asyncio.Task 后标记 is_deleted
    """
    log_usage("delete_task", scenario="OLAP")
    status = _read_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    if status.get("is_deleted"):
        return StandardResponse(code=0, message="任务已删除", data=None, trace_id=None)

    cur_stage = status["pipeline"].get("current_stage", "")

    # 运行中的任务 → 取消 asyncio.Task
    if cur_stage not in ("done", "failed", "cancelled"):
        running_task = _running_tasks.get(task_id)
        if running_task and not running_task.done():
            running_task.cancel()
            try:
                await running_task
            except (asyncio.CancelledError, Exception):
                pass
            # CancelledError handler 里已经设置了 is_deleted，直接返回
            return StandardResponse(code=0, message="任务已取消并删除", data=None, trace_id=None)

    # 已结束的任务 → 直接标记
    status = _read_status(task_id)  # re-read
    if status:
        status["is_deleted"] = True
        _write_status(status)

    return StandardResponse(code=0, message="任务已删除", data=None, trace_id=None)
