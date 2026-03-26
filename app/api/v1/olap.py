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
    """全自动 pipeline: (scheduled wait →) fetch → tokenize → simulate"""
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

        # ---- Stage 1: fetch ----
        await _run_fetch_stage(task_id, start_datetime, end_datetime, app_id, path)

        # ---- Stage 2: tokenize ----
        await _run_tokenize_stage(task_id)

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
        # 不再 re-raise，让 done_callback 正常清理

    except Exception as e:
        logger.exception(f"Pipeline failed for {task_id}")
        status = _read_status(task_id)
        if status and status["pipeline"]["current_stage"] != "failed":
            _set_failed(task_id, status["pipeline"]["current_stage"], str(e))


async def _run_fetch_stage(task_id: str, start_datetime: str, end_datetime: str, app_id: str, path: str = ""):
    """ES 数据拉取"""
    _update_stage(task_id, "fetch", {"status": "running", "message": "正在查询 ES 数据..."}, "fetch")

    start_dt = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    es = ESIndexService(start_dt.strftime("%Y-%m-%d"), app_id=app_id, path=path)
    task_data_dir = _task_dir(task_id)

    # 按小时拆分
    hours = []
    current = start_dt.replace(minute=0, second=0)
    if current < start_dt:
        current = start_dt
    while current < end_dt:
        hour_end = min(current.replace(minute=0, second=0) + timedelta(hours=1), end_dt)
        hours.append((current, hour_end))
        current = hour_end

    total_count = 0
    result_files = []

    for idx, (h_start, h_end) in enumerate(hours):
        h_start_str = h_start.strftime("%Y-%m-%d %H:%M:%S")
        h_end_str = h_end.strftime("%Y-%m-%d %H:%M:%S")
        h_start_tag = h_start.strftime("%Y%m%d_%H%M%S")
        h_end_tag = h_end.strftime("%Y%m%d_%H%M%S")
        result_file = os.path.join(task_data_dir, f"kv_{h_start_tag}_{h_end_tag}.jsonl")

        _update_stage(task_id, "fetch", {
            "status": "running",
            "message": f"正在查询 {h_start_str}~{h_end_str} ({idx + 1}/{len(hours)})",
            "processed_count": total_count,
            "progress": f"{idx + 1}/{len(hours)}"
        })

        def _cb(count, msg, _tid=task_id, _base=total_count):
            _update_stage(_tid, "fetch", {
                "status": "running",
                "message": msg,
                "processed_count": _base + count
            })

        hour_count = await es.query_to_file(h_start_str, h_end_str, result_file, status_callback=_cb)
        total_count += hour_count
        result_files.append({"file": result_file, "hour": f"{h_start_str}~{h_end_str}", "count": hour_count})

    _update_stage(task_id, "fetch", {
        "status": "completed",
        "message": f"查询完成，共 {total_count} 条",
        "total_count": total_count,
        "result_files": result_files
    })

    # 空数据校验：fetch 0 条直接终止 pipeline
    if total_count == 0:
        _update_stage(task_id, "tokenize", {"status": "skipped", "message": "无数据，跳过"})
        _update_stage(task_id, "simulate", {"status": "skipped", "message": "无数据，跳过"})
        status = _read_status(task_id)
        if status:
            status["pipeline"]["current_stage"] = "done"
            status["result"] = {
                "hit_rate": 0, "hit_rate_percent": 0,
                "hit_count": 0, "total_queries": 0,
                "total_tokens": 0, "total_entries": 0,
                "message": "数据提取阶段无匹配数据，请检查查询条件（时间范围、App ID、场景路径）"
            }
            _write_status(status)
        raise RuntimeError("Fetch returned 0 records, pipeline stopped")


async def _run_tokenize_single_file(
    input_file: str, output_dir: str, task_id: str, file_index: int, total_files: int
) -> dict:
    """对单个 json 文件执行 tokenize + convert，返回结果 dict"""
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    short_name = os.path.basename(input_file)
    cache_input_file = os.path.join(output_dir, f"{base_name}_input_ids.txt")

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
            "output": None
        }

    # 检查输出文件
    if not os.path.exists(cache_input_file):
        return {
            "file": short_name,
            "status": "failed",
            "error": "输出文件未生成",
            "output": None
        }

    # 统计行数
    with open(cache_input_file, "r") as f:
        line_count = sum(1 for _ in f)

    return {
        "file": short_name,
        "status": "completed",
        "output": cache_input_file,
        "lines": line_count,
        "error": None
    }


async def _run_tokenize_stage(task_id: str):
    """Token 序列化: 并行处理每个文件，支持部分失败"""
    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": "正在序列化...",
        "files": []
    }, "tokenize")

    task_data_dir = _task_dir(task_id)
    output_dir = os.path.join(task_data_dir, "tokenized")
    os.makedirs(output_dir, exist_ok=True)

    # 收集 fetch 产出的 jsonl 文件
    input_files = sorted(glob.glob(os.path.join(task_data_dir, "kv_*.jsonl")))
    if not input_files:
        _set_failed(task_id, "tokenize", "无输入文件")
        raise RuntimeError("No input files for tokenize")

    total_files = len(input_files)
    max_concurrent = min(TOKENIZE_CONCURRENCY, total_files)

    # 初始化每个文件的状态
    files_status = [
        {"file": os.path.basename(f), "status": "pending"}
        for f in input_files
    ]
    _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"正在序列化，共 {total_files} 个文件",
        "progress": f"0/{total_files}",
        "files": files_status
    })

    # 并行执行，使用信号量控制并发度
    sem = asyncio.Semaphore(max_concurrent)
    results = [None] * total_files

    async def _run_with_sem(idx, input_file):
        async with sem:
            # 标记文件为 running
            files_status[idx]["status"] = "running"
            _update_stage(task_id, "tokenize", {
                "status": "running",
                "message": f"正在序列化 [{completed_count[0] + running_count()}/{total_files}]",
                "progress": f"{completed_count[0]}/{total_files}",
                "files": files_status
            })

            result = await _run_tokenize_single_file(
                input_file, output_dir, task_id, idx + 1, total_files
            )
            results[idx] = result

            # 更新该文件状态
            files_status[idx] = result
            completed_count[0] += 1

            _update_stage(task_id, "tokenize", {
                "status": "running",
                "message": f"序列化进度 {completed_count[0]}/{total_files}",
                "progress": f"{completed_count[0]}/{total_files}",
                "files": files_status
            })

    completed_count = [0]  # mutable counter

    def running_count():
        return sum(1 for f in files_status if f.get("status") == "running")

    tasks = [
        asyncio.create_task(_run_with_sem(idx, f))
        for idx, f in enumerate(input_files)
    ]
    await asyncio.gather(*tasks)

    # 汇总
    success_files = [r for r in results if r and r["status"] == "completed"]
    failed_files = [r for r in results if r and r["status"] == "failed"]
    success_outputs = [r["output"] for r in success_files]
    total_lines = sum(r.get("lines", 0) for r in success_files)

    if not success_files:
        _update_stage(task_id, "tokenize", {
            "status": "failed",
            "message": f"序列化全部失败，{len(failed_files)} 个文件",
            "files": files_status,
            "output_files": [],
            "total_lines": 0
        })
        _set_failed(task_id, "tokenize", f"全部 {len(failed_files)} 个文件序列化失败")
        raise RuntimeError("All tokenize files failed")

    status_msg = f"序列化完成，{len(success_files)}/{total_files} 成功，{total_lines} 条"
    final_status = "completed"
    if failed_files:
        status_msg = f"序列化部分完成，{len(success_files)} 成功 / {len(failed_files)} 失败，{total_lines} 条"

    _update_stage(task_id, "tokenize", {
        "status": final_status,
        "message": status_msg,
        "output_files": success_outputs,
        "total_lines": total_lines,
        "success_count": len(success_files),
        "failed_count": len(failed_files),
        "files": files_status
    })

    # 空数据校验：序列化成功但总行数为 0，跳过模拟
    if total_lines == 0:
        _update_stage(task_id, "simulate", {"status": "skipped", "message": "序列化结果为空，跳过模拟"})
        status = _read_status(task_id)
        if status:
            status["pipeline"]["current_stage"] = "done"
            status["result"] = {
                "hit_rate": 0, "hit_rate_percent": 0,
                "hit_count": 0, "total_queries": 0,
                "total_tokens": 0, "total_entries": 0,
                "message": "序列化阶段无有效数据，无法进行缓存模拟"
            }
            _write_status(status)
        raise RuntimeError("Tokenize produced 0 lines, pipeline stopped")


async def _run_simulate_stage(task_id: str):
    """缓存模拟: 调用 cache_pipeline.py，只使用序列化成功的文件"""
    _update_stage(task_id, "simulate", {"status": "running", "message": "正在模拟缓存命中..."}, "simulate")

    task_data_dir = _task_dir(task_id)
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)

    # 从 tokenize 阶段的状态中获取成功的输出文件
    status = _read_status(task_id)
    tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
    txt_files = tokenize_stage.get("output_files", [])

    if not txt_files:
        # 兜底: 扫描目录
        tokenized_dir = os.path.join(task_data_dir, "tokenized")
        txt_files = sorted(glob.glob(os.path.join(tokenized_dir, "*_input_ids.txt")))

    # 过滤掉空文件（0 字节或只有空行）
    non_empty_files = []
    for f in txt_files:
        if os.path.exists(f) and os.path.getsize(f) > 0:
            non_empty_files.append(f)
    txt_files = non_empty_files

    if not txt_files:
        _set_failed(task_id, "simulate", "无有效 input_ids 文件（全部为空）")
        raise RuntimeError("No non-empty input_ids files for simulate")

    cmd = [
        "python", os.path.join(SCRIPTS_DIR, "cache_pipeline.py"),
        "-i", *txt_files,
        "-o", report_dir,
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
        _set_failed(task_id, "simulate", f"模拟失败 (rc={proc.returncode}): {output_text[-500:]}")
        raise RuntimeError(f"Simulate failed: rc={proc.returncode}")

    # 读取报告
    report_file = os.path.join(report_dir, "cache_report.json")
    result = {}
    if os.path.exists(report_file):
        with open(report_file, "r", encoding="utf-8") as f:
            report = json.load(f)
        # 提取核心指标（report 结构: results[], summary{})
        results_list = report.get("results", [])
        summary = report.get("summary", {})
        if results_list:
            cr = results_list[0]
            result = {
                "hit_rate": cr.get("hit_rate", 0),
                "hit_rate_percent": cr.get("hit_rate_percent", 0),
                "hit_count": cr.get("hit_count", 0),
                "total_queries": cr.get("total_queries", 0),
                "total_tokens": summary.get("total_tokens", 0),
                "total_entries": summary.get("total_entries", 0),
                "report_file": report_file
            }

    _update_stage(task_id, "simulate", {
        "status": "completed",
        "message": f"模拟完成，命中率 {result.get('hit_rate', 0) * 100:.2f}%"
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
    log_usage("kv_qpd", scenario="OLAP")
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
    log_usage("kv_task_list", scenario="OLAP")
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
    log_usage("es_fetch_status", scenario="OLAP")
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
