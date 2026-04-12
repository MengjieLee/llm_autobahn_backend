import asyncio
import fcntl
import gc
import glob
import json
import logging
import re
import threading
import time
import urllib.request
import uuid
import os

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from app.core.api_schema import StandardResponse
from app.conf.config import settings
from app.core.request_context import log_usage
from src.domains.kv.svc import ESIndexService
from app.core import k8s_client


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
OLAP_CONFIG_JSON = os.path.join(BASE_DIR, "app", "conf", "olap_config.json")
REALTIME_CONFIG_JSON = os.path.join(BASE_DIR, "app", "conf", "realtime_config.json")
KUBECONFIG_PATH = os.path.join(BASE_DIR, "app", "conf", "inner_cluster.kubeconfig")

# OLAP 热配置默认值（JSON 读取失败时的兜底）
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
    """热加载 OLAP 配置（每次读文件，修改即生效无需重启）"""
    try:
        with open(OLAP_CONFIG_JSON, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        # 用默认值填充缺失字段
        for k, v in _OLAP_DEFAULTS.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception:
        return dict(_OLAP_DEFAULTS)

# Per-task 写入锁，防止并发 read-modify-write 竞态
_status_locks: Dict[str, asyncio.Lock] = {}


def _get_status_lock(task_id: str) -> asyncio.Lock:
    if task_id not in _status_locks:
        _status_locks[task_id] = asyncio.Lock()
    return _status_locks[task_id]


def _cleanup_task_resources(task_id: str):
    """Pipeline 结束后清理该 task 占用的内存资源"""
    _status_locks.pop(task_id, None)
    _running_tasks.pop(task_id, None)
    gc.collect()

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
    # 原子写入：唯一临时文件 + rename，防止并发写入导致文件损坏
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _notify_task_done(status: dict):
    """
    任务完成/失败通知（百度 IM 机器人）。
    done:   发送命中率报告
    failed: 发送失败原因摘要

    防重复机制：用 fcntl.flock 对 status 文件加互斥锁，在锁内重读文件确认
    notified 未设置后立即写 notified=True，再释放锁发送通知。
    多 uvicorn worker 并发时只有第一个能通过检查。
    """
    task_id = status.get("task_id", "")
    status_path = _status_file(task_id)
    try:
        cfg = _load_olap_config()
        bot_url  = cfg.get("notify_im_bot_url", "")
        bot_toid = cfg.get("notify_im_bot_toid", [])
        if not bot_url or not bot_toid:
            return

        # --- 加互斥文件锁，原子性检查并标记 notified ---
        with open(status_path, "r+", encoding="utf-8") as lock_f:
            fcntl.flock(lock_f, fcntl.LOCK_EX)
            try:
                lock_f.seek(0)
                fresh = json.load(lock_f)
                if fresh.get("notified"):
                    return  # 已被其他 worker 发送，直接跳过
                # 立即写 notified=True，防止其他 worker 再次进入
                fresh["notified"] = True
                fresh["updated_at"] = _now_bjt()
                lock_f.seek(0)
                lock_f.truncate()
                json.dump(fresh, lock_f, ensure_ascii=False, indent=2)
                lock_f.flush()
                os.fsync(lock_f.fileno())
                # 用 fresh 覆盖传入的 status，保证后续构建消息用的是最新数据
                status = fresh
            finally:
                fcntl.flock(lock_f, fcntl.LOCK_UN)
        # --- 锁已释放，在锁外发送通知 ---
        bot_url  = cfg.get("notify_im_bot_url", "")
        bot_toid = cfg.get("notify_im_bot_toid", [])
        if not bot_url or not bot_toid:
            return

        query      = status.get("query", {})
        result     = status.get("result") or {}
        start_dt   = query.get("start_datetime", "")
        end_dt     = query.get("end_datetime", "")
        task_name  = status.get("task_name", status.get("task_id", ""))
        app_id     = query.get("app_id") or "全局"
        models     = query.get("models") or []
        cur_stage  = status["pipeline"].get("current_stage", "")

        header_base = (
            f"**🎯 任务**: {task_name}  `{app_id}`\n"
            f"**🗓️ 时间范围**: {start_dt} ~ {end_dt}\n"
            f"**🤖 模型**: {', '.join(models) if models else '全部'}\n"
        )

        if cur_stage == "done":
            model_lines = []
            for model, stats in result.items():
                hit_pct   = stats.get("hit_rate_percent", 0)
                hit_count = stats.get("hit_count", 0)
                total_q   = stats.get("total_queries", 0)
                total_tok = stats.get("total_tokens", 0)
                color = "green" if hit_pct >= 50 else ("orange" if hit_pct >= 20 else "red")
                model_lines.append(
                    f'> **{model}**: <font color="{color}">{hit_pct:.1f}%</font> ✅'
                    f"  命中 {hit_count:,} / {total_q:,} 次 🧮"
                )
            models_result_md = "\n".join(model_lines) if model_lines else "> 无结果数据"
            content = (
                f"##### KV 模拟命中率报告 📊\n"
                f"{header_base}"
                f"\n**各模型命中率**\n"
                f"{models_result_md}"
            )
        else:
            # failed：提取各阶段失败原因
            stages = status["pipeline"].get("stages", {})
            fail_lines = []
            for stage_name, stage_data in stages.items():
                if isinstance(stage_data, dict) and stage_data.get("status") == "failed":
                    msg = stage_data.get("message", "未知错误")
                    fail_lines.append(f"> **{stage_name}**: {msg[:200]}")
            fail_detail = "\n".join(fail_lines) if fail_lines else "> 未知原因"
            content = (
                f"##### KV Pipeline 任务失败 ❌\n"
                f"{header_base}"
                f"\n**失败原因**\n"
                f"{fail_detail}"
            )

        payload = json.dumps({
            "message": {
                "header": {"toid": bot_toid},
                "body": [{"type": "MD", "content": content}]
            }
        }, ensure_ascii=False).encode("utf-8")

        req = urllib.request.Request(
            bot_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(f"[notify] IM bot 通知成功: task={task_id} stage={cur_stage} http={resp.status}")

    except Exception as e:
        logger.warning(f"[notify] IM bot 通知失败: {e}")


async def _update_stage(task_id: str, stage: str, stage_data: dict, current_stage: str = None):
    """更新某个阶段的状态，自动管理 started_at / completed_at"""
    async with _get_status_lock(task_id):
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


async def _set_result(task_id: str, result: dict):
    async with _get_status_lock(task_id):
        status = _read_status(task_id)
        if not status:
            return
        status["pipeline"]["current_stage"] = "done"
        status["result"] = result
        _write_status(status)


async def _set_failed(task_id: str, stage: str, error_msg: str):
    async with _get_status_lock(task_id):
        status = _read_status(task_id)
        if not status:
            return
        status["pipeline"]["current_stage"] = "failed"
        status["pipeline"]["stages"][stage] = {"status": "failed", "message": error_msg}
        _write_status(status)


# ============================================================
# K8s Job 生命周期监听
# ============================================================
async def _watch_k8s_job(task_id: str):
    """
    轻量级 K8s Job 监听：轮询 Job 状态直到终态，然后触发通知。
    每个 K8s 任务创建后启动一个 asyncio task 运行此函数。
    """
    cfg = _load_olap_config()
    poll_interval = 60  # 每 60 秒轮询一次
    # K8s Job 已到终态后，等待 status.json 追赶的最大轮数
    MAX_STATUS_WAIT_ROUNDS = 10  # 最多再等 10 轮 (10 * 60s = 10 分钟)
    status_wait_rounds = 0

    try:
        while True:
            await asyncio.sleep(poll_interval)
            try:
                job_status = await asyncio.to_thread(
                    k8s_client.get_job_status, KUBECONFIG_PATH, cfg, task_id
                )
            except Exception:
                logger.debug(f"[k8s-watch] 查询 Job 状态异常: {task_id}", exc_info=True)
                continue

            if not job_status:
                # Job 不存在（被 TTL 清理或删除）
                # 检查 status.json，可能 pipeline 已经完成但 Job 被回收了
                status = _read_status(task_id)
                if status:
                    cur_stage = status["pipeline"].get("current_stage", "")
                    if cur_stage in ("done", "failed") and not status.get("notified"):
                        logger.info(f"[k8s-watch] Job 已消失但任务已结束，补发通知: {task_id}")
                        _notify_task_done(status)
                logger.info(f"[k8s-watch] Job 不存在，停止监听: {task_id}")
                return

            k8s_st = job_status["status"]

            if k8s_st in ("completed", "failed"):
                # 读取最新 status 文件
                status = _read_status(task_id)
                if not status:
                    return

                cur_stage = status["pipeline"].get("current_stage", "")

                # 如果 status.json 已经是终态，直接通知
                if cur_stage in ("done", "failed"):
                    _notify_task_done(status)
                    return

                # K8s Job 失败但 status.json 未更新（OOM/DeadlineExceeded等），同步状态后通知
                if k8s_st == "failed":
                    reason = job_status.get("reason", "")
                    msg = job_status.get("message", "")
                    fail_msg = f"K8s Job 异常终止: {reason}" if reason else "K8s Job 异常终止"
                    if msg:
                        fail_msg += f" ({msg})"
                    stage = status["pipeline"].get("current_stage", "fetch")
                    status["pipeline"]["current_stage"] = "failed"
                    status["pipeline"].setdefault("stages", {}).setdefault(stage, {})
                    status["pipeline"]["stages"][stage]["status"] = "failed"
                    status["pipeline"]["stages"][stage]["message"] = fail_msg
                    status["pipeline"]["stages"][stage]["completed_at"] = _now_bjt()
                    _write_status(status)
                    _notify_task_done(status)
                    return

                # K8s Job completed 但 status.json 还不是 done — pipeline 可能还在写文件
                status_wait_rounds += 1
                if status_wait_rounds >= MAX_STATUS_WAIT_ROUNDS:
                    # 等待超时：K8s Job 已 completed 但 status.json 始终没到终态
                    # 标记失败并通知，防止无限空转
                    logger.warning(
                        f"[k8s-watch] K8s Job completed 但 status.json 停滞在 '{cur_stage}'，"
                        f"等待 {status_wait_rounds} 轮后强制标记失败: {task_id}"
                    )
                    stage = cur_stage or "simulate"
                    status["pipeline"]["current_stage"] = "failed"
                    status["pipeline"].setdefault("stages", {}).setdefault(stage, {})
                    status["pipeline"]["stages"][stage]["status"] = "failed"
                    status["pipeline"]["stages"][stage]["message"] = (
                        f"K8s Job 已完成但 Pipeline 状态停滞在 {cur_stage}，"
                        f"可能是 Pod 进程在写入最终结果前被回收"
                    )
                    status["pipeline"]["stages"][stage]["completed_at"] = _now_bjt()
                    _write_status(status)
                    _notify_task_done(status)
                    return

                logger.debug(
                    f"[k8s-watch] K8s Job completed 但 status={cur_stage}，"
                    f"等待 status.json 更新 ({status_wait_rounds}/{MAX_STATUS_WAIT_ROUNDS}): {task_id}"
                )
                continue

    except asyncio.CancelledError:
        logger.info(f"[k8s-watch] 监听被取消: {task_id}")
    except Exception:
        logger.warning(f"[k8s-watch] 监听异常退出: {task_id}", exc_info=True)


# ============================================================
# Stage 4: 分钟级命中率趋势
# ============================================================
async def _run_trend_stage(task_id: str):
    """计算分钟级命中率趋势并保存到 report/hit_rate_trend.json"""
    await _update_stage(task_id, "trend", {"status": "running", "message": "正在计算分钟级命中率趋势..."}, "trend")
    try:
        from scripts.compute_trend import compute_and_save, _collect_model_outputs

        task_data_dir = _task_dir(task_id)
        status = _read_status(task_id)
        tokenize_stage = (status or {}).get("pipeline", {}).get("stages", {}).get("tokenize", {})
        status_model_outputs = tokenize_stage.get("model_outputs")
        model_outputs = _collect_model_outputs(task_data_dir, status_model_outputs)

        if not model_outputs:
            await _update_stage(task_id, "trend", {
                "status": "completed",
                "message": "无 input_ids 文件，跳过趋势计算",
            })
            return

        cfg = _load_olap_config()
        cache_size = cfg.get("pipeline_cache_size", 200000000)
        block_size = cfg.get("pipeline_block_size", 16)

        # 在线程池中执行（CPU 密集 + subprocess 调用）
        output_file = await asyncio.to_thread(
            compute_and_save,
            task_data_dir=task_data_dir,
            cache_size=cache_size,
            block_size=block_size,
            model_outputs=model_outputs,
        )

        await _update_stage(task_id, "trend", {
            "status": "completed",
            "message": f"趋势计算完成 ({len(model_outputs)} 模型)",
            "output_file": output_file,
        })
    except Exception as e:
        # 趋势计算失败不影响整体 pipeline，仅标记 trend 阶段失败
        logger.warning(f"[trend] 趋势计算失败: {task_id}: {e}", exc_info=True)
        await _update_stage(task_id, "trend", {
            "status": "failed",
            "message": f"趋势计算失败: {str(e)[:200]}",
        })


# ============================================================
# Pipeline 后台任务
# ============================================================
async def _run_pipeline(task_id: str, start_datetime: str, end_datetime: str, app_id: str, path: str = "", scheduled_at: str = ""):
    """全自动流水线: (scheduled wait →) fetch+tokenize (streaming) → simulate → trend"""
    try:
        # ---- 定时等待 ----
        if scheduled_at:
            target = datetime.strptime(scheduled_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJT)
            delay = (target - datetime.now(BJT)).total_seconds()
            if delay > 0:
                await _update_stage(task_id, "scheduled", {
                    "status": "waiting",
                    "message": f"等待定时启动 {scheduled_at}",
                    "scheduled_at": scheduled_at
                }, "scheduled")
                await asyncio.sleep(delay)

        # ---- Stage 1+2: fetch → tokenize (streaming pipeline) ----
        await _run_streaming_fetch_tokenize(task_id, start_datetime, end_datetime, app_id, path)

        # ---- Stage 3: simulate ----
        await _run_simulate_stage(task_id)

        # ---- Stage 4: 分钟级命中率趋势 ----
        await _run_trend_stage(task_id)

        # ---- 标记 done ----
        async with _get_status_lock(task_id):
            status = _read_status(task_id)
            if status and status["pipeline"].get("current_stage") not in ("done", "failed"):
                status["pipeline"]["current_stage"] = "done"
                _write_status(status)

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
            await _set_failed(task_id, status["pipeline"]["current_stage"], str(e))

    finally:
        _cleanup_task_resources(task_id)
        # 生命周期通知：本地模式任务完成/失败时立即发送通知
        try:
            status = _read_status(task_id)
            if status and status["pipeline"].get("current_stage") in ("done", "failed"):
                _notify_task_done(status)
        except Exception:
            logger.warning(f"[notify] 本地模式通知失败: {task_id}", exc_info=True)



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
    await _update_stage(task_id, "fetch", {"status": "running", "message": "正在查询 ES 数据..."}, "fetch")
    await _update_stage(task_id, "tokenize", {"status": "pending", "message": "等待数据..."})

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
    fetch_results = []        # {"file": ..., "hour": ..., "count": ...}
    fetch_incomplete = []     # {"file": ..., "hour": ..., "error": ...}
    tokenize_results = []     # {"file": ..., "status": ..., ...}
    fetch_total_count = [0]
    fetch_done_count = [0]
    tokenize_done_count = [0]
    tokenize_total_lines = [0]
    tokenize_total_seconds = [0.0]  # 累计各切片 tokenize 实际耗时（秒），用于计算速度

    # _cb 在线程池中被调用（run_in_executor），不能做文件 I/O
    # 只写内存，由 _progress_flusher 定期刷盘
    _cb_msg = [None]          # 最新的 _cb 消息（线程安全：单次赋值）
    _progress_dirty = [False] # 标记是否有新进度需要刷盘

    cfg = _load_olap_config()
    fetch_sem = asyncio.Semaphore(cfg["pipeline_fetch_concurrency"])
    tokenize_sem = asyncio.Semaphore(cfg["pipeline_tokenize_concurrency"])
    tokenize_tasks = []  # 收集所有 tokenize 协程

    async def _update_fetch_progress():
        msg_parts = [f"拉取进度 {fetch_done_count[0]}/{total_slices}，已获取 {fetch_total_count[0]} 条"]
        if fetch_incomplete:
            msg_parts.append(f"，{len(fetch_incomplete)} 个失败")
        await _update_stage(task_id, "fetch", {
            "status": "running",
            "message": "".join(msg_parts),
            "processed_count": fetch_total_count[0],
            "progress": f"{fetch_done_count[0]}/{total_slices}",
        })

    async def _update_tokenize_progress():
        if tokenize_done_count[0] == 0:
            return
        # 计算基于记录数的序列化速度和剩余记录
        stage_data = {
            "status": "running",
            "message": f"序列化进度 {tokenize_done_count[0]}/{len(fetch_results)}",
            "progress": f"{tokenize_done_count[0]}/{len(fetch_results)}",
            "total_lines": tokenize_total_lines[0],
        }
        if tokenize_total_seconds[0] > 0 and tokenize_total_lines[0] > 0:
            speed = tokenize_total_lines[0] / tokenize_total_seconds[0]  # 记录/秒
            # 已完成 tokenize 的切片文件集合
            done_files = {r["file"] for r in tokenize_results if r.get("status") == "completed"}
            # 从 fetch_results 中累加未完成切片的记录数
            remaining_records = sum(
                fr["count"] for fr in fetch_results
                if os.path.basename(fr["file"]) not in done_files
            )
            stage_data["tokenize_speed"] = round(speed, 2)
            stage_data["remaining_records"] = remaining_records
        await _update_stage(task_id, "tokenize", stage_data)

    async def _progress_flusher():
        """每 2 秒将 _cb 线程回调写入的内存进度刷到 status 文件"""
        while True:
            await asyncio.sleep(2)
            if _progress_dirty[0] and _cb_msg[0]:
                _progress_dirty[0] = False
                await _update_stage(task_id, "fetch", {
                    "status": "running",
                    "message": _cb_msg[0],
                    "processed_count": fetch_total_count[0],
                })

    flusher_task = asyncio.create_task(_progress_flusher())

    async def _fetch_slice(idx: int, h_start: datetime, h_end: datetime):
        """拉取单个切片，per-minute 文件直写 hour 目录"""
        h_start_str = h_start.strftime("%Y-%m-%d %H:%M:%S")
        h_end_str = h_end.strftime("%Y-%m-%d %H:%M:%S")
        hour_dir_name = h_start.strftime("%H")
        hour_dir = os.path.join(task_data_dir, hour_dir_name)
        os.makedirs(hour_dir, exist_ok=True)

        max_scroll_retries = 8
        for scroll_attempt in range(max_scroll_retries + 1):
            es = ESIndexService(h_start.strftime("%Y-%m-%d"), app_id=app_id, path=path)

            async with fetch_sem:
                await _update_fetch_progress()

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

                    for fi in result["files"]:
                        fetch_results.append({
                            "file": fi["file"],
                            "hour": f"{h_start_str}~{h_end_str}",
                            "minute": fi["minute"],
                            "count": fi["count"]
                        })
                        if fi["count"] > 0 and os.path.exists(fi["file"]):
                            t = asyncio.create_task(
                                _tokenize_single_with_tracking(fi["file"], output_dir, task_id)
                            )
                            tokenize_tasks.append(t)

                    await _update_fetch_progress()
                    es.close()
                    return

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
                    await _update_fetch_progress()
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
        await _update_fetch_progress()

    async def _tokenize_single_with_tracking(input_file: str, out_dir: str, tid: str):
        """带信号量和进度追踪的 tokenize 单文件"""
        async with tokenize_sem:
            await _update_tokenize_progress()
            result = await _run_tokenize_single_file(
                input_file, out_dir, tid, 0, 0
            )
            tokenize_results.append(result)
            tokenize_done_count[0] += 1
            if result["status"] == "completed":
                tokenize_total_lines[0] += result.get("lines", 0)
                tokenize_total_seconds[0] += result.get("duration_seconds", 0.0)
            await _update_tokenize_progress()

    # ---- 启动所有 fetch 任务（并行，受信号量控制） ----
    fetch_tasks = [
        asyncio.create_task(_fetch_slice(idx, h_start, h_end))
        for idx, (h_start, h_end) in enumerate(hours)
    ]
    try:
        # 等待所有 fetch 完成
        await asyncio.gather(*fetch_tasks)
    finally:
        # 确保 flusher 在任何退出路径（完成/cancel/异常）下都被清理
        flusher_task.cancel()
        # 释放已完成的 fetch Task 对象（持有协程栈帧、闭包引用）
        fetch_tasks.clear()

    # ---- 更新 fetch 阶段最终状态 ----
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
    await _update_stage(task_id, "fetch", fetch_status)

    # 空数据校验
    if fetch_total_count[0] == 0:
        await _update_stage(task_id, "tokenize", {"status": "skipped", "message": "无数据，跳过"})
        await _update_stage(task_id, "simulate", {"status": "skipped", "message": "无数据，跳过"})
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
    await _update_stage(task_id, "tokenize", {
        "status": "running",
        "message": f"等待序列化完成 ({tokenize_done_count[0]}/{len(fetch_results)})"
    }, "tokenize")

    if tokenize_tasks:
        await asyncio.gather(*tokenize_tasks)
        # 释放已完成的 tokenize Task 对象
        tokenize_tasks.clear()

    # ---- 更新 tokenize 阶段最终状态 ----
    success_files = [r for r in tokenize_results if r["status"] == "completed"]
    failed_files = [r for r in tokenize_results if r["status"] == "failed"]

    # 按 model 聚合所有 txt 文件: model -> [txt_file, ...]
    model_txt_files = {}
    for r in success_files:
        for model, txt_file in r.get("outputs", {}).items():
            model_txt_files.setdefault(model, []).append(txt_file)

    # 按用户选择的模型过滤（不选则保留全部）
    status = _read_status(task_id)
    selected_models = status.get("query", {}).get("models", []) if status else []
    all_detected_models = list(model_txt_files.keys())
    if selected_models:
        skipped_models = [m for m in model_txt_files if m not in selected_models]
        model_txt_files = {m: fs for m, fs in model_txt_files.items() if m in selected_models}
        if skipped_models:
            logger.info(f"[tokenize] 按模型过滤: 保留 {list(model_txt_files.keys())}，跳过 {skipped_models}")

    # 选定模型在数据中不存在：序列化成功但过滤后无匹配模型
    if success_files and selected_models and not model_txt_files:
        msg = (
            f"序列化完成，但所选模型 {selected_models} 在数据中未检测到。"
            f"实际检测到的模型: {all_detected_models}"
        )
        logger.warning(f"[tokenize] {msg}")
        await _update_stage(task_id, "tokenize", {
            "status": "completed",
            "message": msg,
            "model_outputs": {},
            "total_lines": 0,
            "success_count": len(success_files),
            "failed_count": len(failed_files),
            "models": [],
            "all_detected_models": all_detected_models,
            "files": tokenize_results
        })
        await _update_stage(task_id, "simulate", {"status": "skipped", "message": "所选模型未匹配，跳过模拟"})
        # 为每个选定模型返回 0 结果
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
        status = _read_status(task_id)
        if status:
            status["pipeline"]["current_stage"] = "done"
            status["result"] = zero_result
            _write_status(status)
        fetch_results.clear()
        fetch_incomplete.clear()
        tokenize_results.clear()
        raise RuntimeError(f"Selected models {selected_models} not found in data, detected: {all_detected_models}")

    if not success_files:
        await _update_stage(task_id, "tokenize", {
            "status": "failed",
            "message": f"序列化全部失败，{len(failed_files)} 个文件",
            "files": tokenize_results,
            "model_outputs": {},
            "total_lines": 0
        })
        await _set_failed(task_id, "tokenize", f"全部 {len(failed_files)} 个文件序列化失败")
        raise RuntimeError("All tokenize files failed")

    status_msg = (
        f"序列化完成，{len(success_files)}/{len(tokenize_results)} 成功，"
        f"{tokenize_total_lines[0]} 条，{len(model_txt_files)} 个模型"
    )
    if selected_models:
        status_msg += f"（已过滤，检测到 {len(all_detected_models)} 个模型）"
    await _update_stage(task_id, "tokenize", {
        "status": "completed",
        "message": status_msg,
        "model_outputs": {m: fs for m, fs in model_txt_files.items()},
        "total_lines": tokenize_total_lines[0],
        "success_count": len(success_files),
        "failed_count": len(failed_files),
        "models": list(model_txt_files.keys()),
        "all_detected_models": all_detected_models,
        "files": tokenize_results
    })

    # ---- 释放流水线中间数据，避免持续占用内存 ----
    fetch_results.clear()
    fetch_incomplete.clear()
    tokenize_results.clear()
    del success_files, failed_files

    if tokenize_total_lines[0] == 0:
        await _update_stage(task_id, "simulate", {"status": "skipped", "message": "序列化结果为空，跳过模拟"})
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
    input_file: str, output_dir: str, task_id: str, file_index: int, total_files: int,
) -> dict:
    """对单个 jsonl 文件执行 tokenize (per-model 分桶，直接输出 txt)，实时上报进度"""
    import re as _re
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    short_name = os.path.basename(input_file)

    # 每个切片使用独立子目录，避免并行时 pipeline_summary.json 互相覆盖
    slice_output_dir = os.path.join(output_dir, base_name)
    os.makedirs(slice_output_dir, exist_ok=True)

    cfg = _load_olap_config()
    cmd = [
        "python", "-u", os.path.join(SCRIPTS_DIR, "kv_pipeline.py"),
        "-i", input_file,
        "-o", slice_output_dir,
        "-d", cfg["pipeline_default_model"],
        "--tokenize-workers", str(cfg["pipeline_tokenize_workers"]),
        "--tokenize-batch-size", str(cfg["pipeline_tokenize_batch_size"]),
    ]
    # 注意：不再向 kv_pipeline.py 传递 -m 模型过滤参数
    # 让子进程产出所有模型的 txt 文件，模型过滤统一在聚合层完成
    # 这样即使用户选择的模型在数据中不存在，也能正确报告实际检测到的模型

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=BASE_DIR
    )

    # 逐行读取子进程 stdout，实时解析进度并更新 status
    output_tail = deque(maxlen=30)
    last_progress_update = 0  # 上次更新 status 的时间戳，节流 2s
    async for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace").strip()
        output_tail.append(line)

        # 解析 kv_pipeline.py / tokenize_script.py 的进度行
        # 格式: "[INFO] 进度: 10000/298+, 成功: 9800, 失败: 200, 速度: 1234 条/秒"
        # 或:   "  [INFO] 进度: ..."（带缩进，来自 kv_pipeline 转发）
        now = asyncio.get_event_loop().time()
        if now - last_progress_update >= 2:
            progress_msg = None
            # 提取最新的 [INFO] 进度行
            m = _re.search(r'\[INFO\]\s*进度:\s*(\d+)/(\d+)\S*.*成功:\s*(\d+).*失败:\s*(\d+).*速度:\s*([\d.]+)', line)
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
                await _update_stage(task_id, "tokenize", {
                    "status": "running",
                    "message": progress_msg,
                })

    await proc.wait()
    output_text = "\n".join(output_tail)

    if proc.returncode != 0:
        return {
            "file": short_name,
            "status": "failed",
            "error": output_text[-500:],
            "outputs": {}
        }

    # 读取 pipeline_summary.json 获取 per-model 产出
    summary_file = os.path.join(slice_output_dir, "pipeline_summary.json")
    model_outputs = {}  # model -> txt_file
    total_lines = 0
    duration_seconds = 0.0

    if os.path.exists(summary_file):
        with open(summary_file, "r", encoding="utf-8") as f:
            summary = json.load(f)
        duration_seconds = summary.get("duration_seconds", 0.0)
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
        "duration_seconds": duration_seconds,
        "error": None
    }


async def _run_simulate_stage(task_id: str):
    """缓存模拟: 按 model 分组并行调用 cache_pipeline.py，汇总多 model 结果"""
    await _update_stage(task_id, "simulate", {"status": "running", "message": "正在模拟缓存命中..."}, "simulate")

    task_data_dir = _task_dir(task_id)
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)

    # 从 tokenize 阶段获取 per-model 文件列表
    status = _read_status(task_id)
    tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
    model_outputs = tokenize_stage.get("model_outputs", {})

    if not model_outputs:
        # 兜底: 扫描 tokenized 子目录，按 model 分组
        # 文件路径: tokenized/{slice_name}/{slice_name}_{model}_input_ids.txt
        import re
        tokenized_dir = os.path.join(task_data_dir, "tokenized")
        for txt_file in sorted(glob.glob(os.path.join(tokenized_dir, "**", "*_input_ids.txt"), recursive=True)):
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
        await _set_failed(task_id, "simulate", "无有效 input_ids 文件（全部为空）")
        raise RuntimeError("No non-empty input_ids files for simulate")

    await _update_stage(task_id, "simulate", {
        "status": "running",
        "message": f"正在模拟 {len(model_outputs)} 个模型..."
    })

    # 已完成的 model 计数（用于进度更新）
    sim_done_count = [0]

    async def _simulate_single_model(model: str, txt_files: list) -> dict:
        """对单个 model 的所有 txt 文件执行 cache 模拟，实时上报 stdout 进度"""
        model_report_dir = os.path.join(report_dir, model)
        os.makedirs(model_report_dir, exist_ok=True)

        # 更新状态：该 model 开始模拟
        await _update_stage(task_id, "simulate", {
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

        # 逐行读取 stdout，实时解析进度（仅保留尾部用于错误诊断）
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
                    await _update_stage(task_id, "simulate", {
                        "status": "running",
                        "message": progress_msg,
                    })

        await proc.wait()
        output_text = "\n".join(output_tail)

        if proc.returncode != 0:
            sim_done_count[0] += 1
            return {
                "model": model,
                "status": "failed",
                "error": f"模拟失败 (rc={proc.returncode}): {output_text[-500:]}"
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
        await _update_stage(task_id, "simulate", {
            "status": "running",
            "message": f"模拟进度 {sim_done_count[0]}/{len(model_outputs)} 模型完成"
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
    # 释放 simulate Task 和中间数据
    sim_tasks.clear()
    model_outputs.clear()

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
        await _set_failed(task_id, "simulate", f"全部模型模拟失败: {errors}")
        raise RuntimeError("All model simulations failed")

    # 构建 message
    completed_models = [sr for sr in sim_results if sr["status"] == "completed"]
    msg_parts = []
    for sr in completed_models:
        msg_parts.append(f"{sr['model']} {sr['hit_rate'] * 100:.2f}%")
    sim_msg = f"模拟完成 ({len(completed_models)}/{len(sim_results)} 模型): " + ", ".join(msg_parts)

    await _update_stage(task_id, "simulate", {
        "status": "completed" if all_ok else "partial",
        "message": sim_msg,
        "models": [sr["model"] for sr in sim_results],
    })

    # 将 result 暂存到 status 但不设 done（由 pipeline 在 trend 之后统一 _set_result）
    async with _get_status_lock(task_id):
        status = _read_status(task_id)
        if status:
            status["result"] = result
            _write_status(status)


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


def _load_model_list() -> list:
    """热加载模型列表（从 olap_config.json 读取）"""
    return _load_olap_config().get("models", [])


# ============================================================
# API 端点
# ============================================================
@router.get("/kv/file-tree", summary="获取任务数据目录树（懒加载）")
async def kv_file_tree(
    task_id: str = Query(..., description="任务 ID"),
    path: str = Query(default="", description="相对于任务目录的子路径，空表示根目录"),
):
    """
    懒加载目录树：前端每展开一层目录调用一次。
    返回该层的子节点列表，每个节点包含 name/full_path/is_dir/is_leaf/meta。
    """
    status = _read_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="任务不存在")

    task_dir = _task_dir(task_id)
    if not os.path.isdir(task_dir):
        raise HTTPException(status_code=404, detail="任务目录不存在")

    # 拼接目标目录，防止路径穿越
    if path:
        target = os.path.normpath(os.path.join(task_dir, path))
        if not target.startswith(task_dir):
            raise HTTPException(status_code=400, detail="非法路径")
    else:
        target = task_dir

    if not os.path.isdir(target):
        return StandardResponse(code=0, message="success", data=[], trace_id=None)

    children = []
    try:
        entries = sorted(os.listdir(target))
    except OSError:
        entries = []

    for name in entries:
        full = os.path.join(target, name)
        rel = os.path.relpath(full, task_dir)
        node = {
            "name": name,
            "full_path": full,
            "rel_path": rel,
            "is_dir": os.path.isdir(full),
            "is_leaf": not os.path.isdir(full),
        }
        # 文件元信息
        if not node["is_dir"]:
            try:
                size = os.path.getsize(full)
                node["size"] = size
                node["size_label"] = (
                    f"{size / 1024 / 1024:.1f} MB" if size >= 1024 * 1024
                    else f"{size / 1024:.1f} KB" if size >= 1024
                    else f"{size} B"
                )
            except OSError:
                node["size"] = 0
                node["size_label"] = ""
        children.append(node)

    # 根目录请求时返回根信息
    root_info = None
    if not path:
        root_info = {
            "root_path": task_dir,
            "root_name": os.path.basename(task_dir),
        }

    return StandardResponse(
        code=0, message="success",
        data={"root": root_info, "children": children},
        trace_id=None,
    )


# ============================================================
# 日报数据目录
# ============================================================
DAILY_REPORTS_DIR = os.path.join(KV_RESULTS_DIR, "daily_reports")


def _save_daily_report(date_label: str, task_data: dict):
    """
    将已完成任务的 result 写入 daily_reports/{MM-DD}.json。
    每个日期一个文件，内含所有场景的聚合数据。

    task_data 格式:
      {"task_name": "04-08_全场景_各模型", "result": {"glm-5": {...}, ...}}
    """
    os.makedirs(DAILY_REPORTS_DIR, exist_ok=True)
    filepath = os.path.join(DAILY_REPORTS_DIR, f"{date_label}.json")

    # 读取已有数据
    existing = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    task_name = task_data.get("task_name", "")
    result = task_data.get("result", {})
    if not result or not isinstance(result, dict):
        return

    # 提取场景名
    import re as _re
    scenario = task_name
    # 去掉 【xxx】 前缀
    scenario = _re.sub(r"^【[^】]+】", "", scenario)
    # 去掉日期前缀
    target_mm, target_dd = date_label.split("-")
    _patterns = [
        rf"^{_re.escape(date_label)}_(.+)",                         # 04-08_xxx
        rf"^{target_mm}{target_dd}_(.+)",                           # 0408_xxx
        rf"^{_re.escape(date_label)}{target_mm}{target_dd}_(.+)",   # 04-080408_xxx
        rf"^{_re.escape(date_label)}{_re.escape(date_label)}_(.+)", # 04-0804-08_xxx
    ]
    for _pat in _patterns:
        _m = _re.match(_pat, scenario)
        if _m:
            scenario = _m.group(1)
            break
    else:
        return

    # 构建场景数据
    total_hit = sum(r.get("hit_count", 0) for r in result.values())
    total_queries = sum(r.get("total_queries", 0) for r in result.values())
    total_tokens = sum(r.get("total_tokens", 0) for r in result.values())
    hit_rate_pct = round((total_hit / total_queries * 100), 2) if total_queries > 0 else 0

    scenario_data = {
        "hit_rate_percent": hit_rate_pct,
        "hit_count": total_hit,
        "total_queries": total_queries,
        "total_tokens": total_tokens,
        "models": {
            model: {
                "hit_rate_percent": stats.get("hit_rate_percent", 0),
                "hit_count": stats.get("hit_count", 0),
                "total_queries": stats.get("total_queries", 0),
                "total_tokens": stats.get("total_tokens", 0),
            }
            for model, stats in result.items()
        },
        "updated_at": task_data.get("updated_at", _now_bjt()),
    }

    scenarios = existing.get("scenarios", {})
    scenarios[scenario] = scenario_data

    existing.update({
        "date": date_label,
        "updated_at": _now_bjt(),
        "scenarios": scenarios,
    })

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)


# ============================================================
# Dashboard — 场景列表（固定 8 个）
# ============================================================
DASHBOARD_SCENARIOS = [
    "全场景_各模型",
    "coding_plan_各模型",
    "讯飞_全场景_glm-5",
    "无问芯穹_全场景_glm-5",
    "得物_全场景_glm-5",
    "金山_全场景_glm-5",
    "腾讯_全场景_glm-5",
    "智谱_全场景_glm-5",
]

# 时间范围 → 秒数映射
_TIME_RANGE_MAP = {
    "1h": 3600,
    "6h": 21600,
    "1d": 86400,
    "7d": 604800,
    "30d": 2592000,
}

# 缓存: scenario → {task_id, trend_data, updated_at}
_dashboard_cache: dict = {}


def _find_latest_task_for_scenario(scenario: str) -> Optional[dict]:
    """在 status 目录中查找指定场景的最新已完成任务"""
    best = None
    for status_file in glob.glob(os.path.join(KV_STATUS_DIR, "**/*.json"), recursive=True):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            task_name = data.get("task_name", "")
            if scenario not in task_name:
                continue
            if data.get("pipeline", {}).get("current_stage") != "done":
                continue
            if not data.get("result") or not isinstance(data["result"], dict):
                continue
            updated = data.get("updated_at", "")
            if not best or updated > best[0]:
                best = (updated, data)
        except Exception:
            continue
    return best[1] if best else None


def _find_tasks_for_scenario_in_range(scenario: str, cutoff_naive: datetime, now_naive: datetime) -> List[dict]:
    """
    查找指定场景在时间范围内的所有已完成任务，按时间排序。
    用于非实时场景的趋势图数据聚合。
    """
    tasks = []
    for status_file in glob.glob(os.path.join(KV_STATUS_DIR, "**/*.json"), recursive=True):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            task_name = data.get("task_name", "")
            if scenario not in task_name:
                continue
            if data.get("pipeline", {}).get("current_stage") != "done":
                continue
            if not data.get("result") or not isinstance(data["result"], dict):
                continue

            # 解析任务时间
            updated_str = data.get("updated_at", "")
            if not updated_str:
                continue
            try:
                # 兼容格式 "2026-04-10 15:30:00"
                updated_dt = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            # 检查是否在时间范围内
            if cutoff_naive <= updated_dt <= now_naive:
                tasks.append((updated_dt, data))
        except Exception:
            continue

    # 按时间排序
    tasks.sort(key=lambda x: x[0])
    return [t[1] for t in tasks]


def _read_trend_for_task(task_id: str) -> Optional[dict]:
    """读取任务的 hit_rate_trend.json"""
    trend_path = os.path.join(_task_dir(task_id), "report", "hit_rate_trend.json")
    if not os.path.exists(trend_path):
        return None
    try:
        with open(trend_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _parse_trend_time(raw_time: str, year: int) -> tuple:
    """
    解析趋势数据中的时间格式。
    兼容新格式 "MM-DD HH:MM" 和旧格式 "HH:MM"。
    返回: (标准化时间字符串, datetime对象)，解析失败返回 (None, None)
    """
    if not raw_time:
        return None, None

    # 尝试新格式 "MM-DD HH:MM"
    if " " in raw_time and "-" in raw_time:
        try:
            pt = datetime.strptime(f"{year}-{raw_time}", "%Y-%m-%d %H:%M")
            return raw_time, pt
        except ValueError:
            pass

    # 尝试旧格式 "HH:MM"
    if " " not in raw_time and ":" in raw_time:
        try:
            # 用今天补全日期（可能不准确，但至少能显示）
            today = datetime.now(BJT).strftime("%m-%d")
            full_time = f"{today} {raw_time}"
            pt = datetime.strptime(f"{year}-{full_time}", "%Y-%m-%d %H:%M")
            return full_time, pt
        except ValueError:
            pass

    return None, None


def _aggregate_task_results_to_points(tasks: List[dict], year: int, cutoff_naive: datetime, now_naive: datetime) -> tuple:
    """
    将多个任务的结果聚合为时间序列数据点。
    每个任务产生一个数据点（使用任务的 updated_at 作为时间）。
    返回: (overall_points, models_points)
        overall_points: [{time, hit_rate}, ...]
        models_points: {model_name: [{time, hit_rate}, ...]}
    """
    overall_points = []
    models_points = {}

    for task in tasks:
        updated_str = task.get("updated_at", "")
        if not updated_str:
            continue
        try:
            updated_dt = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        # 检查是否在时间范围内
        if not (cutoff_naive <= updated_dt <= now_naive):
            continue

        # 格式化时间
        time_str = updated_dt.strftime("%m-%d %H:%M")

        # 获取整体命中率
        result = task.get("result", {})
        if not result:
            continue

        # 计算整体命中率（各模型加权平均）
        total_hit = sum(r.get("hit_count", 0) for r in result.values())
        total_queries = sum(r.get("total_queries", 0) for r in result.values())
        if total_queries > 0:
            hit_rate = round((total_hit / total_queries) * 100, 2)
            overall_points.append({"time": time_str, "hit_rate": hit_rate})

        # 各模型的命中率
        for model_name, stats in result.items():
            hit_count = stats.get("hit_count", 0)
            total_q = stats.get("total_queries", 0)
            if total_q > 0:
                m_hit_rate = round((hit_count / total_q) * 100, 2)
                if model_name not in models_points:
                    models_points[model_name] = []
                models_points[model_name].append({"time": time_str, "hit_rate": m_hit_rate})

    return overall_points, models_points


def _aggregate_task_trends_to_points(tasks: List[dict], year: int, cutoff_naive: datetime, now_naive: datetime) -> tuple:
    """
    将多个任务的 hit_rate_trend.json 合并为统一分钟级时间序列。
    每个任务产出分钟级数据点，多任务按 time 去重合并（后者覆盖前者）。

    当某个任务没有 trend 数据时，回退到 task result 的单点数据。

    返回: (overall_points, models_points)
        overall_points: [{time, hit_rate}, ...]  hit_rate 为百分比 (0~100)
        models_points: {model_name: {points: [...], stats: {...}}}
    """
    # 使用 dict 按 time 去重（同时间点后者覆盖前者）
    overall_map: Dict[str, float] = {}             # time_str → hit_rate (百分比)
    model_maps: Dict[str, Dict[str, float]] = {}   # model → {time_str → hit_rate}

    for task in tasks:
        task_id = task.get("task_id", "")
        trend = _read_trend_for_task(task_id)

        if trend and trend.get("series"):
            # 优先使用 trend 分钟级数据
            for s in trend["series"]:
                model_name = s.get("model", "")
                for p in s.get("data", []):
                    raw_time = p.get("time", "")
                    if not raw_time:
                        continue
                    time_str, pt = _parse_trend_time(raw_time, year)
                    if pt is None:
                        continue
                    if not (cutoff_naive <= pt <= now_naive):
                        continue
                    hit_rate = p.get("hit_rate")
                    if hit_rate is None:
                        continue
                    # trend 中的 hit_rate 是 0~1，转为百分比
                    hr_pct = round(hit_rate * 100, 2)

                    if model_name == "整体":
                        overall_map[time_str] = hr_pct
                    else:
                        model_maps.setdefault(model_name, {})[time_str] = hr_pct
        else:
            # fallback: 无 trend 数据时用 task result 的单点
            updated_str = task.get("updated_at", "")
            if not updated_str:
                continue
            try:
                updated_dt = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if not (cutoff_naive <= updated_dt <= now_naive):
                continue

            time_str = updated_dt.strftime("%m-%d %H:%M")
            result_data = task.get("result", {})
            if not result_data:
                continue

            # 整体（加权平均）
            total_hit = sum(r.get("hit_count", 0) for r in result_data.values())
            total_queries = sum(r.get("total_queries", 0) for r in result_data.values())
            if total_queries > 0:
                overall_map[time_str] = round((total_hit / total_queries) * 100, 2)

            # 各模型
            for model_name, stats in result_data.items():
                hit_count = stats.get("hit_count", 0)
                total_q = stats.get("total_queries", 0)
                if total_q > 0:
                    model_maps.setdefault(model_name, {})[time_str] = round((hit_count / total_q) * 100, 2)

    # 按时间排序构建 overall_points
    overall_points = [{"time": t, "hit_rate": overall_map[t]} for t in sorted(overall_map.keys())]

    # 按时间排序构建 models_points
    models_points = {}
    for model_name, time_map in model_maps.items():
        m_pts = [{"time": t, "hit_rate": v} for t, v in sorted(time_map.items())]
        if m_pts:
            models_points[model_name] = {
                "points": m_pts,
                "stats": _compute_stats(m_pts),
            }

    return overall_points, models_points


@router.get("/kv/dashboard", summary="获取命中率趋势数据（实时）")
async def kv_dashboard(time_range: str = Query(default="1d", description="时间范围: 1h, 6h, 1d, 7d, 30d")):
    """
    Dashboard API：8 个固定场景的命中率趋势数据。
    - 实时场景（全场景_各模型）：使用单个任务的分钟级 trend 数据
    - 非实时场景：聚合时间范围内多个任务的 hit_rate_trend.json 分钟级数据
    返回: {scenarios: {name: {task_id, points: [{time, hit_rate}], stats: {mean, max, min}, models}}}
    """
    if time_range not in _TIME_RANGE_MAP:
        raise HTTPException(status_code=400, detail=f"不支持的 time_range: {time_range}，可选: {list(_TIME_RANGE_MAP.keys())}")

    range_seconds = _TIME_RANGE_MAP[time_range]
    now_bjt = datetime.now(BJT)
    cutoff_bjt = now_bjt - timedelta(seconds=range_seconds)
    # 趋势数据时间格式 "MM-DD HH:MM"，解析时去掉时区避免 naive/aware 比较问题
    cutoff_naive = cutoff_bjt.replace(tzinfo=None)
    now_naive = now_bjt.replace(tzinfo=None)
    year = now_bjt.year

    result = {}
    for scenario in DASHBOARD_SCENARIOS:
        is_realtime_scenario = (scenario == "全场景_各模型")

        if is_realtime_scenario:
            # 实时场景：使用单个任务的分钟级 trend 数据
            task = _find_latest_task_for_scenario(scenario)
            if not task:
                result[scenario] = None
                continue

            task_id = task.get("task_id", "")
            if not task_id:
                result[scenario] = None
                continue

            trend = _read_trend_for_task(task_id)
            if not trend or not trend.get("series"):
                result[scenario] = {"task_id": task_id, "points": [], "stats": {"mean": 0, "max": 0, "min": 0}, "models": {}}
                continue

            # 解析 trend 数据
            overall_series = None
            model_series = {}
            for s in trend["series"]:
                if s.get("model") == "整体":
                    overall_series = s
                else:
                    model_series[s["model"]] = s

            points = []
            if overall_series:
                for p in overall_series.get("data", []):
                    raw_time = p.get("time", "")
                    if not raw_time:
                        continue
                    time_str, pt = _parse_trend_time(raw_time, year)
                    if pt is None:
                        continue
                    if pt >= cutoff_naive and pt <= now_naive:
                        hit_rate = p.get("hit_rate")
                        if hit_rate is not None:
                            points.append({"time": time_str, "hit_rate": round(hit_rate * 100, 2)})

            # 提取模型级数据
            models = {}
            for model_name, m_series in model_series.items():
                m_points = []
                for p in m_series.get("data", []):
                    raw_time = p.get("time", "")
                    if not raw_time:
                        continue
                    time_str, pt = _parse_trend_time(raw_time, year)
                    if pt is None:
                        continue
                    if pt >= cutoff_naive and pt <= now_naive:
                        hr = p.get("hit_rate")
                        if hr is not None:
                            m_points.append({"time": time_str, "hit_rate": round(hr * 100, 2)})
                if m_points:
                    m_rates = [p["hit_rate"] for p in m_points]
                    models[model_name] = {
                        "points": m_points,
                        "stats": {
                            "mean": round(sum(m_rates) / len(m_rates), 2) if m_rates else 0,
                            "max": max(m_rates) if m_rates else 0,
                            "min": min(m_rates) if m_rates else 0,
                        }
                    }

            rates = [p["hit_rate"] for p in points]
            stats = {
                "mean": round(sum(rates) / len(rates), 2) if rates else 0,
                "max": max(rates) if rates else 0,
                "min": min(rates) if rates else 0,
            }

            result[scenario] = {
                "task_id": task_id,
                "points": points,
                "stats": stats,
                "models": models,
            }

        else:
            # 非实时场景：聚合时间范围内的多个任务
            tasks = _find_tasks_for_scenario_in_range(scenario, cutoff_naive, now_naive)
            if not tasks:
                result[scenario] = None
                continue

            # 使用最新任务的 task_id
            latest_task = tasks[-1] if tasks else None
            task_id = latest_task.get("task_id", "") if latest_task else ""

            # 将任务 trend 数据聚合为分钟级时间序列
            overall_points, models_points = _aggregate_task_trends_to_points(tasks, year, cutoff_naive, now_naive)

            if not overall_points:
                result[scenario] = {"task_id": task_id, "points": [], "stats": {"mean": 0, "max": 0, "min": 0}, "models": {}}
                continue

            # 计算统计数据
            stats = _compute_stats(overall_points)

            # models_points 已包含 stats（由 _aggregate_task_trends_to_points 计算）
            models = models_points

            result[scenario] = {
                "task_id": task_id,
                "points": overall_points,
                "stats": stats,
                "models": models,
            }

    return {"scenarios": result, "time_range": time_range}


# ============================================================
# Realtime — 实时命中率趋势（独立于 Dashboard 的实时管道）
# ============================================================
_REALTIME_DIR = os.path.join(KV_RESULTS_DIR, "realtime")


def _load_realtime_config() -> dict:
    """加载实时 pipeline 独立配置"""
    try:
        with open(REALTIME_CONFIG_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _load_realtime_daily(scenario: str, date_str: str) -> dict:
    """读取 realtime/{scenario}/{date_str}.json 的 data dict"""
    daily_file = os.path.join(_REALTIME_DIR, scenario, f"{date_str}.json")
    if not os.path.exists(daily_file):
        return {}
    try:
        with open(daily_file, "r", encoding="utf-8") as f:
            return json.load(f).get("data", {})
    except Exception:
        return {}


def _compute_stats(data_points: list) -> dict:
    """计算 mean / max / min"""
    rates = [p["hit_rate"] for p in data_points if p.get("hit_rate") is not None]
    if not rates:
        return {"mean": 0, "max": 0, "min": 0}
    return {
        "mean": round(sum(rates) / len(rates), 2),
        "max": round(max(rates), 2),
        "min": round(min(rates), 2),
    }


@router.get("/kv/realtime", summary="获取全场景实时命中率趋势")
async def kv_realtime(
    time_range: str = Query(default="1h", description="时间范围: 1h, 6h, 1d, 7d, 30d"),
):
    """
    从 realtime/ 目录读取常驻 Worker 产出的分钟级趋势数据。
    后端计算 mean/max/min，前端直接展示。
    支持 1h / 6h / 1d / 7d / 30d 跨天查询。
    """
    if time_range not in _TIME_RANGE_MAP:
        raise HTTPException(status_code=400, detail=f"不支持的 time_range: {time_range}")

    cfg = _load_realtime_config()
    if not cfg.get("enabled", True):
        raise HTTPException(status_code=404, detail="实时分析未启用")

    scenario = cfg.get("scenario", "全场景_各模型")
    range_seconds = _TIME_RANGE_MAP[time_range]

    now_bjt = datetime.now(BJT)
    cutoff_bjt = now_bjt - timedelta(seconds=range_seconds)

    # 确定需要加载的天文件列表
    days_to_load = []
    current = cutoff_bjt
    while current.date() <= now_bjt.date():
        days_to_load.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    # 加载所有天文件的 data dict，key 带日期避免跨天覆盖: "YYYY-MM-DD HH:MM" -> {model: hit_rate}
    all_minutes: dict = {}
    for day_str in days_to_load:
        day_data = _load_realtime_daily(scenario, day_str)
        for minute_str, model_rates in day_data.items():
            if isinstance(model_rates, dict):
                # 用带日期的 key 存储，避免跨天冲突
                full_key = f"{day_str} {minute_str}"
                all_minutes[full_key] = model_rates

    # 过滤时间范围，key 转换为 "MM-DD HH:MM"
    filtered_minutes = {}
    for full_key, model_rates in all_minutes.items():
        try:
            dt = datetime.strptime(full_key, "%Y-%m-%d %H:%M")
            dt_bjt = dt.replace(tzinfo=BJT)
            if dt_bjt >= cutoff_bjt.replace(tzinfo=BJT) and dt_bjt <= now_bjt:
                display_time = f"{dt.strftime('%m-%d')} {dt.strftime('%H:%M')}"
                filtered_minutes[display_time] = model_rates
        except ValueError:
            continue

    # 按时间排序
    sorted_minutes = sorted(filtered_minutes.items(), key=lambda x: x[0])

    # 提取模型列表（从配置或数据中检测）
    configured_models = cfg.get("models", "")
    model_list = [m.strip() for m in configured_models.split(",") if m.strip()]
    if not model_list and sorted_minutes:
        model_list = [k for k in sorted_minutes[0][1].keys() if k != "整体"]

    # 构建整体 data points
    overall_points = []
    for time_label, model_rates in sorted_minutes:
        hr = model_rates.get("整体")
        if hr is not None:
            overall_points.append({
                "time": time_label,
                "hit_rate": round(hr * 100, 2) if hr <= 1 else round(hr, 2),
            })

    # 构建整体 stats
    overall_stats = _compute_stats(overall_points)

    # 构建模型级 data points 和 stats
    models = {}
    for model_name in model_list:
        m_points = []
        for time_label, model_rates in sorted_minutes:
            hr = model_rates.get(model_name)
            if hr is not None:
                m_points.append({
                    "time": time_label,
                    "hit_rate": round(hr * 100, 2) if hr <= 1 else round(hr, 2),
                })
        if m_points:
            models[model_name] = {
                "points": m_points,
                "stats": _compute_stats(m_points),
            }

    # 计算 data_status
    total_minutes = int(range_seconds / 60)
    filled_minutes = len(overall_points)
    coverage_pct = round(filled_minutes / total_minutes * 100, 1) if total_minutes > 0 else 0
    latest_minute = sorted_minutes[-1][0] if sorted_minutes else None

    return {
        "scenarios": {
            scenario: {
                "points": overall_points,
                "stats": overall_stats,
                "models": models,
                "data_status": {
                    "total_minutes": total_minutes,
                    "filled_minutes": filled_minutes,
                    "coverage_pct": coverage_pct,
                    "latest_minute": latest_minute,
                },
            }
        },
        "time_range": time_range,
    }


@router.get("/kv/realtime/status", summary="获取实时 Worker 存活状态")
async def kv_realtime_status():
    """
    检查 realtime Worker 是否存活。
    通过 worker_heartbeat 文件的最后修改时间判断（心跳间隔 30s）。
    同时返回队列积压和最新数据时间。
    """
    heartbeat_file = os.path.join(_REALTIME_DIR, "worker_heartbeat")
    alive = False
    last_heartbeat = None

    if os.path.exists(heartbeat_file):
        try:
            mtime = os.path.getmtime(heartbeat_file)
            last_heartbeat = datetime.fromtimestamp(mtime, BJT).strftime("%Y-%m-%d %H:%M:%S")
            # 心跳超过 120 秒视为失活
            alive = (time.time() - mtime) < 120
        except Exception:
            pass

    # 队列积压
    queue_pending = len(glob.glob(os.path.join(_REALTIME_DIR, "queue", "pending", "*.json")))
    queue_running = len(glob.glob(os.path.join(_REALTIME_DIR, "queue", "running", "*.json")))
    queue_failed = len(glob.glob(os.path.join(_REALTIME_DIR, "queue", "failed", "*.json")))

    # 最新数据时间
    cfg = _load_realtime_config()
    scenario = cfg.get("scenario", "全场景_各模型")
    latest_minute = None
    scenario_dir = os.path.join(_REALTIME_DIR, scenario)
    if os.path.isdir(scenario_dir):
        daily_files = sorted(
            [f for f in os.listdir(scenario_dir) if f.endswith(".json")],
            reverse=True,
        )
        for df in daily_files[:2]:  # 只检查最近 2 天
            try:
                with open(os.path.join(scenario_dir, df), "r", encoding="utf-8") as f:
                    data = json.load(f).get("data", {})
                if data:
                    minutes = sorted(data.keys(), reverse=True)
                    if minutes:
                        latest_minute = f"{df[:-5]} {minutes[0]}"
                        break
            except Exception:
                continue

    return {
        "alive": alive,
        "last_heartbeat": last_heartbeat,
        "queue": {
            "pending": queue_pending,
            "running": queue_running,
            "failed": queue_failed,
        },
        "latest_minute": latest_minute,
    }


@router.get("/kv/models", summary="获取可用模型列表（热加载）")
async def kv_models():
    """读取 olap_config.json 中的 models 列表，修改文件即时生效，无需重启服务"""
    models = _load_model_list()
    return StandardResponse(code=0, message="success", data=models, trace_id=None)


@router.get("/kv/qpd", summary="查询当前用户 QPD 配额")
async def kv_qpd(request: Request):
    """返回当前用户今日已用次数、配额上限、是否为 official"""
    user_info = getattr(request.state, "user", {}) or {}
    username = user_info.get("username", "unknown")
    official = _is_official(user_info)
    used = _count_user_today(username)
    qpd_limit = _load_olap_config()["olap_qpd_limit"]
    return StandardResponse(
        code=0, message="success",
        data={
            "username": username,
            "used": used,
            "limit": qpd_limit,
            "is_official": official,
            "remaining": max(0, qpd_limit - used) if not official else -1,
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
        default=None,
        description="场景过滤路径，非空添加 match_phrase 过滤，为空则使用配置默认值",
    ),
    scheduled_at: Optional[str] = Query(
        default=None,
        description="定时启动时间（北京时间），格式 YYYY-MM-DD HH:MM:SS，为空则立即执行",
    ),
    models: Optional[str] = Query(
        default=None,
        description="模型过滤，逗号分隔（如 glm-5,deepseek-v3.2），为空则分析所有检测到的模型",
    ),
    slice_minutes: Optional[int] = Query(
        default=None,
        description="子切片粒度（分钟），覆盖全局 pipeline_slice_minutes。60=不拆分，10=每10分钟一个子切片",
    )
):
    """
    提交全自动 pipeline 任务 (fetch → tokenize → simulate)。
    立即返回任务 ID，后台异步执行。
    """
    log_usage("es_fetch", scenario="OLAP")
    cfg = _load_olap_config()

    # path 默认值（热加载）
    if path is None:
        path = cfg["pipeline_default_path"]

    # 从中间件获取用户信息
    user_info = getattr(request.state, "user", {}) or {}
    username = user_info.get("username", "unknown")
    user_name = user_info.get("name", "未知用户")

    # QPD 限额检查（非 official 用户）
    qpd_limit = cfg["olap_qpd_limit"]
    if not _is_official(user_info):
        used = _count_user_today(username)
        if used >= qpd_limit:
            raise HTTPException(
                status_code=429,
                detail=f"今日配额已用完（{used}/{qpd_limit}），official 身份用户无此限制"
            )

    try:
        start_dt = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="时间格式错误，请使用 YYYY-MM-DD HH:MM:SS")
    if start_dt >= end_dt:
        raise HTTPException(status_code=400, detail="开始时间必须早于结束时间")

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

    # 解析模型过滤列表
    selected_models = [m.strip() for m in models.split(",") if m.strip()] if models else []

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
            "app_id": app_id,
            "models": selected_models
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
            "default_model": cfg["pipeline_default_model"],
            "block_size": cfg["pipeline_block_size"],
            "cache_size": cfg["pipeline_cache_size"],
            "tokenize_concurrency": cfg.get("pipeline_tokenize_concurrency", 4),
            "fetch_concurrency": cfg.get("pipeline_fetch_concurrency", 2),
            "fetch_window_concurrency": cfg.get("pipeline_fetch_window_concurrency", 24),
            "es_scroll_workers": cfg.get("pipeline_es_scroll_workers", 30),
            "tokenize_workers": cfg.get("pipeline_tokenize_workers", 7),
            "tokenize_batch_size": cfg.get("pipeline_tokenize_batch_size", 1000),
            "es_scroll_size": cfg.get("pipeline_es_scroll_size", 10000),
            "slice_minutes": slice_minutes if slice_minutes is not None else cfg.get("pipeline_slice_minutes", 60)
        },
        "result": None,
        "is_deleted": False,
        "scheduled_at": scheduled_at or None
    }
    _write_status(status_data)

    # ---- 启动 pipeline ----
    k8s_enabled = cfg.get("k8s_enabled", True)

    if k8s_enabled:
        # K8s Job 模式：pipeline 在独立 Pod 中运行
        try:
            job_name = k8s_client.create_pipeline_job(
                kubeconfig_path=KUBECONFIG_PATH,
                olap_config=cfg,
                task_id=task_id,
                username=username,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                app_id=app_id,
                path=path or "",
                models=models or "",
            )
            logger.info(f"[k8s] Pipeline Job created: {job_name} for task {task_id}")
            # 启动 K8s Job 生命周期监听，任务终态时自动通知
            asyncio.create_task(_watch_k8s_job(task_id))
        except Exception as e:
            logger.exception(f"[k8s] Failed to create Job for task {task_id}")
            await _set_failed(task_id, "fetch", f"K8s Job 创建失败: {str(e)[:200]}")
            raise HTTPException(status_code=500, detail=f"K8s Job 创建失败: {str(e)[:200]}")
    else:
        # 本地模式：asyncio 后台任务（回滚兼容）
        task = asyncio.create_task(
            _run_pipeline(task_id, start_datetime, end_datetime, app_id, path or "", scheduled_at or "")
        )
        _running_tasks[task_id] = task
        task.add_done_callback(lambda t: _running_tasks.pop(task_id, None))

    return StandardResponse(
        code=0,
        message="任务已提交",
        data={"task_id": task_id},
        trace_id=None
    )


@router.get("/kv/hit-rate-trend/{task_id}", summary="获取分钟级命中率趋势（预计算）")
async def kv_hit_rate_trend(task_id: str):
    """
    读取预计算的趋势数据文件 report/hit_rate_trend.json。
    趋势在 pipeline 的 trend 阶段自动生成；存量任务可用 compute_trend.py 回填。
    """
    status = _read_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    task_data_dir = _task_dir(task_id)
    trend_file = os.path.join(task_data_dir, "report", "hit_rate_trend.json")

    if not os.path.exists(trend_file):
        return StandardResponse(
            code=0, message="趋势数据尚未生成",
            data={"series": []}, trace_id=None
        )

    try:
        with open(trend_file, "r", encoding="utf-8") as f:
            trend_data = json.load(f)
    except Exception as e:
        return StandardResponse(
            code=-1, message=f"读取趋势数据失败: {e}",
            data={"series": []}, trace_id=None
        )

    return StandardResponse(code=0, message="success", data=trend_data, trace_id=None)


@router.get("/kv/status/{task_id}", summary="查询任务状态")
async def es_fetch_status(task_id: str):
    status = _read_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    # K8s 模式：交叉校验 Job 状态，处理 OOM / Pending 等 status.json 无法自行更新的场景
    cfg = _load_olap_config()
    cur_stage = status["pipeline"].get("current_stage", "")
    if cfg.get("k8s_enabled") and cur_stage not in ("done", "failed", "cancelled"):
        try:
            job_status = k8s_client.get_job_status(KUBECONFIG_PATH, cfg, task_id)
            if job_status:
                k8s_st = job_status["status"]
                reason = job_status.get("reason", "")
                msg = job_status.get("message", "")

                if k8s_st == "failed":
                    # Job 已失败（OOMKilled / Error / DeadlineExceeded）但 status.json 未更新
                    fail_msg = f"K8s Job 异常终止: {reason}" if reason else "K8s Job 异常终止"
                    if msg:
                        fail_msg += f" ({msg})"
                    stage = status["pipeline"].get("current_stage", "fetch")
                    status["pipeline"]["current_stage"] = "failed"
                    status["pipeline"].setdefault("stages", {}).setdefault(stage, {})
                    status["pipeline"]["stages"][stage]["status"] = "failed"
                    status["pipeline"]["stages"][stage]["message"] = fail_msg
                    status["pipeline"]["stages"][stage]["completed_at"] = _now_bjt()
                    _write_status(status)
                    logger.warning(f"[k8s-sync] Task {task_id} marked failed: {fail_msg}")

                elif k8s_st == "pending":
                    # Pod 排队中，注入排队信息供前端展示
                    status["pipeline"]["k8s_pending"] = True
                    status["pipeline"]["k8s_pending_reason"] = msg or "等待集群资源分配"

                else:
                    status["pipeline"].pop("k8s_pending", None)
                    status["pipeline"].pop("k8s_pending_reason", None)
        except Exception as e:
            logger.debug(f"[k8s-sync] Failed to check Job status for {task_id}: {e}")

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

    cfg = _load_olap_config()
    k8s_enabled = cfg.get("k8s_enabled", True)

    # K8s 模式：删除 Job（级联删除 Pod，pipeline 进程被 kill）
    if k8s_enabled:
        try:
            k8s_client.delete_pipeline_job(KUBECONFIG_PATH, cfg, task_id)
        except Exception as e:
            logger.warning(f"[k8s] Failed to delete Job for {task_id}: {e}")

    # 运行中的任务 → 取消 asyncio.Task（本地模式）
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
