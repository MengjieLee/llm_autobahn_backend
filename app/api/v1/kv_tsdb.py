"""KV-Cache TSDB 试验路由。

独立于现有 /olap/kv/ 路由，提供基于 TSDB 存储的分钟级命中率读写 API。
"""

import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.conf.config import settings
from app.core.api_schema import StandardResponse

logger = logging.getLogger(__name__)
router = APIRouter()


def _ensure_tsdb_enabled():
    if not settings.TSDB_ENABLED:
        raise HTTPException(status_code=503, detail="TSDB 未启用，请设置 TSDB_ENABLED=true")


# ---- 健康检查 ----

@router.get("/health", summary="TSDB 连接健康检查")
async def tsdb_health():
    _ensure_tsdb_enabled()
    from context.tsdb_connector import get_tsdb_connector
    connector = get_tsdb_connector()
    ok = await connector.health_check()
    if ok:
        return StandardResponse(code=0, message="TSDB 连接正常", data={"status": "ok"}, trace_id=None)
    return StandardResponse(code=-1, message="TSDB 连接失败", data={"status": "error"}, trace_id=None)


# ---- 查询: 分钟级命中率趋势 ----

@router.get("/hit-rate-trend/{task_id}", summary="从 TSDB 查询任务分钟级命中率趋势")
async def tsdb_hit_rate_trend(
    task_id: str,
    start_time: Optional[str] = Query(None, description="起始时间，支持相对时间如 '30 days ago' 或 epoch 毫秒"),
    end_time: Optional[str] = Query(None, description="结束时间，支持相对时间或 epoch 毫秒"),
    app_id: Optional[str] = Query(None, description="按 app_id 过滤"),
):
    """从 TSDB 查询某任务的分钟级命中率趋势。返回格式与 /kv/hit-rate-trend/{task_id} 一致。"""
    _ensure_tsdb_enabled()
    from src.domains.kv.tsdb_service import query_hit_rate_trend, _get_task_app_id

    # 将纯数字字符串转为 int（epoch ms）
    st = int(start_time) if start_time and start_time.isdigit() else start_time
    et = int(end_time) if end_time and end_time.isdigit() else end_time

    trend_data = await query_hit_rate_trend(task_id, st, et, app_id=app_id)

    # 补充 app_id 到响应
    resolved_app_id = app_id or _get_task_app_id(task_id)
    trend_data["app_id"] = resolved_app_id
    trend_data["task_id"] = task_id

    if not trend_data.get("series"):
        return StandardResponse(
            code=0, message="TSDB 中无该任务的趋势数据",
            data=trend_data, trace_id=None
        )
    return StandardResponse(code=0, message="success", data=trend_data, trace_id=None)


# ---- 查询: 按 app_id 聚合 ----

@router.get("/hit-rate-by-app/{app_id}", summary="按 app_id 查询所有任务的命中率趋势")
async def tsdb_hit_rate_by_app(
    app_id: str,
    start_time: Optional[str] = Query(None, description="起始时间"),
    end_time: Optional[str] = Query(None, description="结束时间"),
):
    """按 app_id 聚合查询，返回该 app 下所有任务的分钟级命中率。"""
    _ensure_tsdb_enabled()
    from src.domains.kv.tsdb_service import query_hit_rate_trend

    st = int(start_time) if start_time and start_time.isdigit() else start_time
    et = int(end_time) if end_time and end_time.isdigit() else end_time

    trend_data = await query_hit_rate_trend(task_id=None, start_time=st, end_time=et, app_id=app_id)
    if not trend_data.get("series"):
        return StandardResponse(
            code=0, message=f"TSDB 中无 app_id={app_id} 的趋势数据",
            data={"series": []}, trace_id=None
        )
    return StandardResponse(code=0, message="success", data=trend_data, trace_id=None)


# ---- 写入: 手动导入单个任务 ----

@router.post("/ingest/{task_id}", summary="将任务的文件趋势数据导入 TSDB")
async def tsdb_ingest_task(task_id: str):
    """读取 report/hit_rate_trend.json 并写入 TSDB（用于回填或手动导入）。"""
    _ensure_tsdb_enabled()
    from src.domains.kv.tsdb_service import write_trend_to_tsdb

    # 定位文件
    username = task_id.split("-kv_", 1)[0] if "-kv_" in task_id else "unknown"
    base_dir = os.path.join(settings.OLAP_BASE_DIR, settings.OLAP_DATABASE_DIR)
    task_data_dir = os.path.join(base_dir, "data", username, task_id)
    trend_file = os.path.join(task_data_dir, "report", "hit_rate_trend.json")

    if not os.path.exists(trend_file):
        raise HTTPException(status_code=404, detail=f"趋势文件不存在: {trend_file}")

    with open(trend_file, "r", encoding="utf-8") as f:
        trend_data = json.load(f)

    count = await write_trend_to_tsdb(task_id, trend_data)
    return StandardResponse(
        code=0,
        message=f"成功导入 {count} 个数据点到 TSDB",
        data={"task_id": task_id, "datapoints_written": count},
        trace_id=None,
    )


# ---- 批量回填 ----

@router.post("/backfill", summary="批量回填历史任务数据到 TSDB")
async def tsdb_backfill(
    username: Optional[str] = Query(None, description="指定用户名，为空则回填全部"),
    limit: int = Query(50, description="最多回填任务数"),
):
    """扫描已有任务的 hit_rate_trend.json 并批量写入 TSDB。"""
    _ensure_tsdb_enabled()
    from src.domains.kv.tsdb_service import write_trend_to_tsdb

    base_dir = os.path.join(settings.OLAP_BASE_DIR, settings.OLAP_DATABASE_DIR)
    data_dir = os.path.join(base_dir, "data")

    if not os.path.isdir(data_dir):
        raise HTTPException(status_code=404, detail=f"数据目录不存在: {data_dir}")

    results = []
    count = 0

    # 遍历用户目录
    user_dirs = [username] if username else sorted(os.listdir(data_dir))
    for user in user_dirs:
        user_path = os.path.join(data_dir, user)
        if not os.path.isdir(user_path):
            continue
        for task_id in sorted(os.listdir(user_path)):
            if count >= limit:
                break
            trend_file = os.path.join(user_path, task_id, "report", "hit_rate_trend.json")
            if not os.path.exists(trend_file):
                continue
            try:
                with open(trend_file, "r", encoding="utf-8") as f:
                    trend_data = json.load(f)
                n = await write_trend_to_tsdb(task_id, trend_data)
                results.append({"task_id": task_id, "datapoints": n, "status": "ok"})
                count += 1
            except Exception as e:
                results.append({"task_id": task_id, "datapoints": 0, "status": f"error: {e}"})
                count += 1

    total_points = sum(r["datapoints"] for r in results)
    return StandardResponse(
        code=0,
        message=f"回填完成: {len(results)} 任务, {total_points} 数据点",
        data={"tasks": results, "total_datapoints": total_points},
        trace_id=None,
    )
