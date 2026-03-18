import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from context.auth_client import users_amount
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList


# 日志文件路径配置
LOG_FILE_PATH = os.environ.get(
    "USAGE_LOG_PATH",
    "/mnt/cfs_bj_mt/workspace/chenjieting/iCodes/baidu/personal-code/data_management_app/logs/app.log"
)


logger = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> DatasetsService:
    """基于鉴权中间件注入的 token 构造 service."""
    token = getattr(request.state, "token", "") or ""
    return DatasetsService(auth_token=token)


@router.get("/datasets/metrics", summary="查询数据集指标")
async def datasets_metrics(
    request: Request,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:

    body = {}
    body["groups"] = getattr(request.state, "groups", []) or []
    datasets_lst = await service.list_datasets(DatasetList(**body))
    datasets_amount = len(datasets_lst)

    sub_types = []
    for ds in datasets_lst:
        # 获取 ds 对象中 labels 字段（根据你的数据结构，可能是字典或 Pydantic 模型）
        labels = ds.get("labels", []) if isinstance(ds, dict) else ds.labels
        
        for label in labels:
            # 兼容模型对象或字典格式
            l_name = label.get("label_name") if isinstance(label, dict) else label.label_name
            l_values = label.get("label_values") if isinstance(label, dict) else label.label_values
            
            if l_name == "数据细分类型":
                sub_types.extend(l_values)

    # 3. 使用 Counter 统计分布
    # 结果格式：{"OCR": 10, "VQA": 5, ...}
    dist_dict = dict(Counter(sub_types))

    datasets_distribution = sorted(
        [{"name": k, "value": v} for k, v in dist_dict.items()],
        key=lambda x: x['value'],
        reverse=True
    )

    data = {
        "datasets_amount": datasets_amount,
        "datasets_distribution": datasets_distribution
    }

    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/users/metrics", summary="查询用户指标")
async def users_metrics(
    request: Request,
) -> StandardResponse[dict]:
    data = {
        "users_amount": await users_amount()
    }
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# 需要忽略的无效值
INVALID_VALUES = {"", "-", "unknown", "Unknown", "UNKNOWN", None}


def _is_valid_value(value: Optional[str]) -> bool:
    """检查值是否有效（非空、非 '-'、非 'unknown'）"""
    if value is None:
        return False
    return value.strip() not in INVALID_VALUES


def _parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """
    解析单行日志，返回结构化数据
    日志格式: {time_str} | {user_info} | {scenario} | {message}

    注意：当 user_info 或 scenario 为空、'-' 或 'unknown' 时，返回 None（不纳入统计）
    """
    try:
        parts = line.strip().split(" | ", 3)
        if len(parts) < 4:
            return None

        time_str, user_info, scenario, message = parts

        # 检查 user_info 和 scenario 是否有效，无效则跳过此条记录
        if not _is_valid_value(user_info) or not _is_valid_value(scenario):
            return None

        # 解析时间
        log_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

        return {
            "timestamp": log_time,
            "user": user_info.strip(),
            "scenario": scenario.strip(),
            "action": message
        }
    except (ValueError, IndexError):
        return None


def _read_log_entries(log_path: str, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """读取并解析日志文件"""
    entries = []

    if not os.path.exists(log_path):
        logger.warning(f"Log file not found: {log_path}")
        return entries

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                parsed = _parse_log_line(line)
                if parsed:
                    if start_date is None or parsed["timestamp"] >= start_date:
                        entries.append(parsed)
    except Exception as e:
        logger.error(f"Error reading log file: {e}")

    return entries


def _aggregate_by_period(
    entries: List[Dict[str, Any]],
    period: Literal["day", "week", "month"]
) -> Dict[str, Any]:
    """按时间周期聚合数据"""

    if not entries:
        return {
            "period": period,
            "total_requests": 0,
            "active_users": [],
            "scenario_distribution": [],
            "action_distribution": [],
            "timeline": []
        }

    # 按日期分组
    date_groups = defaultdict(list)
    for entry in entries:
        if period == "day":
            key = entry["timestamp"].strftime("%Y-%m-%d")
        elif period == "week":
            # 按周一为起始
            week_start = entry["timestamp"] - timedelta(days=entry["timestamp"].weekday())
            key = week_start.strftime("%Y-%m-%d")
        else:  # month
            key = entry["timestamp"].strftime("%Y-%m")
        date_groups[key].append(entry)

    # 统计活跃用户
    users = [e["user"] for e in entries if e["user"]]
    user_counts = Counter(users)

    # 统计场景分布
    scenarios = [e["scenario"] for e in entries if e["scenario"]]
    scenario_counts = Counter(scenarios)

    # 统计操作分布
    actions = [e["action"] for e in entries if e["action"]]
    action_counts = Counter(actions)

    # 时间线数据
    timeline = []
    for date_key in sorted(date_groups.keys()):
        group_entries = date_groups[date_key]
        group_users = set(e["user"] for e in group_entries if e["user"])
        timeline.append({
            "date": date_key,
            "requests": len(group_entries),
            "unique_users": len(group_users)
        })

    return {
        "period": period,
        "total_requests": len(entries),
        "active_users": [
            {"name": name, "count": count}
            for name, count in user_counts.most_common(20)
        ],
        "scenario_distribution": [
            {"name": name, "value": count}
            for name, count in scenario_counts.most_common()
        ],
        "action_distribution": [
            {"name": name, "value": count}
            for name, count in action_counts.most_common(20)
        ],
        "timeline": timeline
    }


@router.get("/usage/metrics", summary="查询平台使用情况统计")
async def usage_metrics(
    period: Literal["day", "week", "month"] = Query(
        default="day",
        description="统计周期: day(最近1天), week(最近7天), month(最近30天)"
    ),
) -> StandardResponse[dict]:
    """
    获取平台使用情况统计数据

    - **period**: 统计周期
      - day: 最近1天的数据
      - week: 最近7天的数据
      - month: 最近30天的数据

    返回数据包括:
    - total_requests: 总请求数
    - active_users: 活跃用户排行
    - scenario_distribution: 场景分布 (UI/API)
    - action_distribution: 操作分布
    - timeline: 时间线趋势数据
    """
    # 计算起始时间
    now = datetime.now()
    if period == "day":
        start_date = now - timedelta(days=1)
    elif period == "week":
        start_date = now - timedelta(days=7)
    else:  # month
        start_date = now - timedelta(days=30)

    # 读取并解析日志
    entries = _read_log_entries(LOG_FILE_PATH, start_date)

    # 聚合数据
    data = _aggregate_by_period(entries, period)
    data["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S")
    data["end_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/usage/users", summary="查询用户活跃度详情")
async def usage_users(
    period: Literal["day", "week", "month"] = Query(
        default="week",
        description="统计周期"
    ),
    limit: int = Query(default=50, ge=1, le=200, description="返回用户数量上限"),
) -> StandardResponse[dict]:
    """
    获取用户活跃度详情

    返回每个用户的请求次数、最常用功能等信息
    """
    now = datetime.now()
    if period == "day":
        start_date = now - timedelta(days=1)
    elif period == "week":
        start_date = now - timedelta(days=7)
    else:
        start_date = now - timedelta(days=30)

    entries = _read_log_entries(LOG_FILE_PATH, start_date)

    # 按用户分组统计
    user_stats = defaultdict(lambda: {
        "total_requests": 0,
        "scenarios": Counter(),
        "actions": Counter(),
        "first_seen": None,
        "last_seen": None
    })

    for entry in entries:
        user = entry.get("user")
        if not user:
            continue

        stats = user_stats[user]
        stats["total_requests"] += 1

        if entry.get("scenario"):
            stats["scenarios"][entry["scenario"]] += 1
        if entry.get("action"):
            stats["actions"][entry["action"]] += 1

        ts = entry["timestamp"]
        if stats["first_seen"] is None or ts < stats["first_seen"]:
            stats["first_seen"] = ts
        if stats["last_seen"] is None or ts > stats["last_seen"]:
            stats["last_seen"] = ts

    # 转换为列表格式
    users_list = []
    for user, stats in sorted(user_stats.items(), key=lambda x: x[1]["total_requests"], reverse=True)[:limit]:
        users_list.append({
            "name": user,
            "total_requests": stats["total_requests"],
            "top_scenarios": [
                {"name": k, "count": v}
                for k, v in stats["scenarios"].most_common(5)
            ],
            "top_actions": [
                {"name": k, "count": v}
                for k, v in stats["actions"].most_common(5)
            ],
            "first_seen": stats["first_seen"].strftime("%Y-%m-%d %H:%M:%S") if stats["first_seen"] else None,
            "last_seen": stats["last_seen"].strftime("%Y-%m-%d %H:%M:%S") if stats["last_seen"] else None,
        })

    data = {
        "period": period,
        "total_users": len(users_list),
        "users": users_list
    }

    return StandardResponse(code=0, message="success", data=data, trace_id=None)

