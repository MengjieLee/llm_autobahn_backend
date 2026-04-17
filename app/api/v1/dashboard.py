import logging
import os
import glob
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

_BJT = timezone(timedelta(hours=8))

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from app.conf.config import settings
from context.auth_client import users_amount
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList


# Legacy 日志路径（老系统 Streamlit 写入）
LEGACY_LOG_FILE = os.environ.get(
    "USAGE_LOG_PATH",
    "/mnt/cfs_bj_mt/workspace/chenjieting/iCodes/baidu/personal-code/data_management_app/logs/app.log"
)

# 新系统使用统计日志目录（由 logging_config 中 usage logger 写入）
USAGE_LOG_DIR = settings.usage_log_dir
USAGE_LOG_NAME = settings.usage_log_file_name


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


def _read_log_entries(
    log_path: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """读取并解析单个日志文件"""
    entries = []

    if not os.path.exists(log_path):
        logger.warning(f"Log file not found: {log_path}")
        return entries

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                parsed = _parse_log_line(line)
                if parsed:
                    if (start_date is None or parsed["timestamp"] >= start_date) and \
                       (end_date is None or parsed["timestamp"] <= end_date):
                        entries.append(parsed)
    except Exception as e:
        logger.error(f"Error reading log file: {e}")

    return entries


def _read_all_usage_logs(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """
    合并读取 legacy + 新系统的使用统计日志。
    - Legacy: 单个文件 LEGACY_LOG_FILE
    - 新系统: USAGE_LOG_DIR/usage.log + usage.log.1, usage.log.2 ... (RotatingFileHandler 滚动)
    """
    all_entries = []

    # 1. Legacy 日志
    all_entries.extend(_read_log_entries(LEGACY_LOG_FILE, start_date, end_date))

    # 2. 新系统 usage 日志（主文件 + 滚动备份文件）
    usage_main = os.path.join(USAGE_LOG_DIR, USAGE_LOG_NAME)
    usage_files = [usage_main]
    # RotatingFileHandler 生成 usage.log.1, usage.log.2, ...
    usage_files.extend(sorted(glob.glob(f"{usage_main}.*")))

    for uf in usage_files:
        if uf == LEGACY_LOG_FILE:
            continue  # 避免重复读取
        all_entries.extend(_read_log_entries(uf, start_date, end_date))

    # 按时间排序
    all_entries.sort(key=lambda e: e["timestamp"])
    return all_entries


def _generate_date_keys(
    granularity: str,
    query_start: Optional[datetime],
    query_end: Optional[datetime]
) -> List[str]:
    """根据聚合粒度和日期范围，生成完整的日期 key 列表（用于补零）"""
    if not query_start or not query_end:
        return []

    keys = []
    if granularity == "day":
        current = query_start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = query_end.replace(hour=0, minute=0, second=0, microsecond=0)
        while current <= end_day:
            keys.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif granularity == "week":
        # 从 query_start 所在周的周一开始
        current = query_start - timedelta(days=query_start.weekday())
        current = current.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = query_end.replace(hour=0, minute=0, second=0, microsecond=0)
        while current <= end_day:
            keys.append(current.strftime("%Y-%m-%d"))
            current += timedelta(weeks=1)
    else:  # month
        current_year = query_start.year
        current_month = query_start.month
        end_year = query_end.year
        end_month = query_end.month
        while (current_year, current_month) <= (end_year, end_month):
            keys.append(f"{current_year:04d}-{current_month:02d}")
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
    return keys


def _aggregate_by_period(
    entries: List[Dict[str, Any]],
    period: str,
    query_start: Optional[datetime] = None,
    query_end: Optional[datetime] = None
) -> Dict[str, Any]:
    """按时间周期聚合数据，聚合粒度根据日期跨度自动决定，缺失日期补零"""

    # 对于 "all" period，从实际数据推导日期范围
    if entries:
        if not query_start:
            query_start = min(e["timestamp"] for e in entries)
        if not query_end:
            query_end = max(e["timestamp"] for e in entries)

    # 根据日期跨度自动决定聚合粒度
    if query_start and query_end:
        span_days = (query_end - query_start).days
    else:
        span_days = 0  # 无数据时默认按天

    if span_days <= 31:
        granularity = "day"
    elif span_days <= 365:
        granularity = "week"
    else:
        granularity = "month"

    if not entries:
        # 即使没有数据也要生成完整的时间轴（全部补零）
        all_date_keys = _generate_date_keys(granularity, query_start, query_end)
        timeline = [{"date": k, "requests": 0, "unique_users": 0} for k in all_date_keys]
        return {
            "period": period,
            "total_requests": 0,
            "active_users": [],
            "scenario_distribution": [],
            "action_distribution": [],
            "timeline": timeline
        }

    # 按日期分组
    date_groups = defaultdict(list)
    for entry in entries:
        if granularity == "day":
            key = entry["timestamp"].strftime("%Y-%m-%d")
        elif granularity == "week":
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

    # 生成完整时间轴并补零
    all_date_keys = _generate_date_keys(granularity, query_start, query_end)
    timeline = []
    for date_key in all_date_keys:
        if date_key in date_groups:
            group_entries = date_groups[date_key]
            group_users = set(e["user"] for e in group_entries if e["user"])
            timeline.append({
                "date": date_key,
                "requests": len(group_entries),
                "unique_users": len(group_users)
            })
        else:
            timeline.append({
                "date": date_key,
                "requests": 0,
                "unique_users": 0
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
    period: Literal["week", "month", "year", "all"] = Query(
        default="week",
        description="统计周期: week(近7天), month(近1个月), year(近1年), all(所有)"
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="自定义起始日期，格式 YYYY-MM-DD，优先级高于 period"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="自定义结束日期，格式 YYYY-MM-DD，优先级高于 period"
    ),
) -> StandardResponse[dict]:
    """
    获取平台使用情况统计数据

    - **period**: 统计周期
      - week: 近7天的数据
      - month: 近1个月的数据
      - year: 近1年的数据
      - all: 所有数据
    - **start_date**: 自定义起始日期 (YYYY-MM-DD)，与 end_date 配合使用，优先级高于 period
    - **end_date**: 自定义结束日期 (YYYY-MM-DD)，与 start_date 配合使用，优先级高于 period

    返回数据包括:
    - total_requests: 总请求数
    - active_users: 活跃用户排行
    - scenario_distribution: 场景分布 (UI/API)
    - action_distribution: 操作分布
    - timeline: 时间线趋势数据
    """
    # 计算起始时间
    now = datetime.now(_BJT)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    if start_date and end_date:
        try:
            query_start = datetime.strptime(start_date, "%Y-%m-%d")
            query_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")
    else:
        query_end = today_end  # 统一使用当天结束时间，确保今天的数据完整
        if period == "week":
            query_start = today_start - timedelta(days=6)   # 今天 + 前6天 = 近7天
        elif period == "month":
            query_start = today_start - timedelta(days=29)  # 今天 + 前29天 = 近30天
        elif period == "year":
            query_start = today_start - timedelta(days=364) # 今天 + 前364天 = 近1年
        else:  # all
            query_start = None

    # 读取并解析日志（合并 legacy + 新系统）
    entries = _read_all_usage_logs(query_start, query_end)

    # 聚合数据
    data = _aggregate_by_period(entries, period, query_start, query_end)
    data["start_date"] = query_start.strftime("%Y-%m-%d %H:%M:%S") if query_start else ""
    data["end_date"] = query_end.strftime("%Y-%m-%d %H:%M:%S") if query_end else now.strftime("%Y-%m-%d %H:%M:%S")

    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/usage/users", summary="查询用户活跃度详情")
async def usage_users(
    period: Literal["week", "month", "year", "all"] = Query(
        default="week",
        description="统计周期: week(近7天), month(近1个月), year(近1年), all(所有)"
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="自定义起始日期，格式 YYYY-MM-DD，优先级高于 period"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="自定义结束日期，格式 YYYY-MM-DD，优先级高于 period"
    ),
    limit: int = Query(default=50, ge=1, le=200, description="返回用户数量上限"),
) -> StandardResponse[dict]:
    """
    获取用户活跃度详情

    返回每个用户的请求次数、最常用功能等信息
    """
    now = datetime.now(_BJT)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    if start_date and end_date:
        try:
            query_start = datetime.strptime(start_date, "%Y-%m-%d")
            query_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")
    else:
        query_end = today_end  # 统一使用当天结束时间
        if period == "week":
            query_start = today_start - timedelta(days=6)
        elif period == "month":
            query_start = today_start - timedelta(days=29)
        elif period == "year":
            query_start = today_start - timedelta(days=364)
        else:  # all
            query_start = None

    entries = _read_all_usage_logs(query_start, query_end)

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

