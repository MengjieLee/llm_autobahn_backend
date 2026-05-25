"""TSDB 读写业务逻辑 — kv-cache 分钟级命中率数据。

负责将 Pipeline 产出的 hit_rate_trend 数据写入 TSDB，以及从 TSDB 查询趋势。
"""

import logging
from datetime import datetime, timezone, timedelta

from app.conf.config import settings

logger = logging.getLogger(__name__)

METRIC_NAME = "kv_cache_hit_rate"
BJT = timezone(timedelta(hours=8))


def _parse_time_label_to_epoch_ms(time_label: str, year: int | None = None) -> int | None:
    """将 hit_rate_trend 中的 time 字段 ("MM-DD HH:mm") 转为毫秒时间戳。

    因为原始数据不含年份，默认使用当前年份。
    """
    if not time_label:
        return None
    try:
        if year is None:
            year = datetime.now(BJT).year
        # 格式: "04-11 02:03"
        dt = datetime.strptime(f"{year}-{time_label}", "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=BJT)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def trend_data_to_datapoints(
    task_id: str,
    trend_data: dict,
    year: int | None = None,
    app_id: str = "",
) -> list[dict]:
    """将 hit_rate_trend.json 格式的数据转为 TSDB datapoints。

    trend_data 格式:
    {
        "series": [
            {"model": "整体", "data": [{"time": "04-11 02:03", "hit_rate": 0.59}, ...], "stats": {...}},
            {"model": "glm-5", "data": [{"time": "04-11 02:03", "hit_rate": 0.62}, ...], "stats": {...}},
        ]
    }
    """
    username = task_id.split("-kv_", 1)[0] if "-kv_" in task_id else "unknown"
    datapoints = []

    for series_item in trend_data.get("series", []):
        model = series_item.get("model", "unknown")
        for point in series_item.get("data", []):
            hit_rate = point.get("hit_rate")
            if hit_rate is None:
                continue
            ts = _parse_time_label_to_epoch_ms(point.get("time", ""), year)
            if ts is None:
                continue
            tags = {
                "task_id": task_id,
                "username": username,
                "model": model,
            }
            if app_id:
                tags["app_id"] = app_id
            datapoints.append({
                "metric": METRIC_NAME,
                "tags": tags,
                "timestamp": ts,
                "value": float(hit_rate),
            })

    return datapoints


async def write_trend_to_tsdb(task_id: str, trend_data: dict, year: int | None = None, app_id: str = "") -> int:
    """将一个任务的 trend 数据写入 TSDB。返回写入的数据点数量。"""
    if not settings.TSDB_ENABLED:
        return 0

    from context.tsdb_connector import get_tsdb_connector

    # 如果调用方没传 app_id，尝试从 status 文件读取（API 侧 ingest 场景）
    if not app_id:
        app_id = _get_task_app_id(task_id)

    datapoints = trend_data_to_datapoints(task_id, trend_data, year, app_id=app_id)
    if not datapoints:
        logger.info("[tsdb] No datapoints to write for task %s", task_id)
        return 0

    connector = get_tsdb_connector()
    await connector.write_datapoints(datapoints)
    logger.info("[tsdb] Wrote %d datapoints for task %s (app_id=%s)", len(datapoints), task_id, app_id)
    return len(datapoints)


def _get_task_app_id(task_id: str) -> str:
    """从 task status 文件中读取 app_id。

    兼容两种运行环境:
    - API 服务: 使用 settings.OLAP_BASE_DIR
    - K8s Job: 使用脚本推导的 BASE_DIR (scripts/../)
    """
    import os, json
    username = task_id.split("-kv_", 1)[0] if "-kv_" in task_id else "unknown"

    # 尝试多个可能的路径
    candidates = []
    # 1. settings 配置的路径（API 服务）
    candidates.append(os.path.join(settings.OLAP_BASE_DIR, settings.OLAP_DATABASE_DIR, "status", username, f"{task_id}.json"))
    # 2. 从当前文件位置推导（兼容 K8s Job）
    # tsdb_service.py 在 src/domains/kv/ 下，往上 4 层到项目根目录
    _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    candidates.append(os.path.join(_project_root, "olap_database", "status", username, f"{task_id}.json"))

    for status_file in candidates:
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                status = json.load(f)
            return status.get("query", {}).get("app_id", "")
        except (FileNotFoundError, OSError):
            continue
        except Exception:
            continue
    return ""


async def query_hit_rate_trend(
    task_id: str | None = None,
    start_time: str | int | None = None,
    end_time: str | int | None = None,
    app_id: str | None = None,
) -> dict:
    """从 TSDB 查询分钟级命中率趋势，返回与文件版相同的格式。

    支持按 task_id 或 app_id 过滤（至少提供一个）。
    start_time/end_time 支持:
      - 相对时间: "30 days ago", "1 hour ago"
      - epoch 毫秒: 1778083200000

    返回: {"series": [{"model": "整体", "data": [...], "stats": {...}}, ...]}
    """
    from context.tsdb_connector import get_tsdb_connector

    connector = get_tsdb_connector()

    # 默认查最近 90 天
    if not start_time:
        start_time = "90 days ago"

    # 构建 tag 过滤
    tags = {}
    if task_id:
        tags["task_id"] = [task_id]
    if app_id:
        tags["app_id"] = [app_id]
    if not tags:
        return {"series": []}

    filters = {
        "start": start_time,
        "tags": tags,
    }
    if end_time:
        filters["end"] = end_time

    # 按 model tag 分组，避免数据混在一起
    query = {
        "metric": METRIC_NAME,
        "filters": filters,
        "groupBy": [{"name": "Tag", "tags": ["model"]}],
        "limit": 10000,
    }

    result = await connector.get_datapoints([query])

    # 解析 TSDB 返回
    model_data: dict[str, list[dict]] = {}
    _parse_tsdb_result(result, model_data)

    # 构建与文件版相同的响应格式
    series = []
    for model, data in sorted(model_data.items(), key=lambda x: (x[0] != "整体", x[0])):
        data.sort(key=lambda d: d["time"])
        rates = [d["hit_rate"] for d in data if d["hit_rate"] is not None]
        stats = {
            "mean": round(sum(rates) / len(rates), 4) if rates else 0,
            "max": round(max(rates), 4) if rates else 0,
            "min": round(min(rates), 4) if rates else 0,
        }
        series.append({"model": model, "data": data, "stats": stats})

    return {"series": series}


def _parse_tsdb_result(result, model_data: dict[str, list[dict]]):
    """解析 TSDB get_datapoints 返回的 BceResponse。

    响应结构:
    results: [{
        metric: "kv_cache_hit_rate",
        groups: [{
            groupInfos: [{"model": "glm-5"}],  # groupBy 的 tag 值
            values: [[timestamp_ms, value], ...]
        }, ...],
    }]
    """
    if not result:
        return

    results = getattr(result, "results", None)
    if results is None and isinstance(result, dict):
        results = result.get("results", [])
    if not results:
        return

    for metric_result in results:
        groups = getattr(metric_result, "groups", None)
        if groups is None and isinstance(metric_result, dict):
            groups = metric_result.get("groups", [])
        if not groups:
            continue

        for group in groups:
            # 提取 groupBy 标签信息
            group_infos = getattr(group, "group_infos", None)
            if group_infos is None:
                group_infos = getattr(group, "groupInfos", None)
            if group_infos is None and isinstance(group, dict):
                group_infos = group.get("groupInfos", group.get("group_infos", []))

            # 从 groupInfos 中取 model 名
            # 实际结构: [{name: 'Tag', tags: {model: 'glm-5.1'}}]
            # 注意: tags 可能是 Expando 对象而非 dict
            model = "unknown"
            if isinstance(group_infos, list):
                for info in group_infos:
                    tags_in_info = None
                    if hasattr(info, "tags"):
                        tags_in_info = info.tags
                    elif isinstance(info, dict):
                        tags_in_info = info.get("tags", {})
                    if tags_in_info is not None:
                        if hasattr(tags_in_info, "model"):
                            model = tags_in_info.model
                            break
                        elif isinstance(tags_in_info, dict) and "model" in tags_in_info:
                            model = tags_in_info["model"]
                            break

            # 提取 values: [[ts_ms, value], ...]
            values = getattr(group, "values", None)
            if values is None and isinstance(group, dict):
                values = group.get("values", [])
            if not values:
                continue

            for point in values:
                if isinstance(point, (list, tuple)) and len(point) >= 2:
                    ts_ms, value = point[0], point[1]
                    dt = datetime.fromtimestamp(ts_ms / 1000, tz=BJT)
                    time_label = dt.strftime("%m-%d %H:%M")
                    model_data.setdefault(model, []).append({
                        "time": time_label,
                        "hit_rate": round(float(value), 4) if value is not None else None,
                    })
