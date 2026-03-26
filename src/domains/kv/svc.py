from typing import List, Dict, Any, Callable
from datetime import datetime, timedelta
from .impl import ESIndexClient
from app.conf.config import settings
import json


# 从 settings 读取配置
WINDOW_MINUTES = settings.ES_WINDOW_MINUTES

DEFAULT_APP_ID = settings.ES_DEFAULT_APP_ID
ES_HOST = settings.ES_HOST
ES_AUTH = (settings.ES_USER, settings.ES_PASSWORD)
ES_INDEX_PREFIX = settings.ES_INDEX_PREFIX


class ESIndexService:
    def __init__(self, date: str, app_id: str = DEFAULT_APP_ID, path: str = ""):
        """
        :param date: 日期，格式 YYYY-MM-DD，用于确定索引名称
        :param app_id: 应用 ID，默认 app-3Lut8O2E
        :param path: 场景过滤路径，非空时添加 match_phrase filter
        """
        self.date = date
        self.app_id = app_id
        self.path = path
        self.es = ESIndexClient(
            ES_HOST, ES_AUTH,
            f"{ES_INDEX_PREFIX}{self.date}"
        )

    def _parse_datetime(self, time_str: str) -> datetime:
        """
        解析时间字符串，支持两种格式：
        - HH:MM:SS（使用 self.date 作为日期）
        - YYYY-MM-DD HH:MM:SS（完整日期时间）
        """
        if len(time_str) > 8:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(f"{self.date} {time_str}", "%Y-%m-%d %H:%M:%S")

    def _to_utc(self, dt: datetime) -> str:
        """将北京时间 datetime 转换为 UTC 时间字符串"""
        utc_dt = dt - timedelta(hours=8)
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _get_window_minutes(self, hour: int) -> int:
        """固定 1 分钟窗口"""
        return WINDOW_MINUTES

    def _get_es_for_date(self, date_str: str) -> ESIndexClient:
        """获取指定日期的 ES 客户端，跨日期时按需创建"""
        if date_str == self.date:
            return self.es
        return ESIndexClient(
            ES_HOST, ES_AUTH,
            f"{ES_INDEX_PREFIX}{date_str}"
        )

    def _split_time_windows(self, start_time: str, end_time: str) -> list:
        """
        将大时间范围拆分为多个小窗口，高峰期 (10-19点) 每 2 分钟，其他时段每 5 分钟。
        支持跨日期。

        :param start_time: 开始时间 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param end_time: 结束时间 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :return: [(start_dt, end_dt), ...] datetime 对象列表
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)

        windows = []
        current = start_dt
        while current < end_dt:
            window_minutes = self._get_window_minutes(current.hour)
            window_end = min(current + timedelta(minutes=window_minutes), end_dt)
            windows.append((current, window_end))
            current = window_end

        return windows

    def _build_query_body(self, start_dt: datetime, end_dt: datetime) -> dict:
        """
        构建 ES 查询 body

        :param start_dt: 开始时间（北京时间）datetime
        :param end_dt: 结束时间（北京时间）datetime
        """
        gte_utc = self._to_utc(start_dt)
        lte_utc = self._to_utc(end_dt)

        filters = [
            {"match_all": {}},
            {"match_phrase": {"function": "parseRequestV2"}},
            {
                "range": {
                    "@timestamp": {
                        "gte": gte_utc,
                        "lte": lte_utc,
                        "format": "strict_date_optional_time"
                    }
                }
            }
        ]

        # app_id 非空时才添加过滤
        if self.app_id:
            filters.append({"match_phrase": {"app_id": self.app_id}})

        # 场景过滤：path 非空时添加 match_phrase
        if self.path:
            filters.append({"match_phrase": {"path": self.path}})

        return {
            "query": {
                "bool": {
                    "must": [],
                    "filter": filters,
                    "should": [],
                    "must_not": []
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc",
                        "unmapped_type": "boolean"
                    }
                }
            ]
        }

    async def query(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """
        查询指定时间范围内的数据（小数据量使用）
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)
        body = self._build_query_body(start_dt, end_dt)
        return await self.es.query(body)

    async def query_to_file(self, start_time: str, end_time: str, output_file: str, status_callback: Callable = None) -> int:
        """
        流式查询并写入文件（大数据量推荐，避免内存溢出）
        自动按窗口拆分时间，避免 ES 2GB scroll buffer 限制。
        支持跨日期查询，如 2026-03-23 18:00:00 ~ 2026-03-24 18:00:00

        :param start_time: 开始时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param end_time: 结束时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param output_file: 输出文件路径
        :param status_callback: 状态回调函数
        :return: 总记录数
        """
        windows = self._split_time_windows(start_time, end_time)

        if len(windows) == 1:
            start_dt, end_dt = windows[0]
            body = self._build_query_body(start_dt, end_dt)
            es = self._get_es_for_date(start_dt.strftime("%Y-%m-%d"))
            return await es.query_to_file(body, output_file, status_callback)

        # 多窗口：逐段查询，拼接写入同一个文件
        total_count = 0
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_record = True

            for win_idx, (win_start, win_end) in enumerate(windows):
                win_start_str = win_start.strftime("%Y-%m-%d %H:%M:%S")
                win_end_str = win_end.strftime("%Y-%m-%d %H:%M:%S")
                if status_callback:
                    status_callback(
                        total_count,
                        f"正在查询窗口 {win_idx + 1}/{len(windows)} ({win_start_str}~{win_end_str})，已获取 {total_count} 条..."
                    )

                body = self._build_query_body(win_start, win_end)
                es = self._get_es_for_date(win_start.strftime("%Y-%m-%d"))
                window_count = await es.query_to_file_appender(
                    body, f, first_record, status_callback, total_count
                )
                if window_count > 0:
                    first_record = False
                total_count += window_count

            f.write('\n]')

        if status_callback:
            status_callback(total_count, f"查询完成，共 {total_count} 条记录")

        return total_count
