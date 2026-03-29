import logging
from typing import List, Dict, Any, Callable
from datetime import datetime, timedelta
from .impl import ESIndexClient
from app.conf.config import settings


logger = logging.getLogger("es_query")

# 从 settings 读取配置
WINDOW_MINUTES = settings.ES_WINDOW_MINUTES

DEFAULT_APP_ID = settings.ES_DEFAULT_APP_ID
ES_HOST = settings.ES_HOST
ES_AUTH = (settings.ES_USER, settings.ES_PASSWORD)
ES_INDEX_PREFIX = settings.ES_INDEX_PREFIX

# 自适应降档：默认 60s，2GB 报错后依次降档
_WINDOW_SECONDS_TIERS = [60, 30, 15, 5]


def _is_2gb_error(exc: Exception) -> bool:
    """判断是否为 ES 2GB scroll buffer 超限错误"""
    msg = str(exc)
    return "2GB" in msg or "ReleasableBytesStreamOutput" in msg


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
        self._extra_clients = []  # 跨日期创建的额外客户端

    def close(self):
        """关闭所有 ES 连接，释放连接池内存"""
        self.es.close()
        for c in self._extra_clients:
            c.close()
        self._extra_clients.clear()

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

    def _get_es_for_date(self, date_str: str) -> ESIndexClient:
        """获取指定日期的 ES 客户端，跨日期时按需创建"""
        if date_str == self.date:
            return self.es
        client = ESIndexClient(
            ES_HOST, ES_AUTH,
            f"{ES_INDEX_PREFIX}{date_str}"
        )
        self._extra_clients.append(client)
        return client

    def _split_time_windows(self, start_dt: datetime, end_dt: datetime,
                            window_seconds: int = None) -> list:
        """
        将时间范围拆分为固定秒数的小窗口。

        :param start_dt: 开始时间
        :param end_dt: 结束时间
        :param window_seconds: 窗口大小（秒），默认用配置的 WINDOW_MINUTES * 60
        :return: [(start_dt, end_dt), ...] datetime 对象列表
        """
        if window_seconds is None:
            window_seconds = WINDOW_MINUTES * 60

        windows = []
        current = start_dt
        while current < end_dt:
            window_end = min(current + timedelta(seconds=window_seconds), end_dt)
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

    async def _query_window_adaptive(self, win_start: datetime, win_end: datetime,
                                      f, status_callback: Callable,
                                      base_count: int, win_label: str) -> int:
        """
        对单个窗口执行查询，遇到 2GB 错误时自动降档重试。

        降档策略：
        - 默认用当前窗口的完整时间范围
        - 2GB 报错 → 拆成 30s 子窗口重试
        - 再报错 → 拆成 15s 子窗口重试
        - 15s 仍报错 → 向上抛出异常

        降档只作用于当前窗口，不影响后续窗口。
        """
        span_seconds = int((win_end - win_start).total_seconds())

        # 尝试直接查询（不拆分）
        body = self._build_query_body(win_start, win_end)
        es = self._get_es_for_date(win_start.strftime("%Y-%m-%d"))
        try:
            count = await es.query_to_file_appender(body, f, status_callback, base_count)
            return count
        except Exception as e:
            if not _is_2gb_error(e):
                raise

        logger.warning(f"[adaptive] 2GB hit on {win_label} ({span_seconds}s), trying sub-windows")

        # 依次尝试更小的窗口
        for tier_seconds in _WINDOW_SECONDS_TIERS:
            if tier_seconds >= span_seconds:
                continue  # 跳过比当前窗口还大或相等的档位

            sub_windows = self._split_time_windows(win_start, win_end, tier_seconds)
            logger.info(f"[adaptive] retrying {win_label} with {tier_seconds}s windows ({len(sub_windows)} sub-windows)")

            if status_callback:
                status_callback(base_count, f"{win_label} 降档至 {tier_seconds}s 窗口重试...")

            sub_count = 0
            sub_failed = False
            for sub_idx, (sw_start, sw_end) in enumerate(sub_windows):
                sub_body = self._build_query_body(sw_start, sw_end)
                sub_es = self._get_es_for_date(sw_start.strftime("%Y-%m-%d"))
                try:
                    c = await sub_es.query_to_file_appender(
                        sub_body, f, status_callback, base_count + sub_count
                    )
                    sub_count += c
                except Exception as sub_e:
                    if _is_2gb_error(sub_e):
                        logger.warning(f"[adaptive] 2GB hit on sub-window {sub_idx + 1}/{len(sub_windows)} "
                                       f"({tier_seconds}s), will try next tier")
                        sub_failed = True
                        break
                    raise  # 非 2GB 错误直接抛出

            if not sub_failed:
                logger.info(f"[adaptive] {win_label} OK with {tier_seconds}s windows, count={sub_count}")
                return sub_count

        # 所有档位都失败
        raise RuntimeError(
            f"ES 2GB 限制：{win_label} 即使 {_WINDOW_SECONDS_TIERS[-1]}s 窗口仍超限，"
            f"请检查该时段数据量或联系管理员"
        )

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
        流式查询并写入文件（JSONL 格式，大数据量推荐）
        自动按窗口拆分时间，避免 ES 2GB scroll buffer 限制。
        遇到 2GB 错误时自适应降档（60s → 30s → 15s）。

        :param start_time: 开始时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param end_time: 结束时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param output_file: 输出文件路径
        :param status_callback: 状态回调函数
        :return: 总记录数
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)
        windows = self._split_time_windows(start_dt, end_dt)

        total_count = 0
        with open(output_file, 'w', encoding='utf-8') as f:
            for win_idx, (win_start, win_end) in enumerate(windows):
                win_start_str = win_start.strftime("%Y-%m-%d %H:%M:%S")
                win_end_str = win_end.strftime("%Y-%m-%d %H:%M:%S")
                win_label = f"窗口 {win_idx + 1}/{len(windows)} ({win_start_str}~{win_end_str})"

                if status_callback:
                    status_callback(
                        total_count,
                        f"正在查询{win_label}，已获取 {total_count} 条..."
                    )

                window_count = await self._query_window_adaptive(
                    win_start, win_end, f, status_callback, total_count, win_label
                )
                total_count += window_count

        if status_callback:
            status_callback(total_count, f"查询完成，共 {total_count} 条记录")

        return total_count
