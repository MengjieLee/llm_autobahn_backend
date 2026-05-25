"""百度 TSDB 时序数据库连接器。

封装 bce-python-sdk 的 TsdbClient，提供异步写入/查询接口。
"""

import logging
import asyncio
from datetime import datetime
from typing import Any

from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.tsdb.tsdb_client import TsdbClient

from app.conf.config import settings

logger = logging.getLogger(__name__)


class TSDBConnector:
    """百度 TSDB 连接器（同步 SDK + asyncio.to_thread 包装）"""

    def __init__(self):
        config = BceClientConfiguration(
            credentials=BceCredentials(
                settings.TSDB_ACCESS_KEY_ID,
                settings.TSDB_SECRET_ACCESS_KEY,
            ),
            endpoint=settings.TSDB_ENDPOINT,
        )
        self._client = TsdbClient(config, database=settings.TSDB_DATABASE)
        logger.info("TSDB connector initialized | endpoint=%s db=%s",
                    settings.TSDB_ENDPOINT, settings.TSDB_DATABASE)

    def _sync_write(self, datapoints: list[dict]) -> None:
        """同步批量写入数据点。

        datapoints 格式:
        [
            {
                "metric": "kv_cache_hit_rate",
                "tags": {"task_id": "xxx", "model": "glm-5"},
                "timestamp": 1712797380000,  # ms epoch
                "value": 0.6238
            },
            ...
        ]
        """
        self._client.write_datapoints(datapoints)

    async def write_datapoints(self, datapoints: list[dict]) -> None:
        """异步批量写入。"""
        if not datapoints:
            return
        await asyncio.to_thread(self._sync_write, datapoints)

    def _sync_get_datapoints(self, query_list: list[dict]) -> Any:
        """同步查询数据点。

        query_list 格式:
        [
            {
                "metric": "kv_cache_hit_rate",
                "start_time": "2024-04-11T00:00:00Z",
                "end_time": "2024-04-12T00:00:00Z",
                "tags": ["task_id:xxx", "model:glm-5"],
                "limit": 5000,
            }
        ]
        """
        return self._client.get_datapoints(query_list)

    async def get_datapoints(self, query_list: list[dict]) -> Any:
        """异步查询数据点。"""
        return await asyncio.to_thread(self._sync_get_datapoints, query_list)

    def _sync_get_metrics(self) -> Any:
        return self._client.get_metrics()

    async def health_check(self) -> bool:
        """连接健康检查，尝试获取 metrics 列表。"""
        try:
            result = await asyncio.to_thread(self._sync_get_metrics)
            return bool(result)
        except Exception as e:
            logger.error("TSDB health check failed: %s", e)
            return False


# ---- 单例管理 ----
_connector: TSDBConnector | None = None


def get_tsdb_connector() -> TSDBConnector:
    """获取 TSDB 连接器单例。首次调用时初始化。"""
    global _connector
    if _connector is None:
        _connector = TSDBConnector()
    return _connector
