import logging
from typing import Dict, Optional

import httpx
from fastapi import HTTPException

from .impl import DatasetsClient, DatasetList

logger = logging.getLogger(__name__)


class DatasetsService:
    """服务层：封装业务语义与异常处理，供接口层调用。"""

    def __init__(
        self,
        client: Optional[DatasetsClient] = None,
        auth_token: str = "",
    ):
        self.client = client or DatasetsClient(auth_token=auth_token)

    async def list_datasets(self, filter: Optional[DatasetList] = None) -> list:
        try:
            return self.client.list_datasets(filter)
        except Exception as exc:
            self._handle_error(exc, "查询数据集列表失败")

    def _handle_error(self, exc: Exception, message: str):
        logger.error(f"{message}失败: {exc}")
        raise HTTPException(status_code=500, detail=message) from exc
