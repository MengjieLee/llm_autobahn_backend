import logging
from typing import Optional

import httpx
from fastapi import HTTPException

from .impl import MtpEvalClient

logger = logging.getLogger(__name__)


class MtpEvalService:
    """服务层：封装业务语义与异常处理，供接口层调用。"""

    def __init__(
        self,
        client: Optional[MtpEvalClient] = None,
        auth_token: str = "",
    ):
        self.client = client or MtpEvalClient(auth_token=auth_token)

    # ------------------------------------------------------------------
    # Benchmarks
    # ------------------------------------------------------------------

    async def list_benches(self) -> list:
        try:
            return self.client.list_benches()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取基准测试列表")

    # ------------------------------------------------------------------
    # Connectors
    # ------------------------------------------------------------------

    async def list_connectors(self) -> list:
        try:
            return self.client.list_connectors()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取连接器列表")

    async def create_connector(self, payload: dict) -> dict:
        try:
            return self.client.create_connector(payload)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "创建连接器")

    async def list_connector_presets(self) -> list:
        try:
            return self.client.list_connector_presets()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取连接器预设列表")

    # ------------------------------------------------------------------
    # Service Profiles
    # ------------------------------------------------------------------

    async def list_service_profiles(self) -> list:
        try:
            return self.client.list_service_profiles()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取服务模板列表")

    async def create_service_profile(self, payload: dict) -> dict:
        try:
            return self.client.create_service_profile(payload)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "创建服务模板")

    async def list_service_presets(self) -> list:
        try:
            return self.client.list_service_presets()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取服务预设列表")

    # ------------------------------------------------------------------
    # Task Presets
    # ------------------------------------------------------------------

    async def list_task_presets(self) -> list:
        try:
            return self.client.list_task_presets()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取任务预设列表")

    # ------------------------------------------------------------------
    # Service Preview
    # ------------------------------------------------------------------

    async def preview_service(self, payload: dict) -> dict:
        try:
            return self.client.preview_service(payload)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "预览服务部署脚本")

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    async def list_tasks(self) -> list:
        try:
            tasks = self.client.list_tasks()
            # 按创建时间降序排列
            tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
            return tasks
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取评测任务列表")

    async def get_task(self, task_id: str) -> dict:
        try:
            return self.client.get_task(task_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取评测任务详情")

    async def get_task_launch_config(self, task_id: str) -> dict:
        try:
            return self.client.get_task_launch_config(task_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取任务启动配置")

    async def launch_task(self, payload: dict) -> dict:
        try:
            return self.client.launch_task(payload)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "发起评测任务")

    async def cancel_task(self, task_id: str) -> dict:
        try:
            return self.client.cancel_task(task_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "取消评测任务")

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    async def get_stats(self, hours: int = 24) -> dict:
        try:
            return self.client.get_stats(hours)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "获取统计数据")

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def _handle_error(self, exc: Exception, action: str) -> None:
        logger.exception("%s失败: %s", action, exc)
        status_code = 500
        detail = str(exc)

        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            detail = exc.response.text

        raise HTTPException(status_code=status_code, detail=detail)
