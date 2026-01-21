import logging
from typing import Dict, Optional

import httpx
from fastapi import HTTPException

from .impl import ProcessSchedulerClient

logger = logging.getLogger(__name__)


class ProcessSchedulerService:
    """服务层：封装业务语义与异常处理，供接口层调用。"""

    def __init__(
        self,
        client: Optional[ProcessSchedulerClient] = None,
        auth_token: str = "",
    ):
        self.client = client or ProcessSchedulerClient(auth_token=auth_token)

    def list_jobs(self, params: Optional[dict] = None) -> dict:
        try:
            return self.client.list_job(params or {})
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "查询任务列表")

    def start_job(
        self,
        pipeline_id: str,
        queue: str,
        name: str,
        parameters: Optional[dict] = None,
    ) -> dict:
        try:
            return self.client.start_job(
                pipeline_id=pipeline_id,
                queue=queue,
                name=name,
                parameters=parameters,
            )
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "启动任务")

    def stop_job(self, job_id: str) -> dict:
        try:
            return self.client.stop_job(job_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "停止任务")

    def delete_job(self, job_id: str) -> dict:
        try:
            return self.client.delete_job(job_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "删除任务")

    def create_pipeline(
        self,
        pipeline_name: str,
        yaml_content: str,
        files: Optional[Dict[str, str]] = None,
    ) -> dict:
        try:
            return self.client.create_pipeline(
                pipeline_name=pipeline_name,
                yaml_content=yaml_content,
                files=files,
            )
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "创建 Pipeline")

    def list_pipelines(self, params: Optional[dict] = None) -> dict:
        try:
            return self.client.list_pipeline(params)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "查询 Pipeline 列表")

    def delete_pipeline(self, pipeline_id: str) -> dict:
        try:
            return self.client.delete_pipeline(pipeline_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "删除 Pipeline")

    def get_pipeline_detail(self, pipeline_id: str) -> dict:
        try:
            return self.client.get_pipeline_detail(pipeline_id)
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc, "查询 Pipeline 详情")

    def _handle_error(self, exc: Exception, action: str) -> None:
        logger.exception("%s失败: %s", action, exc)
        status_code = 500
        detail = str(exc)

        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            detail = exc.response.text

        raise HTTPException(status_code=status_code, detail=detail)
