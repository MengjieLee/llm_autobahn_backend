import logging
import os
from typing import Dict, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()
PROCESS_SCHEDULER_HOST = os.getenv("PROCESS_SCHEDULER_HOST")

logger = logging.getLogger(__name__)


class ProcessSchedulerClient:
    """底层 HTTP Client，仅负责请求发送和响应解析。"""

    def __init__(self, host: str = PROCESS_SCHEDULER_HOST, auth_token: str = ""):
        if not host:
            raise ValueError("PROCESS_SCHEDULER_HOST 未配置")

        headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
        self.client = httpx.Client(
            base_url=f"{host}/api/v1",
            headers=headers,
            timeout=20.0,
        )

    def list_job(self, params: Optional[dict] = None) -> dict:
        logger.debug("请求列表任务，params=%s", params)
        response = self.client.get("/jobs", params=params)
        response.raise_for_status()
        return response.json()

    def stop_job(self, job_id: str) -> dict:
        logger.debug("请求停止任务，job_id=%s", job_id)
        response = self.client.post(f"/jobs/{job_id}/stop")
        response.raise_for_status()
        return response.json()

    def start_job(
        self,
        pipeline_id: str,
        queue: str,
        name: str,
        parameters: Optional[dict] = None,
    ) -> dict:
        payload = {
            "pipeline_id": pipeline_id,
            "queue": queue,
            "name": name,
            "parameters": parameters,
        }
        logger.debug("请求启动任务，payload=%s", payload)
        response = self.client.post("/jobs", json=payload)
        response.raise_for_status()
        return response.json()

    def delete_job(self, job_id: str) -> dict:
        logger.debug("请求删除任务，job_id=%s", job_id)
        response = self.client.delete(f"/jobs/{job_id}")
        response.raise_for_status()
        return response.json()

    def create_pipeline(
        self,
        pipeline_name: str,
        yaml_content: str,
        files: Optional[Dict[str, str]] = None,
    ) -> dict:
        """创建新的 pipeline，支持上传多个文件。"""
        form_data = {"name": (None, pipeline_name), "yaml_str": (None, yaml_content)}

        if files:
            for field_name, file_path in files.items():
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"文件不存在: {file_path}")
                if not os.path.isfile(file_path):
                    raise IsADirectoryError(f"不是有效的文件: {file_path}")

                file_name = os.path.basename(file_path)
                form_data[field_name] = (
                    file_name,
                    open(file_path, "rb"),
                    "application/octet-stream",
                )

        try:
            logger.debug("请求创建 pipeline，name=%s, files=%s", pipeline_name, files)
            response = self.client.post("/pipelines", files=form_data)
            response.raise_for_status()
            return response.json()
        finally:
            for value in form_data.values():
                if (
                    isinstance(value, tuple)
                    and len(value) >= 2
                    and hasattr(value[1], "close")
                ):
                    value[1].close()

    def list_pipeline(self, params: Optional[dict] = None) -> dict:
        logger.debug("请求列表 pipeline，params=%s", params)
        response = self.client.get("/pipelines", params=params)
        response.raise_for_status()
        return response.json()

    def delete_pipeline(self, pipeline_id: str) -> dict:
        logger.debug("请求删除 pipeline，pipeline_id=%s", pipeline_id)
        response = self.client.delete(f"/pipelines/{pipeline_id}")
        response.raise_for_status()
        return response.json()

    def get_pipeline_detail(self, pipeline_id: str) -> dict:
        logger.debug("请求获取 pipeline 详情，pipeline_id=%s", pipeline_id)
        response = self.client.get(f"/pipelines/{pipeline_id}")
        response.raise_for_status()
        return response.json()
