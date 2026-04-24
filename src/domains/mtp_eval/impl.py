import logging
import os
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()
MTP_EVAL_HOST = os.getenv("MTP_EVAL_HOST")

logger = logging.getLogger(__name__)


class MtpEvalClient:
    """底层 HTTP Client，代理 qf_mtp_eval 服务的 v2 API。"""

    def __init__(self, host: str = MTP_EVAL_HOST, auth_token: str = ""):
        if not host:
            raise ValueError("MTP_EVAL_HOST 未配置")

        headers = {
            "Vortex-API-PreAuth": "1",
            "fastmtp_eval_session": "76f3e7fcaa2a4c00ae790c45678bf9e8",
            "Cookie": "fastmtp_eval_session=46f976146fb643ea93bef20c25728627",
        }
        cookies = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self.client = httpx.Client(
            base_url=f"{host}/api/v2",
            headers=headers,
            cookies=cookies,
            timeout=60.0,
            verify=False,
        )
        # v1 客户端用于统计等接口
        self.client_v1 = httpx.Client(
            base_url=f"{host}/api/v1",
            headers=headers,
            cookies=cookies,
            timeout=30.0,
            verify=False,
        )

    # ------------------------------------------------------------------
    # Benchmarks
    # ------------------------------------------------------------------

    def list_benches(self) -> list:
        logger.debug("请求获取基准测试列表")
        response = self.client.get("/benches")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Connectors
    # ------------------------------------------------------------------

    def list_connectors(self) -> list:
        logger.debug("请求获取连接器列表")
        response = self.client.get("/connectors")
        response.raise_for_status()
        return response.json()

    def create_connector(self, payload: dict) -> dict:
        logger.debug("请求创建连接器，payload=%s", payload)
        response = self.client.post("/connectors", json=payload)
        response.raise_for_status()
        return response.json()

    def list_connector_presets(self) -> list:
        logger.debug("请求获取连接器预设列表")
        response = self.client.get("/connector-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Service Profiles
    # ------------------------------------------------------------------

    def list_service_profiles(self) -> list:
        logger.debug("请求获取服务模板列表")
        response = self.client.get("/service-profiles")
        response.raise_for_status()
        return response.json()

    def create_service_profile(self, payload: dict) -> dict:
        logger.debug("请求创建服务模板，payload=%s", payload)
        response = self.client.post("/service-profiles", json=payload)
        response.raise_for_status()
        return response.json()

    def list_service_presets(self) -> list:
        logger.debug("请求获取服务预设列表")
        response = self.client.get("/service-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Task Presets
    # ------------------------------------------------------------------

    def list_task_presets(self) -> list:
        logger.debug("请求获取任务预设列表")
        response = self.client.get("/task-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Service Preview
    # ------------------------------------------------------------------

    def preview_service(self, payload: dict) -> dict:
        logger.debug("请求预览服务部署脚本，payload=%s", payload)
        response = self.client.post("/service-preview", json=payload)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def list_tasks(self) -> list:
        logger.debug("请求获取评测任务列表")
        response = self.client.get("/tasks")
        response.raise_for_status()
        return response.json()

    def get_task(self, task_id: str) -> dict:
        logger.debug("请求获取评测任务详情，task_id=%s", task_id)
        response = self.client.get(f"/tasks/{task_id}")
        response.raise_for_status()
        return response.json()

    def get_task_launch_config(self, task_id: str) -> dict:
        logger.debug("请求获取任务 launch config，task_id=%s", task_id)
        response = self.client.get(f"/tasks/{task_id}/launch-config")
        response.raise_for_status()
        return response.json()

    def launch_task(self, payload: dict) -> dict:
        logger.debug("请求发起评测任务，payload=%s", payload)
        response = self.client.post("/tasks/launch", json=payload)
        response.raise_for_status()
        return response.json()

    def cancel_task(self, task_id: str) -> dict:
        logger.debug("请求取消评测任务，task_id=%s", task_id)
        response = self.client.post(f"/tasks/{task_id}/cancel")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Statistics (v1 API)
    # ------------------------------------------------------------------

    def get_stats(self, hours: int = 24) -> dict:
        logger.debug("请求获取统计数据，hours=%s", hours)
        response = self.client_v1.get("/stats", params={"hours": hours})
        response.raise_for_status()
        return response.json()
