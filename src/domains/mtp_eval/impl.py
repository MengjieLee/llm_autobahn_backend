import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()
MTP_EVAL_HOST = os.getenv("MTP_EVAL_HOST")
MTP_EVAL_SESSION = os.getenv("MTP_EVAL_SESSION", "")
MTP_EVAL_COOKIE = os.getenv("MTP_EVAL_COOKIE", "")

logger = logging.getLogger(__name__)


class MtpEvalClient:
    """底层 HTTP Client，代理 qf_mtp_eval 服务的 v2 API。"""

    def __init__(self, host: str = MTP_EVAL_HOST, auth_token: str = ""):
        if not host:
            raise ValueError("MTP_EVAL_HOST 未配置")

        headers = {
            "Vortex-API-PreAuth": "1",
            "fastmtp_eval_session": MTP_EVAL_SESSION,
            "Cookie": f"fastmtp_eval_session={MTP_EVAL_COOKIE}",
        }
        cookies = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self.client = httpx.AsyncClient(
            base_url=f"{host}/api/v2",
            headers=headers,
            cookies=cookies,
            timeout=600.0,
            verify=False,
        )
        self.client_v1 = httpx.AsyncClient(
            base_url=f"{host}/api/v1",
            headers=headers,
            cookies=cookies,
            timeout=30.0,
            verify=False,
        )

    # ------------------------------------------------------------------
    # Benchmarks
    # ------------------------------------------------------------------

    async def list_benches(self) -> list:
        logger.debug("请求获取基准测试列表")
        response = await self.client.get("/benches")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Connectors
    # ------------------------------------------------------------------

    async def list_connectors(self) -> list:
        logger.debug("请求获取连接器列表")
        response = await self.client.get("/connectors")
        response.raise_for_status()
        return response.json()

    async def create_connector(self, payload: dict) -> dict:
        logger.debug("请求创建连接器，payload=%s", payload)
        response = await self.client.post("/connectors", json=payload)
        response.raise_for_status()
        return response.json()

    async def list_connector_presets(self) -> list:
        logger.debug("请求获取连接器预设列表")
        response = await self.client.get("/connector-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Service Profiles
    # ------------------------------------------------------------------

    async def list_service_profiles(self) -> list:
        logger.debug("请求获取服务模板列表")
        response = await self.client.get("/service-profiles")
        response.raise_for_status()
        return response.json()

    async def create_service_profile(self, payload: dict) -> dict:
        logger.debug("请求创建服务模板，payload=%s", payload)
        response = await self.client.post("/service-profiles", json=payload)
        response.raise_for_status()
        return response.json()

    async def list_service_presets(self) -> list:
        logger.debug("请求获取服务预设列表")
        response = await self.client.get("/service-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Task Presets
    # ------------------------------------------------------------------

    async def list_task_presets(self) -> list:
        logger.debug("请求获取任务预设列表")
        response = await self.client.get("/task-presets")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Service Preview
    # ------------------------------------------------------------------

    async def preview_service(self, payload: dict) -> dict:
        logger.debug("请求预览服务部署脚本，payload=%s", payload)
        response = await self.client.post("/service-preview", json=payload)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    async def list_tasks(self, include_deleted: bool = False) -> list:
        logger.debug("请求获取评测任务列表, include_deleted=%s", include_deleted)
        response = await self.client.get("/tasks", params={"include_deleted": include_deleted})
        response.raise_for_status()
        return response.json()

    async def get_task(self, task_id: str) -> dict:
        logger.debug("请求获取评测任务详情，task_id=%s", task_id)
        response = await self.client.get(f"/tasks/{task_id}")
        response.raise_for_status()
        return response.json()

    async def get_task_launch_config(self, task_id: str) -> dict:
        logger.debug("请求获取任务 launch config，task_id=%s", task_id)
        response = await self.client.get(f"/tasks/{task_id}/launch-config")
        response.raise_for_status()
        return response.json()

    async def launch_task(self, payload: dict) -> dict:
        logger.debug("请求发起评测任务，payload=%s", payload)
        response = await self.client.post("/tasks/launch", json=payload, timeout=600.0)
        response.raise_for_status()
        return response.json()

    async def cancel_task(self, task_id: str) -> dict:
        logger.debug("请求取消评测任务，task_id=%s", task_id)
        response = await self.client.post(f"/tasks/{task_id}/cancel")
        response.raise_for_status()
        return response.json()

    async def continue_task(self, task_id: str) -> dict:
        logger.debug("请求继续评测任务，task_id=%s", task_id)
        response = await self.client.post(f"/tasks/{task_id}/continue")
        response.raise_for_status()
        return response.json()

    async def archive_task(self, task_id: str) -> dict:
        logger.debug("请求归档任务，task_id=%s", task_id)
        response = await self.client.post(f"/tasks/{task_id}/archive")
        response.raise_for_status()
        return response.json()

    async def unarchive_task(self, task_id: str) -> dict:
        logger.debug("请求取消归档任务，task_id=%s", task_id)
        response = await self.client.post(f"/tasks/{task_id}/unarchive")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Statistics (v1 API)
    # ------------------------------------------------------------------

    async def get_stats(self, hours: int = 24) -> dict:
        logger.debug("请求获取统计数据，hours=%s", hours)
        response = await self.client_v1.get("/stats", params={"hours": hours})
        response.raise_for_status()
        return response.json()
