import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from app.core.request_context import log_usage
from src.domains.mtp_eval.svc import MtpEvalService

logger = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> MtpEvalService:
    """基于鉴权中间件注入的 token 构造 service."""
    token = getattr(request.state, "token", "") or ""
    return MtpEvalService(auth_token=token)


# ------------------------------------------------------------------
# Request Models
# ------------------------------------------------------------------


class CreateConnectorRequest(BaseModel):
    name: str = Field(..., description="连接器名称")
    kind: str = Field(..., description="连接器类型：local / ssh / kubectl_exec")
    repo_root: str = Field(..., description="代码仓库根目录")
    work_root: Optional[str] = Field(default=None, description="工作目录")
    config: Dict[str, Any] = Field(default_factory=dict, description="连接器特定配置")


class CreateServiceProfileRequest(BaseModel):
    name: str = Field(..., description="服务模板名称")
    service: Dict[str, Any] = Field(..., description="服务配置 JSON")


class ServicePreviewRequest(BaseModel):
    connector_id: str = Field(..., description="连接器 ID")
    service: Dict[str, Any] = Field(..., description="服务配置 JSON")


class LaunchTaskRequest(BaseModel):
    name: str = Field(..., description="任务名称")
    connector_id: str = Field(..., description="连接器 ID")
    service: Dict[str, Any] = Field(default_factory=dict, description="服务部署配置")
    tasks: List[Dict[str, Any]] = Field(..., description="评测任务列表")
    remote_run_root: Optional[str] = Field(default=None, description="远程运行根目录")
    service_profile_id: Optional[str] = Field(default=None, description="服务模板 ID")


# ------------------------------------------------------------------
# Benchmarks
# ------------------------------------------------------------------


@router.get("/benches", summary="获取基准测试列表")
async def list_benches(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_benches")
    data = await service.list_benches()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Connectors
# ------------------------------------------------------------------


@router.get("/connectors", summary="获取连接器列表")
async def list_connectors(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_connectors")
    data = await service.list_connectors()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/connectors", summary="创建连接器")
async def create_connector(
    body: CreateConnectorRequest,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_create_connector")
    data = await service.create_connector(body.model_dump())
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/connector-presets", summary="获取连接器预设列表")
async def list_connector_presets(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_connector_presets")
    data = await service.list_connector_presets()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Service Profiles
# ------------------------------------------------------------------


@router.get("/service-profiles", summary="获取服务模板列表")
async def list_service_profiles(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_service_profiles")
    data = await service.list_service_profiles()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/service-profiles", summary="创建服务模板")
async def create_service_profile(
    body: CreateServiceProfileRequest,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_create_service_profile")
    data = await service.create_service_profile(body.model_dump())
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/service-presets", summary="获取服务预设列表")
async def list_service_presets(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_service_presets")
    data = await service.list_service_presets()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Task Presets
# ------------------------------------------------------------------


@router.get("/task-presets", summary="获取任务预设列表")
async def list_task_presets(
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[list]:
    log_usage("mtp_eval_list_task_presets")
    data = await service.list_task_presets()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Service Preview
# ------------------------------------------------------------------


@router.post("/service-preview", summary="预览服务部署脚本")
async def preview_service(
    body: ServicePreviewRequest,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_preview_service")
    data = await service.preview_service(body.model_dump())
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------


@router.get("/tasks", summary="获取评测任务列表")
async def list_tasks(
    service: MtpEvalService = Depends(get_service),
    poll: Optional[int] = None,
) -> StandardResponse[list]:
    if not poll:
        pass
        # [TODO] 先 pass 待污染源清理后再人工恢复。
        # log_usage("mtp_eval_list_tasks")
    data = await service.list_tasks()
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/tasks/{task_id}", summary="获取评测任务详情")
async def get_task(
    task_id: str,
    service: MtpEvalService = Depends(get_service),
    poll: Optional[int] = None,
) -> StandardResponse[dict]:
    if not poll:
        log_usage("mtp_eval_get_task")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id 不能为空")
    data = await service.get_task(task_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/tasks/{task_id}/launch-config", summary="获取任务启动配置")
async def get_task_launch_config(
    task_id: str,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_get_task_launch_config")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id 不能为空")
    data = await service.get_task_launch_config(task_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/tasks/launch", summary="发起评测任务")
async def launch_task(
    body: LaunchTaskRequest,
    request: Request,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_launch_task")
    payload = body.model_dump()
    # 确保每个 task 都有 id，下游 materialize 流程依赖此字段
    for task in payload.get("tasks") or []:
        if "id" not in task:
            task["id"] = task.get("bench") or str(hash(str(task)))
    # 注入当前用户作为 task owner
    user_info = getattr(request.state, "user", None) or {}
    owner = user_info.get("name") or user_info.get("username") or ""
    if owner:
        payload["owner"] = owner
    data = await service.launch_task(payload)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/tasks/{task_id}/cancel", summary="取消评测任务")
async def cancel_task(
    task_id: str,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_cancel_task")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id 不能为空")
    data = await service.cancel_task(task_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/tasks/{task_id}/continue", summary="继续评测任务")
async def continue_task(
    task_id: str,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_continue_task")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id 不能为空")
    data = await service.continue_task(task_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


# ------------------------------------------------------------------
# Statistics
# ------------------------------------------------------------------


@router.get("/stats", summary="获取评测统计数据")
async def get_statistics(
    hours: int = 24,
    service: MtpEvalService = Depends(get_service),
) -> StandardResponse[dict]:
    log_usage("mtp_eval_get_stats")
    data = await service.get_stats(hours)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)
