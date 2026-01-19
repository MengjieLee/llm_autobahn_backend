import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from src.domain.process_scheduler.svc import ProcessSchedulerService

logger = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> ProcessSchedulerService:
    """基于鉴权中间件注入的 token 构造 service."""
    token = getattr(request.state, "token", "") or ""
    return ProcessSchedulerService(auth_token=token)


class StartJobRequest(BaseModel):
    pipeline_id: str = Field(..., description="Pipeline ID")
    queue: str = Field(..., description="队列名")
    name: str = Field(..., description="任务名称")
    parameters: Optional[Dict[str, Any]] = Field(
        default=None, description="任务参数，可为空"
    )


class CreatePipelineRequest(BaseModel):
    pipeline_name: str = Field(..., description="Pipeline 名称")
    yaml_content: str = Field(..., description="Pipeline YAML 内容（jinja）")
    files: Optional[Dict[str, str]] = Field(
        default=None, description="可选文件映射，形如 {'custom_dir': '/path/to/file'}"
    )


@router.get("/jobs", summary="查询任务列表")
async def list_jobs(
    request: Request, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    params = dict(request.query_params)
    data = service.list_jobs(params)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/jobs", summary="启动任务")
async def start_job(
    body: StartJobRequest, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    data = service.start_job(
        pipeline_id=body.pipeline_id,
        queue=body.queue,
        name=body.name,
        parameters=body.parameters,
    )
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/jobs/{job_id}/stop", summary="停止任务")
async def stop_job(
    job_id: str, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id 不能为空")
    data = service.stop_job(job_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.delete("/jobs/{job_id}", summary="删除任务")
async def delete_job(
    job_id: str, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id 不能为空")
    data = service.delete_job(job_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.post("/pipelines", summary="创建 Pipeline")
async def create_pipeline(
    body: CreatePipelineRequest, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    data = service.create_pipeline(
        pipeline_name=body.pipeline_name,
        yaml_content=body.yaml_content,
        files=body.files,
    )
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/pipelines", summary="查询 Pipeline 列表")
async def list_pipelines(
    request: Request, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    params = dict(request.query_params)
    data = service.list_pipelines(params)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.delete("/pipelines/{pipeline_id}", summary="删除 Pipeline")
async def delete_pipeline(
    pipeline_id: str, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="pipeline_id 不能为空")
    data = service.delete_pipeline(pipeline_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/pipelines/{pipeline_id}", summary="Pipeline 详情")
async def get_pipeline_detail(
    pipeline_id: str, service: ProcessSchedulerService = Depends(get_service)
) -> StandardResponse[dict]:
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="pipeline_id 不能为空")
    data = service.get_pipeline_detail(pipeline_id)
    return StandardResponse(code=0, message="success", data=data, trace_id=None)
