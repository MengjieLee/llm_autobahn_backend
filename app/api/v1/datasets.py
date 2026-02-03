import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList

logger = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> DatasetsService:
    """基于鉴权中间件注入的 token 构造 service."""
    token = getattr(request.state, "token", "") or ""
    return DatasetsService(auth_token=token)


@router.post("/list", summary="查询数据集列表")
async def list_datasets(
    request: Request,
    body: dict,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:
    body["groups"] = getattr(request.state, "groups", []) or []
    data = {"result": await service.list_datasets(DatasetList(**body))}
    return StandardResponse(code=0, message="success", data=data, trace_id=None)
