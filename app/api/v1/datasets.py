import json
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

from app.core.api_schema import StandardResponse
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList
from src.serializers.data_serializer import preview_serializer


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


@router.get("/detail", summary="查询数据集详情")
async def detail_dataset(
    request: Request,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:
    
    filter_dict = {
        "groups": getattr(request.state, "groups", []) or [],
        "name": request.query_params.get("name"),
    }
    dataset_lst = await service.list_datasets(DatasetList(**filter_dict))
    the_dataset = dataset_lst[0] if dataset_lst else None

    if not the_dataset:
        raise HTTPException(status_code=500, detail="数据集不存在")
    
    splits = {}
    for converted_preview_path in the_dataset["converted_preview_paths"]:
        records = []
        with open(converted_preview_path, "r") as f:
            idx = 0
            while idx < 100:
                line = f.readline().strip()
                if not line:
                     continue
                record = json.loads(line)
                records.append(record)
                idx += 1
        splits.update({
            Path(converted_preview_path).stem: preview_serializer(records)
        })

    the_dataset["splits"] = splits
    
    return StandardResponse(code=0, message="success", data=the_dataset, trace_id=None)
