import json
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

from app.core.api_schema import StandardResponse
from context.file_system import fs_manager
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList
from src.serializers.data_serializer import preview_serializer, splits_serializer


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


@router.post("/name-map", summary="查询数据集名称 -> iceberg 表名的映射")
async def name2table(
    request: Request,
    body: dict,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:
    body["groups"] = getattr(request.state, "groups", []) or []
    datasets = await service.list_datasets(DatasetList(**body))
    # 映射规则： {'name_value': tables[0]} 前提是 tables 有值
    # datasets = [
    #     {'name': 'name_value', 'tables':[]},
    #     {'name': 'name_value', 'tables':[]},
    # ] 
    data = {
        dataset["name"].lower(): dataset["tables"][0]
        for dataset in datasets 
        if dataset.get("tables")
    }
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
    
    splits = splits_serializer(the_dataset["converted_preview_paths"], media_root=the_dataset.get("media_root_dir", ""))
    the_dataset["splits"] = splits
    
    return StandardResponse(code=0, message="success", data=the_dataset, trace_id=None)


@router.post("/preview", summary="查询数据集预览")
async def preview_dataset(
    request: Request,
    body: dict,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:
    
    paths = body.get("paths")
    # 按优先级从高到低 ,或者\n 切分 paths 字符串成列表
    if "," in paths:
        paths = [path for path in paths.split(",") if path.strip()]
    elif "\n" in paths:
        paths = [path for path in paths.split("\n") if path.strip()]

    logger.info(f"preview_dataset: {paths}")
    for path in paths:
        if not fs_manager.exists(path):
            raise HTTPException(status_code=500, detail=f"数据路径 {path} 不存在")
        
    the_dataset = {}
    splits = splits_serializer(paths)
    the_dataset["splits"] = splits
    
    return StandardResponse(code=0, message="success", data=the_dataset, trace_id=None)

