import logging
from collections import Counter
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from context.auth_client import users_amount
from src.domains.datasets.svc import DatasetsService
from src.domains.datasets.impl import DatasetList


logger = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> DatasetsService:
    """基于鉴权中间件注入的 token 构造 service."""
    token = getattr(request.state, "token", "") or ""
    return DatasetsService(auth_token=token)


@router.get("/datasets/metrics", summary="查询数据集指标")
async def datasets_metrics(
    request: Request,
    service: DatasetsService = Depends(get_service)
) -> StandardResponse[dict]:

    body = {}
    body["groups"] = getattr(request.state, "groups", []) or []
    datasets_lst = await service.list_datasets(DatasetList(**body))
    datasets_amount = len(datasets_lst)

    sub_types = []
    for ds in datasets_lst:
        # 获取 ds 对象中 labels 字段（根据你的数据结构，可能是字典或 Pydantic 模型）
        labels = ds.get("labels", []) if isinstance(ds, dict) else ds.labels
        
        for label in labels:
            # 兼容模型对象或字典格式
            l_name = label.get("label_name") if isinstance(label, dict) else label.label_name
            l_values = label.get("label_values") if isinstance(label, dict) else label.label_values
            
            if l_name == "数据细分类型":
                sub_types.extend(l_values)

    # 3. 使用 Counter 统计分布
    # 结果格式：{"OCR": 10, "VQA": 5, ...}
    dist_dict = dict(Counter(sub_types))

    datasets_distribution = sorted(
        [{"name": k, "value": v} for k, v in dist_dict.items()],
        key=lambda x: x['value'],
        reverse=True
    )

    data = {
        "datasets_amount": datasets_amount,
        "datasets_distribution": datasets_distribution
    }

    return StandardResponse(code=0, message="success", data=data, trace_id=None)


@router.get("/users/metrics", summary="查询用户指标")
async def users_metrics(
    request: Request,
) -> StandardResponse[dict]:
    data = {
        "users_amount": await users_amount()
    }
    return StandardResponse(code=0, message="success", data=data, trace_id=None)

