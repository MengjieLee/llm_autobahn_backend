from typing import Any, List
import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.core.api_schema import BaseRequest, StandardResponse


logger = logging.getLogger(__name__)
router = APIRouter()


class DomainConfig(BaseModel):
    """单个垂直领域配置示例."""

    name: str = Field(..., description="领域名称，例如: finance, healthcare")
    llm_provider: str = Field(..., description="对应使用的大模型服务提供方")
    description: str | None = Field(default=None, description="领域描述")


class DomainListRequest(BaseRequest):
    """查询已集成的垂类领域列表，请求示例."""

    only_active: bool = Field(
        default=True,
        description="是否只返回已启用的领域",
    )


class DomainListResponseData(BaseModel):
    """返回数据示例."""

    total: int = Field(..., description="领域数量")
    items: List[DomainConfig] = Field(..., description="领域列表")


@router.post(
    "/list",
    response_model=StandardResponse[DomainListResponseData],
    summary="查询已集成的垂类领域列表",
    description="作为大模型垂类领域整合后端的示例接口，返回当前可用的垂直领域配置。",
)
async def list_domains(request: Request, body: DomainListRequest) -> StandardResponse[DomainListResponseData]:
    logger.info(f"list_domains called | only_active={body.only_active}")

    mock_items: List[DomainConfig] = [
        DomainConfig(
            name="finance",
            llm_provider="openai",
            description="金融投研、风控场景",
        ),
        DomainConfig(
            name="healthcare",
            llm_provider="azure_openai",
            description="医疗问诊与知识检索",
        ),
    ]

    if body.only_active:
        items: List[DomainConfig] = mock_items
    else:
        items = mock_items

    data = DomainListResponseData(total=len(items), items=items)
    return StandardResponse[DomainListResponseData](
        code=0,
        message="success",
        data=data,
        trace_id=body.trace_id,
    )
