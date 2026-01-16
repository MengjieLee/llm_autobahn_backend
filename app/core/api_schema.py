from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, Field


T = TypeVar("T")


class BaseRequest(BaseModel):
    """统一请求基础结构，可扩展 trace_id、调用方信息等。"""

    trace_id: Optional[str] = Field(
        default=None, description="链路追踪 ID，用于问题排查"
    )


class StandardResponse(BaseModel, Generic[T]):
    """统一成功响应结构。"""

    code: int = Field(..., description="业务状态码，0 代表成功")
    message: str = Field(..., description="描述信息")
    data: Optional[T] = Field(default=None, description="业务数据")
    trace_id: Optional[str] = Field(
        default=None, description="链路追踪 ID，用于问题排查"
    )


class ErrorResponse(BaseModel):
    """统一错误响应结构。"""

    code: int = Field(..., description="错误码，非 0")
    message: str = Field(..., description="错误描述")
    detail: Optional[Any] = Field(default=None, description="详细错误信息")
    trace_id: Optional[str] = Field(
        default=None, description="链路追踪 ID，用于问题排查"
    )

