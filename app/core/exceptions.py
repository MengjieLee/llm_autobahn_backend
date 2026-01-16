from typing import Any, Optional
import logging

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from app.core.api_schema import ErrorResponse


logger = logging.getLogger(__name__)


class BizException(HTTPException):
    """统一业务异常."""

    def __init__(
        self,
        status_code: int = 400,
        code: int = 10000,
        message: str = "业务异常",
        detail: Any | None = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=message)
        self.biz_code = code
        self.biz_detail = detail


async def http_exception_handler(
    request: Request, exc: HTTPException
) -> JSONResponse:
    trace_id: Optional[str] = getattr(request.state, "trace_id", None)

    logger.error(f"请求失败 | status={exc.status_code} | path={request.url.path} | detail={exc.detail} | trace_id={trace_id}")

    payload = ErrorResponse(
        code=exc.status_code,
        message=exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        detail=None,
        trace_id=trace_id,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=payload.model_dump(),
    )


async def biz_exception_handler(request: Request, exc: BizException) -> JSONResponse:
    trace_id: Optional[str] = getattr(request.state, "trace_id", None)

    logger.error(f"业务请求失败 | biz_code={exc.biz_code} | status={exc.status_code} | path={request.url.path} | detail={exc.biz_detail} | trace_id={trace_id}")

    payload = ErrorResponse(
        code=exc.biz_code,
        message=exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        detail=exc.biz_detail,
        trace_id=trace_id,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=payload.model_dump(),
    )


async def generic_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    trace_id: Optional[str] = getattr(request.state, "trace_id", None)

    logger.error(f"内部服务通用错误 | path={request.url.path} | trace_id={trace_id} ", exc_info=True)

    payload = ErrorResponse(
        code=500,
        message="内部服务器错误",
        detail=str(exc),
        trace_id=trace_id,
    )
    return JSONResponse(
        status_code=500,
        content=payload.model_dump(),
    )

