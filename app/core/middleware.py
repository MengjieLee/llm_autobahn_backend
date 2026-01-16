import time
import uuid
import sys
import logging
from pathlib import Path
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from app.core.api_schema import ErrorResponse
from app.core.request_context import ctx_set_username, ctx_set_trace_id


logger = logging.getLogger(__name__)


# 添加 context 目录到 Python 路径，以便导入 auth_client
_project_root = Path(__file__).parent.parent.parent
_context_path = _project_root / "context"
if str(_context_path) not in sys.path:
    sys.path.insert(0, str(_context_path))

# 导入认证相关函数
from context.auth_client import is_user_valid, get_user


# 不需要鉴权的路径列表
PUBLIC_PATHS = {
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
}

# 不需要鉴权的路径前缀
PUBLIC_PATH_PREFIXES = [
    "/api/v1/openapi",  # OpenAPI 文档相关路径
    "/api/v1/test",  # OpenAPI 文档相关路径
]


def extract_token(request: Request) -> str | None:
    """
    从请求头中提取 token。
    支持的方式：
    1. Authorization: Bearer <token>
    """
    # 优先从 Authorization header 中提取 Bearer token
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()
    
    return None


async def request_id_middleware(request: Request, call_next: Callable) -> Response:
    """为每个请求注入 trace_id，便于日志与响应关联。"""
    trace_id = request.headers.get("X-Trace-Id") or uuid.uuid4().hex
    request.state.trace_id = trace_id
    # 设置到上下文，供日志系统使用
    ctx_set_trace_id(trace_id)

    start = time.time()
    response = await call_next(request)
    cost = (time.time() - start) * 1000

    response.headers["X-Trace-Id"] = trace_id
    response.headers["X-Response-Time-ms"] = f"{cost:.2f}"
    return response


async def auth_middleware(request: Request, call_next: Callable) -> Response:
    """
    基于 header token 的鉴权中间件。
    从请求头中提取 token，验证用户有效性，并将用户信息存储到 request.state 中。
    """
    path = request.url.path
    trace_id = getattr(request.state, "trace_id", None)
    
    # 检查是否为公开路径，不需要鉴权
    if path in PUBLIC_PATHS:
        return await call_next(request)
    
    # 检查是否为公开路径前缀
    for prefix in PUBLIC_PATH_PREFIXES:
        if path.startswith(prefix):
            return await call_next(request)
    
    # 提取 token
    token = extract_token(request)
    
    if not token:
        logger.warning(f"鉴权失败：未提供 token | path={path} | trace_id={trace_id}")
        return JSONResponse(
            status_code=401,
            content=ErrorResponse(
                code=401,
                message="未提供认证 token，请在请求头中添加 Authorization: Bearer <token> 或 X-Token: <token>",
                detail=None,
                trace_id=trace_id,
            ).model_dump(),
        )
    
    # 验证 token 有效性
    if not is_user_valid(token):
        logger.warning(f"鉴权失败：token 无效或已过期 | path={path} | trace_id={trace_id}")
        return JSONResponse(
            status_code=401,
            content=ErrorResponse(
                code=401,
                message="认证 token 无效或已过期，请重新登录",
                detail=None,
                trace_id=trace_id,
            ).model_dump(),
        )
    
    # 获取用户信息并存储到 request.state
    user_info = get_user(token)
    if user_info:
        request.state.user = user_info
        request.state.token = token
        # 设置到上下文，供日志系统使用
        the_name = user_info.get("name") or user_info.get("username")
        ctx_set_username(the_name)
        logger.debug(f"鉴权成功 | username={the_name} | path={path} | trace_id={trace_id}")
    else:
        # 理论上不会到这里，因为 is_user_valid 已经验证通过
        logger.error(f"鉴权失败：无法获取用户信息 | path={path} | trace_id={trace_id}")
        return JSONResponse(
            status_code=401,
            content=ErrorResponse(
                code=401,
                message="无法获取用户信息",
                detail=None,
                trace_id=trace_id,
            ).model_dump(),
        )
    
    return await call_next(request)

