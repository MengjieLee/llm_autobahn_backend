from typing import Any, List, Annotated
import hashlib
import logging
import jwt

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel, Field

from app.conf.config import settings
from app.core.api_schema import StandardResponse, ErrorResponse
from app.core.request_context import log_usage
from context.auth_client import add_or_update_user, is_user_existed, is_user_valid, update_user, get_user


logger = logging.getLogger(__name__)
router = APIRouter()


def generate_auth_token(username):
    """生成认证令牌"""
    # 使用时间戳和随机数生成唯一令牌
    unique_string = f"{username}"
    return hashlib.sha256(unique_string.encode()).hexdigest()


ZtAuthorizationHeader = Annotated[
    str | None,
    Header(
        alias="X-Zt-Authorization",
        description="零信任网关注入的JWT令牌，用于解析用户信息",
    )
]


class AccountModel(BaseModel):
    jwt: str = Field(..., description="jwt")
    name: str = Field(..., description="中文名")
    username: str = Field(..., description="邮箱前缀")
    token: str = Field(..., description="唯一标识符")
    groups: List[str] = Field(default=[], description="权限组")


class AccountResponseData(BaseModel):
    user: AccountModel = Field(..., description="用户信息的字典")


@router.post(
    "/login",
    response_model=StandardResponse[AccountResponseData],
    summary="携带零信任网关 jwt 的登录接口",
    description="从 Header 中解析 X-Zt-Authorization，返回当前应用的用户信息",
)
async def login(
    request: Request,
    zt_authorization: ZtAuthorizationHeader = None
) -> StandardResponse[AccountResponseData]:
    # 调试：打印收到的 headers
    logger.info(f"收到的所有 Headers: {dict(request.headers)}")
    logger.info(f"X-Zt-Authorization: {zt_authorization}")

    logger.info(f"零信任网关验证开始.")

    if not zt_authorization:
        logger.error(f"JWT 校验失败: Header 中无有效的 X-Zt-Authorization")
        return ErrorResponse(
            code=400,
            message="JWT 校验失败",
            detail="Header 中无有效的 X-Zt-Authorization"
        )

    jwt_decoded = jwt.decode(zt_authorization, options={"verify_signature": False})
    name = jwt_decoded.get("name")
    username = jwt_decoded.get("username")
    token = generate_auth_token(username)
    user_dict = await add_or_update_user(token, username, settings.DEFAULT_GROUPS, name)
    data = AccountResponseData(user=AccountModel(
        jwt=zt_authorization,
        name=user_dict.get("name"),
        username=user_dict.get("username"),
        token=user_dict.get("token"),
        groups=user_dict.get("groups"),
    ))
    logger.info(f"零信任网关验证结束.")
    auth_msg = "老用户 re-login" if not user_dict.get("is_new", False) else "新用户注册"
    log_usage(auth_msg, scenario="API", user=name or username)
    return StandardResponse[AccountResponseData](
        code=0,
        message="success",
        data=data,
        trace_id=None
    )


AuthorizationHeader = Annotated[
    str | None,
    Header(
        alias="Authorization",
        description="Vortex token 令牌，用于解析用户信息",
    )
]

class TokenAuthResponseData(BaseModel):
    token: str = Field(..., description="token")


@router.get(
    "/auth",
    response_model=StandardResponse[TokenAuthResponseData],
    summary="快速验证用户 token",
    description="从 Header 中解析 authorization 返回当前应用的用户信息",
)
async def auth(
    request: Request,
    authorization: AuthorizationHeader = None
) -> StandardResponse[TokenAuthResponseData]:
    # 调试：打印收到的 headers
    logger.info(f"收到的所有 Headers: {dict(request.headers)}")

    logger.info(f"用户 token 验证开始.")

    if not authorization:
        logger.error(f"用户 token 校验失败: Header 中无有效的 authorization")
        return ErrorResponse(
            code=400,
            message="用户 token 校验失败",
            detail="Header 中无有效的 authorization"
        )

    token = authorization.replace("Bearer ", "")
    is_existed = await is_user_existed(token)
    if not is_existed:
        logger.error(f"用户 token 校验失败: 用户不存在")
        return ErrorResponse(
            code=400,
            message="用户不存在, 请前往零信任网关完成登录",
            detail="用户不存在, 请前往零信任网关完成登录"
        )
    
    user_dict = await get_user(token)
    name = user_dict.get("name")
    username = user_dict.get("username")

    is_valid = await is_user_valid(token)
    if not is_valid:
        await update_user(token, "name", name)
        await update_user(token, "last_login", current_time)
        logger.info(f"👤 用户【{username}】已存在！更新最后一次登录时间")
        log_usage("老用户", scenario="API", user=name or username)

    logger.info(f"用户 token 验证结束.")
    data = TokenAuthResponseData(token=token)
    return StandardResponse[TokenAuthResponseData](
        code=0,
        message="success",
        data=data,
        trace_id=None
    )

