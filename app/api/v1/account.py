from typing import Any, List, Annotated
import hashlib
import logging
import jwt

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

from app.conf.config import settings
from app.core.api_schema import StandardResponse, ErrorResponse
from context.auth_client import add_or_update_user


logger = logging.getLogger(__name__)
router = APIRouter()


def generate_auth_token(username):
    """生成认证令牌"""
    # 使用时间戳和随机数生成唯一令牌
    unique_string = f"{username}"
    return hashlib.sha256(unique_string.encode()).hexdigest()


XZtAuthorizationHeader = Annotated[
    str | None,
    Header(
        alias="X-Zt-Authorization",
        description="UUAP网关JWT令牌，用于解析用户信息",
    )
]


class AccountModel(BaseModel):
    name: str = Field(..., description="中文名")
    username: str = Field(..., description="邮箱前缀")
    token: str = Field(..., description="唯一标识符")
    groups: List[str] = Field(default=[], description="权限组")


class AccountResponseData(BaseModel):
    user: AccountModel = Field(..., description="用户信息的字典")


@router.post(
    "/login",
    response_model=StandardResponse[AccountResponseData],
    summary="携带 uuap 网关 jwt 的登录接口",
    description="解析 jwt，返回当前应用的用户信息",
)
async def login(
    x_zt_authorization: XZtAuthorizationHeader = None
) -> StandardResponse[AccountResponseData]:
    logger.info(f"UUAP 网关验证开始.")

    if not x_zt_authorization:
        logger.error(f"JWT 校验失败: Header 中无有效的 X-Zt-Authorization 键值")
        return ErrorResponse(
            code=400,
            message="JWT 校验失败",
            detail="Header 中无有效的 X-Zt-Authorization 键值"
        )

    jwt_decoded = jwt.decode(x_zt_authorization, options={"verify_signature": False})
    name = jwt_decoded.get("name")
    username = jwt_decoded.get("username")
    token = generate_auth_token(username)
    user_dict = await add_or_update_user(token, username, settings.DEFAULT_GROUPS, name)
    data = AccountResponseData(user=AccountModel(
        name=user_dict.get("name"),
        username=user_dict.get("username"),
        token=user_dict.get("token"),
        groups=user_dict.get("groups"),
    ))
    logger.info(f"UUAP 网关验证结束.")
    return StandardResponse[AccountResponseData](
        code=0,
        message="success",
        data=data,
        trace_id=None
    )
