from fastapi import APIRouter

from app.api.v1.account import router as account_router
from app.api.v1.domain_integration import router as domain_integration_router


api_router = APIRouter()

# 垂类领域整合相关接口
api_router.include_router(account_router, prefix="/account", tags=["account"])
api_router.include_router(domain_integration_router, prefix="/domain", tags=["domain_integration"])

