from fastapi import APIRouter

from app.api.v1.account import router as account_router
from app.api.v1.process_scheduler import router as process_scheduler_router
from app.api.v1.sql import router as sql_router


api_router = APIRouter()

# 垂类领域整合相关接口
api_router.include_router(account_router, prefix="/account", tags=["account"])
api_router.include_router(process_scheduler_router, prefix="/process_scheduler", tags=["process_scheduler"])
api_router.include_router(sql_router, prefix="/sql", tags=["sql"])

