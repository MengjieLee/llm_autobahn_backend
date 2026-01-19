import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse

from app.api.router import api_router
from app.conf.config import settings
from app.conf.logging_config import setup_logging
from app.core.exceptions import (
    BizException,
    biz_exception_handler,
    generic_exception_handler,
    http_exception_handler,
)
from app.core.middleware import request_id_middleware, auth_middleware
from fastapi.middleware.cors import CORSMiddleware
from app.core.api_schema import ErrorResponse
from context.doris_connector import get_doris_connector, close_doris_connector


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """推荐的 lifespan 生命周期写法，用于记录启动 / 停止日志。"""
    logger.info(f"后台启动 | name={settings.app_name} | api_prefix={settings.api_v1_prefix}")
    doris_connector = None
    try:
        doris_connector = get_doris_connector()
        app.state.doris_connector = doris_connector
        test_result = doris_connector.test_connection()
        if test_result.get("errcode") != 0:
            logger.warning("Doris 连接预检查失败: %s", test_result)
    except Exception as exc:  # noqa: BLE001
        logger.exception("初始化 Doris 连接失败")
    try:
        yield
    finally:
        if doris_connector:
            close_doris_connector()
        logger.info(f"后台终止 | name={settings.app_name}", )


def create_app() -> FastAPI:
    """应用工厂."""
    setup_logging()

    app = FastAPI(
        title=settings.app_name,
        description="LLM Autobahn 垂类领域整合后端 API 文档",
        version=settings.version,
        openapi_url=f"{settings.api_v1_prefix}/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # 中间件（注意执行顺序：从外到内）
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 放行「所有」前端域名（开发环境推荐）
        allow_credentials=True,  # 允许前端携带Cookie/认证信息（必须开启，适配你的Bearer Token）
        allow_methods=["*"],  # 放行「所有」请求方法：GET/POST/PUT/DELETE/OPTIONS等
        allow_headers=["*"],  # 放行「所有」请求头：Authorization/Content-Type等
    )
    app.middleware("http")(request_id_middleware)  # 添加 trace_id
    app.middleware("http")(auth_middleware)  # 鉴权中间件

    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(BizException, biz_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)

    # Pydantic 校验错误 -> 统一格式
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc: RequestValidationError):
        trace_id = getattr(request.state, "trace_id", None)

        # 记录参数校验错误日志，便于排查问题
        logger.warning(f"请求参数校验失败 | path={request.url.path} | trace_id={trace_id} | errors={exc.errors()}")

        return JSONResponse(
            status_code=422,
            content=ErrorResponse(
                code=422,
                message="请求参数校验失败",
                detail=exc.errors(),
                trace_id=trace_id,
            ).model_dump(),
        )

    # 注册路由（按领域模块划分）
    app.include_router(api_router, prefix=settings.api_v1_prefix)

    @app.get(
        "/health",
        summary="健康检查",
        tags=["system"],
    )
    async def health_check():
        return {"status": "ok"}

    return app

app = create_app()
