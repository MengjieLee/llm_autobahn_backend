import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from src.domain.process_scheduler.svc import ProcessSchedulerService
from context.doris_connector import DorisConnectorPydoris, get_doris_connector

logger = logging.getLogger(__name__)
router = APIRouter()


class SQLQueryReqeust(BaseModel):
    sql: str = Field(..., description="sql 语句")


@router.post("/sql_query", summary="启动 sql 在线查询任务")
async def sql_query(
    body: SQLQueryReqeust, doris_svc: DorisConnectorPydoris = Depends(get_doris_connector)
) -> StandardResponse[dict]:
    sql_result = await doris_svc.execute_custom_sql(body.sql)
    return StandardResponse(code=0, message="success", data=sql_result, trace_id=None)
