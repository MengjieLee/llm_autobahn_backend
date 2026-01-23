import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from app.core.request_context import ctx_get_trace_id
from context.doris_connector import DorisConnectorPydoris, get_doris_connector
from src.serializers.data_serializer import doris_data_2_json


logger = logging.getLogger(__name__)
router = APIRouter()


class SQLQueryReqeust(BaseModel):
    sql: str = Field(..., description="sql 语句")


@router.post("/sql_query", summary="启动 sql 在线查询任务")
async def sql_query(
    body: SQLQueryReqeust, doris_svc: DorisConnectorPydoris = Depends(get_doris_connector)
) -> StandardResponse[dict]:
    logger.info(f"sql_query 请求: {body.sql}")
    query_raw_dict = await doris_svc.execute_custom_sql(body.sql)
    logger.debug(f"query_raw_dict 原生结果: {query_raw_dict}")
    query_json = doris_data_2_json(query_raw_dict.get("data", []))
    message = "success"
    trace_id = None
    if not query_json:
        message = query_raw_dict.get("message")
        trace_id = ctx_get_trace_id()
    logger.debug(f"sql_query 结果: {query_json}")
    data = {"result": query_json}
    return StandardResponse(code=0, message=message, data=data, trace_id=trace_id)
