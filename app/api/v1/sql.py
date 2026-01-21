import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.api_schema import StandardResponse
from context.doris_connector import DorisConnectorPydoris, get_doris_connector
from context.file_system import fs_manager


logger = logging.getLogger(__name__)
router = APIRouter()


class SQLQueryReqeust(BaseModel):
    sql: str = Field(..., description="sql 语句")


@router.post("/sql_query", summary="启动 sql 在线查询任务")
async def sql_query(
    body: SQLQueryReqeust, doris_svc: DorisConnectorPydoris = Depends(get_doris_connector)
) -> StandardResponse[dict]:
    logger.info(f"sql_query 开始: {body.sql}")
    sql_result = await doris_svc.execute_custom_sql(body.sql)
    # sql_result = {"test": fs_manager.generate_presigned_url("bos://llm-data-process/dataset/medias/ocr/infographic-vqa/infographicsvqa_images/20471.jpeg")}
    sql_result = {
        "test1": fs_manager.exists("bos://llm-data-process/dataset/medias/ocr/infographic-vqa/infographicsvqa_images/20471.jpeg"),
        "test2": fs_manager.exists("/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/context/file_system/s3.py"),
        "test3": fs_manager.exists("/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/credentials666.txt")
    }
    logger.info(f"sql_query 结束: {sql_result}")
    return StandardResponse(code=0, message="success", data=sql_result, trace_id=None)
