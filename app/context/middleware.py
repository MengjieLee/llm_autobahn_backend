import time
import uuid
from typing import Callable

from fastapi import Request, Response


async def request_id_middleware(request: Request, call_next: Callable) -> Response:
    """为每个请求注入 trace_id，便于日志与响应关联。"""
    trace_id = request.headers.get("X-Trace-Id") or uuid.uuid4().hex
    request.state.trace_id = trace_id

    start = time.time()
    response = await call_next(request)
    cost = (time.time() - start) * 1000

    response.headers["X-Trace-Id"] = trace_id
    response.headers["X-Response-Time-ms"] = f"{cost:.2f}"
    return response

