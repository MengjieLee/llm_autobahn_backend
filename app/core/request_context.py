"""请求上下文管理模块，使用 contextvars 存储请求级别的上下文信息。"""
from contextvars import ContextVar
from typing import Optional

# 定义上下文变量
_username: ContextVar[Optional[str]] = ContextVar("username", default=None)
_trace_id: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)


def ctx_set_username(username: Optional[str]) -> None:
    """设置当前请求的用户名到上下文。"""
    _username.set(username)


def ctx_get_username() -> Optional[str]:
    """从上下文获取当前请求的用户名。"""
    return _username.get()


def ctx_set_trace_id(trace_id: Optional[str]) -> None:
    """设置当前请求的 trace_id 到上下文。"""
    _trace_id.set(trace_id)


def ctx_get_trace_id() -> Optional[str]:
    """从上下文获取当前请求的 trace_id。"""
    return _trace_id.get()
