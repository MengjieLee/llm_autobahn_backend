"""请求上下文管理模块，使用 contextvars 存储请求级别的上下文信息。"""
import logging
from contextvars import ContextVar
from datetime import datetime, timezone, timedelta
from typing import Optional

# 定义上下文变量
_username: ContextVar[Optional[str]] = ContextVar("username", default=None)
_trace_id: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)

_BJT = timezone(timedelta(hours=8))
_usage_logger = logging.getLogger("usage")


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


def log_usage(action: str, scenario: str = "API", user: Optional[str] = None) -> None:
    """
    写入使用统计日志，格式兼容 legacy:
      {time} | {user} | {scenario} | {action}

    :param action:   操作描述，如 "list_datasets"、"SFT数据管理"
    :param scenario: 场景标识，"UI" / "API" / 自定义
    :param user:     用户名；为 None 时自动从请求上下文取
    """
    if user is None:
        user = ctx_get_username() or "-"
    time_str = datetime.now(_BJT).strftime("%Y-%m-%d %H:%M:%S")
    _usage_logger.info(f"{time_str} | {user} | {scenario} | {action}")
