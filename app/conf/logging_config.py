import logging
import os
import time
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

from app.conf.config import settings
from app.core.request_context import ctx_get_username, ctx_get_trace_id

# 北京时间 (UTC+8)，供 Formatter.converter 使用
_BJT = timezone(timedelta(hours=8))


def _bjt_time(timestamp=None):
    """将 UNIX 时间戳转换为北京时间 time.struct_time，替代 time.localtime"""
    dt = datetime.fromtimestamp(timestamp or time.time(), tz=_BJT)
    return dt.timetuple()


class ContextFormatter(logging.Formatter):
    """自定义日志格式化器，自动从请求上下文提取 username 和 trace_id 并添加到日志消息中。"""

    converter = _bjt_time  # 强制北京时间

    def format(self, record: logging.LogRecord) -> str:
        """格式化日志记录，自动添加上下文信息。"""
        # 从上下文获取 username 和 trace_id
        username = ctx_get_username()
        trace_id = ctx_get_trace_id()
        
        # 获取原始消息
        original_msg = record.getMessage()
        
        # 构建需要添加的上下文信息
        context_parts = []
        # 检查消息中是否已包含 username，如果没有则添加
        if username and f"username={username}" not in original_msg:
            context_parts.append(f"username={username}")
        # 检查消息中是否已包含 trace_id，如果没有则添加
        if trace_id and f"trace_id={trace_id}" not in original_msg:
            context_parts.append(f"trace_id={trace_id}")
        
        # 如果存在需要添加的上下文信息，将其添加到日志消息前
        if context_parts:
            context_str = " | ".join(context_parts)
            record.msg = f"{context_str} | {record.msg}"
        
        return super().format(record)


class _ScrollFilter(logging.Filter):
    """屏蔽 elasticsearch scroll 相关日志"""

    def filter(self, record):
        if record.name.startswith("elasticsearch"):
            return "_search/scroll" not in record.getMessage()
        return True


class _PollingFilter(logging.Filter):
    """屏蔽前端高频轮询的 access log（/kv/status/）"""

    def filter(self, record):
        msg = record.getMessage()
        if "/kv/status/" in msg or "/kv/qpd" in msg:
            return False
        return True


def setup_logging() -> None:
    """配置日志：app.log 按天轮转，es_query.log 按大小轮转。"""
    os.makedirs(settings.log_dir, exist_ok=True)

    log_format = (
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(filename)s:%(lineno)d | %(message)s"
    )
    datefmt = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level)

    # ---- App 日志：按天轮转 ----
    log_path = os.path.join(settings.log_dir, settings.log_file_name)
    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(ContextFormatter(log_format, datefmt=datefmt))
    file_handler.addFilter(_ScrollFilter())

    # 清理旧 handler，避免重复
    if root_logger.handlers:
        for h in list(root_logger.handlers):
            root_logger.removeHandler(h)
    root_logger.addHandler(file_handler)

    # ---- ES 查询专用日志：按大小轮转 ----
    os.makedirs(settings.es_log_dir, exist_ok=True)
    es_log_file = os.path.join(settings.es_log_dir, settings.es_log_file_name)
    es_logger = logging.getLogger("es_query")
    es_logger.setLevel(logging.DEBUG)
    es_logger.propagate = False

    es_file_handler = RotatingFileHandler(
        es_log_file,
        maxBytes=settings.es_log_max_bytes,
        backupCount=settings.es_log_backup_count,
        encoding="utf-8",
    )
    es_file_handler.setFormatter(ContextFormatter(log_format, datefmt=datefmt))
    for h in list(es_logger.handlers):
        es_logger.removeHandler(h)
    es_logger.addHandler(es_file_handler)

    # elasticsearch 库日志
    es_lib_logger = logging.getLogger("elasticsearch")
    es_lib_handler = RotatingFileHandler(
        es_log_file,
        maxBytes=settings.es_log_max_bytes,
        backupCount=settings.es_log_backup_count,
        encoding="utf-8",
    )
    es_lib_handler.setFormatter(ContextFormatter(log_format, datefmt=datefmt))
    es_lib_handler.addFilter(_ScrollFilter())
    es_lib_logger.addHandler(es_lib_handler)

    # ---- 使用统计日志（长期追加，按大小轮转）----
    os.makedirs(settings.usage_log_dir, exist_ok=True)
    usage_log_file = os.path.join(settings.usage_log_dir, settings.usage_log_file_name)
    usage_logger = logging.getLogger("usage")
    usage_logger.setLevel(logging.INFO)
    usage_logger.propagate = False

    usage_handler = RotatingFileHandler(
        usage_log_file,
        maxBytes=settings.usage_log_max_bytes,
        backupCount=settings.usage_log_backup_count,
        encoding="utf-8",
    )
    usage_handler.setFormatter(logging.Formatter("%(message)s"))
    for h in list(usage_logger.handlers):
        usage_logger.removeHandler(h)
    usage_logger.addHandler(usage_handler)

    # ---- 屏蔽轮询 access log ----
    logging.getLogger("uvicorn.access").addFilter(_PollingFilter())
