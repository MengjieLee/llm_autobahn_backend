import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from app.conf.config import settings
from app.core.request_context import ctx_get_username, ctx_get_trace_id


class ContextFormatter(logging.Formatter):
    """自定义日志格式化器，自动从请求上下文提取 username 和 trace_id 并添加到日志消息中。"""

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


def setup_logging() -> None:
    """配置日志，单个日志文件不超过 5MB，滚动备份，文件名带日期时间戳。"""
    os.makedirs(settings.log_dir, exist_ok=True)

    # 日志文件增加日期前缀，例如 20260115_1917_app.log
    date_prefix = datetime.now().strftime("%Y%m%d_%H%M")
    log_file_name = f"{date_prefix}_{settings.log_file_name}"
    log_path = os.path.join(settings.log_dir, log_file_name)

    log_format = (
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(filename)s:%(lineno)d | %(message)s"
    )
    datefmt = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 文件轮转
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(ContextFormatter(log_format, datefmt=datefmt))

    # 清理旧 handler，避免重复
    if root_logger.handlers:
        for h in list(root_logger.handlers):
            root_logger.removeHandler(h)

    root_logger.addHandler(file_handler)
