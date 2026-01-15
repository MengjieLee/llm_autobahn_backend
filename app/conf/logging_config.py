import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from app.conf.config import settings


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
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=datefmt))

    # 清理旧 handler，避免重复
    if root_logger.handlers:
        for h in list(root_logger.handlers):
            root_logger.removeHandler(h)

    root_logger.addHandler(file_handler)
