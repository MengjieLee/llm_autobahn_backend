from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """全局配置."""

    app_name: str = "LLM Autobahn Backend"
    version: str = "0.1.0"
    api_v1_prefix: str = "/api/v1"
    debug: bool = True

    # 日志
    log_dir: str = "logs"
    log_file_name: str = "app.log"
    log_max_bytes: int = 100 * 1024 * 1024  # 100MB
    log_backup_count: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

