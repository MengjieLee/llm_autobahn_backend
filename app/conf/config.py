from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置."""

    # Pydantic v2 配置
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # 忽略未声明的环境变量，避免报 Extra inputs
    )

    app_name: str = "LLM Autobahn Backend"
    version: str = "0.1.0"
    api_v1_prefix: str = "/api/v1"
    debug: bool = True

    # 日志
    log_dir: str = "logs"
    log_file_name: str = "app.log"
    log_max_bytes: int = 100 * 1024 * 1024  # 100MB
    log_backup_count: int = 10

    # Doris 默认配置（可通过环境变量覆盖）
    # 对应的环境变量名：DEFAULT_DORIS_HOST / DEFAULT_DORIS_PORT / ...
    DEFAULT_DORIS_HOST: str
    DEFAULT_DORIS_PORT: int
    DEFAULT_DORIS_USER: str
    DEFAULT_DORIS_PASSWORD: str
    DEFAULT_DORIS_CATALOG: str
    DEFAULT_DORIS_DATABASE: str


settings = Settings()
