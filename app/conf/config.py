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
    version: str = "0.1.1"
    api_v1_prefix: str = "/api/v1"
    debug: bool = True

    # 日志
    log_dir: str = "logs"
    log_file_name: str = "app.log"
    log_max_bytes: int = 100 * 1024 * 1024  # 100MB
    log_backup_count: int = 10

    # Doris 默认配置（可通过环境变量覆盖）
    # 对应的环境变量名：DEFAULT_DORIS_HOST / DEFAULT_DORIS_PORT / ...
    #
    # 说明：
    # - 这里提供默认值，避免在未配置 Doris 的环境中（例如本地 docker run）应用直接启动失败
    # - 真实连接由 docker-compose / k8s / .env 注入覆盖
    DEFAULT_DORIS_HOST: str = Field(default="")
    DEFAULT_DORIS_PORT: int = Field(default=0)
    DEFAULT_DORIS_USER: str = Field(default="")
    DEFAULT_DORIS_PASSWORD: str = Field(default="")
    DEFAULT_DORIS_CATALOG: str = Field(default="")
    DEFAULT_DORIS_DATABASE: str = Field(default="")

    @property
    def doris_configured(self) -> bool:
        """
        Doris 连接是否已配置齐全。
        password 允许为空；host/port/user/catalog/database 必须存在。
        """
        return bool(
            self.DEFAULT_DORIS_HOST
            and int(self.DEFAULT_DORIS_PORT or 0) > 0
            and self.DEFAULT_DORIS_USER
            and self.DEFAULT_DORIS_PASSWORD
            and self.DEFAULT_DORIS_CATALOG
            and self.DEFAULT_DORIS_DATABASE
        )


settings = Settings()
