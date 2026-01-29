from pathlib import Path, PosixPath
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
    log_level: str = "INFO" # DEBUG
    TIME_FORMAT: str = "%Y-%m-%d: %H:%M:%S"

    # 账号白名单
    CREDENTIAL_FILE_PATH: PosixPath = Path("/mnt/cfs_bj_mt/workspace/chenjieting/iCodes/baidu/personal-code/data_management_app/credentials.txt")

    # 权限组列表
    DEFAULT_GROUPS: list[str] = []
    GROUP_LIST: list[str] = [
        "official",
        "group_a",
        "group_b",
        "group_c"
    ]
    UNMODIFIABLE_GROUP: str = "official"

    # 日志
    log_dir: str = "logs"
    log_file_name: str = "app.log"
    log_max_bytes: int = 100 * 1024 * 1024  # 100MB
    log_backup_count: int = 10


    ## Doris 连接配置
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

    # S3
    access_key: str = Field(default="")
    secret_key: str = Field(default="")
    session_token: str = Field(default="")
    endpoint: str = Field(default="")
    region: str = Field(default="")

    # 序列化相关
    s3_prefixes: list[str] = ["http", "bos", "s3"]
    bucket_name: str = "llm-data-process"
    s3_default_prefix: str = f"bos://{bucket_name}"
    medium_fields: list[str] = ["image", "images", "video", "videos", "audio", "audios"]
    backup_fields: list[str] = ["absolute_images", "absolute_image", "absolute_videos", "absolute_video", "absolute_audios", "absolute_audio"]
    parse_json_fields: list[str] = medium_fields + backup_fields + ["relative_image", "relative_video", "relative_audio", "conversations", "meta_data"]
    src_root_fields: list[str] = ["src_root_path"]

settings = Settings()
