"""
数据管理应用的常量定义文件
"""
from pathlib import Path


CREDENTIAL_FILE_PATH = Path("./credentials.txt")
DATA_VORTEX_LS_TOKEN_ID = "data_vortex_auth_token"
DATA_VORTEX_LS_JWT_ID = "data_vortex_jwt"
VERSION = "3.0.0"


# 图片相关字段
ABSOLUTE_IMAGE_FIELDS = [
    "absolute_image",
    "absolute_images",
    "video_images",  # 视频图片
    "abs_jpgs",
    "medium_path",
    "abs_videos",
]

IMAGE_FIELDS = [
    "image",   # 单张图片
    "images",  # 多张图片
    "jpgs",
]

RELATIVE_IMAGE_FIELDS = [
    "relative_image",  # 相对路径图片
    "relative_images",  # 相对路径图片
]

# 视频相关字段
ABSOLUTE_VIDEO_FIELDS = [
    "absolute_video",
    "absolute_videos",
    "bos_video_path",  # BOS存储的视频路径
]

VIDEO_FIELDS = [
    "video",       # 视频
]

RELATIVE_VIDEO_FIELDS = [
    "relative_video",  # 相对路径视频
    "relative_videos",  # 相对路径视频
]

# 音频相关字段
ABSOLUTE_AUDIO_FIELDS = [
    "absolute_audio",
    "absolute_audios",
    "bos_audio_path",  # BOS 存储的音频路径
]

AUDIO_FIELDS = [
    "audio",       # 音频
]

RELATIVE_AUDIO_FIELDS = [
    "relative_audio",  # 相对路径音频
    "relative_audios",  # 相对路径音频
]

# 图文交错格式相关字段
INTERLEAVED_CONTENT_FIELDS = [
    "content",  # 图文交错内容
    "content_path",
]

# 对话相关字段
CONVERSATION_FIELDS = [
    "conversations",  # 对话内容
    "query",          # 查询
    "response",       # 响应
    "caption",        # 描述文字
    "global_caption", # video的描述文字
    "messages",       # 对话内容
    "json.conversations"
]

# 元数据相关字段
METADATA_FIELDS = [
    "meta_data",      # 元数据
    "metadata",       # 元数据(另一种写法)
    "id",             # 标识符
]

# 要在详情中显示的常见字段
COMMON_FIELDS = ABSOLUTE_IMAGE_FIELDS + IMAGE_FIELDS + RELATIVE_IMAGE_FIELDS + \
    ABSOLUTE_AUDIO_FIELDS + VIDEO_FIELDS + RELATIVE_VIDEO_FIELDS + \
        ABSOLUTE_AUDIO_FIELDS + AUDIO_FIELDS + RELATIVE_AUDIO_FIELDS + \
            CONVERSATION_FIELDS + INTERLEAVED_CONTENT_FIELDS + ["tags", "id"]

# 要在expander中显示的字段
EXPANDER_FIELDS = METADATA_FIELDS

# 数据格式类型
DATA_FORMAT_TYPES = {
    "STANDARD": "standard",      # 标准格式（独立的图片、视频字段）
    "INTERLEAVED": "interleaved" # 图文交错格式（content字段包含<image>标记）
}

PRIORITY_FIELDS_LIST = ["conversations", "conversations_tokens", "id"]
COMMENT_PATTERN = r'^-- 选中的表中为 .*$'
VORTEX_SQL_PAGE_NAME = "SQL 数据透视"
SQL_TEMPALTES = [
            {
                "name": "查询 tabel 10条记录",
                "description": "SELECT * FROM qianfan_bos_catalog.all_data.infovqa_v1 LIMIT 10;",
                "sql": "SELECT * FROM qianfan_bos_catalog.all_data.infovqa_v1 LIMIT 10;"
            },
            {
                "name": "统计 Token 数分箱",
                "description": 
"""
SELECT
  CONCAT(FLOOR(conversations_tokens / 5) * 5, '-', FLOOR(conversations_tokens / 5) * 5 + 4) AS token_range,
  COUNT(*) AS cnt
FROM qianfan_bos_catalog.all_data.infovqa_v1
GROUP BY FLOOR(conversations_tokens / 5)
ORDER BY FLOOR(conversations_tokens / 5)
""",
                "sql": 
"""
SELECT
  CONCAT(FLOOR(conversations_tokens / 5) * 5, '-', FLOOR(conversations_tokens / 5) * 5 + 4) AS token_range,
  COUNT(*) AS cnt
FROM qianfan_bos_catalog.all_data.infovqa_v1
GROUP BY FLOOR(conversations_tokens / 5)
ORDER BY FLOOR(conversations_tokens / 5)
"""
            },
            {
                "name": "查询 table 结构",
                "description": "SHOW COLUMNS FROM qianfan_bos_catalog.all_data.infovqa_v1;",
                "sql": "SHOW COLUMNS FROM qianfan_bos_catalog.all_data.infovqa_v1;"
            },
            {
                "name": "查询 catalog 列表",
                "description": "SHOW DATABASES;",
                "sql": "SHOW DATABASES;"
            }
        ]

# 权限组列表
DEFAULT_GROUPS = []
GROUP_LIST = [
    "official",
    "group_a",
    "group_b",
    "group_c"
]
UNMODIFIABLE_GROUP = "official"
BADGE_MAPPING = {
    "official": ":violet-badge[👑 official]",
    "group_b": ":blue-badge[🎩 group_b]",
    "group_a": ":green-badge[🎓 group_a]",
    "group_c": ":yellow-badge[🎓 group_c]"
}

TIME_FORMAT = "%Y-%m-%d: %H:%M:%S"
