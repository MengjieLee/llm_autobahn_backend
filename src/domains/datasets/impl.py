import logging
import os
import httpx

from datetime import datetime 
from dotenv import load_dotenv
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict

load_dotenv()
META_DATA_HOST = os.getenv("META_DATA_HOST")

logger = logging.getLogger(__name__)


# ----- 枚举类型定义 -----
class StageEnum(str, Enum):
    sft = "SFT"
    pretrain = "Pretrain"
    dpo = "DPO"
    other = "Other"


class LabelDefinitionCreate(BaseModel):
    """标签定义"""

    name: str  # 如"modality", "language"等
    description: Optional[str] = None
    values: List[str]  # 可选值列表


class LabelDefinition(BaseModel):
    """标签定义"""

    name: str  # 如"modality", "language"等
    description: Optional[str] = None
    values: List[str]  # 可选值列表
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class DatasetLabel(BaseModel):
    """数据集标签关联"""

    label_name: str
    label_values: List[str]  # 选中的值列表


class DatasetList(BaseModel):
    labels: Optional[List[DatasetLabel]] = None
    stages: Optional[List[StageEnum]] = None
    dataset_name_keywords: Optional[str] = None
    tags: Optional[List[str]] = None
    tables: Optional[List[str]] = None
    groups: Optional[List[str]] = None

    model_config = {"use_enum_values": True}


class DatasetCreate(BaseModel):
    name: str
    source: str
    is_open_source: bool
    size: Optional[str] = None
    count: Optional[int] = None
    src_paths: Optional[List[str]] = None
    converted_paths: Optional[List[str]] = None
    converted_preview_paths: Optional[List[str]] = None
    media_root_dir: Optional[str] = None
    stages: List[StageEnum]
    parent_id: Optional[str] = None
    process_note: Optional[str] = None
    tags: Optional[List[str]] = []
    labels: Optional[List[DatasetLabel]] = None  # 使用统一标签系统
    registrant: str
    groups: Optional[List[str]] = []


class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    source: Optional[str] = None
    is_open_source: Optional[bool] = None
    size: Optional[str] = None
    count: Optional[int] = None
    src_paths: Optional[List[str]] = None
    converted_paths: Optional[List[str]] = None
    converted_preview_paths: Optional[List[str]] = None
    media_root_dir: Optional[str] = None
    stages: Optional[List[StageEnum]] = None
    parent_id: Optional[str] = None
    process_note: Optional[str] = None
    tags: Optional[List[str]] = None
    labels: Optional[List[DatasetLabel]] = None
    registrant: Optional[str]
    groups: Optional[List[str]] = []


class Dataset(BaseModel):
    id: str = Field(alias="_id")
    name: str
    source: Optional[str] = None
    is_open_source: Optional[bool] = None
    size: Optional[str] = None
    count: Optional[int] = None
    src_paths: Optional[List[str]] = None
    converted_paths: Optional[List[str]] = None
    converted_preview_paths: Optional[List[str]] = None
    media_root_dir: Optional[str] = None
    stages: List[StageEnum]
    parent_id: Optional[str] = None
    process_note: Optional[str] = None
    tags: Optional[List[str]] = None
    labels: Optional[List[DatasetLabel]] = None
    registrant: str
    created_at: datetime
    updated_at: datetime


class DatasetsClient:
    """底层 HTTP Client，仅负责请求发送和响应解析。"""

    def __init__(self, host: str = META_DATA_HOST, auth_token: str = ""):
        if not host:
            raise ValueError("META_DATA_HOST 未配置")

        headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
        self.client = httpx.Client(
            base_url=host,
            headers=headers,
            timeout=20.0,
        )

    def list_datasets(self, filter: Optional[DatasetList] = None) -> List[Dict]:
        if filter:
            # 在不动 mtdata 代码基础上而作的特判去适配 groups 为空列表的情况返回空的数据集
            filter_dict = filter.model_dump()
            if "groups" in filter_dict and not filter_dict["groups"]:
                filter_dict["groups"] = ['']
            filter_dict = {k: v for k, v in filter_dict.items() if v is not None}
            logger.debug(f"filter: {filter_dict}")
            resp = self.client.post(
                # "/datasets/list", json=filter.model_dump(exclude_none=True)
                "/datasets/list", json=filter_dict
            )
        else:
            resp = self.client.get("/datasets")
        resp.raise_for_status()
        return resp.json()

    
    