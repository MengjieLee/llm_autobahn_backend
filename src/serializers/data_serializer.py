import json
import logging
from typing import List, Dict, Any, Optional

from app.conf.config import settings
from context.file_system import fs_manager

logger = logging.getLogger(__name__)


def doris_data_2_json(raw_data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """
    处理 Doris 格式的数据，将字符串形式的 JSON 字段反序列化为 Python 对象
    
    Args:
        raw_data: 原始数据列表，其中部分字段是 JSON 字符串格式
        
    Returns:
        处理后的字典列表，如果解析失败返回 None
        
    Raises:
        ValueError: 当输入数据格式不符合预期时
    """
    processed_data = []

    if not isinstance(raw_data, list):
        logger.warning("输入数据必须是列表类型")
        return processed_data
    
    exist_medium_fields = []
    raw_data_keys = raw_data[0].keys()
    for field in settings.medium_fields:
        if field in raw_data_keys:
            exist_medium_fields.append(field)

    logger.info(f"存在需要解析的媒体字段: {exist_medium_fields}")
    for item in raw_data:
        try:
            if not isinstance(item, dict):
                logger.warning(f"数据不是字典类型，跳过处理: {item}")
                continue
        
            processed_item = item.copy()
            for field in settings.parse_json_fields:
                if field in processed_item and processed_item[field] is not None and not isinstance(processed_item[field], list):
                    try:
                        processed_item[field] = json.loads(processed_item[field])

                    except json.JSONDecodeError as e:
                        logger.warning(f"字段 {field} 解析失败: {e}, 数据项ID: {item.get('id', '未知ID')}")
                        # 解析失败时保留原始值，不中断整体处理
                        processed_item[field] = item[field]
            
            if exist_medium_fields:
                for medium_field in exist_medium_fields:
                    processed_medium_item = processed_item.get(medium_field).copy()
                    presigned_urls = []
                    for abs_bos_url in processed_medium_item:
                        presigned_urls.append(fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60))
                    if presigned_urls:
                        processed_item[medium_field] = presigned_urls
            
            processed_data.append(processed_item)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析错误: {e}, 数据项: {item.get('id', '未知ID')}")
            return processed_data
        except Exception as e:
            logger.exception(f"处理数据时发生错误: {e}, 数据项: {item.get('id', '未知ID')}")
            return processed_data
    
    return processed_data


if __name__ == "__main__":
    raw_data = {}
    print(doris_data_2_json())
