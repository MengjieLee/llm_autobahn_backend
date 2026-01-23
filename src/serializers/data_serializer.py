import ast
import json
import logging
import os
from typing import List, Dict, Any, Optional

from app.conf.config import settings
from context.file_system import fs_manager

logger = logging.getLogger(__name__)


def safe_json_loads(json_str: str) -> Any:
    """
    安全解析JSON字符串，兼容单引号格式，同时保留值中的单引号
    
    Args:
        json_str: 可能是非标准的JSON字符串
        
    Returns:
        解析后的Python对象，解析失败返回原始字符串
    """
    if not isinstance(json_str, str):
        return json_str
    
    # 先尝试直接解析标准JSON
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    
    # 方案1：修复键名的单引号（只替换 {/[, 后和 : 前的单引号）
    import re
    # 匹配键名的单引号（例如 'name': -> "name":）
    fixed_str = re.sub(r"(?<=[{,])\s*'([^']+?)'\s*:", r'"\1":', json_str)
    # 匹配值的单引号（如果是简单值，例如: '张三' -> "张三"，但跳过包含转义的情况）
    fixed_str = re.sub(r":\s*'([^'\\]*)'\s*(?=[,}])", r':"\1"', fixed_str)
    
    try:
        return json.loads(fixed_str)
    except json.JSONDecodeError:
        pass
    
    # 方案2：使用ast.literal_eval（安全的Python字面量解析）
    try:
        # 防止恶意代码，先做简单校验
        if not json_str.strip().startswith(('{', '[')):
            raise ValueError("不是字典/列表格式")
        return ast.literal_eval(json_str)
    except (SyntaxError, ValueError) as e:
        logger.warning(f"终极解析方案失败: {e}, 原始字符串: {json_str[:100]}")
        return json_str

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
        logger.warning("序列化输入数据必须是列表类型")
        return processed_data

    if not raw_data:
        logger.warning("序列化输入数据为空")
        return processed_data
    
    exist_medium_fields = []
    src_root_path = ""
    backup_medium_field = ""

    raw_data_keys = raw_data[0].keys()
    logging.debug(f"待处理的数据有这些字段: {raw_data_keys}")
    for field in raw_data_keys:
        if field in settings.medium_fields:
            exist_medium_fields.append(field)
            logger.debug(f"存在需要解析的媒体字段: {exist_medium_fields}")
        if not src_root_path and field in settings.src_root_fields:
            src_root_path = field
            logger.debug(f"存在根路径字段: {src_root_path}")
        if not backup_medium_field and field in settings.backup_fields:
            backup_medium_field = field
            logger.debug(f"存在备用媒体路径字段: {backup_medium_field}")

    for item in raw_data:
        try:
            if not isinstance(item, dict):
                logger.warning(f"数据不是字典类型，跳过处理: {item}")
                continue
        
            processed_item = item.copy()
            for field in settings.parse_json_fields:
                field_value = processed_item.get(field)
                if field_value is not None and not isinstance(field_value, list):
                    parsed_field_value = safe_json_loads(field_value)
                    processed_item[field] = parsed_field_value
            
            if exist_medium_fields:
                for medium_field in exist_medium_fields:
                    presigned_urls = []

                    processed_medium_item = processed_item.get(medium_field).copy()
                    for abs_bos_url in processed_medium_item:
                        try:
                            if not any(abs_bos_url.startswith(s3_prefix) for s3_prefix in settings.s3_prefixes) and src_root_path and processed_item[src_root_path]:
                                abs_bos_url = os.path.join(processed_item[src_root_path], abs_bos_url)
                            abs_bos_url_signed = fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60)
                        except ValueError:
                            logger.debug(f"URL 生成预签名失败: {abs_bos_url}")
                            continue
                        if abs_bos_url_signed: presigned_urls.append(abs_bos_url_signed)
                    
                    if not presigned_urls:
                        if backup_medium_field and processed_item[backup_medium_field]:
                            processed_medium_item = processed_item.get(backup_medium_field).copy()
                            for abs_bos_url in processed_medium_item:
                                try:
                                    if not any(abs_bos_url.startswith(s3_prefix) for s3_prefix in settings.s3_prefixes): abs_bos_url = f"{settings.s3_default_prefix}{abs_bos_url}"
                                    abs_bos_url_signed = fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60)
                                except ValueError:
                                    logger.exception(f"备用 URL 生成预签名失败: {abs_bos_url}")
                                    continue
                                if abs_bos_url_signed: presigned_urls.append(abs_bos_url_signed)

                    if presigned_urls: processed_item[medium_field] = presigned_urls
            
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
