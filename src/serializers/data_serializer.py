import ast
import json
import logging
import os
from pathlib import Path
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

def preview_serializer(raw_data: List[Dict[str, Any]], media_root: str = "") -> Optional[List[Dict[str, Any]]]:
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
    src_root_field = ""
    backup_medium_field = ""

    raw_data_keys = raw_data[0].keys()
    logging.debug(f"待处理的数据有这些字段: {raw_data_keys}")
    for field in raw_data_keys:
        if field in settings.medium_fields:
            exist_medium_fields.append(field)
            logger.debug(f"存在需要解析的媒体字段: {exist_medium_fields}")
        if not src_root_field and field in settings.src_root_fields:
            src_root_field = field
            logger.debug(f"存在根路径字段: {src_root_field}")
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
                            if not any(abs_bos_url.startswith(s3_prefix) for s3_prefix in settings.s3_prefixes) and media_root:
                                abs_bos_url = os.path.join(media_root, abs_bos_url)
                            abs_bos_url_signed = fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60)
                        except ValueError:
                            try:
                                if not any(abs_bos_url.startswith(s3_prefix) for s3_prefix in settings.s3_prefixes) and src_root_field and processed_item[src_root_field]:
                                    abs_bos_url = os.path.join(processed_item[src_root_field], abs_bos_url)
                                abs_bos_url_signed = fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60)
                            except ValueError:
                                logger.debug(f"URL 生成预签名失败: {abs_bos_url}")
                                continue
                        if abs_bos_url_signed: presigned_urls.append(abs_bos_url_signed)
                    
                    if not presigned_urls:
                        if backup_medium_field and processed_item[backup_medium_field]:
                            backup_medium_item = processed_item.get(backup_medium_field).copy()
                            for backup_url in backup_medium_item:
                                try:
                                    abs_bos_url = f"{settings.s3_default_prefix}{backup_url}"
                                    if not any(backup_url.startswith(s3_prefix) for s3_prefix in settings.s3_prefixes):
                                        is_exist = fs_manager.exists(abs_bos_url)
                                        if not is_exist:
                                            fs_manager.write_bytes(abs_bos_url, fs_manager.read_bytes(backup_url))
                                    abs_bos_url_signed = fs_manager.generate_presigned_url(uri=abs_bos_url, expiration=2*24*60*60)
                                except Exception as e:
                                    logger.exception(f"备用 URL 生成预签名失败: {abs_bos_url}, detail: {e}")
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

def splits_serializer(paths, media_root=""):
    splits = {}
    for converted_preview_path in paths:
        records = []
        flag = True
        depth = 0
        while flag and depth < 5:
            try:
                file_reader = fs_manager.open_read_stream(converted_preview_path)
                flag = False
            except IsADirectoryError as e:
                logger.warning(f"检测到目录:{converted_preview_path}, 选择第一个叶子文件处理")
                converted_preview_path = fs_manager.listdir(converted_preview_path)[0].path
                depth += 1
        with file_reader as f:
            idx = 0
            while idx < 100:
                line = f.readline().strip()
                if not line:
                     continue
                record = json.loads(line)
                records.append(record)
                idx += 1
        splits.update({
            Path(converted_preview_path).stem: preview_serializer(records, media_root)
        })
    return splits

if __name__ == "__main__":
    # uri = "bos:/llm-data-process/mnt/cfs_bj_mt/workspace/zhangying64/data_etl/ocr/medieval_media/images/dev/L-Cas.C-14.S-Hyb.Paris__BnF__esp__33.H-ebbcb.Part-0/0.png"
    # is_exist = fs_manager.exists(uri)
    backup_url = "/mnt/cfs_bj_mt/workspace/zhangying64/data_etl/ocr/medieval_media/images/dev/L-Cas.C-14.S-Hyb.Paris__BnF__esp__33.H-ebbcb.Part-0/0.png"
    abs_bos_url = f"bos:/llm-data-process{backup_url}"
    fs_manager.write_bytes(abs_bos_url, fs_manager.read_bytes(backup_url))
