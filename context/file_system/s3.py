
import os
from io import BytesIO
from typing import Any, Optional, Tuple, Union

import boto3
from botocore.client import BaseClient, Config


def new_s3_client(
    ak: Optional[str] = None,
    sk: Optional[str] = None,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> BaseClient:
    """
    创建一个新的 S3 客户端。

    Args:
        ak (str): AWS Access Key ID
        sk (str): AWS Secret Access Key
        region (str): 区域，默认为 us-east-1

    Returns:
        BaseClient: 返回一个新的 S3 客户端对象。
    """
    credential_params = {
        "aws_access_key_id": ak,
        "aws_secret_access_key": sk,
        "region_name": region,
        "endpoint_url": endpoint_url,
    }
    return boto3.client(
        "s3",
        config=Config(signature_version="s3v4"),
        **credential_params,
    )

def get_bucket_object_key(path: str) -> Tuple[str, str]:
    """
    从路径中提取存储桶名称和对象键。

    Args:
        path (str): 包含存储桶名称和对象键的路径字符串。

    Returns:
        Tuple[str, str]: 包含存储桶名称和对象键的元组。

    Raises:
        ValueError: 如果路径不符合预期格式，则引发此异常。

    """
    # 定义存储桶名称和对象键
    if path.startswith("bos:/"):
        pure_path = path.removeprefix("bos:/").strip("/")
    elif path.startswith("s3:/"):
        pure_path = path.removeprefix("s3:/").strip("/")
    bucket_name, object_key = pure_path.split("/", 1)
    return bucket_name, object_key

def s3_file_exists(path: str, s3_client: Optional[BaseClient] = None) -> bool:
    """
    判断S3存储桶中的文件是否存在。

    Args:
        path (str): S3存储桶中的文件路径，支持 s3:// 或 bos:// 前缀。
        s3_client (BaseClient, optional): S3客户端对象，默认为None。

    Returns:
        bool: 文件存在返回True，否则返回False。

    """
    if s3_client is None:
        s3_client = new_s3_client()

    try:
        bucket_name, object_key = get_bucket_object_key(path)
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise ValueError(f"Failed to check file existence for {path}: {e}")
    except Exception as e:
        raise ValueError(f"Failed to check file existence for {path}: {e}")

def s3_head_file(path: str, s3_client: Optional[BaseClient] = None) -> Any:
    """
    查询 S3 存储桶中的文件头信息。

    Args:
        path (str): S3 存储桶中的文件路径。

    Returns:
        Any: S3 客户端返回的 head_object 方法的响应结果。

    """
    if s3_client is None:
        s3_client = new_s3_client()
    bucket_name, object_key = get_bucket_object_key(path)
    response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    return response

def s3_listdir(
    path: str,
    s3_client: Optional[BaseClient] = None,
    return_full_path: bool = False,
) -> Tuple[list, list]:
    """
    列出指定前缀（“目录”）下的一层子项。

    Args:
        path (str): 形如 s3://bucket/prefix 或 bos://bucket/prefix 的路径；
                    也兼容当前文件中使用的 s3:/ 与 bos:/。
                    如果 prefix 为空则列出桶根目录。
        s3_client (BaseClient, optional): 复用的 S3 客户端，默认自动创建。
        return_full_path (bool): 是否返回完整路径（协议+桶+键）。默认只返回相对名称。

    Returns:
        Tuple[list, list]: (files, dirs)
            files: 该层的文件名（或完整路径）
            dirs:  该层的“目录名”（或完整路径，不含末尾斜杠）

    Raises:
        ValueError: 路径无效或列举失败时抛出。
    """
    if s3_client is None:
        s3_client = new_s3_client()

    scheme = "s3"
    if path.startswith("bos:/") or path.startswith("bos://"):
        scheme = "bos"

    try:
        bucket, prefix = get_bucket_object_key(path)
    except Exception as e:
        raise ValueError(f"invalid path {path}: {e}")

    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    files: list = []
    dirs: list = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == prefix:
                    continue
                name = key[len(prefix) :]
                files.append(f"{scheme}://{bucket}/{key}" if return_full_path else name)

            for cp in page.get("CommonPrefixes", []):
                dir_prefix = cp.get("Prefix", "")
                name = dir_prefix[len(prefix) :].rstrip("/")
                dirs.append(
                    f"{scheme}://{bucket}/{dir_prefix.rstrip('/')}"
                    if return_full_path
                    else name
                )
        return files, dirs
    except Exception as e:
        raise ValueError(f"Failed to list dir for {path}: {e}")

def s3_load_byte(path: str, s3_client: Optional[BaseClient] = None) -> bytes:
    """
    从 S3 存储桶中加载对象内容并返回为字节流。

    Args:
        path (str): 包含存储桶名称和对象键的路径字符串。

    Returns:
        bytes: 存储桶中对象的字节流内容。

    Raises:
        ValueError: 如果提供的路径无效，即无法解析出存储桶名称和对象键。
    """
    if s3_client is None:
        s3_client = new_s3_client()
    bucket_name, object_key = get_bucket_object_key(path)
    if not (bucket_name and object_key):
        raise ValueError(f"invalid path {path}")

    # 从 S3 获取对象
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    # 读取对象内容
    data = response["Body"].read()
    assert isinstance(data, bytes)
    return data

def upload_file_obj(
    data: Union[BytesIO, bytes], s3_path: str, s3_client: Optional[BaseClient] = None
) -> None:
    """
    将 BytesIO 或 bytes 对象上传到指定 S3 路径。

    Args:
        data (BytesIO | bytes): 要上传的内容。
        s3_path (str): S3 路径，格式为 s3://bucket/key。
        s3_client (BaseClient, optional): S3 客户端，默认为 None。
    """
    if s3_client is None:
        s3_client = new_s3_client()

    bucket_name, object_key = get_bucket_object_key(s3_path)

    # 如果是 bytes，转为 BytesIO
    if isinstance(data, bytes):
        data = BytesIO(data)

    # 重置指针位置
    data.seek(0)

    # 上传
    s3_client.upload_fileobj(data, bucket_name, object_key)

def s3_generate_presigned_url(
    url: str, expiration: int = 3600 * 24 * 2, s3_client: Optional[BaseClient] = None
) -> str:
    """
    生成 S3 对象的公开链接。

    Args:
        url (str): S3 对象的路径，形如 s3://bucket/key。
        expiration (int): URL 的有效期，单位为秒，默认为 2 天。
        s3_client (BaseClient): S3 客户端对象。

    Returns:
        str: 公开URL。

    """
    if s3_client is None:
        s3_client = new_s3_client()
    bucket_name, object_key = get_bucket_object_key(url)
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": object_key},
        ExpiresIn=expiration,
        HttpMethod="GET",
    )
    assert isinstance(presigned_url, str)
    return presigned_url
