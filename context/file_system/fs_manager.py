"""Centralized filesystem manager for context-level operations."""

from __future__ import annotations

import shutil
from typing import BinaryIO, Dict, List, Optional

from botocore.client import BaseClient

from app.conf.config import Settings as Config
from context.file_system.fs_adapter import (
    FS_TYPE_LOCAL,
    FS_TYPE_S3,
    SCHEME_S3,
    FileSystemAdapter,
    ListEntry,
    LocalFileSystemAdapter,
    S3FileSystemAdapter,
    _detect_fs_type,
    _extract_scheme,
    get_fs_cache_key,
    join_uri,
    normalize_dir_uri,
    relative_path,
)
from context.file_system.s3 import new_s3_client


class FileSystemManager:
    """Manage filesystem adapters and high-level filesystem helpers."""

    def __init__(self, config: Config) -> None:
        self._config: Config = config
        self._fs_adapters: Dict[str, FileSystemAdapter] = {}
        self._default_s3_client: Optional[BaseClient] = None

    def get_s3_client(self, config: Optional[Config] = None) -> BaseClient:
        """Return S3 client derived from config."""
        if config is not None:
            return new_s3_client(
                ak=config.access_key,
                sk=config.secret_key,
                region=config.region,
                endpoint_url=config.endpoint,
            )
        return self._get_default_s3_client()

    def _ensure_config(self) -> Config:
        if self._config is None:
            raise ValueError("Config required for filesystem operations")
        return self._config

    def _get_default_s3_client(self) -> BaseClient:
        if self._default_s3_client is None:
            cfg = self._ensure_config()
            self._default_s3_client = new_s3_client(
                ak=cfg.access_key,
                sk=cfg.secret_key,
                region=cfg.region,
                endpoint_url=cfg.endpoint,
            )
        return self._default_s3_client

    def get_fs(self, uri: str) -> FileSystemAdapter:
        """Return filesystem adapter for the given URI."""
        cache_key = get_fs_cache_key(uri)
        adapter = self._fs_adapters.get(cache_key)
        if adapter is None:
            adapter = self._create_fs_adapter(uri)
            self._fs_adapters[cache_key] = adapter
        return adapter

    def read_bytes(self, uri: str) -> bytes:
        """Read raw bytes from URI."""
        return self.get_fs(uri).read_bytes(uri)

    def write_bytes(self, uri: str, data: bytes) -> None:
        """Write raw bytes to URI."""
        self.get_fs(uri).write_bytes(uri, data)

    def read_text(self, uri: str, encoding: str = "utf-8") -> str:
        """Read text from URI."""
        return self.get_fs(uri).read_text(uri, encoding=encoding)

    def write_text(self, uri: str, content: str, encoding: str = "utf-8") -> None:
        """Write text to URI."""
        self.get_fs(uri).write_text(uri, content, encoding=encoding)

    def open_read_stream(self, uri: str) -> BinaryIO:
        """Open read stream for URI."""
        return self.get_fs(uri).open_read_stream(uri)

    def open_write_stream(self, uri: str) -> BinaryIO:
        """Open write stream for URI."""
        return self.get_fs(uri).open_write_stream(uri)

    def exists(self, uri: str) -> bool:
        """Return True if URI exists."""
        return self.get_fs(uri).exists(uri)

    def listdir(self, uri: str) -> List[ListEntry]:
        """List entries under URI."""
        return self.get_fs(uri).listdir(uri)

    def remove(self, uri: str) -> None:
        """Remove file at URI."""
        self.get_fs(uri).remove(uri)

    def getsize(self, uri: str) -> int:
        """Return size of file at URI."""
        return self.get_fs(uri).getsize(uri)

    def generate_presigned_url(self, uri: str, expiration: int = 3600 * 24 * 2) -> str:
        """Generate presigned URL for S3 URI."""
        return self.get_fs(uri).s3_generate_presigned_url(uri, expiration)

    def copy_directory(self, src_uri: str, dst_uri: str) -> None:
        """Recursively copy directory contents across filesystems."""
        src_fs_type = _detect_fs_type(src_uri)
        dst_fs_type = _detect_fs_type(dst_uri)

        src_root = normalize_dir_uri(src_uri, src_fs_type)
        dst_root = normalize_dir_uri(dst_uri, dst_fs_type)

        src_fs = self.get_fs(src_root)
        dst_fs = self.get_fs(dst_root)

        stack: List[str] = [src_root]
        while stack:
            current = stack.pop()
            for entry in src_fs.listdir(current):
                if entry.is_dir:
                    stack.append(entry.path)
                    continue

                rel_file = relative_path(entry.path, src_root, src_fs_type)
                target_uri = join_uri(dst_root, rel_file, dst_fs_type)
                with (
                    src_fs.open_read_stream(entry.path) as src_fh,
                    dst_fs.open_write_stream(target_uri) as dst_fh,
                ):
                    shutil.copyfileobj(src_fh, dst_fh)

    def _create_fs_adapter(self, uri: str) -> FileSystemAdapter:
        fs_type = _detect_fs_type(uri)
        if fs_type == FS_TYPE_LOCAL:
            return LocalFileSystemAdapter()
        if fs_type == FS_TYPE_S3:
            scheme = _extract_scheme(uri) or SCHEME_S3
            return S3FileSystemAdapter(
                client_supplier=self._get_default_s3_client,
                scheme=scheme,
            )
        raise ValueError(f"Unsupported filesystem URI: {uri}")
