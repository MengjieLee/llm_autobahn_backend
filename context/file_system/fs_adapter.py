"""Filesystem adapter abstractions for context-level file operations."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Callable, List, Tuple

import smart_open
from botocore.client import BaseClient

from context.file_system.s3 import (
    s3_file_exists,
    s3_head_file,
    s3_listdir,
    s3_load_byte,
    upload_file_obj,
    s3_generate_presigned_url
)

# Filesystem type identifiers
FS_TYPE_LOCAL = "local"
FS_TYPE_S3 = "s3"

# URI scheme constants
SCHEME_EMPTY = ""
SCHEME_FILE = "file"
SCHEME_S3 = "s3"
SCHEME_BOS = "bos"

# Scheme groupings
_LOCAL_SCHEMES = (SCHEME_EMPTY, SCHEME_FILE)
_S3_SCHEMES = (SCHEME_S3, SCHEME_BOS)

# URI scheme separators
URI_SCHEME_SEPARATOR = "://"
URI_SCHEME_PREFIX = ":"
URI_PATH_SEPARATOR = "/"

# Default encoding
DEFAULT_ENCODING = "utf-8"

# S3 list delimiter
S3_LIST_DELIMITER = "/"


def _starts_with_scheme(uri: str, scheme: str) -> bool:
    return uri.lower().startswith(f"{scheme}{URI_SCHEME_SEPARATOR}")


def _extract_scheme(uri: str) -> str:
    """Extract the scheme from a URI (e.g., 's3', 'bos', 'file'), or empty string if none."""
    if not uri:
        return SCHEME_EMPTY
    lu = uri.lower()
    if _starts_with_scheme(lu, SCHEME_S3):
        return SCHEME_S3
    elif _starts_with_scheme(lu, SCHEME_BOS):
        return SCHEME_BOS
    elif _starts_with_scheme(lu, SCHEME_FILE):
        return SCHEME_FILE
    elif lu.startswith(URI_PATH_SEPARATOR):
        return SCHEME_EMPTY
    raise ValueError(f"Unrecognized URI scheme in: {uri}")


def _strip_scheme(uri: str) -> str:
    """Remove scheme prefix from URI."""
    scheme = _extract_scheme(uri)
    if not scheme:
        return uri
    if scheme in (SCHEME_S3, SCHEME_BOS):
        return uri[len(f"{scheme}{URI_SCHEME_SEPARATOR}") :]
    if scheme == SCHEME_FILE:
        return uri[len(f"{SCHEME_FILE}{URI_SCHEME_SEPARATOR}") :]
    return uri


def _validate_s3_uri(uri: str) -> None:
    scheme = _extract_scheme(uri)
    if scheme not in _S3_SCHEMES:
        raise ValueError(f"Invalid S3 URI: {uri}")
    rest = _strip_scheme(uri)
    if not rest:
        raise ValueError(f"URI missing bucket component: {uri}")
    if rest.startswith(URI_PATH_SEPARATOR):
        raise ValueError(f"Unrecognized filesystem type for URI: {uri}")
    bucket = rest.split(URI_PATH_SEPARATOR, 1)[0]
    if not bucket:
        raise ValueError(f"URI missing bucket component: {uri}")


def _detect_fs_type(uri: str) -> str:
    """Identify filesystem type for given URI.

    Returns:
        FS_TYPE_LOCAL or FS_TYPE_S3
    """
    if not uri:
        raise ValueError("Cannot detect filesystem type for empty URI")

    lu = uri.lower()

    if _starts_with_scheme(lu, SCHEME_S3) or _starts_with_scheme(lu, SCHEME_BOS):
        _validate_s3_uri(uri)
        return FS_TYPE_S3

    if lu.startswith(f"{SCHEME_FILE}{URI_SCHEME_SEPARATOR}"):
        if not lu.startswith(
            f"{SCHEME_FILE}{URI_SCHEME_SEPARATOR}{URI_PATH_SEPARATOR}"
        ):
            raise ValueError(f"Unsupported filesystem URI: {uri}")
        return FS_TYPE_LOCAL

    if uri.startswith(URI_PATH_SEPARATOR):
        return FS_TYPE_LOCAL

    raise ValueError(f"Unrecognized filesystem type for URI: {uri}")


def _normalize_local_path(uri: str) -> str:
    """Convert local URI to absolute filesystem path."""
    fs_type = _detect_fs_type(uri)
    if fs_type != FS_TYPE_LOCAL:
        raise ValueError(f"Unsupported local URI: {uri}")

    scheme = _extract_scheme(uri)
    if scheme == SCHEME_FILE:
        path = _strip_scheme(uri)
        if not path.startswith(URI_PATH_SEPARATOR):
            raise ValueError(f"Unsupported local URI: {uri}")
    else:
        path = uri

    if not path.startswith(URI_PATH_SEPARATOR):
        raise ValueError(f"Unsupported local URI: {uri}")

    return os.path.normpath(path)


def _split_bucket_key(uri: str) -> Tuple[str, str]:
    """Split S3-style URI into bucket and key components."""
    fs_type = _detect_fs_type(uri)
    if fs_type != FS_TYPE_S3:
        raise ValueError(f"Invalid S3 URI: {uri}")

    stripped = _strip_scheme(uri)
    if not stripped or stripped.startswith(URI_PATH_SEPARATOR):
        raise ValueError(f"URI missing bucket component: {uri}")

    if URI_PATH_SEPARATOR in stripped:
        bucket, key = stripped.split(URI_PATH_SEPARATOR, 1)
    else:
        bucket, key = stripped, SCHEME_EMPTY

    if not bucket:
        raise ValueError(f"URI missing bucket component: {uri}")

    return bucket, key


@dataclass
class ListEntry:
    """Represents a filesystem entry returned by adapters."""

    path: str
    is_dir: bool


class FileSystemAdapter(ABC):
    """Abstract filesystem adapter interface."""

    def read_text(self, uri: str, encoding: str = DEFAULT_ENCODING) -> str:
        """Read file contents as text."""
        return self.read_bytes(uri).decode(encoding)

    @abstractmethod
    def read_bytes(self, uri: str) -> bytes:
        """Read file contents as bytes."""

    def write_text(
        self, uri: str, content: str, encoding: str = DEFAULT_ENCODING
    ) -> None:
        """Write text content to file."""
        self.write_bytes(uri, content.encode(encoding))

    @abstractmethod
    def write_bytes(self, uri: str, data: bytes) -> None:
        """Write bytes to file."""

    @abstractmethod
    def open_read_stream(self, uri: str) -> BinaryIO:
        """Open a binary read stream for the given URI."""

    @abstractmethod
    def open_write_stream(self, uri: str) -> BinaryIO:
        """Open a binary write stream for the given URI."""

    @abstractmethod
    def exists(self, uri: str) -> bool:
        """Return True if path exists."""

    @abstractmethod
    def listdir(self, uri: str) -> List[ListEntry]:
        """List directory or prefix contents."""

    @abstractmethod
    def remove(self, uri: str) -> None:
        """Remove file."""

    @abstractmethod
    def getsize(self, uri: str) -> int:
        """Return size of file in bytes."""


class LocalFileSystemAdapter(FileSystemAdapter):
    """Filesystem adapter for local paths."""

    def read_bytes(self, uri: str) -> bytes:
        path = _normalize_local_path(uri)
        with open(path, "rb") as fh:
            return fh.read()

    def write_bytes(self, uri: str, data: bytes) -> None:
        path = _normalize_local_path(uri)
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)

    def open_read_stream(self, uri: str) -> BinaryIO:
        path = _normalize_local_path(uri)
        return open(path, "rb")

    def open_write_stream(self, uri: str) -> BinaryIO:
        path = _normalize_local_path(uri)
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        return open(path, "wb")

    def exists(self, uri: str) -> bool:
        path = _normalize_local_path(uri)
        return os.path.exists(path)

    def listdir(self, uri: str) -> List[ListEntry]:
        path = _normalize_local_path(uri)
        entries: List[ListEntry] = []
        if not os.path.isdir(path):
            return entries
        for name in os.listdir(path):
            child_path = os.path.join(path, name)
            entries.append(ListEntry(path=child_path, is_dir=os.path.isdir(child_path)))
        return entries

    def remove(self, uri: str) -> None:
        path = _normalize_local_path(uri)
        if os.path.isdir(path):
            raise IsADirectoryError(f"Cannot remove directory {path}")
        if os.path.exists(path):
            os.remove(path)

    def getsize(self, uri: str) -> int:
        path = _normalize_local_path(uri)
        return os.path.getsize(path)
    
    def s3_generate_presigned_url(self, uri: str, expiration: int = 3600 * 24 * 2) -> str:
        _validate_s3_uri(uri)
        return s3_generate_presigned_url(uri, expiration, self._client())


class S3FileSystemAdapter(FileSystemAdapter):
    """Filesystem adapter for S3-compatible object storage."""

    def __init__(
        self,
        client_supplier: Callable[[], BaseClient],
        scheme: str = SCHEME_S3,
    ) -> None:
        self._client_supplier = client_supplier
        self._scheme = scheme

    def _client(self) -> BaseClient:
        return self._client_supplier()

    def _format_uri(self, bucket: str, key: str) -> str:
        prefix = f"{self._scheme}{URI_SCHEME_SEPARATOR}"
        return f"{prefix}{bucket}{URI_PATH_SEPARATOR}{key}".rstrip(URI_PATH_SEPARATOR)

    def read_bytes(self, uri: str) -> bytes:
        _validate_s3_uri(uri)
        return s3_load_byte(uri, self._client())

    def write_bytes(self, uri: str, data: bytes) -> None:
        _validate_s3_uri(uri)
        upload_file_obj(data, uri, self._client())

    def open_read_stream(self, uri: str) -> BinaryIO:
        _validate_s3_uri(uri)
        bucket, key = _split_bucket_key(uri)
        if not key:
            raise ValueError(f"Cannot open read stream for bucket root: {uri}")
        client = self._client()
        return smart_open.open(
            f"{SCHEME_S3}{URI_SCHEME_SEPARATOR}{bucket}{URI_PATH_SEPARATOR}{key}",
            mode="rb",
            transport_params={"client": client},
        )

    def open_write_stream(self, uri: str) -> BinaryIO:
        _validate_s3_uri(uri)
        bucket, key = _split_bucket_key(uri)
        if not key:
            raise ValueError(f"Cannot open write stream for bucket root: {uri}")
        client = self._client()
        return smart_open.open(
            f"{SCHEME_S3}{URI_SCHEME_SEPARATOR}{bucket}{URI_PATH_SEPARATOR}{key}",
            mode="wb",
            transport_params={"client": client},
        )

    def exists(self, uri: str) -> bool:
        _validate_s3_uri(uri)
        # todo: 支持目录判断
        return s3_file_exists(uri, self._client())

    def listdir(self, uri: str) -> List[ListEntry]:
        _validate_s3_uri(uri)
        client = self._client()
        files, dirs = s3_listdir(uri, client, return_full_path=True)

        entries: List[ListEntry] = []
        for dir_path in dirs:
            entries.append(ListEntry(path=dir_path, is_dir=True))
        for file_path in files:
            entries.append(ListEntry(path=file_path, is_dir=False))

        return entries

    def remove(self, uri: str) -> None:
        _validate_s3_uri(uri)
        client = self._client()
        bucket, key = _split_bucket_key(uri)
        if not key:
            raise ValueError(f"Cannot remove bucket root: {uri}")
        client.delete_object(Bucket=bucket, Key=key)

    def getsize(self, uri: str) -> int:
        _validate_s3_uri(uri)
        size = s3_head_file(uri, self._client())["ContentLength"]
        assert isinstance(size, int)
        return size
    
    def s3_generate_presigned_url(self, uri: str, expiration: int = 3600 * 24 * 2) -> str:
        _validate_s3_uri(uri)
        return s3_generate_presigned_url(uri, expiration, self._client())


def get_fs_cache_key(uri: str) -> str:
    """Return cache key for filesystem adapter instances."""
    fs_type = _detect_fs_type(uri)
    if fs_type == FS_TYPE_LOCAL:
        return FS_TYPE_LOCAL
    if fs_type == FS_TYPE_S3:
        scheme = _extract_scheme(uri) or SCHEME_S3
        return scheme
    raise ValueError(f"Unsupported filesystem URI: {uri}")


def normalize_dir_uri(uri: str, fs_type: str) -> str:
    """Normalize directory URI to a usable root for walking."""
    if fs_type == FS_TYPE_LOCAL:
        return _normalize_local_path(uri)
    return uri.rstrip(URI_PATH_SEPARATOR)


def relative_path(path: str, root: str, fs_type: str) -> str:
    """Compute path relative to the given root for supported filesystems."""
    if fs_type == FS_TYPE_LOCAL:
        rel = os.path.relpath(_normalize_local_path(path), _normalize_local_path(root))
        return "" if rel == "." else rel

    if fs_type == FS_TYPE_S3:
        root_bucket, root_key = _split_bucket_key(root)
        bucket, key = _split_bucket_key(path)
        if bucket != root_bucket:
            raise ValueError("Source path and root must belong to the same bucket")

        prefix = root_key.rstrip(URI_PATH_SEPARATOR)
        if prefix:
            expected_prefix = f"{prefix}{URI_PATH_SEPARATOR}"
            if not key.startswith(expected_prefix):
                raise ValueError(f"Path {path} not under source root {root}")
            return key[len(expected_prefix) :]
        return key

    raise ValueError(f"Unsupported filesystem type {fs_type}")


def join_uri(base: str, relative_path: str, fs_type: str) -> str:
    """Join base URI with relative path honoring filesystem semantics."""
    if not relative_path:
        return base

    if fs_type == FS_TYPE_LOCAL:
        base_path = _normalize_local_path(base)
        return os.path.join(base_path, relative_path)

    if fs_type == FS_TYPE_S3:
        normalized = relative_path.replace(os.sep, URI_PATH_SEPARATOR)
        return f"{base.rstrip(URI_PATH_SEPARATOR)}{URI_PATH_SEPARATOR}{normalized}"

    raise ValueError(f"Unsupported filesystem type {fs_type}")
