from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol


class LakeStorage(Protocol):
    def atomic_writer(self, relative_path: str) -> AbstractContextManager[BinaryIO]:
        """Return a binary writer that only publishes its file on successful exit."""

    def read_bytes(self, relative_path: str) -> bytes:
        """Read a complete object from the lake."""

    def list_files(self, relative_prefix: str) -> list[str]:
        """List object keys under a lake prefix."""


class LocalLakeStorage:
    def __init__(self, root: Path | str = Path("data/lake")) -> None:
        self.root = Path(root)

    @contextmanager
    def atomic_writer(self, relative_path: str) -> Iterator[BinaryIO]:
        destination = self.root.joinpath(*PurePosixPath(relative_path).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(file_descriptor, "w+b") as output:
                yield output
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_path, destination)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise

    def read_bytes(self, relative_path: str) -> bytes:
        return self.root.joinpath(*PurePosixPath(relative_path).parts).read_bytes()

    def list_files(self, relative_prefix: str) -> list[str]:
        directory = self.root.joinpath(*PurePosixPath(relative_prefix).parts)
        if not directory.exists():
            return []
        return [
            path.relative_to(self.root).as_posix()
            for path in directory.rglob("*")
            if path.is_file()
        ]


class CloudflareR2Storage:
    def __init__(
        self,
        *,
        account_id: str,
        access_key_id: str,
        secret_access_key: str,
        bucket: str,
        endpoint_url: str | None = None,
    ) -> None:
        import boto3

        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
        )

    @contextmanager
    def atomic_writer(self, relative_path: str) -> Iterator[BinaryIO]:
        file_descriptor, temporary_name = tempfile.mkstemp()
        try:
            with os.fdopen(file_descriptor, "w+b") as output:
                yield output
            with open(temporary_name, "rb") as output:
                self.client.upload_fileobj(output, self.bucket, relative_path)
        finally:
            Path(temporary_name).unlink(missing_ok=True)

    def read_bytes(self, relative_path: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=relative_path)
        except self.client.exceptions.NoSuchKey as error:
            raise FileNotFoundError(relative_path) from error
        with response["Body"] as body:
            return bytes(body.read())

    def list_files(self, relative_prefix: str) -> list[str]:
        prefix = relative_prefix.rstrip("/") + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        return [
            item["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet") or item["Key"].endswith(".json")
        ]


def create_lake_storage() -> LakeStorage:
    backend = os.getenv("LAKE_BACKEND", "local").strip().lower()
    if backend == "local":
        return LocalLakeStorage()
    if backend == "r2":
        required = (
            "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_BUCKET",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError(f"Missing required R2 environment variables: {', '.join(missing)}")
        return CloudflareR2Storage(
            account_id=os.environ["R2_ACCOUNT_ID"],
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            bucket=os.environ["R2_BUCKET"],
            endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        )
    raise ValueError(f"Unsupported LAKE_BACKEND {backend!r}; expected 'local' or 'r2'")
