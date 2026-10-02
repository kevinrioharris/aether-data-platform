"""Object locations: the landing zone (s3://landing, or a local folder in tests) and any other folder of
files a connector reads from (S3, MinIO, RustFS, local disk)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import obstore
from obstore.store import LocalStore, from_url

from dataplatform.config import Settings
from dataplatform.lakehouse.storage import storage_options


@dataclass(frozen=True)
class LandingFile:
    path: str  # relative to the landing root, e.g. excel/sales_targets/sales_targets_2026.xlsx
    etag: str  # changes whenever the file is overwritten
    size: int
    last_modified: datetime | None = None


def open_store(uri: str, s3_options: dict[str, str] | None = None):
    """An obstore store rooted at `uri` (`s3://bucket[/prefix]` or a local directory).

    `s3_options` are delta-rs style `AWS_*` options (see `lakehouse.storage.storage_options`).
    """
    if uri.startswith("s3://"):
        opts = s3_options or {}
        config = {k.lower(): v for k, v in opts.items() if k.startswith("AWS_") and k not in _NOT_OBSTORE}
        allow_http = opts.get("AWS_ALLOW_HTTP", "false").lower() == "true"
        return from_url(uri, config=config, client_options={"allow_http": allow_http})
    Path(uri).mkdir(parents=True, exist_ok=True)
    return LocalStore(uri)


class Landing:
    def __init__(self, uri: str, settings: Settings | None = None, *, store=None):
        if store is not None:
            self.store = store
        else:
            self.store = open_store(uri, storage_options(settings, uri) if uri.startswith("s3://") else None)

    def list(self, prefix: str) -> list[LandingFile]:
        return [
            LandingFile(
                path=m["path"],
                etag=(m.get("e_tag") or m["last_modified"].isoformat()).strip('"'),
                size=m["size"],
                last_modified=m["last_modified"],
            )
            for batch in obstore.list(self.store, prefix=prefix or None)
            for m in batch
        ]

    def read(self, path: str) -> bytes:
        return bytes(obstore.get(self.store, path).bytes())

    def put(self, path: str, data: bytes) -> None:
        obstore.put(self.store, path, data)


# delta-rs-only options that obstore rejects.
_NOT_OBSTORE = {"AWS_S3_ALLOW_UNSAFE_RENAME", "AWS_ALLOW_HTTP"}
