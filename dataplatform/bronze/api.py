"""REST API responses → bronze Delta tables (`bronze/api/<source>`), one row per call, payload untouched."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime, timedelta

import pyarrow as pa
from deltalake import write_deltalake

from dataplatform.connectors.rest_api import fetch_json
from dataplatform.connectors.sources import SourceConfig

log = logging.getLogger(__name__)

BRONZE_API_SCHEMA = pa.schema(
    [
        pa.field("payload", pa.string(), nullable=False),
        pa.field("request_url", pa.string(), nullable=False),
        pa.field("window_start", pa.date32()),
        pa.field("window_end", pa.date32()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def bronze_api_uri(bronze_root: str, source: SourceConfig) -> str:
    return f"{bronze_root}/api/{source.name}"


def ingest_windowed_api(
    source: SourceConfig,
    bronze_root: str,
    storage: dict[str, str],
    *,
    as_of: date,
    ingested_at: datetime,
    fetch: Callable[[str, dict | None], tuple[str, str]] = fetch_json,
) -> int:
    """Fetch the `[as_of - lookback_days, as_of]` window and append the raw response. Returns bytes stored."""
    start = as_of - timedelta(days=int(source.get("lookback_days", 7)))
    url = source["url"].format(start=start.isoformat(), end=as_of.isoformat())
    body, request_url = fetch(url, source.get("params"))
    row = {
        "payload": [body],
        "request_url": [request_url],
        "window_start": [start],
        "window_end": [as_of],
        "_ingested_at": [ingested_at],
    }
    write_deltalake(
        bronze_api_uri(bronze_root, source),
        pa.Table.from_pydict(row, schema=BRONZE_API_SCHEMA),
        mode="append",
        storage_options=storage,
    )
    log.info("%s: stored %d bytes from %s", source.name, len(body), request_url)
    return len(body)
