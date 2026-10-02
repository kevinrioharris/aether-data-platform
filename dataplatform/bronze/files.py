"""Landing-zone files → bronze Delta tables (`bronze/files/<source>`).

Each ingested file appends its rows as strings plus lineage columns. A file is identified by
(path, etag): re-uploading an edited file ingests the new version, and re-running never duplicates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
from deltalake import write_deltalake

from dataplatform.connectors import registry
from dataplatform.connectors.base import ConfigurationError, ExtractRequest, IngestionMode
from dataplatform.connectors.files import ROW_NUMBER, FileConnector
from dataplatform.connectors.landing import Landing
from dataplatform.connectors.sources import SourceConfig
from dataplatform.lakehouse import delta

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileIngestResult:
    source: str
    files_seen: int
    files_ingested: int
    rows: int


def bronze_files_uri(bronze_root: str, source: SourceConfig) -> str:
    return f"{bronze_root}/files/{source.name}"


def _already_ingested(uri: str, storage: dict[str, str]) -> set[tuple[str, str]]:
    if not delta.table_exists(uri, storage):
        return set()
    seen = delta.query({"b": uri}, "SELECT DISTINCT _source_file, _file_etag FROM b", storage)
    return set(zip(seen["_source_file"].to_pylist(), seen["_file_etag"].to_pylist(), strict=True))


def file_connector_for(source: SourceConfig, landing: Landing) -> tuple[FileConnector, str, dict]:
    """Build the connector, stream and stream options for a `sources.yml` file source (type excel/csv)."""
    cls = registry.get(source.type)
    connector = cls.from_config(
        {"location": source["landing_prefix"], "file_pattern": source.get("file_pattern")}, landing=landing
    )
    if not isinstance(connector, FileConnector):
        raise ConfigurationError(f"{source.name}: {source.type!r} is not a file connector")
    options = {k: v for k, v in source.options.items() if k in cls.stream_options_model.model_fields}
    stream = source.get("sheet") or connector.default_stream()
    return connector, stream, options


def ingest_file_source(
    source: SourceConfig, landing: Landing, bronze_root: str, storage: dict[str, str], ingested_at: datetime
) -> FileIngestResult:
    """New or changed files of one source → `bronze/files/<source>`. The bronze table itself records which
    (file, etag) pairs were ingested, so the write and the "already seen" state can never disagree."""
    uri = bronze_files_uri(bronze_root, source)
    connector, stream, options = file_connector_for(source, landing)
    state = {"seen": sorted(list(k) for k in _already_ingested(uri, storage))}
    request = ExtractRequest(stream=stream, mode=IngestionMode.INCREMENTAL, options=options, state=state)
    ingested, rows_written = 0, 0

    for batch in connector.extract(request):
        n = batch.records.num_rows
        f = batch.source
        columns: dict[str, pa.Array] = {name: batch.records[name] for name in batch.records.column_names}
        columns["_row_number"] = batch.row_metadata[ROW_NUMBER]
        if source.type == "excel":
            columns["_sheet"] = pa.array([stream] * n, pa.string())
        columns |= {
            "_source_file": pa.array([f["file"]] * n, pa.string()),
            "_file_etag": pa.array([f["etag"]] * n, pa.string()),
            **{f"_file_{k}": pa.array([v] * n, pa.string()) for k, v in f["pattern_groups"].items()},
            "_ingested_at": pa.array([ingested_at] * n, pa.timestamp("us", tz="UTC")),
        }
        # schema_mode="merge": a new column in next year's spreadsheet must not break ingestion.
        write_deltalake(uri, pa.table(columns), mode="append", schema_mode="merge", storage_options=storage)
        ingested += 1
        rows_written += n
        log.info("%s: ingested %s (%d rows)", source.name, f["file"], n)

    files_seen = len(landing.list(source["landing_prefix"]))
    return FileIngestResult(source.name, files_seen, ingested, rows_written)


# Kept for callers written before CSV support.
ingest_excel_source = ingest_file_source
