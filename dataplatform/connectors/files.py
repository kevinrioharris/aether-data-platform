"""File connectors: one format parser on top of any object location (local disk, S3, MinIO, RustFS, uploads).

A source is a *location* (a folder ending in `/`, or a single file) plus an optional regex `file_pattern`.
Named groups in the pattern (e.g. `(?P<fiscal_year>\\d{4})`) are reported per file in `ExtractBatch.source`.
Incremental runs skip files whose (path, etag) was already ingested, so re-uploading an edited file
ingests the new version and re-running never duplicates.

Rows are returned as strings (or null). Typing happens in silver, with types suggested by `discover_schema`.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from abc import abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.csv as pacsv
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from dataplatform.connectors import excel
from dataplatform.connectors.base import (
    Capabilities,
    ConnectionTestResult,
    Connector,
    ConnectorConfig,
    ConnectorError,
    ExtractBatch,
    ExtractRequest,
    IngestionMode,
    SizeEstimate,
    SourceCategory,
    Stream,
)
from dataplatform.connectors.landing import Landing, LandingFile, open_store
from dataplatform.connectors.registry import register
from dataplatform.core.schema import DatasetSchema, infer_string_types

log = logging.getLogger(__name__)

ROW_NUMBER = "source_row"  # row_metadata column: 1-based sheet row / file line of each record


class S3Credentials(ConnectorConfig):
    endpoint_url: str | None = Field(None, description="Custom endpoint for MinIO, RustFS, other S3-compatible stores")
    region: str = "us-east-1"
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    allow_http: bool = False

    def storage_options(self) -> dict[str, str]:
        opts = {"AWS_REGION": self.region, "AWS_ALLOW_HTTP": str(self.allow_http).lower()}
        if self.endpoint_url:
            opts["AWS_ENDPOINT_URL"] = self.endpoint_url
        if self.access_key_id:
            opts["AWS_ACCESS_KEY_ID"] = self.access_key_id
        if self.secret_access_key:
            opts["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key.get_secret_value()
        return opts


class FileSourceConfig(ConnectorConfig):
    location: str = Field(
        description="A folder (ending in '/') or a single file: a local path or s3://bucket/prefix/. "
        "When the platform injects its landing zone, a path relative to it."
    )
    file_pattern: str | None = Field(
        None, description="Regex searched in each file path; named groups become per-file attributes"
    )
    s3: S3Credentials | None = None

    @field_validator("file_pattern")
    @classmethod
    def _compiles(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(f"invalid regex: {e}") from None
        return v


@dataclass(frozen=True)
class MatchedFile:
    file: LandingFile
    groups: dict[str, str]


class FileConnector(Connector):
    """Locating files is shared; subclasses only parse bytes (`_read`) and name streams (`_streams_in`)."""

    category = SourceCategory.FILE
    config_model = FileSourceConfig
    extensions: ClassVar[tuple[str, ...]] = ()

    def __init__(self, config: FileSourceConfig, landing: Landing | None = None):
        super().__init__(config)
        self.config: FileSourceConfig = config
        if landing is not None:  # platform-managed store: location is relative to it
            self._landing, relative, is_dir = landing, config.location.lstrip("/"), None
        else:
            root, relative, is_dir = _split_location(config.location)
            s3 = config.s3.storage_options() if config.s3 else None
            self._landing = Landing(root, store=open_store(root, s3))
        folder = is_dir if is_dir is not None else (relative == "" or relative.endswith("/"))
        self._prefix = relative if folder else relative.rpartition("/")[0]
        self._exact = None if folder else relative
        self._pattern = re.compile(config.file_pattern) if config.file_pattern else None

    # ── files ───────────────────────────────────────────────────────────────
    def list_files(self) -> list[MatchedFile]:
        matched = []
        for f in self._landing.list(self._prefix):
            if self._exact is not None and f.path != self._exact:
                continue
            if self._pattern is not None:
                if (m := self._pattern.search(f.path)) is None:
                    log.warning("%s: skipping %s (does not match %s)", self.type, f.path, self._pattern.pattern)
                    continue
                groups = {k: v for k, v in m.groupdict().items() if v is not None}
            elif self.extensions and not f.path.lower().endswith(self.extensions):
                continue
            else:
                groups = {}
            matched.append(MatchedFile(f, groups))
        return sorted(matched, key=lambda m: m.file.path)

    def _newest(self) -> MatchedFile:
        files = self.list_files()
        if not files:
            raise ConnectorError(f"no matching files in {self.config.location!r}")
        return max(files, key=lambda m: (m.file.last_modified is not None, m.file.last_modified, m.file.path))

    # ── format hooks ────────────────────────────────────────────────────────
    @abstractmethod
    def _streams_in(self, data: bytes, path: str) -> list[Stream]: ...

    @abstractmethod
    def _read(self, data: bytes, stream: str, options: BaseModel, path: str) -> tuple[pa.Table, pa.Table]:
        """→ (records as strings, row metadata with a `source_row` column)."""

    def default_stream(self) -> str:
        streams = self.discover_streams()
        visible = [s for s in streams if not s.metadata.get("hidden")]
        return (visible or streams)[0].id

    # ── Connector API ───────────────────────────────────────────────────────
    def capabilities(self) -> Capabilities:
        return Capabilities(full=True, incremental_files=True, size_estimate=True)

    def test_connection(self) -> ConnectionTestResult:
        try:
            files = self.list_files()
        except Exception as e:  # noqa: BLE001 - any store error is a failed test, reported to the user
            return ConnectionTestResult(False, f"cannot list {self.config.location!r}: {e}")
        if not files:
            return ConnectionTestResult(True, "location is reachable; no matching files yet", {"files": 0})
        return ConnectionTestResult(True, f"found {len(files)} matching file(s)", {"files": len(files)})

    def discover_streams(self) -> list[Stream]:
        newest = self._newest()
        return self._streams_in(self._landing.read(newest.file.path), newest.file.path)

    def preview_data(self, stream: str, options: Mapping[str, Any] | None = None, limit: int = 100) -> pa.Table:
        newest = self._newest()
        records, _ = self._read(
            self._landing.read(newest.file.path), stream, self.validate_stream_options(options or {}), newest.file.path
        )
        return records.slice(0, limit)

    def discover_schema(self, stream: str, options: Mapping[str, Any] | None = None) -> DatasetSchema:
        return infer_string_types(self.preview_data(stream, options, limit=10_000))

    def estimate_size(self, stream: str, options: Mapping[str, Any] | None = None) -> SizeEstimate:
        files = self.list_files()
        return SizeEstimate(bytes=sum(m.file.size for m in files), files=len(files))

    def get_metadata(self) -> dict[str, Any]:
        files = self.list_files()
        return {
            **super().get_metadata(),
            "location": self.config.location,
            "files": len(files),
            "total_bytes": sum(m.file.size for m in files),
        }

    def extract(self, request: ExtractRequest) -> Iterator[ExtractBatch]:
        options = self.validate_stream_options(request.options)
        incremental = request.mode == IngestionMode.INCREMENTAL
        seen = {tuple(p) for p in request.state.get("seen", [])} if incremental else set()
        for m in self.list_files():
            key = (m.file.path, m.file.etag)
            if key in seen:
                continue
            records, row_meta = self._read(self._landing.read(m.file.path), request.stream, options, m.file.path)
            seen.add(key)
            yield ExtractBatch(
                records=records,
                row_metadata=row_meta,
                state={"seen": sorted([list(k) for k in seen])},
                source={
                    "file": m.file.path,
                    "etag": m.file.etag,
                    "size": m.file.size,
                    "stream": request.stream,
                    "pattern_groups": m.groups,
                },
            )


def _split_location(location: str) -> tuple[str, str, bool | None]:
    """→ (store root, path relative to it, is_dir if known)."""
    if location.startswith("s3://"):
        bucket, _, key = location[len("s3://") :].partition("/")
        return f"s3://{bucket}", key, None
    path = Path(location).expanduser()
    if path.is_file():
        return str(path.parent), path.name, False
    return str(path), "", True


def _string_table(columns: list[str], rows: list[list[str | None]]) -> pa.Table:
    return pa.table({name: pa.array([r[i] for r in rows], pa.string()) for i, name in enumerate(columns)})


# ── Excel ─────────────────────────────────────────────────────────────────────


class ExcelOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    header_row: int | None = Field(None, ge=1, description="1-based header row; default: auto-detect")
    header_row_contains: str | None = Field(None, description="Header = first row with a cell equal to this")
    stop_at_first_cell: str | None = Field(None, description="Stop at the first row whose first cell is this (Total)")
    ignore_columns: list[str] = Field(default_factory=list, description="Headers to drop, e.g. formula totals")


@register
class ExcelConnector(FileConnector):
    type = "excel"
    display_name = "Excel workbook (.xlsx)"
    stream_options_model = ExcelOptions
    extensions = (".xlsx", ".xlsm")

    def _streams_in(self, data: bytes, path: str) -> list[Stream]:
        return [
            Stream(
                id=s.name,
                name=s.name,
                namespace=(path,),
                kind="sheet",
                metadata={"hidden": s.state != "visible", "state": s.state},
            )
            for s in excel.list_sheets(data)
        ]

    def _read(self, data: bytes, stream: str, options: ExcelOptions, path: str) -> tuple[pa.Table, pa.Table]:
        try:
            table = excel.read_excel_table(
                data,
                sheet=stream,
                header_row=options.header_row,
                header_row_contains=options.header_row_contains,
                stop_at_first_cell=options.stop_at_first_cell,
                ignore_columns=options.ignore_columns,
            )
        except ValueError as e:
            raise ConnectorError(f"{path}: {e}") from None
        records = _string_table(table.columns, [values for _, values in table.rows])
        return records, pa.table({ROW_NUMBER: pa.array([row for row, _ in table.rows], pa.int32())})


# ── CSV ───────────────────────────────────────────────────────────────────────


class CsvOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    delimiter: str = Field(",", min_length=1, max_length=1)
    quote_char: str = Field('"', min_length=1, max_length=1)
    encoding: str = "utf-8"
    header_row: int = Field(1, ge=0, description="1-based header line; 0 = no header (columns named column_1..n)")
    null_values: list[str] = Field(default_factory=lambda: [""], description="Values read as null")


@register
class CsvConnector(FileConnector):
    type = "csv"
    display_name = "CSV / delimited text"
    stream_options_model = CsvOptions
    extensions = (".csv", ".tsv", ".txt")

    def _streams_in(self, data: bytes, path: str) -> list[Stream]:
        return [Stream(id="files", name=Path(self.config.location.rstrip("/")).name or "files", kind="files")]

    def _read(self, data: bytes, stream: str, options: CsvOptions, path: str) -> tuple[pa.Table, pa.Table]:
        try:
            text = data.decode(options.encoding)
        except (UnicodeDecodeError, LookupError) as e:
            raise ConnectorError(f"{path}: cannot decode as {options.encoding}: {e}") from None
        lines = csv.reader(io.StringIO(text), delimiter=options.delimiter, quotechar=options.quote_char)
        if options.header_row:
            header = next((row for i, row in enumerate(lines, start=1) if i == options.header_row), None)
            if header is None:
                raise ConnectorError(f"{path}: header line {options.header_row} is past the end of the file")
            columns = excel.unique_columns([excel.normalize_column(h) for h in header])
        else:
            first = next(lines, [])
            columns = [f"column_{i}" for i in range(1, len(first) + 1)]
        try:
            table = pacsv.read_csv(
                io.BytesIO(text.encode()),
                read_options=pacsv.ReadOptions(column_names=columns, skip_rows=options.header_row),
                parse_options=pacsv.ParseOptions(delimiter=options.delimiter, quote_char=options.quote_char),
                convert_options=pacsv.ConvertOptions(
                    column_types={c: pa.string() for c in columns},
                    null_values=options.null_values,
                    strings_can_be_null=True,
                    quoted_strings_can_be_null=False,
                ),
            )
        except pa.ArrowInvalid as e:
            raise ConnectorError(f"{path}: {e}") from None
        first_data_line = options.header_row + 1
        rows = pa.array(range(first_data_line, first_data_line + table.num_rows), pa.int32())
        return table, pa.table({ROW_NUMBER: rows})
