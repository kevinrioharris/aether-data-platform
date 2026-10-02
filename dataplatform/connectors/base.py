"""The connector SPI: the only thing the ingestion engine knows about a source.

A connector exposes *streams* (a table, a sheet, an API endpoint, a folder of files) and extracts them as
Arrow record batches. Each batch carries the connector's opaque `state` *after* that batch, so the engine
can commit data and state together and resume exactly there. The engine never interprets the state.

Adding a source (Oracle, Google Sheets, Salesforce, ...) means subclassing `Connector` and registering it
(see `registry.py`); nothing in the ingestion engine changes.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, SecretStr, ValidationError

from dataplatform.core.schema import Column, DatasetSchema


class SourceCategory(StrEnum):
    DATABASE = "database"
    FILE = "file"
    OBJECT_STORAGE = "object_storage"
    API = "api"
    SAAS = "saas"


class IngestionMode(StrEnum):
    FULL = "full"  # re-read everything each run
    INCREMENTAL = "incremental"  # only rows past a cursor (or only new/changed files)
    CDC = "cdc"  # transaction-log change capture (WAL, binlog, SQL Server CDC)
    SNAPSHOT_DIFF = "snapshot_diff"  # full read, diffed against the previous snapshot by key + row hash


class ConnectorError(Exception):
    """A source-side failure (unreachable, auth, unreadable data). Safe to show to users."""


class ConfigurationError(ConnectorError):
    pass


class UnsupportedOperation(ConnectorError):
    pass


class ConnectorConfig(BaseModel):
    """Base for connector configs. Secrets must be `SecretStr` so they never reach logs or API responses."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class NoOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True)
class Capabilities:
    full: bool = True
    incremental_cursor: bool = False  # can filter on a cursor column (updated_at, increasing id)
    incremental_files: bool = False  # can skip files already ingested
    cdc: bool = False  # transaction-log CDC is available *and enabled* on this source
    schema_discovery: bool = True
    preview: bool = True
    size_estimate: bool = False

    @property
    def modes(self) -> list[IngestionMode]:
        modes = [IngestionMode.FULL] if self.full else []
        if self.incremental_cursor or self.incremental_files:
            modes.append(IngestionMode.INCREMENTAL)
        if self.cdc:
            modes.append(IngestionMode.CDC)
        if self.full:
            modes.append(IngestionMode.SNAPSHOT_DIFF)  # done by the engine on top of a full read
        return modes


@dataclass(frozen=True)
class Stream:
    """Something extractable. `id` is stable and unique within a connection (e.g. `public.customers`)."""

    id: str
    name: str
    namespace: tuple[str, ...] = ()  # e.g. (database, schema), (workbook,)
    kind: str = "table"  # table | view | sheet | files | endpoint
    metadata: dict[str, Any] = field(default_factory=dict)  # e.g. {"hidden": True}, {"row_count": 1200}


@dataclass(frozen=True)
class ConnectionTestResult:
    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SizeEstimate:
    bytes: int | None = None
    rows: int | None = None
    files: int | None = None


@dataclass(frozen=True)
class ExtractRequest:
    stream: str
    mode: IngestionMode = IngestionMode.FULL
    options: Mapping[str, Any] = field(default_factory=dict)  # validated against `stream_options_model`
    state: Mapping[str, Any] = field(default_factory=dict)  # from the last committed batch; {} = from scratch
    cursor_field: str | None = None
    batch_size: int = 50_000


@dataclass(frozen=True)
class ExtractBatch:
    """One unit the engine writes and commits atomically together with `state`.

    `records` holds source data only. Provenance that differs per row (e.g. a sheet row number) goes in
    `row_metadata` (same length), and provenance shared by the batch (file, etag, request URL) in `source`.
    """

    records: pa.Table
    state: Mapping[str, Any]
    source: Mapping[str, Any] = field(default_factory=dict)
    row_metadata: pa.Table | None = None


class Connector(ABC):
    type: ClassVar[str]  # registry key, e.g. "postgres", "excel"
    display_name: ClassVar[str]
    category: ClassVar[SourceCategory]
    config_model: ClassVar[type[ConnectorConfig]]
    stream_options_model: ClassVar[type[BaseModel]] = NoOptions

    def __init__(self, config: ConnectorConfig):
        self.config = config

    # ── configuration ───────────────────────────────────────────────────────
    @classmethod
    def validate_configuration(cls, raw: Mapping[str, Any]) -> ConnectorConfig:
        try:
            return cls.config_model.model_validate(dict(raw))
        except ValidationError as e:
            raise ConfigurationError(_format_validation_error(e)) from None

    @classmethod
    def validate_stream_options(cls, raw: Mapping[str, Any]) -> BaseModel:
        try:
            return cls.stream_options_model.model_validate(dict(raw))
        except ValidationError as e:
            raise ConfigurationError(_format_validation_error(e)) from None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any], **deps: Any) -> Connector:
        return cls(cls.validate_configuration(raw), **deps)

    @classmethod
    def spec(cls) -> dict[str, Any]:
        """What the UI needs to render the "configure connection" and "stream options" forms."""
        return {
            "type": cls.type,
            "display_name": cls.display_name,
            "category": cls.category.value,
            "config_schema": cls.config_model.model_json_schema(),
            "stream_options_schema": cls.stream_options_model.model_json_schema(),
        }

    def public_config(self) -> dict[str, Any]:
        """The config with secrets masked; safe for API responses and logs."""
        return _mask_secrets(self.config)

    # ── capabilities & metadata ────────────────────────────────────────────
    def capabilities(self) -> Capabilities:
        """May query the source (e.g. whether `wal_level = logical`), so it is per instance."""
        return Capabilities()

    def get_metadata(self) -> dict[str, Any]:
        return {"type": self.type, "category": self.category.value}

    # ── discovery ───────────────────────────────────────────────────────────
    @abstractmethod
    def test_connection(self) -> ConnectionTestResult: ...

    @abstractmethod
    def discover_streams(self) -> list[Stream]: ...

    @abstractmethod
    def discover_schema(self, stream: str, options: Mapping[str, Any] | None = None) -> DatasetSchema: ...

    @abstractmethod
    def preview_data(self, stream: str, options: Mapping[str, Any] | None = None, limit: int = 100) -> pa.Table: ...

    def estimate_size(self, stream: str, options: Mapping[str, Any] | None = None) -> SizeEstimate | None:
        return None

    # ── extraction ──────────────────────────────────────────────────────────
    @abstractmethod
    def extract(self, request: ExtractRequest) -> Iterator[ExtractBatch]: ...

    def incremental_extract(self, request: ExtractRequest) -> Iterator[ExtractBatch]:
        caps = self.capabilities()
        if not (caps.incremental_cursor or caps.incremental_files):
            raise UnsupportedOperation(f"{self.type} does not support incremental extraction")
        if caps.incremental_cursor and not caps.incremental_files and not request.cursor_field:
            raise ConfigurationError("incremental extraction needs a cursor_field")
        return self.extract(_with_mode(request, IngestionMode.INCREMENTAL))


def _with_mode(request: ExtractRequest, mode: IngestionMode) -> ExtractRequest:
    return ExtractRequest(
        stream=request.stream,
        mode=mode,
        options=request.options,
        state=request.state,
        cursor_field=request.cursor_field,
        batch_size=request.batch_size,
    )


def _format_validation_error(e: ValidationError) -> str:
    return "; ".join(f"{'.'.join(str(p) for p in err['loc']) or 'config'}: {err['msg']}" for err in e.errors())


def _mask_secrets(model: BaseModel) -> dict[str, Any]:
    out = {}
    for name in type(model).model_fields:
        value = getattr(model, name)
        if isinstance(value, SecretStr):
            out[name] = "********" if value.get_secret_value() else ""
        elif isinstance(value, BaseModel):
            out[name] = _mask_secrets(value)
        else:
            out[name] = value
    return out


# ── ingestion-mode recommendation ─────────────────────────────────────────────

_CURSOR_NAME = re.compile(r"^(updated|modified|changed|last_modified|last_updated)(_at|_on|_ts|_date|_time)?$")
_TIMESTAMP_TYPES = ("TIMESTAMP", "DATE", "DATETIME")


@dataclass(frozen=True)
class ModeRecommendation:
    mode: IngestionMode
    reason: str
    cursor_field: str | None = None
    caveats: tuple[str, ...] = ()
    alternatives: tuple[IngestionMode, ...] = ()


def find_cursor_column(schema: DatasetSchema) -> Column | None:
    """A timestamp column that looks maintained on every update (updated_at, modified_on, ...)."""
    return next(
        (
            c
            for c in schema.columns
            if _CURSOR_NAME.match(c.name.lower()) and c.data_type.upper().startswith(_TIMESTAMP_TYPES)
        ),
        None,
    )


def recommend_ingestion_mode(caps: Capabilities, schema: DatasetSchema) -> ModeRecommendation:
    """Pick the strongest mode the source supports. Audit columns are never required: log-based CDC and
    snapshot diffs both capture inserts, updates and deletes without them."""
    alternatives = tuple(caps.modes)
    if caps.cdc and schema.primary_key:
        return ModeRecommendation(
            IngestionMode.CDC,
            "transaction-log CDC is enabled and the table has a primary key: captures inserts, updates and "
            "deletes with low latency, without needing audit columns",
            alternatives=alternatives,
        )
    if caps.incremental_files:
        return ModeRecommendation(
            IngestionMode.INCREMENTAL,
            "only new or changed files are read on each run",
            alternatives=alternatives,
        )
    if caps.incremental_cursor and (cursor := find_cursor_column(schema)):
        return ModeRecommendation(
            IngestionMode.INCREMENTAL,
            f"`{cursor.name}` looks like a last-modified timestamp",
            cursor_field=cursor.name,
            caveats=(
                "hard deletes in the source are not seen; schedule a periodic snapshot_diff to catch them",
                f"rows updated without touching `{cursor.name}` are missed",
            ),
            alternatives=alternatives,
        )
    caveats = () if caps.cdc else ("enable log-based CDC on the source for lower latency and cheaper runs",)
    if caps.full and schema.primary_key:
        return ModeRecommendation(
            IngestionMode.SNAPSHOT_DIFF,
            "no CDC or reliable last-modified column; a full read diffed by primary key and row hash still "
            "detects inserts, updates and deletes",
            caveats=(*caveats, "reads the whole table on every run"),
            alternatives=alternatives,
        )
    return ModeRecommendation(
        IngestionMode.FULL,
        "no primary key, CDC or cursor column: each run reloads the full dataset",
        caveats=(*caveats, "declare a primary key to enable snapshot_diff and change tracking"),
        alternatives=alternatives,
    )
