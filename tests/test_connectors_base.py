"""Connector SPI pieces that don't depend on a particular source: schema inference, mode recommendation,
and plugin registration."""

import pyarrow as pa
import pytest

from dataplatform.connectors import registry
from dataplatform.connectors.base import (
    Capabilities,
    ConnectionTestResult,
    Connector,
    ConnectorConfig,
    ExtractBatch,
    IngestionMode,
    SourceCategory,
    Stream,
    UnsupportedOperation,
    recommend_ingestion_mode,
)
from dataplatform.core.schema import Column, DatasetSchema, infer_string_types, with_type_overrides


def test_string_type_inference_is_conservative():
    table = pa.table(
        {
            "id": ["1", "-2", None, " "],
            "ratio": ["1.5", "2", None, None],
            "zip": ["007", "12", None, None],
            "flag": ["true", "False", None, None],
            "day": ["2026-01-05", "2026-02-01", None, None],
            "ts": ["2026-01-05 10:00:00", "2026-01-05T11:00:00", None, None],
            "tstz": ["2026-01-05T10:00:00Z", "2026-01-05 10:00:00+07:00", None, None],
            "amount": ["32,000", "1", None, None],
            "empty": pa.array([None] * 4, pa.string()),
        }
    )
    types = {c.name: c.data_type for c in infer_string_types(table).columns}
    assert types == {
        "id": "BIGINT",
        "ratio": "DOUBLE",
        "zip": "VARCHAR",  # leading zeros: an identifier, not a number
        "flag": "BOOLEAN",
        "day": "DATE",
        "ts": "TIMESTAMP",
        "tstz": "TIMESTAMP WITH TIME ZONE",
        "amount": "VARCHAR",  # cleaning "32,000" is a transformation, not inference
        "empty": "VARCHAR",
    }


def test_type_overrides():
    schema = infer_string_types(pa.table({"code": ["1", "2"]}))
    assert with_type_overrides(schema, {"code": "VARCHAR"}).column("code").data_type == "VARCHAR"
    with pytest.raises(ValueError, match="unknown columns"):
        with_type_overrides(schema, {"nope": "INTEGER"})


def test_arrow_schema_maps_to_sql_types():
    schema = DatasetSchema.from_arrow(
        pa.schema([("id", pa.int32()), ("price", pa.decimal128(10, 2)), ("ts", pa.timestamp("us", tz="UTC"))]),
        primary_key=("id",),
    )
    assert [c.data_type for c in schema.columns] == ["INTEGER", "DECIMAL(10,2)", "TIMESTAMP WITH TIME ZONE"]


# ── ingestion-mode recommendation ────────────────────────────────────────────

TABLE = DatasetSchema(
    columns=(Column("id", "INTEGER", nullable=False), Column("name", "VARCHAR")), primary_key=("id",)
)
WITH_UPDATED_AT = DatasetSchema(columns=(*TABLE.columns, Column("updated_at", "TIMESTAMP WITH TIME ZONE")))


def test_cdc_wins_when_enabled_even_without_audit_columns():
    rec = recommend_ingestion_mode(Capabilities(cdc=True, incremental_cursor=True), TABLE)
    assert rec.mode == IngestionMode.CDC


def test_cdc_needs_a_primary_key():
    no_pk = DatasetSchema(columns=TABLE.columns)
    assert recommend_ingestion_mode(Capabilities(cdc=True), no_pk).mode == IngestionMode.FULL


def test_cursor_incremental_when_a_last_modified_column_exists():
    rec = recommend_ingestion_mode(Capabilities(incremental_cursor=True), WITH_UPDATED_AT)
    assert (rec.mode, rec.cursor_field) == (IngestionMode.INCREMENTAL, "updated_at")
    assert any("hard deletes" in c for c in rec.caveats)


def test_legacy_table_without_cdc_or_audit_columns_gets_snapshot_diff():
    rec = recommend_ingestion_mode(Capabilities(incremental_cursor=True), TABLE)
    assert rec.mode == IngestionMode.SNAPSHOT_DIFF
    assert IngestionMode.SNAPSHOT_DIFF in rec.alternatives


def test_files_recommend_new_file_incremental():
    rec = recommend_ingestion_mode(Capabilities(incremental_files=True), DatasetSchema(columns=()))
    assert rec.mode == IngestionMode.INCREMENTAL and rec.cursor_field is None


# ── extensibility ────────────────────────────────────────────────────────────


class EchoConfig(ConnectorConfig):
    rows: int = 2


class EchoConnector(Connector):
    """A whole new source type in ~20 lines, with no engine change."""

    type = "test_echo"
    display_name = "Echo"
    category = SourceCategory.SAAS
    config_model = EchoConfig

    def test_connection(self):
        return ConnectionTestResult(True, "ok")

    def discover_streams(self):
        return [Stream(id="numbers", name="numbers")]

    def discover_schema(self, stream, options=None):
        return DatasetSchema.from_arrow(self.preview_data(stream).schema)

    def preview_data(self, stream, options=None, limit=100):
        return pa.table({"n": list(range(self.config.rows))}).slice(0, limit)

    def extract(self, request):
        yield ExtractBatch(records=self.preview_data(request.stream), state={"done": True})


def test_new_connector_plugs_in_without_engine_changes(monkeypatch):
    monkeypatch.setitem(registry._registry, "test_echo", EchoConnector)
    conn = registry.create("test_echo", {"rows": 3})
    assert conn.preview_data("numbers")["n"].to_pylist() == [0, 1, 2]
    with pytest.raises(UnsupportedOperation):
        conn.incremental_extract(None)
    with pytest.raises(ValueError, match="already registered"):

        class Clash(EchoConnector):
            type = "csv"

        registry.register(Clash)
