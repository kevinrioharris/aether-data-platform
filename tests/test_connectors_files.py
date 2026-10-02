"""File connectors (Excel, CSV) through the connector SPI, on local folders."""

from datetime import UTC, datetime

import pytest
from openpyxl import Workbook

from dataplatform.bronze.files import ingest_file_source
from dataplatform.connectors import registry
from dataplatform.connectors.base import ConfigurationError, ConnectorError, ExtractRequest, IngestionMode
from dataplatform.connectors.landing import Landing
from dataplatform.connectors.sources import SourceConfig
from dataplatform.lakehouse import delta

NOW = datetime(2026, 10, 2, tzinfo=UTC)


def workbook(path, sheets: dict[str, list[list]], hidden=()):
    wb = Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for row in rows:
            ws.append(row)
        if name in hidden:
            ws.sheet_state = "hidden"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def sales_workbook(path):
    return workbook(
        path,
        {
            "January": [["Region", "Amount"], ["North", 100], ["South", 200]],
            "February": [["February sales"], [], ["Region", "Amount"], ["North", 150], ["West", 75.5], ["Total", 225.5]],
            "March": [["Region", "Amount"], ["North", 300]],
            "Lookups": [["code"], ["x"]],
        },
        hidden={"Lookups"},
    )


# ── Excel: the "upload sales.xlsx → pick February → preview" flow ────────────


def test_excel_sheets_preview_and_schema(tmp_path):
    path = sales_workbook(tmp_path / "uploads" / "sales.xlsx")
    conn = registry.create("excel", {"location": str(path)})

    assert conn.test_connection().ok
    streams = {s.id: s for s in conn.discover_streams()}
    assert list(streams) == ["January", "February", "March", "Lookups"]
    assert streams["Lookups"].metadata["hidden"] and not streams["February"].metadata["hidden"]
    assert conn.default_stream() == "January"

    opts = {"header_row_contains": "Region", "stop_at_first_cell": "Total"}
    preview = conn.preview_data("February", opts)
    assert preview.to_pylist() == [{"region": "North", "amount": "150"}, {"region": "West", "amount": "75.5"}]

    schema = conn.discover_schema("February", opts)
    assert [(c.name, c.data_type) for c in schema.columns] == [("region", "VARCHAR"), ("amount", "DOUBLE")]


def test_excel_header_auto_detect_and_explicit_row(tmp_path):
    path = workbook(tmp_path / "f.xlsx", {"S": [[None], ["Title"], ["a", "b"], ["1", "2"]]})
    conn = registry.create("excel", {"location": str(path)})
    # auto-detect = first non-empty row (the title); an explicit header row fixes it
    assert conn.preview_data("S").column_names == ["title"]
    assert conn.preview_data("S", {"header_row": 3}).to_pylist() == [{"a": "1", "b": "2"}]


def test_excel_duplicate_and_blank_headers_keep_every_column(tmp_path):
    path = workbook(tmp_path / "f.xlsx", {"S": [["Amount", "amount ", "%", "Note"], [1, 2, 3, "x"]]})
    preview = registry.create("excel", {"location": str(path)}).preview_data("S")
    assert preview.to_pylist() == [{"amount": "1", "amount_2": "2", "column_3": "3", "note": "x"}]


def test_excel_missing_sheet_is_a_connector_error(tmp_path):
    path = sales_workbook(tmp_path / "sales.xlsx")
    with pytest.raises(ConnectorError, match="no sheet 'April'"):
        registry.create("excel", {"location": str(path)}).preview_data("April")


def test_excel_incremental_extract_skips_seen_files_and_rereads_changed_ones(tmp_path):
    folder = tmp_path / "drop"
    sales_workbook(folder / "sales_2026.xlsx")
    workbook(folder / "notes.txt.xlsx.bak", {"S": [["x"]]})  # wrong extension: ignored
    conn = registry.create("excel", {"location": f"{folder}/"})

    req = ExtractRequest(stream="January", mode=IngestionMode.INCREMENTAL)
    (batch,) = conn.incremental_extract(req)
    assert batch.source["file"] == "sales_2026.xlsx" and batch.records.num_rows == 2
    assert batch.row_metadata["source_row"].to_pylist() == [2, 3]

    again = ExtractRequest(stream="January", mode=IngestionMode.INCREMENTAL, state=batch.state)
    assert list(conn.extract(again)) == []

    workbook(folder / "sales_2026.xlsx", {"January": [["Region", "Amount"], ["East", 1]]})  # re-uploaded
    (changed,) = conn.extract(again)
    assert changed.records.to_pylist() == [{"region": "East", "amount": "1"}]

    # a full extract ignores state
    assert len(list(conn.extract(ExtractRequest(stream="January", state=changed.state)))) == 1


# ── CSV ──────────────────────────────────────────────────────────────────────


def test_csv_dialect_encoding_and_nulls(tmp_path):
    text = 'Code;Name;Country;Price;Price\n007;"Müller; GmbH";NA;1,5;2\n008;;DE;;3\n'
    (tmp_path / "suppliers.csv").write_bytes(text.encode("latin-1"))
    conn = registry.create("csv", {"location": str(tmp_path / "suppliers.csv")})
    opts = {"delimiter": ";", "encoding": "latin-1"}

    assert [s.id for s in conn.discover_streams()] == ["files"]
    rows = conn.preview_data("files", opts).to_pylist()
    assert rows[0] == {"code": "007", "name": "Müller; GmbH", "country": "NA", "price": "1,5", "price_2": "2"}
    assert rows[1]["name"] is None and rows[1]["price"] is None  # empty → null, but "NA" stays a value

    types = {c.name: c.data_type for c in conn.discover_schema("files", opts).columns}
    assert types == {"code": "VARCHAR", "name": "VARCHAR", "country": "VARCHAR", "price": "VARCHAR", "price_2": "BIGINT"}


def test_csv_without_header(tmp_path):
    (tmp_path / "a.csv").write_text("1,x\n2,y\n")
    conn = registry.create("csv", {"location": f"{tmp_path}/"})
    (batch,) = conn.extract(ExtractRequest(stream="files", options={"header_row": 0}))
    assert batch.records.column_names == ["column_1", "column_2"]
    assert batch.row_metadata["source_row"].to_pylist() == [1, 2]


def test_csv_header_past_end_of_file(tmp_path):
    (tmp_path / "a.csv").write_text("a,b\n")
    with pytest.raises(ConnectorError, match="header line 5"):
        registry.create("csv", {"location": f"{tmp_path}/"}).preview_data("files", {"header_row": 5})


# ── through the existing bronze file ingestion ──────────────────────────────


def test_csv_source_ingests_into_bronze_like_excel(tmp_path):
    landing = Landing(str(tmp_path / "landing"))
    landing.put("csv/suppliers/prices_2026-10-01.csv", b"sku,price\nA,1.50\nB,2.00\n")
    landing.put("csv/suppliers/readme.md", b"ignored")
    source = SourceConfig(
        name="supplier_prices",
        type="csv",
        options={"landing_prefix": "csv/suppliers/", "file_pattern": r"prices_(?P<price_date>[\d-]+)\.csv$"},
    )
    bronze = str(tmp_path / "bronze")

    first = ingest_file_source(source, landing, bronze, {}, NOW)
    again = ingest_file_source(source, landing, bronze, {}, NOW)
    assert (first.files_seen, first.files_ingested, first.rows, again.files_ingested) == (2, 1, 2, 0)

    rows = delta.query({"b": f"{bronze}/files/supplier_prices"}, "SELECT * FROM b ORDER BY sku", {}).to_pylist()
    assert rows[0] | {"_ingested_at": None, "_file_etag": None} == {
        "sku": "A",
        "price": "1.50",
        "_row_number": 2,
        "_source_file": "csv/suppliers/prices_2026-10-01.csv",
        "_file_etag": None,
        "_file_price_date": "2026-10-01",
        "_ingested_at": None,
    }


def test_excel_bronze_layout_is_unchanged(tmp_path):
    landing = Landing(str(tmp_path / "landing"))
    landing.put("excel/sales/sales_2026.xlsx", sales_workbook(tmp_path / "s.xlsx").read_bytes())
    source = SourceConfig(
        name="sales",
        type="excel",
        options={"landing_prefix": "excel/sales/", "file_pattern": r"sales_(?P<year>\d{4})\.xlsx$", "sheet": "March"},
    )
    ingest_file_source(source, landing, str(tmp_path / "bronze"), {}, NOW)
    table = delta.query({"b": f"{tmp_path}/bronze/files/sales"}, "SELECT * FROM b", {})
    assert table.column_names == [
        "region", "amount", "_row_number", "_sheet", "_source_file", "_file_etag", "_file_year", "_ingested_at",
    ]  # fmt: skip


# ── configuration ────────────────────────────────────────────────────────────


def test_registry_and_config_validation():
    types = [c.type for c in registry.available()]
    assert {"csv", "excel"} <= set(types)
    with pytest.raises(ConfigurationError, match="unknown connector type 'nope'"):
        registry.get("nope")
    with pytest.raises(ConfigurationError, match="file_pattern: Value error, invalid regex"):
        registry.create("csv", {"location": "/tmp/x/", "file_pattern": "("})
    with pytest.raises(ConfigurationError, match="location: Field required"):
        registry.create("csv", {})
    with pytest.raises(ConfigurationError, match="delimiter"):
        registry.get("csv").validate_stream_options({"delimiter": ";;"})

    spec = registry.get("excel").spec()
    assert spec["category"] == "file" and "location" in spec["config_schema"]["properties"]
    assert "header_row" in spec["stream_options_schema"]["properties"]


def test_secrets_are_masked_in_public_config(tmp_path):
    conn = registry.create(
        "csv",
        {"location": f"{tmp_path}/", "s3": {"access_key_id": "AKIA", "secret_access_key": "very-secret"}},
    )
    public = conn.public_config()
    assert public["s3"]["secret_access_key"] == "********"
    assert "very-secret" not in repr(conn.config) and "very-secret" not in str(public)
