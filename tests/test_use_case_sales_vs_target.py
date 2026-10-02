"""Use case 1 end to end, offline: Excel + FX API + silver shop tables → gold mart."""

import importlib.util
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataplatform.bronze.api import ingest_windowed_api
from dataplatform.bronze.files import ingest_excel_source
from dataplatform.config import PROJECT_ROOT
from dataplatform.connectors.excel import read_excel_table
from dataplatform.connectors.landing import Landing
from dataplatform.connectors.sources import load_sources
from dataplatform.gold.runner import lineage, load_models, run_models
from dataplatform.lakehouse import delta
from dataplatform.silver.reference import build_fx_rates, build_sales_targets

SOURCES = load_sources(PROJECT_ROOT / "config" / "sources.yml")
NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)


def _sample_workbook(tmp_path: Path) -> bytes:
    spec = importlib.util.spec_from_file_location("mk", PROJECT_ROOT / "scripts" / "make_sample_excel.py")
    mk = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mk)
    out = tmp_path / "sales_targets_2026.xlsx"
    mk.build(out)
    return out.read_bytes()


def test_excel_reader_finds_the_real_table(tmp_path):
    src = SOURCES["sales_targets"]
    table = read_excel_table(
        _sample_workbook(tmp_path),
        sheet=src["sheet"],
        header_row_contains=src["header_row_contains"],
        stop_at_first_cell=src["stop_at_first_cell"],
        ignore_columns=src["ignore_columns"],
    )
    assert table.columns == ["category", "jan", "feb", "mar", "apr", "may", "jun",
                             "jul", "aug", "sep", "oct", "nov", "dec"]  # fmt: skip
    assert [values[0] for _, values in table.rows] == ["Books", "Electronics", "Fashion", "Home & Living", "Sports"]
    assert table.rows[1][1][8] == "32,000"  # text stays raw in bronze
    assert table.rows[0][0] == 5  # sheet row number kept for lineage


def fake_fx(url, params):
    rates = {"2026-08-31": {"EUR": 0.5, "IDR": 10000.0}, "2026-09-01": {"EUR": 0.8, "IDR": 16000.0}}
    return json.dumps({"base": "USD", "rates": rates}), url


@pytest.fixture
def lake(tmp_path):
    roots = {k: str(tmp_path / k) for k in ["landing", "bronze", "silver", "gold"]}
    landing = Landing(roots["landing"])
    landing.put("excel/sales_targets/sales_targets_2026.xlsx", _sample_workbook(tmp_path))
    landing.put("excel/sales_targets/README.txt", b"not a target file")

    src = SOURCES["sales_targets"]
    first = ingest_excel_source(src, landing, roots["bronze"], {}, NOW)
    again = ingest_excel_source(src, landing, roots["bronze"], {}, NOW)
    assert (first.files_ingested, first.rows, again.files_ingested) == (1, 5, 0)
    build_sales_targets(src, roots["bronze"], roots["silver"], {})

    ingest_windowed_api(SOURCES["fx_rates"], roots["bronze"], {}, as_of=date(2026, 9, 30), ingested_at=NOW, fetch=fake_fx)
    build_fx_rates(SOURCES["fx_rates"], roots["bronze"], roots["silver"], {})

    # Minimal silver shop tables: two Sept orders (EUR on Sunday 6th → Friday rate; IDR), one cancelled.
    def silver(path, rows):
        write_deltalake(f"{roots['silver']}/shop/{path}", pa.Table.from_pylist(rows))

    ts = lambda d: datetime(2026, 9, d, 10, tzinfo=UTC)  # noqa: E731
    silver("orders", [
        {"order_id": 1, "customer_id": 1, "status": "paid", "currency": "EUR", "order_ts": ts(6), "_is_deleted": False},
        {"order_id": 2, "customer_id": 1, "status": "delivered", "currency": "IDR", "order_ts": ts(2), "_is_deleted": False},
        {"order_id": 3, "customer_id": 1, "status": "cancelled", "currency": "USD", "order_ts": ts(2), "_is_deleted": False},
    ])  # fmt: skip
    silver("order_items", [
        {"order_item_id": 10, "order_id": 1, "product_id": 1, "quantity": 2, "unit_price": Decimal("400.00"), "_is_deleted": False},
        {"order_item_id": 11, "order_id": 2, "product_id": 2, "quantity": 1, "unit_price": Decimal("160000000.00"), "_is_deleted": False},
        {"order_item_id": 12, "order_id": 3, "product_id": 1, "quantity": 9, "unit_price": Decimal("999.00"), "_is_deleted": False},
    ])  # fmt: skip
    silver("products", [
        {"product_id": 1, "sku": "A", "category": "Home", "_is_deleted": False},
        {"product_id": 2, "sku": "B", "category": "Electronics", "_is_deleted": False},
    ])  # fmt: skip
    silver("customers", [{"customer_id": 1, "country": "ID", "_is_deleted": False}])
    return roots


def test_sales_vs_target_mart(lake):
    run_models(lake["silver"], lake["gold"], {}, as_of=date(2026, 9, 15))
    fct = delta.query({"f": f"{lake['gold']}/fct_order_items"}, "SELECT * FROM f ORDER BY order_item_id", {})
    rows = {r["order_item_id"]: r for r in fct.to_pylist()}
    assert set(rows) == {10, 11}  # cancelled order excluded
    assert rows[10]["amount_usd"] == Decimal("1000.00")  # 800 EUR / 0.8 (latest rate before Sunday)
    assert rows[11]["amount_usd"] == Decimal("10000.00")  # 160M IDR / 16000

    mart = delta.query(
        {"m": f"{lake['gold']}/mart_sales_vs_target"}, "SELECT * FROM m WHERE category = 'Home'", {}
    ).to_pylist()[0]
    assert mart["target_usd"] == Decimal("38000.00")  # "Home & Living" mapped to Home
    assert mart["period_status"] == "in_progress" and mart["days_elapsed"] == 15
    assert mart["projected_revenue_usd"] == Decimal("2000.00")  # 1000 over 15 of 30 days
    assert mart["status"] == "behind"

    electronics = delta.query(
        {"m": f"{lake['gold']}/mart_sales_vs_target"}, "SELECT target_usd FROM m WHERE category = 'Electronics'", {}
    )
    assert electronics["target_usd"][0].as_py() == Decimal("50000.00")


def test_lineage_is_derived_from_sql():
    edges = set(lineage(load_models()))
    assert ("silver.files_sales_targets", "gold.mart_sales_vs_target") in edges
    assert ("gold.fct_order_items", "gold.mart_sales_vs_target") in edges
    assert ("silver.api_fx_rates", "gold.fct_order_items") in edges
