"""Silver for file and API sources: small reference tables, rebuilt in full on every run.

* `silver/files/sales_targets`: one row per (fiscal_year, month, category) with a typed USD target.
  The newest upload per fiscal year supersedes earlier ones.
* `silver/api/fx_rates`: one row per (rate_date, currency). A later fetch of the same day wins
  (rates can be revised).
"""

from __future__ import annotations

import json
from datetime import date

import pyarrow as pa
from deltalake import write_deltalake

from dataplatform.bronze.api import bronze_api_uri
from dataplatform.bronze.files import bronze_files_uri
from dataplatform.connectors.sources import SourceConfig
from dataplatform.lakehouse import delta, duck

MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


def silver_files_uri(silver_root: str, source: SourceConfig) -> str:
    return f"{silver_root}/files/{source.name}"


def silver_api_uri(silver_root: str, source: SourceConfig) -> str:
    return f"{silver_root}/api/{source.name}"


def _overwrite(uri: str, table: pa.Table, storage: dict[str, str]) -> None:
    write_deltalake(uri, table, mode="overwrite", schema_mode="overwrite", storage_options=storage)


SALES_TARGETS_SQL = f"""
WITH latest_file AS (
    SELECT * FROM bronze
    QUALIFY _ingested_at = max(_ingested_at) OVER (PARTITION BY _file_fiscal_year)
),
long AS (
    UNPIVOT latest_file ON {", ".join(MONTHS)} INTO NAME month_name VALUE target_raw
)
SELECT
    CAST(_file_fiscal_year AS INTEGER)                                              AS fiscal_year,
    make_date(CAST(_file_fiscal_year AS INTEGER),
              list_position({MONTHS}, month_name), 1)                               AS month_start,
    coalesce(a.category, trim(l.category))                                          AS category,
    trim(l.category)                                                                AS source_label,
    CAST(replace(target_raw, ',', '') AS DECIMAL(12, 2))                            AS target_usd,
    _source_file, _row_number, _ingested_at
FROM long l
LEFT JOIN aliases a ON a.source_label = trim(l.category)
ORDER BY fiscal_year, category, month_start
"""


def build_sales_targets(source: SourceConfig, bronze_root: str, silver_root: str, storage: dict[str, str]) -> int:
    bronze = delta.query({"b": bronze_files_uri(bronze_root, source)}, "SELECT * FROM b", storage)
    aliases = source.get("category_aliases") or {}
    con = duck.connect()
    con.register("bronze", bronze)
    con.register(
        "aliases",
        pa.table({"source_label": list(aliases), "category": list(aliases.values())}, schema=_ALIAS_SCHEMA),
    )
    result = con.execute(SALES_TARGETS_SQL).to_arrow_table()
    _overwrite(silver_files_uri(silver_root, source), result, storage)
    return result.num_rows


_ALIAS_SCHEMA = pa.schema([("source_label", pa.string()), ("category", pa.string())])

FX_SCHEMA = pa.schema(
    [
        ("rate_date", pa.date32()),
        ("base_currency", pa.string()),
        ("currency", pa.string()),
        ("rate", pa.float64()),  # units of `currency` per 1 `base_currency`
        ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def flatten_fx_payload(payload: str, ingested_at) -> list[dict]:
    """Frankfurter time series: {"base": "USD", "rates": {"2026-09-01": {"EUR": 0.86, ...}, ...}}"""
    doc = json.loads(payload)
    return [
        {
            "rate_date": date.fromisoformat(day),
            "base_currency": doc["base"],
            "currency": cur,
            "rate": rate,
            "_ingested_at": ingested_at,
        }
        for day, rates in doc["rates"].items()
        for cur, rate in rates.items()
    ]


def build_fx_rates(source: SourceConfig, bronze_root: str, silver_root: str, storage: dict[str, str]) -> int:
    bronze = delta.query({"b": bronze_api_uri(bronze_root, source)}, "SELECT payload, _ingested_at FROM b", storage)
    rows = [
        row
        for payload, ingested_at in zip(bronze["payload"].to_pylist(), bronze["_ingested_at"].to_pylist(), strict=True)
        for row in flatten_fx_payload(payload, ingested_at)
    ]
    con = duck.connect()
    con.register("fx", pa.Table.from_pylist(rows, schema=FX_SCHEMA))
    result = con.execute(
        """
        SELECT CAST(rate_date AS DATE) AS rate_date, base_currency, currency, rate, _ingested_at FROM fx
        QUALIFY row_number() OVER (PARTITION BY rate_date, base_currency, currency ORDER BY _ingested_at DESC) = 1
        ORDER BY rate_date, currency
        """
    ).to_arrow_table()
    _overwrite(silver_api_uri(silver_root, source), result, storage)
    return result.num_rows
