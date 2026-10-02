# Lakehouse Data Platform

A laptop-sized "mini Databricks": **Postgres CDC (Debezium + Kafka)**, **Excel/CSV**, and **REST APIs** → **Delta Lake** medallion layers on S3-compatible storage (RustFS) → a custom **FastAPI + React** workspace. See [PROJECT_PLAN.md](PROJECT_PLAN.md) for the full design and roadmap.

## Status: Phases 1–2 done + Use Case 1 (sales performance vs. target)

```
Postgres (shop) ──WAL──▶ Debezium ──▶ Kafka ──▶ cdc_consumer ──▶ bronze/shop/<table>_cdc     (raw events, Delta)
                                                                     │  Airflow build_silver (every 15 min)
                                                                     ▼
                                                   silver/shop/<table>            (current state, MERGE)
                                                   silver/shop/<table>_history    (SCD2: customers, products)
```

## Use Case 1: Sales performance vs. target

> *"How much did each product category sell per month in USD, against the targets Sales Ops keeps in
> Excel? Which categories are behind this month, and where will they land at the current pace?"*

| Source | How it arrives | Where it lands |
|---|---|---|
| Orders, order lines, products, customers (Postgres) | Log-based CDC: Debezium → Kafka | `bronze/shop/*_cdc` → `silver/shop/*` |
| Monthly targets per category (Excel, maintained by hand) | File dropped in `s3://landing/excel/sales_targets/` | `bronze/files/sales_targets` → `silver/files/sales_targets` |
| Daily ECB FX rates (REST API) | Daily pull of a trailing 120-day window | `bronze/api/fx_rates` → `silver/api/fx_rates` |

Gold ([models](dataplatform/gold/models/)):
- **`fct_order_items`**: one row per order line of a live, non-cancelled order. Local amounts are converted to USD with an `ASOF JOIN` on the latest ECB rate on or before the order date, which covers weekends and holidays.
- **`mart_sales_vs_target`**: revenue vs. target per month × category, with attainment, a run-rate projection for the current month, and a status (`hit`, `missed`, `on_track`, `at_risk`, `behind`).

Real-world messiness the pipeline handles:
- **Spreadsheet layout:** title rows above the header, a formula-only `FY Total` column, a `Total` row, a number typed as text (`"32,000"`), and a label that differs from the database (`Home & Living` → `Home`, mapped in [sources.yml](config/sources.yml)).
- **Revised targets:** re-uploading an edited spreadsheet is detected by its etag, the newer file supersedes the old one, and gold rebuilds automatically through Airflow Assets.
- **Multi-currency sales:** order lines are priced in the order currency (USD/EUR/GBP/IDR/SGD).

```bash
make use-case-1   # run it all once from the CLI (Airflow does the same on schedule)
make report       # print the report from gold
```

```
September 2026
  total $231,486 vs target $215,000 → 108%
  Sports          $74,626 /   $60,000    124%   ✅ hit
  Fashion         $41,662 /   $35,000    119%   ✅ hit
  Books           $41,783 /   $40,000    104%   ✅ hit
  Home            $30,161 /   $30,000    101%   ✅ hit
  Electronics     $43,254 /   $50,000     87%   ❌ missed
```

**Airflow DAGs:**

| DAG | Schedule | What it does |
|---|---|---|
| `build_silver` | every 15 min | CDC → current state + SCD2, quality gates |
| `ingest_files` | every 15 min | new or changed spreadsheets → bronze → silver (emits an Asset) |
| `ingest_fx_rates` | daily 06:00 UTC | FX API → bronze → silver (emits an Asset) |
| `build_gold` | hourly at :30 **or** when targets/FX assets update | SQL models with tests → gold |

## Quick start

Requires Docker (Docker Desktop or OrbStack) and Python 3.11+.

```bash
make install        # venv + dependencies (also creates .env from .env.example)
make up             # Postgres source, Kafka, Debezium, Kafka UI, RustFS, Airflow
make cdc-register   # register the Debezium connector (waits for Kafka Connect)
make cdc-status     # should show RUNNING
make consume        # terminal 1: Kafka → bronze Delta tables
make generate       # terminal 2: live inserts/updates/deletes in the shop DB
make silver         # build silver now (Airflow also does this every 15 min)
make reconcile      # stop the generator first: silver vs. source DB, value by value
```

| UI | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | — (no login locally) |
| Kafka UI (topics, connector) | http://localhost:8085 | — |
| RustFS console (Delta files) | http://localhost:9001 | lakehouse / lakehouse-secret |
| Shop DB | `localhost:5433` (`make psql`) | shop / shop |

### Query the lakehouse with DuckDB

```sql
INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'lakehouse', SECRET 'lakehouse-secret',
               ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false);
SELECT op, count(*) FROM delta_scan('s3://bronze/shop/orders_cdc') GROUP BY op;
-- price history of one product (SCD2)
SELECT unit_price, _valid_from, _valid_to, _is_current
FROM delta_scan('s3://silver/shop/products_history') WHERE product_id = 14 ORDER BY _valid_from;
```

## Design notes

- **Log-based CDC**: `wal_level=logical`, `pgoutput` plugin, and `REPLICA IDENTITY FULL`, so update and delete events carry the full previous row.
- **Exactly-once into bronze**: the Delta table itself records which Kafka offsets were stored. On restart or rebalance the consumer seeks to `max(offset)+1` and drops any replayed events ([cdc_writer.py](dataplatform/bronze/cdc_writer.py)).
- **Bronze is raw**: `before`/`after` stay JSON strings, so source schema changes never break ingestion. Typing and applying changes happen in silver.
- **Silver = CDC applied correctly** ([cdc_apply.py](dataplatform/silver/cdc_apply.py)): events are ordered by Kafka offset (Debezium keys by primary key, so one row's events stay in order). The current state uses `MERGE ... WHEN MATCHED AND s._seq > t._seq`, with deletes as soft-deletes, so replaying old events can't resurrect a row. SCD2 closes the open version and inserts new ones in a single `MERGE`.
- **Exactly-once into silver**: the bronze watermark is written as a Delta *app transaction* in the same commit as the `MERGE`, so a crash can't apply a batch twice or skip one.
- **Quality gates** ([checks.py](dataplatform/silver/checks.py)): unique and non-null primary keys; exactly one open SCD2 version per live row; valid intervals; history agrees with the current state.
- **Backfills**: insert into `debezium_signal` to trigger a Debezium incremental snapshot without stopping the stream.

## Development

```bash
make test   # pytest
make lint   # ruff
```
