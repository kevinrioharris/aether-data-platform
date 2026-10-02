# Lakehouse Data Platform — Project Plan

A self-hosted, laptop-sized "mini Databricks": ingest from **databases (with CDC)**, **Excel/CSV files**, and **REST APIs**, organize everything with a **medallion architecture (Bronze / Silver / Gold)** on **Delta Lake**, orchestrate with **Airflow**, and explore it all through a **custom web workspace** (SQL editor, data catalog, pipeline runs, data quality).

---

## 0. Decisions (locked)

| Decision | Choice | Why |
|---|---|---|
| Scope | Generic multi-source platform with a sample e-commerce business | Shows real-world patterns (CDC, files, APIs) instead of a single API |
| CDC | **Debezium + Kafka** (log-based, Postgres WAL) | Industry standard, captures deletes, exactly how production CDC works |
| Table format | **Delta Lake** on RustFS, an S3-compatible store (via `delta-rs`, no Spark) | ACID, `MERGE` for CDC upserts, time travel, schema evolution |
| Compute | **DuckDB + Polars** | Fast, runs on a laptop, SQL for the editor |
| Orchestration | **Apache Airflow** | Most in-demand orchestrator |
| Workspace app | **FastAPI + React (Vite + TypeScript)** | Real-product feel like Databricks/Snowflake |
| Infra | Docker Compose | One command to start everything |

Open (decide when you reach that phase):
- [ ] GitHub repo name (e.g. `lakehouse-data-platform`)
- [ ] Add a second CDC database (MySQL) in Phase 6?

---

## 1. Architecture

```
 SOURCES                      INGESTION                         LAKEHOUSE (S3/RustFS, Delta)                SERVE
 ───────                      ─────────                         ─────────────────────────────                  ─────
 Postgres "shop" DB ──WAL──▶ Debezium (Kafka Connect) ─▶ Kafka ─▶ CDC consumer ─┐
   (customers, products,                                                        │
    orders, order_items)                                                         ▼
                                                                          [BRONZE]  raw, append-only        ┐
 Excel / CSV files ──drop──▶ s3://landing/ ─▶ file ingestor (Airflow) ──▶  + ingest metadata               │
                                                                                 │                          │   Workspace app
 REST API (FX rates) ───────▶ API ingestor (Airflow) ───────────────────────────┘                          │   (FastAPI + React)
                                                                                 ▼                          ├─▶  • SQL editor (DuckDB)
                                                                          [SILVER]  typed, deduped,         │    • Data catalog + lineage
                                                                           CDC applied via MERGE,           │    • Sources & connectors
                                                                           SCD2 history for dims            │    • Pipeline runs
                                                                                 ▼                          │    • Data quality results
                                                                          [GOLD]    facts, dims, KPIs       ┘

 Airflow orchestrates batch jobs; the CDC consumer runs continuously (micro-batches).
 A platform metadata DB (Postgres) stores sources, runs, DQ results, and lineage for the app.
```

### Sample business: an online shop
| Source | Type | Data |
|---|---|---|
| `shop` Postgres | Database + **CDC** | `customers`, `products`, `orders`, `order_items` (a generator script inserts/updates/deletes continuously) |
| `landing/excel/` | Excel (`.xlsx`) | Monthly **sales targets** per category, **marketing spend** per channel |
| `landing/csv/` | CSV | **Supplier price list** |
| Frankfurter API | REST (free, no key) | Daily **FX rates** (convert order totals to USD) |

### Tech stack
| Layer | Tool |
|---|---|
| Source DB | Postgres 16 (`wal_level=logical`) |
| CDC | Debezium 2.x on Kafka Connect, Kafka (KRaft, no Zookeeper) |
| Object storage | RustFS (S3-compatible; MinIO images are no longer published) |
| Table format | Delta Lake (`deltalake` / delta-rs) |
| Compute | DuckDB (`delta` extension) + Polars |
| Orchestration | Apache Airflow 2.x |
| Data quality | Custom rule engine (YAML rules) → results stored in metadata DB |
| Workspace backend | FastAPI |
| Workspace frontend | React + Vite + TypeScript + Monaco editor |
| Infra / CI | Docker Compose, GitHub Actions (ruff, pytest, frontend build) |

---

## 2. Folder structure

```
.
├── docker-compose.yml
├── .env.example
├── Makefile                      # make up / down / seed / cdc-register / generate
├── config/
│   ├── sources.yml               # every source defined declaratively (connector-driven)
│   └── quality_rules.yml         # data quality rules per table
├── infra/
│   ├── postgres-source/init.sql  # shop schema + seed data + publication
│   ├── debezium/shop-connector.json
│   └── (bucket bootstrap runs in the storage-init container)
├── dataplatform/                 # Python package: the pipeline engine
│   ├── connectors/               # postgres_cdc, excel, csv, rest_api
│   ├── lakehouse/                # delta read/write helpers, storage options
│   ├── bronze/                   # CDC consumer, file & API ingestors
│   ├── silver/                   # CDC MERGE, SCD2, cleaning
│   ├── gold/                     # SQL models + runner (with lineage)
│   ├── quality/                  # DQ rule engine
│   └── metadata/                 # platform metadata DB models
├── dags/                         # Airflow DAGs (thin wrappers around platform/)
├── app/
│   ├── backend/                  # FastAPI
│   └── frontend/                 # React
├── scripts/
│   └── generate_activity.py      # simulates live shop traffic → CDC events
├── sample_data/                  # Excel/CSV samples
└── tests/
```

---

## 3. Medallion layer specs

### Bronze — raw, append-only, never modified
- **CDC tables** (`bronze.shop_<table>_cdc`): one row per Debezium event
  `op (c/u/d/r), before (json), after (json), source_lsn, source_ts_ms, kafka_topic, kafka_partition, kafka_offset, ingested_at`
- **File tables** (`bronze.<file_source>`): rows as read (all strings) + `_source_file, _sheet, _row_number, _ingested_at, _batch_id`
- **API tables** (`bronze.<api_source>`): raw JSON payload + `_request_url, _ingested_at`
- Partitioned by `ingest_date`. Idempotent via `(kafka_partition, kafka_offset)` or file checksum.

### Silver — clean, typed, current state
- CDC → **current-state tables** via Delta `MERGE`: order events by `source_lsn`, keep the latest per primary key, apply deletes (`op = d`).
- **SCD Type 2** for `customers` and `products` (`valid_from, valid_to, is_current`).
- Files/APIs: cast types, trim, standardize names, dedupe on business key.
- Quality gates: not-null keys, unique keys, value ranges, referential integrity (`order_items → orders`).

### Gold — business-ready
- `dim_customer` (SCD2), `dim_product` (SCD2), `dim_date`
- `fct_orders`, `fct_order_items` (with USD amounts via FX rates)
- `mart_daily_sales` (revenue, orders, AOV per day)
- `mart_sales_vs_target` (Excel targets vs. actuals per category/month)
- `mart_marketing_roi` (spend vs. revenue per channel)

---

## 4. Pipelines

| Job | Trigger | What it does |
|---|---|---|
| `cdc_consumer` | Always running (service) | Kafka → `bronze.*_cdc` Delta, micro-batch every ~30s, commits offsets after the Delta write |
| `ingest_files` | Every 15 min, or a sensor on `s3://landing/` | New Excel/CSV → bronze, then move the file to `s3://landing/_processed/` |
| `ingest_api_fx` | `0 6 * * *` | FX rates → bronze |
| `build_silver` | Every 15 min (Airflow Datasets after bronze) | CDC MERGE + SCD2 + clean file/API tables + DQ checks |
| `build_gold` | Hourly | Run gold SQL models in dependency order + DQ checks |
| `backfill_*` | Manual | Re-snapshot the DB (Debezium incremental snapshot) / reprocess bronze → silver |

**Rules:** idempotent writes, no `datetime.now()` in logic (use run intervals), retries with backoff, failed quality checks stop downstream tasks, and every run is logged to the metadata DB.

---

## 5. Build phases (one LinkedIn post each)

### Phase 1: Sources + CDC into Bronze
- [x] `docker-compose.yml`: source Postgres, Kafka (KRaft), Kafka Connect + Debezium, Kafka UI, RustFS object storage
- [x] Shop schema + seed data + logical replication publication
- [x] Register the Debezium connector; see topics in Kafka UI
- [x] `generate_activity.py`: live inserts/updates/deletes
- [x] `cdc_consumer`: Kafka → bronze Delta tables on S3 (RustFS) (idempotent)
- [ ] 📣 "Log-based CDC from Postgres to a Delta Lake with Debezium + Kafka"

### Phase 2: Silver (MERGE + SCD2) + Airflow
- [x] Add Airflow 3 to compose (`airflow standalone`, LocalExecutor, own Postgres metadata DB)
- [x] CDC → current-state MERGE (soft deletes); SCD2 history for customers/products
- [x] Watermark stored atomically in each silver commit (Delta app transactions), so each event is applied exactly once
- [x] Prove correctness: `make reconcile` compares silver with the source DB value by value (0 mismatches)
- [x] pytest for MERGE/SCD2 logic (out-of-order events, delete-then-reinsert, reruns)
- [x] `build_silver` DAG (every 15 min): mapped `apply_cdc` per table → `quality_checks` gate
- [ ] 📣 "Applying CDC correctly: ordering, deletes, and SCD Type 2"

### Phase 3: Files + APIs (connector framework)
- [x] `config/sources.yml`-driven connectors ([connectors/](dataplatform/connectors/)): landing zone (obstore), Excel, REST with retries
- [x] Excel ingestion: header detection, summary-row/formula-column skipping, re-upload detection (etag), schema merge
- [x] FX API ingestion (trailing window, revisions win) + ASOF conversion in gold
- [ ] CSV connector (supplier price list)
- [ ] 📣 "One config file, many sources: building a pluggable ingestion framework"

### Phase 4: Gold + Data Quality + Lineage
- [x] SQL model runner ([gold/runner.py](dataplatform/gold/runner.py)): refs → DAG → lineage, header-declared tests
- [x] **Use Case 1**: `fct_order_items`, `mart_sales_vs_target` + `make report`
- [x] Airflow: `ingest_files`, `ingest_fx_rates`, `build_gold` (cron **or** Asset-triggered)
- [ ] More marts: `mart_daily_sales`, `mart_marketing_roi` (needs the marketing spend Excel), `dim_date`
- [ ] Persist run/test results to a metadata DB for the app
- [ ] 📣 "Medallion architecture end to end: from WAL + Excel + API to business KPIs"

### Phase 5: Workspace App (the "mini Databricks")
- [ ] FastAPI: `/query` (DuckDB over Delta, read-only, row limit), `/catalog`, `/tables/{t}/history` (Delta time travel), `/runs`, `/quality`, `/sources`, `/lineage`
- [ ] React: SQL editor (Monaco + results grid + CSV export), catalog tree (bronze/silver/gold), table page (schema, preview, history, DQ), runs page, lineage graph, sources page (upload an Excel file → triggers ingestion)
- [ ] 📣 "I built my own mini Databricks"

### Phase 6: Polish
- [ ] GitHub Actions CI, README with architecture diagram + GIFs
- [ ] Stretch: MySQL as a second CDC source, schema registry + Avro, Delta `OPTIMIZE`/`VACUUM` jobs, freshness SLAs, auth in the app

---

## Next session: start here
Phases 1–2 and Use Case 1 are running. Start everything with `make up`, then `make consume-docker` and `make generate`.
Run the use case with `make use-case-1` and `make report`; Airflow is at http://localhost:8080.
Next options: **Phase 5** (the workspace app: SQL editor, catalog, lineage, runs) or Use Case 2 (marketing ROI).
