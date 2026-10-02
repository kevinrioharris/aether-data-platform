# Architecture Assessment & Evolution Roadmap

Status: living document. Phases 1–3 (audit, gap analysis, minimum changes) written 2026-10-02.
The phase log at the bottom records what each implementation phase changed.

---

## 1. Current architecture

```
                         ┌──────────────────────── Docker Compose ─────────────────────────┐
 Postgres "shop" ──WAL──▶ Debezium (Kafka Connect) ──▶ Kafka ──▶ cdc-consumer (Python service)
                                                                        │ exactly-once (offsets in Delta)
 Excel in s3://landing ──▶ Airflow: ingest_files ──┐                    ▼
 Frankfurter REST API  ──▶ Airflow: ingest_fx_rates┼──────────▶ BRONZE  (Delta on RustFS/S3)
                                                   │                    │ Airflow: build_silver (MERGE, SCD2)
                                                   └─ hand-coded ──▶ SILVER
                                                      reference builders │ Airflow: build_gold (SQL runner)
                                                                         ▼
                                                                       GOLD ──▶ CLI report
```

| Area | What exists | Where |
|---|---|---|
| Repository | One Python package (`dataplatform`), Airflow DAGs, scripts, infra, 3 test files. **Not a git repository.** | — |
| Backend / API | None. Only CLI (`dataplatform.cli`, `silver.run`, `gold.report`) | — |
| Frontend | None | — |
| Storage | Delta Lake via delta-rs on RustFS (S3 API); buckets `landing/bronze/silver/gold`; local paths in tests | `lakehouse/` |
| Compute | DuckDB (transforms), delta-rs QueryBuilder (reads) | `lakehouse/delta.py`, `lakehouse/duck.py` |
| Ingestion: CDC | Debezium → Kafka → long-running consumer → `bronze/shop/<table>_cdc`; offsets recovered from the Delta table | `bronze/cdc_*.py` |
| Ingestion: files | Excel only: landing prefix + regex, header detection, (path, etag) dedup → `bronze/files/<source>` (all strings) | `connectors/excel.py`, `bronze/files.py` |
| Ingestion: API | One windowed GET with retries → raw payload row in `bronze/api/<source>` | `connectors/rest_api.py`, `bronze/api.py` |
| Silver | CDC → current state (soft deletes) + SCD2, watermark committed atomically as Delta app transactions. Files/API → two hand-written builders | `silver/cdc_apply.py`, `silver/reference.py` |
| Gold | dbt-style SQL runner: refs → DAG → lineage edges, header-declared tests, full refresh | `gold/runner.py`, `gold/models/` |
| Orchestration | 4 hand-written Airflow 3 DAGs, Asset-triggered gold | `dags/` |
| Metadata / catalog | None persisted. Source config in `config/sources.yml`; CDC table schemas hardcoded in `silver/specs.py` | — |
| Data quality | Hardcoded silver gates (PK, SCD2 invariants); gold header tests; results only in logs/XCom | `silver/checks.py`, `gold/runner.py` |
| Lineage | Computed on the fly from gold SQL only | `gold/runner.lineage()` |
| Auth / secrets | None. Credentials in `.env` (identical to `.env.example`) and as defaults in `config.py` | `config.py` |
| Observability | `logging.basicConfig` text logs; Kafka UI; Airflow UI | — |
| Tests | 13 offline tests (bronze CDC, silver MERGE/SCD2, use case end to end). All pass; ruff clean | `tests/` |
| Deployment | Compose: source PG, Kafka (KRaft), Debezium 2.7, Kafka UI, RustFS, Airflow 3.3 standalone, consumer | `docker-compose.yml` |

## 2. What works and should be preserved

These parts are correct and carefully engineered. Keep them, and generalize around them rather than rewriting them:

1. **Exactly-once CDC into bronze** (`cdc_writer.py`): the Delta table is the source of truth for Kafka offsets.
2. **Exactly-once CDC into silver** (`cdc_apply.py`): watermark committed in the same Delta commit as the MERGE; `_seq`-guarded MERGE makes replays harmless; SCD2 in one MERGE. This is the core of the future generic silver engine.
3. **Bronze stores raw data**: JSON `before/after` for CDC, strings for files, raw payloads for APIs. Schema changes never break ingestion.
4. **The messy-Excel reader** (title rows, total rows, formula columns, numbers as text).
5. **The landing abstraction over obstore** (local and S3 behind one interface).
6. **The gold SQL runner**: refs, topological order, lineage edges, tests gate the write.
7. **The `delta.query` workaround** for the pyarrow thread-pool deadlock (documented, keep it).
8. **Orchestrator-agnostic entry points** (`pipelines.py`): DAGs are thin wrappers.
9. **Business-date discipline** (`as_of` from the run, no wall-clock time in logic).
10. The test style: real Delta tables on `tmp_path`, no mocks of the storage layer.

## 3. What is incomplete or missing

| Capability (from the target spec) | State |
|---|---|
| Connector abstraction / plugin registry | **Missing.** `SourceConfig.type` is a string nothing dispatches on |
| Database connectors (batch: full / incremental) | Missing. The only DB path is Debezium CDC for one hardcoded DB |
| MySQL / SQL Server CDC | Missing (Debezium supports both; the consumer is almost generic) |
| Snapshot / hash-comparison mode | Missing |
| Ingestion-mode recommendation | Missing |
| CSV, JSON, XML, Parquet | Missing (CSV planned, not built) |
| Excel: sheet selection UI, header row, ranges, merged cells, hidden sheets, duplicate headers | Partial. **Duplicate headers silently lose a column** (see §4.6) |
| REST: auth, headers, POST, pagination, record path, rate limit | Missing (GET + retries only) |
| Standard bronze envelope (`__event_id`, `__batch_id`, `__operation`, `__schema_version`, …) | Missing. Three different ad-hoc bronze shapes |
| Run IDs / batch IDs | Missing |
| Replay bronze → silver as an operation | Implicit only (delete silver tables) |
| Generic silver (typing, dedupe, delete mode, schema evolution) | CDC part generic in code but driven by hardcoded specs; files/API hand-coded |
| Schema registry / versions / drift policy | Missing. Drift is silently accepted (see §4.4) |
| Catalog, column metadata, tags, owners, sensitivity | Missing (only `description`/`owner` in YAML) |
| Lineage store (source → bronze → silver → gold) | Missing (gold-only, not stored) |
| Configurable DQ rules + persisted results | Missing |
| Pipeline abstraction, run history, metrics | Missing (Airflow DAGs per source) |
| Control plane API | Missing |
| Frontend | Missing |
| AuthN/Z, RBAC, secrets management | Missing |
| Structured logging, metrics, CDC lag, freshness | Missing |

## 4. Architectural problems and technical debt

1. **Source-specific logic inside the engine.** `pipelines.REFERENCE_BUILDERS`, `silver/reference.py` (month unpivot for one spreadsheet, Frankfurter JSON flattening), `bronze/api.py` (date-window URL template), DAGs naming `sales_targets`/`fx_rates`, `Settings.shop_db_*`, `SHOP_TABLES`. Adding a source today means editing engine code in 3–5 places.
2. **Schemas are declared twice.** `silver/specs.py` hand-copies the source DB's column types; they should be discovered and versioned.
3. **Inconsistent bronze contracts.** CDC uses `ingested_at`, files `_ingested_at`, APIs `request_url`; no batch/run id anywhere. Replay and lineage can't be done generically.
4. **Silent schema changes.**
   - Silver file/API tables are rebuilt with `schema_mode="overwrite"`: a renamed column silently replaces the old one.
   - Silver CDC selects only spec columns, so new source columns are silently dropped and removed ones become NULL.
   - Bronze files use `schema_mode="merge"`. That is fine for bronze, but nothing records that the schema changed.
5. **No control-plane state.** Run results go to XCom and logs. There is no catalog, no run history, no lineage store, so nothing for a UI to read.
6. **Bug: duplicate spreadsheet headers lose data.** `bronze/files.py` builds `{name: column}`, so two headers that normalize to the same name keep only the last column, with no error. A header like `%` normalizes to `""`.
7. **Credentials.** Plaintext in `.env` and as defaults in `config.py` (`lakehouse-secret`). Fine for a laptop demo, but it must not become the pattern for user-added connections.
8. **Single-writer assumption.** `AWS_S3_ALLOW_UNSAFE_RENAME=true` relies on one writer per Delta table, enforced today by `max_active_runs=1`. Once the control plane can trigger runs, the platform must enforce one active run per target table (a lease in the metadata DB).
9. **Whole-table reads into memory** (`_already_ingested`, reference builders, gold inputs). Acceptable at laptop scale. New code should stream record batches so this doesn't spread.
10. **Dependency drift.** `pyproject.toml` is unpinned (local venv runs Python 3.14), while the Airflow image pins `deltalake==1.6.6` / `duckdb==1.5.6`. There is no lock file.
11. **No version control.** Refactors are not reversible. Run `git init` and commit a baseline before phase 4.

## 5. Recommended target architecture

```
┌──────────────────────────── CONTROL PLANE ──────────────────────────────┐
│  Web UI (React + Vite + TS)                                             │
│        │ REST/JSON only                                                 │
│  Platform API (FastAPI)                                                 │
│   connections · connectors · datasets · schemas · pipelines · runs ·   │
│   quality · lineage · catalog · users                                   │
│        │                                                                │
│  Metadata DB (Postgres; SQLAlchemy + Alembic)  Secrets (encrypted,      │
│   pipeline defs, run history, schema versions,  pluggable: env/Vault)   │
│   catalog, lineage edges, DQ results, RBAC                              │
└────────┬───────────────────────────────────────────────▲────────────────┘
         │ trigger run(pipeline_id)  (Airflow REST API)   │ run events, metrics,
         ▼                                                │ schema versions, lineage
┌──────────────────────────── DATA PLANE ─────────────────┴───────────────┐
│  Executor: Airflow (one generic `run_pipeline` DAG + schedules from DB) │
│            CDC consumer service (Kafka → bronze), Debezium per source   │
│                                                                         │
│  Connector SPI ── registry (built-ins + entry-point plugins)            │
│   Database (PG/MySQL/MSSQL/Oracle) · File (CSV/Excel/JSON/XML/Parquet)  │
│   over any object location (local/S3/MinIO/RustFS) · REST · SaaS…       │
│        │ Arrow record batches + opaque state                            │
│  Ingestion engine: full | incremental | cdc | snapshot-diff            │
│        ▼                                                                │
│  BRONZE  immutable, standard envelope (__batch_id, __operation, …)      │
│        ▼  silver engine: typing, dedupe, MERGE, delete mode, schema     │
│  SILVER    policy, DQ gates   (existing cdc_apply.py generalized)       │
│        ▼  SQL model runner (existing gold runner, also for silver)      │
│  GOLD   facts, dims, marts  → BI / ML / RAG (Delta + DuckDB/SQL API)    │
└─────────────────────────────────────────────────────────────────────────┘
```

Key decisions:

- **Keep Airflow as the executor.** Do not build a scheduler. Pipelines are defined in the metadata DB; one generic, parameterized DAG executes any pipeline, and the control plane triggers it through Airflow's REST API. Scheduled pipelines are materialized as DAGs by a factory that reads the DB. Hand-written DAGs remain valid for bespoke jobs.
- **Every ingestion mode emits change events into bronze** (`__operation` = insert/update/delete/snapshot). Full loads, snapshot diffs and CDC then all flow through the *same* silver MERGE that already exists and is tested.
- **Files are format × location.** One `FileConnector` parses a format and reads through an object-location abstraction (local, S3, MinIO, RustFS, uploads). An "S3 connector" is the same code with S3 credentials, so there is no N×M duplication.
- **The canonical type vocabulary is DuckDB SQL types** (already used by `CdcTableSpec`). Arrow carries the data.
- **Secrets never leave the backend.** Connector configs use `SecretStr`. The metadata DB stores ciphertext (Fernet, key from env/KMS) behind a `SecretProvider` interface, and API responses mask secrets.
- **Source-specific transforms become configuration or models**, not engine code. The sales-targets unpivot becomes a silver SQL model. The FX flattening becomes a REST record-path config plus a small SQL model.
- **The data plane reports to the control plane** through a `RunRecorder` interface (metadata DB repository). It never imports web code.

## 6. Prioritized implementation roadmap

| # | Phase | Delivers | Depends on |
|---|---|---|---|
| 4 | Minimum refactors | `git init`, pin deps, fix the duplicate-header bug, object-location helper shared by landing and connectors | — |
| 5 | **Connector SPI** | `Connector` base, capabilities, streams, `DatasetSchema`, registry with entry-point plugins, ingestion-mode recommender; Excel/CSV as reference impls; file ingestion runs through the SPI | 4 |
| 6 | Database connectors | Postgres/MySQL/SQL Server: test, discover (PK, types), preview, estimate, full + cursor-incremental extract, CDC capability probe, Debezium config generation; CDC consumer generalized (multi-source, binlog/LSN positions) | 5 |
| 7 | File connectors | JSON/NDJSON, XML, Parquet; Excel: header row/range, merged cells, hidden sheets, type overrides; upload endpoint target | 5 |
| 8 | REST connector | auth (basic/bearer/API key/OAuth2 client creds), methods, headers, record path, 4 pagination styles, incremental params, rate limit, retries | 5 |
| 9 | Medallion engine | standard bronze envelope + run/batch ids; snapshot-diff; generic spec-driven silver (delete mode: soft/hard/tombstone); replay; silver SQL models; migrate the 2 reference builders to config | 5–8 |
| 10 | Control plane + catalog | metadata DB, FastAPI skeleton, connections with encrypted secrets, connectors/discovery/preview endpoints, datasets, schema versions + drift policy (accept/reject/warn) | 5, 9 |
| 11 | Pipelines & orchestration | pipeline model, run records, generic Airflow DAG + factory, retries/timeout/deps, backfill/rerun, per-table write lease | 10 |
| 12 | Data quality | rule engine (not_null, unique, pk, accepted values, range, regex, FK, freshness, duplicates), results per run | 9, 11 |
| 13 | Lineage | edges recorded by ingestion, silver and model runner; lineage API | 10, 11 |
| 14 | Frontend | React app: dashboard, sources wizard (10 steps), datasets, pipelines, DQ, lineage graph, catalog, monitoring | 10–13 |
| 15 | Observability, security & CI | structured JSON logs, metrics (rows, latency, CDC lag, freshness, storage), authN + RBAC + dataset permissions, GitHub Actions | all |

## 7. Preserve / refactor / create

**Preserve as-is:** `bronze/cdc_writer.py`, `bronze/cdc_consumer.py` (generalize config only), `silver/cdc_apply.py` (becomes the generic MERGE core), `silver/checks.py` logic, `gold/runner.py`, `lakehouse/delta.py`, `lakehouse/duck.py`, gold SQL models, the `infra/` setup, all existing tests.

**Refactor:**
- `connectors/excel.py`: dedupe headers, optional header row; later merged cells and ranges.
- `connectors/landing.py`: accept explicit S3 options, not just `Settings`.
- `bronze/files.py`: drive ingestion through the connector SPI.
- `bronze/api.py` + `connectors/rest_api.py`: become the REST connector.
- `silver/reference.py`: becomes config + silver SQL models.
- `silver/specs.py`: specs come from discovered, versioned schemas.
- `config.py`: shop settings move to a connection record; no credential defaults.
- `dags/`: generic `run_pipeline` DAG.
- `bronze/cdc_events.py`: source-agnostic positions, standard envelope.

**Create:** `connectors/base.py`, `connectors/registry.py`, `connectors/files.py`, `core/schema.py`, `ingestion/` (engine, modes, snapshot diff), `catalog/` + `metadata/` (models, repositories, migrations), `quality/`, `lineage/`, `app/backend` (FastAPI), `app/frontend` (React), `secrets/`.

---

## Phase log

### Phase 4–5 (part 1): Connector SPI — 2026-10-02

- Added `pydantic>=2.7` (config validation, JSON Schema for UI forms, `SecretStr`).
- New `dataplatform/core/schema.py`: `Column`, `DatasetSchema`, DuckDB-based type inference for string data.
- New `dataplatform/connectors/base.py`: `Connector` SPI (`validate_configuration`, `test_connection`, `discover_streams`, `discover_schema`, `preview_data`, `estimate_size`, `extract`, `incremental_extract`, `get_metadata`), `Capabilities`, `Stream`, `ExtractRequest`/`ExtractBatch`, `recommend_ingestion_mode`.
- New `dataplatform/connectors/registry.py`: built-ins plus third-party plugins via the `dataplatform.connectors` entry-point group.
- New `dataplatform/connectors/files.py`: `FileConnector` over any object location; `ExcelConnector` and `CsvConnector`.
- Fixed: duplicate or blank spreadsheet headers no longer silently drop columns (`a`, `a_2`, `column_3`).
- `bronze/files.py` now ingests through the SPI (bronze layout unchanged); `type: csv` sources work.
- `Landing` takes explicit S3 options (`open_store`), so connectors can read any bucket, not only the platform's.
- Tests: `tests/test_connectors_base.py`, `tests/test_connectors_files.py` (13 → 34 tests, ruff clean).

Verified:
- Live regression against RustFS: re-running `ingest-files sales_targets` (from the host and inside the Airflow container) ingests 0 files. The `(path, etag)` identity of existing bronze rows is preserved, so nothing is re-ingested.
- An Excel connector configured directly on `s3://landing/...` lists sheets, previews data, infers types, and masks the secret key.

Known limits, deferred to phase 7:
- CSV files are read whole into memory.
- `_row_number` for CSV counts records, so it drifts from physical line numbers when quoted fields contain newlines.
- Excel columns with an empty header cell are still skipped, as before.
- Merged cells and cell ranges are not handled yet.
