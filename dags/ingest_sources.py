"""
### Ingest file and API sources (declared in `config/sources.yml`)

* `ingest_files`: every 15 min, new/changed spreadsheets in `s3://landing/` → bronze → silver.
  Unchanged files are skipped (tracked by path + etag), so frequent runs are cheap.
* `ingest_fx_rates`: daily at 06:00 UTC, fetch the trailing FX window ending on the run's date
  → bronze (raw JSON) → silver. Re-fetching a window picks up revised rates.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, task

SALES_TARGETS = Asset("s3://silver/files/sales_targets")
FX_RATES = Asset("s3://silver/api/fx_rates")

DEFAULTS = {"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True}


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["bronze", "silver", "files"],
    doc_md=__doc__,
)
def ingest_files():
    @task
    def ingest_sales_targets() -> dict:
        from dataplatform import pipelines
        from dataplatform.config import Settings

        return pipelines.ingest_files(Settings.from_env(), "sales_targets").__dict__

    @task(outlets=[SALES_TARGETS])
    def build_sales_targets_silver(result: dict) -> int:
        from dataplatform import pipelines
        from dataplatform.config import Settings

        return pipelines.build_reference_silver(Settings.from_env(), "sales_targets")

    build_sales_targets_silver(ingest_sales_targets())


@dag(
    schedule="0 6 * * *",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["bronze", "silver", "api"],
    doc_md=__doc__,
)
def ingest_fx_rates():
    @task
    def fetch_fx(dag_run=None) -> int:
        from dataplatform import pipelines
        from dataplatform.config import Settings

        # The run's business date (not wall-clock time), so re-running a past day fetches that day's window.
        return pipelines.ingest_api(Settings.from_env(), "fx_rates", as_of=dag_run.run_after.date())

    @task(outlets=[FX_RATES])
    def build_fx_silver(_: int) -> int:
        from dataplatform import pipelines
        from dataplatform.config import Settings

        return pipelines.build_reference_silver(Settings.from_env(), "fx_rates")

    build_fx_silver(fetch_fx())


ingest_files()
ingest_fx_rates()
