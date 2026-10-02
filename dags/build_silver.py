"""
### Build silver from bronze CDC

For each shop table: apply new Debezium events from bronze to
* the **current-state** table (`MERGE`, soft deletes), and
* for `customers` / `products`, the **SCD Type 2 history** table.

Then run the silver quality gates. A failed check fails the run.

Silver is *watermark-driven*: every run processes all bronze events since the last successful
commit (the watermark is stored atomically inside each silver Delta commit). That's why
`catchup` is off. There are no time-sliced intervals to backfill; a single run always catches up.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task

TABLES = ["customers", "products", "orders", "order_items"]


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # exactly one writer per silver table
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["silver", "cdc"],
    doc_md=__doc__,
)
def build_silver():
    @task
    def apply_cdc(table: str) -> list[dict]:
        from dataplatform.config import Settings
        from dataplatform.silver.run import build_table

        return [r.as_dict() for r in build_table(table, Settings.from_env())]

    @task
    def quality_checks() -> dict[str, int]:
        from dataplatform.config import Settings
        from dataplatform.silver.run import run_checks

        results = run_checks(TABLES, Settings.from_env())  # raises QualityCheckError on any failure
        return {f"{r.table}.{r.check}": r.failing_rows for r in results}

    apply_cdc.expand(table=TABLES) >> quality_checks()


build_silver()
