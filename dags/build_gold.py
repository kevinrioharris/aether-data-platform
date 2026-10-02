"""
### Build gold (use case 1: sales performance vs. target)

Runs every SQL model in `dataplatform/gold/models/` in dependency order (DuckDB → Delta).
Each model's tests must pass before it is written; a failure stops the run and leaves the previous
gold version in place.

Scheduled hourly at :30 (after `build_silver` at :15/:30/:45/:00 has applied recent CDC events), and
also as soon as new sales targets or FX rates land in silver.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

SALES_TARGETS = Asset("s3://silver/files/sales_targets")
FX_RATES = Asset("s3://silver/api/fx_rates")


@dag(
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("30 * * * *", timezone="UTC"),
        assets=SALES_TARGETS | FX_RATES,
    ),
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["gold", "use-case-1"],
    doc_md=__doc__,
)
def build_gold():
    @task(outlets=[Asset("s3://gold/fct_order_items"), Asset("s3://gold/mart_sales_vs_target")])
    def run_gold_models(dag_run=None) -> dict:
        from dataplatform import pipelines
        from dataplatform.config import Settings

        results = pipelines.build_gold(Settings.from_env(), as_of=dag_run.run_after.date())
        return {r.model: {"rows": r.rows, "tests_passed": len(r.tests)} for r in results}

    run_gold_models()


build_gold()
