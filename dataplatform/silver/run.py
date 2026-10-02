"""Run the silver layer from the command line (Airflow calls the same functions).

    python -m dataplatform.silver.run                  # apply CDC for all tables + quality checks
    python -m dataplatform.silver.run --table orders
    python -m dataplatform.silver.run --reconcile      # compare silver with the source DB
"""

from __future__ import annotations

import argparse
import logging
import sys

from dataplatform.config import Settings
from dataplatform.lakehouse.storage import storage_options
from dataplatform.silver.cdc_apply import ApplyResult, apply_current_state, apply_history
from dataplatform.silver.checks import CheckResult, QualityCheckError, check_table
from dataplatform.silver.reconcile import ReconcileResult, reconcile_table
from dataplatform.silver.specs import SHOP_TABLES


def build_table(name: str, settings: Settings) -> list[ApplyResult]:
    spec = SHOP_TABLES[name]
    storage = storage_options(settings, settings.silver_uri)
    results = [apply_current_state(spec, settings.bronze_uri, settings.silver_uri, storage)]
    if spec.scd2:
        results.append(apply_history(spec, settings.bronze_uri, settings.silver_uri, storage))
    return results


def run_checks(names: list[str], settings: Settings) -> list[CheckResult]:
    storage = storage_options(settings, settings.silver_uri)
    results = [r for n in names for r in check_table(SHOP_TABLES[n], settings.silver_uri, storage)]
    failures = [r for r in results if not r.passed]
    if failures:
        raise QualityCheckError(failures)
    return results


def reconcile(names: list[str], settings: Settings) -> list[ReconcileResult]:
    storage = storage_options(settings, settings.silver_uri)
    return [reconcile_table(SHOP_TABLES[n], settings.shop_db_conninfo, settings.silver_uri, storage) for n in names]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=sorted(SHOP_TABLES), action="append")
    parser.add_argument("--reconcile", action="store_true")
    args = parser.parse_args()
    settings = Settings.from_env()
    names = args.table or list(SHOP_TABLES)

    if args.reconcile:
        ok = True
        for r in reconcile(names, settings):
            ok &= r.ok
            print(
                f"{'OK  ' if r.ok else 'FAIL'} {r.table:12} source={r.source_rows:5} silver={r.silver_rows:5} "
                f"missing={len(r.missing_in_silver)} extra={len(r.extra_in_silver)} "
                f"mismatched={len(r.value_mismatches)}"
                + ("" if r.ok else f"  e.g. {(r.missing_in_silver + r.extra_in_silver + r.value_mismatches)[:5]}")
            )
        sys.exit(0 if ok else 1)

    for name in names:
        for r in build_table(name, settings):
            print(f"{r.kind:8} {r.table:12} events={r.events_read:5} inserted={r.inserted:5} updated={r.updated:5}")
    for c in run_checks(names, settings):
        print(f"check    {c.table:12} {c.check:28} {'pass' if c.passed else 'FAIL'}")


if __name__ == "__main__":
    main()
