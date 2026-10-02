"""Command-line entry points for the batch pipelines.

    python -m dataplatform.cli upload sample_data/sales_targets_2026.xlsx excel/sales_targets/
    python -m dataplatform.cli ingest-files sales_targets
    python -m dataplatform.cli ingest-api fx_rates [--as-of 2026-09-30]
    python -m dataplatform.cli build-reference sales_targets
    python -m dataplatform.cli build-gold [--as-of 2026-09-30]
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime
from pathlib import Path

from dataplatform import pipelines
from dataplatform.config import Settings
from dataplatform.connectors.landing import Landing


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upload", help="put a local file into the landing zone")
    up.add_argument("file", type=Path)
    up.add_argument("prefix")
    sub.add_parser("ingest-files").add_argument("source")
    api = sub.add_parser("ingest-api")
    api.add_argument("source")
    api.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    sub.add_parser("build-reference").add_argument("source")
    gold = sub.add_parser("build-gold")
    gold.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.cmd == "upload":
        path = f"{args.prefix.rstrip('/')}/{args.file.name}"
        Landing(settings.landing_uri, settings).put(path, args.file.read_bytes())
        print(f"uploaded to {settings.landing_uri}/{path}")
    elif args.cmd == "ingest-files":
        print(pipelines.ingest_files(settings, args.source))
    elif args.cmd == "ingest-api":
        print(f"stored {pipelines.ingest_api(settings, args.source, args.as_of)} bytes")
    elif args.cmd == "build-reference":
        print(f"{args.source}: {pipelines.build_reference_silver(settings, args.source)} rows in silver")
    elif args.cmd == "build-gold":
        for r in pipelines.build_gold(settings, args.as_of):
            print(f"gold.{r.model}: {r.rows} rows, tests: {r.tests}")


if __name__ == "__main__":
    main()
