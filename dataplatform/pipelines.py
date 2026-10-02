"""Orchestrator-agnostic pipeline entry points. Airflow tasks and the CLI both call these."""

from __future__ import annotations

from datetime import UTC, date, datetime

from dataplatform.bronze.api import ingest_windowed_api
from dataplatform.bronze.files import FileIngestResult, ingest_file_source
from dataplatform.config import Settings
from dataplatform.connectors.landing import Landing
from dataplatform.connectors.sources import load_sources
from dataplatform.gold.runner import ModelResult, run_models
from dataplatform.lakehouse.storage import storage_options
from dataplatform.silver.reference import build_fx_rates, build_sales_targets

REFERENCE_BUILDERS = {"sales_targets": build_sales_targets, "fx_rates": build_fx_rates}


def _storage(settings: Settings) -> dict[str, str]:
    return storage_options(settings, settings.bronze_uri)


def ingest_files(settings: Settings, source_name: str) -> FileIngestResult:
    source = load_sources(settings.sources_file)[source_name]
    landing = Landing(settings.landing_uri, settings)
    return ingest_file_source(source, landing, settings.bronze_uri, _storage(settings), datetime.now(UTC))


def ingest_api(settings: Settings, source_name: str, as_of: date) -> int:
    source = load_sources(settings.sources_file)[source_name]
    return ingest_windowed_api(
        source, settings.bronze_uri, _storage(settings), as_of=as_of, ingested_at=datetime.now(UTC)
    )


def build_reference_silver(settings: Settings, source_name: str) -> int:
    source = load_sources(settings.sources_file)[source_name]
    return REFERENCE_BUILDERS[source_name](source, settings.bronze_uri, settings.silver_uri, _storage(settings))


def build_gold(settings: Settings, as_of: date) -> list[ModelResult]:
    return run_models(settings.silver_uri, settings.gold_uri, _storage(settings), as_of=as_of)
