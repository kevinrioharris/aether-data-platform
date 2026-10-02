"""Runtime settings, read from environment variables (and `.env` when present)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional inside containers
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    cdc_topic_pattern: str
    cdc_consumer_group: str
    cdc_batch_max_messages: int
    cdc_batch_max_seconds: float

    s3_endpoint_url: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str
    bronze_uri: str
    silver_uri: str
    gold_uri: str
    landing_uri: str
    sources_file: Path

    shop_db_host: str
    shop_db_port: int
    shop_db_name: str
    shop_db_user: str
    shop_db_password: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            cdc_topic_pattern=_env("CDC_TOPIC_PATTERN", r"^shop\.public\..*"),
            cdc_consumer_group=_env("CDC_CONSUMER_GROUP", "bronze-cdc-consumer"),
            cdc_batch_max_messages=int(_env("CDC_BATCH_MAX_MESSAGES", "5000")),
            cdc_batch_max_seconds=float(_env("CDC_BATCH_MAX_SECONDS", "30")),
            s3_endpoint_url=_env("S3_ENDPOINT_URL", "http://localhost:9000"),
            s3_region=_env("S3_REGION", "us-east-1"),
            s3_access_key=_env("S3_ACCESS_KEY", "lakehouse"),
            s3_secret_key=_env("S3_SECRET_KEY", "lakehouse-secret"),
            bronze_uri=_env("LAKEHOUSE_BRONZE_URI", "s3://bronze").rstrip("/"),
            silver_uri=_env("LAKEHOUSE_SILVER_URI", "s3://silver").rstrip("/"),
            gold_uri=_env("LAKEHOUSE_GOLD_URI", "s3://gold").rstrip("/"),
            landing_uri=_env("LAKEHOUSE_LANDING_URI", "s3://landing").rstrip("/"),
            sources_file=Path(_env("SOURCES_FILE", str(PROJECT_ROOT / "config" / "sources.yml"))),
            shop_db_host=_env("SHOP_DB_HOST", "localhost"),
            shop_db_port=int(_env("SHOP_DB_PORT", "5433")),
            shop_db_name=_env("SHOP_DB_NAME", "shop"),
            shop_db_user=_env("SHOP_DB_USER", "shop"),
            shop_db_password=_env("SHOP_DB_PASSWORD", "shop"),
        )

    @property
    def shop_db_conninfo(self) -> str:
        return (
            f"host={self.shop_db_host} port={self.shop_db_port} dbname={self.shop_db_name} "
            f"user={self.shop_db_user} password={self.shop_db_password}"
        )
