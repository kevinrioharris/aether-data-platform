"""Turn Debezium change events (JSON, schemas disabled) into bronze rows.

Bronze keeps the event as received: `before`/`after`/`source` stay JSON strings, so nothing is lost
if the source schema changes. Typing and applying the changes happens in silver.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

# Tables that are CDC plumbing, not business data.
IGNORED_TABLES = frozenset({"debezium_signal"})

BRONZE_CDC_SCHEMA = pa.schema(
    [
        pa.field("op", pa.string(), nullable=False),  # c=create, u=update, d=delete, r=snapshot read
        pa.field("record_key", pa.string()),  # primary key as JSON, e.g. {"order_id": 7}
        pa.field("before", pa.string()),
        pa.field("after", pa.string()),
        pa.field("source", pa.string()),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("source_lsn", pa.int64()),
        pa.field("source_txid", pa.int64()),
        pa.field("source_ts_ms", pa.int64()),
        pa.field("is_snapshot", pa.bool_(), nullable=False),
        pa.field("event_ts_ms", pa.int64()),
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingest_date", pa.date32(), nullable=False),
    ]
)


@dataclass(frozen=True)
class CdcRecord:
    op: str
    record_key: str | None
    before: str | None
    after: str | None
    source: str | None
    source_table: str
    source_lsn: int | None
    source_txid: int | None
    source_ts_ms: int | None
    is_snapshot: bool
    event_ts_ms: int | None
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    ingested_at: datetime

    @property
    def ingest_date(self):
        return self.ingested_at.date()


def table_name_from_topic(topic: str) -> str:
    """`shop.public.orders` → `orders` (Debezium topics are `<prefix>.<schema>.<table>`)."""
    return topic.rsplit(".", 1)[-1]


def bronze_table_path(topic: str) -> str:
    """`shop.public.orders` → `shop/orders_cdc` (relative to the bronze bucket)."""
    prefix = topic.split(".", 1)[0]
    return f"{prefix}/{table_name_from_topic(topic)}_cdc"


def _dumps(value) -> str | None:
    return None if value is None else json.dumps(value, separators=(",", ":"), sort_keys=True)


def parse_debezium_message(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    ingested_at: datetime,
) -> CdcRecord | None:
    """Parse one Kafka message. Returns None for tombstones and ignored tables."""
    if value is None:
        return None
    table = table_name_from_topic(topic)
    if table in IGNORED_TABLES:
        return None

    event = json.loads(value)
    source = event.get("source") or {}
    return CdcRecord(
        op=event["op"],
        record_key=_dumps(json.loads(key)) if key else None,
        before=_dumps(event.get("before")),
        after=_dumps(event.get("after")),
        source=_dumps(source),
        source_table=source.get("table", table),
        source_lsn=source.get("lsn"),
        source_txid=source.get("txId"),
        source_ts_ms=source.get("ts_ms"),
        # Debezium sends "true"/"first"/"last"/"incremental" during snapshots, "false" otherwise.
        is_snapshot=str(source.get("snapshot", "false")).lower() != "false",
        event_ts_ms=event.get("ts_ms"),
        kafka_topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
        ingested_at=ingested_at,
    )


def records_to_arrow(records: list[CdcRecord]) -> pa.Table:
    columns = {name: [] for name in BRONZE_CDC_SCHEMA.names}
    for r in records:
        for name in BRONZE_CDC_SCHEMA.names:
            columns[name].append(getattr(r, name))
    return pa.Table.from_pydict(columns, schema=BRONZE_CDC_SCHEMA)
