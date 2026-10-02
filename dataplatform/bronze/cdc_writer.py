"""Append CDC batches to bronze Delta tables, and recover Kafka offsets from what was written.

Exactly-once into bronze: the Delta table (not Kafka's consumer group) is the source of truth for
which offsets were already stored. On startup/rebalance the consumer seeks to max(offset)+1 per
partition, and any replayed message at or below that offset is dropped before writing.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake

from dataplatform.lakehouse import delta

PartitionKey = tuple[str, int]  # (topic, partition)


def committed_offsets(table_uri: str, storage_options: dict[str, str]) -> dict[PartitionKey, int]:
    """Highest Kafka offset already stored in the table, per (topic, partition)."""
    if not delta.table_exists(table_uri, storage_options):
        return {}
    agg = delta.query(
        {"t": table_uri},
        "SELECT kafka_topic, kafka_partition, max(kafka_offset) AS kafka_offset_max "
        "FROM t GROUP BY kafka_topic, kafka_partition",
        storage_options,
    )
    return {
        (topic, partition): offset
        for topic, partition, offset in zip(
            agg["kafka_topic"].to_pylist(),
            agg["kafka_partition"].to_pylist(),
            agg["kafka_offset_max"].to_pylist(),
            strict=True,
        )
    }


def drop_already_written(batch: pa.Table, committed: dict[PartitionKey, int]) -> pa.Table:
    """Remove rows whose offset is <= the committed offset for their partition."""
    if batch.num_rows == 0 or not committed:
        return batch
    keep = [
        committed.get((topic, partition), -1) < offset
        for topic, partition, offset in zip(
            batch["kafka_topic"].to_pylist(),
            batch["kafka_partition"].to_pylist(),
            batch["kafka_offset"].to_pylist(),
            strict=True,
        )
    ]
    return batch.filter(pa.array(keep, type=pa.bool_()))


def append_batch(table_uri: str, batch: pa.Table, storage_options: dict[str, str]) -> None:
    write_deltalake(
        table_uri,
        batch,
        mode="append",
        partition_by=["ingest_date"],
        storage_options=storage_options,
    )


def max_offsets(batch: pa.Table) -> dict[PartitionKey, int]:
    """Highest offset in the batch per (topic, partition)."""
    if batch.num_rows == 0:
        return {}
    agg = batch.group_by(["kafka_topic", "kafka_partition"]).aggregate([("kafka_offset", "max")])
    return {
        (t, p): o
        for t, p, o in zip(
            agg["kafka_topic"].to_pylist(),
            agg["kafka_partition"].to_pylist(),
            pc.cast(agg["kafka_offset_max"], pa.int64()).to_pylist(),
            strict=True,
        )
    }
