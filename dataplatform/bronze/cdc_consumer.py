"""Long-running service: Debezium topics in Kafka → bronze Delta tables on S3 (RustFS).

Micro-batches: collect up to N messages or T seconds, write one Delta commit per table, then commit
Kafka offsets. Run with `python -m dataplatform.bronze.cdc_consumer`.
"""

from __future__ import annotations

import logging
import signal
import time
from collections import defaultdict
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition

from dataplatform.bronze.cdc_events import bronze_table_path, parse_debezium_message, records_to_arrow
from dataplatform.bronze.cdc_writer import (
    PartitionKey,
    append_batch,
    committed_offsets,
    drop_already_written,
    max_offsets,
)
from dataplatform.config import Settings
from dataplatform.lakehouse.storage import storage_options

log = logging.getLogger("cdc_consumer")


class BronzeCdcConsumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.storage = storage_options(settings, settings.bronze_uri)
        self.committed: dict[PartitionKey, int] = {}
        self._loaded_tables: set[str] = set()
        self._running = True
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.cdc_consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                # Pick up new tables' topics quickly when they appear.
                "topic.metadata.refresh.interval.ms": 30000,
            }
        )

    def table_uri(self, topic: str) -> str:
        return f"{self.settings.bronze_uri}/{bronze_table_path(topic)}"

    def _load_committed(self, topic: str) -> None:
        uri = self.table_uri(topic)
        if uri not in self._loaded_tables:
            self.committed.update(committed_offsets(uri, self.storage))
            self._loaded_tables.add(uri)

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            self._load_committed(tp.topic)
            last = self.committed.get((tp.topic, tp.partition))
            if last is not None:
                tp.offset = last + 1
            log.info("assigned %s[%d] from offset %s", tp.topic, tp.partition, tp.offset)
        consumer.assign(partitions)

    def stop(self, *_):
        log.info("shutdown requested, flushing current batch")
        self._running = False

    def _collect_batch(self) -> list:
        messages, deadline = [], time.monotonic() + self.settings.cdc_batch_max_seconds
        while self._running and len(messages) < self.settings.cdc_batch_max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            polled = self.consumer.consume(
                num_messages=min(500, self.settings.cdc_batch_max_messages - len(messages)),
                timeout=min(1.0, remaining),
            )
            for msg in polled:
                err = msg.error()
                if err is None:
                    messages.append(msg)
                elif err.code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", err)
        return messages

    def _flush(self, messages: list) -> None:
        ingested_at = datetime.now(UTC)
        by_topic: dict[str, list] = defaultdict(list)
        for msg in messages:
            record = parse_debezium_message(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key(),
                value=msg.value(),
                ingested_at=ingested_at,
            )
            if record is not None:
                by_topic[record.kafka_topic].append(record)

        for topic, records in by_topic.items():
            self._load_committed(topic)
            batch = drop_already_written(records_to_arrow(records), self.committed)
            if batch.num_rows == 0:
                continue
            append_batch(self.table_uri(topic), batch, self.storage)
            self.committed.update(max_offsets(batch))
            log.info("wrote %d events to %s", batch.num_rows, self.table_uri(topic))

        # Delta commits succeeded; now advance the consumer group (for monitoring / lag).
        self.consumer.commit(asynchronous=False)

    def run(self) -> None:
        self.consumer.subscribe([self.settings.cdc_topic_pattern], on_assign=self._on_assign)
        log.info("subscribed to %s", self.settings.cdc_topic_pattern)
        try:
            while self._running:
                messages = self._collect_batch()
                if messages:
                    self._flush(messages)
        finally:
            self.consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    consumer = BronzeCdcConsumer(Settings.from_env())
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()


if __name__ == "__main__":
    main()
