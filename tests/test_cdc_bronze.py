import json
from datetime import UTC, datetime

from dataplatform.bronze.cdc_events import (
    bronze_table_path,
    parse_debezium_message,
    records_to_arrow,
)
from dataplatform.bronze.cdc_writer import (
    append_batch,
    committed_offsets,
    drop_already_written,
    max_offsets,
)

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
TOPIC = "shop.public.orders"


def debezium_event(op, before=None, after=None, lsn=100, snapshot="false"):
    return json.dumps(
        {
            "before": before,
            "after": after,
            "source": {"table": "orders", "lsn": lsn, "txId": 7, "ts_ms": 1, "snapshot": snapshot},
            "op": op,
            "ts_ms": 2,
        }
    ).encode()


def parse(offset, value, partition=0, key=b'{"order_id":1}'):
    return parse_debezium_message(
        topic=TOPIC, partition=partition, offset=offset, key=key, value=value, ingested_at=NOW
    )


def test_bronze_table_path():
    assert bronze_table_path("shop.public.order_items") == "shop/order_items_cdc"


def test_parse_update_keeps_before_and_after():
    rec = parse(5, debezium_event("u", before={"status": "paid"}, after={"status": "shipped"}, lsn=42))
    assert rec.op == "u"
    assert json.loads(rec.before) == {"status": "paid"}
    assert json.loads(rec.after) == {"status": "shipped"}
    assert rec.record_key == '{"order_id":1}'
    assert rec.source_lsn == 42
    assert rec.is_snapshot is False


def test_parse_snapshot_read_is_flagged():
    assert parse(0, debezium_event("r", after={"order_id": 1}, snapshot="true")).is_snapshot is True


def test_tombstones_and_signal_table_are_skipped():
    assert parse(1, None) is None
    signal = parse_debezium_message(
        topic="shop.public.debezium_signal",
        partition=0,
        offset=0,
        key=None,
        value=debezium_event("c", after={}),
        ingested_at=NOW,
    )
    assert signal is None


def test_drop_already_written_filters_per_partition():
    batch = records_to_arrow(
        [parse(o, debezium_event("c", after={"order_id": o}), partition=p) for p in (0, 1) for o in range(5)]
    )
    kept = drop_already_written(batch, {(TOPIC, 0): 2})
    # partition 0 keeps offsets 3,4; partition 1 has nothing committed, keeps all 5
    assert kept.num_rows == 7
    assert max_offsets(kept) == {(TOPIC, 0): 4, (TOPIC, 1): 4}


def test_offsets_recovered_from_delta_table(tmp_path):
    uri = str(tmp_path / "orders_cdc")
    assert committed_offsets(uri, {}) == {}

    append_batch(uri, records_to_arrow([parse(o, debezium_event("c", after={})) for o in range(3)]), {})
    append_batch(uri, records_to_arrow([parse(o, debezium_event("u", after={})) for o in (3, 4)]), {})
    assert committed_offsets(uri, {}) == {(TOPIC, 0): 4}

    # A replay after a crash (offsets 3..6) only writes the new events 5 and 6.
    replay = records_to_arrow([parse(o, debezium_event("u", after={})) for o in range(3, 7)])
    assert drop_already_written(replay, committed_offsets(uri, {})).num_rows == 2
