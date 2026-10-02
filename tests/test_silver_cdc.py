import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from dataplatform.bronze.cdc_events import parse_debezium_message, records_to_arrow
from dataplatform.bronze.cdc_writer import append_batch
from dataplatform.lakehouse import delta
from dataplatform.silver.cdc_apply import apply_current_state, apply_history
from dataplatform.silver.checks import check_table
from dataplatform.silver.specs import SHOP_TABLES

SPEC = SHOP_TABLES["products"]
TOPIC = "shop.public.products"
NOW = datetime(2026, 9, 30, tzinfo=UTC)


def product(pid, price, name=None):
    return {
        "product_id": pid,
        "sku": f"SKU-{pid}",
        "name": name or f"item {pid}",
        "category": "Books",
        "unit_price": str(price),
        "is_active": True,
        "updated_at": "2026-09-30T04:19:37.417093Z",
    }


class Bronze:
    """Writes Debezium-shaped events to a local bronze table with increasing offsets / timestamps."""

    def __init__(self, root):
        self.root = str(root / "bronze")
        self.uri = f"{self.root}/{SPEC.bronze_path}"
        self.offset = 0

    def write(self, *events, order=None):
        records = []
        for op, before, after in events:
            value = {
                "before": before,
                "after": after,
                "op": op,
                "source": {"table": "products", "lsn": self.offset, "ts_ms": 1_790_000_000_000 + self.offset * 1000},
            }
            pid = (after or before)["product_id"]
            records.append(
                parse_debezium_message(
                    topic=TOPIC,
                    partition=0,
                    offset=self.offset,
                    key=json.dumps({"product_id": pid}).encode(),
                    value=json.dumps(value).encode(),
                    ingested_at=NOW,
                )
            )
            self.offset += 1
        if order:  # write in a different physical order to prove ordering comes from _seq
            records = [records[i] for i in order]
        append_batch(self.uri, records_to_arrow(records), {})


@pytest.fixture
def lake(tmp_path):
    bronze = Bronze(tmp_path)
    silver_root = str(tmp_path / "silver")

    def run():
        return (
            apply_current_state(SPEC, bronze.root, silver_root, {}),
            apply_history(SPEC, bronze.root, silver_root, {}),
        )

    def current():
        rows = delta.query({"t": f"{silver_root}/{SPEC.current_path}"}, "SELECT * FROM t ORDER BY product_id", {})
        return {r["product_id"]: r for r in rows.to_pylist()}

    def history(pid):
        rows = delta.query(
            {"t": f"{silver_root}/{SPEC.history_path}"}, f"SELECT * FROM t WHERE product_id = {pid} ORDER BY _seq", {}
        )
        return rows.to_pylist()

    def checks():
        return {c.check: c.failing_rows for c in check_table(SPEC, silver_root, {})}

    return bronze, run, current, history, checks


def test_insert_update_delete_across_batches(lake):
    bronze, run, current, history, checks = lake

    bronze.write(("r", None, product(1, 10)), ("r", None, product(2, 20)))
    cur_result, hist_result = run()
    assert (cur_result.inserted, hist_result.inserted) == (2, 2)
    assert current()[1]["unit_price"] == Decimal("10.00")

    bronze.write(("u", product(1, 10), product(1, 12)), ("d", product(2, 20), None))
    run()
    rows = current()
    assert rows[1]["unit_price"] == Decimal("12.00") and not rows[1]["_is_deleted"]
    assert rows[2]["_is_deleted"]  # soft delete, keeps last values

    h1 = history(1)
    assert [(v["unit_price"], v["_is_current"]) for v in h1] == [(Decimal("10.00"), False), (Decimal("12.00"), True)]
    assert h1[0]["_valid_to"] == h1[1]["_valid_from"]
    assert [v["_is_current"] for v in history(2)] == [False]  # deleted → no open version
    assert all(n == 0 for n in checks().values()), checks()


def test_rerun_without_new_events_is_a_noop(lake):
    bronze, run, current, _, _ = lake
    bronze.write(("c", None, product(1, 10)))
    run()
    cur_result, hist_result = run()
    assert cur_result.events_read == 0 and hist_result.events_read == 0
    assert len(current()) == 1


def test_many_changes_in_one_batch_are_ordered_by_seq(lake):
    bronze, run, current, history, checks = lake
    bronze.write(
        ("c", None, product(1, 10)),
        ("u", product(1, 10), product(1, 11)),
        ("u", product(1, 11), product(1, 15)),
        order=[2, 0, 1],
    )
    run()
    assert current()[1]["unit_price"] == Decimal("15.00")
    versions = history(1)
    assert [v["unit_price"] for v in versions] == [Decimal("10.00"), Decimal("11.00"), Decimal("15.00")]
    assert [v["_is_current"] for v in versions] == [False, False, True]
    assert all(n == 0 for n in checks().values()), checks()


def test_delete_then_reinsert_same_key(lake):
    bronze, run, current, history, checks = lake
    bronze.write(("c", None, product(1, 10)))
    run()
    bronze.write(("d", product(1, 10), None))
    run()
    bronze.write(("c", None, product(1, 99, name="back again")))
    run()
    row = current()[1]
    assert not row["_is_deleted"] and row["name"] == "back again"
    assert [v["_is_current"] for v in history(1)] == [False, True]
    assert all(n == 0 for n in checks().values()), checks()
