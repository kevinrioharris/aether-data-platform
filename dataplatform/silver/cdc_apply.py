"""Bronze CDC events → silver Delta tables.

Two outputs per source table:
  * current state (`silver/<source>/<table>`): one row per primary key, the latest version.
    Deletes are kept as soft-deletes (`_is_deleted`) so that replaying old events can never
    resurrect a deleted row.
  * history (`silver/<source>/<table>_history`, SCD Type 2): every version with
    `_valid_from` / `_valid_to` / `_is_current`.

Incremental and exactly-once: each run reads only bronze events past the watermark and
writes the new watermark (a Delta "app transaction", one per Kafka partition) in the *same*
commit as the MERGE. On top of that, both MERGEs are idempotent (ordered by `_seq`), so
reprocessing events is harmless.

Ordering: `_seq` is the Kafka offset. Debezium keys messages by primary key, so all events for
one row land in one partition, in commit order.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable
from deltalake.transaction import Transaction

from dataplatform.bronze.cdc_events import BRONZE_CDC_SCHEMA
from dataplatform.lakehouse import delta, duck
from dataplatform.silver.specs import CdcTableSpec

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApplyResult:
    table: str
    kind: str  # "current" | "history"
    events_read: int
    inserted: int = 0
    updated: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _q(col: str) -> str:
    return f'"{col}"'


def typed_events_sql(spec: CdcTableSpec) -> str:
    """Bronze JSON events → typed rows. Deletes carry their values in `before`."""
    cols = ",\n       ".join(
        f"CAST(json_extract_string(coalesce(after, before), '$.{c}') AS {t}) AS {_q(c)}"
        for c, t in spec.columns.items()
    )
    return f"""
SELECT {cols},
       op = 'd'                                     AS _is_deleted,
       op                                           AS _op,
       kafka_offset                                 AS _seq,
       CAST(epoch_ms(source_ts_ms) AS TIMESTAMPTZ)  AS _source_ts,
       now()                                        AS _processed_at
FROM bronze"""


def current_state_sql(spec: CdcTableSpec) -> str:
    pk = ", ".join(_q(c) for c in spec.primary_key)
    return f"""
SELECT * FROM ({typed_events_sql(spec)})
QUALIFY row_number() OVER (PARTITION BY {pk} ORDER BY _seq DESC) = 1"""


def history_versions_sql(spec: CdcTableSpec) -> str:
    """New SCD2 versions: one per non-delete event, closed by the key's next event in the batch."""
    pk = ", ".join(_q(c) for c in spec.primary_key)
    cols = ", ".join(_q(c) for c in spec.columns)
    return f"""
WITH ev AS ({typed_events_sql(spec)}),
ordered AS (
    SELECT *, lead(_source_ts) OVER (PARTITION BY {pk} ORDER BY _seq) AS _next_ts FROM ev
)
SELECT {cols},
       _source_ts         AS _valid_from,
       _next_ts           AS _valid_to,
       _next_ts IS NULL   AS _is_current,
       _seq, _op, _processed_at
FROM ordered
WHERE NOT _is_deleted"""


def history_merge_source_sql(spec: CdcTableSpec) -> str:
    """New versions (`_action='insert'`) plus one row per key that closes its open version."""
    pk = ", ".join(_q(c) for c in spec.primary_key)
    return f"""
SELECT *, 'insert' AS _action FROM ({history_versions_sql(spec)})
UNION ALL BY NAME
SELECT {pk},
       min(_source_ts) AS _valid_to,
       min(_seq)       AS _seq,
       now()           AS _processed_at,
       'close'         AS _action
FROM ({typed_events_sql(spec)})
GROUP BY {pk}"""


def _run_duckdb(sql: str, bronze: pa.Table) -> pa.Table:
    con = duck.connect()
    con.register("bronze", bronze)
    return con.execute(sql).to_arrow_table()


def _ensure_table(uri: str, schema_sql: str, storage: dict[str, str]) -> DeltaTable:
    if not delta.table_exists(uri, storage):
        schema = _run_duckdb(schema_sql + "\nLIMIT 0", BRONZE_CDC_SCHEMA.empty_table()).schema
        DeltaTable.create(uri, schema, storage_options=storage)
    return DeltaTable(uri, storage_options=storage)


def _app_id(spec: CdcTableSpec, kind: str, partition: int) -> str:
    return f"silver/{spec.source}/{spec.name}/{kind}/p{partition}"


def _read_new_events(
    spec: CdcTableSpec, kind: str, bronze_uri: str, target: DeltaTable, storage: dict[str, str]
) -> tuple[pa.Table, list[Transaction]]:
    """Bronze rows past the per-partition watermark, and the watermark transactions to commit."""
    parts = delta.query({"b": bronze_uri}, "SELECT DISTINCT kafka_partition AS p FROM b", storage)
    if parts.num_rows == 0:
        return BRONZE_CDC_SCHEMA.empty_table(), []
    watermarks = {}
    for p in parts["p"].to_pylist():
        wm = target.transaction_version(_app_id(spec, kind, p))
        watermarks[p] = -1 if wm is None else wm
    where = " OR ".join(f"(kafka_partition = {p} AND kafka_offset > {wm})" for p, wm in watermarks.items())
    events = delta.query({"b": bronze_uri}, f"SELECT * FROM b WHERE {where}", storage)
    if events.num_rows == 0:
        return events, []
    maxes = events.group_by("kafka_partition").aggregate([("kafka_offset", "max")])
    txns = [
        Transaction(app_id=_app_id(spec, kind, p), version=o)
        for p, o in zip(maxes["kafka_partition"].to_pylist(), maxes["kafka_offset_max"].to_pylist(), strict=True)
    ]
    return events, txns


def _pk_match(spec: CdcTableSpec) -> str:
    return " AND ".join(f"t.{_q(c)} = s.{_q(c)}" for c in spec.primary_key)


def apply_current_state(
    spec: CdcTableSpec, bronze_root: str, silver_root: str, storage: dict[str, str]
) -> ApplyResult:
    bronze_uri = f"{bronze_root}/{spec.bronze_path}"
    if not delta.table_exists(bronze_uri, storage):
        return ApplyResult(spec.name, "current", 0)
    target = _ensure_table(f"{silver_root}/{spec.current_path}", current_state_sql(spec), storage)

    events, txns = _read_new_events(spec, "current", bronze_uri, target, storage)
    if events.num_rows == 0:
        return ApplyResult(spec.name, "current", 0)

    source = _run_duckdb(current_state_sql(spec), events)
    metrics = (
        target.merge(
            source,
            predicate=_pk_match(spec),
            source_alias="s",
            target_alias="t",
            commit_properties=CommitProperties(app_transactions=txns),
        )
        .when_matched_update_all(predicate="s._seq > t._seq")
        .when_not_matched_insert_all()
        .execute()
    )
    result = ApplyResult(
        spec.name,
        "current",
        events.num_rows,
        inserted=metrics.get("num_target_rows_inserted", 0),
        updated=metrics.get("num_target_rows_updated", 0),
    )
    log.info("silver current %s: %s", spec.name, result)
    return result


def apply_history(spec: CdcTableSpec, bronze_root: str, silver_root: str, storage: dict[str, str]) -> ApplyResult:
    bronze_uri = f"{bronze_root}/{spec.bronze_path}"
    if not delta.table_exists(bronze_uri, storage):
        return ApplyResult(spec.name, "history", 0)
    target = _ensure_table(f"{silver_root}/{spec.history_path}", history_versions_sql(spec), storage)

    events, txns = _read_new_events(spec, "history", bronze_uri, target, storage)
    if events.num_rows == 0:
        return ApplyResult(spec.name, "history", 0)

    source = _run_duckdb(history_merge_source_sql(spec), events)
    target_cols = target.schema().to_arrow().names
    metrics = (
        target.merge(
            source,
            # A close row matches the key's open version (only if it is older than the batch);
            # an insert row matches an already-written identical version (replay → no-op).
            predicate=(
                f"{_pk_match(spec)} AND ("
                "(s._action = 'close' AND t._is_current AND t._seq < s._seq)"
                " OR (s._action = 'insert' AND t._seq = s._seq))"
            ),
            source_alias="s",
            target_alias="t",
            commit_properties=CommitProperties(app_transactions=txns),
        )
        .when_matched_update(
            updates={"_valid_to": "s._valid_to", "_is_current": "false", "_processed_at": "s._processed_at"},
            predicate="s._action = 'close'",
        )
        .when_not_matched_insert(
            updates={c: f"s.{_q(c)}" for c in target_cols},
            predicate="s._action = 'insert'",
        )
        .execute()
    )
    result = ApplyResult(
        spec.name,
        "history",
        events.num_rows,
        inserted=metrics.get("num_target_rows_inserted", 0),
        updated=metrics.get("num_target_rows_updated", 0),
    )
    log.info("silver history %s: %s", spec.name, result)
    return result
