"""Small Delta Lake helpers shared by all layers.

Reads go through delta-rs' own SQL engine (QueryBuilder) instead of `DeltaTable.to_pyarrow_table()`:
the pyarrow dataset path can leave pyarrow's global thread pool deadlocked at interpreter exit after
reading from S3, which would hang Airflow tasks and CLI runs.
"""

from __future__ import annotations

import pyarrow as pa
from deltalake import DeltaTable, QueryBuilder


def table_exists(uri: str, storage: dict[str, str]) -> bool:
    return DeltaTable.is_deltatable(uri, storage_options=storage)


def query(tables: dict[str, str], sql: str, storage: dict[str, str]) -> pa.Table:
    """Run `sql` over Delta tables registered as `{name: uri}` and return a pyarrow Table."""
    qb = QueryBuilder()
    for name, uri in tables.items():
        qb.register(name, DeltaTable(uri, storage_options=storage))
    return _plain_types(pa.table(qb.execute(sql).read_all()))


def _plain_types(table: pa.Table) -> pa.Table:
    """DataFusion returns string_view/binary_view; downstream engines (DuckDB scans) want plain types."""
    fields = [
        f.with_type(pa.string()) if f.type == pa.string_view()
        else f.with_type(pa.binary()) if f.type == pa.binary_view()
        else f
        for f in table.schema
    ]
    target = pa.schema(fields, metadata=table.schema.metadata)
    return table if target == table.schema else table.cast(target)
