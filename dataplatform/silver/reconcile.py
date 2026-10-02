"""Prove silver is correct: compare the live rows in silver with the source database, value by value.

Only meaningful when the source is quiet and the pipeline has caught up (stop the generator,
let the consumer flush, run silver, then reconcile).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import psycopg

from dataplatform.lakehouse import delta
from dataplatform.silver.specs import CdcTableSpec


@dataclass
class ReconcileResult:
    table: str
    source_rows: int
    silver_rows: int
    missing_in_silver: list = field(default_factory=list)
    extra_in_silver: list = field(default_factory=list)
    value_mismatches: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing_in_silver or self.extra_in_silver or self.value_mismatches)


def reconcile_table(
    spec: CdcTableSpec, conninfo: str, silver_root: str, storage: dict[str, str]
) -> ReconcileResult:
    cols = list(spec.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    pk_idx = [cols.index(c) for c in spec.primary_key]

    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(f"SELECT {col_list} FROM {spec.name}")
        source = {tuple(r[i] for i in pk_idx): tuple(r) for r in cur.fetchall()}

    silver_tbl = delta.query(
        {"t": f"{silver_root}/{spec.current_path}"}, f"SELECT {col_list} FROM t WHERE NOT _is_deleted", storage
    )
    silver = {tuple(r[c] for c in spec.primary_key): tuple(r[c] for c in cols) for r in silver_tbl.to_pylist()}

    return ReconcileResult(
        table=spec.name,
        source_rows=len(source),
        silver_rows=len(silver),
        missing_in_silver=sorted(source.keys() - silver.keys()),
        extra_in_silver=sorted(silver.keys() - source.keys()),
        value_mismatches=sorted(k for k in source.keys() & silver.keys() if source[k] != silver[k]),
    )
