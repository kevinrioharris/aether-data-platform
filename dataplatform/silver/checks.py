"""Quality gates on silver CDC tables. Any failed check fails the pipeline run."""

from __future__ import annotations

from dataclasses import dataclass

from dataplatform.lakehouse import delta
from dataplatform.silver.specs import CdcTableSpec


@dataclass(frozen=True)
class CheckResult:
    table: str
    check: str
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


class QualityCheckError(Exception):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        super().__init__("; ".join(f"{f.table}.{f.check}: {f.failing_rows} failing rows" for f in failures))


def _count(tables: dict[str, str], sql: str, storage: dict[str, str]) -> int:
    return delta.query(tables, f"SELECT count(*) AS n FROM ({sql}) AS x", storage)["n"][0].as_py()


def check_table(spec: CdcTableSpec, silver_root: str, storage: dict[str, str]) -> list[CheckResult]:
    pk = ", ".join(f'"{c}"' for c in spec.primary_key)
    pk_null = " OR ".join(f'"{c}" IS NULL' for c in spec.primary_key)
    current = {"cur": f"{silver_root}/{spec.current_path}"}
    if not delta.table_exists(current["cur"], storage):
        return []

    checks = {
        "pk_unique": (current, f"SELECT {pk} FROM cur GROUP BY {pk} HAVING count(*) > 1"),
        "pk_not_null": (current, f"SELECT 1 FROM cur WHERE {pk_null}"),
    }
    history_uri = f"{silver_root}/{spec.history_path}"
    if spec.scd2 and delta.table_exists(history_uri, storage):
        both = {**current, "hist": history_uri}
        pk_join = " AND ".join(f'h."{c}" = c."{c}"' for c in spec.primary_key)
        checks |= {
            "scd2_one_current_version": (
                {"hist": history_uri},
                f"SELECT {pk} FROM hist WHERE _is_current GROUP BY {pk} HAVING count(*) > 1",
            ),
            "scd2_valid_interval": ({"hist": history_uri}, "SELECT 1 FROM hist WHERE _valid_to < _valid_from"),
            "scd2_open_iff_current": (
                {"hist": history_uri},
                "SELECT 1 FROM hist WHERE _is_current <> (_valid_to IS NULL)",
            ),
            # The two silver tables must agree: live rows ⇔ an open history version.
            "scd2_matches_current_state": (
                both,
                f"SELECT 1 FROM cur c FULL OUTER JOIN (SELECT * FROM hist WHERE _is_current) h ON {pk_join} "
                f"WHERE (c._is_deleted IS DISTINCT FROM false) <> (h._seq IS NULL)",
            ),
        }
    return [CheckResult(spec.name, name, _count(tables, sql, storage)) for name, (tables, sql) in checks.items()]
