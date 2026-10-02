"""A tiny dbt-style runner for gold SQL models (DuckDB compute, Delta output).

* Each `models/<name>.sql` is one gold table, written to `<gold_root>/<name>` (full refresh).
* Models reference inputs as `silver.<domain>_<table>` (→ `<silver_root>/<domain>/<table>`) and
  `gold.<model>`. Dependencies (and so run order and lineage) are parsed from those references.
* Tests are declared in the header: `-- test: unique(col[, col])`, `-- test: not_null(col)`, or
  `-- test: assert_empty: <SELECT returning failing rows>`. A failing test stops the run before
  anything downstream is written.
* `getvariable('as_of')` gives models the business date of the run (never wall-clock time).
"""

from __future__ import annotations

import graphlib
import logging
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

from dataplatform.lakehouse import delta, duck

log = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).parent / "models"
_REF = re.compile(r"\b(silver|gold)\.([a-z][a-z0-9_]*)\b")
_TEST = re.compile(r"^--\s*test:\s*(.+)$", re.MULTILINE)


@dataclass(frozen=True)
class Model:
    name: str
    sql: str
    silver_inputs: frozenset[str]
    gold_inputs: frozenset[str]
    tests: tuple[str, ...]
    description: str = ""


@dataclass
class ModelResult:
    model: str
    rows: int
    tests: dict[str, int] = field(default_factory=dict)  # test → failing rows


class ModelTestError(Exception):
    pass


def silver_uri_for(ref: str, silver_root: str) -> str:
    """`shop_order_items` → `<silver_root>/shop/order_items` (the domain is before the first `_`)."""
    domain, _, table = ref.partition("_")
    return f"{silver_root}/{domain}/{table}"


def load_models(models_dir: Path = MODELS_DIR) -> dict[str, Model]:
    models = {}
    for path in sorted(models_dir.glob("*.sql")):
        sql = path.read_text()
        refs = _REF.findall(sql)
        header = [ln[2:].strip() for ln in sql.splitlines() if ln.startswith("--") and "test:" not in ln]
        models[path.stem] = Model(
            name=path.stem,
            sql=sql,
            silver_inputs=frozenset(n for layer, n in refs if layer == "silver"),
            gold_inputs=frozenset(n for layer, n in refs if layer == "gold") - {path.stem},
            tests=tuple(t.strip() for t in _TEST.findall(sql)),
            description=" ".join(header),
        )
    return models


def run_order(models: dict[str, Model]) -> list[str]:
    graph = {name: set(m.gold_inputs) for name, m in models.items()}
    for deps in graph.values():
        if missing := deps - models.keys():
            raise ValueError(f"unknown gold model(s) referenced: {missing}")
    return list(graphlib.TopologicalSorter(graph).static_order())


def lineage(models: dict[str, Model]) -> list[tuple[str, str]]:
    """Edges (upstream, downstream) with fully qualified names, for docs and the workspace app."""
    edges = []
    for m in models.values():
        edges += [(f"silver.{s}", f"gold.{m.name}") for s in sorted(m.silver_inputs)]
        edges += [(f"gold.{g}", f"gold.{m.name}") for g in sorted(m.gold_inputs)]
    return edges


def _test_sql(test: str, table: str) -> str:
    if m := re.fullmatch(r"unique\((.+)\)", test):
        cols = m.group(1)
        return f"SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1"
    if m := re.fullmatch(r"not_null\((\w+)\)", test):
        return f"SELECT 1 FROM {table} WHERE {m.group(1)} IS NULL"
    if m := re.fullmatch(r"assert_empty:\s*(.+)", test):
        return m.group(1)
    raise ValueError(f"unknown test syntax: {test!r}")


def run_models(
    silver_root: str,
    gold_root: str,
    storage: dict[str, str],
    *,
    as_of: date,
    models: dict[str, Model] | None = None,
) -> list[ModelResult]:
    models = models or load_models()
    con = duck.connect()
    con.execute("CREATE SCHEMA silver; CREATE SCHEMA gold;")
    con.execute(f"SET VARIABLE as_of = DATE '{as_of.isoformat()}'")

    for ref in sorted({s for m in models.values() for s in m.silver_inputs}):
        con.register("_input", delta.query({"t": silver_uri_for(ref, silver_root)}, "SELECT * FROM t", storage))
        con.execute(f"CREATE TABLE silver.{ref} AS SELECT * FROM _input")
        con.unregister("_input")

    results = []
    for name in run_order(models):
        model = models[name]
        table: pa.Table = con.execute(model.sql).to_arrow_table()
        con.register("_output", table)
        con.execute(f"CREATE TABLE gold.{name} AS SELECT * FROM _output")
        con.unregister("_output")

        result = ModelResult(name, table.num_rows)
        for test in model.tests:
            failing = con.execute(f"SELECT count(*) FROM ({_test_sql(test, f'gold.{name}')})").fetchone()[0]
            result.tests[test] = failing
        if failed := {t: n for t, n in result.tests.items() if n}:
            raise ModelTestError(f"gold.{name}: failed tests {failed}")

        write_deltalake(
            f"{gold_root}/{name}", table, mode="overwrite", schema_mode="overwrite", storage_options=storage
        )
        log.info("gold.%s: %d rows, %d tests passed", name, table.num_rows, len(result.tests))
        results.append(result)
    return results
