"""Dataset schemas as the platform describes them: columns with DuckDB SQL types (the compute engine's
type system, also used by silver specs), plus where each column came from in the source.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pyarrow as pa

from dataplatform.lakehouse import duck


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str  # DuckDB SQL type, e.g. INTEGER, VARCHAR, DECIMAL(10,2), TIMESTAMP WITH TIME ZONE
    nullable: bool = True
    source_name: str | None = None  # column name as the source spells it
    source_type: str | None = None  # type as the source reports it (e.g. "numeric(10,2)", "text")
    description: str = ""


@dataclass(frozen=True)
class DatasetSchema:
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)

    @classmethod
    def from_arrow(cls, schema: pa.Schema, primary_key: tuple[str, ...] = ()) -> DatasetSchema:
        types = arrow_to_sql_types(schema)
        return cls(
            columns=tuple(
                Column(name=f.name, data_type=types[f.name], nullable=f.nullable, source_type=str(f.type))
                for f in schema
            ),
            primary_key=primary_key,
        )

    def as_dict(self) -> dict:
        return {
            "columns": [c.__dict__.copy() for c in self.columns],
            "primary_key": list(self.primary_key),
            "metadata": dict(self.metadata),
        }


def arrow_to_sql_types(schema: pa.Schema) -> dict[str, str]:
    """Arrow field types → DuckDB SQL type names, as DuckDB itself maps them."""
    con = duck.connect()
    con.register("_empty", schema.empty_table())
    return {name: dtype for name, dtype, *_ in con.execute("DESCRIBE _empty").fetchall()}


# Narrowest first: a column gets the first type whose predicate holds for every non-blank value.
# Predicates are stricter than TRY_CAST alone, which accepts "1.5" as BIGINT and "2026-01-05 10:00" as DATE.
# Integers with leading zeros ("007", zip codes) are identifiers and stay VARCHAR.
_TZ_SUFFIX = r"(Z|[+-][0-9]{2}(:?[0-9]{2})?)$"
_INFERENCE_RULES = [
    ("BIGINT", r"regexp_full_match(v, '[-+]?(0|[1-9][0-9]*)') AND TRY_CAST(v AS BIGINT) IS NOT NULL"),
    ("DOUBLE", r"TRY_CAST(v AS DOUBLE) IS NOT NULL AND NOT regexp_full_match(v, '[-+]?0[0-9]+')"),
    ("BOOLEAN", "lower(v) IN ('true', 'false')"),
    ("DATE", r"regexp_full_match(v, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AND TRY_CAST(v AS DATE) IS NOT NULL"),
    ("TIMESTAMP", f"TRY_CAST(v AS TIMESTAMP) IS NOT NULL AND NOT regexp_matches(v, '{_TZ_SUFFIX}')"),
    ("TIMESTAMP WITH TIME ZONE", "TRY_CAST(v AS TIMESTAMPTZ) IS NOT NULL"),
]


def infer_string_types(table: pa.Table) -> DatasetSchema:
    """Suggest SQL types for raw string columns (files are landed as strings; typing happens in silver).

    A column is typed only if *every* non-blank value fits; otherwise it stays VARCHAR. Values like
    "32,000" stay VARCHAR on purpose: cleaning them is a transformation, not an inference.
    """
    con = duck.connect()
    con.register("t", table)
    columns = []
    for f in table.schema:
        inferred = "VARCHAR"
        if pa.types.is_string(f.type) or pa.types.is_large_string(f.type):
            values = f'(SELECT trim("{f.name}") AS v FROM t WHERE nullif(trim("{f.name}"), \'\') IS NOT NULL)'
            non_blank = con.execute(f"SELECT count(*) FROM {values}").fetchone()[0]
            if non_blank:
                for candidate, predicate in _INFERENCE_RULES:
                    if con.execute(f"SELECT bool_and({predicate}) FROM {values}").fetchone()[0]:
                        inferred = candidate
                        break
        elif not pa.types.is_null(f.type):
            inferred = arrow_to_sql_types(pa.schema([f]))[f.name]
        columns.append(Column(name=f.name, data_type=inferred, source_name=f.name, source_type=str(f.type)))
    return DatasetSchema(columns=tuple(columns))


def with_type_overrides(schema: DatasetSchema, overrides: dict[str, str]) -> DatasetSchema:
    """Apply user-chosen types (e.g. keep a zero-padded code as VARCHAR)."""
    unknown = overrides.keys() - set(schema.names)
    if unknown:
        raise ValueError(f"type overrides for unknown columns: {sorted(unknown)}")
    return replace(
        schema,
        columns=tuple(replace(c, data_type=overrides.get(c.name, c.data_type)) for c in schema.columns),
    )
