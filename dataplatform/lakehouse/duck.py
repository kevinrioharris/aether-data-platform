"""DuckDB connections used as the compute engine for transformations."""

from __future__ import annotations

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # TIMESTAMPTZ values come back as UTC timestamps in Arrow, matching the Delta tables.
    con.execute("SET TimeZone = 'UTC'")
    return con
