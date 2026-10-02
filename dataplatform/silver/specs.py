"""Silver table definitions for the CDC source: primary keys and typed columns (DuckDB types)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CdcTableSpec:
    name: str
    primary_key: tuple[str, ...]
    columns: dict[str, str]  # column → DuckDB type, in source order
    scd2: bool = False  # also keep full history (SCD Type 2)
    source: str = "shop"

    @property
    def bronze_path(self) -> str:
        return f"{self.source}/{self.name}_cdc"

    @property
    def current_path(self) -> str:
        return f"{self.source}/{self.name}"

    @property
    def history_path(self) -> str:
        return f"{self.source}/{self.name}_history"


SHOP_TABLES: dict[str, CdcTableSpec] = {
    spec.name: spec
    for spec in [
        CdcTableSpec(
            name="customers",
            primary_key=("customer_id",),
            columns={
                "customer_id": "INTEGER",
                "email": "VARCHAR",
                "full_name": "VARCHAR",
                "country": "VARCHAR",
                "created_at": "TIMESTAMPTZ",
                "updated_at": "TIMESTAMPTZ",
            },
            scd2=True,
        ),
        CdcTableSpec(
            name="products",
            primary_key=("product_id",),
            columns={
                "product_id": "INTEGER",
                "sku": "VARCHAR",
                "name": "VARCHAR",
                "category": "VARCHAR",
                "unit_price": "DECIMAL(10,2)",
                "is_active": "BOOLEAN",
                "updated_at": "TIMESTAMPTZ",
            },
            scd2=True,
        ),
        CdcTableSpec(
            name="orders",
            primary_key=("order_id",),
            columns={
                "order_id": "INTEGER",
                "customer_id": "INTEGER",
                "status": "VARCHAR",
                "currency": "VARCHAR",
                "order_ts": "TIMESTAMPTZ",
                "updated_at": "TIMESTAMPTZ",
            },
        ),
        CdcTableSpec(
            name="order_items",
            primary_key=("order_item_id",),
            columns={
                "order_item_id": "INTEGER",
                "order_id": "INTEGER",
                "product_id": "INTEGER",
                "quantity": "INTEGER",
                "unit_price": "DECIMAL(10,2)",
            },
        ),
    ]
}
