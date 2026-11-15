"""
Sales Bronze ingestion configuration.

This module contains entity-specific metadata only.
Generic ingestion behavior belongs in the ingestion framework, not here.
"""

SALES_ENTITIES = {
    "customer": {
        "source_table": "samples.bakehouse.sales_customers",
        "target_schema": "sales_bronze",
        "target_table": "customer",
        "load_strategy": "full_refresh",
        "watermark_column": None,
        "primary_key": None,
    },
    "transaction": {
        "source_table": "samples.bakehouse.sales_transactions",
        "target_schema": "sales_bronze",
        "target_table": "transaction",
        "load_strategy": "incremental",
        "watermark_column": None,
        "primary_key": None,
    },
    "supplier": {
        "source_table": "samples.bakehouse.sales_suppliers",
        "target_schema": "sales_bronze",
        "target_table": "supplier",
        "load_strategy": "full_refresh",
        "watermark_column": None,
        "primary_key": None,
    },
}
