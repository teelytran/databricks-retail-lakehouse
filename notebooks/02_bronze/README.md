# Bronze Layer

The Bronze layer preserves source data with minimal transformation and adds technical metadata required for traceability and operational support.

## Design

This project uses a metadata-driven ingestion pattern:

```text
entity configuration
        ↓
reusable ingestion framework
        ↓
Bronze Delta table
```

The ingestion logic is shared, while entity-specific settings remain in configuration.

## Files

- `ingest_sales_entity.py` — reusable Databricks ingestion notebook
- `config/sales_entities.py` — entity-specific ingestion metadata

## Current Entities

- customer
- transaction
- supplier

## Production Principles

- Avoid duplicated ingestion notebooks
- Keep source/target mappings in configuration
- Add technical metadata for traceability
- Validate source-to-target loads
- Do not claim a load is incremental until a valid watermark strategy exists
- Keep orchestration separate from ingestion logic

## Next Step

Inspect the source schemas and define valid primary-key and watermark columns before enabling true incremental extraction.
