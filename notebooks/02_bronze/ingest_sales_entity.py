# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Bronze Ingestion Framework
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Provide one reusable Bronze ingestion entry point for Sales-domain entities.
# MAGIC Entity-specific behavior is driven by configuration rather than duplicated notebooks.
# MAGIC
# MAGIC ## Current Scope
# MAGIC
# MAGIC - Select an entity at runtime
# MAGIC - Resolve source and target from configuration
# MAGIC - Create a batch identifier
# MAGIC - Read full-refresh or incremental source data
# MAGIC - Add technical ingestion metadata
# MAGIC - Write a managed Delta table
# MAGIC - Validate the load
# MAGIC - Advance the watermark only after successful validation
# MAGIC
# MAGIC ## Design Note
# MAGIC
# MAGIC The incremental pattern uses a persisted high-water mark in
# MAGIC `sales_metadata.ingestion_watermark`. A future hardening step will
# MAGIC address retry idempotency and late-arriving records.

# COMMAND ----------

# ============================================================
# 1. IMPORT DEPENDENCIES
# ============================================================

from datetime import datetime

from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max

from config.sales_entities import SALES_ENTITIES

# COMMAND ----------

# ============================================================
# 2. CREATE NOTEBOOK PARAMETERS
# ============================================================

dbutils.widgets.dropdown(
    "entity",
    "customer",
    ["customer", "transaction", "supplier"],
)

dbutils.widgets.text(
    "catalog_name",
    "workspace",
)

dbutils.widgets.text(
    "source_system",
    "databricks_samples",
)

# COMMAND ----------

# ============================================================
# 3. READ AND VALIDATE PARAMETERS
# ============================================================

entity = dbutils.widgets.get("entity").strip()
catalog_name = dbutils.widgets.get("catalog_name").strip()
source_system = dbutils.widgets.get("source_system").strip()

if entity not in SALES_ENTITIES:
    raise ValueError(f"Unsupported entity: {entity}")

if not catalog_name:
    raise ValueError("catalog_name cannot be empty")

if not source_system:
    raise ValueError("source_system cannot be empty")

# COMMAND ----------

# ============================================================
# 4. RESOLVE ENTITY CONFIGURATION
# ============================================================

entity_config = SALES_ENTITIES[entity]

source_table = entity_config["source_table"]
target_schema = entity_config["target_schema"]
target_table_name = entity_config["target_table"]
load_strategy = entity_config["load_strategy"]
watermark_column = entity_config["watermark_column"]
primary_key = entity_config["primary_key"]

target_table = f"{catalog_name}.{target_schema}.{target_table_name}"
watermark_table = f"{catalog_name}.sales_metadata.ingestion_watermark"

print(f"Entity: {entity}")
print(f"Source table: {source_table}")
print(f"Target table: {target_table}")
print(f"Load strategy: {load_strategy}")
print(f"Watermark column: {watermark_column}")
print(f"Primary key: {primary_key}")

# COMMAND ----------

# ============================================================
# 5. CREATE BATCH CONTEXT
# ============================================================

batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Batch ID: {batch_id}")

# COMMAND ----------

# ============================================================
# 6. READ SOURCE DATA
# ============================================================

df_source_base = spark.table(source_table)
previous_watermark = None
new_watermark = None

if load_strategy == "full_refresh":
    df_source = df_source_base

elif load_strategy == "incremental":
    if not watermark_column:
        raise ValueError(
            f"Incremental ingestion is configured for {entity}, "
            "but no watermark_column has been defined."
        )

    df_watermark = (
        spark.table(watermark_table)
        .filter(
            (col("entity") == entity)
            & (col("watermark_column") == watermark_column)
        )
        .select("last_successful_watermark")
        .limit(1)
    )

    watermark_rows = df_watermark.collect()

    if watermark_rows:
        previous_watermark = watermark_rows[0]["last_successful_watermark"]

    if previous_watermark is None:
        # First incremental run: no persisted high-water mark exists yet,
        # so the source is loaded from the beginning.
        df_source = df_source_base
        print("No prior watermark found. Running initial incremental load.")
    else:
        df_source = df_source_base.filter(
            col(watermark_column) > lit(previous_watermark)
        )
        print(f"Previous watermark: {previous_watermark}")

else:
    raise ValueError(f"Unsupported load_strategy: {load_strategy}")

source_count = df_source.count()

print(f"Source rows selected for this run: {source_count}")
display(df_source.limit(10))

# COMMAND ----------

# ============================================================
# 7. CAPTURE NEW WATERMARK
# ============================================================

if load_strategy == "incremental" and source_count > 0:
    new_watermark = (
        df_source
        .agg(spark_max(col(watermark_column)).alias("new_watermark"))
        .collect()[0]["new_watermark"]
    )

    print(f"Candidate new watermark: {new_watermark}")

# COMMAND ----------

# ============================================================
# 8. ADD BRONZE TECHNICAL METADATA
# ============================================================

df_bronze = (
    df_source
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_source_system", lit(source_system))
    .withColumn("_source_table", lit(source_table))
    .withColumn("_entity", lit(entity))
)

# COMMAND ----------

# ============================================================
# 9. MAP LOAD STRATEGY TO PHYSICAL WRITE MODE
# ============================================================

if load_strategy == "full_refresh":
    write_mode = "overwrite"
elif load_strategy == "incremental":
    write_mode = "append"
else:
    raise ValueError(f"Unsupported load_strategy: {load_strategy}")

print(f"Physical write mode: {write_mode}")

# COMMAND ----------

# ============================================================
# 10. WRITE TO BRONZE DELTA TABLE
# ============================================================

if source_count > 0:
    (
        df_bronze.write
        .format("delta")
        .mode(write_mode)
        .saveAsTable(target_table)
    )

    print(f"Write completed: {target_table}")
else:
    print("No source rows selected. Write skipped.")

# COMMAND ----------

# ============================================================
# 11. VALIDATE LOAD
# ============================================================

if source_count > 0:
    df_target = spark.table(target_table)

    if load_strategy == "full_refresh":
        target_count = df_target.count()

        print(f"Source count: {source_count}")
        print(f"Target count: {target_count}")

        if source_count != target_count:
            raise Exception(
                f"Validation failed: source_count={source_count}, "
                f"target_count={target_count}"
            )

        print("PASS: Full refresh row counts match.")

    elif load_strategy == "incremental":
        current_batch_count = (
            df_target
            .filter(col("_batch_id") == batch_id)
            .count()
        )

        print(f"Source count: {source_count}")
        print(f"Current batch count: {current_batch_count}")

        if source_count != current_batch_count:
            raise Exception(
                f"Validation failed: source_count={source_count}, "
                f"current_batch_count={current_batch_count}"
            )

        print("PASS: Incremental batch row counts match.")
else:
    print("No rows to validate.")

# COMMAND ----------

# ============================================================
# 12. ADVANCE WATERMARK AFTER SUCCESSFUL VALIDATION
# ============================================================

if load_strategy == "incremental" and source_count > 0:
    spark.sql(f"""
        MERGE INTO {watermark_table} AS target
        USING (
            SELECT
                '{entity}' AS entity,
                '{watermark_column}' AS watermark_column,
                TIMESTAMP('{new_watermark}') AS last_successful_watermark,
                current_timestamp() AS updated_at
        ) AS source
        ON target.entity = source.entity
        AND target.watermark_column = source.watermark_column

        WHEN MATCHED THEN UPDATE SET
            target.last_successful_watermark = source.last_successful_watermark,
            target.updated_at = source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            entity,
            watermark_column,
            last_successful_watermark,
            updated_at
        )
        VALUES (
            source.entity,
            source.watermark_column,
            source.last_successful_watermark,
            source.updated_at
        )
    """)

    print(f"Watermark advanced to: {new_watermark}")
