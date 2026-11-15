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
# MAGIC - Validate the configured primary key
# MAGIC - Add technical ingestion metadata
# MAGIC - Write full-refresh entities by overwrite
# MAGIC - Write incremental entities idempotently with Delta MERGE
# MAGIC - Validate the load
# MAGIC - Advance the watermark only after successful validation
# MAGIC
# MAGIC ## Design Note
# MAGIC
# MAGIC Incremental entities use a persisted high-water mark in
# MAGIC `sales_metadata.ingestion_watermark`. Delta MERGE prevents a retry
# MAGIC from inserting the same business key twice. Late-arriving records
# MAGIC will be addressed as a separate hardening step.

# COMMAND ----------

# ============================================================
# 1. IMPORT DEPENDENCIES
# ============================================================

from datetime import datetime

from delta.tables import DeltaTable
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
# 7. VALIDATE PRIMARY KEY IN THE SELECTED SOURCE BATCH
# ============================================================

if source_count > 0:
    if not primary_key:
        raise ValueError(f"No primary_key configured for entity: {entity}")

    null_key_count = df_source.filter(col(primary_key).isNull()).count()
    distinct_key_count = df_source.select(primary_key).distinct().count()

    if null_key_count > 0:
        raise ValueError(
            f"Primary-key validation failed for {entity}: "
            f"{null_key_count} null {primary_key} values found."
        )

    if distinct_key_count != source_count:
        raise ValueError(
            f"Primary-key validation failed for {entity}: "
            f"source_count={source_count}, "
            f"distinct_{primary_key}={distinct_key_count}."
        )

    print(
        f"PASS: {primary_key} is non-null and unique "
        "within the selected source batch."
    )

# COMMAND ----------

# ============================================================
# 8. CAPTURE CANDIDATE NEW WATERMARK
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
# 9. ADD BRONZE TECHNICAL METADATA
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
# 10. WRITE TO BRONZE DELTA TABLE
# ============================================================

expected_insert_count = 0

if source_count == 0:
    print("No source rows selected. Write skipped.")

elif load_strategy == "full_refresh":
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    expected_insert_count = source_count
    print(f"Full refresh completed: {target_table}")

elif load_strategy == "incremental":
    target_exists = spark.catalog.tableExists(target_table)

    if not target_exists:
        (
            df_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

        expected_insert_count = source_count
        print(f"Initial incremental load created: {target_table}")

    else:
        existing_keys = (
            spark.table(target_table)
            .select(primary_key)
            .distinct()
        )

        expected_insert_count = (
            df_source
            .select(primary_key)
            .join(existing_keys, on=primary_key, how="left_anti")
            .count()
        )

        delta_target = DeltaTable.forName(spark, target_table)

        (
            delta_target.alias("target")
            .merge(
                df_bronze.alias("source"),
                f"target.`{primary_key}` = source.`{primary_key}`",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(
            f"Incremental MERGE completed: {target_table}. "
            f"Expected new rows: {expected_insert_count}"
        )

# COMMAND ----------

# ============================================================
# 11. VALIDATE LOAD
# ============================================================

if source_count == 0:
    print("No rows to validate.")

else:
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
        current_batch_insert_count = (
            df_target
            .filter(col("_batch_id") == batch_id)
            .count()
        )

        print(f"Source rows selected: {source_count}")
        print(f"Expected new inserts: {expected_insert_count}")
        print(f"Actual current-batch inserts: {current_batch_insert_count}")

        if expected_insert_count != current_batch_insert_count:
            raise Exception(
                f"Validation failed: expected_insert_count="
                f"{expected_insert_count}, "
                f"current_batch_insert_count={current_batch_insert_count}"
            )

        print("PASS: Incremental MERGE is retry-safe and validated.")

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
