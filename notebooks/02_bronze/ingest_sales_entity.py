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
# MAGIC - Read source data
# MAGIC - Add technical ingestion metadata
# MAGIC - Map ingestion strategy to physical write mode
# MAGIC - Write a managed Delta table
# MAGIC - Perform basic row-count validation
# MAGIC
# MAGIC ## Important
# MAGIC
# MAGIC Incremental extraction is intentionally not implemented yet.
# MAGIC We will first inspect the actual source schemas and select a valid watermark column.

# COMMAND ----------

# ============================================================
# 1. IMPORT DEPENDENCIES
# ============================================================

from datetime import datetime

from pyspark.sql.functions import current_timestamp, lit

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

df_source = spark.table(source_table)
source_count = df_source.count()

print(f"Source row count: {source_count}")
display(df_source.limit(10))

# COMMAND ----------

# ============================================================
# 7. ADD BRONZE TECHNICAL METADATA
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
# 8. MAP LOAD STRATEGY TO PHYSICAL WRITE MODE
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
# 9. PROTECT AGAINST FALSE INCREMENTAL LOADS
# ============================================================

if load_strategy == "incremental" and not watermark_column:
    raise ValueError(
        f"Incremental ingestion is configured for {entity}, "
        "but no watermark_column has been defined yet. "
        "Inspect the source schema before enabling incremental ingestion."
    )

# COMMAND ----------

# ============================================================
# 10. WRITE TO BRONZE DELTA TABLE
# ============================================================

(
    df_bronze.write
    .format("delta")
    .mode(write_mode)
    .saveAsTable(target_table)
)

print(f"Write completed: {target_table}")

# COMMAND ----------

# ============================================================
# 11. VALIDATE LOAD
# ============================================================

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
        .filter(f"_batch_id = '{batch_id}'")
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
