# Databricks notebook source
# MAGIC %md
# MAGIC # SmartMFG Machine Scheduling Optimization - Setup
# MAGIC **Notebook:** 00_setup.py
# MAGIC
# MAGIC This notebook:
# MAGIC - Creates the Unity Catalog, schema, and landing volumes
# MAGIC - Configures the environment for the full pipeline
# MAGIC
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC **Catalog:** satsen_catalog
# MAGIC **Schema:** smartmfg_machine_optimization_1

# COMMAND ----------
# MAGIC %md ## 1. Configuration

# COMMAND ----------

CATALOG = "satsen_catalog"
SCHEMA  = "smartmfg_machine_optimization_1"
VOLUME  = "landing_zone"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Volume  : {VOLUME}")

# COMMAND ----------
# MAGIC %md ## 2. Create Catalog (idempotent)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Catalog '{CATALOG}' ready")

# COMMAND ----------
# MAGIC %md ## 3. Create Schema (idempotent)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")  # USE CATALOG was set above; UC requires simple schema name here
print(f"✅ Schema '{CATALOG}.{SCHEMA}' ready")

# COMMAND ----------
# MAGIC %md ## 4. Create Landing Volume (idempotent)

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}
    COMMENT 'Landing zone for raw CSV/JSON files before ingestion into Bronze tables'
""")
print(f"✅ Volume '{CATALOG}.{SCHEMA}.{VOLUME}' ready")

# COMMAND ----------
# MAGIC %md ## 5. Create Sub-folders in Volume

# COMMAND ----------

import os

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
sub_folders  = ["machines", "work_orders", "sensor_data", "routings"]

for folder in sub_folders:
    dbutils.fs.mkdirs(f"{volume_path}/{folder}")
    print(f"  📁 {volume_path}/{folder}")

print("✅ Volume sub-directories created")

# COMMAND ----------
# MAGIC %md ## 6. Verify Setup

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

print("=" * 55)
print("  SmartMFG Machine Optimization - Setup Complete")
print("=" * 55)
print(f"  Catalog : {CATALOG}")
print(f"  Schema  : {CATALOG}.{SCHEMA}")
print(f"  Volume  : {volume_path}")
print("  Next    : Run 01_data_generator.py")
print("=" * 55)
