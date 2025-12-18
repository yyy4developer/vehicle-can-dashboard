# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup (Reset & Recreate)
# MAGIC 
# MAGIC スキーマとVolumesを **削除して再作成** するセットアップノートブック。
# MAGIC **⚠️ 既存のデータはすべて削除されます！**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ウィジェットでパラメータを受け取る
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "yao_demo_vehicle_app", "Schema Name")
dbutils.widgets.dropdown("reset", "true", ["true", "false"], "Reset (drop all)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RESET = dbutils.widgets.get("reset") == "true"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Reset: {RESET}")

if RESET:
    print("⚠️  WARNING: All existing data will be deleted!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Schema (if reset=true)

# COMMAND ----------

if RESET:
    print(f"🗑️  Dropping schema {CATALOG}.{SCHEMA} and all contents...")
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
        print(f"✅ Schema dropped: {CATALOG}.{SCHEMA}")
    except Exception as e:
        print(f"⚠️  Could not drop schema: {e}")
else:
    print("Skipping reset (reset=false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ Schema created: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Verify schema
display(spark.sql(f"DESCRIBE SCHEMA {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volumes

# COMMAND ----------

# Create raw volume for CAN data
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw
COMMENT 'Raw CAN frame data'
""")
print(f"✅ Volume created: {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------

# Create dbc volume for DBC files
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.dbc
COMMENT 'DBC files for CAN decoding'
""")
print(f"✅ Volume created: {CATALOG}.{SCHEMA}.dbc")

# COMMAND ----------

# Create videos volume for dashcam footage
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.videos
COMMENT 'Dashcam video files'
""")
print(f"✅ Volume created: {CATALOG}.{SCHEMA}.videos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all volumes in schema
print(f"Volumes in {CATALOG}.{SCHEMA}:")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# List all tables (should be empty after reset)
print(f"Tables in {CATALOG}.{SCHEMA}:")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# Summary
print("=" * 60)
print("✅ Setup complete!")
print("=" * 60)
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Volumes: raw, dbc, videos")
print(f"  Reset performed: {RESET}")
print()
print("Next steps:")
print("  Run 'data-generation' job to generate all data")
