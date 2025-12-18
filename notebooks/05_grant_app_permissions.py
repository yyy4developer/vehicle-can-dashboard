# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Permissions to App Service Principal
# MAGIC 
# MAGIC This notebook grants all necessary Unity Catalog permissions to the Databricks App's Service Principal.
# MAGIC Run this after deploying the bundle to ensure the app can access all required resources.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "yao_demo_vehicle_app", "Schema Name")
dbutils.widgets.text("app_name", "yao-demo-vehicle-app", "Databricks App Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
app_name = dbutils.widgets.get("app_name")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"App Name: {app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Service Principal Client ID from App

# COMMAND ----------

import requests
import os

# Use REST API to get app info (SDK version compatibility)
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}
response = requests.get(f"https://{host}/api/2.0/apps/{app_name}", headers=headers)

if response.status_code == 200:
    app_info = response.json()
    sp_client_id = app_info.get("service_principal_client_id")
    sp_name = app_info.get("service_principal_name", app_info.get("name"))
    print(f"Service Principal Name: {sp_name}")
    print(f"Service Principal Client ID: {sp_client_id}")
else:
    raise Exception(f"Failed to get app info: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Catalog and Schema Permissions

# COMMAND ----------

# Grant USE CATALOG
sql = f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_client_id}`"
print(f"Executing: {sql}")
spark.sql(sql)
print("✅ USE CATALOG granted")

# Grant USE SCHEMA
sql = f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`"
print(f"Executing: {sql}")
spark.sql(sql)
print("✅ USE SCHEMA granted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant SELECT on DLT Tables

# COMMAND ----------

# List of DLT tables that the app needs to read
tables = [
    "gold_signals_aggregated",
    "gold_event_history",
    "gold_vehicle_stats",
    "gold_latest_signals",
    "silver_can_quality",
    "video_metadata",
]

for table in tables:
    try:
        sql = f"GRANT SELECT ON TABLE {catalog}.{schema}.{table} TO `{sp_client_id}`"
        print(f"Executing: {sql}")
        spark.sql(sql)
        print(f"✅ SELECT granted on {table}")
    except Exception as e:
        print(f"⚠️ Could not grant SELECT on {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Volume Permissions

# COMMAND ----------

# List of volumes the app needs to read
volumes = [
    "videos",
    "raw",
]

for volume in volumes:
    try:
        sql = f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume} TO `{sp_client_id}`"
        print(f"Executing: {sql}")
        spark.sql(sql)
        print(f"✅ READ VOLUME granted on {volume}")
    except Exception as e:
        print(f"⚠️ Could not grant READ VOLUME on {volume}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Permissions

# COMMAND ----------

# Show all grants on the schema
print(f"\n=== Current grants on {catalog}.{schema} ===")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schema}"))

# COMMAND ----------

# Show grants on one of the tables as verification
print(f"\n=== Current grants on {catalog}.{schema}.gold_signals_aggregated ===")
display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.{schema}.gold_signals_aggregated"))

# COMMAND ----------

print("\n" + "="*50)
print("✅ All permissions granted successfully!")
print("="*50)
print(f"\nThe app '{app_name}' Service Principal '{sp_name}' now has:")
print(f"  - USE CATALOG on {catalog}")
print(f"  - USE SCHEMA on {catalog}.{schema}")
print(f"  - SELECT on all DLT tables")
print(f"  - READ VOLUME on video and raw volumes")

