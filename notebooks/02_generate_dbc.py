# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate DBC File
# MAGIC 
# MAGIC cantoolsと互換性のあるDBCファイルを生成するノートブック
# MAGIC 
# MAGIC ## 前提条件
# MAGIC - Volume `dbc` が作成済み

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ウィジェットでパラメータを受け取る
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "yao_demo_vehicle_app", "Schema Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = "dbc"
DBC_FILENAME = "vehicle.dbc"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Volume: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBC Content

# COMMAND ----------

DBC_CONTENT = '''VERSION ""

NS_ :
    NS_DESC_
    CM_
    BA_DEF_
    BA_
    VAL_
    CAT_DEF_
    CAT_
    FILTER
    BA_DEF_DEF_
    EV_DATA_
    ENVVAR_DATA_
    SGTYPE_
    SGTYPE_VAL_
    BA_DEF_SGTYPE_
    BA_SGTYPE_
    SIG_TYPE_REF_
    VAL_TABLE_
    SIG_GROUP_
    SIG_VALTYPE_
    SIGTYPE_VALTYPE_
    BO_TX_BU_
    BA_REL_
    BA_SGTYPE_REL_
    SG_MUL_VAL_

BS_:

BU_: ECU1 ECU2

BO_ 256 VehicleSpeed: 8 ECU1
 SG_ speed_kmh : 0|16@1+ (0.01,0) [0|655.35] "km/h" Vector__XXX

BO_ 257 EngineData: 8 ECU1
 SG_ rpm : 0|16@1+ (0.25,0) [0|16383.75] "rpm" Vector__XXX
 SG_ throttle : 16|8@1+ (0.4,0) [0|102] "%" Vector__XXX

BO_ 258 BrakeData: 8 ECU2
 SG_ pressure : 0|8@1+ (0.4,0) [0|102] "%" Vector__XXX
 SG_ active : 8|1@1+ (1,0) [0|1] "" Vector__XXX

BO_ 259 SteeringData: 8 ECU2
 SG_ angle : 0|16@1+ (0.1,-1080) [-1080|1080] "deg" Vector__XXX

CM_ BO_ 256 "Vehicle speed message";
CM_ BO_ 257 "Engine data message";
CM_ BO_ 258 "Brake system data";
CM_ BO_ 259 "Steering wheel angle";
CM_ SG_ 256 speed_kmh "Vehicle speed in km/h";
CM_ SG_ 257 rpm "Engine RPM";
CM_ SG_ 257 throttle "Throttle position percentage";
CM_ SG_ 258 pressure "Brake pressure percentage";
CM_ SG_ 258 active "Brake pedal active flag";
CM_ SG_ 259 angle "Steering wheel angle in degrees";

BA_DEF_ BO_ "TxPeriod" INT 0 10000;
BA_DEF_DEF_ "TxPeriod" 100;
BA_ "TxPeriod" BO_ 256 20;
BA_ "TxPeriod" BO_ 257 10;
BA_ "TxPeriod" BO_ 258 20;
BA_ "TxPeriod" BO_ 259 50;
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display DBC Summary

# COMMAND ----------

print("DBC File Summary:")
print("=" * 60)
print("""
| Arb ID | Message Name   | Period | Signals                    |
|--------|----------------|--------|----------------------------|
| 0x100  | VehicleSpeed   | 20ms   | speed_kmh (0.01 km/h)      |
| 0x101  | EngineData     | 10ms   | rpm (0.25), throttle (0.4) |
| 0x102  | BrakeData      | 20ms   | pressure (0.4), active     |
| 0x103  | SteeringData   | 50ms   | angle (0.1, offset=-1080)  |
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Volume

# COMMAND ----------

# Output path
output_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{DBC_FILENAME}"
print(f"Saving DBC file to: {output_path}")

# Write DBC file
dbutils.fs.put(output_path, DBC_CONTENT, overwrite=True)
print(f"✅ DBC file saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# Verify file was created
print("Verifying DBC file...")
print("-" * 60)
content = dbutils.fs.head(output_path, 1000)
print(content)

# COMMAND ----------

# List files in volume
print(f"\nFiles in /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/:")
display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"))

# COMMAND ----------

# Summary
print("=" * 60)
print("✅ DBC file generation complete!")
print("=" * 60)
print(f"  Output: {output_path}")
