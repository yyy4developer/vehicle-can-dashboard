# Databricks notebook source
# MAGIC %pip install cantools --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC # Vehicle CAN Decoder UDFs
# MAGIC 
# MAGIC cantoolsを使用してDBCファイルからシグナルをデコードするUDFを定義

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, 
    BooleanType, MapType, ArrayType
)
import cantools

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get configuration from pipeline settings or widgets
try:
    # Try to get from spark conf (DLT pipeline)
    catalog = spark.conf.get("catalog", "")
    schema = spark.conf.get("schema", "yao_demo_vehicle_app")
except Exception:
    # Fallback to defaults
    catalog = ""
    schema = "yao_demo_vehicle_app"

# Try to get from widgets if available (standalone notebook)
try:
    dbutils.widgets.text("catalog", catalog, "Catalog Name")
    dbutils.widgets.text("schema", schema, "Schema Name")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except Exception:
    pass

print(f"Configuration: catalog={catalog}, schema={schema}")
DBC_PATH = f"/Volumes/{catalog}/{schema}/dbc/vehicle.dbc"
print(f"DBC Path: {DBC_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load DBC File

# COMMAND ----------

def load_dbc_file(dbc_path: str):
    """Load and parse DBC file using cantools"""
    # Read DBC content from Volume
    local_path = dbc_path.replace("/Volumes/", "/dbfs/Volumes/")
    with open(local_path, "r") as f:
        dbc_content = f.read()
    
    # Parse DBC
    db = cantools.database.load_string(dbc_content, database_format="dbc")
    return db

# Load DBC at module level for reuse
try:
    CAN_DB = load_dbc_file(DBC_PATH)
    print(f"✅ Loaded DBC file: {DBC_PATH}")
    print(f"   Messages: {[msg.name for msg in CAN_DB.messages]}")
except Exception as e:
    print(f"⚠️ Could not load DBC file: {e}")
    CAN_DB = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Decoder UDFs

# COMMAND ----------

# Schema for decoded signals
DECODED_SIGNAL_SCHEMA = StructType([
    StructField("message_name", StringType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("throttle_pct", DoubleType(), True),
    StructField("brake_pressure", DoubleType(), True),
    StructField("brake_active", BooleanType(), True),
    StructField("steering_angle", DoubleType(), True),
])

def create_decode_udf(dbc_path: str):
    """Create a UDF that decodes CAN frames using the DBC file"""
    
    def decode_can_frame(arb_id: int, data: bytes) -> dict:
        """
        Decode a CAN frame using cantools and DBC file.
        
        Args:
            arb_id: CAN Arbitration ID
            data: Raw CAN data bytes
            
        Returns:
            Dictionary with decoded signal values (always returns struct, never None)
        """
        # Always return a consistent struct type
        result = {
            "message_name": None,
            "speed_kmh": None,
            "rpm": None,
            "throttle_pct": None,
            "brake_pressure": None,
            "brake_active": None,
            "steering_angle": None,
        }
        
        if data is None or len(data) == 0:
            return result
        
        try:
            # Load DBC inside UDF (broadcast variable would be better for production)
            local_path = dbc_path.replace("/Volumes/", "/dbfs/Volumes/")
            with open(local_path, "r") as f:
                dbc_content = f.read()
            db = cantools.database.load_string(dbc_content, database_format="dbc")
            
            # Find message by arbitration ID
            message = db.get_message_by_frame_id(arb_id)
            if message is None:
                return result
                
            result["message_name"] = message.name
            
            # Decode signals
            decoded = message.decode(bytes(data))
            
            # Map decoded signals to output schema
            if message.name == "VehicleSpeed":
                result["speed_kmh"] = decoded.get("speed_kmh")
            elif message.name == "EngineData":
                result["rpm"] = decoded.get("rpm")
                result["throttle_pct"] = decoded.get("throttle")
            elif message.name == "BrakeData":
                result["brake_pressure"] = decoded.get("pressure")
                result["brake_active"] = bool(decoded.get("active", 0))
            elif message.name == "SteeringData":
                result["steering_angle"] = decoded.get("angle")
                
            return result
            
        except Exception as e:
            # Return None values on error
            return result
    
    return decode_can_frame

# Create the UDF
decode_can_udf = F.udf(
    create_decode_udf(DBC_PATH), 
    DECODED_SIGNAL_SCHEMA
)

# Register UDF for SQL use
spark.udf.register("decode_can_frame", create_decode_udf(DBC_PATH), DECODED_SIGNAL_SCHEMA)

print("✅ Registered UDF: decode_can_frame")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Message Info UDF (for quality metrics)

# COMMAND ----------

# Schema for message info
MESSAGE_INFO_SCHEMA = StructType([
    StructField("message_name", StringType(), True),
    StructField("expected_period_ms", DoubleType(), True),
])

def create_message_info_udf(dbc_path: str):
    """Create a UDF that returns message info from DBC"""
    
    def get_message_info(arb_id: int) -> dict:
        """Get message name and expected period from DBC"""
        result = {
            "message_name": None,
            "expected_period_ms": 100.0,  # Default period
        }
        
        try:
            local_path = dbc_path.replace("/Volumes/", "/dbfs/Volumes/")
            with open(local_path, "r") as f:
                dbc_content = f.read()
            db = cantools.database.load_string(dbc_content, database_format="dbc")
            
            message = db.get_message_by_frame_id(arb_id)
            if message is None:
                return result
                
            result["message_name"] = message.name
            
            # Get cycle time from DBC attributes
            if hasattr(message, "cycle_time") and message.cycle_time:
                result["expected_period_ms"] = float(message.cycle_time)
            elif "TxPeriod" in (message.dbc_specifics or {}):
                result["expected_period_ms"] = float(message.dbc_specifics["TxPeriod"])
            else:
                # Fallback to known periods
                periods = {256: 20, 257: 10, 258: 20, 259: 50}
                result["expected_period_ms"] = float(periods.get(arb_id, 100))
                
            return result
            
        except Exception:
            return result
    
    return get_message_info

# Register UDF for SQL use
spark.udf.register("get_message_info", create_message_info_udf(DBC_PATH), MESSAGE_INFO_SCHEMA)

print("✅ Registered UDF: get_message_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test UDFs (Optional)

# COMMAND ----------

# Test the decode UDF with sample data
if CAN_DB is not None:
    print("\n📋 Testing decode_can_frame UDF:")
    print("-" * 50)
    
    # Create test data
    test_data = [
        (256, bytes([0x27, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])),  # VehicleSpeed
        (257, bytes([0x00, 0x10, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00])),  # EngineData
        (258, bytes([0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])),  # BrakeData
        (259, bytes([0x00, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])),  # SteeringData
    ]
    
    for arb_id, data in test_data:
        decoder = create_decode_udf(DBC_PATH)
        result = decoder(arb_id, data)
        print(f"  ARB_ID {arb_id}: {result}")

