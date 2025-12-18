# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Download Sample Videos
# MAGIC 
# MAGIC ダッシュボードでテスト用のサンプル動画をダウンロードするノートブック
# MAGIC 
# MAGIC ## 前提条件
# MAGIC - Volume `videos` が作成済み

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ウィジェットでパラメータを受け取る
dbutils.widgets.text("catalog", "yunyi_catalog", "Catalog Name")
dbutils.widgets.text("schema", "yao_demo_vehicle_app", "Schema Name")
dbutils.widgets.text("vehicle_id", "VH001", "Vehicle ID")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VEHICLE_ID = dbutils.widgets.get("vehicle_id")
VOLUME = "videos"

OUTPUT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Volume: {VOLUME}")
print(f"  Output: {OUTPUT_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camera Configuration
# MAGIC 
# MAGIC Note: In production, replace these with actual dashcam footage URLs or upload your own videos.

# COMMAND ----------

# Camera names for 4-way dashcam setup
CAMERAS = ["front", "rear", "left", "right"]

# Driving video URLs from reliable sources
# Using archive.org for reliable direct download
DRIVING_VIDEO_URLS = {
    # Archive.org - City driving footage (public domain)
    "front": "https://ia800302.us.archive.org/26/items/City_Drive/City_Drive.mp4",
    "rear": "https://ia800302.us.archive.org/26/items/City_Drive/City_Drive.mp4",
    "left": "https://ia800302.us.archive.org/26/items/City_Drive/City_Drive.mp4",
    "right": "https://ia800302.us.archive.org/26/items/City_Drive/City_Drive.mp4",
}

# Fallback video (small, always works)
FALLBACK_VIDEO_URL = "https://www.w3schools.com/html/mov_bbb.mp4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Sample Videos

# COMMAND ----------

import urllib.request
import os
from datetime import datetime

downloaded_files = []

for camera in CAMERAS:
    try:
        # Try driving video first, fallback to sample
        video_url = DRIVING_VIDEO_URLS.get(camera, FALLBACK_VIDEO_URL)
        local_path = f"/tmp/{camera}_driving.mp4"
        output_path = f"{OUTPUT_BASE}/{camera}_driving.mp4"
        
        print(f"📹 Downloading {camera} driving video...")
        print(f"   URL: {video_url}")
        
        try:
            # Download to local temp
            urllib.request.urlretrieve(video_url, local_path)
            file_size = os.path.getsize(local_path)
            
            # If file is too small (< 10KB), likely failed - use fallback
            if file_size < 10000:
                raise Exception("File too small, using fallback")
                
        except Exception as download_error:
            print(f"   ⚠️ Primary URL failed ({download_error}), trying fallback...")
            urllib.request.urlretrieve(FALLBACK_VIDEO_URL, local_path)
        
        # Get file size
        file_size = os.path.getsize(local_path)
        
        # Copy to Volume
        dbutils.fs.cp(f"file:{local_path}", output_path)
        
        # Cleanup local file
        os.remove(local_path)
        
        downloaded_files.append({
            "camera": camera,
            "path": output_path,
            "size": file_size
        })
        
        print(f"✅ Downloaded {camera}: {output_path} ({file_size:,} bytes)")
        
    except Exception as e:
        print(f"❌ Error downloading {camera}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Downloaded Files

# COMMAND ----------

print(f"Files in {OUTPUT_BASE}:")
display(dbutils.fs.ls(OUTPUT_BASE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Video Metadata Table
# MAGIC 
# MAGIC Create a table to track video files and their timestamps

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime, timedelta

# Schema for video metadata
schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("camera", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_size_bytes", LongType(), True),
])

# Create metadata from downloaded files
now = datetime.now()
sample_data = []

for df in downloaded_files:
    sample_data.append((
        f"vid_{VEHICLE_ID}_{df['camera']}", 
        df['camera'], 
        VEHICLE_ID, 
        now, 
        now + timedelta(minutes=10), 
        df['path'], 
        df['size']
    ))

# If no files were downloaded, create placeholder metadata
if not sample_data:
    for camera in CAMERAS:
        sample_data.append((
            f"vid_{VEHICLE_ID}_{camera}", 
            camera, 
            VEHICLE_ID, 
            now, 
            now + timedelta(minutes=10), 
            f"{OUTPUT_BASE}/{camera}_driving.mp4", 
            0
        ))

video_df = spark.createDataFrame(sample_data, schema)

# Save as table
table_name = f"{CATALOG}.{SCHEMA}.video_metadata"
video_df.write.mode("overwrite").saveAsTable(table_name)

print(f"✅ Video metadata saved to: {table_name}")
display(video_df)

# COMMAND ----------

# Summary
print("=" * 60)
print("✅ Video setup complete!")
print("=" * 60)
print(f"  Placeholders created: {len(CAMERAS)}")
print(f"  Metadata table: {CATALOG}.{SCHEMA}.video_metadata")
print(f"  Output directory: {OUTPUT_BASE}")
