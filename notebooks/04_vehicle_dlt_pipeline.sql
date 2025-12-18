-------------------------------------------------------
-- VEHICLE CAN DATA DLT PIPELINE
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- Delta Live Tables パイプラインでCANデータを処理:
-- - Bronze: 生CANフレームの取り込み
-- - Silver: DBCファイルを使用したシグナル復号・品質メトリクス
-- - Gold: イベント検出・統計集計
--
-- すべてのテーブルは同一スキーマ内でテーブル名プレフィックスで区別:
-- - bronze_* : 生データ
-- - silver_* : クレンジング済みデータ
-- - gold_*   : 集計・分析用データ
--
-- 前提条件:
-- - 04_vehicle_dlt_udf.py が同じパイプラインに含まれていること
-- - DBCファイルが /Volumes/{catalog}/{schema}/dbc/vehicle.dbc に存在すること
--
-- 使用方法:
-- このファイルは DLT Pipeline として実行してください。
-- databricks.yml で定義された Pipeline から参照されます。
-------------------------------------------------------

-------------------------------------------------------
-- BRONZE LAYER: Raw CAN Frames
-------------------------------------------------------
-- Ingest raw CAN frames from parquet files using Auto Loader
-- Adds ingestion metadata for lineage tracking
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_can_frames
  (
    -- Data quality constraints
    CONSTRAINT valid_arb_id EXPECT (arb_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_timestamp EXPECT (ts IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_data EXPECT (data IS NOT NULL) ON VIOLATION DROP ROW
  )
  COMMENT "Raw CAN frames ingested from parquet files"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = "true"
  )
AS 
SELECT 
  ts,
  channel,
  arb_id,
  dlc,
  data,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${raw_path}/*/*/can_frames.parquet/",
  format => 'parquet',
  schema => 'ts DOUBLE, channel STRING, arb_id LONG, dlc LONG, data BINARY'
);

-------------------------------------------------------
-- SILVER LAYER: Decoded CAN Signals (SQL decoding)
-------------------------------------------------------
-- Decode CAN frames using pure SQL expressions
-- Based on DBC signal definitions:
--   0x100 (256) VehicleSpeed: bytes[0:2] BE uint16, scale=0.01
--   0x101 (257) EngineData: rpm bytes[0:2] BE uint16 scale=0.25, throttle byte[2] scale=0.4
--   0x102 (258) BrakeData: pressure byte[0] scale=0.4, active byte[1]
--   0x103 (259) SteeringData: angle bytes[0:2] BE uint16 scale=0.1 offset=-1080
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_can_signals
  (
    -- Only process known CAN message types
    CONSTRAINT valid_message_type EXPECT (arb_id IN (256, 257, 258, 259)) ON VIOLATION DROP ROW
  )
  COMMENT "Decoded CAN signals (SQL-based decoding)"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  CAST(from_unixtime(ts) AS TIMESTAMP) AS timestamp,
  ts,
  arb_id,
  channel,
  -- Message name based on arb_id
  CASE arb_id
    WHEN 256 THEN 'VehicleSpeed'
    WHEN 257 THEN 'EngineData'
    WHEN 258 THEN 'BrakeData'
    WHEN 259 THEN 'SteeringData'
  END AS message_name,
  -- VehicleSpeed (0x100): bytes[0:2] big-endian uint16, scale=0.01
  CASE WHEN arb_id = 256 THEN
    (CAST(CONV(hex(substring(data, 1, 1)), 16, 10) AS INT) * 256 + 
     CAST(CONV(hex(substring(data, 2, 1)), 16, 10) AS INT)) * 0.01
  END AS speed_kmh,
  -- EngineData (0x101): rpm = bytes[0:2] BE uint16 scale=0.25
  CASE WHEN arb_id = 257 THEN
    (CAST(CONV(hex(substring(data, 1, 1)), 16, 10) AS INT) * 256 + 
     CAST(CONV(hex(substring(data, 2, 1)), 16, 10) AS INT)) * 0.25
  END AS rpm,
  -- EngineData (0x101): throttle = byte[2] scale=0.4
  CASE WHEN arb_id = 257 THEN
    CAST(CONV(hex(substring(data, 3, 1)), 16, 10) AS INT) * 0.4
  END AS throttle_pct,
  -- BrakeData (0x102): pressure = byte[0] scale=0.4
  CASE WHEN arb_id = 258 THEN
    CAST(CONV(hex(substring(data, 1, 1)), 16, 10) AS INT) * 0.4
  END AS brake_pressure,
  -- BrakeData (0x102): active = byte[1] != 0
  CASE WHEN arb_id = 258 THEN
    CAST(CONV(hex(substring(data, 2, 1)), 16, 10) AS INT) != 0
  END AS brake_active,
  -- SteeringData (0x103): angle = bytes[0:2] BE uint16, scale=0.1, offset=-1080
  CASE WHEN arb_id = 259 THEN
    (CAST(CONV(hex(substring(data, 1, 1)), 16, 10) AS INT) * 256 + 
     CAST(CONV(hex(substring(data, 2, 1)), 16, 10) AS INT)) * 0.1 - 1080
  END AS steering_angle,
  source_file,
  ingestion_time
FROM STREAM LIVE.bronze_can_frames;

-------------------------------------------------------
-- SILVER LAYER: CAN Message Quality Metrics
-------------------------------------------------------
-- Calculate message timing quality per arbitration ID
-- Expected periods (in ms):
--   0x100 (256) VehicleSpeed: 20ms
--   0x101 (257) EngineData: 10ms
--   0x102 (258) BrakeData: 20ms
--   0x103 (259) SteeringData: 50ms
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_can_quality
  COMMENT "CAN message quality metrics per arbitration ID"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  window.start AS window_start,
  window.end AS window_end,
  arb_id,
  -- Message name based on arb_id
  CASE arb_id
    WHEN 256 THEN 'VehicleSpeed'
    WHEN 257 THEN 'EngineData'
    WHEN 258 THEN 'BrakeData'
    WHEN 259 THEN 'SteeringData'
  END AS message_name,
  channel,
  COUNT(*) AS message_count,
  -- Expected period in ms
  CASE arb_id
    WHEN 256 THEN 20.0
    WHEN 257 THEN 10.0
    WHEN 258 THEN 20.0
    WHEN 259 THEN 50.0
    ELSE 100.0
  END AS expected_period_ms,
  -- Expected count per minute
  CASE arb_id
    WHEN 256 THEN 3000  -- 60000 / 20
    WHEN 257 THEN 6000  -- 60000 / 10
    WHEN 258 THEN 3000  -- 60000 / 20
    WHEN 259 THEN 1200  -- 60000 / 50
    ELSE 600
  END AS expected_count,
  -- Missing rate calculation
  1 - (COUNT(*) / CASE arb_id
    WHEN 256 THEN 3000.0
    WHEN 257 THEN 6000.0
    WHEN 258 THEN 3000.0
    WHEN 259 THEN 1200.0
    ELSE 600.0
  END) AS missing_rate,
  MIN(ts) AS first_ts,
  MAX(ts) AS last_ts
FROM STREAM LIVE.bronze_can_frames
GROUP BY 
  window(CAST(from_unixtime(ts) AS TIMESTAMP), '1 minute'),
  arb_id,
  channel;

-------------------------------------------------------
-- GOLD LAYER: Aggregated Signals (100ms intervals)
-------------------------------------------------------
-- Aggregate signals at 100ms intervals for analysis
-- Combines all signal types into single rows using MAX to get non-null values
-- Each signal type comes from a different CAN message, so we merge them
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_signals_aggregated
  COMMENT "Aggregated signals at 100ms intervals"
AS 
SELECT 
  window.start AS timestamp,
  -- Use MAX to get the non-null value (each signal type only appears in one message type)
  MAX(speed_kmh) AS speed_kmh,
  MAX(rpm) AS rpm,
  MAX(throttle_pct) AS throttle_pct,
  MAX(brake_pressure) AS brake_pressure,
  MAX(CASE WHEN brake_active THEN 1 ELSE 0 END) = 1 AS brake_active,
  MAX(steering_angle) AS steering_angle,
  source_file
FROM LIVE.silver_can_signals
GROUP BY 
  window(timestamp, '100 milliseconds'),
  source_file;

-------------------------------------------------------
-- GOLD LAYER: Event Detection
-------------------------------------------------------
-- Detect driving events from aggregated signal data:
-- - hard_brake: Deceleration > 35 km/h/s (~1g emergency brake)
-- - hard_acceleration: Acceleration > 35 km/h/s (~1g aggressive accel)
-- - sharp_turn: Steering change > 200 deg/s (rapid steering input)
-- 
-- Note: LAG() window function is not supported in streaming,
-- so we use a MATERIALIZED VIEW instead of STREAMING TABLE.
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_event_history
  COMMENT "Detected driving events (hard brake, acceleration, sharp turns)"
AS 
SELECT 
  timestamp,
  CASE 
    WHEN acceleration < -35 THEN 'hard_brake'
    WHEN acceleration > 35 THEN 'hard_acceleration'
    WHEN steering_diff > 20 THEN 'sharp_turn'
  END AS event_type,
  speed_kmh,
  acceleration,
  steering_angle,
  steering_diff,
  brake_pressure,
  source_file
FROM (
  SELECT 
    timestamp,
    speed_kmh,
    steering_angle,
    brake_pressure,
    source_file,
    (speed_kmh - LAG(speed_kmh, 1) OVER (PARTITION BY source_file ORDER BY timestamp)) / 0.1 AS acceleration,
    ABS(steering_angle - LAG(steering_angle, 1) OVER (PARTITION BY source_file ORDER BY timestamp)) AS steering_diff
  FROM LIVE.gold_signals_aggregated
)
WHERE 
  acceleration < -35 OR 
  acceleration > 35 OR 
  steering_diff > 20;

-------------------------------------------------------
-- GOLD LAYER: Daily Vehicle Statistics
-------------------------------------------------------
-- Calculate daily vehicle statistics:
-- - Average/max speed and RPM
-- - Approximate distance traveled
-- - Sample counts and time range
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_vehicle_stats
  COMMENT "Daily vehicle statistics"
AS 
SELECT 
  DATE(timestamp) AS date,
  source_file,
  AVG(speed_kmh) AS avg_speed_kmh,
  MAX(speed_kmh) AS max_speed_kmh,
  AVG(rpm) AS avg_rpm,
  MAX(rpm) AS max_rpm,
  SUM(
    CASE 
      WHEN speed_kmh IS NOT NULL 
      THEN speed_kmh * 0.02 / 3600 
      ELSE 0 
    END
  ) AS distance_km,
  COUNT(*) AS sample_count,
  MIN(timestamp) AS first_timestamp,
  MAX(timestamp) AS last_timestamp
FROM LIVE.silver_can_signals
GROUP BY 
  DATE(timestamp),
  source_file;

-------------------------------------------------------
-- GOLD LAYER: Latest Signals (Real-time Dashboard)
-------------------------------------------------------
-- Get latest signal values per source per second
-- Merges all signal types into single rows
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_latest_signals
  COMMENT "Latest signal values for real-time dashboard"
AS 
SELECT 
  window.end AS timestamp,
  -- Use MAX to get the non-null value from each signal type
  MAX(speed_kmh) AS speed_kmh,
  MAX(rpm) AS rpm,
  MAX(throttle_pct) AS throttle_pct,
  MAX(brake_pressure) AS brake_pressure,
  MAX(CASE WHEN brake_active THEN 1 ELSE 0 END) = 1 AS brake_active,
  MAX(steering_angle) AS steering_angle,
  source_file
FROM LIVE.silver_can_signals
GROUP BY 
  window(timestamp, '1 second'),
  source_file;

-------------------------------------------------------
-- ARCHITECTURE: DBC-based Decoding
-------------------------------------------------------
-- 
-- 1. UDF Definition (04_vehicle_dlt_udf.py):
--    - Loads DBC file from Volume using cantools
--    - Defines decode_can_frame UDF
--    - Defines get_message_info UDF
-- 
-- 2. Benefits of DBC-based Approach:
--    - Single source of truth for signal definitions
--    - Easy to update decoding by changing DBC file
--    - Standard automotive industry format
--    - Self-documenting signal specifications
-- 
-- 3. DBC File Location:
--    /Volumes/{catalog}/{schema}/dbc/vehicle.dbc
-- 
-- 4. Supported Messages:
--    - 0x100 (256): VehicleSpeed - speed_kmh
--    - 0x101 (257): EngineData - rpm, throttle
--    - 0x102 (258): BrakeData - pressure, active
--    - 0x103 (259): SteeringData - angle
-- 
-- 5. Performance Considerations:
--    - UDF loads DBC on each partition (consider broadcast)
--    - For high-volume data, consider caching DBC in memory
--    - Production: Use Spark broadcast variables
-------------------------------------------------------
