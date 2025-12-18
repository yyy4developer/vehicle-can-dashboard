# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - CAN Data Generator
# MAGIC 
# MAGIC CANダミーデータを生成するノートブック。
# MAGIC リアルな運転シナリオとイベント（急ブレーキ、急加速、急ハンドル）を含む。
# MAGIC 
# MAGIC ## 運転シナリオ
# MAGIC 10分間の典型的なドライブを再現:
# MAGIC 1. **発進・加速** (0:00-1:00) - 駐車場から道路へ
# MAGIC 2. **市街地走行** (1:00-3:00) - 信号停止、右左折
# MAGIC 3. **高速道路合流** (3:00-4:00) - 急加速で合流
# MAGIC 4. **高速巡航** (4:00-6:00) - 100km/h安定走行
# MAGIC 5. **追い越し** (6:00-6:30) - 加速して車線変更
# MAGIC 6. **緊急ブレーキ** (6:30-7:00) - 前方障害物回避
# MAGIC 7. **高速道路出口** (7:00-8:00) - 減速してランプへ
# MAGIC 8. **市街地戻り** (8:00-9:30) - 交差点通過
# MAGIC 9. **駐車** (9:30-10:00) - 減速して停止
# MAGIC 
# MAGIC ## 前提条件
# MAGIC - Volume `raw` が作成済み
# MAGIC - クラスターに numpy, pandas がインストール済み

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ウィジェットでパラメータを受け取る
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "yao_demo_vehicle_app", "Schema Name")
dbutils.widgets.text("vehicle_id", "VH001", "Vehicle ID")
dbutils.widgets.text("duration_seconds", "600", "Duration (seconds)")
dbutils.widgets.text("scenario", "realistic", "Scenario Type")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VEHICLE_ID = dbutils.widgets.get("vehicle_id")
DURATION_SECONDS = int(dbutils.widgets.get("duration_seconds"))
SCENARIO_TYPE = dbutils.widgets.get("scenario")

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Vehicle ID: {VEHICLE_ID}")
print(f"  Duration: {DURATION_SECONDS}s")
print(f"  Scenario: {SCENARIO_TYPE}")

# COMMAND ----------

# Output path configuration
VOLUME = "raw"
OUTPUT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# CAN message definitions
CAN_MESSAGES = {
    0x100: {"name": "VehicleSpeed", "period_ms": 20},
    0x101: {"name": "EngineData", "period_ms": 10},
    0x102: {"name": "BrakeData", "period_ms": 20},
    0x103: {"name": "SteeringData", "period_ms": 50},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import numpy as np
import pandas as pd
import struct
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Tuple
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driving Scenario Generator

# COMMAND ----------

@dataclass
class VehicleState:
    """Current vehicle state"""
    speed_kmh: float = 0.0
    rpm: float = 800.0
    throttle_pct: float = 0.0
    brake_pressure: float = 0.0
    brake_active: bool = False
    steering_angle: float = 0.0  # -1080 to 1080 degrees


@dataclass
class DrivingPhase:
    """A phase in the driving scenario"""
    name: str
    start_sec: float
    end_sec: float
    target_speed: float
    events: List[dict]  # List of events within this phase


class RealisticDrivingScenario:
    """
    Generates realistic driving scenarios based on predefined phases.
    
    10分間のリアルなドライブシナリオ:
    - 発進、市街地、高速、緊急ブレーキ、駐車
    """
    
    def __init__(self, duration_seconds: int = 600):
        self.duration = duration_seconds
        self.state = VehicleState()
        self.events: List[dict] = []
        self.phases = self._create_phases()
        
    def _create_phases(self) -> List[DrivingPhase]:
        """Define the driving phases for a 10-minute realistic scenario"""
        # Scale phases to actual duration
        scale = self.duration / 600.0
        
        phases = [
            # Phase 1: 発進・加速 (0:00-1:00) - 駐車場から道路へ
            DrivingPhase(
                name="departure",
                start_sec=0 * scale,
                end_sec=60 * scale,
                target_speed=40,
                events=[
                    {"time_offset": 5, "type": "start_engine", "description": "エンジン始動"},
                ]
            ),
            
            # Phase 2: 市街地走行 (1:00-3:00) - 信号停止、右左折
            DrivingPhase(
                name="city_driving",
                start_sec=60 * scale,
                end_sec=180 * scale,
                target_speed=50,
                events=[
                    {"time_offset": 20, "type": "traffic_stop", "description": "信号停止"},
                    {"time_offset": 50, "type": "right_turn", "description": "右折", "steering": 450},
                    {"time_offset": 80, "type": "traffic_stop", "description": "信号停止"},
                    {"time_offset": 100, "type": "left_turn", "description": "左折", "steering": -400},
                ]
            ),
            
            # Phase 3: 高速道路合流 (3:00-4:00) - 急加速で合流
            DrivingPhase(
                name="highway_merge",
                start_sec=180 * scale,
                end_sec=240 * scale,
                target_speed=100,
                events=[
                    {"time_offset": 10, "type": "hard_acceleration", "description": "合流加速", "throttle": 95},
                    {"time_offset": 40, "type": "lane_change_right", "description": "本線合流", "steering": 180},
                ]
            ),
            
            # Phase 4: 高速巡航 (4:00-6:00) - 100km/h安定走行
            DrivingPhase(
                name="highway_cruise",
                start_sec=240 * scale,
                end_sec=360 * scale,
                target_speed=100,
                events=[
                    {"time_offset": 30, "type": "slight_curve_right", "description": "緩やかなカーブ（右）", "steering": 80},
                    {"time_offset": 70, "type": "slight_curve_left", "description": "緩やかなカーブ（左）", "steering": -90},
                ]
            ),
            
            # Phase 5: 追い越し (6:00-6:30) - 加速して車線変更
            DrivingPhase(
                name="overtaking",
                start_sec=360 * scale,
                end_sec=390 * scale,
                target_speed=120,
                events=[
                    {"time_offset": 5, "type": "hard_acceleration", "description": "追い越し加速", "throttle": 90},
                    {"time_offset": 10, "type": "lane_change_left", "description": "追い越し車線へ", "steering": -200},
                    {"time_offset": 20, "type": "lane_change_right", "description": "走行車線へ戻る", "steering": 190},
                ]
            ),
            
            # Phase 6: 緊急ブレーキ (6:30-7:00) - 前方障害物回避
            DrivingPhase(
                name="emergency_braking",
                start_sec=390 * scale,
                end_sec=420 * scale,
                target_speed=60,
                events=[
                    {"time_offset": 5, "type": "emergency_brake", "description": "緊急ブレーキ！前方障害物", "brake": 100},
                    {"time_offset": 8, "type": "evasive_steering", "description": "回避ステアリング", "steering": -350},
                ]
            ),
            
            # Phase 7: 高速道路出口 (7:00-8:00) - 減速してランプへ
            DrivingPhase(
                name="highway_exit",
                start_sec=420 * scale,
                end_sec=480 * scale,
                target_speed=50,
                events=[
                    {"time_offset": 10, "type": "deceleration", "description": "出口へ減速"},
                    {"time_offset": 30, "type": "exit_curve", "description": "ランプカーブ", "steering": 300},
                ]
            ),
            
            # Phase 8: 市街地戻り (8:00-9:30) - 交差点通過
            DrivingPhase(
                name="city_return",
                start_sec=480 * scale,
                end_sec=570 * scale,
                target_speed=40,
                events=[
                    {"time_offset": 20, "type": "traffic_stop", "description": "信号停止"},
                    {"time_offset": 40, "type": "pedestrian_stop", "description": "歩行者待ち", "brake": 70},
                    {"time_offset": 60, "type": "right_turn", "description": "右折", "steering": 380},
                ]
            ),
            
            # Phase 9: 駐車 (9:30-10:00) - 減速して停止
            DrivingPhase(
                name="parking",
                start_sec=570 * scale,
                end_sec=600 * scale,
                target_speed=0,
                events=[
                    {"time_offset": 10, "type": "parking_maneuver", "description": "駐車操作", "steering": -500},
                    {"time_offset": 25, "type": "full_stop", "description": "完全停止"},
                ]
            ),
        ]
        return phases
    
    def _get_current_phase(self, time_sec: float) -> DrivingPhase:
        """Get the current driving phase based on time"""
        for phase in self.phases:
            if phase.start_sec <= time_sec < phase.end_sec:
                return phase
        return self.phases[-1]
    
    def _apply_event(self, event: dict, timestamp: datetime):
        """Apply an event to the vehicle state"""
        event_type = event["type"]
        
        if event_type in ["emergency_brake", "pedestrian_stop"]:
            self.state.brake_pressure = event.get("brake", 100)
            self.state.brake_active = True
            self.state.throttle_pct = 0
        elif event_type == "traffic_stop":
            self.state.brake_pressure = 50
            self.state.brake_active = True
            self.state.throttle_pct = 0
        elif event_type == "hard_acceleration":
            self.state.throttle_pct = event.get("throttle", 90)
            self.state.brake_pressure = 0
            self.state.brake_active = False
        elif event_type in ["right_turn", "left_turn", "lane_change_right", "lane_change_left", 
                           "exit_curve", "evasive_steering", "parking_maneuver",
                           "slight_curve_right", "slight_curve_left"]:
            self.state.steering_angle = event.get("steering", 0)
        elif event_type == "deceleration":
            self.state.throttle_pct = 10
            self.state.brake_pressure = 20
            self.state.brake_active = True
        elif event_type == "full_stop":
            self.state.brake_pressure = 30
            self.state.brake_active = True
            self.state.throttle_pct = 0
        
        # Record event
        self.events.append({
            "timestamp": timestamp,
            "type": event_type,
            "description": event.get("description", event_type),
            "speed_kmh": self.state.speed_kmh
        })
    
    def generate_timeline(self, start_time: datetime) -> List[dict]:
        """Generate vehicle state timeline at 10ms resolution"""
        timeline = []
        dt_ms = 10  # 10ms resolution
        scale = self.duration / 600.0
        
        # Build event schedule
        event_schedule = {}
        for phase in self.phases:
            for event in phase.events:
                event_time_ms = int((phase.start_sec + event["time_offset"] * scale) * 1000)
                event_schedule[event_time_ms] = event
        
        active_event_duration = 0
        current_phase = None
        
        for ms in range(0, self.duration * 1000, dt_ms):
            time_sec = ms / 1000.0
            timestamp = start_time + timedelta(milliseconds=ms)
            
            # Get current phase
            phase = self._get_current_phase(time_sec)
            if phase != current_phase:
                current_phase = phase
            
            # Check for events
            if ms in event_schedule and active_event_duration <= 0:
                self._apply_event(event_schedule[ms], timestamp)
                active_event_duration = random.randint(2000, 4000)  # Event lasts 2-4 seconds
            
            # Update physics towards target speed
            self._update_physics(dt_ms / 1000.0, phase.target_speed)
            
            # Event recovery
            if active_event_duration > 0:
                active_event_duration -= dt_ms
                if active_event_duration <= 0:
                    # Gradually return to normal
                    self.state.brake_pressure = 0
                    self.state.brake_active = False
                    self.state.throttle_pct = 30
            
            timeline.append({
                "timestamp": timestamp,
                "speed_kmh": self.state.speed_kmh,
                "rpm": self.state.rpm,
                "throttle_pct": self.state.throttle_pct,
                "brake_pressure": self.state.brake_pressure,
                "brake_active": self.state.brake_active,
                "steering_angle": self.state.steering_angle,
            })
        
        return timeline
    
    def _update_physics(self, dt: float, target_speed: float):
        """Physics update with target speed tracking"""
        speed_diff = target_speed - self.state.speed_kmh
        
        # Auto throttle/brake to reach target speed (cruise control behavior)
        if not self.state.brake_active and self.state.throttle_pct < 50:
            if speed_diff > 5:
                self.state.throttle_pct = min(60, 30 + speed_diff)
            elif speed_diff < -5:
                self.state.throttle_pct = max(0, 10)
                self.state.brake_pressure = min(30, -speed_diff)
                self.state.brake_active = self.state.brake_pressure > 10
            else:
                self.state.throttle_pct = 25 + random.uniform(-5, 5)
        
        # Acceleration from throttle
        if self.state.throttle_pct > 0 and not self.state.brake_active:
            accel = self.state.throttle_pct * 0.12
            self.state.speed_kmh = min(140, self.state.speed_kmh + accel * dt)
        
        # Deceleration from braking
        if self.state.brake_active:
            decel = self.state.brake_pressure * 0.4
            self.state.speed_kmh = max(0, self.state.speed_kmh - decel * dt)
        
        # Natural deceleration
        if self.state.throttle_pct == 0 and not self.state.brake_active:
            self.state.speed_kmh = max(0, self.state.speed_kmh - 3 * dt)
        
        # RPM based on speed and throttle (simulated automatic transmission)
        gear = min(6, max(1, int(self.state.speed_kmh / 25) + 1))
        base_rpm = 800 + (self.state.speed_kmh / gear) * 80
        throttle_rpm = self.state.throttle_pct * 15
        self.state.rpm = min(6500, max(800, base_rpm + throttle_rpm + random.uniform(-30, 30)))
        
        # Steering naturally returns to center with some road noise
        self.state.steering_angle *= (1 - 2.0 * dt)
        self.state.steering_angle += random.uniform(-3, 3)
        
        # Brake release when not active
        if not self.state.brake_active:
            self.state.brake_pressure = max(0, self.state.brake_pressure - 100 * dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAN Frame Encoder

# COMMAND ----------

def encode_vehicle_speed(speed_kmh: float) -> bytes:
    """Encode vehicle speed to CAN data (arb_id 0x100)"""
    # speed_kmh: bytes[0:2], scale=0.01
    speed_raw = int(speed_kmh / 0.01)
    return struct.pack(">H", speed_raw) + b'\x00' * 6


def encode_engine_data(rpm: float, throttle_pct: float) -> bytes:
    """Encode engine data to CAN data (arb_id 0x101)"""
    # rpm: bytes[0:2], scale=0.25; throttle: byte[2], scale=0.4
    rpm_raw = int(rpm / 0.25)
    throttle_raw = int(throttle_pct / 0.4)
    return struct.pack(">HB", rpm_raw, throttle_raw) + b'\x00' * 5


def encode_brake_data(pressure: float, active: bool) -> bytes:
    """Encode brake data to CAN data (arb_id 0x102)"""
    # pressure: byte[0], scale=0.4; active: byte[1] bit0
    pressure_raw = int(pressure / 0.4)
    active_raw = 1 if active else 0
    return struct.pack(">BB", pressure_raw, active_raw) + b'\x00' * 6


def encode_steering_data(angle: float) -> bytes:
    """Encode steering data to CAN data (arb_id 0x103)"""
    # angle: bytes[0:2], scale=0.1, offset=-1080
    angle_raw = int((angle + 1080) / 0.1)
    return struct.pack(">H", angle_raw) + b'\x00' * 6


def state_to_can_frames(timestamp: datetime, state: dict) -> List[dict]:
    """Convert vehicle state to CAN frames"""
    ts = timestamp.timestamp()
    frames = []
    
    # VehicleSpeed (0x100)
    frames.append({
        "ts": ts,
        "channel": "can0",
        "arb_id": 0x100,
        "dlc": 8,
        "data": encode_vehicle_speed(state["speed_kmh"])
    })
    
    # EngineData (0x101)
    frames.append({
        "ts": ts,
        "channel": "can0",
        "arb_id": 0x101,
        "dlc": 8,
        "data": encode_engine_data(state["rpm"], state["throttle_pct"])
    })
    
    # BrakeData (0x102)
    frames.append({
        "ts": ts,
        "channel": "can0",
        "arb_id": 0x102,
        "dlc": 8,
        "data": encode_brake_data(state["brake_pressure"], state["brake_active"])
    })
    
    # SteeringData (0x103)
    frames.append({
        "ts": ts,
        "channel": "can0",
        "arb_id": 0x103,
        "dlc": 8,
        "data": encode_steering_data(state["steering_angle"])
    })
    
    return frames

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------

def generate_can_data(
    vehicle_id: str = "VH001",
    duration_seconds: int = 600,
) -> Tuple[pd.DataFrame, List[dict]]:
    """Generate CAN data for a vehicle session with realistic driving scenario"""
    
    scenario = RealisticDrivingScenario(duration_seconds)
    start_time = datetime.now()
    
    # Generate timeline
    timeline = scenario.generate_timeline(start_time)
    
    # Convert to CAN frames (sample based on message periods)
    can_frames = []
    last_sent = {arb_id: 0 for arb_id in CAN_MESSAGES}
    
    for state in timeline:
        ts_ms = int((state["timestamp"] - start_time).total_seconds() * 1000)
        
        for frame in state_to_can_frames(state["timestamp"], state):
            arb_id = frame["arb_id"]
            period_ms = CAN_MESSAGES[arb_id]["period_ms"]
            
            if ts_ms - last_sent[arb_id] >= period_ms:
                can_frames.append(frame)
                last_sent[arb_id] = ts_ms
    
    df = pd.DataFrame(can_frames)
    return df, scenario.events

# COMMAND ----------

# Generate data
print(f"Generating {DURATION_SECONDS}s of realistic CAN data...")
can_df, events = generate_can_data(VEHICLE_ID, DURATION_SECONDS)

print(f"✅ Generated {len(can_df)} CAN frames")
print(f"✅ Detected events: {len(events)}")
print("\n📋 イベントタイムライン:")
for e in events:
    desc = e.get('description', e['type'])
    print(f"  [{e['timestamp'].strftime('%H:%M:%S')}] {desc} (速度: {e['speed_kmh']:.1f} km/h)")

# COMMAND ----------

# Preview data
display(can_df.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Volume

# COMMAND ----------

# Create output path with timestamp
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"{OUTPUT_BASE}/{VEHICLE_ID}/{timestamp_str}"

print(f"Output path: {output_path}")

# COMMAND ----------

# Convert to Spark DataFrame and save
spark_df = spark.createDataFrame(can_df)
spark_df.write.mode("overwrite").parquet(f"{output_path}/can_frames.parquet")
print(f"✅ CAN frames saved to: {output_path}/can_frames.parquet")

# Save events as well
if events:
    events_df = spark.createDataFrame(pd.DataFrame(events))
    events_df.write.mode("overwrite").parquet(f"{output_path}/events.parquet")
    print(f"✅ Events saved to: {output_path}/events.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Saved Data

# COMMAND ----------

# Verify saved data
print("Verifying saved data...")
display(spark.read.parquet(f"{output_path}/can_frames.parquet").limit(10))

# COMMAND ----------

# Summary
print("=" * 60)
print("✅ Data generation complete!")
print("=" * 60)
print(f"  Vehicle ID: {VEHICLE_ID}")
print(f"  Duration: {DURATION_SECONDS}s")
print(f"  CAN frames: {len(can_df)}")
print(f"  Events: {len(events)}")
print(f"  Output: {output_path}")
