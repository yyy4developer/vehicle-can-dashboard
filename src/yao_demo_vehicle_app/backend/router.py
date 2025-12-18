from __future__ import annotations
from typing import Annotated, Optional
from datetime import datetime, timedelta
import os
import random
import math
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User as UserOut

from .models import (
    VersionOut,
    SignalDataOut,
    SignalTimeSeriesOut,
    EventOut,
    EventListOut,
    EventType,
    VehicleStatsOut,
    VehicleStatsSummaryOut,
    CANQualityOut,
    CANQualityMetric,
    TimeRange,
    CameraType,
    VideoMetadataOut,
    VideoListOut,
)
from .dependencies import get_obo_ws  # Used only for /current-user endpoint
from .config import conf
from .runtime import rt
from .logger import logger

api = APIRouter(prefix=conf.api_prefix)


# ============================================
# Version & User
# ============================================

@api.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@api.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


# ============================================
# Mock Data Generator (Fallback)
# ============================================

def _generate_mock_signals(
    start_time: datetime,
    duration_minutes: int = 10,
    interval_ms: int = 1000
) -> list[SignalDataOut]:
    """Generate mock signal data for demo purposes"""
    signals = []
    current_speed = 60.0
    current_rpm = 2500.0
    current_throttle = 30.0
    current_brake = 0.0
    current_steering = 0.0
    
    num_points = int(duration_minutes * 60 * 1000 / interval_ms)
    
    for i in range(num_points):
        speed_delta = random.gauss(0, 2)
        current_speed = max(0, min(180, current_speed + speed_delta))
        
        current_rpm = 800 + current_speed * 30 + random.gauss(0, 100)
        current_throttle = max(0, min(100, 30 + random.gauss(0, 10)))
        
        if random.random() < 0.05:
            current_brake = random.uniform(20, 80)
        else:
            current_brake = max(0, current_brake - 5)
        
        current_steering = current_steering * 0.9 + random.gauss(0, 20)
        current_steering = max(-200, min(200, current_steering))
        
        timestamp = start_time + timedelta(milliseconds=i * interval_ms)
        
        signals.append(SignalDataOut(
            timestamp=timestamp,
            speed_kmh=round(current_speed, 2),
            rpm=round(current_rpm, 0),
            throttle_pct=round(current_throttle, 1),
            brake_pressure=round(current_brake, 1),
            brake_active=current_brake > 10,
            steering_angle=round(current_steering, 1),
        ))
    
    return signals


def _generate_mock_events(
    start_time: datetime,
    duration_minutes: int = 10
) -> list[EventOut]:
    """Generate mock events for demo purposes"""
    events = []
    event_types = list(EventType)
    
    num_events = random.randint(3, 8)
    
    for i in range(num_events):
        event_time = start_time + timedelta(
            minutes=random.uniform(0, duration_minutes)
        )
        event_type = random.choice(event_types)
        
        events.append(EventOut(
            id=f"evt_{i+1:03d}",
            event_type=event_type,
            timestamp=event_time,
            speed_kmh=random.uniform(30, 100),
            acceleration=random.uniform(-30, 30) if event_type != EventType.SHARP_TURN else None,
            steering_angle=random.uniform(-300, 300) if event_type == EventType.SHARP_TURN else None,
            brake_pressure=random.uniform(50, 100) if event_type == EventType.HARD_BRAKE else None,
            vehicle_id="VH001",
        ))
    
    return sorted(events, key=lambda e: e.timestamp)


# ============================================
# Data Fetchers from DLT Tables
# ============================================

def _fetch_signals_from_dlt(
    time_range: TimeRange,
    vehicle_id: str,
    limit: int = 600
) -> list[SignalDataOut]:
    """Fetch signals from gold_signals_aggregated table using Service Principal
    
    Uses relative time filter based on MAX(timestamp) in the table, not current_timestamp().
    This ensures we get data even if the table contains historical data.
    """
    logger.info(f"Fetching signals from DLT for time_range={time_range}, vehicle_id={vehicle_id}")
    try:
        # Time filter based on range - use MAX(timestamp) as reference instead of current_timestamp()
        # This ensures we get data even if the table has historical data
        if time_range == TimeRange.LAST_10_MIN:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 10 MINUTES FROM gold_signals_aggregated)"
        elif time_range == TimeRange.LAST_1_HOUR:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 1 HOUR FROM gold_signals_aggregated)"
        elif time_range == TimeRange.TODAY:
            time_filter = "DATE(timestamp) = (SELECT DATE(MAX(timestamp)) FROM gold_signals_aggregated)"
        elif time_range == TimeRange.LAST_24_HOURS:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 24 HOURS FROM gold_signals_aggregated)"
        else:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 10 MINUTES FROM gold_signals_aggregated)"
        
        sql = f"""
        SELECT 
            timestamp,
            speed_kmh,
            rpm,
            throttle_pct,
            brake_pressure,
            brake_active,
            steering_angle
        FROM gold_signals_aggregated
        WHERE {time_filter}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        rows = rt.execute_sql(sql)
        signals = []
        
        for row in rows:
            # Parse timestamp
            ts = row.get("timestamp")
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00").replace("+00:00", ""))
            
            signals.append(SignalDataOut(
                timestamp=ts or datetime.now(),
                speed_kmh=float(row.get("speed_kmh") or 0),
                rpm=float(row.get("rpm") or 0),
                throttle_pct=float(row.get("throttle_pct") or 0),
                brake_pressure=float(row.get("brake_pressure") or 0),
                brake_active=bool(row.get("brake_active")),
                steering_angle=float(row.get("steering_angle") or 0),
            ))
        
        # Reverse to get chronological order
        signals.reverse()
        return signals
        
    except Exception as e:
        logger.warning(f"Failed to fetch signals from DLT: {e}")
        return []


def _fetch_events_from_dlt(
    time_range: TimeRange,
    event_type: Optional[EventType] = None,
    limit: int = 50
) -> list[EventOut]:
    """Fetch events from gold_event_history table using Service Principal
    
    Uses relative time filter based on MAX(timestamp) in the table, not current_timestamp().
    """
    try:
        # Time filter based on range - use MAX(timestamp) as reference
        if time_range == TimeRange.LAST_10_MIN:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 10 MINUTES FROM gold_event_history)"
        elif time_range == TimeRange.LAST_1_HOUR:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 1 HOUR FROM gold_event_history)"
        elif time_range == TimeRange.TODAY:
            time_filter = "DATE(timestamp) = (SELECT DATE(MAX(timestamp)) FROM gold_event_history)"
        elif time_range == TimeRange.LAST_24_HOURS:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 24 HOURS FROM gold_event_history)"
        else:
            time_filter = "timestamp >= (SELECT MAX(timestamp) - INTERVAL 10 MINUTES FROM gold_event_history)"
        
        # Event type filter
        event_filter = ""
        if event_type:
            event_filter = f"AND event_type = '{event_type.value}'"
        
        sql = f"""
        SELECT 
            timestamp,
            event_type,
            speed_kmh,
            acceleration,
            steering_angle,
            brake_pressure,
            source_file
        FROM gold_event_history
        WHERE {time_filter} {event_filter}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        rows = rt.execute_sql(sql)
        events = []
        
        for i, row in enumerate(rows):
            ts = row.get("timestamp")
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00").replace("+00:00", ""))
            
            evt_type_str = row.get("event_type", "hard_brake")
            try:
                evt_type = EventType(evt_type_str)
            except ValueError:
                evt_type = EventType.HARD_BRAKE
            
            accel_val = row.get("acceleration")
            steering_val = row.get("steering_angle")
            brake_val = row.get("brake_pressure")
            
            events.append(EventOut(
                id=f"evt_{i+1:03d}",
                event_type=evt_type,
                timestamp=ts or datetime.now(),
                speed_kmh=float(row.get("speed_kmh") or 0),
                acceleration=float(accel_val) if accel_val is not None else None,
                steering_angle=float(steering_val) if steering_val is not None else None,
                brake_pressure=float(brake_val) if brake_val is not None else None,
                vehicle_id="VH001",
            ))
        
        return events
        
    except Exception as e:
        logger.warning(f"Failed to fetch events from DLT: {e}")
        return []


def _fetch_stats_from_dlt(vehicle_id: str) -> Optional[VehicleStatsOut]:
    """Fetch statistics from gold_vehicle_stats table using Service Principal"""
    try:
        sql = """
        SELECT 
            date,
            source_file,
            avg_speed_kmh,
            max_speed_kmh,
            avg_rpm,
            max_rpm,
            distance_km,
            sample_count,
            first_timestamp,
            last_timestamp
        FROM gold_vehicle_stats
        ORDER BY date DESC
        LIMIT 1
        """
        
        rows = rt.execute_sql(sql)
        if not rows:
            return None
        
        row = rows[0]
        
        # Fetch event counts
        event_sql = """
        SELECT 
            event_type,
            COUNT(*) as count
        FROM gold_event_history
        WHERE DATE(timestamp) = current_date()
        GROUP BY event_type
        """
        event_rows = rt.execute_sql(event_sql)
        
        hard_brake_count = 0
        hard_accel_count = 0
        sharp_turn_count = 0
        
        for er in event_rows:
            evt_type = er.get("event_type", "")
            count = int(er.get("count", 0))
            if evt_type == "hard_brake":
                hard_brake_count = count
            elif evt_type == "hard_acceleration":
                hard_accel_count = count
            elif evt_type == "sharp_turn":
                sharp_turn_count = count
        
        # Parse date
        date_val = row.get("date")
        if isinstance(date_val, str):
            date_val = datetime.fromisoformat(date_val)
        
        # Calculate driving duration
        first_ts = row.get("first_timestamp")
        last_ts = row.get("last_timestamp")
        duration_minutes = 0.0
        if first_ts and last_ts:
            if isinstance(first_ts, str):
                first_ts = datetime.fromisoformat(first_ts.replace("Z", "+00:00").replace("+00:00", ""))
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts.replace("Z", "+00:00").replace("+00:00", ""))
            if isinstance(first_ts, datetime) and isinstance(last_ts, datetime):
                duration_minutes = (last_ts - first_ts).total_seconds() / 60
        
        return VehicleStatsOut(
            vehicle_id=vehicle_id,
            date=date_val or datetime.now(),
            avg_speed_kmh=float(row.get("avg_speed_kmh") or 0),
            max_speed_kmh=float(row.get("max_speed_kmh") or 0),
            avg_rpm=float(row.get("avg_rpm") or 0),
            max_rpm=float(row.get("max_rpm") or 0),
            distance_km=float(row.get("distance_km") or 0),
            total_events=hard_brake_count + hard_accel_count + sharp_turn_count,
            hard_brake_count=hard_brake_count,
            hard_acceleration_count=hard_accel_count,
            sharp_turn_count=sharp_turn_count,
            driving_duration_minutes=round(duration_minutes, 1),
        )
        
    except Exception as e:
        logger.warning(f"Failed to fetch stats from DLT: {e}")
        return None


def _fetch_latest_signal_from_dlt(vehicle_id: str) -> Optional[SignalDataOut]:
    """Fetch latest signal from gold_latest_signals table using Service Principal"""
    try:
        sql = """
        SELECT 
            timestamp,
            speed_kmh,
            rpm,
            throttle_pct,
            brake_pressure,
            brake_active,
            steering_angle
        FROM gold_latest_signals
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        rows = rt.execute_sql(sql)
        if not rows:
            return None
        
        row = rows[0]
        ts = row.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00").replace("+00:00", ""))
        
        return SignalDataOut(
            timestamp=ts or datetime.now(),
            speed_kmh=float(row.get("speed_kmh") or 0),
            rpm=float(row.get("rpm") or 0),
            throttle_pct=float(row.get("throttle_pct") or 0),
            brake_pressure=float(row.get("brake_pressure") or 0),
            brake_active=bool(row.get("brake_active")),
            steering_angle=float(row.get("steering_angle") or 0),
        )
        
    except Exception as e:
        logger.warning(f"Failed to fetch latest signal from DLT: {e}")
        return None


def _fetch_quality_from_dlt(vehicle_id: str) -> Optional[CANQualityOut]:
    """Fetch quality metrics from silver_can_quality table using Service Principal"""
    try:
        sql = """
        SELECT 
            window_start,
            window_end,
            arb_id,
            message_name,
            channel,
            message_count,
            expected_count,
            missing_rate,
            expected_period_ms
        FROM silver_can_quality
        WHERE window_end >= current_timestamp() - INTERVAL 5 MINUTES
        ORDER BY window_end DESC, arb_id
        """
        
        rows = rt.execute_sql(sql)
        if not rows:
            return None
        
        # Group by window
        window_start = None
        window_end = None
        metrics = []
        total_health = 0.0
        
        for row in rows:
            ws_val = row.get("window_start")
            we_val = row.get("window_end")
            
            if isinstance(ws_val, str):
                ws_val = datetime.fromisoformat(ws_val.replace("Z", "+00:00").replace("+00:00", ""))
            if isinstance(we_val, str):
                we_val = datetime.fromisoformat(we_val.replace("Z", "+00:00").replace("+00:00", ""))
            
            if window_start is None:
                window_start = ws_val
                window_end = we_val
            
            missing_rate = float(row.get("missing_rate") or 0)
            
            metrics.append(CANQualityMetric(
                arb_id=int(row.get("arb_id") or 0),
                message_name=row.get("message_name") or "Unknown",
                channel=row.get("channel") or "can0",
                message_count=int(row.get("message_count") or 0),
                expected_count=int(row.get("expected_count") or 0),
                missing_rate=round(missing_rate, 4),
                period_ms=int(row.get("expected_period_ms") or 0),
            ))
            
            total_health += (1 - missing_rate)
        
        overall_health = total_health / len(metrics) if metrics else 1.0
        
        return CANQualityOut(
            window_start=window_start or datetime.now() - timedelta(minutes=1),
            window_end=window_end or datetime.now(),
            metrics=metrics,
            overall_health=round(overall_health, 4),
        )
        
    except Exception as e:
        logger.warning(f"Failed to fetch quality from DLT: {e}")
        return None


# ============================================
# Signal Endpoints
# ============================================

@api.get(
    "/signals",
    response_model=SignalTimeSeriesOut,
    operation_id="getSignals"
)
async def get_signals(
    time_range: TimeRange = Query(default=TimeRange.LAST_10_MIN),
    vehicle_id: str = Query(default="VH001"),
):
    """Get signal time series data from DLT tables using Service Principal"""
    logger.info(f"get_signals called: time_range={time_range}, vehicle_id={vehicle_id}, warehouse_id={conf.unity.warehouse_id}")
    
    # Try to fetch from DLT
    if conf.unity.warehouse_id:
        try:
            signals = _fetch_signals_from_dlt(time_range, vehicle_id)
            if signals:
                logger.info(f"Fetched {len(signals)} signals from DLT")
                return SignalTimeSeriesOut(signals=signals, count=len(signals))
        except Exception as e:
            logger.error(f"Error fetching signals from DLT: {type(e).__name__}: {e}", exc_info=True)
            # Fall through to mock data
    
    # Fallback to mock data
    logger.info("Using mock data for signals (no warehouse configured or no data)")
    now = datetime.now()
    
    if time_range == TimeRange.LAST_10_MIN:
        duration = 10
    elif time_range == TimeRange.LAST_1_HOUR:
        duration = 60
    elif time_range == TimeRange.TODAY:
        duration = 60
    else:
        duration = 10
    
    start_time = now - timedelta(minutes=duration)
    signals = _generate_mock_signals(start_time, duration)
    
    return SignalTimeSeriesOut(
        signals=signals,
        count=len(signals)
    )


@api.get(
    "/signals/latest",
    response_model=SignalDataOut,
    operation_id="getLatestSignal"
)
async def get_latest_signal(
    vehicle_id: str = Query(default="VH001"),
):
    """Get the latest signal values using Service Principal"""
    # Try to fetch from DLT
    if conf.unity.warehouse_id:
        signal = _fetch_latest_signal_from_dlt(vehicle_id)
        if signal:
            return signal
    
    # Fallback to mock data
    now = datetime.now()
    signals = _generate_mock_signals(now - timedelta(seconds=1), duration_minutes=1, interval_ms=1000)
    return signals[-1] if signals else SignalDataOut(timestamp=now)


# ============================================
# Event Endpoints
# ============================================

@api.get(
    "/events",
    response_model=EventListOut,
    operation_id="getEvents"
)
async def get_events(
    time_range: TimeRange = Query(default=TimeRange.LAST_10_MIN),
    event_type: Optional[EventType] = Query(default=None),
    vehicle_id: str = Query(default="VH001"),
    limit: int = Query(default=50, le=200),
):
    """Get driving events from DLT tables using Service Principal"""
    # Try to fetch from DLT
    if conf.unity.warehouse_id:
        events = _fetch_events_from_dlt(time_range, event_type, limit)
        if events:
            return EventListOut(events=events, total=len(events))
    
    # Fallback to mock data
    logger.info("Using mock data for events")
    now = datetime.now()
    
    if time_range == TimeRange.LAST_10_MIN:
        duration = 10
    elif time_range == TimeRange.LAST_1_HOUR:
        duration = 60
    else:
        duration = 10
    
    start_time = now - timedelta(minutes=duration)
    events = _generate_mock_events(start_time, duration)
    
    if event_type:
        events = [e for e in events if e.event_type == event_type]
    
    return EventListOut(
        events=events[:limit],
        total=len(events)
    )


# ============================================
# Statistics Endpoints
# ============================================

@api.get(
    "/stats",
    response_model=VehicleStatsOut,
    operation_id="getStats"
)
async def get_stats(
    time_range: TimeRange = Query(default=TimeRange.TODAY),
    vehicle_id: str = Query(default="VH001"),
):
    """Get vehicle statistics from DLT tables using Service Principal"""
    # Try to fetch from DLT
    if conf.unity.warehouse_id:
        stats = _fetch_stats_from_dlt(vehicle_id)
        if stats:
            return stats
    
    # Fallback to mock data
    now = datetime.now()
    
    return VehicleStatsOut(
        vehicle_id=vehicle_id,
        date=now,
        avg_speed_kmh=round(random.uniform(40, 70), 1),
        max_speed_kmh=round(random.uniform(100, 140), 1),
        avg_rpm=round(random.uniform(2000, 3500), 0),
        max_rpm=round(random.uniform(5000, 6500), 0),
        distance_km=round(random.uniform(50, 200), 1),
        total_events=random.randint(5, 15),
        hard_brake_count=random.randint(1, 5),
        hard_acceleration_count=random.randint(1, 4),
        sharp_turn_count=random.randint(1, 3),
        driving_duration_minutes=round(random.uniform(60, 180), 0),
    )


@api.get(
    "/stats/summary",
    response_model=VehicleStatsSummaryOut,
    operation_id="getStatsSummary"
)
async def get_stats_summary(
    vehicle_id: str = Query(default="VH001"),
):
    """Get summary statistics for dashboard cards using Service Principal"""
    # Try to fetch latest signal and stats from DLT
    if conf.unity.warehouse_id:
        latest = _fetch_latest_signal_from_dlt(vehicle_id)
        stats = _fetch_stats_from_dlt(vehicle_id)
        
        if latest:
            return VehicleStatsSummaryOut(
                current_speed_kmh=latest.speed_kmh or 0,
                current_rpm=latest.rpm or 0,
                current_throttle_pct=latest.throttle_pct or 0,
                current_brake_pressure=latest.brake_pressure or 0,
                current_steering_angle=latest.steering_angle or 0,
                avg_speed_kmh=stats.avg_speed_kmh if stats else 0,
                max_speed_kmh=stats.max_speed_kmh if stats else 0,
                total_events=stats.total_events if stats else 0,
                distance_km=stats.distance_km if stats else 0,
            )
    
    # Fallback to mock data
    base_speed = 65 + math.sin(datetime.now().timestamp() / 10) * 20
    
    return VehicleStatsSummaryOut(
        current_speed_kmh=round(max(0, base_speed + random.gauss(0, 5)), 1),
        current_rpm=round(800 + base_speed * 30 + random.gauss(0, 200), 0),
        current_throttle_pct=round(random.uniform(20, 50), 1),
        current_brake_pressure=round(random.uniform(0, 20), 1),
        current_steering_angle=round(random.gauss(0, 30), 1),
        avg_speed_kmh=round(random.uniform(45, 65), 1),
        max_speed_kmh=round(random.uniform(100, 130), 1),
        total_events=random.randint(3, 12),
        distance_km=round(random.uniform(30, 150), 1),
    )


# ============================================
# CAN Quality Endpoints
# ============================================

CAN_MESSAGE_NAMES = {
    0x100: "VehicleSpeed",
    0x101: "EngineData",
    0x102: "BrakeData",
    0x103: "SteeringData",
}

CAN_PERIODS = {
    0x100: 20,
    0x101: 10,
    0x102: 20,
    0x103: 50,
}


@api.get(
    "/quality",
    response_model=CANQualityOut,
    operation_id="getQuality"
)
async def get_quality(
    vehicle_id: str = Query(default="VH001"),
):
    """Get CAN communication quality metrics from DLT tables using Service Principal"""
    # Try to fetch from DLT
    if conf.unity.warehouse_id:
        quality = _fetch_quality_from_dlt(vehicle_id)
        if quality:
            return quality
    
    # Fallback to mock data
    now = datetime.now()
    window_start = now - timedelta(minutes=1)
    
    metrics = []
    total_health = 0.0
    
    for arb_id, name in CAN_MESSAGE_NAMES.items():
        period_ms = CAN_PERIODS[arb_id]
        expected_count = int(60000 / period_ms)
        
        missing_rate = random.uniform(0, 0.05)
        actual_count = int(expected_count * (1 - missing_rate))
        
        metrics.append(CANQualityMetric(
            arb_id=arb_id,
            message_name=name,
            channel="can0",
            message_count=actual_count,
            expected_count=expected_count,
            missing_rate=round(missing_rate, 4),
            period_ms=period_ms,
        ))
        
        total_health += (1 - missing_rate)
    
    overall_health = total_health / len(metrics) if metrics else 1.0
    
    return CANQualityOut(
        window_start=window_start,
        window_end=now,
        metrics=metrics,
        overall_health=round(overall_health, 4),
    )


# ============================================
# Video Streaming
# ============================================

def _fetch_video_metadata_from_dlt(
    vehicle_id: str,
    camera: Optional[CameraType] = None,
) -> list[VideoMetadataOut]:
    """Fetch video metadata from video_metadata table using Service Principal"""
    try:
        camera_filter = ""
        if camera:
            camera_filter = f"AND camera = '{camera.value}'"
        
        sql = f"""
        SELECT video_id, camera, vehicle_id, start_time, end_time, file_path, file_size_bytes
        FROM video_metadata
        WHERE vehicle_id = '{vehicle_id}' {camera_filter}
        ORDER BY start_time DESC
        """
        
        rows = rt.execute_sql(sql)
        videos = []
        
        for row in rows:
            start_ts = row.get("start_time")
            end_ts = row.get("end_time")
            
            if isinstance(start_ts, str):
                start_ts = datetime.fromisoformat(start_ts.replace("Z", ""))
            if isinstance(end_ts, str):
                end_ts = datetime.fromisoformat(end_ts.replace("Z", ""))
            
            camera_val = row.get("camera", "front")
            try:
                camera_type = CameraType(camera_val)
            except ValueError:
                camera_type = CameraType.FRONT
            
            videos.append(VideoMetadataOut(
                video_id=row.get("video_id") or "",
                camera=camera_type,
                vehicle_id=row.get("vehicle_id") or vehicle_id,
                start_time=start_ts or datetime.now(),
                end_time=end_ts or datetime.now(),
                file_path=row.get("file_path") or "",
                file_size_bytes=int(row.get("file_size_bytes") or 0) if row.get("file_size_bytes") else None,
            ))
        
        return videos
    except Exception as e:
        logger.warning(f"Failed to fetch video metadata: {e}")
        return []


def _generate_mock_video_metadata(vehicle_id: str) -> list[VideoMetadataOut]:
    """Generate mock video metadata"""
    now = datetime.now()
    videos = []
    for camera in CameraType:
        videos.append(VideoMetadataOut(
            video_id=f"vid_{vehicle_id}_{camera.value}",
            camera=camera,
            vehicle_id=vehicle_id,
            start_time=now - timedelta(minutes=10),
            end_time=now,
            file_path=f"/Volumes/{conf.unity.catalog}/{conf.unity.schema_name}/videos/{camera.value}_driving.mp4",
            file_size_bytes=383631,
        ))
    return videos


@api.get("/videos", response_model=VideoListOut, operation_id="getVideos")
async def get_videos(
    vehicle_id: str = Query(default="VH001"),
    camera: Optional[CameraType] = Query(default=None),
):
    """Get list of available videos with metadata using Service Principal"""
    if conf.unity.warehouse_id:
        videos = _fetch_video_metadata_from_dlt(vehicle_id, camera)
        if videos:
            return VideoListOut(videos=videos, total=len(videos))
    
    videos = _generate_mock_video_metadata(vehicle_id)
    if camera:
        videos = [v for v in videos if v.camera == camera]
    return VideoListOut(videos=videos, total=len(videos))


def _get_cached_video_path(camera: CameraType) -> str:
    """Get the local cache path for a video file"""
    import tempfile
    cache_dir = os.path.join(tempfile.gettempdir(), "yao_demo_vehicle_app_videos")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{camera.value}_driving.mp4")


def _download_video_to_cache(camera: CameraType) -> str:
    """Download video from Databricks Volume to local cache"""
    local_path = _get_cached_video_path(camera)
    
    # Check if already cached
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.info(f"Video already cached at: {local_path}")
        return local_path
    
    catalog = conf.unity.catalog
    schema = conf.unity.schema_name
    volume_path = f"/Volumes/{catalog}/{schema}/videos/{camera.value}_driving.mp4"
    
    logger.info(f"Downloading video from Volume: {volume_path} to {local_path}")
    
    try:
        import urllib.request
        import socket
        
        # Use direct REST API call with shorter timeout instead of SDK
        # The SDK has issues with Files API in Databricks Apps environment
        sp_ws = rt.ws
        host = sp_ws.config.host.rstrip('/')
        
        # Get auth headers from the workspace client
        auth_headers = sp_ws.config.authenticate()
        
        # Use Files API REST endpoint directly with shorter timeout
        url = f"{host}/api/2.0/fs/files{volume_path}"
        
        req = urllib.request.Request(url)
        for key, value in auth_headers.items():
            req.add_header(key, value)
        
        # Set timeout to 30 seconds
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30)
            with urllib.request.urlopen(req) as response:
                content_bytes = response.read()
        finally:
            socket.setdefaulttimeout(old_timeout)
        
        if not content_bytes:
            raise ValueError("Video file is empty")
        
        # Write to local cache
        with open(local_path, 'wb') as f:
            f.write(content_bytes)
        
        logger.info(f"Video downloaded successfully: {len(content_bytes)} bytes to {local_path}")
        return local_path
        
    except Exception as e:
        logger.error(f"Failed to download video: {type(e).__name__}: {e}", exc_info=True)
        # Clean up partial file if exists
        if os.path.exists(local_path):
            os.remove(local_path)
        raise


@api.get("/video/{camera}/stream", operation_id="streamVideo")
async def stream_video(
    camera: CameraType,
    vehicle_id: str = Query(default="VH001"),
):
    """Stream video from local cache (downloaded from Databricks Volumes)"""
    try:
        # Download to local cache if not already cached
        local_path = _download_video_to_cache(camera)
        
        if not os.path.exists(local_path):
            raise HTTPException(status_code=404, detail=f"Video file not found for camera '{camera.value}'")
        
        file_size = os.path.getsize(local_path)
        logger.info(f"Streaming video from cache: {local_path} ({file_size} bytes)")
        
        return FileResponse(
            path=local_path,
            media_type="video/mp4",
            filename=f"{camera.value}_driving.mp4",
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
            }
        )
    except HTTPException:
        raise
    except TimeoutError as e:
        logger.warning(f"Video download timed out for camera '{camera.value}': {e}")
        raise HTTPException(
            status_code=503, 
            detail=f"Video download timed out for camera '{camera.value}'. Please try again later."
        )
    except Exception as e:
        logger.error(f"Failed to stream video: {type(e).__name__}: {e}", exc_info=True)
        raise HTTPException(
            status_code=503, 
            detail=f"Video temporarily unavailable for camera '{camera.value}': {type(e).__name__}"
        )


@api.get("/video/{camera}/url", operation_id="getVideoUrl")
async def get_video_url(
    camera: CameraType,
    vehicle_id: str = Query(default="VH001"),
):
    """Get video streaming URL info"""
    catalog = conf.unity.catalog
    schema = conf.unity.schema_name
    volume_path = f"/Volumes/{catalog}/{schema}/videos/{camera.value}_driving.mp4"
    
    return {
        "camera": camera.value,
        "vehicle_id": vehicle_id,
        "stream_url": f"/api/video/{camera.value}/stream?vehicle_id={vehicle_id}",
        "file_path": volume_path,
    }
