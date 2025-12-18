from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum
from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# ============================================
# Event Types
# ============================================

class EventType(str, Enum):
    HARD_BRAKE = "hard_brake"
    HARD_ACCELERATION = "hard_acceleration"
    SHARP_TURN = "sharp_turn"


# ============================================
# Vehicle Signals
# ============================================

class SignalData(BaseModel):
    """Real-time signal data point"""
    timestamp: datetime
    speed_kmh: Optional[float] = None
    rpm: Optional[float] = None
    throttle_pct: Optional[float] = None
    brake_pressure: Optional[float] = None
    brake_active: Optional[bool] = None
    steering_angle: Optional[float] = None


class SignalDataOut(SignalData):
    """Output model for signal data"""
    pass


class SignalTimeSeriesOut(BaseModel):
    """Time series of signal data"""
    signals: List[SignalDataOut]
    count: int


# ============================================
# Events
# ============================================

class EventIn(BaseModel):
    """Input model for creating an event"""
    event_type: EventType
    timestamp: datetime
    speed_kmh: Optional[float] = None
    acceleration: Optional[float] = None
    steering_angle: Optional[float] = None
    brake_pressure: Optional[float] = None
    vehicle_id: str = "VH001"


class EventOut(BaseModel):
    """Output model for event data"""
    id: str
    event_type: EventType
    timestamp: datetime
    speed_kmh: Optional[float] = None
    acceleration: Optional[float] = None
    steering_angle: Optional[float] = None
    brake_pressure: Optional[float] = None
    vehicle_id: str


class EventListOut(BaseModel):
    """List of events"""
    events: List[EventOut]
    total: int


# ============================================
# Vehicle Statistics
# ============================================

class VehicleStatsOut(BaseModel):
    """Vehicle statistics summary"""
    vehicle_id: str
    date: datetime
    avg_speed_kmh: float = 0.0
    max_speed_kmh: float = 0.0
    avg_rpm: float = 0.0
    max_rpm: float = 0.0
    distance_km: float = 0.0
    total_events: int = 0
    hard_brake_count: int = 0
    hard_acceleration_count: int = 0
    sharp_turn_count: int = 0
    driving_duration_minutes: float = 0.0


class VehicleStatsSummaryOut(BaseModel):
    """Summary stats for dashboard"""
    current_speed_kmh: float = 0.0
    current_rpm: float = 0.0
    current_throttle_pct: float = 0.0
    current_brake_pressure: float = 0.0
    current_steering_angle: float = 0.0
    avg_speed_kmh: float = 0.0
    max_speed_kmh: float = 0.0
    total_events: int = 0
    distance_km: float = 0.0


# ============================================
# CAN Quality
# ============================================

class CANQualityMetric(BaseModel):
    """Quality metric for a single CAN message"""
    arb_id: int
    message_name: str
    channel: str = "can0"
    message_count: int = 0
    expected_count: int = 0
    missing_rate: float = 0.0
    period_ms: int = 0


class CANQualityOut(BaseModel):
    """CAN communication quality metrics"""
    window_start: datetime
    window_end: datetime
    metrics: List[CANQualityMetric]
    overall_health: float = Field(
        default=1.0, 
        description="Overall health score (0-1)"
    )


# ============================================
# Time Range Filter
# ============================================

class TimeRange(str, Enum):
    LAST_10_MIN = "10m"
    LAST_1_HOUR = "1h"
    TODAY = "today"
    LAST_24_HOURS = "24h"
    LAST_7_DAYS = "7d"


# ============================================
# Video Models
# ============================================

class CameraType(str, Enum):
    FRONT = "front"
    REAR = "rear"
    LEFT = "left"
    RIGHT = "right"


class VideoMetadataOut(BaseModel):
    """Video metadata"""
    video_id: str
    camera: CameraType
    vehicle_id: str
    start_time: datetime
    end_time: datetime
    file_path: str
    file_size_bytes: Optional[int] = None


class VideoListOut(BaseModel):
    """List of video metadata"""
    videos: List[VideoMetadataOut]
    total: int
