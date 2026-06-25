"""
Activity Schemas
================

Pydantic models for activity-related data structures.
These models now exactly match the database schema based on validation.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Activity(BaseModel):
    """Activity data matching the activities.activities table"""
    activity_id: int = Field(..., description="Unique activity identifier")
    activity_name: Optional[str] = Field(None, description="Name/title of the activity")
    activity_type_id: Optional[int] = Field(None, description="Activity type identifier")
    activity_type_name: Optional[str] = Field(None, description="Activity type name")
    distance_m: Optional[float] = Field(None, description="Distance in meters")
    duration_s: Optional[float] = Field(None, description="Duration in seconds")
    duration_elapsed_s: Optional[float] = Field(None, description="Elapsed duration in seconds")
    duration_moving_s: Optional[float] = Field(None, description="Moving duration in seconds")
    calories: Optional[int] = Field(None, description="Calories burned")
    calories_bmr: Optional[int] = Field(None, description="BMR calories")
    steps: Optional[int] = Field(None, description="Number of steps")
    stride_length_cm_avg: Optional[float] = Field(None, description="Average stride length in cm")
    cadence_double_spm_max: Optional[int] = Field(None, description="Maximum double cadence in SPM")
    cadence_running_spm_max: Optional[int] = Field(None, description="Maximum running cadence in SPM")
    cadence_running_spm_avg: Optional[int] = Field(None, description="Average running cadence in SPM")
    heartrate_avg: Optional[int] = Field(None, description="Average heart rate")
    heartrate_max: Optional[int] = Field(None, description="Maximum heart rate")
    speed_mps_avg: Optional[float] = Field(None, description="Average speed in m/s")
    speed_mps_max: Optional[float] = Field(None, description="Maximum speed in m/s")
    elevation_m_gain: Optional[float] = Field(None, description="Elevation gain in meters")
    elevation_m_loss: Optional[float] = Field(None, description="Elevation loss in meters")
    elevation_m_max: Optional[float] = Field(None, description="Maximum elevation in meters")
    elevation_m_min: Optional[float] = Field(None, description="Minimum elevation in meters")
    start_time_utc: Optional[datetime] = Field(None, description="Activity start time UTC")
    end_time_utc: Optional[datetime] = Field(None, description="Activity end time UTC")
    start_timestamp_utc: Optional[datetime] = Field(None, description="Activity start timestamp UTC")
    latitude_start: Optional[float] = Field(None, description="Starting latitude")
    longitude_start: Optional[float] = Field(None, description="Starting longitude")
    latitude_end: Optional[float] = Field(None, description="Ending latitude")
    longitude_end: Optional[float] = Field(None, description="Ending longitude")
    temperature_c_min: Optional[float] = Field(None, description="Minimum temperature in Celsius")
    temperature_c_max: Optional[float] = Field(None, description="Maximum temperature in Celsius")
    intensity_minutes_moderate: Optional[int] = Field(None, description="Moderate intensity minutes")
    intensity_minutes_vigorous: Optional[int] = Field(None, description="Vigorous intensity minutes")
    training_effect_aerobic: Optional[float] = Field(None, description="Aerobic training effect")
    training_effect_anerobic: Optional[float] = Field(None, description="Anaerobic training effect")
    training_load: Optional[float] = Field(None, description="Training load")
    water_ml_estimated: Optional[int] = Field(None, description="Estimated water intake in ml")
    is_downloaded: Optional[bool] = Field(None, description="Whether activity data is downloaded")
    activity_path: Optional[str] = Field(None, description="Activity path/route")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "activity_id": 12345,
                "activity_name": "Morning Run",
                "activity_type_id": 1,
                "activity_type_name": "running",
                "distance_m": 8500.5,
                "duration_s": 2700.0,
                "duration_elapsed_s": 2700.0,
                "duration_moving_s": 2650.0,
                "calories": 650,
                "calories_bmr": 450,
                "steps": 9250,
                "stride_length_cm_avg": 85.5,
                "cadence_double_spm_max": 180,
                "cadence_running_spm_max": 92,
                "cadence_running_spm_avg": 88,
                "heartrate_avg": 145,
                "heartrate_max": 172,
                "speed_mps_avg": 3.15,
                "speed_mps_max": 4.87,
                "elevation_m_gain": 125.3,
                "elevation_m_loss": 118.7,
                "elevation_m_max": 170.5,
                "elevation_m_min": 45.2,
                "start_time_utc": "2023-06-24T07:30:00",
                "end_time_utc": "2023-06-24T08:15:00",
                "start_timestamp_utc": "2023-06-24T07:30:00",
                "latitude_start": 37.7749,
                "longitude_start": -122.4194,
                "latitude_end": 37.8049,
                "longitude_end": -122.4794,
                "temperature_c_min": 15.5,
                "temperature_c_max": 18.2,
                "intensity_minutes_moderate": 45,
                "intensity_minutes_vigorous": 12,
                "training_effect_aerobic": 3.8,
                "training_effect_anerobic": 0.2,
                "training_load": 125.5,
                "water_ml_estimated": 750,
                "is_downloaded": True,
                "activity_path": "morning_run_path"
            }
        }

class ActivityDetail(BaseModel):
    """Detailed activity information with telemetry"""
    activity_id: int = Field(..., description="Unique activity identifier")
    detail_id: int = Field(..., description="Unique detail record identifier")
    timestamp: datetime = Field(..., description="Timestamp of the data point")
    latitude: Optional[float] = Field(None, description="Latitude coordinate")
    longitude: Optional[float] = Field(None, description="Longitude coordinate")
    heart_rate: Optional[int] = Field(None, description="Heart rate at timestamp")
    speed: Optional[float] = Field(None, description="Speed in m/s at timestamp")
    cadence: Optional[int] = Field(None, description="Cadence in RPM at timestamp")
    power: Optional[int] = Field(None, description="Power in watts at timestamp")
    elevation: Optional[float] = Field(None, description="Elevation in meters at timestamp")
    distance: Optional[float] = Field(None, description="Cumulative distance in meters")

    class Config:
        from_attributes = True

class ActivityMetric(BaseModel):
    """Activity metrics and statistics"""
    activity_id: int = Field(..., description="Unique activity identifier")
    metric_name: str = Field(..., description="Name of the metric")
    metric_value: float = Field(..., description="Value of the metric")
    metric_unit: str = Field(..., description="Unit of measurement")
    metric_category: Optional[str] = Field(None, description="Category of the metric")

    class Config:
        from_attributes = True

class Segment(BaseModel):
    """GPS segment information"""
    segment_id: int = Field(..., description="Unique segment identifier")
    segment_name: str = Field(..., description="Name of the segment")
    description: Optional[str] = Field(None, description="Description of the segment")
    distance_meters: float = Field(..., description="Distance of the segment in meters")
    elevation_gain: float = Field(..., description="Elevation gain in meters")
    elevation_loss: float = Field(..., description="Elevation loss in meters")
    avg_grade: float = Field(..., description="Average grade percentage")
    max_grade: float = Field(..., description="Maximum grade percentage")
    start_latitude: float = Field(..., description="Starting latitude")
    start_longitude: float = Field(..., description="Starting longitude")
    end_latitude: float = Field(..., description="Ending latitude")
    end_longitude: float = Field(..., description="Ending longitude")
    created_date: datetime = Field(..., description="Date segment was created")
    is_active: bool = Field(..., description="Whether segment is active")

    class Config:
        from_attributes = True

class SegmentMatch(BaseModel):
    """Segment match result"""
    match_id: int = Field(..., description="Unique match identifier")
    activity_id: int = Field(..., description="Activity identifier")
    segment_id: int = Field(..., description="Segment identifier")
    match_confidence: float = Field(..., description="Confidence score (0-1)")
    match_distance: float = Field(..., description="Matched distance in meters")
    match_time: float = Field(..., description="Time to complete matched portion in seconds")
    avg_speed: float = Field(..., description="Average speed on segment in m/s")
    max_speed: float = Field(..., description="Maximum speed on segment in m/s")
    avg_heart_rate: Optional[int] = Field(None, description="Average heart rate on segment")
    start_time: datetime = Field(..., description="Start time of the match")
    end_time: datetime = Field(..., description="End time of the match")
    is_confirmed: bool = Field(..., description="Whether match is confirmed by user")

    class Config:
        from_attributes = True