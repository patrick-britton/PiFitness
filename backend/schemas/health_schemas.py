"""
Health Schemas
==============

Pydantic models for health-related data structures.
These models now exactly match the database schema based on validation.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class HeartRate(BaseModel):
    """Heart rate data matching the health.heartrate_raw table"""
    ts_utc: datetime = Field(..., description="Timestamp of the reading")
    heartrate_bpm: int = Field(..., description="Beats per minute")
    activity_label: Optional[str] = Field(None, description="Activity label")
    hr_date: Optional[datetime] = Field(None, description="Date of the reading")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "ts_utc": "2023-06-24T07:30:00",
                "heartrate_bpm": 72,
                "activity_label": "resting",
                "hr_date": "2023-06-24T00:00:00"
            }
        }

class SleepData(BaseModel):
    """Sleep data matching the health.sleep_totals table"""
    sleep_end_date: datetime = Field(..., description="Date the sleep period ended")
    sleep_start_utc: Optional[datetime] = Field(None, description="When sleep started")
    sleep_end_utc: Optional[datetime] = Field(None, description="When sleep ended")
    sleep_score: int = Field(..., description="Overall sleep quality score")
    heartrate_bpm: Optional[int] = Field(None, description="Average heart rate during sleep")
    spo2: Optional[float] = Field(None, description="Average SpO2 during sleep")
    breaths_per_min: Optional[float] = Field(None, description="Average breaths per minute during sleep")
    hrv_value: Optional[float] = Field(None, description="Average HRV during sleep")
    sleep_duration_s: Optional[int] = Field(None, description="Total sleep duration in seconds")
    rem_sleep_s: Optional[int] = Field(None, description="REM sleep duration in seconds")
    light_sleep_s: Optional[int] = Field(None, description="Light sleep duration in seconds")
    awake_sleep_s: Optional[int] = Field(None, description="Awake time during sleep period in seconds")
    deep_sleep_s: Optional[int] = Field(None, description="Deep sleep duration in seconds")
    score_label: Optional[str] = Field(None, description="Sleep score label")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "sleep_end_date": "2023-06-24T00:00:00",
                "sleep_start_utc": "2023-06-23T22:30:00",
                "sleep_end_utc": "2023-06-24T06:30:00",
                "sleep_score": 85,
                "heartrate_bpm": 58,
                "spo2": 96.5,
                "breaths_per_min": 14.2,
                "hrv_value": 45.8,
                "sleep_duration_s": 28800,
                "rem_sleep_s": 6300,
                "light_sleep_s": 12600,
                "awake_sleep_s": 1800,
                "deep_sleep_s": 9300,
                "score_label": "excellent"
            }
        }

class BodyComposition(BaseModel):
    """Body composition measurements"""
    composition_id: int = Field(..., description="Unique composition record identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    timestamp: datetime = Field(..., description="When the measurement was taken")
    weight_kg: Optional[float] = Field(None, description="Weight in kilograms")
    body_fat_percentage: Optional[float] = Field(None, description="Body fat percentage")
    muscle_mass_kg: Optional[float] = Field(None, description="Muscle mass in kilograms")
    bone_mass_kg: Optional[float] = Field(None, description="Bone mass in kilograms")
    water_percentage: Optional[float] = Field(None, description="Water percentage")
    bmi: Optional[float] = Field(None, description="Body Mass Index")
    visceral_fat: Optional[int] = Field(None, description="Visceral fat level")
    metabolic_age: Optional[int] = Field(None, description="Metabolic age")
    measurement_method: Optional[str] = Field(None, description="How the measurement was taken")

    class Config:
        from_attributes = True

class StressData(BaseModel):
    """Stress level data"""
    stress_id: int = Field(..., description="Unique stress record identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    timestamp: datetime = Field(..., description="When the stress level was recorded")
    stress_level: int = Field(..., description="Stress level (0-100)")
    stress_duration: Optional[float] = Field(None, description="Duration of stress period in seconds")
    stress_source: Optional[str] = Field(None, description="Identified source of stress")

    class Config:
        from_attributes = True

class HRVData(BaseModel):
    """Heart Rate Variability data"""
    hrv_id: int = Field(..., description="Unique HRV record identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    timestamp: datetime = Field(..., description="When the HRV was measured")
    hrv_value: float = Field(..., description="HRV value in milliseconds")
    measurement_duration: Optional[int] = Field(None, description="Duration of measurement in seconds")
    hrv_status: Optional[str] = Field(None, description="HRV status (low, normal, high)")

    class Config:
        from_attributes = True

class RespirationData(BaseModel):
    """Respiration rate data"""
    respiration_id: int = Field(..., description="Unique respiration record identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    timestamp: datetime = Field(..., description="When the respiration rate was recorded")
    breaths_per_minute: float = Field(..., description="Breaths per minute")
    measurement_method: Optional[str] = Field(None, description="How respiration was measured")

    class Config:
        from_attributes = True