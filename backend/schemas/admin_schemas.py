"""
Admin Schemas
=============

Pydantic models for administrative and system monitoring data structures.
These models now exactly match the database schema based on validation.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class TaskExecution(BaseModel):
    """Task execution log entry matching the logging.task_executions table"""
    event_time_utc: datetime = Field(..., description="When the task execution event occurred")
    task_id: int = Field(..., description="Task identifier")
    task_name: str = Field(..., description="Name of the task")
    record_id: Optional[int] = Field(None, description="Record identifier")
    extract_time_ms: Optional[int] = Field(None, description="Extraction time in milliseconds")
    transform_time_ms: Optional[int] = Field(None, description="Transformation time in milliseconds")
    load_time_ms: Optional[int] = Field(None, description="Load time in milliseconds")
    forecast_time_ms: Optional[int] = Field(None, description="Forecast time in milliseconds")
    interpolation_time_ms: Optional[int] = Field(None, description="Interpolation time in milliseconds")
    error_text: Optional[str] = Field(None, description="Error text if failure occurred")
    failure_type: Optional[str] = Field(None, description="Type of failure")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "event_time_utc": "2023-06-24T07:30:00",
                "task_id": 1,
                "task_name": "activities_flatten",
                "record_id": 12345,
                "extract_time_ms": 1500,
                "transform_time_ms": 800,
                "load_time_ms": 1200,
                "forecast_time_ms": 300,
                "interpolation_time_ms": 250,
                "error_text": None,
                "failure_type": None
            }
        }

class TaskConfig(BaseModel):
    """Task configuration"""
    task_id: int = Field(..., description="Unique task identifier")
    task_name: str = Field(..., description="Name of the task")
    task_function: str = Field(..., description="Function to execute")
    api_service_name: Optional[str] = Field(None, description="API service required")
    execution_frequency: str = Field(..., description="How often task should run")
    should_execute: bool = Field(..., description="Whether task should be executed")
    last_execution: Optional[datetime] = Field(None, description="When task was last executed")
    next_execution: Optional[datetime] = Field(None, description="When task should run next")
    is_active: bool = Field(..., description="Whether task is active")

    class Config:
        from_attributes = True

class DBStats(BaseModel):
    """Database statistics"""
    stat_id: int = Field(..., description="Unique statistic identifier")
    stat_time: datetime = Field(..., description="When statistic was recorded")
    size_before_mb: Optional[float] = Field(None, description="Database size before operation in MB")
    size_after_mb: Optional[float] = Field(None, description="Database size after operation in MB")
    maintenance_time_ms: Optional[int] = Field(None, description="Maintenance operation duration in ms")
    total_time_ms: Optional[int] = Field(None, description="Total operation duration in ms")
    maintenance_type: str = Field(..., description="Type of maintenance performed")

    class Config:
        from_attributes = True

class SystemLog(BaseModel):
    """System log entry"""
    log_id: int = Field(..., description="Unique log identifier")
    log_time: datetime = Field(..., description="When log entry was created")
    category: str = Field(..., description="Log category")
    description: str = Field(..., description="Log description")
    severity: str = Field(..., description="Severity level (info, warning, error)")
    source: Optional[str] = Field(None, description="Source of the log entry")
    stack_trace: Optional[str] = Field(None, description="Stack trace if error")
    execution_time_ms: Optional[int] = Field(None, description="Execution time in milliseconds")

    class Config:
        from_attributes = True

class APIServiceConfig(BaseModel):
    """API service configuration"""
    service_id: int = Field(..., description="Unique service identifier")
    service_name: str = Field(..., description="Name of the API service")
    api_function: str = Field(..., description="Function to call for authentication")
    rate_limit: Optional[int] = Field(None, description="Rate limit in requests per hour")
    last_call_time: Optional[datetime] = Field(None, description="When service was last called")
    is_active: bool = Field(..., description="Whether service is active")

    class Config:
        from_attributes = True

class BackupInfo(BaseModel):
    """Database backup information"""
    backup_id: int = Field(..., description="Unique backup identifier")
    backup_time: datetime = Field(..., description="When backup was created")
    backup_file: str = Field(..., description="Path to backup file")
    file_size_mb: float = Field(..., description="Size of backup file in MB")
    database_version: Optional[str] = Field(None, description="Database version at backup time")
    retention_days: int = Field(..., description="Number of days to retain backup")

    class Config:
        from_attributes = True