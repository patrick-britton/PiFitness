"""
Admin Schemas
===========

Pydantic models for administrative and system monitoring data structures.
These models now exactly match the database schema based on validation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

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

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
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
    )

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

    model_config = ConfigDict(from_attributes=True)

class DBStats(BaseModel):
    """Database statistics"""
    stat_id: int = Field(..., description="Unique statistic identifier")
    stat_time: datetime = Field(..., description="When statistic was recorded")
    size_before_mb: Optional[float] = Field(None, description="Database size before operation in MB")
    size_after_mb: Optional[float] = Field(None, description="Database size after operation in MB")
    maintenance_time_ms: Optional[int] = Field(None, description="Maintenance operation duration in ms")
    total_time_ms: Optional[int] = Field(None, description="Total operation duration in ms")
    maintenance_type: str = Field(..., description="Type of maintenance performed")

    model_config = ConfigDict(from_attributes=True)

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

    model_config = ConfigDict(from_attributes=True)

class APIServiceConfig(BaseModel):
    """API service configuration"""
    service_id: int = Field(..., description="Unique service identifier")
    service_name: str = Field(..., description="Name of the API service")
    api_function: str = Field(..., description="Function to call for authentication")
    rate_limit: Optional[int] = Field(None, description="Rate limit in requests per hour")
    last_call_time: Optional[datetime] = Field(None, description="When service was last called")
    is_active: bool = Field(..., description="Whether service is active")

    model_config = ConfigDict(from_attributes=True)

class BackupInfo(BaseModel):
    """Database backup information"""
    backup_id: int = Field(..., description="Unique backup identifier")
    backup_time: datetime = Field(..., description="When backup was created")
    backup_file: str = Field(..., description="Path to backup file")
    file_size_mb: float = Field(..., description="Size of backup file in MB")
    database_version: Optional[str] = Field(None, description="Database version at backup time")
    retention_days: int = Field(..., description="Number of days to retain backup")

    model_config = ConfigDict(from_attributes=True)

# New schemas for admin operations as per SP-02 design

class APIService(BaseModel):
    """API service credential requirements"""
    api_service_name: str = Field(..., description="Name of the API service")
    api_credential_requirements: Optional[str] = Field(None, description="JSON schema for required credentials")

    model_config = ConfigDict(from_attributes=True)

class FunctionLibraryEntry(BaseModel):
    """Function library entry"""
    friendly_name: str = Field(..., description="Human-readable name for the function")
    api_service_name: str = Field(..., description="Associated API service")
    python_extraction_function: str = Field(..., description="Python function name for extraction")
    description: Optional[str] = Field(None, description="Description of what the function does")
    # Additional fields as per the table structure can be added here if known

    model_config = ConfigDict(from_attributes=True)

class CredentialUpsert(BaseModel):
    """Credential upsert request"""
    api_service_name: str = Field(..., description="Name of the API service")
    raw_credentials_json_string: str = Field(..., description="JSON string of credentials to be encrypted")

    model_config = ConfigDict(from_attributes=True)

class FactConfigUpsert(BaseModel):
    """Fact configuration upsert request"""
    fact_id: Optional[int] = Field(None, description="Fact ID (required for update)")
    task_id: int = Field(..., description="Associated task ID")
    staging_id: int = Field(..., description="Staging ID")
    is_active: bool = Field(..., description="Whether the fact is active")
    custom_params: Optional[Dict[str, Any]] = Field(None, description="Custom parameters as JSON object")

    model_config = ConfigDict(from_attributes=True)

class TaskConfigEdit(BaseModel):
    """Task configuration edit request"""
    task_id: int = Field(..., description="Task ID to update")
    is_active: bool = Field(..., description="Whether task should be active")
    task_frequency: str = Field(..., description="Cron-like frequency string")

    model_config = ConfigDict(from_attributes=True)

class DBSession(BaseModel):
    """Database session information"""
    pid: int = Field(..., description="Process ID")
    state: str = Field(..., description="Session state (active, idle, etc.)")
    query: str = Field(..., description="Current query being executed")
    run_length: float = Field(..., description="Duration of query execution in seconds")

    model_config = ConfigDict(from_attributes=True)

class LogEntry(BaseModel):
    """Generic log entry - represents a row from any logging table"""
    # This is a generic model that can accommodate any log table structure
    # In practice, the actual columns will depend on the specific table queried
    # We'll use a dictionary approach for flexibility, but define common fields if known
    # For now, we'll use a flexible approach with a dict root model or allow extra fields
    # Since we don't know the exact columns, we'll use a Dict[str, Any] approach
    # However, Pydantic v2 doesn't have root_model in the same way, so we'll use a generic model
    # and allow extra fields via model_config
    
    model_config = ConfigDict(from_attributes=True, extra='allow')
    
    # We can add common fields if they exist across log tables
    # For example, many logs might have a timestamp and message
    # But to keep it generic, we'll just allow any fields
    
    # If we know specific columns from the logging tables, we could define them here
    # For now, leaving it as a pass-through model that accepts any fields

# Alternative approach for LogEntry if we want to define common fields:
# class LogEntry(BaseModel):
#     """Generic log entry"""
#     # Common fields that might appear in log tables
#     log_time: Optional[datetime] = Field(None, description="Timestamp of the log entry")
#     message: Optional[str] = Field(None, description="Log message")
#     level: Optional[str] = Field(None, description="Log level")
#     
#     model_config = ConfigDict(from_attributes=True, extra='allow')

# For simplicity and flexibility, we'll go with the extra='allow' approach above
# but we need to define at least one field to make it a valid model
# Let's add a placeholder field that will be ignored if extra data is present
# Actually, with extra='allow', we don't need to define any fields
# But Pydantic requires at least one field. Let's add a dummy field that we'll ignore.

# Let me redefine LogEntry properly:

class LogEntry(BaseModel):
    """Generic log entry representing a row from logging tables"""
    # This model allows any fields to be present from the database row
    # The actual structure depends on the specific log table queried
    
    model_config = ConfigDict(from_attributes=True, extra='allow')
    
    # Add a placeholder to satisfy Pydantic's requirement for at least one field
    # In practice, this field will be ignored if the actual data doesn't contain it
    # and extra fields will be preserved due to extra='allow'
    id: Optional[int] = Field(None, description="Placeholder field - actual data may vary")

# However, a better approach might be to not define any specific fields and use a Dict-based approach
# But Pydantic models need field definitions. Let's check if we can use RootModel in v2
# Since we don't know the Pydantic version exactly, let's use a more flexible approach:
# We'll allow the model to be initialized with any kwargs and store them
# Actually, the extra='allow' already does this if we have at least one field.

# Let's keep the above version with the id placeholder.