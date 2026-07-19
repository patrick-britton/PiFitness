"""
Activity Processing Schemas
===========================

Pydantic models for the Activity Processing & Playlist Shuffle feature.
Cross-surface data contract matching frontend types.
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, model_validator


class ProcessActivityRequest(BaseModel):
    """Request body for POST /api/activities/process"""
    playlist_name: Optional[str] = Field(
        None,
        description="Playlist name selection: 'Running', 'Jogging', 'No Playlist', or 'Manual Processing'",
    )
    manual_start_utc: Optional[str] = Field(
        None,
        description="ISO 8601 datetime — required when playlist_name is 'Manual Processing'",
    )
    manual_end_utc: Optional[str] = Field(
        None,
        description="ISO 8601 datetime — required when playlist_name is 'Manual Processing'",
    )

    @model_validator(mode='after')
    def validate_request(self):
        valid_playlists = ['Running', 'Jogging', 'No Playlist', 'Manual Processing']
        if self.playlist_name is not None and self.playlist_name not in valid_playlists:
            raise ValueError(f"playlist_name must be one of {valid_playlists}")
        if self.playlist_name == 'Manual Processing':
            if not self.manual_start_utc or not self.manual_end_utc:
                raise ValueError("manual_start_utc and manual_end_utc are required for Manual Processing")
        return self


class ProcessStepResultData(BaseModel):
    """Optional result data from a processing step"""
    song_count: Optional[int] = Field(None, description="Number of songs heard")
    first_song: Optional[str] = Field(None, description="First song heard")
    last_song: Optional[str] = Field(None, description="Last song heard")
    playlist_shuffled: Optional[bool] = Field(None, description="Whether playlist was successfully shuffled")
    playlist_id: Optional[str] = Field(None, description="The Spotify playlist ID")


class ProcessStepResult(BaseModel):
    """Result of a single processing step"""
    step_id: str = Field(..., description="Step identifier")
    status: str = Field(..., description="'complete', 'error', or 'skipped'")
    elapsed_ms: int = Field(..., description="Elapsed time in milliseconds")
    error: Optional[str] = Field(None, description="Error message if status is 'error'")
    result: Optional[ProcessStepResultData] = Field(None, description="Optional result data")


class ProcessActivityResponse(BaseModel):
    """Response body for POST /api/activities/process"""
    success: bool = Field(..., description="True if all steps completed successfully")
    steps: List[ProcessStepResult] = Field(..., description="Ordered array of step results")
    error: Optional[str] = Field(None, description="Overall error message if a step failed fatally")