"""
Activity Processing Schemas
===========================

Pydantic models for the Activity Processing & Playlist Shuffle feature.
Cross-surface data contract matching frontend types.
"""

from datetime import datetime, timezone
from typing import Optional, List, Literal
from pydantic import BaseModel, Field, model_validator


class ProcessActivityRequest(BaseModel):
    """Request body for POST /api/activities/process.

    Mirrors the frontend contract: mode selects the most recent Walk or Run;
    music (one of 'running' | 'jogging' | 'no_music') is required iff last_run.
    """
    mode: Literal['last_walk', 'last_run'] = Field(
        ...,
        description="Selection mode: 'last_walk' or 'last_run'",
    )
    music: Optional[Literal['running', 'jogging', 'no_music']] = Field(
        None,
        description="Music option. Required when mode == 'last_run'; omitted for 'last_walk'",
    )

    @model_validator(mode='after')
    def validate_mode_music(self):
        if self.mode == 'last_run' and self.music is None:
            raise ValueError("music is required when mode == 'last_run'")
        return self


class ProcessStepResultData(BaseModel):
    """Optional result data from a processing step"""
    song_count: Optional[int] = Field(None, description="Number of songs heard")
    songs_sent: Optional[int] = Field(None, description="Number of songs sent to Spotify")
    first_song: Optional[str] = Field(None, description="First song heard")
    last_song: Optional[str] = Field(None, description="Last song heard")
    playlist_shuffled: Optional[bool] = Field(None, description="Whether playlist was successfully shuffled")
    playlist_id: Optional[str] = Field(None, description="The Spotify playlist ID")


class ProcessStepStartEvent(BaseModel):
    """Streaming event emitted when a step begins executing.

    Each step emits one of these (status 'running') before its terminal event.
    """
    step_id: str = Field(..., description="Step identifier")
    status: Literal['running'] = Field(
        default='running', description="Always 'running' for a start event"
    )
    started_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="ISO 8601 timestamp when the step started",
    )


class ProcessSummaryData(BaseModel):
    """End-of-run summary embedded in the terminal NDJSON event (FR-14)."""
    total_elapsed_ms: Optional[int] = Field(
        None,
        description="Total execution time of the whole run in milliseconds (FR-14)",
    )
    playlist_shuffled: Optional[bool] = Field(
        None,
        description="Whether the playlist was shuffled (null when playlist steps were skipped)",
    )
    segments_matched: Optional[int] = Field(
        None,
        description="Number of segment matches recorded for the processed activity (null when segment steps were skipped)",
    )
    courses_matched: Optional[int] = Field(
        None,
        description="Count of matched courses (null when segment steps were skipped) (FR-14)",
    )
    course_found: Optional[bool] = Field(
        None,
        description="Whether any matched segment is a course (null when segment steps were skipped)",
    )
    course_name: Optional[str] = Field(
        None,
        description="Name of the matched course, when course_found is true",
    )
    activity_id: Optional[int] = Field(
        None,
        description="Activity the summary refers to (the resolved activity id)",
    )


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