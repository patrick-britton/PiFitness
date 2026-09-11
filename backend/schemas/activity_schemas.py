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


# ---------------------------------------------------------------------------
# Activity Report Schemas (009-001)
# Mirrors the frontend cross-surface contract in
# frontend/pifitness/src/lib/types/activity-report.ts.
# GET /api/activities/report?activity_type=Run|Walk
# ---------------------------------------------------------------------------


class ActivityReportHeader(BaseModel):
    """Metrics displayed in the report's summary header for a single activity."""
    start_utc: str = Field(..., description="UTC start timestamp (ISO 8601); the UI renders local time")
    distance_mi: float = Field(..., description="Activity distance in miles")
    distance_display: str = Field(..., description="Formatted distance from vw_activity_header_stats (e.g. '4.6 mi')")
    elevation_m_gain: Optional[float] = Field(None, description="Total elevation gain (m) from activities.activities")
    total_time_s: float = Field(
        ..., description="Total duration in seconds (per OQ-4: activity_time_s from vw_activity_summary)"
    )
    total_time_text: str = Field(..., description="Formatted total time as h:mm:ss.ms")
    total_time_display: str = Field(..., description="Duration display from vw_activity_header_stats (e.g. '46:45.7')")
    pace_text: str = Field(..., description="Formatted pace as m:ss.ms/mi")
    pace_display: str = Field(..., description="Pace display from vw_activity_header_stats (e.g. '10:09.9/mi')")
    hr_median: Optional[float] = Field(None, description="Median heart rate, when available")
    hr_max: Optional[float] = Field(None, description="Maximum heart rate, when available")
    show_efficiency_placeholder: bool = Field(
        ..., description="True for Run/Trail activities -> show the running-efficiency placeholder"
    )


class ActivityReportSegment(BaseModel):
    """A course or crossed segment with its comparison against prior/best efforts."""
    segment_id: int = Field(..., description="Segment identifier")
    name: str = Field(..., description="Segment (or course) name")
    is_course: bool = Field(..., description="True when this row is the course (is_course = true)")
    all_time_rank: Optional[int] = Field(None, description="Overall rank (fastest = 1), when defined")
    total_attempts: int = Field(..., description="Total attempts on this segment/course (the 'B' in 'A/B')")
    prior_delta_s: Optional[float] = Field(
        None, description="Seconds faster (negative) or slower (positive) than the prior attempt"
    )
    best_delta_s: Optional[float] = Field(
        None, description="Seconds faster (negative) or slower (positive) than the best-ever attempt"
    )


class ActivityElevationPoint(BaseModel):
    """A single per-minute elevation sample."""
    minute: int = Field(..., description="Elapsed minute (0-based) within the activity")
    elevation_m: float = Field(..., description="Average elevation for the minute (m)")


class ActivityHeartratePoint(BaseModel):
    """A single per-minute heart-rate sample."""
    minute: int = Field(..., description="Elapsed minute (0-based) within the activity")
    heartrate_bpm: Optional[float] = Field(None, description="Average heart rate for the minute (bpm)")


class ActivityPacePoint(BaseModel):
    """A single per-minute pace sample (numeric + formatted text)."""
    minute: int = Field(..., description="Elapsed minute (0-based) within the activity")
    pace_sec_per_mi: Optional[float] = Field(None, description="Numeric pace in seconds per mile (one decimal)")
    pace_text: Optional[str] = Field(None, description="Formatted pace as m:ss.ms")


class ActivityReportCharts(BaseModel):
    """Per-minute chart series served with the report."""
    elevation: List[ActivityElevationPoint] = Field(..., description="Per-minute elevation series")
    heartrate: List[ActivityHeartratePoint] = Field(..., description="Per-minute heart-rate series")
    pace: List[ActivityPacePoint] = Field(..., description="Per-minute pace series")


class ActivityReport(BaseModel):
    """Full report for a single activity."""
    activity_id: int = Field(..., description="The resolved activity id (footer caption)")
    activity_type: Literal['Run', 'Walk'] = Field(
        ..., description="Which activity-type selection produced this report"
    )
    header: ActivityReportHeader = Field(..., description="Summary header metrics")
    course: Optional[ActivityReportSegment] = Field(
        None, description="The matched course row, when the activity is a course effort; else null"
    )
    segments: List[ActivityReportSegment] = Field(
        ..., description="Crossed non-course segments (excludes the course row itself)"
    )
    has_segments: bool = Field(
        ..., description="True when the report has any course or segment rows (drives FR-5 nav)"
    )
    charts: ActivityReportCharts = Field(..., description="Per-minute chart series (elevation/heartrate/pace)")
    course_path: List[List[float]] = Field(..., description="Simplified course path as [lon, lat] pairs")