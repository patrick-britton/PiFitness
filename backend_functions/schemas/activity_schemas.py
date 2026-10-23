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
    distance_m: Optional[float] = Field(None, description="Target activity distance in metres (resolve_activity, 004-007 OQ-1)")
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
# Activity Report Schemas (009-001, reworked 004-006)
# Mirrors the frontend cross-surface contract in
# frontend/pifitness/src/lib/types/activity-report.ts.
# GET /api/activities/report?activity_id=<id>?
# Optional id; default = newest qualifying activity (run / trail run / walk /
# hike, >1000 m, non-null activity_path — 004-006 resolved OQ-1 B).
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
    activity_name: Optional[str] = Field(
        None, description="Activity name from activities.activities (title fallback when not a course)"
    )
    activity_type_name: str = Field(
        ..., description="Raw activity type name (e.g. 'running', 'trail_running', 'hiking', 'walking')"
    )
    distance_m: float = Field(
        ..., description="Total activity distance in METERS (exact; x-axis max for HR/elevation charts)"
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
    """A single chart sample; x-axis is cumulative distance in meters (004-006)."""
    distance_m: float = Field(..., description="Cumulative distance at the sample (meters)")
    elevation_m: float = Field(..., description="Average elevation for the sample (m)")


class ActivityHeartratePoint(BaseModel):
    """A single heart-rate chart sample; x-axis is cumulative distance in meters (004-006)."""
    distance_m: float = Field(..., description="Cumulative distance at the sample (meters)")
    heartrate_bpm: Optional[float] = Field(None, description="Average heart rate for the sample (bpm)")


class ActivityReportCharts(BaseModel):
    """Chart series served with the report (009-006, reworked 004-006)."""
    elevation: List[ActivityElevationPoint] = Field(..., description="Elevation series keyed by distance")
    heartrate: List[ActivityHeartratePoint] = Field(..., description="Heart-rate series keyed by distance")


class ActivityReportNav(BaseModel):
    """Prev/next navigation across the qualifying-activity set (004-006)."""
    prev_activity_id: Optional[int] = Field(
        None, description="Prior (older) qualifying activity id; null on the oldest"
    )
    next_activity_id: Optional[int] = Field(
        None, description="Next (newer) qualifying activity id; null on the newest"
    )


class ActivityEfficiencyMetric(BaseModel):
    """One running-efficiency metric: current attempt vs prior-10 course average."""
    current: Optional[float] = Field(None, description="Current attempt's value, when recorded")
    prior10_avg: Optional[float] = Field(
        None, description="Mean over the prior 10 attempts at the same course, when any exist"
    )


class ActivityReportEfficiency(BaseModel):
    """Bullet-chart data; present only for run/trail-run course efforts, else null (004-006)."""
    avg_cadence: ActivityEfficiencyMetric = Field(..., description="Avg cadence (spm)")
    avg_stride_length: ActivityEfficiencyMetric = Field(..., description="Avg stride length (cm)")
    avg_gct: ActivityEfficiencyMetric = Field(..., description="Avg ground contact time (ms)")
    avg_vert_ratio: ActivityEfficiencyMetric = Field(..., description="Avg vertical ratio (%)")


class ActivityReport(BaseModel):
    """Full report for a single activity."""
    activity_id: int = Field(..., description="The resolved activity id (footer caption)")
    nav: ActivityReportNav = Field(
        ..., description="Prev/next navigation across the qualifying-activity set (004-006)"
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
    efficiency: Optional[ActivityReportEfficiency] = Field(
        None,
        description="Bullet data for run/trail-run course efforts; null otherwise (004-006)",
    )
    charts: ActivityReportCharts = Field(
        ..., description="Chart series (elevation/heartrate) keyed by cumulative distance"
    )
    course_path: List[List[float]] = Field(..., description="Simplified course path as [lon, lat] pairs")