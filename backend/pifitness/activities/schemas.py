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
    """End-of-run summary embedded in the terminal NDJSON event."""
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


# ---------------------------------------------------------------------------
# Leaderboard Schemas (009-002)
# Cross-surface data contract matching frontend types (src/lib/types/leaderboards.ts).
# ---------------------------------------------------------------------------

SegmentKindFilter = Literal["course", "segment"]


class SegmentListQuery(BaseModel):
    """Query params for GET /api/activities/leaderboard/segments."""

    name: Optional[str] = Field(
        None, description="Case-insensitive substring match on segment_name; empty = any"
    )
    kind: Optional[SegmentKindFilter] = Field(
        None, description="Course/segment kind; None = both"
    )
    min_distance: float = Field(0, ge=0, description="Minimum distance in miles; 0 = any")
    activity_type: Optional[str] = Field(
        None,
        description="Activity-type pattern (Run, Trail Run, Hike, Walk, Bike, Ski); None = any",
    )


class SegmentListRow(BaseModel):
    """One row of the filterable segment/course list (vw_segments_effort_stats)."""

    segment_id: int = Field(..., description="Segment identifier")
    segment_name: str = Field(..., description="Segment (or course) name")
    is_course: bool = Field(..., description="True when this row is a course")
    activity_type_name: Optional[str] = Field(
        None, description="Activity type of the segment's matched activities"
    )
    distance_mi: float = Field(..., description="Segment distance in miles")
    elevation_gain: Optional[float] = Field(None, description="Elevation gain in meters")
    last_effort: Optional[str] = Field(
        None, description="Last effort start date (ISO 8601 date); None when never attempted"
    )
    matched_activity_count: int = Field(..., description="Count of matched activities (attempts)")


class LeaderboardEffort(BaseModel):
    """A single ranked effort on a segment, with fitness/preparedness context."""

    activity_id: int = Field(..., description="Effort activity id")
    activity_start_point: int = Field(..., description="Effort start point within the activity (m)")
    activity_end_point: int = Field(..., description="Effort end point within the activity (m)")
    rank: int = Field(
        ..., description="Active rank in the default range (all_time_rank); re-filtered client-side"
    )
    all_time_rank: Optional[int] = Field(None, description="All-time rank (fastest = 1)")
    last_365_rank: Optional[int] = Field(
        None,
        description="Last-365-day rank — window-scoped ORDERING value (never NULL; not a membership flag)",
    )
    current_cycle_rank: Optional[int] = Field(
        None,
        description="Current training-cycle rank — window-scoped ORDERING value (never NULL; not a membership flag)",
    )
    recency_rank: Optional[int] = Field(None, description="Recency rank (1 = most recent)")
    cycle_name: Optional[str] = Field(
        None,
        description=(
            "DB-computed recency bucket (009-009 OQ-4): 'Current Cycle' | 'Last 365' | "
            "'All Time'; the authoritative membership signal. 'Most Recent' is derived "
            "client-side from recency_rank"
        ),
    )
    start_time_utc: str = Field(..., description="Effort start timestamp (ISO 8601)")
    elapsed_duration_s: float = Field(..., description="Effort duration in seconds")
    gap_s: float = Field(
        ..., description="Seconds behind the fastest effort in the default (all-time) ranking"
    )
    pace_str: Optional[str] = Field(None, description="Formatted pace text (m:ss/mi)")
    avg_hr: Optional[float] = Field(None, description="Average heart rate (bpm)")
    max_hr: Optional[float] = Field(None, description="Maximum heart rate (bpm)")
    vo2_max_value: Optional[float] = Field(None, description="VO2 max at the time of the effort")
    resting_hr_asleep: Optional[float] = Field(None, description="Resting heart rate while asleep (bpm)")
    resting_hr_awake: Optional[float] = Field(None, description="Resting heart rate while awake (bpm)")
    training_load_acute: Optional[float] = Field(None, description="Acute training load")
    training_load_pct: Optional[float] = Field(None, description="Training load percent")
    weight_lb: Optional[float] = Field(None, description="Weight at the time of the effort (lb)")
    fat_pct: Optional[float] = Field(None, description="Body-fat percent")
    muscle_pct: Optional[float] = Field(None, description="Muscle percent")
    avg_cadence: Optional[float] = Field(None, description="Average cadence (spm)")
    avg_vert_osc: Optional[float] = Field(None, description="Average vertical oscillation (cm)")
    avg_vert_ratio: Optional[float] = Field(None, description="Average vertical ratio")
    avg_gct: Optional[float] = Field(None, description="Average ground contact time (ms)")
    avg_stride_length: Optional[float] = Field(None, description="Average stride length (cm)")
    avg_temp: Optional[float] = Field(None, description="Average air temperature (F)")
    altitude_acclimation_m: Optional[float] = Field(None, description="Altitude acclimation (m)")
    heat_acclimation_pct: Optional[float] = Field(None, description="Heat acclimation percent")
    sleep_hours: Optional[float] = Field(None, description="Sleep hours before the effort")
    sleep_score: Optional[float] = Field(None, description="Sleep score")
    awake_hours: Optional[float] = Field(None, description="Awake hours before the effort")
    avg_perf: Optional[float] = Field(None, description="Average performance condition")
    avg_balance: Optional[float] = Field(
        None, description="Average ground contact balance left percent"
    )


class Leaderboard(BaseModel):
    """A segment's full effort leaderboard (all rank columns; range filtering is client-side)."""

    segment_id: int = Field(..., description="The segment this leaderboard belongs to")
    segment_name: str = Field(..., description="Segment (or course) name")
    is_course: bool = Field(..., description="True when the target is a course")
    efforts: List[LeaderboardEffort] = Field(
        ..., description="Ranked efforts (dropped client-side when no rank in the active range)"
    )


class LeaderboardEffortSelection(BaseModel):
    """One selected effort for the visuals staging write."""

    activity_id: int = Field(..., description="Effort activity id")
    start_point: int = Field(..., description="Effort start point within the activity (m)")
    end_point: int = Field(..., description="Effort end point within the activity (m)")


class LeaderboardVisualsRequest(BaseModel):
    """Request body for POST /api/activities/leaderboard/visuals."""

    segment_id: int = Field(..., description="The segment the compared efforts belong to")
    efforts: List[LeaderboardEffortSelection] = Field(
        ..., min_length=1, description="Selected efforts (1..N)"
    )


class SegmentVisualSeriesPoint(BaseModel):
    """Numeric telemetry series samples for one compared effort (canonical axes)."""

    elapsed_s: float = Field(
        ..., description="Canonical elapsed time within the effort (s, aligned across efforts)"
    )
    distance_m: float = Field(..., description="Canonical distance within the effort (m)")
    elevation_m: Optional[float] = Field(None, description="Smoothed elevation (m)")
    heartrate_bpm: Optional[float] = Field(None, description="Heart rate (bpm)")
    cadence_spm: Optional[float] = Field(None, description="Cadence (spm)")
    speed_mmps: Optional[float] = Field(None, description="Speed (mm/s)")
    vert_ratio: Optional[float] = Field(None, description="Vertical ratio")
    vert_oscillation_cm: Optional[float] = Field(None, description="Vertical oscillation (cm)")
    air_temp_c: Optional[float] = Field(None, description="Air temperature (F)")
    stride_length_cm: Optional[float] = Field(None, description="Stride length (cm)")
    ground_contact_time_ms: Optional[float] = Field(None, description="Ground contact time (ms)")


class SegmentVisualEffort(BaseModel):
    """One compared effort's replay path + telemetry series."""

    id_val: str = Field(
        ..., description="Comparison label (activity date, suffixed for same-day repeats)"
    )
    meta_rank: int = Field(..., description="Comparison rank (0 = most recent effort)")
    path_coords: List[List[float]] = Field(
        ..., description="Replay path as [lon, lat] pairs ordered by elapsed time"
    )
    series: List[SegmentVisualSeriesPoint] = Field(
        ..., description="Downsampled telemetry series aligned to canonical elapsed time"
    )


class SegmentVisuals(BaseModel):
    """Generated visuals for the selected efforts (staging read via vw_activity_segments_aggregated)."""

    segment_id: int = Field(..., description="The segment the compared efforts belong to")
    segment_name: str = Field(..., description="Segment (or course) name")
    efforts: List[SegmentVisualEffort] = Field(..., description="One entry per selected effort")
