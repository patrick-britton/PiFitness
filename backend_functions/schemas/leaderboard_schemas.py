"""
Leaderboard Schemas
===================

Pydantic models for the Leaderboards feature (009-002).
Cross-surface data contract matching frontend types (src/lib/types/leaderboards.ts).
"""

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


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