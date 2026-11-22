"""
Segment Schemas
===============

Pydantic models for the Segment Management feature (009-003).
"""

from pydantic import BaseModel, Field
from typing import Optional, List


class ActivitiesQuery(BaseModel):
    activity_type: Optional[str] = None


class ActivitySummary(BaseModel):
    activity_id: int
    activity_type_name: str
    start_time_utc: str
    distance_m: float
    child_segment_count: int
    matched_count: int


class ActivitiesResponse(BaseModel):
    data: List[ActivitySummary]
    count: int


class RouteQuery(BaseModel):
    start_m: float
    end_m: float


class ActivityRoute(BaseModel):
    path_coords: List[List[float]]
    elevations: List[float]
    elapsed_s: List[int]
    distance_m: float


class RouteResponse(BaseModel):
    data: ActivityRoute


class CreateSegmentRequest(BaseModel):
    activity_id: int
    start_m: float
    end_m: float
    name: str
    is_course: bool
    map_style: str


class CreateSegmentResponse(BaseModel):
    segment_id: int


class SegmentListQuery(BaseModel):
    name: Optional[str] = None
    min_distance: Optional[float] = None
    kind: Optional[str] = None  # 'course' or 'segment'
    activity_type: Optional[str] = None


class SegmentListRow(BaseModel):
    segment_id: int
    segment_name: str
    is_course: bool
    activity_type_name: Optional[str]
    distance_mi: float
    elevation_gain: Optional[float]
    last_effort: Optional[str]
    matched_activity_count: int
    # Reference-geometry pointers (009-003 T02b/OQ-3): the segment's own GPS
    # path is fetched as getRoute(activity_reference_id, reference window).
    activity_reference_id: Optional[int] = None
    reference_start_point: Optional[int] = None
    reference_end_point: Optional[int] = None


class SegmentListResponse(BaseModel):
    data: List[SegmentListRow]
    count: int


class GeometryDeviation(BaseModel):
    distance_deviation_m: float
    polygon_deviation: float
    hausdorff_deviation: Optional[float]
    frechet_deviation: Optional[float]


class CandidateEffort(BaseModel):
    activity_id: int
    # Matched effort window inside the candidate activity (009-003 T06b/OQ-3).
    best_start_dist: int
    best_end_dist: int
    confidence: float
    deviation: GeometryDeviation
    start_time_utc: str
    distance_m: float
    elevation_gain: Optional[float]
    confidence_scaled: float
    distance_deviation_scaled: float
    polygon_deviation_scaled: float
    hausdorff_deviation_scaled: Optional[float]
    frechet_deviation_scaled: Optional[float]


class CandidatesResponse(BaseModel):
    data: List[CandidateEffort]
    count: int
    existing_match_count: int


class ConfirmMatchRequest(BaseModel):
    activity_id: int


class RejectMatchRequest(BaseModel):
    activity_id: int


class MatchOperationResponse(BaseModel):
    message: str
    segment: dict


class BulkConfirmRequest(BaseModel):
    confirm_all: bool


class BulkRejectRequest(BaseModel):
    confidence_over: Optional[float] = None


class BulkRejectResponse(BaseModel):
    rejected: int


class HausdorffScoringRequest(BaseModel):
    run_scoring: bool


class FrechetScoringRequest(BaseModel):
    run_scoring: bool


class ScoringResponse(BaseModel):
    message: str
    candidates: List[CandidateEffort]


class DeleteSegmentResponse(BaseModel):
    message: str
    segment_id: int


class ResetMatchTablesRequest(BaseModel):
    confirm_reset: bool


class ResetResponse(BaseModel):
    message: str
    truncated_count: int


class RenameSegmentRequest(BaseModel):
    new_name: str


class RenameResponse(BaseModel):
    segment_id: int
    segment_name: str


class CoursesQuery(BaseModel):
    name: Optional[str] = None
    min_distance: Optional[float] = None
    max_distance: Optional[float] = None
    page: Optional[int] = None
    page_size: Optional[int] = None


class CourseInfo(BaseModel):
    course_id: int
    course_name: str
    distance_mi: float
    elevation_gain: Optional[float]
    last_run: Optional[str]
    attempt_count: int
    is_new: bool
    # Reference-geometry pointers (009-003 T02c/OQ-4): the course's own GPS
    # path is fetched as getRoute(activity_reference_id, reference window).
    activity_reference_id: Optional[int] = None
    reference_start_point: Optional[int] = None
    reference_end_point: Optional[int] = None


class CoursesResponse(BaseModel):
    data: List[CourseInfo]
    total_count: int
    page: int
    page_size: int
    total_pages: int


class MapStyle(BaseModel):
    id: str
    name: str
    requires_satellite_token: bool


class MapStylesResponse(BaseModel):
    data: List[MapStyle]
    satellite_available: bool
