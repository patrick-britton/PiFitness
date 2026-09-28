"""
Segments API Endpoints
======================

FastAPI endpoints for segment data (courses/routes).
Includes the Segment Management list endpoints (009-003, T02).
"""

from decimal import Decimal
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from backend_functions.database_functions import sql_to_dict
from backend.schemas.segment_schemas import (
    CreateSegmentRequest,
    CreateSegmentResponse,
    ConfirmMatchRequest,
    RejectMatchRequest,
    BulkConfirmRequest,
    HausdorffScoringRequest,
    FrechetScoringRequest,
    DeleteSegmentResponse,
    ResetMatchTablesRequest,
    ResetResponse,
    RenameSegmentRequest,
    RenameResponse,
)
from backend_functions.queries import (
    get_leaderboard_segments,
    get_segment_reference_windows,
    get_segment_activities,
    get_courses_page,
    get_activity_route,
    create_segment,
    get_match_candidates,
    run_find_matches,
    finalize_candidate_match,
    bulk_confirm_matches,
    run_extra_scoring,
    delete_segment,
    reset_match_tables,
    rename_segment,
)
from backend_functions.service_logins import mapbox_token

router = APIRouter(prefix="/api", tags=["segments"])


def _json_safe(value):
    """Coerce Decimal/date/datetime to JSON-safe primitives."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _safe_row(row: dict) -> dict:
    return {k: _json_safe(v) for k, v in row.items()}


def _with_reference_window(rows, refs, id_key="segment_id") -> list:
    """Add a segment's reference-geometry pointers to picker/course rows (009-003, T02b/OQ-3, T02c/OQ-4).

    Keeps the existing columns exactly as they are and appends the
    three reference scalars so the Match Activities / Course Review modes can
    request the segment's own GPS path. Missing refs (unknown id) become nulls.
    `id_key` selects the row's segment-id column ('segment_id' for the
    /segments/list picker, 'course_id' for the /segments/courses page).
    """
    data = []
    for row in rows:
        enriched = _safe_row(row)
        info = refs.get(int(row[id_key])) or {}
        enriched["activity_reference_id"] = _json_safe(info.get("activity_reference_id"))
        enriched["reference_start_point"] = _json_safe(info.get("reference_start_point"))
        enriched["reference_end_point"] = _json_safe(info.get("reference_end_point"))
        data.append(enriched)
    return data


# ---------------------------------------------------------------------------
# Segment Management list endpoints (009-003, T02)
# ---------------------------------------------------------------------------

VALID_SEGMENT_KINDS = ('course', 'segment')
VALID_SEGMENT_ACTIVITY_TYPES = ('Run', 'Trail Run', 'Hike', 'Walk', 'Bike', 'Ski')


@router.get("/segments/activities")
async def get_segment_activities_route(
    activity_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """
    Source-activity selection list for Create Segment mode (009-003, FR-1).

    One row per activity (newest first) with type, start time, distance,
    child course/segment counts, and matched counts, per the
    ActivitiesResponse contract.
    """
    if activity_type is not None and not activity_type.strip():
        activity_type = None
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=422, detail="limit must be 1..200")
    if offset < 0:
        raise HTTPException(status_code=422, detail="offset must be >= 0")
    try:
        rows = get_segment_activities(
            activity_type=activity_type,
            limit=limit,
            offset=offset,
        ) or []
        data = [_safe_row(r) for r in rows]
        return {"data": data, "count": len(data)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segment activities: {str(e)}",
        )


@router.get("/segments/list")
async def get_segment_list_route(
    name: Optional[str] = None,
    kind: Optional[str] = None,
    min_distance: float = 0.0,
    activity_type: Optional[str] = None,
):
    """
    Filterable segment/course picker list for Match Activities mode (009-003, FR-7).

    Thin handler over the 009-002 get_leaderboard_segments helper (same
    vw_segments_effort_stats view, same legacy activity-type semantics), shaped
    to the SegmentListResponse contract. Reuses the helper rather than
    duplicating SQL so filter behavior stays consistent across surfaces.

    The rows are then enriched (T02b/OQ-3) with the segment's reference-geometry
    pointers — activity_reference_id, reference_start_point,
    reference_end_point — from one primary-key read over activities.segments,
    because the modes render a segment's own GPS path from its reference
    activity + window (`GET /segments/{id}/route` is keyed by activity_id and
    cannot serve a segment_id). The 009-002 leaderboard list contract is
    untouched.
    """
    if kind is not None and kind not in VALID_SEGMENT_KINDS:
        raise HTTPException(
            status_code=422,
            detail=f"kind must be one of {list(VALID_SEGMENT_KINDS)} or omitted",
        )
    if activity_type is not None and activity_type not in VALID_SEGMENT_ACTIVITY_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"activity_type must be one of {list(VALID_SEGMENT_ACTIVITY_TYPES)} or omitted",
        )
    if min_distance < 0:
        raise HTTPException(status_code=422, detail="min_distance must be >= 0")
    try:
        rows = get_leaderboard_segments(
            name=name or None,
            kind=kind,
            min_distance=min_distance,
            activity_type=activity_type,
        ) or []
        refs = get_segment_reference_windows([r["segment_id"] for r in rows])
        data = _with_reference_window(rows, refs)
        return {"data": data, "count": len(data)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segment list: {str(e)}",
        )


@router.get("/segments/courses")
async def get_segment_courses_route(
    name: Optional[str] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    page: int = 1,
    page_size: int = 20,
):
    """
    Paginated course-review list (009-003, FR-15) matching the CoursesResponse
    contract (total/total_pages for pager, is_new flag for new-course counts).

    Rows are enriched (T02c/OQ-4) with the course's reference-geometry
    pointers — activity_reference_id, reference_start_point,
    reference_end_point — from one primary-key read over activities.segments,
    because the modes render a course's own GPS path from its reference
    activity + window (`GET /segments/{id}/route` is keyed by activity_id and
    cannot serve a course_id). Same one-PK-read pattern as T02b; pagination
    totals are untouched.
    """
    if page < 1:
        raise HTTPException(status_code=422, detail="page must be >= 1")
    if page_size < 1 or page_size > 100:
        raise HTTPException(status_code=422, detail="page_size must be 1..100")
    if min_distance is not None and min_distance < 0:
        raise HTTPException(status_code=422, detail="min_distance must be >= 0")
    if max_distance is not None and max_distance < 0:
        raise HTTPException(status_code=422, detail="max_distance must be >= 0")
    if (
        min_distance is not None
        and max_distance is not None
        and max_distance < min_distance
    ):
        raise HTTPException(
            status_code=422, detail="max_distance must be >= min_distance"
        )
    try:
        result = get_courses_page(
            name=name or None,
            min_distance=min_distance,
            max_distance=max_distance,
            page=page,
            page_size=page_size,
        )
        refs = get_segment_reference_windows([int(r["course_id"]) for r in result["data"]])
        result["data"] = _with_reference_window(result["data"], refs, id_key="course_id")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch courses: {str(e)}",
        )
# ---------------------------------------------------------------------------
# Map styles (009-003, T04/FR-3)
# ---------------------------------------------------------------------------

# Static basemap catalogue mirroring LeaderboardVisuals.buildStyleOptions:
# free OSM/Carto raster styles are always available; Mapbox styles (incl.
# the two satellite variants) require the Mapbox token; blank vector last.
_MAP_STYLES = [
    {"id": "positron", "name": "Positron", "requires_satellite_token": False},
    {"id": "osm", "name": "OpenStreetMap", "requires_satellite_token": False},
    {"id": "darkmatter", "name": "Dark Matter", "requires_satellite_token": False},
    {"id": "mb-basic", "name": "MB Basic", "requires_satellite_token": True},
    {"id": "mb-streets", "name": "MB Streets", "requires_satellite_token": True},
    {"id": "mb-outdoors", "name": "MB Outdoors", "requires_satellite_token": True},
    {"id": "mb-light", "name": "MB Light", "requires_satellite_token": True},
    {"id": "mb-dark", "name": "MB Dark", "requires_satellite_token": True},
    {"id": "mb-satellite", "name": "Satellite", "requires_satellite_token": True},
    {"id": "mb-sat-streets", "name": "Sat + Streets", "requires_satellite_token": True},
    {"id": "blank", "name": "Blank", "requires_satellite_token": False},
]


@router.get("/segments/map-styles")
async def get_segment_map_styles():
    """
    Available basemap styles for the Create Segment map selector (FR-3).

    Returns the static catalogue plus satellite_available (Mapbox token
    present). No DB call — static list + one credential lookup, matching
    the MapStylesResponse contract. Frontend hides token-gated styles when
    satellite_available is false.
    """
    try:
        token = mapbox_token()
    except Exception:
        token = None
    return {"data": _MAP_STYLES, "satellite_available": bool(token)}


@router.post("/segments/create", response_model=CreateSegmentResponse)
async def create_segment_route(request: CreateSegmentRequest):
    """
    Create a course/segment from an activity range (009-003, FR-5).

    Calls activities.segment_matching_segment_creation via the create_segment
    helper (same two-CALL flow as the legacy Streamlit creation path),
    then finalizes the source activity's own confirmed match. Returns the
    new segment_id per the CreateSegmentResponse contract. map_style is
    accepted for contract symmetry (frontend's active basemap) and ignored
    server-side. Validation errors -> 422; missing activity -> 404.
    """
    try:
        segment_id = create_segment(
            segment_name=request.name,
            activity_id=request.activity_id,
            start_m=int(request.start_m),
            end_m=int(request.end_m),
            is_course=request.is_course,
        )
        return {"segment_id": segment_id}
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)


@router.post("/segments/reset", response_model=ResetResponse)
async def reset_match_tables_route(request: ResetMatchTablesRequest):
    """
    Destructive reset of matching state (009-003, FR-14).

    Truncates segment_matches, segment_match_exclusions, distinct_segments and
    segments_details, then resets activities.segments (RESTART IDENTITY
    CASCADE). Declared before the /segments/{segment_id}/... routes: static
    path, no collision. Requires the deliberate-activation flag
    confirm_reset=true (OQ-2: the UI gates this behind a confirmation dialog).
    """
    if not request.confirm_reset:
        raise HTTPException(status_code=422, detail="confirm_reset must be true")
    try:
        return reset_match_tables(bool(request.confirm_reset))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Match reset failed: {str(e)}")


@router.get("/segments/{activity_id}/route")
async def get_activity_route_endpoint(
    activity_id: int,
    start_m: Optional[float] = None,
    end_m: Optional[float] = None,
):
    """
    Activity route + elevation for Create Segment mode (009-003, FR-2/FR-4).

    Returns path_coords ([lon, lat] pairs) and elevations (1:1 aligned)
    from activities.activity_details, plus the activity distance_m, matching
    the RouteResponse contract. Optional start_m/end_m trims the range
    (trim gates, FR-4). Declared before the legacy /segments/{segment_id}
    routes below; static /activities, /list, /courses stay above so no
    path collision.
    """
    if start_m is not None and start_m < 0:
        raise HTTPException(status_code=422, detail="start_m must be >= 0")
    if end_m is not None and end_m < 0:
        raise HTTPException(status_code=422, detail="end_m must be >= 0")
    if start_m is not None and end_m is not None and end_m < start_m:
        raise HTTPException(status_code=422, detail="end_m must be >= start_m")
    try:
        route = get_activity_route(activity_id, start_m=start_m, end_m=end_m)
        if route is None:
            raise HTTPException(status_code=404, detail="Activity not found")
        return {"data": route}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activity route: {str(e)}",
        )


@router.get("/segments")
async def get_all_segments():
    """
    Get all segments (courses/routes). RouteResponse contract lives at
    GET /segments/{activity_id}/route above (declared after the static
    /activities, /list, /courses routes so no path collision).

    Returns:
        List of all segment records
    """
    try:
        sql = "SELECT * FROM activities.segments ORDER BY segment_name"
        segments = sql_to_dict(sql)
        return {"data": segments, "count": len(segments)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segments: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Matching pipeline (009-003, T06/FR-9..FR-12)
# ---------------------------------------------------------------------------

@router.post("/segments/{segment_id}/matches/find")
async def find_matches_route(segment_id: int):
    """
    Trigger the find-matches pipeline (FR-9): match_activities ->
    pair_generation -> matches_all_polygon -> mass_confirmation(1).
    Synchronous blocking CALLs; the UI shows a spinner (OQ-1).
    Returns the refreshed candidate set (ScoringResponse shape).
    """
    try:
        return run_find_matches(int(segment_id))
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=500, detail=msg)


@router.get("/segments/{segment_id}/matches/candidates")
async def get_candidates_route(segment_id: int):
    """Candidate efforts with confidence/deviation metrics (FR-9, FR-10)."""
    try:
        seg = sql_to_dict(
            "SELECT segment_id FROM activities.segments WHERE segment_id = %s",
            (segment_id,),
        )
        if not seg:
            raise HTTPException(status_code=404, detail=f"segment_id={segment_id} not found")
        return get_match_candidates(int(segment_id))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch candidates: {str(e)}")


@router.post("/segments/{segment_id}/matches/confirm")
async def confirm_match_route(segment_id: int, request: ConfirmMatchRequest):
    """Confirm one candidate effort (FR-11) via finalize_match(TRUE, ...)."""
    try:
        return finalize_candidate_match(int(segment_id), int(request.activity_id), True)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg or "not a pending candidate" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)


@router.post("/segments/{segment_id}/matches/reject")
async def reject_match_route(segment_id: int, request: RejectMatchRequest):
    """Reject one candidate effort (FR-11) via finalize_match(FALSE, ...)."""
    try:
        return finalize_candidate_match(int(segment_id), int(request.activity_id), False)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg or "not a pending candidate" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)


@router.post("/segments/{segment_id}/matches/bulk-confirm")
async def bulk_confirm_route(segment_id: int, request: BulkConfirmRequest):
    """Mass-approve remaining candidates (FR-11); requires confirm_all=true."""
    try:
        return bulk_confirm_matches(int(segment_id), bool(request.confirm_all))
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)


@router.post("/segments/{segment_id}/scoring/hausdorff")
async def hausdorff_scoring_route(segment_id: int, request: HausdorffScoringRequest):
    """Run Hausdorff scoring over the candidate set (FR-12); requires run_scoring=true."""
    if not request.run_scoring:
        raise HTTPException(status_code=422, detail="run_scoring must be true")
    try:
        return run_extra_scoring(int(segment_id), "hausdorff")
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=500, detail=msg)


@router.post("/segments/{segment_id}/scoring/frechet")
async def frechet_scoring_route(segment_id: int, request: FrechetScoringRequest):
    """Run Freschet scoring over the candidate set (FR-12); requires run_scoring=true."""
    if not request.run_scoring:
        raise HTTPException(status_code=422, detail="run_scoring must be true")
    try:
        return run_extra_scoring(int(segment_id), "frechet")
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=500, detail=msg)


# ---------------------------------------------------------------------------
# Segment maintenance (009-003, T07/FR-13, FR-16)
# ---------------------------------------------------------------------------

@router.patch("/segments/{segment_id}/name", response_model=RenameResponse)
async def rename_segment_route(segment_id: int, request: RenameSegmentRequest):
    """
    Rename a course/segment (FR-16) from the course-review view.

    Single parameterized UPDATE (legacy update_course_name semantics).
    Blank name -> 422; unknown segment -> 404.
    """
    try:
        return rename_segment(int(segment_id), request.new_name)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)


@router.delete("/segments/{segment_id}", response_model=DeleteSegmentResponse)
async def delete_segment_route(segment_id: int):
    """
    Delete a segment together with its match rows and detail records (FR-13).

    Also clears the segment's exclusion and distinct-segment rows (NO ACTION
    FK would otherwise block the parent DELETE). Transactional; unknown
    segment -> 404.
    """
    try:
        return delete_segment(int(segment_id))
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=422, detail=msg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Segment delete failed: {str(e)}")


@router.get("/segments/{segment_id}/matches")
async def get_segment_matches_by_id(segment_id: int):
    """
    Get all activities that match a specific segment.

    Args:
        segment_id: The segment ID

    Returns:
        List of segment match records for this segment
    """
    try:
        sql = """
            SELECT
                sd.*,
                a.activity_name,
                a.start_timestamp_utc
            FROM activities.segments_details sd
            JOIN activities.activities a ON sd.activity_id = a.activity_id
            WHERE sd.segment_id = %s
            ORDER BY sd.start_time_utc
        """
        matches = sql_to_dict(sql, (segment_id,))
        return {"data": matches, "count": len(matches)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segment matches: {str(e)}",
        )