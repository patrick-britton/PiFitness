"""
Activities Query Functions
===========================

Database query functions for activity data (GPS tracks, segments, metrics).
These functions return plain Python data structures with no Streamlit dependencies.
"""

import json
from typing import List, Dict, Any, Optional, Union, Sequence
from datetime import date
from psycopg2.extras import execute_values
from backend_functions.database_functions import qec, sql_to_dict, get_conn, one_sql_result
from backend_functions.db_schema import get_columns


def get_activities_list(
    limit: int = 50,
    offset: int = 0,
    activity_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Sequence[Dict[str, Any]]:
    """
    Retrieve a list of activities with optional filtering.

    Args:
        limit: Maximum number of activities to return
        offset: Pagination offset
        activity_type: Filter by activity type (e.g., 'run', 'ride')
        start_date: Filter by start date (inclusive)
        end_date: Filter by end date (inclusive)

    Returns:
        Sequence[Dict[str, Any]]: List of activity records
    """
    # Get available columns to ensure query works across environments
    available_cols = get_columns('activities', 'activities')
    safe_cols = []
    for col in ['activity_id', 'activity_name', 'start_timestamp_utc',
                'duration_moving_s', 'distance_m', 'elevation_gain_m',
                'avg_heart_rate_bpm', 'max_heart_rate_bpm']:
        if col in available_cols:
            safe_cols.append(col)

    sql = f"""
        SELECT {', '.join(safe_cols)}
        FROM activities.activities
    """
    params = []
    conditions = []

    if activity_type:
        conditions.append("activity_type = %s")
        params.append(activity_type)

    if start_date:
        conditions.append("start_timestamp_utc >= %s")
        params.append(start_date)

    if end_date:
        conditions.append("start_timestamp_utc <= %s")
        params.append(end_date)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY start_timestamp_utc DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    result = sql_to_dict(sql, params)
    return result if result else []


def get_activity_by_id(activity_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single activity by ID.

    Args:
        activity_id: The activity ID

    Returns:
        Optional[Dict[str, Any]]: Activity record or None if not found
    """
    sql = """
        SELECT * FROM activities.activities
        WHERE activity_id = %s
    """
    result = sql_to_dict(sql, (activity_id,))
    return result[0] if result else None


def get_activity_telemetry(activity_id: int) -> Sequence[Dict[str, Any]]:
    """
    Retrieve telemetry data (GPS, heart rate, cadence) for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        Sequence[Dict[str, Any]]: List of telemetry data points
    """
    sql = """
        SELECT
            ts_utc AS timestamp_utc,
            latitude,
            longitude,
            elevation_m,
            heartrate_bpm,
            cadence_spm,
            speed_mmps / 1000.0 AS speed_mps,
            distance_mm / 1000.0 AS distance_m
        FROM activities.activity_details
        WHERE activity_id = %s
        ORDER BY ts_utc
    """
    result = sql_to_dict(sql, (activity_id,))
    return result if result else []


def get_segment_matches(activity_id: int) -> Sequence[Dict[str, Any]]:
    """
    Retrieve segment matches for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        Sequence[Dict[str, Any]]: List of segment match records
    """
    sql = """
        SELECT
            sd.segment_id,
            s.segment_name,
            sd.distance_m,
            sd.elapsed_duration_s AS duration_s,
            sd.max_hr AS max_heart_rate_bpm,
            sd.avg_hr AS avg_heart_rate_bpm,
            sd.start_time_utc,
            sd.avg_cadence,
            sd.avg_temp
        FROM activities.segments_details sd
        JOIN activities.segments s ON sd.segment_id = s.segment_id
        WHERE sd.activity_id = %s
        ORDER BY sd.start_time_utc
    """
    result = sql_to_dict(sql, (activity_id,))
    return result if result else []


def get_recent_activities(limit: int = 10) -> Sequence[Dict[str, Any]]:
    """
    Retrieve the most recent activities.

    Args:
        limit: Maximum number of activities to return

    Returns:
        Sequence[Dict[str, Any]]: List of recent activity records
    """
    sql = """
        SELECT
            activity_id,
            activity_name,
            activity_type,
            start_timestamp_utc,
            duration_moving_s,
            distance_m,
            elevation_gain_m
        FROM activities.activities
        ORDER BY start_timestamp_utc DESC
        LIMIT %s
    """
    result = sql_to_dict(sql, (limit,))
    return result if result else []


def get_activity_stats(activity_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve aggregated statistics for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        Optional[Dict[str, Any]]: Activity statistics or None if not found
    """
    sql = """
        SELECT
            activity_id,
            COUNT(*) AS telemetry_points,
            MIN(elevation_m) AS min_elevation_m,
            MAX(elevation_m) AS max_elevation_m,
            AVG(heartrate_bpm) AS avg_heart_rate_bpm,
            MAX(heartrate_bpm) AS max_heart_rate_bpm,
            AVG(speed_mmps / 1000.0) * 3.6 AS avg_speed_kph,
            MAX(speed_mmps / 1000.0) * 3.6 AS max_speed_kph
        FROM activities.activity_details
        WHERE activity_id = %s
        GROUP BY activity_id
    """
    result = sql_to_dict(sql, (activity_id,))
    return result[0] if result else None


def resolve_latest_activity_id(activity_type: str) -> Optional[int]:
    """
    Resolve the most recent activity_id for a run/walk activity type (009-001, FR-2).

    Uses the same activities.vw_last_activity_id_by_type view as Activity Processing.
    The view bakes in most-recent selection (max activity_id) and distance constraints.

    Args:
        activity_type: 'Run' or 'Walk' (the view's activity_type key)

    Returns:
        Optional[int]: The resolved activity_id, or None if the view has no row.
    """
    sql = """
        SELECT activity_id
        FROM activities.vw_last_activity_id_by_type
        WHERE activity_type = %s
    """
    row = one_sql_result(sql, (activity_type,))
    return int(row) if row is not None else None


def get_activity_report_header(activity_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve the report summary header values for an activity (009-001 FR-4 / 009-006 OQ-1).

    Sourced from the canonical activities.activities row (start_time_utc,
    distance_m, duration_s, elevation_m_gain) per the human's OQ-1 direction,
    not from the charting/aggregate view. HR median/75th/max are computed
    separately via get_activity_percentile_hr (kept for accuracy, OQ-4).

    Args:
        activity_id: The activity ID

    Returns:
        Optional[Dict[str, Any]]: Header values or None if the activity has no row.
    """
    sql = """
        SELECT
            start_time_utc AS start_utc,
            m2mi(distance_m) AS distance_mi,
            duration_s AS total_time_s,
            elevation_m_gain
        FROM activities.activities
        WHERE activity_id = %s
    """
    result = sql_to_dict(sql, (activity_id,))
    return result[0] if result else None


def get_activity_report_header_view_by_type(activity_type_basic: str) -> Optional[Dict[str, Any]]:
    """
    Resolve the report target + header in ONE query (009-007 B2).

    Queries activities.vw_activity_header_stats for the single row matching
    activity_type_basic ('run' | 'walk'). By construction the view holds one
    row per type, always with a non-null activity_path — the returned
    activity_id is the canonical target for all downstream report queries
    (charts, course path, efforts). Returns display strings (distance,
    duration, pace), HR median/max, start_time_utc, plus elevation_m_gain
    and numeric distance/time joined from activities.activities (the view
    does not carry elevation gain). Returns None when the view has no row
    for the type (→ endpoint 404, no fallback per human direction).
    """
    sql = """
        SELECT
            v.activity_id,
            v.activity_type_basic,
            v.distance_mi AS distance_display,
            v.duration_display,
            v.pace_display,
            v.start_time_utc AS start_utc,
            v.hr_median,
            v.hr_maximum,
            a.elevation_m_gain,
            m2mi(a.distance_m) AS distance_mi,
            a.duration_elapsed_s,
            a.duration_s
        FROM activities.vw_activity_header_stats v
        LEFT JOIN activities.activities a ON a.activity_id = v.activity_id
        WHERE v.activity_type_basic = %s
    """
    result = sql_to_dict(sql, (activity_type_basic,))
    return result[0] if result else None


def get_activity_report_header_view(activity_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve the report header from activities.vw_activity_header_stats (009-007).

    Returns display strings (distance_mi, duration_display, pace_display),
    HR median/max, start_time_utc, and elevation_m_gain joined from
    activities.activities (the view does not carry elevation gain).
    Returns None when the activity has no row in the view.
    """
    sql = """
        SELECT
            v.activity_id,
            v.activity_type_basic,
            v.distance_mi AS distance_display,
            v.duration_display,
            v.pace_display,
            v.start_time_utc AS start_utc,
            v.hr_median,
            v.hr_maximum,
            a.elevation_m_gain,
            m2mi(a.distance_m) AS distance_mi,
            a.duration_elapsed_s,
            a.duration_s
        FROM activities.vw_activity_header_stats v
        LEFT JOIN activities.activities a ON a.activity_id = v.activity_id
        WHERE v.activity_id = %s
    """
    result = sql_to_dict(sql, (activity_id,))
    return result[0] if result else None


def get_activity_percentile_hr(activity_id: int, frac: float) -> Optional[float]:
    """
    Compute a heart-rate percentile over an activity's telemetry (009-001, FR-4).

    Args:
        activity_id: The activity ID
        frac: The percentile fraction (0.5 for median, 0.75 for 75th).

    Returns:
        Optional[float]: The percentile HR value, or None if no HR data.
    """
    sql = """
        SELECT percentile_cont(%s) WITHIN GROUP (ORDER BY heartrate_bpm)
        FROM activities.activity_details
        WHERE activity_id = %s AND heartrate_bpm IS NOT NULL
    """
    row = one_sql_result(sql, (frac, activity_id))
    return float(row) if row is not None else None


_PACE_SEC_PER_MI_MM = 1609344.0  # millimeters in a mile (speed is mm/s)


def _format_pace_mmss(sec_per_mi: Optional[float]) -> Optional[str]:
    """Format seconds-per-mile as m:ss.ms (one decimal); minutes may exceed 60.

    The minutes count continues past 60 rather than converting to hours, per the
    report's pace formatting rule (009-006). Returns None for an undefined pace.
    """
    if sec_per_mi is None:
        return None
    secs = round(float(sec_per_mi), 1)
    mins = int(secs) // 60
    rem = secs - mins * 60
    return f"{mins}:{rem:04.1f}"


def get_activity_report_charts(activity_id: int) -> List[Dict[str, Any]]:
    """
    Per-minute elevation/heartrate/pace series for an activity (009-006).

    Uses the provided per-minute aggregate over activities.activity_details
    (minute-of-activity buckets) and adds a computed numeric pace (seconds per
    mile) plus formatted pace text (m:ss.ms) for the frontend charts.

    Args:
        activity_id: The activity ID

    Returns:
        List[Dict[str, Any]]: One row per elapsed minute with keys
        minute, elevation_m, heartrate_bpm, speed_mps, pace_sec_per_mi, pace_text.
    """
    sql = """
        WITH pre_aggs AS (
            SELECT activity_id,
                   floor(elapsed_duration_s / 60) AS minute_of_activity,
                   round(avg(heartrate_bpm), 1)   AS heartrate_bpm,
                   round(avg(elevation_m), 0)     AS elevation_m,
                   avg(speed_mmps::NUMERIC(25,3)) AS speed_mmps
            FROM activities.activity_details
            WHERE activity_id = %s AND elapsed_duration_s IS NOT NULL
            GROUP BY activity_id, minute_of_activity
        )
        SELECT
            minute_of_activity,
            heartrate_bpm,
            elevation_m,
            round(speed_mmps / 1000.0, 1) AS speed_mps,
            CASE
                WHEN speed_mmps = 0 OR speed_mmps IS NULL THEN NULL
                ELSE round(%s / speed_mmps, 1)
            END AS pace_sec_per_mi
        FROM pre_aggs
        ORDER BY minute_of_activity ASC
    """
    rows = sql_to_dict(sql, (activity_id, _PACE_SEC_PER_MI_MM))
    return [
        {
            "minute": int(r["minute_of_activity"]),
            "elevation_m": r["elevation_m"],
            "heartrate_bpm": r["heartrate_bpm"],
            "speed_mps": r["speed_mps"],
            "pace_sec_per_mi": r["pace_sec_per_mi"],
            "pace_text": _format_pace_mmss(r["pace_sec_per_mi"]),
        }
        for r in rows
    ]


def get_activity_course_path(activity_id: int) -> List[List[float]]:
    """
    Return the activity's course path as a list of [lon, lat] pairs (009-006, OQ-2/A).

    Reads the single activity_path linestring as its own lightweight single-row
    query (per OQ-3, never fanned out with the per-minute aggregate), simplifies
    it for rendering, and returns coordinate pairs. Returns [] when no path exists.

    Args:
        activity_id: The activity ID

    Returns:
        List[List[float]]: [[lon, lat], ...] or [] when no stored path.
    """
    sql = """
        SELECT ST_AsGeoJSON(ST_SimplifyVW(activity_path::geometry, %s)) AS path_geojson
        FROM activities.activities
        WHERE activity_id = %s AND activity_path IS NOT NULL
    """
    row = one_sql_result(sql, (0.0001, activity_id))
    if not row:
        return []
    try:
        geojson = json.loads(row)
    except (TypeError, ValueError):
        return []
    coords = (geojson or {}).get("coordinates") or []
    return [[float(c[0]), float(c[1])] for c in coords]


def get_activity_report_efforts(activity_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve course + crossed-segment comparison rows for the report (009-001, FR-3/FR-4).

    Returns the activity's segment rows from vw_activity_summary (name, is_course,
    all_time_rank, best_delta_s, prior_delta_s) plus a per-segment total_attempts
    denominator (count(*) over vw_segment_leaderboard for the segment) per OQ-3.
    Course detail is sourced from the leaderboard view via is_course = true (OQ-4).

    Args:
        activity_id: The activity ID

    Returns:
        List[Dict[str, Any]]: Effort rows ordered courses first, then start time.
    """
    sql = """
        SELECT
            s.segment_id,
            s.segment_name AS name,
            s.is_course,
            s.all_time_rank,
            s.best_delta_s,
            s.prior_delta_s,
            lb.total_attempts
        FROM activities.vw_activity_summary s
        LEFT JOIN (
            SELECT segment_id, count(*) AS total_attempts
            FROM activities.vw_segment_leaderboard
            GROUP BY segment_id
        ) lb ON lb.segment_id = s.segment_id
        WHERE s.activity_id = %s
        ORDER BY s.is_course DESC, s.activity_start_utc
    """
    result = sql_to_dict(sql, (activity_id,))
    return result if result else []


# Legacy activity-type filter semantics (FR-2): Run matches '%run%' but excludes
# trail-run types; every other type matches by its type-name pattern.
_ACTIVITY_TYPE_PATTERNS = {
    'Run': "activity_type_name ILIKE %s AND activity_type_name NOT ILIKE %s",
    'Trail Run': "activity_type_name ILIKE %s",
    'Hike': "activity_type_name ILIKE %s",
    'Walk': "activity_type_name ILIKE %s",
    'Bike': "activity_type_name ILIKE %s",
    'Ski': "activity_type_name ILIKE %s",
}

_ACTIVITY_TYPE_PARAMS = {
    'Run': ('%run%', '%trail%'),
    'Trail Run': ('%trail%run%',),
    'Hike': ('%hik%',),
    'Walk': ('%walk%',),
    'Bike': ('%bik%',),
    'Ski': ('%ski%',),
}


def get_leaderboard_segments(
    name: Optional[str] = None,
    kind: Optional[str] = None,
    min_distance: float = 0.0,
    activity_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve the filterable segment/course list for the Leaderboards page (009-002, FR-1/FR-2).

    Reads `activities.vw_segments_effort_stats` (per-segment; OQ-1) and applies:
    - name: case-insensitive substring on segment_name (FR-2)
    - kind: 'course' -> is_course, 'segment' -> NOT is_course, None -> both (FR-2)
    - min_distance: distance_mi >= min_distance (FR-2)
    - activity_type: legacy type-name patterns; Run excludes trail types (FR-2)

    All filters are server-side (AC-1); SQL is parameterized (no string interpolation).

    Args:
        name: Substring filter; None/empty = any.
        kind: 'course' | 'segment' | None.
        min_distance: Minimum distance in miles; 0 = any.
        activity_type: One of _ACTIVITY_TYPE_PATTERNS keys; None = any.

    Returns:
        List[Dict[str, Any]]: Segment rows ordered courses first, then last effort.
    """
    sql = """
        SELECT
            segment_id,
            segment_name,
            is_course,
            activity_type_name,
            distance_mi,
            elevation_gain,
            last_effort,
            matched_activity_count
        FROM activities.vw_segments_effort_stats
        WHERE (%s::text IS NULL OR segment_name ILIKE %s)
          AND (%s::text IS NULL OR (
                CASE WHEN %s::text = 'course' THEN is_course
                     WHEN %s::text = 'segment' THEN NOT is_course
                     ELSE TRUE END))
          AND distance_mi >= %s
    """
    params: List[Any] = [name, f"%{name}%" if name else None, kind, kind, kind, min_distance]

    if (activity_type or '') in _ACTIVITY_TYPE_PATTERNS:
        sql += f" AND ({_ACTIVITY_TYPE_PATTERNS[activity_type]})"
        params.extend(_ACTIVITY_TYPE_PARAMS[activity_type])

    sql += " ORDER BY is_course DESC, last_effort DESC NULLS LAST"

    result = sql_to_dict(sql, tuple(params))
    return result if result else []


def get_segment_reference_windows(
    segment_ids: Sequence[int],
) -> Dict[int, Dict[str, Any]]:
    """
    Resolve the reference-geometry pointers for a set of segments (009-003, T02b/OQ-3).

    Match Activities and Course Review render a segment's own GPS path from its
    reference activity plus reference window — the legacy Streamlit source
    (`get_activity_df(activity_reference_id, reference_start_point,
    reference_end_point)`) — because `GET /api/segments/{id}/route` is keyed by
    activity_id and cannot serve a segment_id.

    ONE parameterized read over the `activities.segments` primary key for the
    ids the caller just returned, projecting three scalars only (never the
    geometry columns), so the picker list gains them without pushing
    `segment_path` blobs to the client or re-running the filtered list query.

    Args:
        segment_ids: Segment ids to resolve.

    Returns:
        Dict[int, Dict[str, Any]]: segment_id -> {
            activity_reference_id, reference_start_point, reference_end_point }.
        Unknown ids are simply absent (caller emits nulls).
    """
    ids = [int(s) for s in segment_ids]
    if not ids:
        return {}
    rows = sql_to_dict(
        """
        SELECT segment_id,
               activity_reference_id,
               reference_start_point,
               reference_end_point
        FROM activities.segments
        WHERE segment_id = ANY(%s)
        """,
        (ids,),
    ) or []
    return {int(r["segment_id"]): r for r in rows}


def get_segment_leaderboard(segment_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve the ranked effort leaderboard for one segment/course (009-002, FR-4..FR-8).

    Serves the live `activities.vw_segment_leaderboard` view directly (OQ-2: the legacy
    leaderboard_update is a no-write on-demand SELECT, so no refresh call is made).
    Computes server-side:
    - gap_s: seconds behind the segment's fastest effort (FR-8)
    - rank:  row_number by all_time_rank (client re-filters by range via the
             four rank columns; FR-6/AC-5)

    Also projects `cycle_name` — the view's pre-computed recency bucket
    ('Current Cycle' | 'Last 365' | 'All Time', 009-009 OQ-4/T17). It is the ONLY
    membership signal for the lollipop buckets and the Range window: the rank
    columns stay window-scoped ORDERING values (live check 2026-09-16: both
    remaining rank columns are non-NULL for every row, so null-checks cannot
    express membership).

    One query, projected columns only (no SELECT *), parameterized.

    Args:
        segment_id: The target segment/course id.

    Returns:
        List[Dict[str, Any]]: Effort rows ordered by all-time rank (NULLs last).
    """
    sql = """
        WITH lb AS (
            SELECT
                segment_id,
                is_course,
                segment_name,
                cycle_name,
                activity_id,
                activity_start_point,
                activity_end_point,
                distance_mi,
                start_time_utc,
                all_time_rank,
                last_365_rank,
                current_cycle_rank,
                recency_rank,
                elapsed_duration_s,
                pace_str,
                weight_lb,
                muscle_pct,
                fat_pct,
                vo2_max_value,
                altitude_acclimation_m,
                heat_acclimation_pct,
                training_load_acute,
                training_load_pct,
                resting_hr_asleep,
                resting_hr_awake,
                sleep_hours,
                sleep_score,
                awake_hours,
                max_hr,
                avg_hr,
                avg_cadence,
                avg_temp,
                avg_vert_osc,
                avg_vert_ratio,
                avg_gct,
                avg_perf,
                avg_balance,
                avg_stride_length
            FROM activities.vw_segment_leaderboard
            WHERE segment_id = %s
        ), fastest AS (
            SELECT min(elapsed_duration_s) AS best_s
            FROM lb
        )
        SELECT
            lb.*,
            round((lb.elapsed_duration_s - f.best_s), 1) AS gap_s,
            row_number() OVER (ORDER BY lb.all_time_rank NULLS LAST) AS rank
        FROM lb
        CROSS JOIN fastest f
        ORDER BY lb.all_time_rank NULLS LAST
    """
    result = sql_to_dict(sql, (segment_id,))
    return result if result else []


def get_segment_visuals_telemetry() -> List[Dict[str, Any]]:
    """
    Retrieve the generated comparison telemetry for the staged efforts (009-002, FR-10).

    Reads `activities.vw_activity_segments_aggregated`, built off `vw_segment_comps`
    joining the `temp_activity_segment_metas` staging table (staged by the T05 visuals
    endpoint before this helper runs). Projects only the columns the SegmentVisuals
    contract consumes; the view downsamples to one sample per canonical elapsed second.

    Returns:
        List[Dict[str, Any]]: One row per compared effort (meta_rank, id_val,
        path_coords, and per-series arrays ordered by elapsed time).
    """
    sql = """
        SELECT
            id_val,
            meta_rank,
            path_coords,
            x_time,
            x_distance,
            y_time,
            y_hr,
            y_elevation,
            y_cadence,
            y_speed,
            y_vert_ratio,
            y_osc,
            y_temp,
            y_stride,
            y_gct
        FROM activities.vw_activity_segments_aggregated
        ORDER BY meta_rank, id_val
    """
    result = sql_to_dict(sql)
    return result if result else []


def stage_leaderboard_visuals(segment_id: int, efforts: List[Dict[str, Any]]) -> None:
    """
    Transactionally TRUNCATE + set-based multi-row INSERT of the selected efforts
    into activities.temp_activity_segment_metas (009-002, T05).

    A single parameterized execute_values statement replaces the legacy per-row
    f-string loop (viz_factory/leaderboards.py::load_segment_selection_data), so the
    staging table always contains exactly the N selected efforts and repeated calls
    replace (never append). Wrapped in one transaction so a failure mid-insert
    rolls back both the TRUNCATE and the INSERT.

    Args:
        segment_id: The segment the compared efforts belong to (passed through for
            contract symmetry; the staging table is keyed by activity_id + points).
        efforts: Selected efforts, each {activity_id, start_point, end_point}.
    """
    rows = [
        (int(e["activity_id"]), int(e["start_point"]), int(e["end_point"]))
        for e in efforts
    ]

    conn = get_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE activities.temp_activity_segment_metas;")
        execute_values(
            cur,
            "INSERT INTO activities.temp_activity_segment_metas "
            "(activity_id, start_point, end_point) VALUES %s",
            rows,
            template="(%s, %s, %s)",
        )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_activity_route(
    activity_id: int,
    start_m: Optional[float] = None,
    end_m: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve route geometry + elevation for Segment Management (009-003, T03/FR-2, FR-4).

    Single activity-meta lookup plus ONE ordered GPS scan of
    activities.activity_details (lat/lon non-null, ordered by ts_utc):
    path_coords as [lon, lat] pairs, elevations aligned 1:1 with path points.
    Optional start_m/end_m trims by distance_mm (trim-gate support, FR-4);
    None = full range. Parameterized, no pandas, no in-memory rollups.

    Args:
        activity_id: The source activity.
        start_m: Trim start in meters; None = 0.
        end_m: Trim end in meters; None = full distance.

    Returns:
        Dict {path_coords, elevations, distance_m} or None when no activity row.
    """
    meta = sql_to_dict(
        "SELECT activity_id, distance_m FROM activities.activities WHERE activity_id = %s",
        (activity_id,),
    )
    if not meta:
        return None
    sql = """
        SELECT longitude, latitude, elevation_m
        FROM activities.activity_details
        WHERE activity_id = %s
          AND latitude IS NOT NULL AND longitude IS NOT NULL
          AND (%s::numeric IS NULL OR distance_mm / 1000.0 >= %s)
          AND (%s::numeric IS NULL OR distance_mm / 1000.0 <= %s)
        ORDER BY ts_utc
    """
    rows = sql_to_dict(sql, (activity_id, start_m, start_m, end_m, end_m)) or []
    path_coords = [[float(r["longitude"]), float(r["latitude"])] for r in rows]
    elevations = [float(r["elevation_m"]) if r["elevation_m"] is not None else 0.0 for r in rows]
    dist = meta[0].get("distance_m")
    return {
        "path_coords": path_coords,
        "elevations": elevations,
        "distance_m": float(dist) if dist is not None else 0.0,
    }


def get_match_candidates(segment_id: int) -> Dict[str, Any]:
    """
    Retrieve candidate efforts + existing match count for Match Activities mode
    (009-003, T06/FR-9, FR-10).

    Reads the live activities.vw_temp_segment_matches_downselect rows for the
    segment and joins per-activity distance/elevation. ONE query for
    candidates + ONE COUNT for confirmed existing matches (excluding the
    reference activity — mirroring the legacy existing-matches count).
    Scaled *_scaled fields use legacy safe_minmax semantics (value / col max).
    """
    cand_sql = """
        SELECT
            v.activity_id,
            v.confidence,
            v.best_start_dist,
            v.best_end_dist,
            v.dist_deviation,
            v.polygon_deviation_m,
            v.hausdorff_deviation_m,
            v.freschet_deviation_m,
            a.start_time_utc,
            a.distance_m,
            a.elevation_m_gain
        FROM activities.vw_temp_segment_matches_downselect v
        JOIN activities.activities a ON a.activity_id = v.activity_id
        WHERE v.segment_id = %s
        ORDER BY v.confidence DESC
    """
    rows = sql_to_dict(cand_sql, (segment_id,)) or []
    exist_sql = """
        SELECT COUNT(DISTINCT sm.activity_id) AS cnt
        FROM activities.segment_matches sm
        JOIN activities.segments s ON s.segment_id = sm.segment_id
        WHERE sm.segment_id = %s
          AND sm.match_confirmed
          AND sm.activity_id != s.activity_reference_id
    """
    exist = sql_to_dict(exist_sql, (segment_id,))
    existing_match_count = int(exist[0]["cnt"]) if exist else 0

    def _col_max(key: str) -> float:
        vals = [float(r[key]) for r in rows if r.get(key) is not None]
        return max(vals) if vals else 0.0

    max_conf = _col_max("confidence")
    max_dist = _col_max("dist_deviation")
    max_poly = _col_max("polygon_deviation_m")
    max_haus = _col_max("hausdorff_deviation_m")
    max_fresh = _col_max("freschet_deviation_m")

    def _scale(val, mx):
        if val is None or not mx:
            return None if val is None else 0.0
        return float(val) / float(mx)

    data = []
    for r in rows:
        start = r.get("start_time_utc")
        data.append({
            "activity_id": int(r["activity_id"]),
            # Matched effort window inside the candidate activity (T06b/OQ-3).
            # The columns are already fetched above; the UI needs them to draw
            # the candidate's own route instead of the whole activity.
            "best_start_dist": int(r["best_start_dist"]) if r["best_start_dist"] is not None else 0,
            "best_end_dist": int(r["best_end_dist"]) if r["best_end_dist"] is not None else 0,
            "confidence": float(r["confidence"]) if r["confidence"] is not None else 0.0,
            "deviation": {
                "distance_deviation_m": float(r["dist_deviation"]) if r["dist_deviation"] is not None else 0.0,
                "polygon_deviation": float(r["polygon_deviation_m"]) if r["polygon_deviation_m"] is not None else 0.0,
                "hausdorff_deviation": float(r["hausdorff_deviation_m"]) if r["hausdorff_deviation_m"] is not None else None,
                "frechet_deviation": float(r["freschet_deviation_m"]) if r["freschet_deviation_m"] is not None else None,
            },
            "start_time_utc": start.isoformat() if hasattr(start, "isoformat") else str(start or ""),
            "distance_m": float(r["distance_m"]) if r["distance_m"] is not None else 0.0,
            "elevation_gain": float(r["elevation_m_gain"]) if r["elevation_m_gain"] is not None else None,
            "confidence_scaled": _scale(r.get("confidence"), max_conf) or 0.0,
            "distance_deviation_scaled": _scale(r.get("dist_deviation"), max_dist) or 0.0,
            "polygon_deviation_scaled": _scale(r.get("polygon_deviation_m"), max_poly) or 0.0,
            "hausdorff_deviation_scaled": _scale(r.get("hausdorff_deviation_m"), max_haus),
            "frechet_deviation_scaled": _scale(r.get("freschet_deviation_m"), max_fresh),
        })
    return {"data": data, "count": len(data), "existing_match_count": existing_match_count}


def run_find_matches(segment_id: int) -> Dict[str, Any]:
    """
    Run the legacy find-matches pipeline for a segment (009-003, T06/FR-9).

    Exact legacy sequence from segment_creation.py::render_segment_matches:
    match_activities -> pair_generation -> matches_all_polygon ->
    mass_confirmation(1). Synchronous blocking CALLs; frontend shows a spinner
    per OQ-1. Parameterized; no pandas.
    """
    seg = sql_to_dict(
        "SELECT segment_id FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    steps = [
        ("CALL activities.segment_matching_match_activities(%s)", [int(segment_id)]),
        ("CALL activities.segment_matching_pair_generation(%s)", [int(segment_id)]),
        ("CALL activities.segment_matches_all_polygon()", None),
        ("CALL activities.segment_matching_mass_confirmation(1)", None),
    ]
    for proc, params in steps:
        err = qec(proc, params)
        if err:
            raise ValueError(f"find-matches step failed [{proc}]: {err}")
    result = get_match_candidates(int(segment_id))
    return {"message": f"Find matches complete for segment {segment_id}", "candidates": result["data"]}


def finalize_candidate_match(
    segment_id: int, activity_id: int, approved: bool
) -> Dict[str, Any]:
    """
    Confirm or reject one candidate effort (009-003, T06/FR-11).

    Looks up best_start/end_dist + confidence from
    vw_temp_segment_matches_downselect, CALLs
    segment_matching_finalize_match(approved, ...), then CALLs
    staging.update_segment_details(NULL) on approval (legacy confirm flow).
    Returns the MatchOperationResponse shape.
    """
    seg = sql_to_dict(
        "SELECT segment_id, segment_name FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    cand = sql_to_dict(
        """SELECT best_start_dist, best_end_dist, confidence
           FROM activities.vw_temp_segment_matches_downselect
           WHERE segment_id = %s AND activity_id = %s""",
        (segment_id, activity_id),
    )
    if not cand:
        raise ValueError(
            f"activity_id={activity_id} is not a pending candidate for segment_id={segment_id}"
        )
    start = int(cand[0]["best_start_dist"])
    end = int(cand[0]["best_end_dist"])
    conf = float(cand[0]["confidence"]) if cand[0]["confidence"] is not None else 0.0
    flag = "TRUE" if approved else "FALSE"
    err = qec(
        f"CALL activities.segment_matching_finalize_match({flag}, %s, %s, %s, %s, %s::NUMERIC)",
        [int(activity_id), int(segment_id), start, end, conf],
    )
    if err:
        raise ValueError(f"finalize match failed: {err}")
    if approved:
        # Scoped to the confirmed activity: the unscoped NULL variant scans
        # the full segment_matches join and hit a corrupt heap page
        # (live probe: invalid page in block 173078). Same procedure,
        # bounded work, Pi-5 friendly.
        err = qec("CALL staging.update_segment_details(%s);", [int(activity_id)])
        if err:
            raise ValueError(f"update_segment_details failed: {err}")
    matched = sql_to_dict(
        "SELECT COUNT(*) AS cnt FROM activities.segment_matches WHERE segment_id = %s AND match_confirmed",
        (segment_id,),
    )
    verb = "confirmed" if approved else "rejected"
    return {
        "message": f"Match {verb} for activity {activity_id}",
        "segment": {
            "segment_id": int(segment_id),
            "segment_name": str(seg[0].get("segment_name") or ""),
            "matched_count": int(matched[0]["cnt"]) if matched else 0,
        },
    }


def bulk_confirm_matches(segment_id: int, confirm_all: bool) -> Dict[str, Any]:
    """
    Mass-approve remaining candidates (009-003, T06/FR-11).

    Legacy flow: segment_matching_mass_confirmation(4) +
    staging.update_segment_details(NULL). Requires confirm_all=True.
    """
    if not confirm_all:
        raise ValueError("confirm_all must be true to run bulk confirm")
    seg = sql_to_dict(
        "SELECT segment_id, segment_name FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    err = qec("CALL activities.segment_matching_mass_confirmation(4)")
    if err:
        raise ValueError(f"mass confirmation failed: {err}")
    err = qec("CALL staging.update_segment_details(NULL);")
    if err:
        raise ValueError(f"update_segment_details failed: {err}")
    matched = sql_to_dict(
        "SELECT COUNT(*) AS cnt FROM activities.segment_matches WHERE segment_id = %s AND match_confirmed",
        (segment_id,),
    )
    return {
        "message": f"Bulk confirm complete for segment {segment_id}",
        "segment": {
            "segment_id": int(segment_id),
            "segment_name": str(seg[0].get("segment_name") or ""),
            "matched_count": int(matched[0]["cnt"]) if matched else 0,
        },
    }


def run_extra_scoring(segment_id: int, kind: str) -> Dict[str, Any]:
    """
    Run Hausdorff or Freschet scoring over the candidate set (009-003, T06/FR-12).

    Legacy: segment_matches_all_hausdorff() / segment_matches_all_freschet().
    Returns the refreshed candidate set (ScoringResponse shape).
    """
    seg = sql_to_dict(
        "SELECT segment_id FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    proc = {
        "hausdorff": "CALL activities.segment_matches_all_hausdorff()",
        "frechet": "CALL activities.segment_matches_all_freschet()",
    }.get(kind)
    if proc is None:
        raise ValueError(f"unknown scoring kind: {kind}")
    err = qec(proc)
    if err:
        raise ValueError(f"{kind} scoring failed: {err}")
    result = get_match_candidates(int(segment_id))
    return {"message": f"{kind} scoring complete for segment {segment_id}", "candidates": result["data"]}


def create_segment(
    segment_name: str,
    activity_id: int,
    start_m: int,
    end_m: int,
    is_course: bool,
) -> int:
    """
    Create a course/segment from an activity range (009-003, T05/FR-5).

    Mirrors the legacy frontend_functions/segment_creation.py::new_segment_creation
    flow exactly: CALL activities.segment_matching_segment_creation, resolve the
    new id via MAX(segment_id), then CALL
    activities.segment_matching_finalize_match(TRUE, ...) to record the source
    activity's own confirmed match (confidence 0). Parameterized; no pandas.

    Args:
        segment_name: New segment/course name (non-empty, <= 200 chars).
        activity_id: Source activity.
        start_m: Trim-gate start in meters.
        end_m: Trim-gate end in meters.
        is_course: True = course, False = segment.

    Returns:
        int: The new segment_id.

    Raises:
        ValueError: On validation failure or missing source/new row.
    """
    name = (segment_name or "").strip()
    if not name:
        raise ValueError("name must be non-empty")
    if len(name) > 200:
        raise ValueError("name must be <= 200 characters")
    if start_m < 0 or end_m < 0:
        raise ValueError("start_m and end_m must be >= 0")
    if end_m < start_m:
        raise ValueError("end_m must be >= start_m")
    meta = sql_to_dict(
        "SELECT activity_id, distance_m FROM activities.activities WHERE activity_id = %s",
        (activity_id,),
    )
    if not meta:
        raise ValueError(f"activity_id={activity_id} not found")
    dist = meta[0].get("distance_m")
    if dist is not None and end_m > float(dist):
        raise ValueError(
            f"end_m ({end_m}) exceeds activity distance ({float(dist):.1f} m)"
        )
    err = qec(
        "CALL activities.segment_matching_segment_creation(%s, %s, %s, %s, %s)",
        [name, int(activity_id), int(start_m), int(end_m), bool(is_course)],
    )
    if err:
        raise ValueError(f"segment creation failed: {err}")
    new_id = one_sql_result("SELECT MAX(segment_id) FROM activities.segments")
    if new_id is None:
        raise ValueError("segment creation produced no row")
    err = qec(
        "CALL activities.segment_matching_finalize_match(TRUE, %s, %s, %s, %s, 0::NUMERIC)",
        [int(activity_id), int(new_id), int(start_m), int(end_m)],
    )
    if err:
        raise ValueError(f"finalize match failed: {err}")
    return int(new_id)


def delete_segment(segment_id: int) -> Dict[str, Any]:
    """
    Delete a segment with its match rows and detail records (009-003, T07/FR-13).

    Mirrors the legacy Streamlit delete flow (segment_matches -> segments ->
    segments_details) but adds the child tables that reference the segment, in
    ONE transaction:

    - activities.segment_match_exclusions: FK segment_match_exclusions_segment_id_fkey
      is ON DELETE NO ACTION (verified live), so a plain parent DELETE fails
      whenever exclusion rows exist -> delete first.
    - activities.distinct_segments: derived pair rows keyed by segment_id /
      match_seg_id; removed so no orphan pairing rows survive.
    - activities.segment_matches, activities.segments_details: match + effort
      records for the segment (FR-13).
    - activities.segments: the segment row itself.

    Five parameterized single-row DELETEs on a single connection (Pi-5: no
    scans, no per-row round-trips, no pandas). Rolls back as a unit.

    Args:
        segment_id: The segment/course to remove.

    Returns:
        Dict {message, segment_id, deleted_matches, deleted_details}.

    Raises:
        ValueError: When the segment does not exist or a statement fails.
    """
    seg = sql_to_dict(
        "SELECT segment_id, segment_name FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    name = seg[0].get("segment_name") or ""
    deleted_matches = 0
    deleted_details = 0
    conn = get_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM activities.segment_match_exclusions WHERE segment_id = %s",
            (segment_id,),
        )
        cur.execute(
            "DELETE FROM activities.distinct_segments WHERE segment_id = %s OR match_seg_id = %s",
            (segment_id, segment_id),
        )
        cur.execute(
            "DELETE FROM activities.segment_matches WHERE segment_id = %s",
            (segment_id,),
        )
        deleted_matches = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        cur.execute(
            "DELETE FROM activities.segments_details WHERE segment_id = %s",
            (segment_id,),
        )
        deleted_details = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        cur.execute(
            "DELETE FROM activities.segments WHERE segment_id = %s",
            (segment_id,),
        )
        removed = cur.rowcount
        if not removed:
            raise ValueError(f"segment_id={segment_id} not found")
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "message": (
            f"Segment {segment_id} '{name}' deleted "
            f"({deleted_matches} match rows, {deleted_details} detail rows removed)"
        ),
        "segment_id": int(segment_id),
        "deleted_matches": deleted_matches,
        "deleted_details": deleted_details,
    }


def reset_match_tables(confirm_reset: bool) -> Dict[str, Any]:
    """
    Destructive reset of matching data (009-003, T07/FR-14).

    Legacy sequence (segment_creation.py reset button): TRUNCATE
    segment_matches -> segment_match_exclusions -> distinct_segments ->
    segments RESTART IDENTITY CASCADE, extended with segments_details.

    Rationale for segments_details: the segments truncate restarts the
    segment_id identity, so surviving effort rows would re-attach to newly
    created segments that recycle those ids (phantom efforts / leaderboard
    rows). Legacy left them behind; resetting them keeps the wipe consistent
    with the per-segment delete in delete_segment (FR-13).

    All five TRUNCATEs run in ONE transaction on a single connection, so a
    failure leaves the matching data untouched. TRUNCATE does not scan rows
    (Pi-5 friendly; also avoids the pre-existing corrupt heap page that an
    unscoped segment_matches scan hits). No pandas.

    Args:
        confirm_reset: Deliberate-activation flag (OQ-2: confirmation dialog).

    Returns:
        Dict {message, truncated_count} where truncated_count is the number of
        tables truncated.

    Raises:
        ValueError: When confirm_reset is not true or a statement fails.
    """
    if not confirm_reset:
        raise ValueError("confirm_reset must be true")
    statements = [
        "TRUNCATE TABLE activities.segment_matches",
        "TRUNCATE TABLE activities.segment_match_exclusions",
        "TRUNCATE TABLE activities.distinct_segments",
        "TRUNCATE TABLE activities.segments_details",
        "TRUNCATE TABLE activities.segments RESTART IDENTITY CASCADE",
    ]
    conn = get_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "message": (
            "Match tables reset: segment_matches, segment_match_exclusions, "
            "distinct_segments, segments_details truncated and segments reset"
        ),
        "truncated_count": len(statements),
    }


def rename_segment(segment_id: int, new_name: str) -> Dict[str, Any]:
    """
    Rename a course/segment (009-003, T07/FR-16).

    Same single parameterized UPDATE as the legacy course-review rename
    (viz_factory/run_list.py::update_course_name). One existence check + one
    UPDATE; no scans, no pandas.

    Args:
        segment_id: The course/segment to rename.
        new_name: New name (stripped, non-empty, <= 200 chars).

    Returns:
        Dict {segment_id, segment_name} per the RenameResponse contract.

    Raises:
        ValueError: On validation failure or unknown segment_id.
    """
    name = (new_name or "").strip()
    if not name:
        raise ValueError("new_name must be non-empty")
    if len(name) > 200:
        raise ValueError("new_name must be <= 200 characters")
    seg = sql_to_dict(
        "SELECT segment_id FROM activities.segments WHERE segment_id = %s",
        (segment_id,),
    )
    if not seg:
        raise ValueError(f"segment_id={segment_id} not found")
    err = qec(
        "UPDATE activities.segments SET segment_name = %s WHERE segment_id = %s",
        [name, int(segment_id)],
    )
    if err:
        raise ValueError(f"segment rename failed: {err}")
    return {"segment_id": int(segment_id), "segment_name": name}


def get_segment_name(segment_id: int) -> str:
    """
    Resolve the segment_name for a segment_id (009-002, T05).

    Used by the visuals endpoint to populate the SegmentVisuals.segment_name
    contract field from the request's segment_id. Falls back to "" when unknown.
    """
    result = sql_to_dict(
        "SELECT segment_name FROM activities.segments WHERE segment_id = %s LIMIT 1",
        (segment_id,),
    )
    if not result:
        return ""
    return str(result[0].get("segment_name") or "")


def get_segment_activities(
    activity_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Retrieve the source-activity selection list for Segment Management (009-003, T02/FR-1).

    One row per activity in activities.activities (newest first) with:
    - child_segment_count: segments created FROM this activity
      (activities.segments.activity_reference_id = activity_id)
    - matched_count: segment efforts belonging to this activity
      (activities.segments_details.activity_id = activity_id)

    Counts are correlated subqueries (no JOIN fan-out), parameterized, with
    LIMIT/OFFSET pagination for Pi-5 memory bounds. Activity-type filtering
    reuses the legacy _ACTIVITY_TYPE_PATTERNS semantics (Run excludes trail).

    Args:
        activity_type: One of _ACTIVITY_TYPE_PATTERNS keys; None = any.
        limit: Max rows (1..200).
        offset: Pagination offset.

    Returns:
        List[Dict[str, Any]]: Activity summary rows.
    """
    sql = """
        SELECT
            a.activity_id,
            a.activity_type_name,
            a.start_timestamp_utc AS start_time_utc,
            a.distance_m,
            (SELECT COUNT(*) FROM activities.segments s
              WHERE s.activity_reference_id = a.activity_id) AS child_segment_count,
            (SELECT COUNT(*) FROM activities.segments_details sd
              WHERE sd.activity_id = a.activity_id) AS matched_count
        FROM activities.activities a
        WHERE (%s::text IS NULL OR TRUE)
    """
    params: List[Any] = [activity_type]
    # Replace the no-op predicate with the legacy type pattern when known;
    # unknown strings fall back to a case-insensitive substring match.
    if activity_type in _ACTIVITY_TYPE_PATTERNS:
        sql = sql.replace(
            "WHERE (%s::text IS NULL OR TRUE)",
            f"WHERE ({_ACTIVITY_TYPE_PATTERNS[activity_type]})",
        )
        params = list(_ACTIVITY_TYPE_PARAMS[activity_type])
    elif activity_type:
        sql = sql.replace(
            "WHERE (%s::text IS NULL OR TRUE)",
            "WHERE (a.activity_type_name ILIKE %s)",
        )
        params = [f"%{activity_type}%"]
    else:
        sql = sql.replace(
            "WHERE (%s::text IS NULL OR TRUE)",
            "WHERE (TRUE)",
        )
        params = []

    sql += " ORDER BY a.start_timestamp_utc DESC NULLS LAST LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    result = sql_to_dict(sql, tuple(params))
    return result if result else []


def get_courses_page(
    name: Optional[str] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    page: int = 1,
    page_size: int = 20,
) -> Dict[str, Any]:
    """
    Retrieve the paginated course-review list for Segment Management (009-003, T02/FR-15).

    Reads activities.vw_segments_effort_stats filtered to is_course rows and
    projects the CourseInfo contract shape (segment_id -> course_id,
    segment_name -> course_name, matched_activity_count -> attempt_count,
    last_effort -> last_run, is_new = last_effort IS NULL).

    Single round-trip: COUNT(*) OVER() carries the filtered total alongside
    the page rows (Pi-5: avoids a second count query).

    Args:
        name: Case-insensitive substring on segment_name; None/empty = any.
        min_distance: Minimum distance_mi; None = any.
        max_distance: Maximum distance_mi; None = any.
        page: 1-based page number.
        page_size: Rows per page.

    Returns:
        Dict with data/total_count/page/page_size/total_pages.
    """
    offset = (page - 1) * page_size
    sql = """
        SELECT
            segment_id AS course_id,
            segment_name AS course_name,
            distance_mi,
            elevation_gain,
            last_effort AS last_run,
            matched_activity_count AS attempt_count,
            (last_effort IS NULL) AS is_new,
            COUNT(*) OVER() AS total_count
        FROM activities.vw_segments_effort_stats
        WHERE is_course = TRUE
          AND (%s::text IS NULL OR segment_name ILIKE %s)
          AND (%s::numeric IS NULL OR distance_mi >= %s)
          AND (%s::numeric IS NULL OR distance_mi <= %s)
        ORDER BY last_effort DESC NULLS LAST, course_name
        LIMIT %s OFFSET %s
    """
    params: List[Any] = [
        name, f"%{name}%" if name else None,
        min_distance, min_distance,
        max_distance, max_distance,
        page_size, offset,
    ]
    rows = sql_to_dict(sql, tuple(params)) or []
    total_count = int(rows[0]["total_count"]) if rows else 0
    data = [
        {k: r[k] for k in (
            "course_id", "course_name", "distance_mi", "elevation_gain",
            "last_run", "attempt_count", "is_new",
        )}
        for r in rows
    ]
    total_pages = (total_count + page_size - 1) // page_size if total_count else 0
    return {
        "data": data,
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


__all__ = [
    'get_activities_list',
    'get_activity_by_id',
    'get_activity_telemetry',
    'get_segment_matches',
    'get_recent_activities',
    'get_activity_stats',
    'resolve_latest_activity_id',
    'get_activity_report_header',
    'get_activity_report_header_view',
    'get_activity_report_header_view_by_type',
    'get_activity_percentile_hr',
    'get_activity_report_efforts',
    'get_activity_report_charts',
    'get_activity_course_path',
    # 009-002 Leaderboards
    'get_leaderboard_segments',
    'get_segment_leaderboard',
    'get_segment_visuals_telemetry',
    'stage_leaderboard_visuals',
    'get_segment_name',
    # 009-003 Segment Management (T02)
    'get_segment_activities',
    'get_courses_page',
    # 009-003 Segment Management (T02b)
    'get_segment_reference_windows',
    # 009-003 Segment Management (T03)
    'get_activity_route',
    # 009-003 Segment Management (T06)
    'get_match_candidates',
    'run_find_matches',
    'finalize_candidate_match',
    'bulk_confirm_matches',
    'run_extra_scoring',
    # 009-003 Segment Management (T05)
    'create_segment',
    # 009-003 Segment Management (T07)
    'delete_segment',
    'reset_match_tables',
    'rename_segment',
]