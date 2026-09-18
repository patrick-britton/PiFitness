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
]