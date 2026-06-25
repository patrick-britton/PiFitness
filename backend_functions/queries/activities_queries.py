"""
Activities Query Functions
===========================

Database query functions for activity data (GPS tracks, segments, metrics).
These functions return plain Python data structures with no Streamlit dependencies.
"""

from typing import List, Dict, Any, Optional, Union, Sequence
from datetime import date
from backend_functions.database_functions import qec, sql_to_dict
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
            timestamp_utc,
            latitude_deg,
            longitude_deg,
            elevation_m,
            heart_rate_bpm,
            cadence_rpm,
            speed_mps,
            distance_m
        FROM activities.activity_telemetry
        WHERE activity_id = %s
        ORDER BY timestamp_utc
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
            sm.segment_match_id,
            sm.segment_id,
            s.segment_name,
            sm.match_confidence,
            sm.duration_s,
            sm.distance_m,
            sm.elevation_gain_m,
            sm.avg_heart_rate_bpm,
            sm.max_heart_rate_bpm,
            sm.start_timestamp_utc
        FROM activities.segment_matches sm
        JOIN activities.segments s ON sm.segment_id = s.segment_id
        WHERE sm.activity_id = %s
        ORDER BY sm.start_timestamp_utc
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
            AVG(heart_rate_bpm) AS avg_heart_rate_bpm,
            MAX(heart_rate_bpm) AS max_heart_rate_bpm,
            AVG(speed_mps) * 3.6 AS avg_speed_kph,
            MAX(speed_mps) * 3.6 AS max_speed_kph
        FROM activities.activity_telemetry
        WHERE activity_id = %s
        GROUP BY activity_id
    """
    result = sql_to_dict(sql, (activity_id,))
    return result[0] if result else None


__all__ = [
    'get_activities_list',
    'get_activity_by_id',
    'get_activity_telemetry',
    'get_segment_matches',
    'get_recent_activities',
    'get_activity_stats',
]