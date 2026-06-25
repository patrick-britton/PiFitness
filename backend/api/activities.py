"""
Activities API Endpoints
========================

FastAPI endpoints for activity data (GPS tracks, segments, metrics).
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date

from backend_functions.queries import (
    get_activities_list,
    get_activity_by_id,
    get_activity_telemetry,
    get_segment_matches,
)

router = APIRouter(prefix="/api/activities", tags=["activities"])


@router.get("")
async def list_activities(
    limit: Optional[int] = 50,
    offset: Optional[int] = 0,
    activity_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    """
    List activities with optional filtering.

    Args:
        limit: Maximum number of activities to return
        offset: Pagination offset
        activity_type: Filter by activity type (e.g., 'run', 'ride')
        start_date: Filter by start date (inclusive)
        end_date: Filter by end date (inclusive)

    Returns:
        List of activity summaries
    """
    try:
        activities = get_activities_list(
            limit=limit or 50,
            offset=offset or 0,
            activity_type=activity_type,
            start_date=start_date,
            end_date=end_date,
        )
        return {"data": activities, "count": len(activities)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activities: {str(e)}",
        )


@router.get("/{activity_id}")
async def get_activity(activity_id: int):
    """
    Get a single activity by ID with full details.

    Args:
        activity_id: The activity ID

    Returns:
        Activity record with metadata
    """
    try:
        activity = get_activity_by_id(activity_id)
        if not activity:
            raise HTTPException(status_code=404, detail="Activity not found")
        return {"data": activity}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activity: {str(e)}",
        )


@router.get("/{activity_id}/telemetry")
async def get_activity_telemetry_data(activity_id: int):
    """
    Get telemetry data (GPS, heart rate, cadence) for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        Telemetry data points
    """
    try:
        telemetry = get_activity_telemetry(activity_id)
        if not telemetry:
            raise HTTPException(status_code=404, detail="Telemetry not found")
        return {"data": telemetry}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telemetry: {str(e)}",
        )


@router.get("/{activity_id}/segments")
async def get_activity_segment_matches(activity_id: int):
    """
    Get segment matches for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        List of segment match records
    """
    try:
        segments = get_segment_matches(activity_id)
        return {"data": segments, "count": len(segments)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segment matches: {str(e)}",
        )