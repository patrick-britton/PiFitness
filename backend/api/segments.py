"""
Segments API Endpoints
======================

FastAPI endpoints for segment data (courses/routes).
"""

from fastapi import APIRouter, HTTPException
from backend_functions.database_functions import sql_to_dict

router = APIRouter(prefix="/api", tags=["segments"])

@router.get("/segments")
async def get_all_segments():
    """
    Get all segments (courses/routes).

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