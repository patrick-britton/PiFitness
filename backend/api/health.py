"""
Health API Endpoints
====================

FastAPI endpoints for health metrics (heart rate, sleep, weight, etc.).
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date

from backend_functions.queries import (
    get_weight_targets,
    get_weight_viz_data,
    get_heart_rate_timeseries,
    get_sleep_data,
)

router = APIRouter(prefix="/api/health", tags=["health"])


@router.get("/weight-targets")
async def get_weight_targets_endpoint():
    """
    Get weight targets.

    Returns:
        List of weight target records
    """
    try:
        targets = get_weight_targets()
        return {"data": targets, "count": len(targets)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch weight targets: {str(e)}",
        )


@router.get("/heartrate")
async def get_heart_rate_timeseries_endpoint(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: Optional[int] = 1000,
):
    """
    Get heart rate time series data.

    Args:
        start_date: Filter by start date (inclusive)
        end_date: Filter by end date (inclusive)
        limit: Maximum number of data points to return

    Returns:
        List of heart rate data points from health.heartrate_raw
    """
    try:
        data = get_heart_rate_timeseries(
            start_date=start_date,
            end_date=end_date,
            limit=limit or 1000,
        )
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch heart rate data: {str(e)}",
        )


@router.get("/sleep")
async def get_sleep_data_endpoint(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    """
    Get sleep data.

    Args:
        start_date: Filter by sleep end date (inclusive)
        end_date: Filter by sleep end date (inclusive)

    Returns:
        List of sleep records from health.sleep_totals
    """
    try:
        data = get_sleep_data(
            start_date=start_date,
            end_date=end_date,
        )
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch sleep data: {str(e)}",
        )