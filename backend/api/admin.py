"""
Admin API Endpoints
===================

FastAPI endpoints for administrative functions (task monitoring, DB stats, etc.).
"""

from fastapi import APIRouter, HTTPException
from typing import Optional

from backend_functions.queries import (
    get_task_execution_view,
    get_task_scheduling_view,
    get_distinct_task_names,
    get_event_history,
    get_active_db_sessions,
    get_api_service_list,
)

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/tasks")
async def list_tasks():
    """
    List all tasks with execution history.

    Returns:
        List of task execution records
    """
    try:
        tasks = get_task_execution_view()
        return {"data": tasks, "count": len(tasks)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch tasks: {str(e)}",
        )


@router.get("/tasks/schedule")
async def get_task_schedule():
    """
    Get task scheduling configuration.

    Returns:
        List of task configuration records
    """
    try:
        schedule = get_task_scheduling_view()
        return {"data": schedule, "count": len(schedule)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task schedule: {str(e)}",
        )


@router.get("/tasks/names")
async def get_task_names():
    """
    Get distinct task names.

    Returns:
        List of task names
    """
    try:
        names = get_distinct_task_names()
        return {"data": names, "count": len(names)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task names: {str(e)}",
        )

@router.post("/tasks/{task_name}/execute")
async def execute_task(task_name: str):
    """
    Trigger execution of a background task.

    Args:
        task_name: The name of the task to execute

    Returns:
        Execution result or error
    """
    try:
        from backend_functions.task_execution import execute_task_by_name
        result = execute_task_by_name(task_name)
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute task: {str(e)}",
        )

@router.get("/events")
async def get_events():
    """
    Get event history.

    Returns:
        List of event history records
    """
    try:
        events = get_event_history()
        return {"data": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch events: {str(e)}",
        )

@router.get("/db-sessions")
async def get_db_sessions():
    """
    Get active database sessions.

    Returns:
        List of active database session records
    """
    try:
        sessions = get_active_db_sessions()
        return {"data": sessions, "count": len(sessions)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch DB sessions: {str(e)}",
        )

@router.get("/services")
async def get_services():
    """
    Get API services list.

    Returns:
        List of API service records
    """
    try:
        services = get_api_service_list()
        return {"data": services, "count": len(services)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch services: {str(e)}",
        )
