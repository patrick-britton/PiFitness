"""
Exercise Timer API Endpoints
============================

FastAPI endpoints for the Exercise Timer feature
(exercises.exercise_timers / exercises.exercise_attempts).

Endpoints (contract from .features/designs_active/007-001_design.md):
    GET    /api/exercises                    -> { data: ExerciseTimerSummary[], count }
    GET    /api/exercises/{exercise_id}      -> { exercise, last_attempt }
    POST   /api/exercises                    -> created ExerciseTimer (201); 409 duplicate name
    PUT    /api/exercises/{exercise_id}      -> updated ExerciseTimer; 404 / 409
    DELETE /api/exercises/{exercise_id}      -> 200; cascades the timer's attempts (OQ-1)
    POST   /api/exercises/{exercise_id}/attempts -> saved ExerciseAttempt (201)
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException

from backend_functions.queries import (
    get_exercise_timer,
    get_last_attempt,
    list_exercise_summaries,
    create_exercise,
    update_exercise,
    delete_exercise,
    create_attempt,
    ExerciseError,
    ExerciseNotFoundError,
    ExerciseNameConflictError,
    ExerciseValidationError,
)
from backend.schemas.exercises_schemas import (
    ExerciseAttemptCreateRequest,
    ExerciseCreateRequest,
    ExerciseUpdateRequest,
)

router = APIRouter(prefix="/api/exercises", tags=["exercises"])


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

_TIMER_DATETIME_FIELDS = ("created_at", "updated_at")
_ATTEMPT_DATETIME_FIELDS = ("started_at", "ended_at", "created_at")


def _serialize_timer(timer: dict) -> dict:
    """Normalize timer NUMERIC interval + datetime fields for JSON."""
    if timer is None:
        return None
    out = dict(timer)
    if out.get("interval_seconds") is not None:
        out["interval_seconds"] = float(out["interval_seconds"])
    for field in _TIMER_DATETIME_FIELDS:
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


def _serialize_attempt(attempt: dict) -> dict:
    """Normalize attempt NUMERIC interval + datetime fields for JSON."""
    if attempt is None:
        return None
    out = dict(attempt)
    if out.get("interval_seconds_used") is not None:
        out["interval_seconds_used"] = float(out["interval_seconds_used"])
    for field in _ATTEMPT_DATETIME_FIELDS:
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


def _serialize_summary(summary: dict) -> dict:
    """Normalize a summary row: timer fields + derived aggregate numerics."""
    out = dict(summary)
    if out.get("interval_seconds") is not None:
        out["interval_seconds"] = float(out["interval_seconds"])
    for field in _TIMER_DATETIME_FIELDS:
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    # MAX() over an empty set yields NULL; keep as-is so the UI sees null stats.
    return out
# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
async def list_timers():
    """List every timer with per-timer attempt aggregates (OQ-2), ordered by name."""
    try:
        data = [_serialize_summary(s) for s in list_exercise_summaries()]
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch exercise timers: {str(e)}")


@router.get("/{exercise_id}")
async def get_timer(exercise_id: int):
    """Get a single timer with its most recent attempt (progress-bar calibration)."""
    try:
        timer = get_exercise_timer(exercise_id)
        if timer is None:
            raise HTTPException(status_code=404, detail="Exercise timer not found")
        last_attempt = None
        if timer is not None:
            last_attempt = get_last_attempt(exercise_id)
        return {
            "exercise": _serialize_timer(timer),
            "last_attempt": _serialize_attempt(last_attempt),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch exercise timer: {str(e)}")


@router.post("", status_code=201)
async def create_timer(req: ExerciseCreateRequest):
    """Create a new exercise timer. 409 on case-insensitive duplicate name."""
    try:
        timer = create_exercise(
            name=req.name,
            interval_seconds=req.interval_seconds,
            notes=req.notes,
        )
        return _serialize_timer(timer)
    except ExerciseNameConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ExerciseError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create exercise timer: {str(e)}")


@router.put("/{exercise_id}")
async def update_timer(exercise_id: int, req: ExerciseUpdateRequest):
    """Edit an existing timer (partial update). 404 if missing, 409 on duplicate name."""
    try:
        timer = update_exercise(
            exercise_id=exercise_id,
            name=req.name,
            interval_seconds=req.interval_seconds,
            notes=req.notes,
        )
        return _serialize_timer(timer)
    except ExerciseNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ExerciseNameConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ExerciseError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update exercise timer: {str(e)}")


@router.delete("/{exercise_id}")
async def delete_timer(exercise_id: int):
    """
    Permanently delete a timer and ALL of its attempt history (OQ-1).
    Always returns 200 on success; the cascade runs in one transaction (T02).
    """
    try:
        delete_exercise(exercise_id)
        return {"success": True}
    except ExerciseNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ExerciseError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete exercise timer: {str(e)}")


@router.post("/{exercise_id}/attempts", status_code=201)
async def save_attempt(exercise_id: int, req: ExerciseAttemptCreateRequest):
    """
    Save one confirmed attempt. Only user-confirmed paced_count/total_count and
    the interval snapshot are written (FR-8, FR-10).
    """
    try:
        attempt = create_attempt(
            exercise_id=exercise_id,
            started_at=req.started_at,
            ended_at=req.ended_at,
            interval_seconds_used=req.interval_seconds_used,
            paced_count=req.paced_count,
            total_count=req.total_count,
            notes=req.notes,
        )
        return _serialize_attempt(attempt)
    except ExerciseNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ExerciseValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except ExerciseError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save exercise attempt: {str(e)}")