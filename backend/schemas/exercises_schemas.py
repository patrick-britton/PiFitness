"""
Exercise Timer Schemas
======================

Pydantic request/response models for the Exercise Timer feature.

These mirror the frontend contract types in
`frontend/pifitness/src/lib/types/exercises.ts`.
"""

from typing import Optional

from pydantic import BaseModel, Field


class ExerciseCreateRequest(BaseModel):
    """Request body for POST /api/exercises (create a timer)."""
    name: str = Field(..., min_length=1, description="Display name (unique case-insensitively)")
    interval_seconds: float = Field(
        ..., gt=0, description="Pacing interval in seconds (CHECK interval_seconds > 0)"
    )
    notes: Optional[str] = Field(None, description="Optional notes (unused in v1)")


class ExerciseUpdateRequest(BaseModel):
    """Request body for PUT /api/exercises/{id} (edit a timer)."""
    name: Optional[str] = Field(None, min_length=1, description="Display name (unique case-insensitively)")
    interval_seconds: Optional[float] = Field(None, gt=0, description="Pacing interval in seconds")
    notes: Optional[str] = Field(None, description="Optional notes (unused in v1)")


class ExerciseAttemptCreateRequest(BaseModel):
    """
    Request body for POST /api/exercises/{id}/attempts (save a confirmed attempt).

    Only user-confirmed paced_count and total_count are sent — the client-side
    countdown/count-up defaults are never persisted (FR-8). `interval_seconds_used`
    snapshots the interval in effect during the run (FR-10).
    """
    started_at: str = Field(..., description="ISO timestamp when the run started (after the 5→1 countdown)")
    ended_at: str = Field(..., description="ISO timestamp when Stop was pressed")
    interval_seconds_used: float = Field(..., gt=0, description="Snapshot of the timer's interval during the run")
    paced_count: int = Field(..., ge=0, description="User-confirmed on-pace reps (CHECK >= 0)")
    total_count: int = Field(..., ge=0, description="User-confirmed total reps (CHECK >= paced_count)")
    notes: Optional[str] = Field(None, description="Optional notes (unused in v1)")