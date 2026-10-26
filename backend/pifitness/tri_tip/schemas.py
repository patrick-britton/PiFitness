"""
Tri-tip Timer Schemas
=====================

Pydantic request/response models for the Tri-tip Timer feature.

These mirror the frontend contract types in
`frontend/pifitness/src/lib/types/tri-tip.ts`.
"""

from datetime import datetime
from typing import List, Literal, Optional
from pydantic import BaseModel, Field


TriTipShape = Literal["Short+Fat", "Long+Skinny", "Typical"]
TriTipStatus = Literal["initiated", "active", "complete"]


class TriTipInitiateRequest(BaseModel):
    """Request body for POST /api/food/tri-tip (initiate an event)."""
    weight_lbs: float = Field(..., gt=0, description="Weight in pounds (CHECK weight_lbs > 0)")
    shape: TriTipShape = Field(..., description="One meat shape")


class TriTipPlaceRequest(BaseModel):
    """Request body for POST /api/food/tri-tip/{id}/place."""
    grill_temp_f: float = Field(..., description="Grill temperature in °F")


class TriTipReadingRequest(BaseModel):
    """Request body for POST /api/food/tri-tip/{id}/readings."""
    grill_temp_f: float = Field(..., description="Grill temperature in °F")
    internal_temp_f: float = Field(..., description="Internal meat temperature in °F")
    note: Optional[str] = Field(None, description="Optional note")


class TriTipCurvePoint(BaseModel):
    """A single point on the predicted temperature curve."""
    grill_min: float = Field(..., description="Minutes elapsed since the event's first reading")
    internal_temp_f: float = Field(..., description="Predicted internal temperature in °F")


class TriTipPrediction(BaseModel):
    """Prediction / ETA object (OQ-1)."""
    projected_done_at: str = Field(..., description="ISO time-of-day the meat is projected to reach target temp")
    minutes_remaining: float = Field(..., description="Relative minutes between now and projected_done_at")
    curve: List[TriTipCurvePoint] = Field(..., description="Predicted curve from t0 to target temp")


class TriTipReferenceEvent(BaseModel):
    """A prior completed event normalized to its own t0 for gray chart reference (OQ-2)."""
    event: dict = Field(..., description="The prior completed event record")
    readings: List[dict] = Field(..., description="Readings tagged with grill_min (minutes since that event's t0)")


class TriTipActiveResponse(BaseModel):
    """Response for GET /api/food/tri-tip/active."""
    event: Optional[dict] = Field(None, description="The current in-progress event, or null")
    readings: List[dict] = Field(default_factory=list, description="Readings for the in-progress event")
    prediction: Optional[TriTipPrediction] = Field(None, description="Prediction for the in-progress event")
    references: List[TriTipReferenceEvent] = Field(default_factory=list, description="Prior completed events as chart references")
    blocked_by: Optional[dict] = Field(None, description="Event blocking a new initiate, when applicable")