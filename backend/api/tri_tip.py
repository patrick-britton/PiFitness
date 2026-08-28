"""
Tri-tip Timer API Endpoints
===========================

FastAPI endpoints for the Tri-tip Timer feature (food.tri_tip_events /
food.tri_tip_readings).

Endpoints (contract from .features/designs_active/005-001_design.md):
    GET    /api/food/tri-tip          -> list events
    GET    /api/food/tri-tip/active   -> current in-progress event + prediction + references
    GET    /api/food/tri-tip/{id}     -> single event with readings
    POST   /api/food/tri-tip          -> initiate event (weight + shape)
    POST   /api/food/tri-tip/{id}/place    -> place meat (first reading @ 38F)
    POST   /api/food/tri-tip/{id}/readings -> record a reading while active
    POST   /api/food/tri-tip/{id}/complete -> pull meat (status -> complete)
    DELETE /api/food/tri-tip/{id}     -> abandon (removes event + cascade readings)
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from backend_functions.queries import (
    get_tri_tip_event,
    list_tri_tip_events,
    get_event_readings,
    get_active_event,
    initiate_tri_tip,
    place_tri_tip,
    add_tri_tip_reading,
    complete_tri_tip,
    abandon_tri_tip,
    TriTipActiveEventExistsError,
    TriTipNotFoundError,
    TriTipStateError,
    TriTipError,
)
from backend_functions.tri_tip_prediction import build_tri_tip_prediction
from backend.schemas.tri_tip_schemas import (
    TriTipInitiateRequest,
    TriTipPlaceRequest,
    TriTipReadingRequest,
    TriTipActiveResponse,
    TriTipReferenceEvent,
)

router = APIRouter(prefix="/api/food/tri-tip", tags=["tri-tip"])


def _now_utc() -> datetime:
    """Aware UTC now for prediction timing."""
    return datetime.now(timezone.utc)


def _as_timestamp(value):
    """Coerce a value to a timezone-aware datetime, or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


_NUMERIC_FIELDS = (
    "weight_lbs",
    "target_internal_temp_f",
    "grill_temp_f",
    "internal_temp_f",
)


def _coerce_numerics(record: dict) -> dict:
    """Coerce DB NUMERIC (Decimal/str) fields to float for JSON."""
    for field in _NUMERIC_FIELDS:
        if field in record and record[field] is not None:
            record[field] = float(record[field])
    return record


def _serialize_event(event: dict) -> dict:
    """Normalize event datetime fields to ISO strings for JSON serialization."""
    if event is None:
        return None
    out = dict(event)
    if isinstance(out.get("started_at"), datetime):
        out["started_at"] = out["started_at"].isoformat()
    if isinstance(out.get("completed_at"), datetime):
        out["completed_at"] = out["completed_at"].isoformat()
    if isinstance(out.get("created_at"), datetime):
        out["created_at"] = out["created_at"].isoformat()
    return _coerce_numerics(out)


def _serialize_readings(readings) -> list:
    """Normalize reading record timestamps to ISO strings."""
    out = []
    for r in readings:
        r2 = dict(r)
        if isinstance(r2.get("recorded_at"), datetime):
            r2["recorded_at"] = r2["recorded_at"].isoformat()
        if isinstance(r2.get("created_at"), datetime):
            r2["created_at"] = r2["created_at"].isoformat()
        out.append(_coerce_numerics(r2))
    return out


def _load_prior_references(exclude_event_id: int) -> list:
    """
    Prior COMPLETED events (for shape-aware prediction learning and as gray
    chart references), each with readings tagged with grill_min since t0.
    """
    events = list_tri_tip_events(limit=500)
    priors = []
    for ev in events:
        if ev["tri_tip_id"] == exclude_event_id:
            continue
        if ev.get("status") != "complete":
            continue
        readings = get_event_readings(ev["tri_tip_id"])
        if not readings:
            continue
        t0_dt = _as_timestamp(ev.get("started_at"))
        tagged = []
        if t0_dt is not None:
            for r in readings:
                r_dt = _as_timestamp(r.get("recorded_at"))
                r2 = dict(r)
                r2["grill_min"] = round((r_dt - t0_dt).total_seconds() / 60.0, 2) if r_dt else None
                tagged.append(r2)
        else:
            tagged = [dict(r) for r in readings]
        priors.append({"event": ev, "readings": tagged})
    return priors


def _build_active_response() -> TriTipActiveResponse:
    """Assemble the active-event read-model (contract shape) for GET /active."""
    event = get_active_event()
    if event is None:
        return TriTipActiveResponse(event=None, readings=[], prediction=None, references=[], blocked_by=None)

    readings = get_event_readings(event["tri_tip_id"])
    prior_events = _load_prior_references(exclude_event_id=event["tri_tip_id"])
    prediction = build_tri_tip_prediction(
        event=event,
        readings=readings,
        prior_events=prior_events,
        now=_now_utc(),
    )
    references = [
        TriTipReferenceEvent(event=_serialize_event(p["event"]), readings=_serialize_readings(p["readings"]))
        for p in prior_events
    ]
    return TriTipActiveResponse(
        event=_serialize_event(event),
        readings=_serialize_readings(readings),
        prediction=prediction,
        references=references,
        blocked_by=None,
    )


@router.get("")
async def list_events(limit: int = 100):
    """List tri-tip events, most recent first."""
    try:
        data = list_tri_tip_events(limit=limit)
        data = [_serialize_event(e) for e in data]
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tri-tip events: {str(e)}")


@router.get("/active")
async def get_active():
    """Get the current in-progress event with its readings, prediction, and references."""
    try:
        return _build_active_response()
    except TriTipError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch active tri-tip: {str(e)}")


@router.get("/{tri_tip_id}")
async def get_event(tri_tip_id: int):
    """Get a single event with its readings."""
    try:
        event = get_tri_tip_event(tri_tip_id)
        if event is None:
            raise HTTPException(status_code=404, detail="Tri-tip event not found")
        readings = get_event_readings(tri_tip_id)
        return {"event": _serialize_event(event), "readings": _serialize_readings(readings)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tri-tip event: {str(e)}")


@router.post("", status_code=201)
async def create_event(req: TriTipInitiateRequest):
    """Initiate a new tri-tip event (weight + shape)."""
    try:
        event = initiate_tri_tip(weight_lbs=req.weight_lbs, shape=req.shape)
        return _serialize_event(event)
    except TriTipActiveEventExistsError as e:
        blocking = _serialize_event(e.blocking_event)
        raise HTTPException(status_code=409, detail={"message": str(e), "blocked_by": blocking})
    except TriTipError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initiate tri-tip: {str(e)}")


@router.post("/{tri_tip_id}/place")
async def place_meat(tri_tip_id: int, req: TriTipPlaceRequest):
    """Place the meat: record the first reading @ 38F and activate the event."""
    try:
        event = place_tri_tip(tri_tip_id, grill_temp_f=req.grill_temp_f)
        return _serialize_event(event)
    except TriTipNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except TriTipStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except TriTipError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to place meat: {str(e)}")


@router.post("/{tri_tip_id}/readings", status_code=201)
async def add_reading(tri_tip_id: int, req: TriTipReadingRequest):
    """Record a reading for an active event."""
    try:
        reading = add_tri_tip_reading(
            tri_tip_id,
            grill_temp_f=req.grill_temp_f,
            internal_temp_f=req.internal_temp_f,
            note=req.note,
        )
        return _serialize_readings([reading])[0]
    except TriTipNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except TriTipStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except TriTipError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to record reading: {str(e)}")


@router.post("/{tri_tip_id}/complete")
async def complete(tri_tip_id: int):
    """Pull the meat: mark the event complete and set completed_at = MAX(recorded_at)."""
    try:
        event = complete_tri_tip(tri_tip_id)
        return _serialize_event(event)
    except TriTipNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except TriTipStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except TriTipError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to complete tri-tip: {str(e)}")


@router.delete("/{tri_tip_id}")
async def abandon(tri_tip_id: int):
    """Abandon this tri-tip and all recorded readings (FK cascade)."""
    try:
        abandon_tri_tip(tri_tip_id)
        return {"success": True}
    except TriTipNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except TriTipError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to abandon tri-tip: {str(e)}")