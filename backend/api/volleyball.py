"""
Volleyball Scorekeeping API Endpoints
=====================================

FastAPI endpoints for the Beach Volleyball scorekeeping feature
(volleyball.games / volleyball.points).

Endpoints (contract from .features/designs_active/006-001_design.md):
    GET    /api/sports/volleyball                      -> game history (completed_at desc, w/ scores)
    GET    /api/sports/volleyball/active               -> active game + points + derived score, or game: null
    POST   /api/sports/volleyball                      -> create game (opponent name only); 409 if one is active
    POST   /api/sports/volleyball/{id}/points          -> add a point for a team
    DELETE /api/sports/volleyball/{id}/points/{team}   -> remove that team's most recent point
    POST   /api/sports/volleyball/{id}/end             -> end game (completed_at = MAX(recorded_at))
    DELETE /api/sports/volleyball/{id}                 -> abandon game (cascade deletes its points)
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException

from backend_functions.queries import (
    get_game_detail,
    get_active_game,
    list_game_history,
    create_game,
    add_point,
    remove_last_point,
    end_game,
    abandon_game,
    VolleyballActiveGameExistsError,
    VolleyballNotFoundError,
    VolleyballStateError,
    VolleyballError,
)
from backend.schemas.volleyball_schemas import (
    VolleyballCreateGameRequest,
    VolleyballAddPointRequest,
)

router = APIRouter(prefix="/api/sports/volleyball", tags=["volleyball"])


def _serialize_game(game: dict) -> dict:
    """Normalize game datetime fields to ISO strings for JSON serialization."""
    if game is None:
        return None
    out = dict(game)
    for field in ("started_at", "completed_at", "created_at"):
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


def _serialize_point(point: dict) -> dict:
    """Normalize a point record's timestamps to ISO strings."""
    out = dict(point)
    for field in ("recorded_at", "created_at"):
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


def _serialize_detail(detail: dict) -> dict:
    """Shape a game detail (game + points + score) to the contract."""
    if detail is None:
        return None
    return {
        "game": _serialize_game(detail["game"]),
        "points": [_serialize_point(p) for p in detail["points"]],
        "score": detail["score"],
    }


@router.get("")
async def history():
    """Game history: completed games with final derived scores, completed_at desc."""
    try:
        return {"games": list_game_history()}
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch volleyball history: {str(e)}")


@router.get("/active")
async def active():
    """Current active game with points and derived score, or game: null."""
    try:
        game = get_active_game()
        if game is None:
            return {"game": None}
        detail = get_game_detail(game["game_id"])
        return {"game": _serialize_detail(detail)}
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch active volleyball game: {str(e)}")


@router.post("", status_code=201)
async def create(req: VolleyballCreateGameRequest):
    """Create a new game (opponent name only). 409 when one is already active."""
    try:
        game = create_game(team_b_name=req.team_b_name)
        return _serialize_game(game)
    except VolleyballActiveGameExistsError as e:
        blocking = _serialize_game(e.blocking_game)
        raise HTTPException(status_code=409, detail={"message": str(e), "blocked_by": blocking})
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create volleyball game: {str(e)}")


@router.post("/{game_id}/points", status_code=201)
async def add(game_id: int, req: VolleyballAddPointRequest):
    """Record one point for a team on the active game."""
    try:
        point = add_point(game_id, scoring_team=req.scoring_team)
        return _serialize_point(point)
    except VolleyballNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except VolleyballStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add point: {str(e)}")


@router.delete("/{game_id}/points/{scoring_team}")
async def remove(game_id: int, scoring_team: str):
    """Remove the most recent point recorded by ONE team (per-team undo)."""
    try:
        remove_last_point(game_id, scoring_team)
        return {"success": True}
    except VolleyballNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except VolleyballStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to remove point: {str(e)}")


@router.post("/{game_id}/end")
async def end(game_id: int):
    """End the game: status='completed', completed_at = MAX(recorded_at)."""
    try:
        game = end_game(game_id)
        return _serialize_game(game)
    except VolleyballNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except VolleyballStateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to end volleyball game: {str(e)}")


@router.delete("/{game_id}")
async def abandon(game_id: int):
    """Abandon the game; its points are removed by FK cascade."""
    try:
        abandon_game(game_id)
        return {"success": True}
    except VolleyballNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except VolleyballError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to abandon volleyball game: {str(e)}")
