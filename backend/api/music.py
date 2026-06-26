"""
Music API Endpoints
====================

FastAPI endpoints for music data (playlists, ratings, shuffle, playback).

Note: Some endpoints (now-playing, shuffle) require further extraction
from frontend_functions/ into backend functions. Those are marked as
PENDING and will be added when the backend extraction is complete.
"""

from fastapi import APIRouter, HTTPException
from typing import Optional

from backend_functions.queries import (
    get_playlist_config,
    get_playlist_isrc_stats,
    get_recent_plays,
    get_rating_eligible_playlists,
    get_rating_eligible_count,
    record_recommendation_decision,
    get_playlists_not_containing_isrc,
)

router = APIRouter(prefix="/api/music", tags=["music"])


# ---------------------------------------------------------------------------
# Playlists
# ---------------------------------------------------------------------------


@router.get("/playlists")
async def list_playlists():
    """
    Get all playlists with ELO configuration.

    Returns:
        List of playlist configuration records
    """
    try:
        playlists = get_playlist_config()
        return {"data": playlists, "count": len(playlists)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch playlists: {str(e)}",
        )


@router.get("/playlists/{playlist_id}/tracks")
async def get_playlist_tracks(playlist_id: str):
    """
    Get ISRC stats for tracks in a specific playlist.

    Args:
        playlist_id: The Spotify playlist ID

    Returns:
        List of track ISRC stats for the playlist
    """
    try:
        tracks = get_playlist_isrc_stats(playlist_id)
        return {"data": tracks, "count": len(tracks)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch playlist tracks: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Ratings / ELO
# ---------------------------------------------------------------------------


@router.get("/ratings")
async def list_rating_eligible_playlists():
    """
    Get playlists eligible for rating.

    Returns:
        List of playlists available for ELO rating
    """
    try:
        playlists = get_rating_eligible_playlists()
        return {"data": playlists, "count": len(playlists)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch rating-eligible playlists: {str(e)}",
        )


@router.get("/ratings/eligible-count")
async def get_unrated_count():
    """
    Get count of tracks eligible for rating.

    Returns:
        Count of unrated tracks
    """
    try:
        count = get_rating_eligible_count()
        return {"count": count}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get eligible count: {str(e)}",
        )


@router.post("/ratings")
async def record_rating(
    playlist_id: str,
    isrc: str,
    was_promoted: bool,
):
    """
    Record a recommendation decision for a track.

    Args:
        playlist_id: The playlist ID
        isrc: The track ISRC
        was_promoted: Whether the track was promoted (liked)
    """
    try:
        result = record_recommendation_decision(
            playlist_id=playlist_id,
            isrc=isrc,
            was_promoted=was_promoted,
        )
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to record rating: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Recent Plays
# ---------------------------------------------------------------------------


@router.get("/recent-plays")
async def list_recent_plays(limit: Optional[int] = 20):
    """
    Get recent play history.

    Args:
        limit: Maximum number of recent plays to return

    Returns:
        List of recent play records
    """
    try:
        plays = get_recent_plays(limit=limit or 20)
        return {"data": plays, "count": len(plays)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch recent plays: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Now Playing — PENDING: requires backend extraction from frontend_functions
# ---------------------------------------------------------------------------


@router.get("/now-playing")
async def get_now_playing():
    """
    Get current Spotify playback state.

    PENDING: This endpoint currently returns a stub. The get_current_playback()
    function needs to be extracted from frontend_functions/music_module.py into
    backend_functions/music_functions.py before this can return real data.

    Returns:
        Stub response indicating feature is pending
    """
    return {"data": None, "status": "pending", "message": "Now playing endpoint requires further backend extraction"}


# ---------------------------------------------------------------------------
# Smart Shuffle — PENDING: requires backend extraction from frontend_functions
# ---------------------------------------------------------------------------


@router.post("/shuffle")
async def smart_shuffle(
    playlist_id: str,
    limit: Optional[int] = 30,
):
    """
    Generate a smart shuffle for a playlist.

    PENDING: The smart_shuffle() function needs to be extracted from
    frontend_functions/music_module.py into backend_functions/music_functions.py
    before this endpoint can return real data.

    Args:
        playlist_id: The playlist ID to shuffle
        limit: Number of tracks to include

    Returns:
        Stub response indicating feature is pending
    """
    return {
        "data": None,
        "status": "pending",
        "message": "Smart shuffle endpoint requires further backend extraction",
        "playlist_id": playlist_id,
        "limit": limit or 30,
    }