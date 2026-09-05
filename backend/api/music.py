"""
Music API Endpoints
====================

FastAPI endpoints for music data (playlists, ratings, shuffle, playback).

Note: The shuffle endpoint still requires further extraction
from frontend_functions/ into backend functions. It is marked as
PENDING and will be added when the backend extraction is complete.
Now-playing is served by backend_functions.music_functions.resolve_now_playing
(feature 008-001).
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pathlib import Path
from typing import Optional

from backend_functions.database_functions import sql_to_dict, sql_to_list, one_sql_result, qec
from backend_functions.music_functions import (
    resolve_now_playing,
    save_matchup_results,
    album_image_retrieval,
    playlist_upload,
    playlist_to_db,
)
from backend_functions.service_logins import get_spotify_client, check_rate_limit_cached, sql_rate_limited
from backend_functions.queries import (
    add_isrc_to_local_playlist,
    add_into_current_ratings,
    add_soft_rejection_exclusion,
    get_playlist_config,
    get_playlist_config_view,
    get_playlist_isrc_stats,
    get_playlists_not_containing_isrc,
    get_recent_plays,
    get_rating_eligible_count,
    get_rating_eligible_playlists,
    record_recommendation_decision,
    remove_recommendation,
    get_matchup,
    score_matchup,
    compute_shuffle_order,
)
import time as _time
from pydantic import BaseModel, Field

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
        # Get all playlists from playlist_config table
        from backend_functions.database_functions import sql_to_dict
        sql = "SELECT * FROM music.playlist_config ORDER BY playlist_name"
        playlists = sql_to_dict(sql)
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
# Ratings — Matchup (008-004)
# ---------------------------------------------------------------------------


@router.get("/ratings/matchup")
async def get_matchup_endpoint(
    playlist_id: Optional[str] = Query(default=None, description="Optional playlist to scope the matchup"),
):
    """
    Get a head-to-head matchup for rating (FR-3/FR-5).

    Returns the primary track plus the challenger (closest score in the same
    playlist). When the playlist has only one rateable track, challenger is
    null. Returns 204 when no rateable tracks exist.
    """
    try:
        matchup = get_matchup(playlist_id=playlist_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get matchup: {str(e)}",
        )

    if matchup is None:
        return {"ok": True, "primary": None, "challenger": None}

    return {"ok": True, **matchup}


@router.post("/ratings/matchup/score")
async def score_matchup_endpoint(
    playlist_id: str,
    isrc: str,
    isrc_vs: str,
    margin: int = Query(ge=-5, le=5, description="Rating margin -5..-1 or +1..+5 (no zero)"),
):
    """
    Score a matchup and return the next one (FR-6/FR-7).

    Validates that margin is non-zero, recomputes both scores, writes history,
    updates standings, and returns the next matchup.
    """
    if margin == 0:
        raise HTTPException(
            status_code=422,
            detail="Margin cannot be zero — no draws allowed",
        )

    try:
        result = score_matchup(
            playlist_id=playlist_id,
            isrc=isrc,
            isrc_vs=isrc_vs,
            margin=margin,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to score matchup: {str(e)}",
        )

    # Load next matchup (same playlist scope as the one just scored)
    try:
        next_matchup = get_matchup(playlist_id=playlist_id)
    except Exception:
        next_matchup = None

    return {"ok": True, "next": next_matchup, "scores": result}


# ---------------------------------------------------------------------------
# Recent Plays
# ---------------------------------------------------------------------------


@router.get("/recent-plays")
async def list_recent_plays(
    limit: int = Query(default=20, ge=10, le=100, multiple_of=10),
):
    """
    Get recent play history with per-set scale anchors (FR-7).

    Args:
        limit: Row count (10-100, step 10, default 20)

    Returns:
        {plays, scale} per RecentPlaysResponse contract
    """
    try:
        rows = get_recent_plays(limit=limit)
        if not rows:
            return {
                "plays": [],
                "scale": {
                    "minRating": 1500,
                    "maxRating": 1500,
                    "maxPlaycountLast30": 0,
                    "maxPlaycountTotal": 0,
                },
            }

        plays = []
        for r in rows:
            plays.append({
                "isrc": r["isrc"],
                "lastPlayedAtUtc": r["lastPlayedAtUtc"],
                "trackName": r["trackName"],
                "artistName": r["artistName"],
                "playlistName": r.get("playlistName"),
                "rating": r["rating"],
                "playcountLast30": r["playcountLast30"],
                "playcountTotal": r["playcountTotal"],
            })

        scale = {
            "minRating": rows[0]["minRating"],
            "maxRating": rows[0]["maxRating"],
            "maxPlaycountLast30": rows[0]["maxPlaycountLast30"],
            "maxPlaycountTotal": rows[0]["maxPlaycountTotal"],
        }
        return {"plays": plays, "scale": scale}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch recent plays: {str(e)}",
        )


@router.get("/service-status")
async def service_status():
    """
    Report Spotify service status (FR-9/AC-12).

    Returns:
        {spotify: {rateLimited, rateLimitClearedUtc}}
    """
    try:
        rate_limited = bool(sql_rate_limited())
        cleared_utc = None
        if rate_limited:
            cleared_utc = one_sql_result(
                """SELECT rate_limit_cleared_utc::text
                   FROM api_services.api_service_list
                   WHERE api_service_name = 'Spotify'"""
            )
        return {
            "spotify": {
                "rateLimited": rate_limited,
                "rateLimitClearedUtc": cleared_utc,
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch service status: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Now Playing
# ---------------------------------------------------------------------------


@router.get("/now-playing")
async def get_now_playing():
    """
    Get current Spotify playback state (008-001 contract).

    Persists the raw poll to staging (FR-1/AC-9), then resolves track,
    playlist-family context, and rating. A rate-limited service loads no
    content (AC-12); anything unresolvable returns the idle response (AC-8).

    Returns:
        { playing, rateLimited, track: NowPlayingTrack | null, refreshedAt }
    """
    try:
        return resolve_now_playing()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch now playing: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Now Playing actions
# ---------------------------------------------------------------------------


def _get_spotify_client():
    """Return the spotify client or None (rate-limited / unauthenticated)."""
    rate_limited = bool(check_rate_limit_cached())
    if rate_limited:
        return None
    client = get_spotify_client(None)
    sp = client.get("client") if client else None
    return sp


@router.post("/now-playing/skip")
async def now_playing_skip():
    """
    Advance Spotify playback to the next track (FR-6).

    The view re-fetches on the next GET /now-playing poll.
    """
    sp = _get_spotify_client()
    if sp is None:
        raise HTTPException(status_code=503, detail="Spotify not available")
    try:
        sp.next_track()
        return {"ok": True, "message": "Skipped to next track"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to skip: {str(e)}")


@router.post("/now-playing/promote")
async def now_playing_promote():
    """
    Promote the current recommendation to its parent playlist (FR-6/AC-5).

    Adds the track locally and on Spotify, records the promotion decision,
    removes it from the recommendation set, and seeds a rating.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "recommendation":
        return {"ok": False, "message": "Promote only applies to recommendations"}

    isrc = track["isrc"]
    parent_id = ctx.get("parentPlaylistId")
    if not parent_id:
        return {"ok": False, "message": "No parent playlist found"}

    track_id = track["trackId"]
    track_name = track["trackName"]
    current_elo = track.get("rating", {}).get("value", 1500)

    try:
        add_isrc_to_local_playlist(parent_id, isrc)
    except Exception:
        pass

    sp = _get_spotify_client()
    if sp:
        try:
            sp.playlist_add_items(parent_id, [track_id])
        except Exception:
            pass

    record_recommendation_decision(parent_id, isrc, was_promoted=True)
    remove_recommendation(parent_id, isrc)

    try:
        add_into_current_ratings(parent_id, isrc, current_elo)
    except Exception:
        pass

    return {"ok": True, "message": f"{track_name} added to playlist"}


@router.post("/now-playing/soft-reject")
async def now_playing_soft_reject():
    """
    Soft-reject the current recommendation (FR-6/AC-5).

    Writes a recommendation exclusion carrying the predicted rating and
    removes the track from the recommendation set.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "recommendation":
        return {"ok": False, "message": "Soft Reject only applies to recommendations"}

    isrc = track["isrc"]
    parent_id = ctx.get("parentPlaylistId")
    if not parent_id:
        return {"ok": False, "message": "No parent playlist found"}

    predicted_elo = track.get("rating", {}).get("value", 1500)

    add_soft_rejection_exclusion(parent_id, isrc, predicted_elo)
    remove_recommendation(parent_id, isrc)

    return {"ok": True, "message": f"{track['trackName']} excluded from future recommendations"}


@router.post("/now-playing/hard-reject")
async def now_playing_hard_reject():
    """
    Hard-reject the current recommendation (FR-6/AC-5).

    Records the rejection decision, removes the track from the recommendation
    set, and advances to the next track.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "recommendation":
        return {"ok": False, "message": "Hard Reject only applies to recommendations"}

    isrc = track["isrc"]
    parent_id = ctx.get("parentPlaylistId")
    if not parent_id:
        return {"ok": False, "message": "No parent playlist found"}

    track_name = track["trackName"]

    record_recommendation_decision(parent_id, isrc, was_promoted=False)
    remove_recommendation(parent_id, isrc)

    sp = _get_spotify_client()
    if sp:
        try:
            sp.next_track()
        except Exception:
            pass

    return {"ok": True, "message": f"{track_name} rejected from recommendations"}


@router.post("/now-playing/remove")
async def now_playing_remove():
    """
    Remove the current track from its playlist family (FR-6/AC-7, OQ-4).

    Implements the resolved OQ-4 guard contract:
    - Family miss -> "the playlist cannot be found"
    - Per-playlist track miss -> "<track name> not found on <playlist name>" (soft, continue)
    - Full success -> confirm removal
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "regular":
        return {"ok": False, "message": "Remove only applies to playlist tracks"}

    isrc = track["isrc"]
    playlist_id = ctx.get("playlistId")
    if not playlist_id:
        return {"ok": False, "message": "No playlist context"}

    track_name = track["trackName"]

    # Look up the family (OQ-4 guard)
    family_rows = sql_to_dict(
        """SELECT DISTINCT child_playlist_id
           FROM music.vw_playlist_families
           WHERE playlist_id = %s""",
        (playlist_id,),
    )
    if not family_rows:
        return {"ok": False, "message": "the playlist cannot be found"}

    # Get playlist names for error messages
    family_ids = [r["child_playlist_id"] for r in family_rows]
    names = sql_to_dict(
        """SELECT playlist_id, playlist_name
           FROM music.playlist_config
           WHERE playlist_id = ANY(%s)""",
        (family_ids,),
    )
    name_map = {r["playlist_id"]: r["playlist_name"] for r in names}

    messages = []
    removed_any = False
    # Collect family playlists where the track was found locally so we can
    # also remove from each on Spotify (FR-6: "locally and on Spotify").
    spotify_playlist_ids = []

    for fid in family_ids:
        exists = one_sql_result(
            "SELECT 1 FROM music.playlist_isrcs WHERE playlist_id = %s AND isrc = %s",
            (fid, isrc),
        )
        if not exists:
            pname = name_map.get(fid, fid)
            messages.append(f"{track_name} not found on {pname}")
            continue

        qec(
            "DELETE FROM music.playlist_isrcs WHERE playlist_id = %s AND isrc = %s",
            (fid, isrc),
        )
        qec(
            """INSERT INTO music.playlist_recommendation_exclusions (playlist_id, isrc)
               VALUES (%s, %s)
               ON CONFLICT (playlist_id, isrc) DO NOTHING""",
            (fid, isrc),
        )
        removed_any = True
        spotify_playlist_ids.append(fid)

    # Spotify removal for every family playlist where the track was found locally
    # (FR-6/AC-7: "every playlist in the current family locally and on Spotify").
    track_list = sql_to_list(
        "SELECT DISTINCT track_id FROM music.all_tracks WHERE track_isrc = %s",
        (isrc,),
    )
    if track_list:
        sp = _get_spotify_client()
        if sp:
            for sid in spotify_playlist_ids:
                try:
                    sp.playlist_remove_all_occurrences_of_items(sid, track_list)
                except Exception:
                    pass

    if not removed_any:
        return {"ok": True, "message": "; ".join(messages) if messages else "No changes made"}

    confirmation = f"{track_name} removed from playlist"
    if messages:
        confirmation += " (" + "; ".join(messages) + ")"
    return {"ok": True, "message": confirmation}


@router.post("/now-playing/rank-up")
async def now_playing_rank_up():
    """
    Bump up the current track's rating against a straw man (FR-6/AC-7).

    Records the matchup in ratings_history and merges the new rating.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "regular":
        return {"ok": False, "message": "Rank Up only applies to playlist tracks"}

    isrc = track["isrc"]
    playlist_id = ctx.get("playlistId")
    if not playlist_id:
        return {"ok": False, "message": "No playlist context"}

    current_elo = track.get("rating", {}).get("value", 1500)
    track_name = track["trackName"]

    save_matchup_results(
        hd={"isrc": isrc, "playlistId": playlist_id, "currentELO": current_elo},
        ad=None,
        mr=2,
    )

    return {"ok": True, "message": f"{track_name} ranked up"}


@router.post("/now-playing/rank-down")
async def now_playing_rank_down():
    """
    Bump down the current track's rating against a straw man (FR-6/AC-7).

    Records the matchup in ratings_history and merges the new rating.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    ctx = track.get("context", {})
    if ctx.get("relationshipType") != "regular":
        return {"ok": False, "message": "Rank Down only applies to playlist tracks"}

    isrc = track["isrc"]
    playlist_id = ctx.get("playlistId")
    if not playlist_id:
        return {"ok": False, "message": "No playlist context"}

    current_elo = track.get("rating", {}).get("value", 1500)
    track_name = track["trackName"]

    save_matchup_results(
        hd={"isrc": isrc, "playlistId": playlist_id, "currentELO": current_elo},
        ad=None,
        mr=-2,
    )

    return {"ok": True, "message": f"{track_name} ranked down"}


@router.post("/now-playing/add-to-playlist")
async def now_playing_add_to_playlist(playlist_id: str):
    """
    Add the current track to the specified playlist (FR-6/AC-6).

    Adds locally and on Spotify.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"ok": False, "message": "No track is playing"}

    track = state["track"]
    isrc = track["isrc"]
    track_id = track["trackId"]
    track_name = track["trackName"]

    try:
        add_isrc_to_local_playlist(playlist_id, isrc)
    except Exception:
        pass

    sp = _get_spotify_client()
    if sp:
        try:
            sp.playlist_add_items(playlist_id, [track_id])
        except Exception:
            pass

    return {"ok": True, "message": f"{track_name} added to playlist"}


@router.get("/now-playing/add-targets")
async def now_playing_add_targets():
    """
    List playlists eligible to add the current track to (FR-6/AC-6).

    Returns playlists that are configured (auto-shuffle, recommendations, or
    manual-shuffle) and don't already contain the track.
    """
    state = resolve_now_playing()
    if not state.get("playing") or not state.get("track"):
        return {"playlists": [], "eligible": False}

    isrc = state["track"]["isrc"]
    playlists = get_playlists_not_containing_isrc(isrc)

    return {
        "playlists": [
            {"playlist_id": p["playlist_id"], "playlist_name": p["playlist_name"]}
            for p in playlists
        ],
        "eligible": len(playlists) > 0,
    }


@router.get("/album-art/{album_id}")
async def get_album_art(album_id: str):
    """
    Serve album art from the local cache, downloading on first need (FR-1/OQ-1).

    Resolves the JPEG path via the ported ``album_image_retrieval`` helper.
    The response is a ``FileResponse``; if no image can be obtained a 404 is
    returned.
    """
    filepath = album_image_retrieval(album_id)
    if filepath is None:
        raise HTTPException(status_code=404, detail=f"Album art not found for {album_id}")
    return FileResponse(filepath, media_type="image/jpeg")


# ---------------------------------------------------------------------------
# Playlist Shuffle (008-003) — manual, one-shot weighted shuffle
# ---------------------------------------------------------------------------


class ShuffleConfigBody(BaseModel):
    """Weights/minutes for a preview or config reconcile (FR-3/FR-6, OQ-1)."""
    ratingsWeight: int = Field(ge=0, le=50, description="Ratings weight 0-50")
    recencyWeight: int = Field(ge=0, le=50, description="Recency weight 0-50")
    randomWeight: int = Field(ge=0, le=50, description="Randomness weight 0-50")
    minutesToSync: int = Field(ge=30, le=9999, description="Minutes 30-9999 (9999 = no limit)")


class ShuffleFlagsBody(BaseModel):
    """Boolean playlist-config flags for a checkbox reconcile (008-003)."""
    autoShuffle: bool = False
    manualShuffle: bool = False
    makeRecs: bool = False
    seedsOnly: bool = False


class ShuffleSendRequest(BaseModel):
    """Send-to-Spotify request body (FR-5/FR-6, OQ-1)."""
    playlistId: str = Field(..., description="Source playlist id")
    ratingsWeight: int = Field(ge=0, le=50)
    recencyWeight: int = Field(ge=0, le=50)
    randomWeight: int = Field(ge=0, le=50)
    minutesToSync: int = Field(ge=30, le=9999)


@router.get("/shuffle/playlists")
async def shuffle_playlists():
    """
    Selection grid of parent (child-excluded) playlists for Playlist Shuffle (FR-1).

    Reads `music.vw_playlist_config`. Each row carries the fields needed by the
    selection grid plus the saved tuning defaults. Returns `{data, count}`.
    """
    try:
        playlists = get_playlist_config_view()
        return {"data": playlists, "count": len(playlists)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch shuffle playlists: {str(e)}",
        )


@router.get("/shuffle")
async def shuffle_data(playlist_id: str):
    """
    Playlist shuffle data for a selected playlist (FR-1/FR-2).

    Returns the source playlist's saved config (weights/minutes), the resolved
    target (shuffle-child) playlist id, and the raw per-track stats rows needed
    to compute the preview. When the playlist has no stats rows, `rows` is empty
    and `targetPlaylistId` is null (OQ-3).
    """
    try:
        stats = get_playlist_isrc_stats(playlist_id)
        config_rows = get_playlist_config(playlist_id)
        target_playlist_id = stats[0].get("target_playlist_id") if stats else None

        cfg = config_rows[0] if config_rows else {}
        config = {
            "ratingsWeight": cfg.get("ratings_weight", 0),
            "recencyWeight": cfg.get("recency_weight", 0),
            "randomWeight": cfg.get("randomness_weight", 0),
            "minutesToSync": cfg.get("minutes_to_sync", 9999),
            "autoShuffle": bool(cfg.get("auto_shuffle", False)),
            "manualShuffle": bool(cfg.get("manual_shuffle", False)),
            "makeRecs": bool(cfg.get("make_recs", False)),
            "seedsOnly": bool(cfg.get("seeds_only", False)),
        }
        return {
            "playlistId": playlist_id,
            "targetPlaylistId": target_playlist_id,
            "config": config,
            "rows": stats,
            "count": len(stats),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch shuffle data: {str(e)}",
        )


@router.post("/shuffle/preview")
async def shuffle_preview(
    body: ShuffleConfigBody,
    playlist_id: str,
):
    """
    Compute the weighted shuffle order for the live preview (FR-3/FR-4).

    Delegates to ``compute_shuffle_order``, the single source of truth shared
    with ``/shuffle/send``. When the playlist has no rows, returns an empty
    ``rows`` list (the frontend shows 'No Songs found on this playlist', OQ-3).
    """
    try:
        result = compute_shuffle_order(
            playlist_id,
            body.ratingsWeight,
            body.recencyWeight,
            body.randomWeight,
            body.minutesToSync,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute shuffle preview: {str(e)}",
        )

    rows = [
        {
            "newPosition": r.get("new_position"),
            "trackArtist": r.get("track_artist"),
            "recency_pct": r.get("recency_pct"),
            "ratings_pct": r.get("ratings_pct"),
            "random_pct": r.get("random_pct"),
            "duration_s": r.get("duration_s"),
            "durationBarMax": result.get("max_duration_s", 0),
            "isrc": r.get("isrc"),
            "trackId": r.get("track_id"),
            "targetPlaylistId": result.get("target_playlist_id"),
        }
        for r in result.get("rows", [])
    ]
    return {"rows": rows, "count": len(rows)}


@router.post("/shuffle/config")
async def shuffle_config(
    body: ShuffleConfigBody,
    playlist_id: str,
):
    """
    Reconcile tuning inputs to the source playlist config immediately (OQ-4 / AC-8).

    Writes the three weights and minutes to ``music.playlist_config`` on each
    input change so the saved config stays in sync even if the user never sends.
    Returns ``{ok, message}``.
    """
    try:
        qec(
            "UPDATE music.playlist_config SET "
            "ratings_weight = %s, recency_weight = %s, randomness_weight = %s, "
            "minutes_to_sync = %s WHERE playlist_id = %s",
            (
                body.ratingsWeight,
                body.recencyWeight,
                body.randomWeight,
                body.minutesToSync,
                playlist_id,
            ),
        )
        return {"ok": True, "message": "Playlist config updated"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reconcile playlist config: {str(e)}",
        )


@router.post("/shuffle/flags")
async def shuffle_flags(
    body: ShuffleFlagsBody,
    playlist_id: str,
):
    """
    Reconcile boolean playlist flags to the source config immediately.

    Writes auto_shuffle, manual_shuffle, make_recs, and seeds_only to
    ``music.playlist_config`` on each checkbox change so the saved config stays
    in sync. Returns ``{ok, message}``.
    """
    try:
        qec(
            "UPDATE music.playlist_config SET "
            "auto_shuffle = %s, manual_shuffle = %s, make_recs = %s, seeds_only = %s "
            "WHERE playlist_id = %s",
            (
                body.autoShuffle,
                body.manualShuffle,
                body.makeRecs,
                body.seedsOnly,
                playlist_id,
            ),
        )
        return {"ok": True, "message": "Playlist flags updated"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reconcile playlist flags: {str(e)}",
        )


@router.post("/shuffle/send")
async def shuffle_send(body: ShuffleSendRequest):
    """
    Push the shuffled order to the target Spotify playlist (FR-5/FR-6/FR-7).

    Recomputes the order via ``compute_shuffle_order``, uploads it to the
    shuffle-child target via ``playlist_upload`` (atomic replace first 100 +
    batched appends), waits, re-runs the playlist-detail re-sync via
    ``playlist_to_db``, and persists the weights/minutes/last-auto-shuffled
    timestamp to config for BOTH the source and target playlist ids.
    Returns ``{ok: False}`` when there are no rows (OQ-3) or no client.
    """
    try:
        result = compute_shuffle_order(
            body.playlistId,
            body.ratingsWeight,
            body.recencyWeight,
            body.randomWeight,
            body.minutesToSync,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute shuffle order: {str(e)}",
        )

    rows = result.get("rows", [])
    target_playlist_id = result.get("target_playlist_id")
    if not rows:
        return {"ok": False, "message": "No Songs found on this playlist"}
    if not target_playlist_id:
        return {"ok": False, "message": "No target playlist for this source"}

    track_list = [r["track_id"] for r in rows]

    sp = _get_spotify_client()
    if sp is None:
        return {"ok": False, "message": "Spotify client unavailable or rate-limited"}

    client = {"client": sp}
    try:
        playlist_upload(client=client, list_id=target_playlist_id, track_list=track_list)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Spotify playlist upload failed: {str(e)}",
        )

    _time.sleep(2)

    try:
        playlist_to_db(client=client, list_id=target_playlist_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Playlist detail sync failed: {str(e)}",
        )

    try:
        qec(
            "UPDATE music.playlist_config SET "
            "ratings_weight = %s, recency_weight = %s, randomness_weight = %s, "
            "minutes_to_sync = %s, last_auto_shuffled_utc = CURRENT_TIMESTAMP "
            "WHERE playlist_id IN (%s, %s)",
            (
                body.ratingsWeight,
                body.recencyWeight,
                body.randomWeight,
                body.minutesToSync,
                body.playlistId,
                target_playlist_id,
            ),
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to persist shuffle config: {str(e)}",
        )

    return {"ok": True, "message": f"Playlist shuffled ({len(track_list)} tracks)"}


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