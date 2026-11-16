"""Public service functions for the music domain.

Extracted from router modules so that tests and other callers import from a
service layer rather than reaching into router private names (PY-005) or
calling DB functions directly from route handlers (PY-006).
"""

from backend_functions.database_functions import (
    one_sql_result,
    qec,
    sql_to_dict,
    sql_to_list,
)
from pifitness.integrations.spotify.provider import get_spotify_client
from pifitness.integrations.spotify.provider import check_rate_limit_cached


def get_spotify_client_for_music():
    """Return the spotify client or None (rate-limited / unauthenticated).

    Public extraction of the router-private ``_get_spotify_client()`` helper
    that lived in ``backend/api/music.py``.
    """
    rate_limited = bool(check_rate_limit_cached())
    if rate_limited:
        return None
    client = get_spotify_client(None)
    sp = client.get("client") if client else None
    return sp


def get_playlist_configs():
    """Return all playlist configuration records ordered by name.

    Public extraction of the inline ``sql_to_dict`` call that lived in the
    ``list_playlists`` route handler in ``backend/api/music.py``.
    """
    sql = "SELECT * FROM music.playlist_config ORDER BY playlist_name"
    return sql_to_dict(sql)


def get_rate_limit_cleared_utc():
    """Return the rate-limit cleared timestamp for Spotify, or None.

    Public extraction of the inline ``one_sql_result`` call that lived in the
    ``rate_limit`` route handler in ``backend/api/music.py``.
    """
    return one_sql_result(
        """SELECT rate_limit_cleared_utc::text
           FROM api_services.api_service_list
           WHERE api_service_name = 'Spotify'"""
    )


def remove_isrc_from_playlist(playlist_id: str, isrc: str) -> bool:
    """Remove an ISRC from a playlist in both local DB and recommendation exclusions.

    Public extraction of the inline ``qec`` DELETE/INSERT calls that lived in
    the ``remove_recommendation`` route handler in ``backend/api/music.py``.

    Returns True if any row was affected locally.
    """
    removed_any = False
    qec(
        "DELETE FROM music.playlist_isrcs WHERE playlist_id = %s AND isrc = %s",
        (playlist_id, isrc),
    )
    removed_any = True
    qec(
        """INSERT INTO music.playlist_recommendation_exclusions (playlist_id, isrc)
           VALUES (%s, %s)
           ON CONFLICT (playlist_id, isrc) DO NOTHING""",
        (playlist_id, isrc),
    )
    return removed_any


def get_track_ids_by_isrc(isrc: str) -> list:
    """Return distinct local track IDs for a given ISRC.

    Public extraction of the inline ``sql_to_list`` call that lived in the
    ``remove_recommendation`` route handler in ``backend/api/music.py``.
    """
    return sql_to_list(
        "SELECT DISTINCT track_id FROM music.all_tracks WHERE track_isrc = %s",
        (isrc,),
    )


def update_playlist_config(
    playlist_id: str,
    target_playlist_id: str,
    ratings_weight: float,
    recency_weight: float,
    randomness_weight: float,
    minutes_to_sync: int,
) -> None:
    """Persist shuffle configuration weights to ``music.playlist_config``.

    Public extraction of the inline ``qec`` UPDATE call that lived in the
    ``smart_shuffle`` route handler in ``backend/api/music.py``.
    """
    qec(
        "UPDATE music.playlist_config SET "
        "ratings_weight = %s, recency_weight = %s, randomness_weight = %s, "
        "minutes_to_sync = %s, last_auto_shuffled_utc = CURRENT_TIMESTAMP "
        "WHERE playlist_id IN (%s, %s)",
        (
            ratings_weight,
            recency_weight,
            randomness_weight,
            minutes_to_sync,
            playlist_id,
            target_playlist_id,
        ),
    )