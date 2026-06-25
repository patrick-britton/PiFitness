"""
Music Query Functions
======================

Database query functions extracted from frontend_functions/music_module.py.
These functions return plain Python data structures with no Streamlit dependencies.
"""

from typing import List, Dict, Any, Optional, Sequence, Union
from datetime import datetime
from backend_functions.database_functions import qec, sql_to_dict, sql_to_list, sql_to_lookup_dict, one_sql_result
from backend.schemas.music_schemas import Track, Playlist, TrackRecommendation

def get_rating_eligible_count() -> int:
    """
    Get the count of tracks eligible for rating.

    Returns:
        int: Number of tracks eligible for rating
    """
    sql = "SELECT COUNT(*) FROM music.vw_rating_eligible"
    return one_sql_result(sql) or 0

def get_isrc_dupe_count() -> int:
    """
    Get the count of potential duplicate ISRCs.

    Returns:
        int: Number of potential duplicate ISRC pairs
    """
    sql = "SELECT COUNT(*) FROM music.vw_isrc_dupe_review"
    return one_sql_result(sql) or 0

def get_isrc_dupe_match() -> Sequence[Dict[str, Any]]:
    """
    Get a single ISRC duplicate match for review.

    Returns:
        Sequence[Dict[str, Any]]: List containing one ISRC duplicate match record
    """
    sql = "SELECT * FROM music.vw_isrc_dupe_review LIMIT 1"
    result = sql_to_dict(sql)
    return result if result else []

def get_playlist_config(playlist_id: str) -> Sequence[Dict[str, Any]]:
    """
    Get playlist configuration by ID.

    Args:
        playlist_id (str): The playlist identifier

    Returns:
        Sequence[Dict[str, Any]]: Playlist configuration data with all 17 fields
    """
    sql = """SELECT
                playlist_id,
                playlist_name,
                playlist_description,
                track_count,
                last_verified_utc,
                is_active,
                auto_shuffle,
                last_auto_shuffled_utc,
                last_synced_utc,
                make_recs,
                manual_shuffle,
                minutes_to_sync,
                prior_track_count,
                randomness_weight,
                ratings_weight,
                recency_weight,
                seeds_only
            FROM music.playlist_config WHERE playlist_id = %s"""
    result = sql_to_dict(sql, (playlist_id,))
    return result if result else []

def get_playlist_isrc_stats(playlist_id: str) -> Sequence[Dict[str, Any]]:
    """
    Get ISRC statistics for a playlist.

    Args:
        playlist_id (str): The playlist identifier

    Returns:
        Sequence[Dict[str, Any]]: ISRC statistics for the playlist
    """
    sql = "SELECT * FROM music.vw_playlist_isrc_stats WHERE playlist_id = %s"
    result = sql_to_dict(sql, (playlist_id,))
    return result if result else []

def get_recent_plays(limit: int = 20) -> Sequence[Dict[str, Any]]:
    """
    Get recent play history.

    Args:
        limit (int): Maximum number of tracks to return

    Returns:
        Sequence[Dict[str, Any]]: Recent play history data
    """
    sql = """SELECT * FROM (SELECT * FROM music.vw_recent_plays) LIMIT %s"""
    result = sql_to_dict(sql, (limit,))
    return result if result else []

def get_rating_eligible_playlists() -> Dict[str, str]:
    """
    Get playlists that have tracks eligible for rating.

    Returns:
        Dict[str, str]: Dictionary mapping playlist names to playlist IDs
    """
    sql = """SELECT playlist_name, playlist_id FROM music.vw_rating_eligible_playlists"""
    return sql_to_lookup_dict(sql) or {}

def get_playlists_not_containing_isrc(isrc: str) -> Sequence[Dict[str, Any]]:
    """
    Get playlists that don't contain a specific ISRC.

    Args:
        isrc (str): The ISRC to check against playlists

    Returns:
        Sequence[Dict[str, Any]]: Playlists that don't contain the ISRC
    """
    sql = """SELECT
                pc.playlist_id,
                pc.playlist_name
            FROM music.playlist_config pc
            INNER JOIN (SELECT DISTINCT playlist_id,
                       MAX(CASE WHEN isrc = %s THEN 1 ELSE 0 END) as on_playlist
                        from music.playlist_isrcs
                        GROUP BY playlist_id
                        ) spd on spd.playlist_id = pc.playlist_id and on_playlist = 0
            WHERE pc.auto_shuffle or make_recs or manual_shuffle"""
    result = sql_to_dict(sql, (isrc,))
    return result if result else []

def process_isrc_dupe_acceptance(d: Dict[str, Any], accept_match: bool) -> Union[str, List[str]]:
    """
    Process ISRC duplicate acceptance or rejection.

    Args:
        d (Dict[str, Any]): ISRC duplicate data
        accept_match (bool): Whether to accept the match

    Returns:
        Union[str, List[str]]: Result message from the database operations
    """
    if accept_match:
        tn = 'isrc_swaps'
        conflict = 'original_isrc'
    else:
        tn = 'isrc_non_swaps'
        conflict = 'original_isrc, mapped_isrc'

    insert_sql = f"""INSERT INTO music.{tn} (
                    original_isrc,
                    mapped_isrc,
                    mapping_time_utc)
                    VALUES
                    (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT
                    ({conflict})
                    DO UPDATE SET
                    mapped_isrc = %s,
                    mapping_time_utc = CURRENT_TIMESTAMP;
                    """

    param1 = [d.get('isrc1'), d.get('preferred_isrc'), d.get('preferred_isrc')]
    param2 = [d.get('isrc2'), d.get('preferred_isrc'), d.get('preferred_isrc')]
    result1 = qec(insert_sql, param1)
    result2 = qec(insert_sql, param2)

    del_sql = """DELETE FROM music.isrc_possible_dupes
                WHERE (isrc1 = %s and isrc2 = %s)
                OR (isrc1 = %s and isrc2 = %s)"""
    params = [d.get('isrc1'), d.get('isrc2'), d.get('isrc2'), d.get('isrc1')]
    result3 = qec(del_sql, params)

    return f"Results: {result1}, {result2}, {result3}"

def add_isrc_to_local_playlist(playlist_id: str, isrc: str) -> Union[str, List[str], None]:
    """
    Add an ISRC to a local playlist.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC to add

    Returns:
        Union[str, List[str], None]: Result message or None
    """
    # Add to parent playlist locally
    ins_sql = """INSERT INTO music.playlist_isrcs (playlist_id, isrc) VALUES (%s, %s)"""
    params = (playlist_id, isrc)
    return qec(ins_sql, params)

def record_recommendation_decision(playlist_id: str, isrc: str, was_promoted: bool) -> Union[str, List[str]]:
    """
    Record a recommendation decision.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC of the track
        was_promoted (bool): Whether the track was promoted

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    # Record decision
    rec_sql = """
    SELECT
        playlist_id,
        isrc,
        elo_track_linear,
        elo_track_random_forest as elo_track_rf,
        elo_track_neural_net,
        elo_track_pairwise,
        elo_track_predicted,
        artist_elo as artist_elo_snap,
        genre_elo as genre_elo_snap,
        popularity as popularity_snap,
        %s as was_promoted,
        'second' as model_version,
        NULL as notes
        FROM music.track_recommendations
        WHERE playlist_id = %s and isrc = %s;
    """

    ins_sql = f"""INSERT INTO music.track_recommendations {rec_sql}"""
    return qec(ins_sql, [was_promoted, playlist_id, isrc])

def remove_recommendation(playlist_id: str, isrc: str) -> Union[str, List[str]]:
    """
    Remove a recommendation from a playlist.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC of the track to remove

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    del_sql = """DELETE FROM music.track_recommendations WHERE playlist_id = %s and isrc = %s;"""
    params = (playlist_id, isrc)
    return qec(del_sql, params)

def add_into_current_ratings(playlist_id: str, isrc: str, current_elo: float) -> Union[str, List[str]]:
    """
    Add a track to current ratings.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC of the track
        current_elo (float): The current ELO rating

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    rat_sql = """INSERT INTO music.ratings (playlist_id, isrc, elo_rating, rating_count, wins,
                 losses) VALUES (%s, %s, %s, %s, %s, %s)"""
    params = (playlist_id, isrc, current_elo, 0, 0, 0)
    return qec(rat_sql, params)

def update_playlist_config_weights(
    playlist_id: str,
    target_playlist_id: str,
    ratings_weight: int,
    recency_weight: int,
    randomness_weight: int,
    minutes_to_sync: int
) -> Union[str, List[str]]:
    """
    Update playlist configuration weights.

    Args:
        playlist_id (str): The original playlist identifier
        target_playlist_id (str): The target playlist identifier
        ratings_weight (int): Ratings weight
        recency_weight (int): Recency weight
        randomness_weight (int): Randomness weight
        minutes_to_sync (int): Minutes to sync

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    update_sql = """UPDATE music.playlist_config SET
                    ratings_weight = %s,
                    recency_weight = %s,
                    randomness_weight = %s,
                    minutes_to_sync = %s,
                    last_auto_shuffled_utc = CURRENT_TIMESTAMP
                    WHERE playlist_id in (%s, %s);"""
    return qec(update_sql, [ratings_weight, recency_weight, randomness_weight, minutes_to_sync, playlist_id, target_playlist_id])

def record_rating_history(
    playlist_id: str,
    isrc: str,
    isrc_vs: str,
    isrc_elo: float,
    isrc_vs_elo: float,
    home_new_elo: float,
    away_new_elo: float,
    margin: int
) -> Union[str, List[str]]:
    """
    Record rating history for a track comparison.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC of the home track
        isrc_vs (str): The ISRC of the away track
        isrc_elo (float): The ELO rating of the home track
        isrc_vs_elo (float): The ELO rating of the away track
        home_new_elo (float): The new ELO rating for the home track
        away_new_elo (float): The new ELO rating for the away track
        margin (int): The rating margin

    Returns:
        Union[str, List[str]]: Result message from the database operations
    """
    hist_sql = """INSERT INTO music.ratings_history (
                playlist_id,
                isrc,
                isrc_vs,
                elo_old,
                elo_new,
                rating_result)
                VALUES (%s, %s, %s, %s, %s, %s);
                """

    home_params = [playlist_id, isrc, isrc_vs, isrc_elo, home_new_elo, -margin]
    away_params = [playlist_id, isrc_vs, isrc, isrc_vs_elo, away_new_elo, margin]

    returns1 = qec(hist_sql, home_params)
    returns2 = qec(hist_sql, away_params)

    return f"History results: {returns1}, {returns2}"

def update_ratings_from_view() -> Union[str, List[str]]:
    """
    Update ratings using the rating update view.

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    update_sql = """INSERT INTO music.ratings (
                    playlist_id,
                    isrc,
                    elo_rating,
                    rating_count,
                    rating_wins,
                    rating_losses,
                    last_rated_utc
                    )
                    SELECT * FROM music.vw_rating_update
                    ON CONFLICT (playlist_id, isrc)
                    DO UPDATE SET
                    elo_rating = EXCLUDED.elo_rating,
                    rating_count = EXCLUDED.rating_count,
                    rating_wins = EXCLUDED.rating_wins,
                    rating_losses = EXCLUDED.rating_losses,
                    last_rated_utc = EXCLUDED.last_rated_utc
                    WHERE music.ratings.last_rated_utc < EXCLUDED.last_rated_utc;
                    """
    return qec(update_sql)

def add_soft_rejection_exclusion(
    playlist_id: str,
    isrc: str,
    current_elo: float
) -> Union[str, List[str]]:
    """
    Add a soft rejection exclusion for a track.

    Args:
        playlist_id (str): The playlist identifier
        isrc (str): The ISRC of the track
        current_elo (float): The current ELO rating

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    soft_sql = """INSERT INTO music.playlist_recommendation_exclusions (playlist_id, isrc, elo_track_predicted)
                VALUES (%s, %s, %s)"""
    params = (playlist_id, isrc, current_elo)
    return qec(soft_sql, params)

# Additional music query functions can be added here as needed
# For example:
# def get_playlist_tracks(playlist_id: str) -> List[Track]:
#     """Get all tracks for a specific playlist"""
#     pass
#
# def get_track_recommendations(playlist_id: str) -> List[TrackRecommendation]:
#     """Get track recommendations for a playlist"""
#     pass