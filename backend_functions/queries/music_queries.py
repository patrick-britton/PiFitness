"""
Music Query Functions
======================

Database query functions extracted from frontend_functions/music_module.py.
These functions return plain Python data structures with no Streamlit dependencies.
"""

from typing import List, Dict, Any, Optional, Sequence, Union
from datetime import datetime
from backend_functions.database_functions import qec, sql_to_dict, sql_to_list, sql_to_lookup_dict, one_sql_result
from backend_functions.music_functions import elo_update
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
    Get recent play history with per-set scale anchors (feature 008-001, FR-7).

    Reads the canonical sources behind music.vw_recent_plays directly so each
    row can also carry `isrc` (the view does not project it) and so the scale
    anchors are computed over the LIMITED result set, not the whole view.
    No schema change (AC-11): read-only, no DDL.

    Each row dict carries the contract columns
    (isrc, lastPlayedAtUtc, trackName, artistName, playlistName, rating,
    playcountLast30, playcountTotal) plus the per-set anchors (minRating,
    maxRating, maxPlaycountLast30, maxPlaycountTotal) repeated on every row;
    the API layer (T05) splits them into {plays, scale}. `rating` is the
    ELO (1500 baseline); the UI renders the rating bar on the fixed
    1300–1700 range rather than these per-set anchors.

    Args:
        limit (int): Maximum number of tracks to return (newest first)

    Returns:
        Sequence[Dict[str, Any]]: Recent play rows with scale anchors, or []
    """
    sql = """WITH ranked_plays AS (
                 SELECT ps.isrc,
                        ps.last_played_at_utc,
                        ps.playcount_last_30,
                        ps.playcount_total
                 FROM (
                     SELECT lh.isrc,
                            MAX(lh.played_at_utc) AS last_played_at_utc,
                            COUNT(DISTINCT CASE
                                WHEN lh.played_at_utc > (CURRENT_TIMESTAMP - '30 days'::interval)
                                THEN lh.played_at_utc END) AS playcount_last_30,
                            COUNT(DISTINCT lh.played_at_utc) AS playcount_total
                     FROM music.vw_listening_history lh
                     GROUP BY lh.isrc
                 ) ps
                 ORDER BY ps.last_played_at_utc DESC, ps.isrc
                 LIMIT %s
             )
             SELECT rp.isrc,
                    rp.last_played_at_utc AS "lastPlayedAtUtc",
                    COALESCE(allt.track_name_clean, '') AS "trackName",
                    COALESCE(allt.artist_display_name, '') AS "artistName",
                    pc.playlist_name AS "playlistName",
                    COALESCE(r.elo_rating, 1500) AS rating,
                    rp.playcount_last_30 AS "playcountLast30",
                    rp.playcount_total AS "playcountTotal",
                    MIN(COALESCE(r.elo_rating, 1500)) OVER () AS "minRating",
                    MAX(COALESCE(r.elo_rating, 1500)) OVER () AS "maxRating",
                    MAX(rp.playcount_last_30) OVER () AS "maxPlaycountLast30",
                    MAX(rp.playcount_total) OVER () AS "maxPlaycountTotal"
             FROM ranked_plays rp
             JOIN music.vw_listening_history lh
               ON lh.isrc = rp.isrc AND lh.played_at_utc = rp.last_played_at_utc
             JOIN music.vw_best_track_id bt ON bt.isrc = rp.isrc
             JOIN music.all_tracks allt ON allt.track_id = bt.best_track_id
             LEFT JOIN music.playlist_relationships pr
               ON pr.child_playlist_id = lh.playlist_id
             LEFT JOIN music.vw_ratings r
               ON r.isrc = lh.isrc
              AND r.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id)
             LEFT JOIN music.playlist_config pc
               ON pc.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id)
             ORDER BY rp.last_played_at_utc DESC, rp.isrc"""
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


def get_matchup(playlist_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Build a rating matchup: one rateable track plus a challenger from the same
    playlist with the closest current ELO score.

    The PRIMARY track is selected from music.vw_rating_eligible (tracks not
    recently played that need rating). The CHALLENGER is selected from
    music.playlist_isrcs (ALL tracks in the playlist, including recently-played
    ones) — ordered by closest ELO score to the primary. This matches the legacy
    get_matchup_dictionary behavior.

    Args:
        playlist_id: Optional playlist to scope the matchup. None = any rateable track.

    Returns:
        Dict with 'primary' and 'challenger' MatchupTrack dicts, or None when no
        rateable tracks exist. 'challenger' is None when the playlist has only
        one track total (no other track to challenge against).
    """
    matchup_sql = """
        WITH primary_track AS (
            SELECT isrc, playlist_id
            FROM music.vw_rating_eligible
            WHERE (%s IS NULL OR playlist_id = %s)
            LIMIT 1
        ),
        all_playlist_tracks AS (
            -- ALL tracks in the same playlist as the primary (excluding primary itself)
            -- Challenger can be any track, not just rateable ones
            SELECT isrc, playlist_id
            FROM music.playlist_isrcs
            WHERE playlist_id = (SELECT playlist_id FROM primary_track)
              AND isrc != (SELECT isrc FROM primary_track)
        ),
        rated AS (
            -- ELO ratings for primary + all playlist tracks
            SELECT isrc, playlist_id, COALESCE(elo_rating, 1500) AS elo
            FROM music.ratings
            WHERE playlist_id = (SELECT playlist_id FROM primary_track)
              AND isrc IN (SELECT isrc FROM primary_track
                           UNION SELECT isrc FROM all_playlist_tracks)
        ),
        matchup AS (
            SELECT
                pt.playlist_id,
                pt.isrc,
                COALESCE(pr.elo, 1500) AS isrc_elo,
                apt.isrc AS isrc_vs,
                COALESCE(vr.elo, 1500) AS isrc_vs_elo,
                ROW_NUMBER() OVER (
                    ORDER BY abs(COALESCE(pr.elo, 1500) - COALESCE(vr.elo, 1500))
                ) AS row_num
            FROM primary_track pt
            LEFT JOIN rated pr ON pr.isrc = pt.isrc AND pr.playlist_id = pt.playlist_id
            LEFT JOIN all_playlist_tracks apt ON apt.playlist_id = pt.playlist_id
            LEFT JOIN rated vr ON vr.isrc = apt.isrc AND vr.playlist_id = apt.playlist_id
            WHERE apt.isrc IS NOT NULL
        )
        SELECT
            m.playlist_id,
            pc.playlist_name,
            m.isrc,
            m.isrc_elo,
            at1.track_name_clean AS isrc_track,
            at1.artist_display_name AS isrc_artist,
            at1.album_id AS isrc_album_id,
            m.isrc_vs,
            m.isrc_vs_elo,
            at2.track_name_clean AS isrc_vs_track,
            at2.artist_display_name AS isrc_vs_artist,
            at2.album_id AS isrc_vs_album_id
        FROM matchup m
        INNER JOIN music.playlist_config pc ON pc.playlist_id = m.playlist_id
        INNER JOIN music.vw_best_track_id bt1 ON bt1.isrc = m.isrc
        INNER JOIN music.all_tracks at1 ON at1.track_isrc = bt1.isrc
        LEFT JOIN music.vw_best_track_id bt2 ON bt2.isrc = m.isrc_vs
        LEFT JOIN music.all_tracks at2 ON at2.track_isrc = bt2.isrc
        WHERE m.row_num = 1
    """

    rows = sql_to_dict(matchup_sql, (playlist_id, playlist_id))

    # No rateable tracks at all
    if not rows:
        # Determine if it's "no tracks at all" vs "no challenger"
        primary_sql = """
            SELECT isrc, playlist_id
            FROM music.vw_rating_eligible
            WHERE (%s IS NULL OR playlist_id = %s)
            LIMIT 1
        """
        primary_rows = sql_to_dict(primary_sql, (playlist_id, playlist_id))
        if not primary_rows:
            return None
        # Primary exists but no challenger — fetch primary display values only
        primary = primary_rows[0]
        isrc = primary["isrc"]
        pid = primary["playlist_id"]
        display_sql = """
            SELECT
                pc.playlist_name,
                at.track_name_clean AS track_name,
                at.artist_display_name AS artist_name,
                at.album_id,
                COALESCE(r.elo_rating, 1500) AS score
            FROM music.playlist_config pc
            INNER JOIN music.vw_best_track_id bt ON bt.isrc = %s
            INNER JOIN music.all_tracks at ON at.track_isrc = bt.isrc
            LEFT JOIN music.ratings r ON r.isrc = %s AND r.playlist_id = %s
            WHERE pc.playlist_id = %s
            LIMIT 1
        """
        drows = sql_to_dict(display_sql, (isrc, isrc, pid, pid))
        if not drows:
            return None
        d = drows[0]
        return {
            "primary": {
                "isrc": isrc,
                "playlistId": pid,
                "playlistName": d["playlist_name"],
                "trackName": d["track_name"],
                "artistName": d["artist_name"],
                "albumId": d["album_id"],
                "albumArtUrl": f"/api/music/album-art/{d['album_id']}" if d["album_id"] else None,
                "score": d["score"],
            },
            "challenger": None,
        }

    row = rows[0]
    return {
        "primary": {
            "isrc": row["isrc"],
            "playlistId": row["playlist_id"],
            "playlistName": row["playlist_name"],
            "trackName": row["isrc_track"],
            "artistName": row["isrc_artist"],
            "albumId": row["isrc_album_id"],
            "albumArtUrl": f"/api/music/album-art/{row['isrc_album_id']}" if row["isrc_album_id"] else None,
            "score": row["isrc_elo"],
        },
        "challenger": {
            "isrc": row["isrc_vs"],
            "playlistId": row["playlist_id"],
            "playlistName": row["playlist_name"],
            "trackName": row["isrc_vs_track"],
            "artistName": row["isrc_vs_artist"],
            "albumId": row["isrc_vs_album_id"],
            "albumArtUrl": f"/api/music/album-art/{row['isrc_vs_album_id']}" if row["isrc_vs_album_id"] else None,
            "score": row["isrc_vs_elo"],
        },
    }


def score_matchup(
    playlist_id: str,
    isrc: str,
    isrc_vs: str,
    margin: int,
) -> Dict[str, Any]:
    """
    Score a matchup between two tracks.

    Recomputes both scores via elo_update (K=100, margin multiplier 1+|margin|/5),
    writes two mirrored history rows (one per side, opposite result signs), and
    upserts both standings via the recency-guarded vw_rating_update.

    Args:
        playlist_id: The playlist both tracks belong to.
        isrc: The home track ISRC.
        isrc_vs: The challenger (away) track ISRC.
        margin: Rating margin in [-5..-1, +1..+5] (no zero/draw).
               Positive = home wins, negative = away wins.

    Returns:
        Dict with ok status and the new scores for both tracks.
    """
    # Fetch current ELOs (default 1500 baseline when unrated)
    elo_sql = """SELECT isrc, COALESCE(elo_rating, 1500) AS elo
                 FROM music.ratings
                 WHERE playlist_id = %s AND isrc IN (%s, %s)"""
    elo_rows = sql_to_dict(elo_sql, (playlist_id, isrc, isrc_vs))
    elo_map = {r["isrc"]: r["elo"] for r in elo_rows} if elo_rows else {}

    home_elo = elo_map.get(isrc, 1500)
    away_elo = elo_map.get(isrc_vs, 1500)

    # Recompute scores via existing ELO formula.
    # Positive margin = home wins, negative = away wins.
    home_new_elo, away_new_elo = elo_update(
        home_elo=home_elo,
        away_elo=away_elo,
        result=margin,
    )

    # Write two mirrored history rows with correct signs:
    # winner (positive margin = home) gets +margin, loser gets -margin.
    hist_sql = """INSERT INTO music.ratings_history (
                    playlist_id, isrc, isrc_vs, elo_old, elo_new, rating_result
                  ) VALUES (%s, %s, %s, %s, %s, %s)"""
    qec(hist_sql, (playlist_id, isrc, isrc_vs, home_elo, home_new_elo, margin))
    qec(hist_sql, (playlist_id, isrc_vs, isrc, away_elo, away_new_elo, -margin))

    # Upsert both standings (recency-guarded via vw_rating_update)
    update_ratings_from_view()

    return {
        "ok": True,
        "isrc": isrc,
        "isrcVs": isrc_vs,
        "homeNewElo": home_new_elo,
        "awayNewElo": away_new_elo,
    }
# def get_track_recommendations(playlist_id: str) -> List[TrackRecommendation]:
#     """Get track recommendations for a playlist"""
#     pass