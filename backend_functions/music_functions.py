import pandas as pd

from backend_functions.database_functions import get_conn, sql_to_list, elapsed_ms, qec, sql_to_dict, one_sql_result
from backend_functions.logging_functions import log_app_event, start_timer
from backend_functions.service_logins import get_spotify_client
from backend_functions.task_execution import json_loading, task_log
import time



def get_playlist_list(list_type=None):
    sql = "SELECT * FROM music.vw_playlist_detail_sync_logic"
    return sql_to_list(sql)


def playlist_to_db(client=None, list_id=None):
    # Connects to Spotify API and downloads all tracks
    # Uploads JSON to DB, which is then processed via stored procedure.

    # Monitor performance, start the timer
    t0 = start_timer()

    task_name = 'Playlist Detail Sync'

    # Put the single (or multiple) playlist into a list
    if not list_id:
        playlists = get_playlist_list()
    else:
        playlists = [list_id]

    # Ensure we actually have playlists
    if not playlists:
        return client

    log_app_event(cat='Playlist Detail Sync', desc=f"Syncing songs from {len(playlists)}", exec_time=elapsed_ms(t0))
    # Refresh the client, if needed
    client = get_spotify_client(client)
    sp = client.get("client")

    # Initialize Results
    all_items=[]
    e=None
    # Iterate through list of playlists
    for l in playlists:
        if l != playlists[0]:
            time.sleep(10) # Sleep for 10 seconds between playlists
        try:
            results = sp.playlist_items(playlist_id=l, additional_types=['track'])
        except Exception as e:
            log_app_event(cat='Playlist Fetch Failure', desc=f"ID: {l}", err=e)
            continue

        # Get the next page of results
        while results:
            all_items.append(results)
            results = sp.next(results)

    extract_ms = elapsed_ms(t0)

    # Stop if no results
    if not all_items:
        task_log(task_name=task_name,
                 e_time=extract_ms,
                 l_time=None,
                 t_time=None,
                 fail_type='No playlist items',
                 fail_text=f"{len(playlists)} playlist(s) attempted: {e}")
        return client

    # Load blob to postgres
    t0 = start_timer()
    try:
        json_loading(all_items, 'playlist_details')
        load_ms = elapsed_ms(t0)
    except Exception as e:
        task_log(task_name=task_name,
                 e_time=extract_ms,
                 l_time=elapsed_ms(t0),
                 t_time=None,
                 fail_type='No playlist items',
                 fail_text=f"{len(playlists)} playlist(s) attempted: {e}")
        return client

    # Integrate Results
    t0 = start_timer()
    try:
        sql = f"CALL staging.flatten_playlist_details();"
        qec(sql)
        transform_ms = elapsed_ms(t0)


    except Exception as e:
        task_log(task_name=task_name,
                 e_time=extract_ms,
                 l_time=load_ms,
                 t_time=elapsed_ms(t0),
                 fail_type='No playlist items',
                 fail_text=f"{len(playlists)} playlist(s) attempted: {e}")
        return client

    for l in playlists:
        up_sql = "UPDATE music.playlist_config SET last_synced = CURRENT_TIMESTAMP where playlist_id = %s"
        params = [l,]
        qec(up_sql, params)

    task_log(task_name=task_name,
             e_time=extract_ms,
             l_time=load_ms,
             t_time=transform_ms,
             fail_type=None,
             fail_text=None)
    return client


def playlist_sync_auto(client=None):
    return playlist_to_db(client=client, list_id=None)

def playlist_sync_seeds(client=None):
    return playlist_to_db(client=client, list_id=None)


def playlist_reset(client=None, list_id=None):
    if not list_id:
        return client

    client = get_spotify_client(client)
    sp = client.get("client")
    sp.playlist_replace_items(list_id, [])
    return client

def playlist_upload(client=None, list_id=None, track_list=None):
    if not list_id or not track_list:
        return client

    client = get_spotify_client(client)
    sp = client.get("client")
    num_songs = len(track_list)
    if num_songs < 100:
        batch_size = num_songs
    else:
        batch_size = 100

    for i in range(0, len(track_list), batch_size):
        batch = track_list[i:i + batch_size]
        if len(batch) > 0:
            sp.playlist_add_items(list_id, batch)
    return client

def ensure_playlist_relationships(client):
    del_sql = """DELETE FROM music.playlist_relationships pr
                WHERE pr.child_playlist_id in
                 (SELECT DISTINCT playlist_id FROM music.playlist_config WHERE not is_active)"""
    qec(del_sql)

    sql = """SELECT * from music.missing_relationships"""
    d = sql_to_dict(sql)
    if not d:
        return

    client = get_spotify_client(client)

    for item in d:
        id = item.get("playlist_id")
        name = item.get("playlist_name")
        auto = item.get("needs_auto")
        man = item.get("needs_manual")
        rec = item.get("needs_recs")
        if auto or man:
            name = f"{name} (a)"
            desc = f"Auto-shuffled copy of {name}."
            playlist_type = 'auto' if auto else 'manual'
            client, new_id = gen_playlist(client, name, desc)
            ins_sql = f"""INSERT INTO music.playlist_relationships (
            parent_playlist_id,
            child_playlist_id,
            child_playlist_type)
            VALUES
            ('{id}', '{new_id}', '{playlist_type}');"""
            try:
                qec(ins_sql)
                print('success')
                print(ins_sql)
            except Exception as e:
                print('ERROR')
                print(ins_sql)
                print(e)
        if rec:
            name = f"{name} (r)"
            desc = f"Auto-Recommendations for {name}."
            playlist_type = 'recommendation'
            client, new_id = gen_playlist(client, name, desc)
            ins_sql = f"""INSERT INTO music.playlist_relationships (
                        parent_playlist_id,
                        child_playlist_id,
                        child_playlist_type)
                        VALUES
                        ('{id}', '{new_id}', '{playlist_type}');"""
            try:
                qec(ins_sql)
                print('success')
                print(ins_sql)
            except Exception as e:
                print('ERROR')
                print(ins_sql)
                print(e)

    return client


def gen_playlist(client, name, description):
    client = get_spotify_client(client)
    sp = client.get("client")
    user_id = sp.me()["id"]
    playlist = sp.user_playlist_create(user=user_id,
                                           name=name,
                                           public=False,
                                           description=description)
    playlist_id = playlist["id"]
    return client, playlist_id


def auto_shuffle_playlists():
    sql = "SELECT DISTINCT playlist_id from music.playlist_config WHERE is_active and auto_shuffle"
    playlists = sql_to_list(sql)
    client=None
    for l in playlists:
        sql = f"""SELECT DISTINCT target_playlist_id, track_id,
            default_new_order 
            FROM music.vw_playlist_isrc_stats WHERE playlist_id = '{l}'
            ORDER BY default_new_order asc;"""
        df = pd.read_sql(sql, get_conn(alchemy=True))
        if df.empty:
            continue

        id = df['target_playlist_id'].iloc[0]
        track_list = df['track_id'].to_list()
        client = playlist_reset(None, id)
        client = playlist_upload(client, id, track_list)
        continue
    return


def get_now_playing(client=None):
    # Gets the currently playing trackId and playlistID from spotify
    client = get_spotify_client(client)
    sp = client.get("client")

    playback = sp.current_user_playing_track()


    if playback is None or playback.get("item") is None:
        return None

    track = playback["item"]

    # Send song information to database
    try:
        json_loading(playback, 'now_playing')
        print('JSON loaded')
    except Exception as e:
        print(f'JSON Failed to load: {e}')
        return

    try:
        result = qec("call staging.flatten_now_playing();")
        print(f'JSON Flattened')
    except Exception as e:
        result = f"JSON failed to flatten: {e}"

    print(result)

    # Extract track-level info
    track_id = track["id"]
    track_isrc = track['external_ids'].get('isrc')
    # timestamp = track["timestamp"]
    t0 = start_timer()
    progress = playback["progress_ms"]
    # If playback is happening in a playlist, extract it
    context = playback.get("context")
    playlist_id = None
    playlist_name = None
    print(context)
    if context and context.get("type") == "playlist":
        playlist_uri = context.get("uri")  # e.g. spotify:playlist:12345
        if playlist_uri and playlist_uri.startswith("spotify:playlist:"):
            playlist_id = playlist_uri.split(":")[-1]
            try:
                playlist_data = sp.playlist(playlist_id, fields="name")
                playlist_name = playlist_data.get("name")
            except Exception:
                playlist_name = None
                playlist_id = None

    # Look up ISRC ratings from sql
    sql= f"SELECT isrc FROM music.vw_track_id_to_isrc WHERE track_id = '{track_id}';"
    isrc = one_sql_result(sql)
    print(f"From API: {track_isrc}")
    print(f"From SQL: {isrc}")
    duration_ms = track.get("duration_ms")
    complete_at = t0 + (duration_ms - progress)
    done_in_s = round((duration_ms-progress)/1000,1)

    # Lookup best track info from database
    sql = f"""SELECT DISTINCT
                b.track_isrc as isrc,
                b.track_id,
                b.track_name_clean as track_name,
                b.artist_display_name as artist_name,
                alb.album_name_clean as album_name,
                b.album_id	
            FROM music.vw_best_track_id bt
                INNER JOIN music.all_tracks b on b.track_id = bt.best_track_id
                INNER JOIN music.all_albums alb on alb.album_id = b.album_id
            WHERE b.track_isrc = '{isrc}' """

    track_dict = sql_to_dict(sql)[0]
    track_name = track_dict.get("track_name")
    best_track_id = track_dict.get("track_id")
    artist_name = track_dict.get("artist_name")
    album_name = track_dict.get("album_name")
    album_id = track_dict.get("album_id")

    # Get rating & playlist info
    elo = 1500
    child_id = None
    parent_id = None
    playlist_type = None
    if playlist_name:
        sql = f"""SELECT child_playlist_id, parent_playlist_id, child_playlist_type FROM
                music.playlist_relationships
                 WHERE child_playlist_id = '{playlist_id}';"""
        temp_d = sql_to_dict(sql)
        if temp_d:
            playlist_dict = temp_d[0]
            if playlist_dict:
                parent_id = playlist_dict.get("parent_playlist_id")
                playlist_type = playlist_dict.get("child_playlist_type")
                print(f"Playlist Type 1 : {playlist_type} : eval {playlist_type == 'recommendation'}")
            fetch_id = parent_id if parent_id else playlist_id
            if playlist_type == 'recommendation':
                sql = f"""SELECT elo_track_predicted as elo_rating FROM music.track_recommendations
                        WHERE isrc='{isrc}' and playlist_id='{fetch_id}'"""
            else:
                sql = f"""SELECT elo_rating from music.ratings
                        WHERE isrc='{isrc}' and playlist_id='{fetch_id}';"""
            elo = one_sql_result(sql)

    # print(f"Playlist Type 2 : {playlist_type} : eval {playlist_type == 'recommendation'}")
    if playlist_id == '':
        playlist_id = None

    return {
        "playlistId": playlist_id,
        "trackId": track_id,
        "bestTrackId": best_track_id,
        "isrc": isrc,
        "currentELO": elo,
        "playlistName": playlist_name,
        "trackName": track_name,
        "artistName": artist_name,
        "albumName": album_name,
        "albumId": album_id,
        "isRecPlaylist": playlist_type == 'recommendation',
        'parentPlaylist': parent_id,
        'completeAtTS': complete_at,
        'doneInS': done_in_s
    }


def add_isrc_to_local(id, isrc):
    # Add to parent playlist locally
    ins_sql = f"""INSERT INTO music.playlist_isrcs (playlist_id, isrc) VALUES (%s, %s)"""
    params = (id, isrc)
    es = qec(ins_sql, params)
    return es

def record_recommendation_decision(id, isrc, was_promoted):
    # Record decision
    rec_sql = f"""SELECT 
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
	WHERE playlist_id = %s and isrc = %s ;"""

    ins_sql = f"""INSERT INTO music.track_recommendations {rec_sql}"""
    qec(ins_sql, [was_promoted, id, isrc])


    return

def remove_recommendation(id, isrc):
    del_sql = f"""DELETE FROM music.track_recommendations WHERE playlist_id = %s and isrc = %s;"""
    params = (id, isrc)
    qec(del_sql, params)
    return

def add_into_current_ratings(id, isrc, elo):
    rat_sql = f"""INSERT INTO music.ratings (playlist_id, isrc, elo_rating, rating_count, wins,
                 losses) VALUES (%s, %s, %s, %s, %s, %s)"""
    params = (id, isrc, elo, 0, 0, 0)
    qec(rat_sql, params)
    return


def save_matchup_results(hd, ad, mr):

    if ad:
        home_new_elo, away_new_elo = elo_update(
            home_elo=hd.get("currentELO"),
            away_elo=ad.get("currentELO"),
            result=mr
        )
        hd["newELO"] = home_new_elo
        hd["matchResult"] = mr
        hd["isrcVS"] = ad.get("isrc")
        ad["newELO"] = away_new_elo
        ad["isrcVS"] = hd.get("isrc")
        ad["matchResult"] = -mr  # corrected: home win = +1 → away = -1
        two_d = [hd, ad]
    else:
        home_new_elo, away_new_elo = elo_update(
            home_elo=hd.get("currentELO") or 1500,
            away_elo=hd.get("currentELO") or 1500,
            result=mr
        )

        hd["newELO"] = home_new_elo
        hd["matchResult"] = mr
        hd["isrcVS"] = 'strawman'
        two_d = [hd]

    # Use a single shared connection and explicit transaction
    try:
        for d in two_d:
            playlist_id = d.get("playlistId")
            isrc = d.get("isrc")
            isrc_vs = d.get("isrcVS")
            old_elo = d.get("currentELO") or 1500
            new_elo = d.get("newELO")
            match_result = d.get("matchResult")
            params = (playlist_id, isrc, isrc_vs,
                      int(old_elo), int(new_elo), match_result)

            insert_sql = """
                INSERT INTO music.ratings_history (
                    playlist_id, isrc, isrc_vs, elo_old, elo_new, rating_result
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            qec(insert_sql, params)


            # Merge ratings
            params=(isrc, playlist_id)
            update_sql = """INSERT INTO music.ratings (
                            playlist_id,
                            isrc,
                            elo_rating,
                            rating_count,
                            rating_wins,
                            rating_losses,
                            last_rated_utc
                            )
                        SELECT * FROM music.vw_ratings_update
                        WHERE isrc = %s AND playlist_id = %s
                        ON CONFLICT (playlist_id, isrc)
                        DO UPDATE SET
                            elo_rating=EXCLUDED.elo_rating,
                            rating_count=EXCLUDED.rating_count,
                            rating_wins=EXCLUDED.rating_wins,
                            rating_losses=EXCLUDED.rating_losses,
                            last_rated_utc=EXCLUDED.last_rated_utc
                        WHERE music.ratings.last_rated_utc IS DISTINCT FROM EXCLUDED.last_rated_utc;"""
            qec(update_sql, params)


    except Exception as e:
        log_app_event(cat='Ratings', desc="Error saving matchup results", err=e)
        raise

    return

def elo_update(home_elo, away_elo, result, k=100):
    # Takes in the starting ratings & match result, spits out the new ratings

    # Expected scores based on ELO difference
    expected_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
    expected_away = 1 - expected_home

    # Scale result to [0,1], where 0 = home lost, 1 = home won
    # matchResult = 0 gives 0.5 (draw)
    actual_home = (result + 5) / 10
    actual_away = 1 - actual_home

    # Scale adjustment by margin of victory
    margin_multiplier = 1 + (abs(result) / 5)  # between 1x and 2x impact

    # Update ratings
    homeNewELO = round(home_elo + k * margin_multiplier * (actual_home - expected_home),0)
    awayNewELO = round(away_elo + k * margin_multiplier * (actual_away - expected_away),0)

    return homeNewELO, awayNewELO