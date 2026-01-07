import pandas as pd

from backend_functions.database_functions import get_conn, sql_to_list, elapsed_ms, qec, sql_to_dict
from backend_functions.logging_functions import log_app_event, start_timer
from backend_functions.service_logins import get_spotify_client
from backend_functions.task_execution import json_loading, task_log
import time



def get_playlist_list(list_type=None):

    if list_type == 'seeds':
        sql = """SELECT DISTINCT playlist_id from music.playlist_config WHERE
        seeds_only and track_count >0 """
    elif list_type == 'once':
        sql = """SELECT DISTINCT playlist_id from music.playlist_config
            WHERE NOT seeds_only and NOT auto_shuffle and NOT make_recs AND is_active and track_count > 0"""
    else:
        sql = """SELECT DISTINCT playlist_id from music.playlist_config WHERE
                is_active AND (auto_shuffle or make_recs) and track_count > 0"""

    return sql_to_list(sql)


def playlist_to_db(client=None, list_id=None, list_type=None):
    # Connects to Spotify API and downloads all tracks
    # Uploads JSON to DB, which is then processed via stored procedure.

    # Monitor performance, start the timer
    t0 = start_timer()

    task_name = 'Playlist Detail Sync'
    if not list_type:
        list_type = 'auto'


    if list_type == 'once':
        task_name = 'One-time Seed Generation'
    elif list_type == 'seeds':
        task_name = 'Dynamic Seed Generation'


    # Put the single (or multiple) playlist into a list
    if not list_id:
        playlists = get_playlist_list(list_type)
    else:
        playlists = [list_id]

    # Ensure we actually have playlists
    if not playlists:
        return client

    # Refresh the client, if needed
    client = get_spotify_client(client)
    sp = client.get("client")

    # Initialize Results
    all_items=[]
    e=None
    # Iterate through list of playlists
    for l in playlists:
        if l != playlists[0]:
            time.sleep(2)
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
        sql = f"CALL staging.flatten_playlist_details('{list_type}');"
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

    task_log(task_name=task_name,
             e_time=extract_ms,
             l_time=load_ms,
             t_time=transform_ms,
             fail_type=None,
             fail_text=None)
    return client


def playlist_sync_auto(client=None):
    return playlist_to_db(client=client, list_id=None, list_type='auto')

def playlist_sync_seeds(client=None):
    return playlist_to_db(client=client, list_id=None, list_type='seeds')

def playlist_sync_one_time(client=None):
    return playlist_to_db(client=client, list_id=None, list_type='once')

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

    return client


def gen_playlist(client, name, description):
    client = get_spotify_client(client)
    sp = client.get("client")
    user_id = sp.me()["id"]
    playlist = sp.user_playlist_create(user=user_id,
                                           name=name,
                                           public=False,
                                           description=description)
    return client, playlist
