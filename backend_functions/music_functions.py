import pandas as pd

from backend_functions.database_functions import get_conn, sql_to_list, elapsed_ms, qec
from backend_functions.logging_functions import log_app_event, start_timer
from backend_functions.service_logins import get_spotify_client
from backend_functions.task_execution import json_loading, task_log


def playlist_to_db(client=None, list_id=None):
    # Connects to Spotify API and downloads all tracks
    # Uploads JSON to DB, which is then processed via stored procedure.

    # Monitor performance, start the timer
    t0 = start_timer()

    # Put the single (or multiple) playlist into a list
    if not list_id:
        sql = """SELECT DISTINCT playlist_id from music.playlist_config WHERE
        is_active AND (auto_shuffle or make_recs) and track_count > 0"""
        playlists = sql_to_list(sql)
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
        task_log('playlist_details',
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
        task_log('playlist_details',
                 e_time=extract_ms,
                 l_time=elapsed_ms(t0),
                 t_time=None,
                 fail_type='No playlist items',
                 fail_text=f"{len(playlists)} playlist(s) attempted: {e}")
        return client

    # Integrate Results
    t0 = start_timer()
    try:
        sql = "CALL staging.flatten_playlist_details();"
        qec(sql)
        transform_ms = elapsed_ms(t0)
    except Exception as e:
        task_log('playlist_details',
                 e_time=extract_ms,
                 l_time=load_ms,
                 t_time=elapsed_ms(t0),
                 fail_type='No playlist items',
                 fail_text=f"{len(playlists)} playlist(s) attempted: {e}")
        return client

    task_log('playlist_details',
             e_time=extract_ms,
             l_time=load_ms,
             t_time=transform_ms,
             fail_type=None,
             fail_text=None)
    return client