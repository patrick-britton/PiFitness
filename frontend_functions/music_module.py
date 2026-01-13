import os
import time

import pandas as pd
import requests
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_conn, qec, sql_to_list
from backend_functions.file_handlers import album_art_path
from backend_functions.music_functions import playlist_reset, playlist_upload, get_now_playing, add_isrc_to_local, \
    record_recommendation_decision, remove_recommendation, add_into_current_ratings, save_matchup_results, \
    playlist_to_db
from backend_functions.service_logins import get_spotify_client
from backend_functions.task_execution import task_executioner
from frontend_functions.music_widgets import playlist_config_table, render_shuffle_df
from frontend_functions.nav_buttons import nav_button, nav_widget, clear_nav
from frontend_functions.streamlit_helpers import ss_pop


# # Music Page
        # 'music': {'now_playing': {'icon': 'radio', 'label': 'Now Playing'},
        #           'listen_history': {'icon': 'download', 'label': 'Sync History'},
        #           'list_config': {'icon': 'tune', 'label': 'Playlist Config'},
        #           'list_shuffle': {'icon': 'shuffle', 'label': 'Playlist Shuffle'},
        #           'track_ratings': {'icon': 'voting_chip', 'label': 'Ratings'},
        #           'isrc_clean': {'icon': 'cleaning_services', 'label': 'Review ISRCs'},
        #           'sync_playlists': {'icon': 'queue_music', 'label': 'Playlist Sync'},
        #             },

def render_music():
    # music_nav_key = 'music'
    # nav_button(music_nav_key)
    # st.subheader("Spotify Controls")
    #
    # nav_selection = ss.get(f"{music_nav_key}_active_decode")
    nav_selection = nav_widget('music', 'Music Controls')

    # Set Default
    if not nav_selection:
        nav_selection='now_playing'

    if nav_selection == 'now_playing':
        render_now_playing()
    elif nav_selection == 'listen_history':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'list_config':
        render_playlist_config()
    elif nav_selection == 'list_shuffle':
        render_playlist_shuffle()
    elif nav_selection == 'track_ratings':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'isrc_clean':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'sync_playlists':
        st.info(f"{nav_selection} module not yet built")
    else:
        st.error(f"Uncaught nav exception for music: {nav_selection}")
    return


def render_playlist_config():
    # Allow for manual loading of playlist headers
    if st.button(label=':material/sync: Sync Playlists'):
        task_name = 'Playlist Header Sync'
        task_executioner(force_task_name=task_name, force_task=True)
        st.rerun()

    # Read/write of playlist settings.
    playlist_config_table()

    return


def render_playlist_shuffle(list_id=None):
    # Forces users to pick a playlist, then gives them weighting options
    if ss.get("pl_selection") is None:
        key_val = 'de_playlist_config_df_selection'
        selection_value = ss.get(key_val)
        if not selection_value:
            playlist_config_table(is_selection=True, list_id=list_id)
            return
        else:
            ss.pl_selection = selection_value["selection"]["rows"]

    row = ss.pl_selection
    row_index = row[0]
    id = ss.pc_df.iloc[row_index]["playlist_id"]
    ratings_weight = int(ss.pc_df.iloc[row_index]["ratings_weight"])
    recency_weight = int(ss.pc_df.iloc[row_index]["recency_weight"])
    randomness_weight = int(ss.pc_df.iloc[row_index]["randomness_weight"])
    minutes_to_sync = int(ss.pc_df.iloc[row_index]["minutes_to_sync"])

    rating_col, recency_col, rand_col, minutes_col = st.columns(spec=[1,1,1,1], gap="small")

    with rating_col:
        new_ratings_weight = st.number_input(label='Ratings Weight',
                                             min_value=0,
                                             max_value=50,
                                             step=1,
                                             format='%d',
                                             value=ratings_weight)

    with recency_col:
        new_recency_weight = st.number_input(label='Recency Weight',
                                             min_value=0,
                                             max_value=50,
                                             step=1,
                                             format='%d',
                                             value=recency_weight)

    with rand_col:
        new_randomness_weight = st.number_input(label='Randomness Weight',
                                             min_value=0,
                                             max_value=50,
                                             step=1,
                                             format='%d',
                                             value=randomness_weight)

    with minutes_col:
        new_minutes_to_sync = st.number_input(label='Minutes to Sync',
                                             min_value=1,
                                             max_value=9999,
                                             step=15,
                                             format='%d',
                                             value=minutes_to_sync)

    rtw = new_ratings_weight if new_ratings_weight else ratings_weight
    rcw = new_recency_weight if new_recency_weight else recency_weight
    rnw = new_randomness_weight if new_randomness_weight else randomness_weight
    mts = new_minutes_to_sync if new_minutes_to_sync else minutes_to_sync

    if "shuffle_df" not in ss:
        sql = f"SELECT * FROM music.vw_playlist_isrc_stats WHERE playlist_id = '{id}'"
        ss.shuffle_df = pd.read_sql(sql, get_conn(alchemy=True))

    df = render_shuffle_df(rcw, rtw, rnw, mts)

    if st.button(f':material/cloud_upload: Send to Spotify :gray[*{df['target_playlist_id'].iloc[0]}*]'):
        track_list = df['track_id'].to_list()
        target_list_id = df['target_playlist_id'].iloc[0]
        with st.spinner('Sending new order to Spotify', show_time=True):
            client = playlist_reset(client=None,
                                    list_id=target_list_id)
            client = playlist_upload(client=client,
                                     list_id=target_list_id,
                                     track_list=track_list)

        with st.spinner('Updating DB play order & config settings', show_time=True):
            playlist_to_db(client=client, list_id=target_list_id, list_type=None)
            update_sql = f"""UPDATE music.playlist_config SET
                            ratings_weight = {rtw},
                            recency_weight={rcw},
                            randomness_weight={rnw},
                            minutes_to_sync={mts}
                            WHERE playlist_id = '{id}';"""
            qec(update_sql)
        pop_list = ['shuffle_df',
                    'pl_selection',
                    'pc_df',
                    'listens_df','new_run_synced']
        clear_nav('running')
        clear_nav('music')
        ss_pop(pop_list)
        st.rerun()
    return


def render_now_playing():
    # Intialize the client, if needed
    if "spotify_client" not in ss:
        ss.spotify_client = get_spotify_client()

    # Get the song details
    if "refresh_dict" not in ss or ss.refresh_dict:
        ss.now_dict = get_now_playing(ss.spotify_client)
        ss.refresh_dict = False

    if not ss.now_dict:
        st.info('No songs currently playing')
        return



    # Load the default values
    is_rec_playlist = ss.now_dict.get("isRecPlaylist")
    playlist_id = ss.now_dict.get("playlistId")
    track_name = ss.now_dict.get("trackName")
    artist_name = ss.now_dict.get("artistName")
    album_id = ss.now_dict.get("albumId")
    isrc = ss.now_dict.get("isrc")

    # Greeting msg
    np_msg = f'__Now Playing__: :blue[__{track_name}__] by :blue[__{artist_name}__] '

    action_button_dict = {'skip': {'icon': 'skip_next'},
                          'refresh': {'icon': 'refresh'}}

    # Get the playlist Options if not on a playlist
    if not playlist_id:
        msg =  ":gray[*not from playlist*]"
        sql = f"""SELECT
                    pc.playlist_id,
                    pc.playlist_name 
                FROM music.playlist_config pc
                INNER JOIN (SELECT DISTINCT playlist_id,
			                MAX(CASE WHEN isrc = '{isrc}' THEN 1 ELSE 0 END) as on_playlist
                                from music.playlist_isrcs
                                GROUP BY playlist_id
                                ) spd on spd.playlist_id = pc.playlist_id and on_playlist = 0
                    WHERE pc.auto_shuffle or make_recs or manual_shuffle"""
        playlists = pd.read_sql(sql, con=get_conn(alchemy=True))
        list_options = {'playlist_add': {'icon': 'add_2'}}
        playlist_options = {
            f":material/add_2: Add to {row.playlist_name}": row.playlist_id
            for _, row in playlists.iterrows()
        }
    elif is_rec_playlist:
        msg = (f"Listening to recommended songs from {ss.now_dict.get("playlistName")}  \n__{track_name}__ "
               f"by __{ss.now_dict.get("artistName")}__")
        list_options = {'promote': {'icon': 'add_2', 'label':'Promote'},
                        'soft_reject': {'icon': 'call_missed', 'label': 'Soft Reject'},
                        'hard_reject': {'icon': 'thumb_down', 'label': 'Hard Reject'}}

    else:
        msg = f"*from playlist __{ss.now_dict.get("playlistName")}__ {ss.now_dict.get("currentELO")}*"
        list_options = {'remove': {'icon': 'remove_selection', 'label': 'Remove'},
                        'rank_up': {'icon': 'arrow_upward', 'label': 'Rank Up'},
                        'rank_down': {'icon': 'arrow_downward', 'label': 'Rank Down'}}

    # Display message and album art
    st.write(f"{np_msg}  \n {msg}")
    w = 100 if ss.is_mobile else 300
    st.image(album_image_retrieval(album_id), width=w)

    # Render the options
    action_button_dict = action_button_dict | list_options | {'reset': {'icon': 'check_indeterminate_small'}}

    action_key= 'np_nav'
    ss.np_action_choice = nav_widget(action_key, 'Options', action_button_dict)

    # do nothing if no action
    if not ss.np_action_choice:
        st.write(':gray[*please make a selection above*]')
        return

    action_choice = ss.np_action_choice
    if action_choice == 'reset':
        st.rerun()

    elif action_choice == 'skip':
        st.toast(f"{track_name} skipped", duration=3)
        ss.np_action_choice = None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        skip()

    elif action_choice == 'refresh':
        ss.refresh_dict = True
        ss.np_action_choice=None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.toast(f"Now Playing refreshed", duration=3)
        st.rerun()

    elif action_choice == "promote":
        # Promote the currently playing track from recommended to parent playlist
        # Add to parent playlist locally
        parent_id = ss.now_dict.get("parentPlaylist")
        add_isrc_to_local(parent_id, isrc)

        # Add to spotify playlist
        track_id = ss.now_dict.get("trackId")
        add_item(track_id, parent_id)

        # Record decision
        record_recommendation_decision(parent_id, isrc, was_promoted=True)

        # Remove from recommendations
        remove_recommendation(parent_id, isrc)

        # Add to current ratings
        current_elo = ss.now_dict.get("currentELO")
        add_into_current_ratings(parent_id, isrc, current_elo)
        st.toast(f"{track_name} added to playlist", duration=3)
        ss.np_action_choice = None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.rerun()

    elif action_choice == "soft_reject":
        # Exclude from recommendations but don't use for model training
        parent_id = ss.now_dict.get("parentPlaylist")
        current_elo = ss.now_dict.get("currentELO")
        soft_sql = f"""INSERT INTO music.playlist_recommendation_exclusions (playlist_id, isrc, elo_track_predicted) 
                        VALUES (%s, %s, %s)"""
        params = (parent_id, isrc, current_elo)
        qec(soft_sql, params)
        st.toast(f"{track_name} excluded from future recommendations", duration=3)
        remove_recommendation(parent_id, isrc)
        ss.np_action_choice = None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.rerun()

    elif action_choice == "hard_reject":
        # Hard Reject recommended song from list and record decision
        parent_id = ss.now_dict.get("parentPlaylist")
        # Record decision
        record_recommendation_decision(parent_id, isrc, was_promoted=False)
        # Remove from recommendations
        remove_recommendation(parent_id, isrc)
        st.toast(f"{track_name} rejected from recommendations", duration=3)
        ss.refresh_dict = True
        ss.np_action_choice = None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        skip()


    elif action_choice == "remove":
        # Remove the track from spotify and local playlist details
        del_sql = f"""DELETE FROM music.playlist_isrcs where playlist_id = %s and isrc = %s"""
        params = (playlist_id, isrc)
        qec(del_sql, params)
        remove_track_from_spotify_playlist(isrc, playlist_id)
        ss.np_action_choice = None
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.rerun()


    elif action_choice == "rank_up":
        # Bumps up the ELO rating of the current song against a straw man.
        current_elo = ss.now_dict.get("currentELO")
        home_dict = {"isrc": isrc,
                     "playlistId": playlist_id,
                     "currentELO": current_elo
                     }
        save_matchup_results(
            hd=home_dict,
            ad=None,
            mr=2
        )
        st.toast(f"{track_name} ranked up", duration=3)
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.rerun()

    elif action_choice == "rank_down":
        # Bumps down the ELO rating of the current song against a straw man.
        current_elo = ss.now_dict.get("currentELO")
        home_dict = {"isrc": isrc,
                     "playlistId": playlist_id,
                     "currentELO": current_elo
                     }
        save_matchup_results(
            hd=home_dict,
            ad=None,
            mr=-2
        )
        st.toast(f"{track_name} ranked down", duration=3)
        ss[f"{action_key}_current"] = None
        ss[f"{action_key}_active_decode"] = None
        st.rerun()

    elif action_choice == 'playlist_add' and playlist_options:
        st.info('Select the playlist to add to:')

        for btn in playlist_options.keys():
            id = playlist_options.get(btn)
            if st.button(label=btn, key=f'pl_btn_key_{id}'):
                with st.spinner(text="Adding to playlist...", show_time=True):
                    e = add_isrc_to_local(id, isrc)
                    if e:
                        for v in e:
                            st.warning(v)
                        time.sleep(5)
                    add_item(ss.now_dict.get("bestTrackId"), id)
                ss[f"{action_key}_current"] = None
                ss[f"{action_key}_active_decode"] = None
                st.rerun()
    else:
        st.error(f'Uncaught now playing action selection: {action_choice}')

    return

def spotify_client_init():
    if "spotify_client" not in ss:
        ss.spotify_client = get_spotify_client()
    return ss.spotify_client.get('client')


def skip():
    sp = spotify_client_init()
    sp.next_track()
    time.sleep(1)
    ss.refresh_dict = True
    st.rerun()
    return

def add_item(tid, pid):
    sp = spotify_client_init()
    sp.playlist_add_items(pid, [tid])
    return

def remove_track_from_spotify_playlist(isrc, list_id):
    sql = f"SELECT DISTINCT track_id FROM music.all_tracks where track_isrc = '{isrc}';"
    track_list = sql_to_list(sql)
    if not track_list:
        return

    sp = spotify_client_init()
    sp.playlist_remove_all_occurrences_of_items(list_id, track_list)
    return

def album_image_retrieval(album_id):
    # Returns the path of an album image -- downloading it if necessary

    filename = f"{album_id}.jpg"
    filepath = os.path.join(album_art_path(), filename)

    # Return immediately if file already exists
    if os.path.exists(filepath):
        return filepath

    sp = spotify_client_init()
    # Fetch album info
    album = sp.album(album_id)
    images = album.get("images", [])
    if not images:
        raise ValueError(f"No images found for album {album_id}")

    # Take the first (largest) image
    image_url = images[0]["url"]

    # Download and save
    resp = requests.get(image_url)
    resp.raise_for_status()
    with open(filepath, "wb") as f:
        f.write(resp.content)

    return filepath