import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_conn
from backend_functions.task_execution import task_executioner
from frontend_functions.music_widgets import playlist_config_table, render_shuffle_df
from frontend_functions.nav_buttons import nav_button


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
    music_nav_key = 'music'
    nav_button(music_nav_key)
    st.subheader("Spotify Controls")

    nav_selection = ss.get(f"{music_nav_key}_active_decode")

    if not nav_selection:
        nav_selection='now_playing'
    if nav_selection == 'now_playing':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'listen_history':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'list_config':
        render_playlist_config(music_nav_key)
    elif nav_selection == 'list_shuffle':
        render_playlist_shuffle()
    elif nav_selection == 'track_ratings':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'isrc_clean':
        st.info(f"{nav_selection} module not yet built")
    elif nav_selection == 'sync_playlists':
        st.info(f"{nav_selection} module not yet built")
    else:
        st.error(f"Uncaught nav exception for __{music_nav_key}__")
    return


def render_playlist_config(nav_key):
    # Allow for manual loading of playlist headers
    if st.button(label=':material/sync: Sync Playlists'):
        task_name = 'Playlist Header Sync'
        task_executioner(force_task_name=task_name, force_task=True)
        st.rerun()

    # Read/write of playlist settings.
    playlist_config_table()

    return

def render_playlist_shuffle():
    # Forces users to pick a playlist, then gives them weighting options
    if ss.get("pl_selection") is None:
        key_val = 'de_playlist_config_df_selection'
        selection_value = ss.get(key_val)
        if not selection_value:
            playlist_config_table(is_selection=True)
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

    if st.button(':material/cloud_upload: Send to Spotify'):
        track_list = df['track_id'].to_list()
        st.info('Upload Not built yet.')


