from logging import disable

import numpy as np
import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_conn, qec
from backend_functions.helper_functions import convert_to_json_serializable
from frontend_functions.streamlit_helpers import sync_df_from_data_editor


def playlist_config_table(is_selection=False):
    sql = """SELECT * FROM music.vw_playlist_config"""

    ss.pc_df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    # Convert entire dataframe to JSON-serializable types
    ss.pc_df = ss.pc_df.map(convert_to_json_serializable)

    if ss.pc_df.empty:
        st.info("Sync playlists to configure")
        return

    # set the configuration
    max_songs = int(ss.pc_df["track_count"].max())

    if is_selection:
        cols = ['playlist_name',
                'track_count',
                'ratings_weight',
                'recency_weight',
                'randomness_weight',
                'minutes_to_sync',
                'playlist_id']
    else:
        cols = ['playlist_name',
                'track_count',
                'auto_shuffle',
                'manual_shuffle',
                'make_recs',
                'seeds_only',
                'ratings_weight',
                'recency_weight',
                'randomness_weight',
                'minutes_to_sync',
                'is_active',
                'playlist_id']

    col_config = {'is_active': st.column_config.CheckboxColumn(label='Active?',
                                                                  disabled=True),
                  'playlist_name': st.column_config.TextColumn(label='Name',
                                                               pinned=True,
                                                               disabled=True
                                                               ),
                  'track_count': st.column_config.ProgressColumn(label='Songs',
                                                                 min_value=0,
                                                                 max_value = max_songs,
                                                                 pinned=True,
                                                                 format='%d'),
                  'auto_shuffle': st.column_config.CheckboxColumn(label='Auto Shuffle?',
                                                                  disabled=False),
                  'manual_shuffle': st.column_config.CheckboxColumn(label='Manual Shuffle?',
                                                                  disabled=False),
                  'make_recs': st.column_config.CheckboxColumn(label='Generate Rec?',
                                                                  disabled=False),
                  'seeds_only': st.column_config.CheckboxColumn(label='Seeds only?',
                                                                  disabled=False),
                  'ratings_weight': st.column_config.NumberColumn(label="ELO Bias",
                                                                  min_value=1,
                                                                  max_value=20),
                  'recency_weight': st.column_config.NumberColumn(label="Recency Bias",
                                                                  min_value=1,
                                                                  max_value=20),
                  'randomness_weight': st.column_config.NumberColumn(label="Random Bias",
                                                                  min_value=1,
                                                                  max_value=20),
                  'minutes_to_sync': st.column_config.NumberColumn(label="Sync Minutes",
                                                                   min_value=30,
                                                                   max_value=9999,
                                                                   step=30),
                  'playlist_id': None
                  }
    key_val = 'de_playlist_config_df'
    if is_selection:
        st.dataframe(data=ss.pc_df,
                      key=f"{key_val}_selection",
                     hide_index=True,
                     column_order=cols,
                     column_config=col_config,
                     selection_mode="single-row",
                     on_select="rerun"
                      )
    else:
        st.data_editor(data=ss.pc_df,
                   key=key_val,
                   on_change=sync_df_from_data_editor,
                   num_rows="fixed",
                   column_order=cols,
                   column_config=col_config,
                   hide_index=True,
                   args=(ss.pc_df, key_val, 'playlist_id'))
    return

def render_shuffle_df(rcw, rtw, rnw, mts):
    # Renders & rerenders the dataframe as adjustments are made
    if "shuffle_df" not in ss or ss.shuffle_df.empty:
        return None

    df = ss.shuffle_df.copy()
    max_dur = int(df['duration_s'].max())

    cols = ['new_track_order',
            'track_artist',
            'recency_pct',
            'ratings_pct',
            'random_pct',
            'duration_s',
            'track_id',
            'target_playlist_id']

    col_config = {
        'track_artist': st.column_config.TextColumn(label='Song',
                                                    pinned=False,
                                                    width="medium",
                                                    disabled=True),
        'recency_pct': st.column_config.ProgressColumn(label="Last Heard",
                                                       min_value=0,
                                                       max_value=1,
                                                       width=30),
        'ratings_pct': st.column_config.ProgressColumn(label="Rating",
                                                       min_value=0,
                                                       max_value=1,
                                                       width=30),
        'random_pct': st.column_config.ProgressColumn(label="Random",
                                                      min_value=0,
                                                      max_value=1,
                                                      width=30),
        'duration_s':st.column_config.ProgressColumn(label="Duration",
                                                     min_value=0,
                                                     max_value=max_dur,
                                                     width=30,
                                                     format='%d'),
        'track_id': None,
        'target_playlist_id': None}

    # Update Dataframe order
    df['play_score'] = ((df['ratings_pct'] * rtw) +
                            (df['recency_pct'] * rcw) +
                            (df['random_pct'] * (rnw / 10)))

    df = df.sort_values(by="play_score", ascending=False).reset_index(drop=True)

    if mts != 9999:
            df['running_sum_min'] = (df['duration_s'].cumsum()) / 60
            df = df[df['running_sum_min'] <= mts].copy()

    st.dataframe(data=df,
                 column_order=cols,
                 column_config=col_config,
                 hide_index=True)

    return df



