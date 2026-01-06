import numpy as np
import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_conn, qec
from backend_functions.helper_functions import convert_to_json_serializable
from frontend_functions.streamlit_helpers import sync_df_from_data_editor


def playlist_config_table():
    sql = """SELECT * FROM music.playlist_config
            ORDER BY is_active DESC, 
            seeds_only asc,
            auto_shuffle DESC,
            make_recs DESC,
            track_count DESC;"""

    ss.pc_df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    # Convert entire dataframe to JSON-serializable types
    ss.pc_df = ss.pc_df.map(convert_to_json_serializable)

    if ss.pc_df.empty:
        st.info("Sync playlists to configure")
        return

    # set the configuration
    max_songs = int(ss.pc_df["track_count"].max())

    cols = ['playlist_name',
            'track_count',
            'auto_shuffle',
            'make_recs',
            'seeds_only',
            'ratings_weight',
            'recency_weight',
            'randomness_weight',
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
                  'playlist_id': None
                  }
    key_val = 'de_playlist_config_df'
    st.data_editor(data=ss.pc_df,
                   key=key_val,
                   on_change=sync_df_from_data_editor,
                   num_rows="fixed",
                   column_order=cols,
                   column_config=col_config,
                   hide_index=True,
                   args=(ss.pc_df, key_val, 'playlist_id'))
    return





