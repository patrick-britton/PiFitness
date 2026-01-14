import streamlit as st
from streamlit import session_state as ss
import pandas as pd

from backend_functions.database_functions import get_conn
from backend_functions.task_execution import task_executioner
from frontend_functions.music_module import render_playlist_shuffle
from frontend_functions.music_widgets import playlist_config_table
from frontend_functions.nav_buttons import nav_widget


def render_running_module():
    nav_selection = nav_widget('running', 'Run Options')

    if nav_selection is None:
        nav_selection = 'run_charting'

    if nav_selection == 'run_charting':
        render_run_charting()
    elif nav_selection == 'new_run_process':
        process_new_run()
    elif nav_selection == 'run_forecast':
        render_run_forecast()
    else:
        st.info(f'Uncaught run navigation: {nav_selection}')
    return


def render_run_charting():
    st.info('Run Charting not yet built')
    return


def render_run_forecast():
    st.info('Run forecasting not yet built')
    return


def process_new_run():

    # Sync All activities
    if ss.get("new_run_synced") is None:
        with st.spinner('Making sure I have all known activities', show_time=True):
            task_executioner('Sync Garmin Activities')
            ss.new_run_synced = True


    # Get most recent activity details
    if ss.get("listens_df") is None:
        sel_sql = """SELECT * FROM activities.vw_watch_music_heard;"""
        ss.listens_df = pd.read_sql(sel_sql, get_conn(alchemy=True))

    df = ss.listens_df.copy()
    options = df['playlist_name'].unique().tolist()
    options.append('No playlist')

    sel = st.segmented_control(label='Which playlist did you listen to?',
                               options=options,
                               key='sc_pl_selection')
    if not sel:
        st.write(':gray[*make your selection above*]')
        return

    if sel == 'No playlist':
        st.info('What did you even come here for?')
        return


    filtered_df = df[df['playlist_name'] == sel].copy()
    cols = ['track_order', 'track_name_clean', 'artist_display_name', 'played_at_utc']
    col_config = {'track_order': st.column_config.NumberColumn(label='#',
                                                               pinned=True,
                                                               disabled=True,
                                                               format='%d'),
                'track_name_clean': st.column_config.TextColumn(label='Title',
                                                                pinned=False,
                                                                disabled=True),
                  'artist_display_name': st.column_config.TextColumn(label='Artist',
                                                                     pinned=False,
                                                                     disabled=True),
                  'played_at_utc': st.column_config.DatetimeColumn(label='Played At',
                                                                   format='distance',
                                                                   pinned=False,
                                                                   disabled=True)
                  }

    st.write('You heard these songs')
    st.dataframe(filtered_df, column_order=cols, column_config=col_config, hide_index=True, on_select='ignore')
    cols = ['played_at_utc', 'isrc', 'playlist_id']
    if st.button(':material/database_upload: Insert into listening history'):
        with st.spinner('Loading to SQL', show_time=True):
            narrow_df = filtered_df[cols]
            narrow_df.to_sql(schema='music', name='listening_history', con=get_conn(alchemy=True), if_exists='append', index=False )
            st.toast(f"{len(narrow_df)} tracks uploaded to SQL", duration=3)
            ss.rp_new_order = True
            ss.target_id = narrow_df['playlist_id'].iloc[0]

    if not ss.get("rp_new_order"):
        return

    st.write('New playlist order will be:')
    render_playlist_shuffle(list_id=ss.target_id)

    return








    # Ask which playlist was utilized


    # Insert listening history


    # Get activity specific details


    # Generate charting

    return



