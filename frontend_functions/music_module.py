import streamlit as st
from streamlit import session_state as ss

from frontend_functions.music_widgets import playlist_config_table
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
        st.info(f"{nav_selection} module not yet built")
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
    st.info('Button - sync playlists')
    playlist_config_table()

    return

