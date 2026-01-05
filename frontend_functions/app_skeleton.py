import streamlit as st
from streamlit import session_state as ss
from backend_functions.helper_functions import reverse_key_lookup
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from frontend_functions.admin_module import render_admin_module
from frontend_functions.homepage import render_homepage
from frontend_functions.nav_buttons import nav_button
from frontend_functions.streamlit_helpers import ss_debug





def render_skeleton():
    st.set_page_config(layout="wide")
    nav_key = 'main'

    nav_button(nav_key)

    nav_selection = ss.get(f"{nav_key}_active_decode")
    if not nav_selection:
        nav_selection='home'

    # Current options:
    # {"home": {'icon': "home"},
    #  'music': {'icon': "music_cast"},
    #  "running": {'icon': "sprint"},
    #  "food": {'icon': "local_dining"},
    #  "admin": {'icon': "shield_person"}
    if nav_selection == 'home':
        render_homepage()
    elif nav_selection == 'admin':
        render_admin_module()
    elif nav_selection == 'music':
        st.info('MUSIC NOT YET BUILT')
    elif nav_selection == 'running':
        st.info('RUNNING NOT YET BUILT')
    elif nav_selection == 'food':
        st.info('FOOD NOT YET BUILT')
    else:
        st.error('Uncaught navigation selection')

    ss.qgp = st.query_params()
    debug_var_list = [f"{nav_key}_current",
                      f"{nav_key}_active",
                      f"{nav_key}_active_decode",
                      "qgp"
                      ]
    ss_debug(debug_var_list)
    return