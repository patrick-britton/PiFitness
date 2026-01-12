import streamlit as st
from streamlit import session_state as ss
from backend_functions.helper_functions import reverse_key_lookup
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from frontend_functions.admin_module import render_admin_module
from frontend_functions.health_module import render_health_module
from frontend_functions.homepage import render_homepage
from frontend_functions.music_module import render_music
from frontend_functions.nav_buttons import nav_button, nav_widget
from frontend_functions.running_module import render_running_module
from frontend_functions.streamlit_helpers import ss_debug


def init_session():
    st.set_page_config(layout="wide")
    if "is_dark_mode" not in ss:
        ss.is_dark_mode = st.context.theme.type == "dark"
        device = st.query_params.to_dict().get("device")
        ss.is_mobile = False
        if device:
            ss.is_mobile = device == 'mobile'
    return


def render_skeleton():
    t0= start_timer()
    init_session()

    nav_selection = nav_widget(nav_key='main', nav_title='Modules')

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
        render_music()
    elif nav_selection == 'running':
        render_running_module()
    elif nav_selection == 'food':
        st.info('FOOD NOT YET BUILT')
    elif nav_selection == 'health':
        render_health_module()
    else:
        st.error(f'Uncaught skeleton navigation selection: {nav_selection}')


    st.write(f":gray[*Rendered in {elapsed_ms(t0)} ms*]")

    ss.qgp = st.query_params.to_dict()
    debug_var_list = ['np_action_choice',
                      'is_dark_mode'
                      ]
    ss_debug(debug_var_list)
    return