import streamlit as st
from streamlit import session_state as ss
from backend_functions.helper_functions import reverse_key_lookup
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from frontend_functions.admin_module import render_admin_module
from frontend_functions.homepage import render_homepage


def top_button_dictionary():
    d = {"home": ":material/home:",
         "admin": ":material/shield_person:"}
    return d

def render_skeleton():
    st.set_page_config(layout="wide")
    tbd = top_button_dictionary()

    ss.top_nav_selection = st.pills(label="top_nav_selection",
                                    label_visibility="hidden",
                                    options=list(set(tbd.values())),
                                    default=tbd.get("home"))

    ss.simple_selection = reverse_key_lookup(tbd, ss.top_nav_selection)
    if ss.simple_selection == 'home':
        render_homepage()
    elif ss.simple_selection == 'admin':
        render_admin_module()