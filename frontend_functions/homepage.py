import pandas as pd
import streamlit as st
from streamlit import session_state as ss
from backend_functions.logging_functions import start_timer, elapsed_ms


from backend_functions.viz_factory.task_summary import render_task_summary_dashboard
from backend_functions.viz_factory.db_size import render_db_size_dashboard


def render_homepage():

    st.info('Wow, such empty :(')
    return






