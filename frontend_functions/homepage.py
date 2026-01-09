import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.image_creation import render_task_summary_svg, render_db_size_summary
from backend_functions.viz_factory.task_summary import render_task_summary_dashboard


def render_homepage():
    # st.write(render_task_summary_svg())
    render_task_summary_dashboard(is_dark_mode=ss.get("is_dark_mode"), is_mobile=ss.get("is_mobile"))
    st.divider()

    return






