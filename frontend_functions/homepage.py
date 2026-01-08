import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.image_creation import render_task_summary_svg


def render_homepage():
    # st.write(render_task_summary_svg())
    st.image(render_task_summary_svg())
    return






