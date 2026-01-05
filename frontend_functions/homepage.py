import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_log_tables, get_conn, get_log_data
from backend_functions.helper_functions import col_value, add_time_ago_column
from frontend_functions.admin_module_widgets import task_execution_chart


def render_homepage():
    task_execution_chart()
    return






