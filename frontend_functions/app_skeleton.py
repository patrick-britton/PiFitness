import streamlit as st

from backend_functions.database_functions import get_conn
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms


def render_homepage():
    t0 = start_timer()
    try:
        get_conn()
        st.success(f"DB Connection Successful in {elapsed_ms(t0)} ms!")
        log_app_event(cat='Database', desc='First DB Connection', exec_time=elapsed_ms(t0))
    except Exception as e:
        st.error(f"DB Connection Failure in {elapsed_ms(t0)} ms //n {e}")