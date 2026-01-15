import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import one_sql_result
from backend_functions.logging_functions import start_timer, elapsed_ms
from backend_functions.service_logins import sql_rate_limited, rate_limit_test

from backend_functions.viz_factory.task_summary import render_task_summary_dashboard
from backend_functions.viz_factory.db_size import render_db_size_dashboard


def render_homepage():
    rate_limit_widget()
    st.info('Wow, such empty :(')
    return


def rate_limit_widget():
    if sql_rate_limited():
        st.warning('__:material/brightness_alert: SPOTIFY CURRENTLY UNDER RATE LIMITATIONS__')
        sql = "SELECT rate_limit_cleared_utc from api_services.api_service_list where api_service_name = 'Spotify'"
        until = one_sql_result(sql)
        st.write(f'Expires at: {until}')
        ss.rate_limited = True
    else:
        ss.rate_limited = False
        if st.button(':material/reset_wrench: Rate Limits not detected. Reset & Retest'):
            if sql_rate_limited():
                st.warning('__:material/brightness_alert: SPOTIFY CURRENTLY UNDER RATE LIMITATIONS__')
                sql = "SELECT rate_limit_cleared_utc from api_services.api_service_list where api_service_name = 'Spotify'"
                until = one_sql_result(sql)
                st.write(f'Expires at: {until}')
                ss.rate_limited = True
            else:
                ss.rate_limited, seconds = rate_limit_test(sp_token=None)
                if ss.rate_limited:
                    st.warning('__:material/brightness_alert: SPOTIFY CURRENTLY UNDER RATE LIMITATIONS__')
                    st.write(f"Expires in {seconds/60} minutes")
                else:
                    st.balloons()
                    st.success("You are not under rate limitations!")
    return





