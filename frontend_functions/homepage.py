import os
import sys
from pathlib import Path

import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import one_sql_result
from backend_functions.elevation_tiles import reconcile_elevation_tiles
from backend_functions.service_logins import (
    sql_rate_limited,
    rate_limit_test,
    garmin_creds,
    pirate_garmin_login,
)
from backend_functions.json_extractors import get_pirate_data
from frontend_functions.music_module import rating_display_module
from frontend_functions.streamlit_helpers import sse


def render_test_widget():
    """Dev-only widget to exercise the inlined Garmin auth client.

    Uses the backend's inlined pirate-garmin auth (backend_functions.pirate_garmin_auth
    via service_logins.pirate_garmin_login) — no dependency on the pirate-garmin_clone
    package being importable.
    """
    if not sse('step_no'):
        ss.step_no = 0

    if ss.step_no == 0:
        if st.button('Garmin Login Test'):
            try:
                result = pirate_garmin_login()
            except Exception as e:
                st.error(f'Garmin login failed: {e}')
                result = None

            if result and result.get("client"):
                ss.garmin_client = result["client"]
                st.success('Garmin client established')
                ss.step_no = 1
            else:
                st.error('Garmin login returned no client')

    if ss.step_no == 1:
        if st.button('Test Endpoint'):
            try:
                client = ss.garmin_client
                # Exercise a simple endpoint through the inlined client
                data, error = get_pirate_data(client, "usersummary.daily")
                if error:
                    st.error(f'Endpoint error: {error}')
                else:
                    st.json(data)
            except Exception as e:
                st.error(f'Request failed: {e}')

    st.write(f"Step no: {ss.get('step_no')}")
    return




def render_homepage():
    rate_limit_widget()
    rating_display_module()
    dupe_widget()
    render_test_widget()

    if st.button('Load Mapping Tiles'):
        reconcile_elevation_tiles()
    return




def dupe_widget():
    sql = "SELECT COUNT(*) FROM music.vw_isrc_dupe_review"
    isrc_count = one_sql_result(sql)
    if isrc_count >0:
        st.info(f"{int(isrc_count/2)} potential duplicate isrcs found")
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





