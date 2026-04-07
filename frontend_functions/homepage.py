import os
import sys
from pathlib import Path

import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import one_sql_result
from backend_functions.elevation_tiles import reconcile_elevation_tiles
from backend_functions.service_logins import sql_rate_limited, rate_limit_test, garmin_creds
from frontend_functions.music_module import rating_display_module


def render_test_widget():
    ss.step_no=0

    if ss.step_no==0:
        if st.button('Path Test'):
            ss.project_root = Path(__file__).parent.absolute().parent
        # Point to the 'src' directory inside the cloned repo
            ss.pirate_src_path = ss.project_root / "pirate-garmin_clone" / "src"
            st.write(f"Root: {ss.project_root} || Path: {ss.pirate_src_path}")
            ss.step_no=1

    if ss.step_no==1:
        if ss.pirate_src_path.exists():
            sys.path.insert(0, str(ss.pirate_src_path))
            ss.step_no = 2
        else:
            st.error(f"Warning: Could not find source at {ss.pirate_src_path}")


    if ss.step_no==2:
        if st.button('Imports'):
            try:
                from pirate_garmin.cli import app
                from typer.testing import CliRunner
                ss.step_no = 3
            except Exception as e:
                st.error(f'Imports Failed: {e}')


            # 3. Credential Setup
    if ss.step_no==3:
        if st.button('Cred Setup'):

            ss.email, ss.password = garmin_creds()
            os.environ["GARMIN_USERNAME"] = ss.email
            os.environ["GARMIN_PASSWORD"] = ss.password
            # Set to "true" for your Raspberry Pi later, "false" for Windows testing
            os.environ["PIRATE_GARMIN_HEADLESS"] = "false"
            st.success('Creds Established')
            ss.step_no=4

    if ss.step_no==4:
        if st.button('Client Runner'):

            # 4. Execution
            ss.runner = CliRunner()
            st.success('Client Runner established')

            if st.button('Final Login'):
                result = ss.runner.invoke(app, ["login"])
                st.info(f'Full success: {result}')
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





