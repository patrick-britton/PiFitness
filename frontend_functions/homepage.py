import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import one_sql_result
from backend_functions.elevation_tiles import reconcile_elevation_tiles
from backend_functions.service_logins import sql_rate_limited, rate_limit_test
from frontend_functions.music_module import rating_display_module



def render_homepage():
    rate_limit_widget()
    rating_display_module()
    dupe_widget()

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





