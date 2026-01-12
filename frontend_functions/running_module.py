import streamlit as st
from streamlit import session_state as ss

from backend_functions.task_execution import task_executioner
from frontend_functions.nav_buttons import nav_widget


def render_running_module():
    nav_selection = nav_widget('running', 'Run Options')

    if nav_selection is None:
        nav_selection = 'run_charting'

    if nav_selection == 'run_charting':
        render_run_charting()
    elif nav_selection == 'new_run_process':
        process_new_run()
    elif nav_selection == 'run_forecast':
        render_run_forecast()
    else:
        st.info(f'Uncaught run navigation: {nav_selection}')
    return


def render_run_charting():
    st.info('Run Charting not yet built')
    return


def render_run_forecast():
    st.info('Run forecasting not yet built')
    return


def process_new_run():

    # Sync All activities
    if ss.get("new_run_synced") is None:
        task_executioner('Sync Garmin Activities')
        ss.new_run_synced = True

    # Get most recent activity details


    # Ask which playlist was utilized


    # Insert listening history


    # Get activity specific details


    # Generate charting

    return



