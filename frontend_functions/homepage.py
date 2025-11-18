import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_log_tables, get_conn, get_log_data


def render_homepage():
    log_display()


def log_display():
    if "table_list" not in ss:
        ss.table_list = get_log_tables(as_list=True)

    if "table_selection" not in ss:
        ss.table_selection = 'task_executions'
        ss.log_table_df = get_log_data(ss.table_selection)

    log_config = {"event_time_utc": None,
                  "event_time_local": st.column_config.DatetimeColumn(label="Time",
                                                                      pinned=True,
                                                                      disabled=True),
                  "api_service_name": st.column_config.TextColumn(label="API",
                                                                  pinned=False,
                                                                  disabled=True),
                  "event_category": st.column_config.TextColumn(label="Category",
                                                            pinned=False,
                                                            disabled=True),
                  "event_description": st.column_config.TextColumn(label="Desc",
                                                            pinned=False,
                                                            disabled=True),
                  "event_name": st.column_config.TextColumn(label="Event",
                                                            pinned=False,
                                                            disabled=True),
                  "table_name": st.column_config.TextColumn(label="Table",
                                                            pinned=False,
                                                            disabled=True),
                  "maintenance_type": st.column_config.TextColumn(label="Type",
                                                                  pinned=False,
                                                                  disabled=True),
                  "task_name": st.column_config.TextColumn(label="Task",
                                                           pinned=False,
                                                           disabled=True),
                  "execution_time_ms": st.column_config.ProgressColumn(label="Execution Time",
                                                                       min_value=0,
                                                                       format='plain',
                                                                        pinned=False
                                                                       ),
                  "size_before_mb": st.column_config.ProgressColumn(label='Size Before',
                                                                    min_value=0,
                                                                    format='plain',
                                                                    pinned=False),
                  "size_after_mb": st.column_config.ProgressColumn(label='Size After',
                                                                    min_value=0,
                                                                    format='plain',
                                                                    pinned=False),
                  "maintenance_time_ms": st.column_config.ProgressColumn(label='Maint. Time',
                                                                   min_value=0,
                                                                   format='plain',
                                                                   pinned=False),
                  "total_time_ms": st.column_config.ProgressColumn(label='Total Time',
                                                                   min_value=0,
                                                                   format='plain',
                                                                   pinned=False),
                  "record_id": None,
                  "extract_time_ms": st.column_config.ProgressColumn(label="Extract",
                                                                 min_value=0,
                                                                 format='%d',
                                                            pinned=False),
                  "load_time_ms": st.column_config.ProgressColumn(label="Load",
                                                                     min_value=0,
                                                                     format='%d',
                                                                     pinned=False),
                  "Transform": st.column_config.ProgressColumn(label="Transform/Execute",
                                                                     min_value=0,
                                                                     format='%d',
                                                                     pinned=False),
                  "token_age_s": st.column_config.ProgressColumn(label="Token Age",
                                                                 min_value=0,
                                                                 format='%d',
                                                            pinned=False),
                  "total_size_mb": st.column_config.ProgressColumn(label='Total Size',
                                                                   min_value=0,
                                                                   format="plain",
                                                            pinned=False),
                  "table_size_mb": st.column_config.ProgressColumn(label='Tables',
                                                                   min_value=0,
                                                                   format="plain",
                                                                   pinned=False),
                  "index_size_mb": st.column_config.ProgressColumn(label='Indexes',
                                                                   min_value=0,
                                                                   format="plain",
                                                                   pinned=False),
                  "failure_type": st.column_config.TextColumn(label="Error",
                                                            pinned=False,
                                                            disabled=True),
                  "error_text": st.column_config.TextColumn(label="Error",
                                                            pinned=False,
                                                            disabled=True)}

    if "log_table_df" in ss and ss.log_table_df is not None:
        st.data_editor(ss.log_table_df, hide_index=True,
                       column_config=log_config)
    else:
        ss.log_table_df = get_log_data(ss.table_selection)
        st.rerun()

    cols = st.columns(len(ss.table_list))
    for idx, table in enumerate(ss.table_list):
        with cols[idx]:
            button_type = "primary" if table == ss.table_selection else "secondary"
            if st.button(table, key=f"btn_{table}", type=button_type):
                if table != ss.table_selection:  # Only reload if different
                    ss.table_selection = table
                    ss.log_table_df = get_log_data(ss.table_selection)
                    st.rerun()


