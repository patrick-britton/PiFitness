import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from backend_functions.database_functions import get_log_tables, get_conn


def render_homepage():
    st.info("Wow, such empty")


def clear_log_cache():
    # Clear the cached log dataframe when table selection changes.
    if "log_table_df" in ss:
        ss.log_table_df = None


def log_display():
    if "table_list" not in ss:
        ss.table_list = get_log_tables()
        st.rerun()

    if "table_selection" not in ss:
        ss.table_selection = ss.table_list[0]


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
                                                                        pinned=False,
                                                                        disabled=True
                                                                       ),
                  "size_before_mb": st.column_config.ProgressColumn(label='Size Before',
                                                                    min_value=0,
                                                                    format='plain',
                                                                    pinned=False,
                                                                    disabled=True),
                  "size_after_mb": st.column_config.ProgressColumn(label='Size After',
                                                                    min_value=0,
                                                                    format='plain',
                                                                    pinned=False,
                                                                    disabled=True),
                  "maintenance_time_ms": st.column_config.ProgressColumn(label='Maint. Time',
                                                                   min_value=0,
                                                                   format='plain',
                                                                   pinned=False,
                                                                   disabled=True),
                  "total_time_ms": st.column_config.ProgressColumn(label='Total Time',
                                                                   min_value=0,
                                                                   format='plain',
                                                                   pinned=False,
                                                                   disabled=True),
                  "record_id": None,
                  "extract_time_ms": st.column_config.ProgressColumn(label="Extract",
                                                                 min_value=0,
                                                                 format='%d',
                                                            pinned=False,
                                                            disabled=True),
                  "load_time_ms": st.column_config.ProgressColumn(label="Load",
                                                                     min_value=0,
                                                                     format='%d',
                                                                     pinned=False,
                                                                     disabled=True),
                  "Transform": st.column_config.ProgressColumn(label="Transform/Execute",
                                                                     min_value=0,
                                                                     format='%d',
                                                                     pinned=False,
                                                                     disabled=True),
                  "token_age_s": st.column_config.ProgressColumn(label="Token Age",
                                                                 min_value=0,
                                                                 format='%d',
                                                            pinned=False,
                                                            disabled=True),
                  "total_size_mb": st.column_config.ProgressColumn(label='Total Size',
                                                                   min_value=0,
                                                                   format="plain",
                                                            pinned=False,
                                                            disabled=True),
                  "table_size_mb": st.column_config.ProgressColumn(label='Tables',
                                                                   min_value=0,
                                                                   format="plain",
                                                                   pinned=False,
                                                                   disabled=True),
                  "index_size_mb": st.column_config.ProgressColumn(label='Indexes',
                                                                   min_value=0,
                                                                   format="plain",
                                                                   pinned=False,
                                                                   disabled=True),
                  "failure_type": st.column_config.TextColumn(label="Error",
                                                            pinned=False,
                                                            disabled=True),
                  "error_text": st.column_config.TextColumn(label="Error",
                                                            pinned=False,
                                                            disabled=True)}

    if "log_table_df" in ss and ss.log_table_df is not None:
        st.data_editor(df=ss.log_table_df, hide_index=True,
                       column_config=log_config)
    else:
        sql = f"""SELECT * FROM logging.{ss.table_selection} ORDER BY event_time_utc DESC"""
        df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
        df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], utc=True)
        df["event_time_local"] = df["event_time_utc"].dt.tz_convert("America/Los_Angeles")
        ss.log_table_df = df
        st.rerun()


    ss.table_selection = st.radio(label="Which Log table to display:",
                                  options=ss.table_list,
                                  key="home_log_table_selection",
                                  index=0,
                                  on_change=clear_log_cache)


