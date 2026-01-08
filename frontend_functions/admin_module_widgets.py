from datetime import datetime, timezone

import pandas as pd
import altair as alt
from backend_functions.database_functions import get_conn
import streamlit as st
from streamlit import session_state as ss


def task_execution_chart():
    sql = "SELECT * FROM logging.vw_task_executions"
    df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    if df.empty:
        return

    # width = 250 if ss.is_mobile else 700
    # row_height = 5
    task_count = df["task_name"].nunique()
    max_extract = int(df['max_extract'].iloc[0])+1
    max_load = int(df['max_load'].iloc[0])+1
    max_transform = int(df['max_transform'].iloc[0])+1
    max_elt = int(df['max_elt'].iloc[0])+1

    cols = ['task_name',
            'success_pct',
            'median_extract_s',
            'median_load_s',
            'median_transform_s',
            'etl_time_s']

    col_config = {'task_name': st.column_config.TextColumn(label='Name',
                                                           pinned=True,
                                                           disabled=True),
            'success_pct': st.column_config.ProgressColumn(label='Success%',
                                                           pinned=False,
                                                           width=20,
                                                           min_value=0,
                                                           max_value=1,
                                                           format="%.2f%%"),
            'median_extract_s': st.column_config.ProgressColumn(label='E',
                                                           pinned=False,
                                                           width=20,
                                                           min_value=0,
                                                           max_value=max_extract,
                                                                format="%.2f"
                                                                ),
            'median_load_s': st.column_config.ProgressColumn(label='L',
                                                           pinned=False,
                                                           width=20,
                                                           min_value=0,
                                                           max_value=max_load,
                                                                format="%.2f"),
            'median_transform_s': st.column_config.ProgressColumn(label='T',
                                                           pinned=False,
                                                           width=20,
                                                           min_value=0,
                                                           max_value=max_transform,
                                                                format="%.2f"),
            'etl_time_s': st.column_config.BarChartColumn(label=None,
                                                           pinned=False,
                                                           width="large",
                                                          color="auto-inverse",
                                                           y_min=0,
                                                           y_max=max_elt,
                                                                format="%.2f")}

    st.write(f"__{task_count}__ tasks with history in last 30 days")
    st.dataframe(data=df,
                 column_order=cols,
                 column_config=col_config,
                 height="content",
                 hide_index=True
                 )
    return