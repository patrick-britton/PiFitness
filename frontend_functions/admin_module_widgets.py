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

    chart_width = 250 if ss.is_mobile else 700
    row_height = 5
    pass_color = "white" if ss.is_dark_mode else "darkslategrey"
    max_etl = df["etl_time_s"].max()
    task_count = df["task_name"].nunique()
    x_min = df["event_time_utc"].min()
    x_max = datetime.now(timezone.utc)
    total_height = (task_count * row_height) + 10
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                "event_time_utc:T",
                scale=alt.Scale(domain=[x_min, x_max]),
                axis=alt.Axis(title=None, labels=False, ticks=False),
            ),
            y=alt.Y(
                "etl_time_s:Q",
                scale=alt.Scale(domain=[0, max_etl]),
                axis=alt.Axis(title=None, labels=False, ticks=False),
            ),
            color=alt.condition(
                alt.datum.is_failure,
                alt.value("red"),
                alt.value(pass_color),
            ),
            row=alt.Row(
                "task_name:N",
                header=alt.Header(
                    title=None,
                    labelAngle=0,
                    labelAlign="right",
                    labelOrient="left",
                    labelBaseline="middle",
                    labelPadding=3
                ),
                sort=alt.SortField(
                    field="task_rank",
                    order="ascending")
            ),
            tooltip=[
                "task_name:N",
                "event_time_utc:T",
                "etl_time_s:Q",
                "is_failure:N",
            ],
        )
        .properties(
            width=chart_width,
            height=total_height,
        )
        .configure_view(stroke=None)
        .configure_axis(grid=False)
        .configure_facet(spacing=5)
    )
    box = st.container(border=True)
    with box:
        st.write("__Task Executions__:")
        st.altair_chart(chart)
    return