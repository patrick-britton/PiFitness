from datetime import datetime, timezone

import pandas as pd
import altair as alt
from backend_functions.database_functions import get_conn
import streamlit as st


def task_execution_chart():
    sql = "SELECT * FROM logging.vw_task_executions"
    df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    if df.empty:
        return

    chart_width = 500
    row_height = 40
    max_etl = df["etl_time_s"].max()
    task_count = df["task_name"].nunique()
    x_min = df["event_time_utc"].min()
    x_max = datetime.now(timezone.utc)
    total_height = task_count * row_height
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
                alt.value("black"),
            ),
            row=alt.Row(
                "task_name:N",
                header=alt.Header(
                    title=None,
                    labelAngle=0,
                    labelAlign="left",
                ),
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
    st.altair_chart(chart)
    return