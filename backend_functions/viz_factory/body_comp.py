import streamlit as st
import pandas as pd
import altair as alt
from backend_functions.database_functions import get_conn


def render_weight_viz(sql, chart_type, xaxis, xlimit, history_limit, yaxis, ylabel):

    df = pd.read_sql(sql, con=get_conn(alchemy=True))

    # 2. Extract Scale Bounds
    # We assume lower_bound and upper_bound are consistent columns in the view
    upper_scale_pad = 1.05
    lower_scale_pad = .95


    y_min = df[yaxis].min().min() * lower_scale_pad
    y_max = df[yaxis].max().max()  * upper_scale_pad
    x_min = df[xaxis].min().min()
    x_max = df[xaxis].max().max()
    line_min = df[xlimit].min().min()
    line_max = df[xlimit].max().max()
    xshorthand = f"{xaxis}:Q"
    y_shorthand = f"{yaxis[0]}:Q"
    color_shorthand = f"{xlimit}:N"
    opacity_shorthand = f"{xlimit}:Q"

    # 3. Create Altair Chart
    chart = alt.Chart(df).mark_line(interpolate='monotone').encode(
        x=alt.X(xshorthand,
                title='Day #',
                scale=alt.Scale(domain=[x_min, x_max])),
        y=alt.Y(y_shorthand,
                title=ylabel,
                scale=alt.Scale(domain=[y_min, y_max], clamp=True)),
        # Color by year
        color=alt.Color(color_shorthand,
                        scale=alt.Scale(scheme='greys'),
                        title=chart_type,
                        sort='ascending'),
        # Dynamic Opacity: More transparent for older years (-6)
        opacity=alt.Opacity(opacity_shorthand,
                            scale=alt.Scale(domain=[line_min, line_max], range=[0.05, 1.0]),
                            legend=None),
        # Dynamic Size: Thinner for older years (-6)
        size=alt.Size(opacity_shorthand,
                      scale=alt.Scale(domain=[line_min, line_max], range=[0.3, 5]),
                      legend=None),
        tooltip=[
            alt.Tooltip(xlimit, title='Offset'),
            alt.Tooltip(xaxis, title='Day'),
            alt.Tooltip(yaxis[0], title=ylabel, format='.1f')
        ]
    ).properties(
        width='container',
        height=500,
        title='Yearly Weight Progression Comparison'
    ).interactive()

    # 4. Render in Streamlit
    st.altair_chart(chart, width="stretch")