import pandas as pd
import streamlit as st
import altair as alt

# Ensure you have your db connection import
from backend_functions.database_functions import get_conn


def clean_pg_array(val):
    if isinstance(val, list): return val
    if not val or val == '{}': return []
    # Remove braces and split
    return [float(x) for x in str(val).strip('{}').split(',') if x]


def render_task_summary_dashboard(is_dark_mode=True, is_mobile=False):
    st.write("__Task Summary__")

    df = pd.read_sql("SELECT * FROM tasks.vw_task_summary_chart", con=get_conn(alchemy=True))
    if df.empty: return

    df['etl_time_s'] = df['etl_time_s'].apply(clean_pg_array)
    df['row_index'] = range(len(df))

    # Convert numeric columns to float to avoid Decimal issues
    numeric_cols = ['median_extract_s', 'median_load_s', 'median_transform_s', 'max_elt']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Prepare text labels
    text_data = []
    for idx, row in df.iterrows():
        text_data.append({
            'row_index': idx,
            'task_name': row['task_name'],
            'age_label': row['age_label'],
        })
    text_df = pd.DataFrame(text_data)

    # Prepare stacked bar data
    stack_data = []
    for idx, row in df.iterrows():
        stack_data.extend([
            {'row_index': idx, 'type': 'Extract', 'value': float(row['median_extract_s']), 'order': 0},
            {'row_index': idx, 'type': 'Load', 'value': float(row['median_load_s']), 'order': 1},
            {'row_index': idx, 'type': 'Transform', 'value': float(row['median_transform_s']), 'order': 2}
        ])
    stack_df = pd.DataFrame(stack_data)

    # Explode sparkline data with adjusted y-position
    spark_data = []
    max_val = float(df['max_elt'].max())

    for idx, row in df.iterrows():
        history = row['etl_time_s']
        for i, val in enumerate(history):
            val_float = float(val)
            normalized_val = (val_float / max_val) * 0.8
            spark_data.append({
                'row_index': idx,
                'position': i,
                'y_bottom': idx,
                'y_top': idx + normalized_val,
                'is_last': i == len(history) - 1
            })
    spark_df = pd.DataFrame(spark_data)

    # Theme colors
    text_color = "#e5e7eb" if is_dark_mode else "#0f172a"
    sub_text_color = "#9ca3af" if is_dark_mode else "#64748b"
    border_color = "#334155" if is_dark_mode else "#e2e8f0"

    width = 450 if is_mobile else 750
    row_height = 35
    total_height = row_height * len(df)
    sparkline_width = 400

    # Column 1: Task names
    # Calculate the max character length across both text fields
    max_task_chars = df['task_name'].str.len().max()
    max_age_chars = df['age_label'].str.len().max()

    # Use the larger of the two
    max_chars = max(max_task_chars, max_age_chars)

    # 7px is a good estimate for size 11 bold,
    # but you might want to add a small buffer (e.g., +10px) for the 'dx' offset
    dynamic_width = (max_chars * 1) + 0
    task_labels = alt.Chart(text_df).mark_text(
        align='left',
        baseline='bottom',
        dx=0,
        dy=-2,
        fontSize=11,
        fontWeight=700
    ).encode(
        y=alt.Y('row_index:O', axis=None),
        text='task_name:N',
        color=alt.value(text_color)
    ).properties(width=dynamic_width, height=total_height)

    # Age labels
    age_labels = alt.Chart(text_df).mark_text(
        align='left',
        baseline='top',
        dx=0,
        dy=2,
        fontSize=9,
        fontStyle='italic'
    ).encode(
        y=alt.Y('row_index:O', axis=None),
        text='age_label:N',
        color=alt.value(sub_text_color)
    ).properties(width=dynamic_width, height=total_height)

    text_column = (task_labels + age_labels)

    # Column 2: Stacked bars
    bars = alt.Chart(stack_df).mark_bar(size=15).encode(
        x=alt.X('value:Q', stack='zero', axis=None),
        y=alt.Y('row_index:O', axis=None),
        color=alt.Color('type:N',
                        scale=alt.Scale(domain=['Extract', 'Load', 'Transform'],
                                        range=['#3b82f6', '#f0690f', '#B7B7B7']),
                        legend=None),
        order='order:O'
    ).properties(width=100, height=total_height)

    # Column 3: Sparklines
    sparks = alt.Chart(spark_df).mark_rect().encode(
        x=alt.X('position:O', axis=None),
        y=alt.Y('y_bottom:Q', axis=None, scale=alt.Scale(domain=[-0.1, len(df)])),
        y2='y_top:Q',
        color=alt.condition(
            alt.datum.is_last,
            alt.value('#3b82f6'),
            alt.value('#475569')
        )
    ).properties(width=sparkline_width, height=total_height)

    # Add row separators (same width as sparklines)
    separators = alt.Chart(pd.DataFrame({'y': range(1, len(df))})).mark_rule(
        strokeWidth=1,
        color=border_color
    ).encode(
        y=alt.Y('y:Q', axis=None, scale=alt.Scale(domain=[-0.1, len(df)]))
    ).properties(width=sparkline_width, height=total_height)

    # Combine sparklines and separators
    sparkline_column = sparks + separators

    # Combine all columns
    chart = alt.hconcat(
        text_column,
        bars,
        sparkline_column,
        spacing=15 # Adjust for horizontal gap between columns
    ).configure_view(
        strokeWidth=0
    ).configure_concat(
        spacing=0
    ).configure(
        # VITAL: This removes the "halo" of white space around the entire chart
        padding={"left": -20, "top": 0, "right": 0, "bottom": 0}
    )

    st.altair_chart(chart, use_container_width=False)
    return