import pandas as pd
import streamlit as st
import altair as alt

# Ensure you have your db connection import
from backend_functions.database_functions import get_conn, sql_to_dict


def clean_pg_array(val):
    if isinstance(val, list): return val
    if not val or val == '{}': return []
    # Remove braces and split
    return [float(x) for x in str(val).strip('{}').split(',') if x]


def render_task_summary_dashboard(is_dark_mode=True, is_mobile=False):
    st.write("__Task Summary__")

    sql = "SELECT * FROM tasks.vw_task_summary_chart"
    task_list = sql_to_dict(sql)
    if not task_list: return

    task_count = len(task_list)

    # Recency Colors
    recency_light_gray = "#d1d5db" if is_dark_mode else "#9ca3af"
    recency_dark_gray = "#6b7280" if is_dark_mode else "#4b5563"
    recency_light_red = "#f27878"
    recency_red = "#ef4444"
    recency_blue = "#3b82f6"

    # Prepare text labels
    text_data = []
    stack_data = []
    recency_data = []
    exec_data = []
    row_idx = -1
    one_day = -24*60
    day_half = -36 * 60
    lower_range = -48 * 60
    upper_range = 48 * 60
    max_exec = task_list[0].get('max_executions')
    max_exec = -10 if max_exec < -10 else max_exec
    for task in task_list:
        row_idx += 1
        if not task.get('is_active_failure'):
            text_data.append({
                'row_index': row_idx,
                'task_name': task.get('task_name'),
                'err_msg': f"Last: {task.get('time_ago_execution')}, Next: {task.get('time_ago_next')}, Value: {task.get('time_ago_value')}",
                'color': 'black'
            })
        else:
            text_data.append({
                'row_index': row_idx,
                'task_name': f"{task.get('task_name')}",
                'err_msg': f"{task.get('last_failure_msg')}",
                'color': 'red'
            })
        stack_data.extend([
            {'row_index': row_idx, 'type': 'Login', 'value': float(task.get('login_ms') or 0), 'order': 0, 'value_sec': round(float(task.get('login_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Extract', 'value': float(task.get('extract_ms') or 0), 'order': 1, 'value_sec': round(float(task.get('extract_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Load', 'value': float(task.get('load_ms') or 0), 'order': 2, 'value_sec': round(float(task.get('load_ms') or 0)/1000,0)},
            {'row_index': row_idx, 'type': 'Flatten', 'value': float(task.get('flatten_ms') or 0), 'order': 3, 'value_sec': round(float(task.get('flatten_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Parse', 'value': float(task.get('parse_ms') or 0), 'order': 4, 'value_sec': round(float(task.get('parse_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Interpolate', 'value': float(task.get('interpolation_ms') or 0), 'order': 5, 'value_sec': round(float(task.get('interpolation_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Forecast', 'value': float(task.get('forecasting_ms') or 0), 'order': 6, 'value_sec': round(float(task.get('forecasting_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Python', 'value': float(task.get('python_ms') or 0), 'order': 7, 'value_sec': round(float(task.get('python_ms') or 0)/1000,1)},
            {'row_index': row_idx, 'type': 'Admin', 'value': float(task.get('admin_ms') or 0), 'order': 8, 'value_sec': round(float(task.get('admin_ms') or 0)/1000,1)}
        ])
        exec_data.extend([
            {'row_index': row_idx, 'type': 'Executions', 'color': 'gray', 'value': -float(task.get('execution_count') or 0)}
        ])
        last_val = float(task.get('execution_minutes_ago') or 0)
        value_val = float(task.get('value_recency_minutes_ago') or 0)
        next_val = float(task.get('next_planned_execution_minutes') or 0)

        last_val = last_val if last_val > lower_range else lower_range
        value_val = value_val if value_val > lower_range else lower_range
        next_val = next_val if next_val > lower_range else lower_range
        next_val = next_val if next_val < upper_range else upper_range

        # Determine colors based on threshold
        if last_val > one_day:
            c_last = recency_light_gray
        elif last_val > day_half:
            c_last = recency_light_red
        else:
            c_last = recency_red

        if value_val > one_day:
            c_value = recency_dark_gray
        elif value_val > day_half:
            c_value = recency_light_red
        else:
            c_value = recency_red

        c_next = recency_blue

        recency_data.extend([
            {'row_index': row_idx, 'type': 'Last', 'value': last_val, 'color': c_last, 'time_val': task.get('last_executed_utc')},
            {'row_index': row_idx, 'type': 'Value', 'value': value_val, 'color': c_value, 'time_val': task.get('value_recency')},
            {'row_index': row_idx, 'type': 'Next', 'value': next_val, 'color': c_next, 'time_val': task.get('next_planned_execution_utc')}
        ])
    text_df = pd.DataFrame(text_data)
    stack_df = pd.DataFrame(stack_data)
    recency_df = pd.DataFrame(recency_data)
    exec_df = pd.DataFrame(exec_data)


    # Theme colors
    text_color = "#e5e7eb" if is_dark_mode else "#0f172a"
    sub_text_color = "#9ca3af" if is_dark_mode else "#64748b"
    border_color = "#334155" if is_dark_mode else "#e2e8f0"

    width = 450 if is_mobile else 750
    row_height = 35
    total_height = row_height * task_count
    sparkline_width = 400

    # Column 1: Task names
    # Calculate the max character length across both text fields
    max_task_chars = text_df['task_name'].str.len().max()
    max_err_chars = text_df['err_msg'].str.len().max()


    # Use the larger of the two
    max_chars = max(max_task_chars, max_err_chars)

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
        color=alt.Color('color:N', scale=None)
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
        text='err_msg:N',
        color=alt.value(sub_text_color)
    ).properties(width=dynamic_width, height=total_height)

    text_column = (task_labels + age_labels)


    # Colum 1B - execution count
    exec_width = 25
    exec_bar = alt.Chart(exec_df).mark_bar(size=10).encode(
        x=alt.X('value:Q', stack='zero', axis=None, scale=alt.Scale(domain=[max_exec,0])),
        y=alt.Y('row_index:O', axis=None),
        color=alt.Color('color:N', scale=None),
        tooltip=[
            alt.Tooltip('value:Q', title='# Executions', format='.0f')
        ]
    ).properties(width=exec_width, height=total_height)


    # --- COLUMN 2: Recency (Bar + Ticks) ---
    recency_width = 100
    # Fixed scale: +/- 2 Days (48 hours * 60 mins = 2880)
    recency_scale = alt.Scale(domain=[lower_range, upper_range], clamp=True)

    # Base Recency Chart
    recency_base = alt.Chart(recency_df).encode(
        y=alt.Y('row_index:O', axis=None)
    )

    # Layer A: The "Last" Bar (Grows left from 0)
    recency_bar = recency_base.transform_filter(
        alt.datum.type == 'Last'
    ).mark_bar(height=15).encode(
        x=alt.X('value:Q', scale=recency_scale, axis=None),
        # x2=alt.value(recency_width / 2),  # Set zero point to middle of chart visually?
        # Actually, better to map 0 via data.
        # But Altair bar needs x2 for range.
        # Let's use x2=0 explicitly:
        x2=alt.datum.zero if 'zero' in recency_df.columns else alt.value(0),
        # Note: mixing datum and value in x2 is tricky.
        # Easier approach: simple x=value, and Altair defaults start at 0.
        color=alt.Color('color:N', scale=None),
        tooltip=[alt.Tooltip('time_val:T', title='Last Executed', format='%Y-%m-%d %H:%M')]
    )

    # Layer B: The "Value" and "Next" Ticks (Vertical lines)
    recency_ticks = recency_base.transform_filter(
        alt.datum.type != 'Last'
    ).mark_tick(thickness=2, height=18).encode(
        x=alt.X('value:Q', scale=recency_scale, axis=None),
        color=alt.Color('color:N', scale=None),
        tooltip=[alt.Tooltip('time_val:T', title='Last Value', format='%Y-%m-%d %H:%M')]
    )

    # Add a center reference rule at 0
    center_rule = alt.Chart(pd.DataFrame({'x': [0]})).mark_rule(
        strokeDash=[2, 2], color=border_color, opacity=1,
        tooltip=None
    ).encode(x=alt.X('x:Q', scale=recency_scale))

    recency_column = (recency_bar + recency_ticks + center_rule).properties(
        width=recency_width,
        height=total_height
    )




    # Column 3: Stacked bars
    bars = alt.Chart(stack_df).mark_bar(size=15).encode(
        x=alt.X('value:Q', stack='zero', axis=None),
        y=alt.Y('row_index:O', axis=None),
        color=alt.Color('type:N',
                        scale=alt.Scale(domain=['Login',
                                                'Extract',
                                                'Load',
                                                'Flatten',
                                                'Parse',
                                                'Interpolate',
                                                'Forecast',
                                                'Python',
                                                'Admin'],
                                        range=['#B7B7B7', # Login
                                               '#0000F5', # Extract
                                               '#f0690f', # Load
                                               '#6B2346', # Flatten
                                               '#9B5278', # Parse
                                               '#EA33F7', # Interpolate
                                               '#8C1AF6', # Forecast
                                               '#f0690f', # Python
                                               '#B7B7B7', # Load
                                               ]),
                        legend=None),
        order='order:O',
        tooltip=[
            alt.Tooltip('type:N', title='Task Phase'),
            alt.Tooltip('value_sec:Q', title='Duration (s)', format='.1f')
        ]
    ).properties(width=50, height=total_height)


    # Add row separators (same width as sparklines)
    separators = alt.Chart(pd.DataFrame({'y': range(1, task_count)})).mark_rule(
        strokeWidth=1,
        color=border_color
    ).encode(
        y=alt.Y('y:Q', axis=None, scale=alt.Scale(domain=[-0.1, task_count]))
    ).properties(width=sparkline_width, height=total_height)


    # Combine all columns
    chart = alt.hconcat(

        exec_bar,
        text_column,
        recency_column,
        bars,
        spacing=1 # Adjust for horizontal gap between columns
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


