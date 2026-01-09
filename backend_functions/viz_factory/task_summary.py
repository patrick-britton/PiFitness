from backend_functions.database_functions import get_conn
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import io
import base64


def get_theme(is_dark_mode=False, is_mobile=False):
    # Colors for high contrast and executive feel
    colors = {
        "text_main": "#e5e7eb" if is_dark_mode else "#0f172a",
        "text_sub": "#9ca3af" if is_dark_mode else "#64748b",
        "stack": ["#3b82f6", "#10b981", "#ec4899"],
        "spark": "#475569",
        "border": "#334155" if is_dark_mode else "#e2e8f0"
    }
    sizes = {
        "font_main": 6,
        "font_sub": 6,
        "px_width": 450 if is_mobile else 750,
        "row_height_inches": 0.2  # Very tight height
    }
    return {**colors, **sizes}


def render_row_to_base64(row, max_median_sum, max_history_val, theme):
    # fig_w remains fixed to prevent scaling
    fig_w = theme["px_width"] / 100
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(fig_w, theme["row_height_inches"]),
                                        gridspec_kw={'width_ratios': [2, 1, 6]})
    fig.patch.set_alpha(0)

    # --- Column 1: Text ---
    ax1.axis('off')

    # ADJUSTED: Moved from 0.60 to 0.70 to clear the bottom line
    ax1.text(0, 0.80, row['task_name'],
             fontsize=theme["font_main"],
             weight='700',
             color=theme["text_main"],
             va='center',
             ha='left',
             transform=ax1.transAxes)

    # ADJUSTED: Moved from 0.20 to 0.15 to add whitespace buffer
    ax1.text(0, 0.01, row['age_label'],
             fontsize=theme["font_sub"],
             style='italic',
             color=theme["text_sub"],
             va='center',
             ha='left',
             transform=ax1.transAxes)

    # --- Column 2: Stacked Bar ---
    ax2.axis('off')
    # Use a small vertical offset to center the bar relative to the two lines of text
    e, l, t = row['median_extract_s'], row['median_load_s'], row['median_transform_s']
    ax2.barh(0.4, e, color=theme["stack"][0], height=0.4)
    ax2.barh(0.4, l, left=e, color=theme["stack"][1], height=0.4)
    ax2.barh(0.4, t, left=e + l, color=theme["stack"][2], height=0.4)
    ax2.set_xlim(0, max_median_sum * 1.05)
    ax2.set_ylim(0, 1)  # Lock y-axis to keep bar centered

    # --- Column 3: Sparkline ---
    ax3.axis('off')
    history = row['etl_time_s']
    if len(history) > 0:
        x = np.arange(len(history))
        ax3.bar(x, history, color=theme["spark"], width=0.7)
        ax3.bar(x[-1], history[-1], color=theme["stack"][0], width=0.7)
    ax3.set_ylim(0, max_history_val)

    buf = io.BytesIO()
    # Ensure no clipping of the adjusted text
    fig.savefig(buf, format="png", bbox_inches='tight', pad_inches=0.00, dpi=360)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def render_task_summary_dashboard(is_dark_mode=True, is_mobile=False):
    st.write("__Task Summary__")

    # Database retrieval (replace with your actual connection)
    df = pd.read_sql("SELECT * FROM tasks.vw_task_summary_chart", con=get_conn(alchemy=True))
    if df.empty: return

    # Data Cleaning
    def clean_pg_array(val):
        if isinstance(val, list): return val
        clean = re.sub(r'[{}]', '', str(val))
        return [float(x) for x in clean.split(',') if x.strip()]

    df['etl_time_s'] = df['etl_time_s'].apply(clean_pg_array)
    df['total_median'] = df['median_extract_s'] + df['median_load_s'] + df['median_transform_s']

    global_max_median = df['total_median'].max() or 1
    global_max_history = df['max_elt'].max() or 1
    theme = get_theme(is_dark_mode, is_mobile)

    # Build the HTML block
    # Using a 1px border-bottom for the separators
    rows_html = ""
    for _, row in df.iterrows():
        img_b64 = render_row_to_base64(row, global_max_median, global_max_history, theme)
        rows_html += f"""
        <div style="border-bottom: 1px solid {theme['border']}; padding: 6px 0; width: {theme['px_width']}px;">
            <img src="data:image/png;base64,{img_b64}" style="width: {theme['px_width']}px; display: block;">
        </div>
        """

    # Wrap the entire thing in a fixed-width container
    container_html = f"""
    <div style="width: {theme['px_width']}px; font-family: sans-serif; margin: 0; padding: 0;">
        {rows_html}
    </div>
    """

    # Use components.html to lock the width and height
    # We calculate height roughly as (row_count * 45px)
    calculated_height = len(df) * 45
    components.html(container_html, height=calculated_height, width=theme['px_width'] + 20)
    return