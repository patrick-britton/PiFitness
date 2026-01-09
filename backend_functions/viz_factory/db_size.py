from backend_functions.database_functions import get_conn
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import io
import base64




def get_db_viz_theme(is_dark_mode=False, is_mobile=False):
    colors = {
        "text_main": "#e5e7eb" if is_dark_mode else "#0f172a",
        "text_sub": "#9ca3af" if is_dark_mode else "#64748b",
        # Consistent Color Palette for both charts
        "palette": {
            "table": "#3b82f6",  # Blue
            "index": "#f0690f",  # Emerald
            "other": "#B7B7B7"  # Pink
        },
        "border": "#334155" if is_dark_mode else "#e2e8f0"
    }
    sizes = {
        "font_main": 8.0 if is_mobile else 9.0,
        "font_sub": 7.0 if is_mobile else 7.5,
        "px_width": 450 if is_mobile else 750,
        "bar_height": 0.01,
        "dpi": 360
    }
    return {**colors, **sizes}


def get_db_size_viz_html(is_dark_mode, is_mobile):
    """
    Renders the database size visualization into a single HTML/Base64 string.
    Cached to prevent performance lag and 'blank' image re-runs.
    """
    theme = get_db_viz_theme(is_dark_mode, is_mobile)

    # 1. DATA RETRIEVAL
    # Ensure get_conn() is defined in your global scope
    try:
        df_hist = pd.read_sql("SELECT * FROM logging.vw_db_size_chart ORDER BY date_utc ASC",
                              con=get_conn(alchemy=True))
        df_break = pd.read_sql("SELECT * FROM logging.vw_db_size ORDER BY total_size_mb DESC",
                               con=get_conn(alchemy=True))
    except Exception as e:
        return f"<p style='color:red;'>Database Error: {str(e)}</p>"

    if df_hist.empty or df_break.empty:
        return "<p style='color:gray;'>No data available for database metrics.</p>"

    # 2. DIMENSION CALCULATIONS
    top_height_in = 2.2
    row_height_in = 0.32
    total_height_in = top_height_in + (len(df_break) * row_height_in) + 0.6
    fig_w_in = theme["px_width"] / 100

    # 3. OBJECT-ORIENTED FIGURE INITIALIZATION
    # This replaces plt.figure() to prevent global state conflicts
    fig = Figure(figsize=(fig_w_in, total_height_in))
    canvas = FigureCanvasAgg(fig)
    fig.patch.set_alpha(0)

    # --- PART 1: TOP CHART (GROWTH OVER TIME) ---
    top_pos_y_start = (total_height_in - top_height_in + 0.3) / total_height_in
    top_chart_height_frac = (top_height_in - 0.8) / total_height_in

    ax_top = fig.add_axes([0.1, top_pos_y_start, 0.85, top_chart_height_frac])
    ax_top.set_facecolor('none')

    dates = pd.to_datetime(df_hist['date_utc'])
    t_mb, i_mb, o_mb = df_hist['table_size_mb'], df_hist['index_size_mb'], df_hist['other_size_mb']

    # Draw Stacked Bars
    ax_top.bar(dates, t_mb, color=theme["palette"]["table"], width=0.8)
    ax_top.bar(dates, i_mb, bottom=t_mb, color=theme["palette"]["index"], width=0.8)
    ax_top.bar(dates, o_mb, bottom=t_mb + i_mb, color=theme["palette"]["other"], width=0.8)

    # Styling Top Axes (Clean & No Grid)
    ax_top.tick_params(colors=theme["text_sub"], labelsize=theme["font_sub"], length=0)
    for spine in ax_top.spines.values():
        spine.set_visible(False)
    ax_top.yaxis.grid(False)

    # Format X-Axis to prevent overlap
    ax_top.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=7))
    ax_top.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

    # --- PART 2: BOTTOM CHART (TABLE BREAKDOWN) ---
    start_y_in = total_height_in - top_height_in - 0.2
    max_total = df_break['total_size_mb'].max() or 1

    for i, row in df_break.iterrows():
        current_row_y_in = start_y_in - (i * row_height_in)
        y_pos_frac = current_row_y_in / total_height_in
        h_frac = row_height_in / total_height_in

        # Column 1: Text (Right-Justified)
        ax_text = fig.add_axes([0.0, y_pos_frac, 0.35, h_frac])
        ax_text.axis('off')
        ax_text.text(0.95, 0.65, row['table_name'], fontsize=theme["font_main"],
                     weight='700', color=theme["text_main"], ha='right', va='center', transform=ax_text.transAxes)
        ax_text.text(0.95, 0.20, f"{row['total_size_mb']:,} MB", fontsize=theme["font_sub"],
                     style='italic', color=theme["text_sub"], ha='right', va='center', transform=ax_text.transAxes)

        # Column 2: Slim Bars
        ax_bar = fig.add_axes([0.34, y_pos_frac, 0.25, h_frac])
        ax_bar.axis('off')

        # Lock vertical coordinate system
        ax_bar.set_ylim(0, 1)

        # Draw bar slim (height=0.2) and centered (y=0.5)
        ts, idx, oth = row['table_size_mb'], row['index_size_mb'], row['other_size_mb']
        ax_bar.barh(0.5, ts, color=theme["palette"]["table"], height=0.35, align='center')
        ax_bar.barh(0.5, idx, left=ts, color=theme["palette"]["index"], height=0.35, align='center')
        ax_bar.barh(0.5, oth, left=ts + idx, color=theme["palette"]["other"], height=0.35, align='center')
        ax_bar.set_xlim(0, max_total * 1.05)

        # Separator Line (using Line2D for Object-Oriented consistency)
        import matplotlib.lines as mlines
        line = mlines.Line2D([0, 1], [0, 0], transform=ax_text.transAxes, color=theme['border'], lw=0.5)
        ax_text.add_line(line)

    # 4. SAVE TO BASE64
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches='tight', pad_inches=0.05, dpi=theme["dpi"])
    encoded = base64.b64encode(buf.getvalue()).decode()

    # Wrap in fixed-width HTML
    return f"""
    <div style="width: {theme['px_width']}px; margin: 0 auto;">
        <img src="data:image/png;base64,{encoded}" style="width: {theme['px_width']}px; display: block;">
    </div>
    """
    return


def render_db_size_dashboard(is_dark_mode=True, is_mobile=False):
    st.write("__Database Size__")
    theme = get_db_viz_theme(is_dark_mode, is_mobile)

    # Get the HTML from cache (passing parameters as keys)
    html_content = get_db_size_viz_html(is_dark_mode, is_mobile)

    # Display using st.markdown or st.write
    st.write(html_content, unsafe_allow_html=True)
    return