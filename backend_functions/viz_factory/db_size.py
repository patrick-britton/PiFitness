import streamlit as st
import pandas as pd
import altair as alt
from backend_functions.database_functions import get_conn


def render_db_size_dashboard(is_dark_mode=True, is_mobile=False):
    st.write("__Database Size__")

    # 1. DATA RETRIEVAL
    try:
        df_hist = pd.read_sql("SELECT * FROM logging.vw_db_size_chart ORDER BY date_utc ASC",
                              con=get_conn(alchemy=True))
        df_break = pd.read_sql("SELECT * FROM logging.vw_db_size ORDER BY total_size_mb DESC",
                               con=get_conn(alchemy=True))
    except Exception as e:
        st.error(f"Database Error: {str(e)}")
        return

    if df_hist.empty or df_break.empty:
        st.info("No data available.")
        return

    # 2. PREP DATA
    numeric_cols = ['table_size_mb', 'index_size_mb', 'other_size_mb', 'total_size_mb']
    for col in numeric_cols:
        if col in df_hist.columns: df_hist[col] = df_hist[col].astype(float)
        if col in df_break.columns: df_break[col] = df_break[col].astype(float)

    # Melt for stacked bar logic
    df_hist_melted = df_hist.melt(id_vars=['date_utc'],
                                  value_vars=['table_size_mb', 'index_size_mb', 'other_size_mb'],
                                  var_name='Type', value_name='MB')

    df_break_melted = df_break.melt(id_vars=['table_name', 'total_size_mb'],
                                    value_vars=['table_size_mb', 'index_size_mb', 'other_size_mb'],
                                    var_name='Type', value_name='MB')

    # Theme Variables
    text_color = "#e5e7eb" if is_dark_mode else "#0f172a"
    sub_text_color = "#9ca3af" if is_dark_mode else "#64748b"
    palette = ['#3b82f6', '#f0690f', '#B7B7B7']
    domain = ['table_size_mb', 'index_size_mb', 'other_size_mb']

    chart_width = 400 if is_mobile else 650
    bar_size = chart_width / (len(df_hist_melted)/2.5)
    bar_size = 1 if bar_size<1 else bar_size

    # --- PART 1: TOP CHART (GROWTH) ---
    growth_chart = alt.Chart(df_hist_melted).mark_bar(size=bar_size, width={'band': 0.2}).encode(
        x=alt.X('date_utc:T', axis=alt.Axis(orient='bottom', format='%m-%d', title=None, grid=False, labelColor=sub_text_color)),
        y=alt.Y('sum(MB):Q', axis=alt.Axis(title=None, grid=False, labelColor=sub_text_color, offset=0)),
        color=alt.Color('Type:N', scale=alt.Scale(domain=domain, range=palette), legend=None),
        order=alt.Order('Type:N', sort='descending')
    ).properties(width=chart_width, height=120)

    # --- PART 2: BOTTOM CHART (BREAKDOWN) ---
    # Calculate dynamic width for labels (approx 7px per char + buffer)
    max_name_len = df_break['table_name'].str.len().max()
    max_size_len = df_break['total_size_mb'].astype(str).str.len().max()
    text_col_width = max(max_name_len, max_size_len)

    base_break = alt.Chart(df_break).encode(y=alt.Y('table_name:N', sort=None, axis=None))

    # Bold table names
    table_names = base_break.mark_text(
        align='right', dx=-15, fontSize=10, fontWeight=700, color=text_color
    ).encode(text='table_name:N')

    # Italicized size labels (FIXED: Using transform_calculate for suffix)
    size_labels = base_break.transform_calculate(
        label_text="format(datum.total_size_mb, ',.1f') + ' MB'"
    ).mark_text(
        align='right', dx=-15, dy=9, fontSize=9, fontStyle='italic', color=sub_text_color
    ).encode(text='label_text:N')

    # Stacked Bars
    break_bars = alt.Chart(df_break_melted).mark_bar(height=10).encode(
        y=alt.Y('table_name:N', sort=None, axis=None),
        x=alt.X('sum(MB):Q', axis=None),
        color=alt.Color('Type:N', scale=alt.Scale(domain=domain, range=palette), legend=None),
        order=alt.Order('Type:N', sort='descending')
    ).properties(width=chart_width-text_col_width-150)

    # Combine Breakdown Row
    breakdown_layout = alt.hconcat(
        (table_names + size_labels).properties(width=text_col_width),
        break_bars,
        spacing=-50
    )


    # Final Vertical Assembly with global configuration
    final_chart = alt.vconcat(
        growth_chart,
        breakdown_layout,
        spacing=15,
        center=True
    ).configure(
        padding={"left": 0, "top": 0, "right": 0, "bottom": 0}
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        domain=False,
        ticks=False
    ).resolve_scale(y='independent')

    st.altair_chart(growth_chart, use_container_width=False)
    st.altair_chart(breakdown_layout, use_container_width=False)
    return