import streamlit as st
from streamlit import session_state as ss
import pandas as pd


def format_duration(seconds):
    if pd.isna(seconds): return ""
    sign = "-" if seconds < 0 else ("+" if seconds > 0 else "")
    secs = abs(int(seconds))
    return f"{sign}{secs // 60}:{secs % 60:02d}"


def render_leaderboard(df_raw):
    # --- 1. SELECTION CONTROLS ---
    col1, col2, col3 = st.columns(3)
    with col1:
        range_choice = st.selectbox("Range", ["All Time", "Last 365", "Current Cycle", "Most Recent"])
    with col2:
        type_choice = st.selectbox("Leaderboard Type", ["Basic", "Fitness", "Advanced", "Environment"])


    # --- 2. DATA PREPARATION ---
    rank_col_map = {
        "All Time": "all_time_rank",
        "Last 365": "last_365_rank",
        "Current Cycle": "current_cycle_rank",
        "Most Recent": "recency_rank"
    }
    active_rank_col = rank_col_map[range_choice]
    df_proc = df_raw.dropna(subset=[active_rank_col]).sort_values(active_rank_col).copy()

    # Calculate Gaps
    df_proc['gap_s'] = df_proc['elapsed_duration_s'] - df_proc['elapsed_duration_s'].iloc[0]
    gap_min = 0 #df_proc['gap_s'].min()
    gap_max = df_proc['gap_s'].max()
    if gap_max <1:
        gap_max=1


    basic_cols = [
                  'start_time_utc',
                  'pace_str',
                  'gap_s',
                  'avg_hr']
    st.write(active_rank_col)
    ld_col_config = {active_rank_col: st.column_config.NumberColumn("#",
                     format='%d',
                     width=10),
                     'start_time_utc': st.column_config.DateColumn('Date',
                                                                   format='yyyy-MMM-DD',
                                                                   width=30),
                     'gap_s': st.column_config.ProgressColumn('Gap (s)',
                                                              format='%d',
                                                              min_value=gap_min,
                                                              max_value=gap_max,
                                                              color='auto-inverse')
                     }


    final_cols = [active_rank_col] + basic_cols
    rankings= st.dataframe(df_proc,
                 column_order= final_cols,
                 column_config= ld_col_config,
                 key='lb_df_selection',
                 selection_mode='multi-row',
                 hide_index=True,
                 on_select='rerun')
    st.write(ss.get('lb_df_selection'))
    if rankings.selection.rows:
        activity_list = []
        for idx in rankings.selection.rows:
            st.write(idx)
            # row_idx = rankings.selection.rows[idx]
            activity_list.append(int(df_proc.iloc[idx]['activity_id']))
        st.write(activity_list)
    #

    #
    # # --- 3. EXPLICIT COLUMN CONFIGURATION ---
    # # No iteration: each entry is explicitly defined for total control over labeling
    # col_config = ld_config_dict(df_proc)
    #
    # # --- 4. DYNAMIC VIEW LOGIC ---
    # type_cols_map = {
    #     "Basic": ['pace_str', 'avg_hr', 'max_hr', 'avg_cadence'],
    #     "Fitness": ['avg_perf', 'vo2_max_value', 'training_load_acute', 'weight_lb'],
    #     "Advanced": ['avg_vert_osc', 'avg_vert_ratio', 'avg_gct'],
    #     "Environment": ['avg_temp', 'heat_acclimation_pct']
    # }
    #
    # selected_metrics = type_cols_map[type_choice]
    #
    #
    # # Build the final column order
    # display_order = [active_rank_col, 'start_time_utc', 'dl_elapsed_duration_s']
    #
    #
    # for m in selected_metrics:
    #     display_order.append(f"{prefix}{m}" if prefix else m)
    #
    # # --- 5. RENDER INTERACTIVE TABLE ---
    #
    #
    # event = st.dataframe(
    #     df_proc,
    #     column_order=display_order,
    #     column_config=col_config,
    #     hide_index=True,
    #     use_container_width=True,
    #     on_select="rerun",
    #     selection_mode="single-row"
    # )
    #
    # # --- 6. CLICK ACTION ---
    # if event.selection.rows:
    #     row_idx = event.selection.rows[0]
    #     selected_id = df_proc.iloc[row_idx]['activity_id']
    #     st.success(f"Row Selected: **{selected_id}**")
    return
