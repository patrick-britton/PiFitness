import streamlit as st
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
        range_choice = st.selectbox("Range", ["All Time", "Last 365", "Current Cycle"])
    with col2:
        type_choice = st.selectbox("Leaderboard Type", ["Basic", "Fitness", "Advanced", "Environment"])
    with col3:
        view_mode = st.radio("View Mode", ["Normal", "Delta by Leader", "Delta by Most Recent"])

    # --- 2. DATA PREPARATION ---
    rank_col_map = {
        "All Time": "all_time_rank",
        "Last 365": "last_365_rank",
        "Current Cycle": "current_cycle_rank"
    }
    active_rank_col = rank_col_map[range_choice]
    df_proc = df_raw.dropna(subset=[active_rank_col]).sort_values(active_rank_col).copy()

    # Metrics for Delta Calculation
    metrics = [
        'elasped_duration_s', 'avg_hr', 'max_hr', 'avg_cadence', 'avg_perf',
        'vo2_max_value', 'training_load_acute', 'weight_lb', 'avg_vert_osc',
        'avg_vert_ratio', 'avg_gct', 'avg_temp', 'heat_acclimation_pct'
    ]

    # Calculate Deltas (Leader)
    leader_row = df_proc.iloc[0]
    for col in metrics:
        df_proc[f"dl_{col}"] = df_proc[col] - leader_row[col]

    # Calculate Deltas (Most Recent)
    mr_rows = df_raw[df_raw['recency_rank'] == 1]
    if not mr_rows.empty:
        mr_row = mr_rows.iloc[0]
        for col in metrics:
            df_proc[f"dm_{col}"] = df_proc[col] - mr_row[col]

    # Format Duration Deltas
    df_proc['dl_elasped_duration_s'] = df_proc['dl_elasped_duration_s'].apply(format_duration)
    df_proc['dm_elasped_duration_s'] = df_proc['dm_elasped_duration_s'].apply(format_duration)

    # --- 3. EXPLICIT COLUMN CONFIGURATION ---
    # No iteration: each entry is explicitly defined for total control over labeling
    col_config = {
        active_rank_col: st.column_config.NumberColumn("Rank", format="%d", width=20),
        "start_time_utc": st.column_config.DatetimeColumn("Date", format="yyyy-MMM-DD", width=55),
        "pace_str": st.column_config.TextColumn("Pace", width=30),

        # Duration & Splits
        "elasped_duration_s": st.column_config.NumberColumn("Duration", format="%d s"),
        "dl_elasped_duration_s": st.column_config.TextColumn("S Behind", width=30),
        "dm_elasped_duration_s": st.column_config.TextColumn("Split (Δ Rec)"),

        # BASIC METRICS
        "avg_hr": st.column_config.NumberColumn("Avg HR", format="%d bpm"),
        "dl_avg_hr": st.column_config.NumberColumn("Avg HR (Δ Ldr)", format="%+d bpm"),
        "dm_avg_hr": st.column_config.NumberColumn("Avg HR (Δ Rec)", format="%+d bpm"),

        "max_hr": st.column_config.NumberColumn("Max HR", format="%d bpm"),
        "dl_max_hr": st.column_config.NumberColumn("Max HR (Δ Ldr)", format="%+d bpm"),
        "dm_max_hr": st.column_config.NumberColumn("Max HR (Δ Rec)", format="%+d bpm"),

        "avg_cadence": st.column_config.NumberColumn("Cadence", format="%d spm"),
        "dl_avg_cadence": st.column_config.NumberColumn("Cadence (Δ Ldr)", format="%+d spm"),
        "dm_avg_cadence": st.column_config.NumberColumn("Cadence (Δ Rec)", format="%+d spm"),

        # FITNESS METRICS
        "avg_perf": st.column_config.NumberColumn("Performance", format="%d"),
        "dl_avg_perf": st.column_config.NumberColumn("Perf (Δ Ldr)", format="%+d"),
        "dm_avg_perf": st.column_config.NumberColumn("Perf (Δ Rec)", format="%+d"),

        "vo2_max_value": st.column_config.NumberColumn("VO2 Max", format="%.1f"),
        "dl_vo2_max_value": st.column_config.NumberColumn("VO2 (Δ Ldr)", format="%+.1f"),
        "dm_vo2_max_value": st.column_config.NumberColumn("VO2 (Δ Rec)", format="%+.1f"),

        "training_load_acute": st.column_config.NumberColumn("Acute Load", format="%d"),
        "dl_training_load_acute": st.column_config.NumberColumn("Load (Δ Ldr)", format="%+d"),
        "dm_training_load_acute": st.column_config.NumberColumn("Load (Δ Rec)", format="%+d"),

        "weight_lb": st.column_config.NumberColumn("Weight", format="%.1f lb"),
        "dl_weight_lb": st.column_config.NumberColumn("Weight (Δ Ldr)", format="%+.1f lb"),
        "dm_weight_lb": st.column_config.NumberColumn("Weight (Δ Rec)", format="%+.1f lb"),

        # ADVANCED METRICS
        "avg_vert_osc": st.column_config.NumberColumn("Vert Osc", format="%.1f cm"),
        "dl_avg_vert_osc": st.column_config.NumberColumn("Osc (Δ Ldr)", format="%+.1f cm"),
        "dm_avg_vert_osc": st.column_config.NumberColumn("Osc (Δ Rec)", format="%+.1f cm"),

        "avg_vert_ratio": st.column_config.NumberColumn("Vert Ratio", format="%.1f %%"),
        "dl_avg_vert_ratio": st.column_config.NumberColumn("Ratio (Δ Ldr)", format="%+.1f %%"),
        "dm_avg_vert_ratio": st.column_config.NumberColumn("Ratio (Δ Rec)", format="%+.1f %%"),

        "avg_gct": st.column_config.NumberColumn("GCT", format="%d ms"),
        "dl_avg_gct": st.column_config.NumberColumn("GCT (Δ Ldr)", format="%+d ms"),
        "dm_avg_gct": st.column_config.NumberColumn("GCT (Δ Rec)", format="%+d ms"),

        # ENVIRONMENT METRICS
        "avg_temp": st.column_config.NumberColumn("Temp", format="%.1f °C"),
        "dl_avg_temp": st.column_config.NumberColumn("Temp (Δ Ldr)", format="%+.1f °C"),
        "dm_avg_temp": st.column_config.NumberColumn("Temp (Δ Rec)", format="%+.1f °C"),

        "heat_acclimation_pct": st.column_config.NumberColumn("Heat Acclim", format="%d %%"),
        "dl_heat_acclimation_pct": st.column_config.NumberColumn("Acclim (Δ Ldr)", format="%+d %%"),
        "dm_heat_acclimation_pct": st.column_config.NumberColumn("Acclim (Δ Rec)", format="%+d %%"),
    }

    # --- 4. DYNAMIC VIEW LOGIC ---
    type_cols_map = {
        "Basic": ['pace_str', 'avg_hr', 'max_hr', 'avg_cadence'],
        "Fitness": ['avg_perf', 'vo2_max_value', 'training_load_acute', 'weight_lb'],
        "Advanced": ['avg_vert_osc', 'avg_vert_ratio', 'avg_gct'],
        "Environment": ['avg_temp', 'heat_acclimation_pct']
    }

    selected_metrics = type_cols_map[type_choice]

    # Determine column prefix for swapping
    prefix = ""
    if view_mode == "Delta by Leader":
        prefix = "dl_"
    elif view_mode == "Delta by Most Recent":
        prefix = "dm_"

    # Build the final column order
    display_order = [active_rank_col, 'start_time_utc', 'dl_elasped_duration_s']


    for m in selected_metrics:
        display_order.append(f"{prefix}{m}" if prefix else m)

    # --- 5. RENDER INTERACTIVE TABLE ---


    event = st.dataframe(
        df_proc,
        column_order=display_order,
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row"
    )

    # --- 6. CLICK ACTION ---
    if event.selection.rows:
        row_idx = event.selection.rows[0]
        selected_id = df_proc.iloc[row_idx]['activity_id']
        st.success(f"Row Selected: **{selected_id}**")
    return