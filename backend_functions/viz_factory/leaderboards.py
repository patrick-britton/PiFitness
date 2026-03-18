import json
import math

import numpy as np
import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from streamlit import session_state as ss
import pandas as pd

from backend_functions.database_functions import con_cur, get_conn
from backend_functions.logging_functions import elapsed_ms
from backend_functions.service_logins import mapbox_token
from frontend_functions.streamlit_helpers import ss_pop, sse, start_timer


def format_duration(seconds):
    if pd.isna(seconds): return ""
    sign = "-" if seconds < 0 else ("+" if seconds > 0 else "")
    secs = abs(int(seconds))
    return f"{sign}{secs // 60}:{secs % 60:02d}"


def render_leaderboard(df_raw):
    init_map_basics()
    # --- 1. SELECTION CONTROLS ---
    col1, col2, col3 = st.columns(3)
    with col1:
        range_choice = st.selectbox("Range", ["All Time", "Last 365", "Current Cycle", "Most Recent"])
    with col2:
        type_choice = st.selectbox("Leaderboard Type", ["Basic", "Fitness", "Advanced", "Environment", "Preparedness"])

    # st.write(df_raw.columns)
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
    df_proc['gap_s'] = round(df_proc['elapsed_duration_s'] - df_proc['elapsed_duration_s'].iloc[0],1)
    gap_min = 0 #df_proc['gap_s'].min()
    gap_max = df_proc['gap_s'].max()
    if gap_max <1:
        gap_max=1

    basic_cols = col_selector(type_choice)




    # st.write(active_rank_col)
    ld_col_config = {active_rank_col: st.column_config.NumberColumn("#",
                     format='%d',
                     width=10),
                     'start_time_utc': st.column_config.DateColumn('Date',
                                                                   format='yyyy-MMM-DD',
                                                                   width=30),
                     'gap_s': st.column_config.ProgressColumn('Gap (s)',
                                                              format='%f',
                                                              min_value=safe_minmax(df_proc,
                                                                                    'gap_s',
                                                                                    0,
                                                                                    False),
                                                              max_value=safe_minmax(df_proc,
                                                                                    'gap_s',
                                                                                    0,
                                                                                    True),
                                                              color='auto-inverse',
                                                              width=30),
                     'avg_hr': st.column_config.ProgressColumn('Avg HR',
                                                              min_value=60,
                                                              max_value = safe_minmax(df_proc,
                                                                                    'avg_hr',
                                                                                    60,
                                                                                    True),
                                                              format='%d',
                                                               width=30),
                     'vo2_max_value': st.column_config.ProgressColumn('VO2',
                                                               min_value=safe_minmax(df_proc,
                                                                                     'vo2_max_value',
                                                                                     None,
                                                                                     False),
                                                               max_value=safe_minmax(df_proc,
                                                                                     'vo2_max_value',
                                                                                     None,
                                                                                     True),
                                                               format='%f',
                                                                      color='auto',
                                                               width=30),
                     'resting_hr_asleep': st.column_config.ProgressColumn('RHR',
                                                                      min_value=safe_minmax(df_proc,
                                                                                            'resting_hr_asleep',
                                                                                            30,
                                                                                            False),
                                                                      max_value=safe_minmax(df_proc,
                                                                                            'resting_hr_asleep',
                                                                                            30,
                                                                                            True),
                                                                      format='%d',
                                                                          color='auto-inverse',
                                                                      width=30),
                     'training_load_acute': st.column_config.ProgressColumn('Acute Load',
                                                                          min_value=safe_minmax(df_proc,
                                                                                                'training_load_acute',
                                                                                                None,
                                                                                                False),
                                                                          max_value=safe_minmax(df_proc,
                                                                                                'training_load_acute',
                                                                                                None,
                                                                                                True),
                                                                          format='%d',
                                                                          color='auto',
                                                                          width=30),
                     'weight_lb': st.column_config.ProgressColumn('Weight',
                                                                            min_value=safe_minmax(df_proc,
                                                                                                  'weight_lb',
                                                                                                  None,
                                                                                                  False),
                                                                            max_value=safe_minmax(df_proc,
                                                                                                  'weight_lb',
                                                                                                  None,
                                                                                                  True),
                                                                            format='%f',
                                                                            color='auto-inverse',
                                                                            width=30),
                     'fat_pct': st.column_config.ProgressColumn('Fat%',
                                                                  min_value=safe_minmax(df_proc,
                                                                                        'fat_pct',
                                                                                        None,
                                                                                        False),
                                                                  max_value=safe_minmax(df_proc,
                                                                                        'fat_pct',
                                                                                        None,
                                                                                        True),
                                                                  format='percent',
                                                                  color='auto-inverse',
                                                                  width=30),
                     'muscle_pct': st.column_config.ProgressColumn('Fat%',
                                                                min_value=safe_minmax(df_proc,
                                                                                      'muscle_pct',
                                                                                      None,
                                                                                      False),
                                                                max_value=safe_minmax(df_proc,
                                                                                      'muscle_pct',
                                                                                      None,
                                                                                      True),
                                                                format='percent',
                                                                color='auto',
                                                                width=30),
                     'avg_cadence': st.column_config.ProgressColumn('Cadence',
                                                                   min_value=safe_minmax(df_proc,
                                                                                         'avg_cadence',
                                                                                         None,
                                                                                         False),
                                                                   max_value=safe_minmax(df_proc,
                                                                                         'avg_cadence',
                                                                                         None,
                                                                                         True),
                                                                   format='%d',
                                                                   color='auto',
                                                                   width=30),
                     'avg_vert_osc': st.column_config.ProgressColumn('Vert. Osc.',
                                                                    min_value=safe_minmax(df_proc,
                                                                                          'avg_vert_osc',
                                                                                          None,
                                                                                          False),
                                                                    max_value=safe_minmax(df_proc,
                                                                                          'avg_vert_osc',
                                                                                          None,
                                                                                          True),
                                                                    format='%f',
                                                                    color='auto-inverse',
                                                                    width=30),
                     'avg_gct': st.column_config.ProgressColumn('GCT',
                                                                     min_value=safe_minmax(df_proc,
                                                                                           'avg_gct',
                                                                                           None,
                                                                                           False),
                                                                     max_value=safe_minmax(df_proc,
                                                                                           'avg_gct',
                                                                                           None,
                                                                                           True),
                                                                     format='%d',
                                                                     color='auto-inverse',
                                                                     width=30),
                     'avg_stride_length': st.column_config.ProgressColumn('Stride',
                                                                min_value=safe_minmax(df_proc,
                                                                                      'avg_stride_length',
                                                                                      None,
                                                                                      False),
                                                                max_value=safe_minmax(df_proc,
                                                                                      'avg_stride_length',
                                                                                      None,
                                                                                      True),
                                                                format='%f',
                                                                color='auto',
                                                                width=30),
                     'avg_temp': st.column_config.ProgressColumn('Temp',
                                                                          min_value=safe_minmax(df_proc,
                                                                                                'avg_temp',
                                                                                                None,
                                                                                                False),
                                                                          max_value=safe_minmax(df_proc,
                                                                                                'avg_temp',
                                                                                                None,
                                                                                                True),
                                                                          format='%f',
                                                                          color='auto-inverse',
                                                                          width=30),
                     'altitude_acclimation_m': st.column_config.ProgressColumn('Acclimation',
                                                                 min_value=safe_minmax(df_proc,
                                                                                       'altitude_acclimation_m',
                                                                                       None,
                                                                                       False),
                                                                 max_value=safe_minmax(df_proc,
                                                                                       'altitude_acclimation_m',
                                                                                       None,
                                                                                       True),
                                                                 format='%f',
                                                                 color='auto',
                                                                 width=30),
                     'training_load_pct': st.column_config.ProgressColumn('Load %',
                                                                    min_value=safe_minmax(df_proc,
                                                                                          'training_load_pct',
                                                                                          None,
                                                                                          False),
                                                                    max_value=safe_minmax(df_proc,
                                                                                          'training_load_pct',
                                                                                          None,
                                                                                          True),
                                                                    format='percent',
                                                                    color='auto',
                                                                    width=30),
                     'sleep_hours': st.column_config.ProgressColumn('Sleep',
                                                                               min_value=safe_minmax(df_proc,
                                                                                                     'sleep_hours',
                                                                                                     None,
                                                                                                     False),
                                                                               max_value=safe_minmax(df_proc,
                                                                                                     'sleep_hours',
                                                                                                     None,
                                                                                                     True),
                                                                               format='%f',
                                                                               color='auto',
                                                                               width=30),
                     'sleep_score': st.column_config.ProgressColumn('Sleep Score',
                                                                    min_value=safe_minmax(df_proc,
                                                                                          'sleep_score',
                                                                                          None,
                                                                                          False),
                                                                    max_value=safe_minmax(df_proc,
                                                                                          'sleep_score',
                                                                                          None,
                                                                                          True),
                                                                    format='%d',
                                                                    color='auto',
                                                                    width=30),
                     'awake_hours': st.column_config.ProgressColumn('Awake Time',
                                                                    min_value=safe_minmax(df_proc,
                                                                                          'awake_hours',
                                                                                          None,
                                                                                          False),
                                                                    max_value=safe_minmax(df_proc,
                                                                                          'awake_hours',
                                                                                          None,
                                                                                          True),
                                                                    format='%f',
                                                                    color='auto',
                                                                    width=30),
                     }


    final_cols = [active_rank_col] + basic_cols
    rankings= st.dataframe(df_proc,
                 column_order= final_cols,
                 column_config= ld_col_config,
                 key='lb_df_selection',
                 selection_mode='multi-row',
                 hide_index=True,
                 on_select='rerun')
    activity_list = []
    if rankings.selection.rows:
        for idx in rankings.selection.rows:
            # row_idx = rankings.selection.rows[idx]
            activity_dict = {}
            activity_dict['id'] = int(df_proc.iloc[idx]['activity_id'])
            activity_dict['start'] = int(df_proc.iloc[idx]['activity_start_point'])
            activity_dict['end'] = int(df_proc.iloc[idx]['activity_end_point'])
            activity_list.append(activity_dict)
        # st.write(activity_list)

    if activity_list:
        if st.button('Generate Visuals'):
            load_segment_selection_data(activity_list)
            ss_pop('df_race')
            if not sse('df_race'):
                with st.spinner("Crunching telemetry..."):
                    ss.df_race = get_and_prep_telemetry()


    if not sse('df_race'):
        return

    # Display the static "Final Results" Chart
    # chart_col, config_col = st.columns(spec=[3,1], gap="small", border=False)
    map_style_widget()




    fig = create_animated_map_viz(ss.df_race)
    st.plotly_chart(figure_or_data=fig, width='content'
                    # , config={'width':800, 'height':450}
                    )
    fig2 = create_telemetry_charts_viz(ss.df_race)
    st.plotly_chart(fig2, width='content'
                    # , config={'width':800, 'height':570*3}
                    )

    if st.button('Refresh'):
        ss_pop('sm_leaderboard_df')
        st.rerun()
    return


def load_segment_selection_data(activity_list):
    con, cur = con_cur()

    trunc_sql = "TRUNCATE TABLE activities.temp_activity_segment_metas;"

    cur.execute(trunc_sql)
    con.commit()

    for a in activity_list:
        ins_sql = f"""INSERT INTO activities.temp_activity_segment_metas (
                 activity_id, start_point, end_point)
                VALUES ({int(a.get('id'))}, {int(a.get('start'))}, {int(a.get('end'))});"""
        cur.execute(ins_sql)
    con.commit()
    con.close()
    return

def get_and_prep_telemetry():
    # 1. Fetch from your aggregated view
    query = f"""SELECT * FROM activities.vw_activity_segments_aggregated"""
    df = pd.read_sql(query, con=get_conn(alchemy=True))
    cols_to_fix = ['path_coords', 'map_timestamps', 'x_time', 'x_distance',
                   'y_hr', 'y_cadence', 'y_speed', 'y_elevation', 'y_temp',
                   'y_balance', 'y_performance', 'y_osc', 'y_stride', 'y_gct', 'y_time_delta',
                   'y_hr_delta']

    for col in cols_to_fix:
        if isinstance(df[col].iloc[0], str):
            df[col] = df[col].apply(json.loads)

    df_long = df.explode(cols_to_fix)

    df_long[['lon', 'lat']] = pd.DataFrame(df_long['path_coords'].tolist(), index=df_long.index)

    # Cast metrics to numeric (explode can leave them as 'object' type)
    numeric_cols = ['x_distance', 'x_time', 'y_hr',
                    'y_cadence',
                    'y_elevation',
                    'y_time_delta'
                   #  'y_speed',
                   #  'y_temp',
                   # 'y_balance',
                   #  'y_performance',
                   #  'y_osc',
                   #  'y_stride',
                   #  'y_gct'
                    ]
    df_long[numeric_cols] = df_long[numeric_cols].apply(pd.to_numeric)
    return df_long


def create_animated_map_viz(df_long):
    activities = df_long['id_val'].unique()
    ref_act = activities[0]

    filtered_acts = [act for act in activities if act != ref_act]

    colors = ['#000000', '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A']

    # --- 1. View & Bound Calculations (Reinstated) ---
    min_lat, max_lat = df_long['lat'].min(), df_long['lat'].max()
    min_lon, max_lon = df_long['lon'].min(), df_long['lon'].max()
    center_lat = round((max_lat + min_lat) / 2, 5)
    center_lon = round((max_lon + min_lon) / 2, 5)

    lat_span = abs(max_lat - min_lat)
    lon_span = abs(max_lon - min_lon)
    adjusted_lat_span = lat_span * (16 / 9) * 1.1
    adjusted_lon_span = lon_span * 1.1
    max_bound_deg = max(adjusted_lat_span, adjusted_lon_span)
    max_bound = max_bound_deg * 111
    # st.caption(max_bound_deg)
    base_zoom = 14.75 if max_bound_deg > 0.018 else 15.41
    zoom = base_zoom - np.log(max_bound) if not math.isnan(base_zoom - np.log(max_bound)) else 16.78
    zoom_adj = ss.get('zoom_adj')
    if zoom_adj:
        zoom = zoom + zoom_adj


    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[5, 1],
        specs=[[{"type": "mapbox"}, {"type": "xy"}]],
        horizontal_spacing=0.1
    )

    ghost_data = df_long[df_long['id_val'] == activities[0]]
    fig.add_trace(go.Scattermapbox(
        lat=ghost_data['lat'],
        lon=ghost_data['lon'],
        mode='lines',
        line=dict(width=5, color='rgba(0, 0, 0, .1)'),  # Thin, light gray
        showlegend=False,
        hoverinfo='skip',
        name="Course Outline"
    ), row=1, col=1)

    final_values = [df_long[df_long['id_val'] == a].iloc[-1]['y_time_delta'] for a in filtered_acts]
    max_slots = 5

    y_slots = [f"{i}" for i in filtered_acts]
    while len(y_slots) <= max_slots:
        y_slots.append("")

    fig.add_trace(go.Bar(
        y=y_slots,
        x=final_values,
        orientation='h',
        marker_color='rgba(211, 211, 211, 0.25)',
        # text=[f"{int(v)}s" for v in final_values],
        # textposition='outside',
        showlegend=False,
        hoverinfo='skip',
        width=0.5
    ), row=1, col=2)

    # --- 3. LAYER 2 (MIDDLE): ANIMATED GROWING LINES ---
    line_indices = []
    for i, id_val in enumerate(activities):
        color = colors[i % len(colors)]
        fig.add_trace(go.Scattermapbox(
            lat=[], lon=[], mode='lines',
            line=dict(width=4, color=color),
            name=str(id_val), legendgroup=str(id_val),
            showlegend=False,
        ))
        line_indices.append(len(fig.data) - 1)

    # --- 4. LAYER 3 (TOP): ANIMATED LEAD MARKERS ---
    animated_indices = []
    for i, id_val in enumerate(activities):
        color = colors[i % len(colors)]

        # Add Map Marker
        fig.add_trace(go.Scattermapbox(
            mode='markers', marker=dict(size=14, color=color),
            showlegend=False, legendgroup=str(id_val)
        ), row=1, col=1)
        animated_indices.append(len(fig.data) - 1)

        # Add Dynamic Bar (Only for non-reference activities)
        if id_val != ref_act:
            slot_idx = filtered_acts.index(id_val)
            fig.add_trace(go.Bar(
                y=[y_slots[slot_idx]],  # Matches exactly the slot from the gray trace
                x=[0],
                orientation='h',
                marker_color=color,
                text=["0s"],
                textposition='outside',
                showlegend=False,
                legendgroup=str(id_val),
                width=0.5
            ), row=1, col=2)
            animated_indices.append(len(fig.data) - 1)


    # Combine indices for the animation engine
    all_animated_indices = line_indices + animated_indices

    # --- 5. BUILD FRAMES ---
    max_len = max([len(df_long[df_long['id_val'] == act]) for act in activities])
    num_acts = len(y_slots)
    if num_acts <= 2:
        target_frames = 400
        frame_duration = 50
    elif num_acts == 3:
        target_frames = 350
        frame_duration = 65
    elif num_acts == 4:
        target_frames = 250
        frame_duration = 90
    else:  # 5+ activities (Maximum braking)
        target_frames = 150
        frame_duration = 140
    step = max(1, max_len // target_frames)
    frames = []
    slider_steps = []

    for k in range(0, max_len, step):
        frame_data = []
        current_dist = int(df_long[df_long['id_val'] == activities[0]].iloc[
                               min(k, len(df_long[df_long['id_val'] == activities[0]]) - 1)]['x_distance'])
        frame_name = f"{current_dist}m"
        frame_id = str(k)

        # Add Line Data first
        for id_val in activities:
            data = df_long[df_long['id_val'] == id_val]
            idx = min(k, len(data) - 1)
            line_slice = data.iloc[0: idx + 1]
            frame_data.append(go.Scattermapbox(lat=line_slice['lat'], lon=line_slice['lon']))

        # Add Marker Data second
        for id_val in activities:
            d = df_long[df_long['id_val'] == id_val]
            row = d.iloc[min(k, len(d) - 1)]
            frame_data.append(go.Scattermapbox(lat=[row['lat']], lon=[row['lon']]))

            if id_val != ref_act:
                slot_idx = filtered_acts.index(id_val)
                val = row['y_time_delta']
                frame_data.append(go.Bar(y=[y_slots[slot_idx]],
                                         x=[val], text=[f"{int(val)}s"],
                                         width=0.2))

        frames.append(go.Frame(data=frame_data, traces=all_animated_indices, name=frame_id))

        # Add Slider Step
        slider_step = {
            "args": [[frame_id],
                     {"frame": {"duration": frame_duration, "redraw": True}, "mode": "immediate", "transition": {"duration": 0}}],
            "label": frame_name,
            "method": "animate"
        }
        # if k % 250 == 0:
        slider_steps.append(slider_step)

    fig.frames = frames

    # --- 4. Layout & Styles (Reinstated) ---
    token = mapbox_token()  # Custom function
    view = ss.style_dict.get(ss.get('cc_map_style'), 'positron')
    final_values = [df_long[df_long['id_val'] == a].iloc[-1]['y_time_delta'] for a in filtered_acts]
    x_min, x_max = min(final_values), max(final_values)
    x_range = [
        x_min * 1.8 if x_min < 0 else -20,
        x_max * 1.8 if x_max > 0 else 20
    ]
    fig.update_layout(
        height=500,
        width=850,
        autosize=False,
        barmode='overlay',
        mapbox_style=view,  # Assuming 'view' logic from previous steps
        mapbox_accesstoken=token if token else None,
        showlegend=False,
        mapbox=dict(center=dict(lat=center_lat, lon=center_lon), zoom=zoom),
        margin=dict(l=0, r=0, t=0, b=50),  # Space for slider
        xaxis=dict(
            showticklabels=True,
            showline=True,
            ticks="outside",
            range=x_range,
            # showticks=False,  # Explicitly disable the tick marks
            # tickfont=dict(size=0),  # Force font size to zero as a backup
            fixedrange=True,
            showgrid=False,
            zeroline=True,
            zerolinecolor='rgba(255,255,255,0.2)'
        ),
        xaxis2=dict(
            showticklabels=False,
            range=x_range,
            # showticks=False,
            # tickfont=dict(size=0),
            fixedrange=True,
            showgrid=False,
            zeroline=True,
            zerolinecolor='rgba(255,255,255,0.2)'
        ),
        # Do the same for Y-axes just in case
        yaxis=dict(
            showticklabels=True,   # Switch this from False to True
            tickmode='array',
            type='category',
            categoryarray=y_slots,
            range=[-0.5, max_slots - 0.5],
            tickvals=y_slots,      # Use your list: ['Slot_0', 'Slot_1', ...]
            ticktext=y_slots,      # This is what actually displays
            fixedrange=True,
            showgrid=False,
            side='left'            # Ensures they stay on the left of the bars
    ),

        updatemenus=[{
            "buttons": [
                {
                    "args": [None, {
                        "frame": {"duration": frame_duration, "redraw": True},
                        "fromcurrent": True,
                        "mode": "immediate"  # Ensures it starts from where you are
                    }],
                    "label": "▶ Play",
                    "method": "animate"
                },
                {
                    "args": [[None], {  # Note the [None] in a list - this is key
                        "frame": {"duration": 0, "redraw": True},
                        "mode": "immediate",
                        "transition": {"duration": 0}
                    }],
                    "label": "⏸ Pause",
                    "method": "animate"
                }
            ],
            "direction": "left",
            "pad": {"r": 10, "t": 65},
            "showactive": False,
            "type": "buttons",
            "x": 0, "xanchor": "left", "y": 0, "yanchor": "top"
        }],

        sliders=[{
            "active": 0,
            "yanchor": "top",
            "xanchor": "left",
            "currentvalue": {"font": {"size": 14, "family": "monospace"}, "prefix": "Distance: ", "visible": True, "xanchor": "right"},
            "pad": { "t": 50},
            # "transition": {"duration": 300, "easing": "cubic-in-out"},
            "len": 0.85,
            "xanchor": "left",
            "x": 0.2, "y": 0,
            "steps": slider_steps
        }]
    )

    return fig


def create_telemetry_charts_viz(df_long):
    activities = df_long['id_val'].unique()
    colors = ['#000000', '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A']

    base_elev_data = df_long[df_long['id_val'] == activities[0]]

    min_el = base_elev_data['y_elevation'].min()
    max_el = base_elev_data['y_elevation'].max()
    avg_el = base_elev_data['y_elevation'].mean()
    diff = max_el - min_el

    if diff < 100:
        lower_bound = min(min_el - 10, avg_el - 60)
        upper_bound = max(max_el + 10, avg_el + 60)
    else:
        lower_bound = min_el - 10
        upper_bound = max_el + 10


    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]]
    )

    # 1. ADD BACKGROUND ELEVATION (Canonical)
    base_elev_data = df_long[df_long['id_val'] == activities[0]]
    for r in [1, 2, 3]:
        fig.add_trace(go.Scatter(
            x=base_elev_data['x_distance'], y=base_elev_data['y_elevation'],
            mode='lines', fill='tozeroy',
            fillcolor='rgba(211, 211, 211, 0.15)',  # Color/Opacity control
            line=dict(color='rgba(211, 211, 211, 0.45)', width=1),
            showlegend=False, hoverinfo='skip'
        ), row=r, col=1, secondary_y=True)

    # 2. ADD ACTIVITY METRICS
    for i, id_val in enumerate(activities):
        data = df_long[df_long['id_val'] == id_val]
        color = colors[i % len(colors)]

        # Row 1: Time Gap (Line)
        fig.add_trace(go.Scatter(x=data['x_distance'], y=data['y_time_delta'], mode='lines', line=dict(color=color),
                                 name=str(id_val), legendgroup=str(id_val), showlegend=True), row=1, col=1, secondary_y=False,
                      )

        # Row 2: Heart Rate (Line)
        fig.add_trace(
            go.Scatter(x=data['x_distance'], y=data['y_hr'], mode='lines', line=dict(color=color), showlegend=False,
                       legendgroup=str(id_val)), row=2, col=1, secondary_y=False)

        # Row 3: Cadence (Dot Plot)
        fig.add_trace(go.Scatter(
            x=data['x_distance'], y=data['y_cadence'],
            mode='markers',
            marker=dict(size=5, color=color, opacity=0.5),  # Cadence Opacity control
            showlegend=False, legendgroup=str(id_val)
        ), row=3, col=1, secondary_y=False)

    # 3. STYLING
    fig.update_yaxes(showticklabels=False, showgrid=False, secondary_y=True)  # Hide Elevation Labels

    # Sync Zooming (Fix for your original bug)
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(
        # range=[lower_bound, upper_bound],
        showticklabels=True,
        showgrid=True,
        secondary_y=False
    )
    fig.update_yaxes(
        range=[lower_bound, upper_bound],
        showticklabels=False,
        showgrid=False,
        secondary_y=True
    )
    fig.update_layout(showlegend=True,hovermode="x unified", template="plotly_dark",
                      margin=dict(t=80, b=0, l=0, r=0),
                      height=900,
                      width=850,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    fig.update_yaxes(title_text="Gap", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="HR", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Cadence", row=3, col=1, secondary_y=False)
    return fig

def map_style_widget():
    token = mapbox_token()

    satellite_dict = {'MB Basic': 'basic',
                      'MB Streets': 'streets',
                      'MB Outdoors': 'outdoors',
                      'MB Light': 'light',
                      'MB Dark': 'dark',
                      'Satellite': 'satellite',
                      'Sat + Streets': 'satellite-streets'}
    if token:
        ss.style_dict = ss.style_dict | satellite_dict
    style_col, zoom_col = st.columns(spec=[1,1], border=False, gap="small")
    with style_col:
        st.selectbox(label='Map Style',
                 key='cc_map_style',
                 options=list(ss.style_dict.keys()),
                     on_change=update_map_options)

    with zoom_col:
        st.slider(label='Zoom',
                  min_value=-1.0,
                  max_value=1.0,
                  step=0.01,
                  key='zoom_adj',
                  value=0.0)


    return

def update_map_options():
    map_style = ss.style_dict.get(ss.get('cc_map_style'))
    if not map_style:
        ss.map_style = 'carto-positron'
    else:
        ss.map_style = map_style

    if ss.map_style in ('carto-darkmatter', 'dark'):
        ss.cc_color1 = '#FFFFFF'
    else:
        ss.cc_color1 = '#000000'

def init_map_basics():
    if not sse('style_dict'):
        ss.style_dict = {'Positron': 'carto-positron',
                         'Open Map': 'open-street-map',
                         'Dark Matter': 'carto-darkmatter',
                         'Whiteout': 'white-bg'}
        ss.map_style = 'carto-positron'
        ss.cc_color1 = '#000000'
    return


def col_selector(col_type):
    # type_choice = st.selectbox("Leaderboard Type", ["Basic", "Fitness", "Advanced", "Environment"])

    if col_type == 'Basic':
        return  ['start_time_utc',
                  'pace_str',
                  'gap_s',
                  'avg_hr',
                 'weight_lb']

    if col_type == 'Fitness':
        return ['gap_s',
                'vo2_max_value',
                'resting_hr_asleep',
                'training_load_acute',
                'weight_lb',
                'fat_pct',
                'muscle_pct']

    if col_type == 'Advanced':
        return ['gap_s',
                'avg_cadence',
                'avg_vert_osc',
                'avg_gct',
                'avg_stride_length']

    if col_type == 'Environment':
        return ['gap_s',
                'avg_temp',
                'altitude_acclimation_m'
                ]

    if col_type == 'Preparedness':
        return ['gap_s',
                'training_load_acute',
                'training_load_pct',
                'sleep_hours',
                'sleep_score',
                'awake_hours']

# start_time_utc

# elapsed_duration_s
# pace_str
# weight_lb
# muscle_lb
# fat_lb
# vo2_max_value
# altitude_acclimation_m
# heat_acclimation_pct
# training_load_acute
# training_load_pct
# resting_hr_asleep
# resting_hr_awake
# sleep_duration_s
# sleep_score
# light_sleep_s
# deep_sleep_s
# awake_sleep_s
# rem_sleep_s
# awake_s_before_activity
# max_hr
# avg_hr
# avg_cadence
# avg_temp
# avg_vert_osc
# avg_vert_ratio
# avg_gct
# avg_perf
# avg_balance
# gap_s

def safe_minmax(df, col_name, min_default=None, return_max=True):
    if df.empty:
        if return_max:
            return 1
        else:
            return 0

    if min_default:
        min_val = min_default
    else:
        min_val = df[col_name].min()
        if not pd.notna(min_val) or not isinstance(min_val, (int, float, np.number)):
            min_val =0

    max_val = df[col_name].max()
    if not pd.notna(min_val) or not isinstance(min_val, (int, float, np.number)) or max_val == min_val:
        max_val = min_val + 1

    if return_max:
        return max_val
    else:
        return min_val