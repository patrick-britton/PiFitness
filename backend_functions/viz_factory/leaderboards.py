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
        type_choice = st.selectbox("Leaderboard Type", ["Basic", "Fitness", "Advanced", "Environment"])

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
    chart_col, config_col = st.columns(spec=[3,1], gap="small", border=False)

    with config_col:
        map_style_widget()

    with chart_col:
        fig = create_static_leaderboard_viz(ss.df_race)
        st.plotly_chart(fig, width=800)

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


def create_static_leaderboard_viz(df_long):
    activities = df_long['id_val'].unique()
    colors = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A']

    # --- 1. View & Bound Calculations (Unchanged) ---
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

    base_zoom = 14.55 if max_bound_deg > 0.0264 else 15.01
    zoom = base_zoom - np.log(max_bound) if not math.isnan(base_zoom - np.log(max_bound)) else 16.78

    fig = make_subplots(
        rows=5, cols=1,
        row_heights=[0.4, 0.2, 0.2, 0.2, 0.2],
        vertical_spacing=0.03,
        shared_xaxes=True,
        specs=[[{"type": "mapbox"}], [{"type": "xy"}], [{"type": "xy"}], [{"type": "xy"}], [{"type": "xy"}]]
    )

    # --- 2. ADD STATIC LINES (The Course) ---
    for i, id_val in enumerate(activities):
        data = df_long[df_long['id_val'] == id_val]
        color = colors[i % len(colors)]

        fig.add_trace(go.Scattermapbox(lat=data['lat'], lon=data['lon'], mode='lines', line=dict(width=3, color=color),
                                       name=f"{id_val}", legendgroup=f"group{id_val}"), row=1, col=1)
        fig.add_trace(
            go.Scatter(x=data['x_distance'], y=data['y_time_delta'], mode='lines', line=dict(color=color), name="Time",
                       legendgroup=f"group{id_val}", showlegend=False), row=2, col=1)
        fig.add_trace(go.Scatter(x=data['x_distance'], y=data['y_elevation'], mode='lines', line=dict(color=color),
                                 name="Elevation", legendgroup=f"group{id_val}", showlegend=False), row=3, col=1)
        fig.add_trace(
            go.Scatter(x=data['x_distance'], y=data['y_hr'], mode='lines', line=dict(color=color), name="Heart Rate",
                       legendgroup=f"group{id_val}", showlegend=False), row=4, col=1)
        fig.add_trace(
            go.Scatter(x=data['x_distance'], y=data['y_cadence'], mode='lines', line=dict(color=color), name="Cadence",
                       legendgroup=f"group{id_val}", showlegend=False), row=5, col=1)

    # --- 3. ADD MOVING DOTS (The Racers) ---
    # We must save the indices of these dot traces so the animation knows exactly what to move
    dot_indices = []
    current_trace_idx = len(activities) * 5

    for i, id_val in enumerate(activities):
        data = df_long[df_long['id_val'] == id_val]
        color = colors[i % len(colors)]
        row0 = data.iloc[0]  # Starting position of each metric

        # Notice: mode='markers' is used here to create the dot
        fig.add_trace(
            go.Scattermapbox(lat=[row0['lat']], lon=[row0['lon']], mode='markers', marker=dict(size=12, color=color),
                             showlegend=False), row=1, col=1)
        dot_indices.append(current_trace_idx);
        current_trace_idx += 1

        fig.add_trace(go.Scatter(x=[row0['x_distance']], y=[row0['y_time_delta']], mode='markers',
                                 marker=dict(size=8, color=color), showlegend=False), row=2, col=1)
        dot_indices.append(current_trace_idx);
        current_trace_idx += 1

        fig.add_trace(go.Scatter(x=[row0['x_distance']], y=[row0['y_elevation']], mode='markers',
                                 marker=dict(size=8, color=color), showlegend=False), row=3, col=1)
        dot_indices.append(current_trace_idx);
        current_trace_idx += 1

        fig.add_trace(
            go.Scatter(x=[row0['x_distance']], y=[row0['y_hr']], mode='markers', marker=dict(size=8, color=color),
                       showlegend=False), row=4, col=1)
        dot_indices.append(current_trace_idx);
        current_trace_idx += 1

        fig.add_trace(
            go.Scatter(x=[row0['x_distance']], y=[row0['y_cadence']], mode='markers', marker=dict(size=8, color=color),
                       showlegend=False), row=5, col=1)
        dot_indices.append(current_trace_idx);
        current_trace_idx += 1

    # --- 4. BUILD ANIMATION FRAMES ---
    # Find the longest effort to set the timeline
    max_len = max([len(df_long[df_long['id_val'] == act]) for act in activities])

    # DOWNSAMPLING: Force the animation into ~150 frames. Any more causes browser lag.
    step = max(1, max_len // 150)

    frames = []
    for k in range(0, max_len, step):
        frame_data = []
        for id_val in activities:
            data = df_long[df_long['id_val'] == id_val]
            # If someone finished early, lock their dot at their final row
            idx = min(k, len(data) - 1)
            row = data.iloc[idx]

            # The order here MUST match the order we added the Moving Dots in step 3
            frame_data.extend([
                go.Scattermapbox(lat=[row['lat']], lon=[row['lon']]),
                go.Scatter(x=[row['x_distance']], y=[row['y_time_delta']]),
                go.Scatter(x=[row['x_distance']], y=[row['y_elevation']]),
                go.Scatter(x=[row['x_distance']], y=[row['y_hr']]),
                go.Scatter(x=[row['x_distance']], y=[row['y_cadence']])
            ])

        frames.append(go.Frame(data=frame_data, traces=dot_indices, name=f'frame_{k}'))

    fig.frames = frames

    # --- 5. LAYOUT & FIXES ---
    token = mapbox_token()  # Assuming this is a custom function of yours
    view = ss.style_dict.get(ss.get('cc_map_style'))
    if not view:
        view = 'open-street-map'  # Simplified for the example

    fig.update_layout(
        height=1400,
        mapbox_style=view,
        mapbox_accesstoken=token if token else None,
        mapbox=dict(center=dict(lat=center_lat, lon=center_lon), zoom=zoom),
        margin=dict(l=0, r=0, t=0, b=0),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),

        # Add the Play Button
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            y=1.05,  # Positioned near the top/legend
            x=0.8,
            buttons=[dict(
                label="▶ PLAY RACE",
                method="animate",
                args=[None, dict(frame=dict(duration=50, redraw=True), fromcurrent=True, mode="immediate")]
            )]
        )]
    )

    # THE FIX FOR THE ZOOM BUG: Lock the Cartesian (Line Chart) axes
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)

    fig.update_yaxes(title_text="Time Gap", row=2, col=1)
    fig.update_yaxes(title_text="Elev (m)", row=3, col=1)
    fig.update_yaxes(title_text="HR (bpm)", row=4, col=1)
    fig.update_yaxes(title_text="Cadence", row=5, col=1)
    fig.update_xaxes(title_text="Distance (m)", row=5, col=1)

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

    st.selectbox(label='Map Style',
             key='cc_map_style',
             options=list(ss.style_dict.keys()),
                 on_change=update_map_options)


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
        ss.style_dict = {'Open Map': 'open-street-map',
                         'Positron': 'carto-positron',
                         'Dark Matter': 'carto-darkmatter',
                         'Whiteout': 'white-bg'}
        ss.map_style = 'carto-positron'
        ss.cc_color1 = '#000000'
    return