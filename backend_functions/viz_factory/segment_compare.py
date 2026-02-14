import math

import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from shapely import wkb
import numpy as np

from backend_functions.database_functions import qec, one_sql_result
from frontend_functions.streamlit_helpers import ss_pop


def render_segment_compare(segments_list, list_type='Race'):

    # --- 1. PRE-PROCESSING ---
    processed_dfs = []
    active_labels = []
    max_zoom = get_auto_zoom(segments_list, padding=0)

    for item in segments_list:
        df = parse_trajectory_to_df(item['activity_path'], item['effort_label'])
        if not df.empty:
            processed_dfs.append(df)
            active_labels.append(item['effort_label'])

    if not processed_dfs:
        st.warning("No valid path data found.")
        return

    if list_type == 'Race':
        gap_df = interpolate_for_gap_analysis(processed_dfs, baseline_label='most_recent')
    else:
        gap_df = pd.DataFrame()

    # --- 2. CONTROLS ---
    if list_type =='Race':
        st.subheader('Race Analysis')
    elif list_type == 'Merge Segment':
        st.subheader('Segment Merge')
        st.write(f"__{segments_list[0].get('segment_name')}__ vs :red[__{segments_list[1].get('segment_name')}__] : __{segments_list[0].get('match_confidence')}__")
        st.write(f":gray[*{segments_list[0].get('segment_id')} vs {segments_list[1].get('segment_id')}*]")
        m_col, dnm_col = st.columns(spec=[1,1], gap="small", border=False)
        with m_col:
            if st.button(':material/cell_merge: Merge Segments'):
                merge_segments(segments_list, merge=True)
                ss_pop(['seg_dict', 'man_seg_dict'])
                st.rerun()
        with dnm_col:
            if st.button(':material/call_split: Do Not Merge'):
                merge_segments(segments_list, merge=False)
                ss_pop(['seg_dict', 'man_seg_dict'])
                st.rerun()
    elif list_type == 'Verify Segment':
        st.subheader('Segment Verification')
        st.write(
            f"__{segments_list[0].get('segment_name')}__ vs :red[__{segments_list[1].get('activity_date')}__] : __{segments_list[0].get('match_confidence')}__")
        c_col, deny_col, create_col, rematch_col = st.columns(spec=[1, 1, 1, 1], gap="small", border=False)
        with c_col:
            if st.button(':material/check_circle: Confirm Match'):
                verify_segments(segments_list, is_valid=True)
                st.rerun()
        with deny_col:
            if st.button(':material/cancel: Deny Match'):
                verify_segments(segments_list, is_valid=True)
                st.rerun()
        with rematch_col:
            if st.button(':material/recycling: Reject & rematch'):
                verify_segments(segments_list, is_valid=False, rematch=True)
                st.rerun()
        with create_col:
            if st.button(':material/add_circle: Create New Course'):
                verify_segments(segments_list, is_valid=False, create=True)
                st.rerun()

    elif list_type == 'Course Review':
        seg=segments_list[0]
        kv = f'ncn_kv_{seg.get('segment_id')}'
        st.text_input(f"Course ID# {seg.get('segment_id')}",
                      key=kv,
                      value= seg.get('segment_name'),
                      on_change=update_course_name,
                      args=(kv, int(seg.get('segment_id')))
                      )
        st.write(f'__Distance__: {seg.get('distance_mi')} :green[__:material/trending_up:__ {seg.get('elevation_gain')}] :red[__:material/trending_down:__ {seg.get('elevation_loss')}]')
        st.write(f"__Last Effort__: {seg.get('last_event_utc')} __Matched Efforts__: {seg.get('matched_activities')}")


    max_common_dist = min(d['cum_dist'].max() for d in processed_dfs)
    x_pad = int(max_common_dist * .05)
    shared_x_range = [-x_pad, max_common_dist+x_pad]
    anim_speed = st.select_slider(
        "Replay Speed",
        options=['Slow', 'Normal', 'Fast', 'Turbo'],
        value='Normal'
    )

    dur_map = {'Slow': 200, 'Normal': 100, 'Fast': 50, 'Turbo': 10}
    frame_dur = dur_map[anim_speed]

    # --- 3. SHARED CLOCK ---
    max_time = max(d['time'].max() for d in processed_dfs)
    target_frames = 500
    time_step = max(1, int(max_time / target_frames))
    frames_t = range(0, int(max_time) + time_step, time_step)

    style_config = get_style_config()

    # >>> CHANGED: single subplot figure
    fig = make_subplots(
        rows=3,
        cols=1,
        row_heights=[0.5, 0.25, 0.25],
        specs=[
            [{"type": "mapbox"}],
            [{"type": "xy"}],
            [{"type": "xy"}]
        ],
        vertical_spacing=0.05
    )

    # --- 4. STATIC TRACES ---
    for df in processed_dfs:
        lbl = df['label'].iloc[0]
        style = style_config.get(lbl)

        # Map ghost line
        fig.add_trace(
            go.Scattermapbox(
                lon=df['lon'],
                lat=df['lat'],
                mode="lines",
                line=dict(color=style['color'], width=3),
                opacity=0.3,
                hoverinfo="skip",
                showlegend=False
            ),
            row=1, col=1
        )

        # Elevation ghost line
        fig.add_trace(
            go.Scatter(
                x=df['cum_dist'],
                y=df['elev'],
                mode="lines",
                line=dict(color=style['color'], width=2),
                opacity=0.3,
                hoverinfo="skip",
                showlegend=False
            ),
            row=2, col=1
        )

    # Gap static lines
    if not gap_df.empty:
        for label in gap_df['label'].unique():
            subset = gap_df[gap_df['label'] == label]
            style = style_config.get(label)

            fig.add_trace(
                go.Scatter(
                    x=subset['dist'],
                    y=subset['time_gap'],
                    mode='lines',
                    line=dict(color=style['color'], width=2),
                    hovertemplate='%{y:.1f}s gap at %{x:.0f}m<extra></extra>',
                    showlegend=False
                ),
                row=3, col=1
            )

    # --- 5. DYNAMIC TRACES (markers + cursor) ---
    marker_start_idx = len(fig.data)

    for df in processed_dfs:
        style = style_config.get(df['label'].iloc[0])

        fig.add_trace(
            go.Scattermapbox(
                lon=[df['lon'].iloc[0]],
                lat=[df['lat'].iloc[0]],
                mode="markers",
                marker=dict(size=12, color=style['color']),
                showlegend=False
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=[df['cum_dist'].iloc[0]],
                y=[df['elev'].iloc[0]],
                mode="markers",
                marker=dict(size=10, color=style['color']),
                showlegend=False
            ),
            row=2, col=1
        )

    # >>> NEW: animated gap cursor
    if list_type=='Race':
        fig.add_trace(
            go.Scatter(
                x=[0, 0],
                y=[gap_df['time_gap'].min(), gap_df['time_gap'].max()],
                mode="lines",
                line=dict(color="black", dash="solid"),
                showlegend=False
            ),
            row=3, col=1
        )

    # --- 6. FRAMES ---
    frames = []

    for t in frames_t:
        frame_data = []

        for df in processed_dfs:
            lon = np.interp(t, df['time'], df['lon'])
            lat = np.interp(t, df['time'], df['lat'])
            dist = np.interp(t, df['time'], df['cum_dist'])
            elev = np.interp(t, df['time'], df['elev'])

            frame_data.append(go.Scattermapbox(lon=[lon], lat=[lat]))
            frame_data.append(go.Scatter(x=[dist], y=[elev]))

        # Gap cursor position
        if list_type == 'Race':
            frame_data.append(
                go.Scatter(
                    x=[dist, dist],
                    y=[gap_df['time_gap'].min(), gap_df['time_gap'].max()]
                )
        )

        frames.append(
            go.Frame(
                data=frame_data,
                traces=list(range(marker_start_idx, len(fig.data))),
                name=str(t)
            )
        )

    fig.frames = frames

    # --- 7. LAYOUT ---
    center_lat = processed_dfs[0]['lat'].mean()
    center_lon = processed_dfs[0]['lon'].mean()

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=max_zoom
        ),
        updatemenus=[dict(
            type="buttons",
            direction="left",
            showactive=False,
            x=0.10,
            y=1.05,
            xanchor="center",
            yanchor="top",
            pad=dict(t=0, r=0),
            bgcolor="white",
            bordercolor="black",
            font=dict(color="black", size=14),
            buttons=[
                dict(
                    label="▶ Play",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=frame_dur, redraw=True),
                                    transition=dict(duration=0),
                                    fromcurrent=True,
                                    mode="immediate"
                                )
                            ],
                        ),
                        dict(
                            label="⏸ Pause",
                            method="animate",
                            args=[
                                [],                      # <-- THIS is the key
                                dict(
                                    mode="immediate"
                                )
                            ],
                        ),
                    ],
                )
            ],
        height=900,
        template="plotly_dark",
        uirevision="race",   # >>> NEW: preserve zoom/pan
        margin=dict(l=0, r=0, t=30, b=0),
        showlegend=False
    )

    fig.update_yaxes(autorange="reversed", row=3, col=1)
    fig.update_xaxes(title_text="", row=2, col=1, range=shared_x_range, autorange=False)
    fig.update_xaxes(title_text="Distance (m)", row=3, col=1, range=shared_x_range, autorange=False)

    render_custom_legend(active_labels)
    st.plotly_chart(fig, width='content')
    st.write(active_labels)
    return


def parse_trajectory_to_df(wkb_hex, label):
    """
    Parses a PostGIS HEX string (LineStringZM) into a structured DataFrame.
    Calculates cumulative distance for 'Gap' analysis.
    """
    # 1. Parse WKB
    try:
        line = wkb.loads(bytes.fromhex(wkb_hex))
        # coords structure: [(x, y, z, m), ...]
        coords = list(line.coords)
    except Exception as e:
        st.error(f"Error parsing geometry for {label}: {e}")
        return pd.DataFrame()

    # 2. Create DataFrame
    # Note: Z is Elevation, M is Elapsed Time (seconds)
    df = pd.DataFrame(coords, columns=['lon', 'lat', 'elev', 'time'])

    # 3. Calculate Cumulative Distance (Meters)
    # Using a vectorized Haversine approximation for speed
    R = 6371000  # Earth radius in meters
    phi1 = np.radians(df['lat'].shift(1))
    phi2 = np.radians(df['lat'])
    dphi = np.radians(df['lat'].diff())
    dlambda = np.radians(df['lon'].diff())

    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    df['dist_step'] = R * c
    df['dist_step'] = df['dist_step'].fillna(0)
    df['cum_dist'] = df['dist_step'].cumsum()

    # 4. Add Metadata
    df['label'] = label

    return df


def interpolate_for_gap_analysis(all_dfs, baseline_label='most_recent', step_meters=10):
    """
    Resamples all dataframes to a common distance grid to calculate time gaps.
    """
    # Find the dataframe that matches the baseline label
    baseline_dfs = [d for d in all_dfs if d['label'].iloc[0] == baseline_label]
    if not baseline_dfs:
        return pd.DataFrame()  # No baseline, cannot calculate gaps

    base_df = baseline_dfs[0]

    # Create a common distance grid (0, 10, 20... up to max distance)
    max_dist = min([d['cum_dist'].max() for d in all_dfs])
    common_dist = np.arange(0, max_dist, step_meters)

    gap_data = []

    # Calculate Time at Distance X for Baseline
    # np.interp(target_x, known_x, known_y)
    base_times = np.interp(common_dist, base_df['cum_dist'], base_df['time'])

    for df in all_dfs:
        label = df['label'].iloc[0]
        if label == baseline_label:
            continue

        # Calculate Time at Distance X for Comparison
        comp_times = np.interp(common_dist, df['cum_dist'], df['time'])

        # Calculate Gap (Negative = Ahead of Baseline, Positive = Behind)
        # Gap = Effort Time - Baseline Time
        time_diffs = comp_times - base_times

        # Store result
        temp_df = pd.DataFrame({
            'dist': common_dist,
            'time_gap': time_diffs,
            'label': label
        })
        gap_data.append(temp_df)

    if gap_data:
        return pd.concat(gap_data, ignore_index=True)
    return pd.DataFrame()



def get_style_config():
    d = {
    'most_recent': {'color': '#000000', 'opacity': 1.0, 'name': 'Baseline'}, # Black
    'prior_attempt': {'color': '#f0690f', 'opacity': 0.6, 'name': 'Prior Attempt'}, # Orange
    'best_of_cycle': {'color': '#1440f0', 'opacity': 0.8, 'name': 'Best This Cycle'}, # Blue
    'best_in_last_year': {'color': '#19D3F3', 'opacity': 0.6, 'name': 'Best This Year'}, # Cyan
    'best_all_time': {'color': '#B89230', 'opacity': 0.4, 'name': 'Best Ever'},
    'primary_segment': {'color': '#000000', 'opacity': 1.0, 'name': 'Primary Segment'}, # Black
    'pretender_segment': {'color': '#EA3323', 'opacity': 0.5, 'name': 'Pretender Segment'}, # Red
    'segment': {'color': '#000000', 'opacity': 1.0, 'name': 'Known Segment'},  # Black
    'activity': {'color': '#EA3323', 'opacity': 0.5, 'name': 'Matched Activity'},  # Red
        'course': {'color': '#000000', 'opacity': 1.0, 'name': 'Selected Course'},
    'other': {'color': '#B7B7B7', 'opacity': 0.1, 'name': 'Other'} # Gray
    }
    return d


def render_custom_legend(active_labels):
    """
    Renders a clean HTML legend for the active traces.
    """
    legend_html = '<div style="display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 10px; justify-content: center;">'

    for label in active_labels:
        style = get_style_config().get(label, {'color': 'gray', 'name': label})
        legend_html += f'''<div style="display: flex; align-items: center;">
        <span style="display: inline-block; width: 12px; height: 12px;
        background-color: {style['color']}; border-radius: 50%; margin-right: 5px;"></span>
        <span style="font-size: 14px; color: {style['color']};">{style['name']}</span></div>'''
    legend_html += '</div>'
    st.markdown(legend_html, unsafe_allow_html=True)
    return


def get_auto_zoom(seg_list, padding=0.1):
    """Get optimal zoom from shapely geometry with safety checks"""
    zoom = 0
    lon_span_list = []
    lat_span_list = []
    try:
        for seg in seg_list:
            seg_path = seg.get('activity_path')
            trajectory = wkb.loads(bytes.fromhex(seg_path))

            bounds = trajectory.bounds  # (minx, miny, maxx, maxy)

            lon_span_list.append((bounds[2] - bounds[0]) * (1 + padding))
            lat_span_list.append((bounds[3] - bounds[1]) * (1 + padding))

        lon_span = max(lon_span_list)
        lat_span = max(lat_span_list)

        # Handle edge case of single point or very small area
        if lon_span < 0.0001:
            lon_span = 0.001
        if lat_span < 0.0001:
            lat_span = 0.001

        # Prevent overflow/underflow
        if lon_span > 360:
            lon_span = 360
        if lat_span > 180:
            lat_span = 180

        zoom_lon = math.log2(360 / lon_span) if lon_span > 0 else 15
        zoom_lat = math.log2(180 / lat_span) if lat_span > 0 else 15

        zoom = min(zoom_lon, zoom_lat)
        return zoom
    except Exception as e:
        return 13


def merge_segments(seg_l, merge=False):
    losing_act_list = []
    losing_seg = None
    for seg in seg_l:
        print(f"{seg.get('segment_id')}, {seg.get('effort_label')}")
        if seg.get('effort_label') == 'primary_segment':
            win_seg = seg.get('segment_id')
            print(f"Winning id: {win_seg}")
        else:
            losing_seg = seg.get('segment_id')
            print(f"Losing id: {losing_seg}")
            losing_act_list = seg.get('matched_activities')

    if not losing_seg:
        st.error(f'No losing segment identified {seg_l}')

        return

    if merge:
        if seg_l:
            if losing_act_list:
                for a in losing_act_list:
                    try:
                        up_sql = f"""UPDATE activities.segment_matches
                                    SET segment_id = {win_seg},
                                    match_ts_utc = CURRENT_TIMESTAMP
                                    WHERE segment_id = {losing_seg} and activity_id = {a}"""
                        qec(up_sql)
                    except Exception as e:
                        del_sql = f"""DELETE FROM activities.segment_matches
                                    WHERE segment_id = {losing_seg} and activity_id = {a}"""
                        qec(del_sql)
            del_sql = f"DELETE FROM activities.overlap_queue where segment_id_a = {losing_seg} or segment_id_b = {losing_seg}"
            qec(del_sql)
            del_sql = f"DELETE from activities.segments where segment_id = {losing_seg}"
            qec(del_sql)
    else:
        del_sql = f"DELETE FROM activities.overlap_queue where segment_id_a = {win_seg} and segment_id_b = {losing_seg}"
        qec(del_sql)
        ins_sql = f"""INSERT INTO activities.distinct_segments (segment_id, match_seg_id) VALUES (%s, %s)"""
        params = [int(win_seg), int(losing_seg)]
        qec(ins_sql, params)
    return

def get_segment_sql(id1, id2):
    sql = f"""WITH ec as (
            SELECT 
            segment_id,
            count(DISTINCT CASE WHEN match_confirmed THEN NULL else activity_id END) as unconfirmed_effort_count,
            COUNT(DISTINCT CASE WHEN match_confirmed THEN activity_id else NULL END) as confirmed_effort_count,
            max(activity_id) as max_id,
            array_agg(activity_id) as activity_list
            FROM activities.segment_matches
            WHERE segment_id in ({int(id1)}, {int(id2)}) 
            GROUP by segment_id
            )
            
            , top_match as (
            SELECT *, row_number() OVER (ORDER BY match_confidence desc) as match_rank FROM (
            SELECT 
            s.segment_id,
            s.segment_name,
            s.segment_path,
            sm.segment_id as matching_segment_id,
            sm.segment_name as matching_segment_name,
            sm.segment_path as matching_segment_path,
            match_confidence(s.segment_path, sm.segment_path) as match_confidence
            FROM activities.segments s
            INNER JOIN activities.segments sm on sm.segment_id = {int(id2)}

            WHERE s.segment_id = {int(id1)}
            ) sub
            ORDER BY match_confidence desc
            )
            
            , u as (
            SELECT match_rank, match_confidence, segment_id, segment_name, segment_path from top_match
            UNION 
            SELECT match_rank, match_confidence, matching_segment_id as segment_id, matching_segment_name as segment_name, matching_segment_path as segment_path from top_match
            )
            
            
            SELECT
            match_rank,
            match_confidence,
            segment_id,
            segment_name,
            CASE WHEN segment_rank = 1
            THEN 'primary_segment'
            ELSE 'pretender_segment'
            end as effort_label,
            segment_path as activity_path,
            activity_list as matched_activities
            FROM (
            SELECT 
            u.match_rank,
            u.match_confidence,
            u.segment_id,
            u.segment_name,
            u.segment_path,
            ec.activity_list,
            ROW_NUMBER() over (PARTITION BY u.match_rank ORDER BY COALESCE(confirmed_effort_count,0) desc, COALESCE(unconfirmed_effort_count,0) desc, COALESCE(max_id,0) DESC) as segment_rank
            from u
            LEFT JOIN ec on u.segment_id = ec.segment_id
            ) subquery"""
    print(sql)
    return sql



def verify_segments(segments_list, is_valid=True, create=False, rematch=False):

    activity_id = segments_list[0].get('activity_id')
    segment_id = segments_list[0].get('segment_id')
    params = [int(segment_id), int(activity_id)]
    if is_valid:
        up_sql = f"""UPDATE activities.segment_matches SET match_confirmed = TRUE
                    WHERE segment_id = %s and activity_id = %s"""

        qec(up_sql, params)
    elif create:
        create_course_from_sql(activity_id)
        del_sql = f"DELETE FROM activities.segment_matches WHERE segment_id = %s and activity_id = %s"
        qec(del_sql, params)
    elif rematch:
        del_sql = f"DELETE FROM activities.segment_matches WHERE segment_id = %s and activity_id = %s"
        returns = qec(del_sql, params)
        if returns:
            print(returns)
        else:
            print('deleted from matches')
        ins_sql = f"""INSERT INTO activities.segment_match_exclusions (segment_id, activity_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"""
        returns = qec(ins_sql, params)
        if returns:
            print(returns)
        else:
            print('Added to exclusions')
        rematch_activity(activity_id)

    else:
        del_sql = f"DELETE FROM activities.segment_matches WHERE segment_id = %s and activity_id = %s"
        qec(del_sql, params)
        ins_sql = f"""INSERT INTO activities.segment_match_exclusions (segment_id, activity_id) VALUES (%s, %s)"""
        qec(ins_sql, params)
    return

def create_course_from_sql(activity_id):

    max_id = int(one_sql_result(f"""SELECT MAX(elapsed_duration_s) FROM activities.activity_details WHERE activity_id = {int(activity_id)}"""))

    new_seg_sql = f"""
            CALL staging.create_segment_from_activity(
			{int(activity_id)},
			0,
			{max_id},
			TRUE);"""
    returns = qec(new_seg_sql)
    if returns:
        print(returns)
    else:
        print('segment creation okay')

    new_id_sql = f"""SELECT max(segment_id) from activities.segments;"""
    new_id = int(one_sql_result(new_id_sql))

    match_sql = f"""
		INSERT INTO activities.segment_matches (
        segment_id,
		activity_id,
		activity_start_point,
		activity_end_point,
		activity_path,
		match_confirmed,
		match_confidence,
		match_ts_utc
    	)
		SELECT
		{new_id} as segment_id,
		activity_id,
		0 as activity_start_point,
		{max_id} as activity_end_point,
		activity_path,
		TRUE as match_confirmed,
		100.0 as match_confidence,
		CURRENT_TIMESTAMP as match_ts_utc
		FROM activities.activities
		WHERE activity_id = {activity_id}
		ON CONFLICT (segment_id, activity_id)
		DO UPDATE SET
		activity_start_point = EXCLUDED.activity_start_point,
		activity_end_point = EXCLUDED.activity_end_point,
		activity_path = EXCLUDED.activity_path,
		match_confirmed = EXCLUDED.match_confirmed,
		match_confidence = EXCLUDED.match_confidence,
		match_ts_utc = EXCLUDED.match_ts_utc;"""
    qec(match_sql)
    return

def rematch_activity(activity_id):
    max_sec = int(one_sql_result(f"SELECT MAX(elapsed_duration_s) FROM activities.activity_details WHERE activity_id = {activity_id}"))
    sql = f"""CALL staging.match_activity_to_segment(
                {activity_id},
                0,
                {max_sec},
                TRUE);"""
    qec(sql)
    return

def update_course_name(key_val, seg_id):
    new_name = ss.get(key_val)
    if not new_name:
        return

    if len(new_name) <5:
        st.toast('New name must be more than 4 characters', duration=5)
        return

    up_sql = f"""UPDATE activities.segments SET segment_name = %s WHERE segment_id = %s;"""
    params = (new_name, seg_id)
    qec(up_sql, params)
    return