import math

import pandas as pd
import streamlit as st
from fontTools.cu2qu.cu2qu import NAN
from plotly.subplots import make_subplots
from streamlit import session_state as ss
import numpy as np
import plotly.graph_objects as go

from backend_functions.database_functions import get_conn, sql_to_dict, qec, one_sql_result, sql_to_list
from backend_functions.running_functions import leaderboard_update
from backend_functions.service_logins import mapbox_token
from backend_functions.viz_factory.leaderboards import render_leaderboard
from backend_functions.viz_factory.segment_compare import render_segment_compare
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_segment_creation():
    st.divider()
    init_map_basics()
    if st.button(':red[:material/crisis_alert: Reset Match Tables]'):
        qec('TRUNCATE activities.segment_matches;')
        qec('TRUNCATE activities.segment_match_exclusions;')
        qec('TRUNCATE activities.distinct_segments;')
        returns = qec('TRUNCATE activities.segments RESTART IDENTITY CASCADE')
        if returns:
            st.error(returns)
            return
        ss_pop(['cc_selected_id', 'comp_df', 'result_df'])
        st.rerun()

    st.write('__SEGMENT CREATION__')
    ca_col, emergency_reset_col = st.columns(spec=[1,1], gap="small", border=False)


    if not sse('cc_selected_id'):
        ss_pop('comp_df')
        select_course_create_activity()
        return


    render_activity(ss.cc_selected_id)
    # for sval in ss:
    #     st.caption(f"{sval}: {ss.get(sval)}")
    return

def plot_route_map(df_main, df_compare=None, focus_type=None):
    fig = go.Figure()

    if not sse('cc_color1'):
        ss.cc_color1= '#FFFFFF'


    # Base Activity Line
    fig.add_trace(go.Scattermapbox(
        lat=df_main['latitude'],
        lon=df_main['longitude'],
        mode='lines',
        opacity=0.5 if df_compare is not None else 1.0,
        line=dict(width=4, color=ss.cc_color1),
        name='Main Activity',
        customdata=np.stack((df_main['elevation_m'], df_main['distance_m']), axis=-1),
        hovertemplate=(
            "<b>Time:</b> %{customdata[1]}m<br>"
            "<b>Elevation:</b> %{customdata[0]:.1f}m<br>"
            "<b>Lat:</b> %{lat:.5f}<br>"
            "<b>Lon:</b> %{lon:.5f}<extra></extra>"
        )
    ))

    if focus_type == 'start':
        df_main = df_main[df_main['distance_m'] <= df_main['distance_m'].min() + 150]
        subrow = df_main.head(1)
    elif focus_type == 'end':
        df_main = df_main[df_main['distance_m'] >= df_main['distance_m'].max()-150]
        subrow = df_main.tail(1)

    # Calculate initial bounds based on main activity
    min_lat = df_main['latitude'].min()
    max_lat = df_main['latitude'].max()
    min_lon = df_main['longitude'].min()
    max_lon = df_main['longitude'].max()
    if focus_type in ('start', 'end'):
        center_lat = subrow['latitude'].max()
        center_lon = subrow['longitude'].max()
    else:
        center_lat = round((max_lat+min_lat) / 2,5)
        center_lon = round((max_lon+min_lon) / 2,5)

    # Step 10 Overlay Match Comparison
    if df_compare is not None:
        fig.add_trace(go.Scattermapbox(
            lat=df_compare['latitude'],
            lon=df_compare['longitude'],
            mode='lines',
            opacity = 0.5,
            line=dict(width=4, color='#EA3323'),
            name='Segment Match',
            customdata=np.stack((df_compare['elevation_m'], df_compare['distance_m']), axis=-1),
            hovertemplate=(
                "<b>Match Time:</b> %{customdata[1]}s<br>"
                "<b>Match Elev:</b> %{customdata[0]:.1f}m<br>"
                "<b>Lat:</b> %{lat:.5f}<br>"
                "<b>Lon:</b> %{lon:.5f}<extra></extra>"
            )
        ))

        # Expand bounds if the comparison dataframe goes beyond the main dataframe
        min_lat = min(min_lat, df_compare['latitude'].min())
        max_lat = max(max_lat, df_compare['latitude'].max())
        min_lon = min(min_lon, df_compare['longitude'].min())
        max_lon = max(max_lon, df_compare['longitude'].max())

    lat_span = abs(max_lat - min_lat)
    lon_span = abs(max_lon - min_lon)
    adjusted_lat_span = lat_span * (16 / 9) * 1.1
    adjusted_lon_span = lon_span * 1.1
    max_bound_deg = max(adjusted_lat_span, adjusted_lon_span)
    max_bound = max_bound_deg* 111

    if max_bound_deg > 0.0264:
        base_zoom = 14.55
    else:
        base_zoom = 15.15
    zoom = base_zoom - np.log(max_bound)
    if math.isnan(zoom):
        zoom = 16.78
        # st.write(zoom, df_main['latitude'].max(), min_lat, max_lon, min_lon)

    token = mapbox_token()
    if token:
        fig.update_layout(mapbox_accesstoken=token)

    view = ss.style_dict.get(ss.get('cc_map_style'))
    if not view:
        view = 'open-street-map'

    # Apply auto-zooming bounds
    fig.update_layout(
        mapbox_style=view, # "carto-positron",
        mapbox=dict(
            # bounds=dict(
            #     west=min_lon,  # minimum longitude
            #     east=max_lon,  # maximum longitude
            #     south=min_lat,  # minimum latitude
            #     north=max_lat  # maximum latitude
            # ),
            center=dict(lat=center_lat, lon=center_lon),
            zoom = zoom
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=True if df_compare is not None else False,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )

    return fig


def plot_elevation_profile(df_main, df_compare=None):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_main['distance_m'],
        y=df_main['elevation_m'],
        mode='lines',
        # fill='tozeroy',
        line=dict(color='#000000'),
        name='Elevation',
        customdata=np.stack((df_main['latitude'], df_main['longitude']), axis=-1),
        hovertemplate=(
            "<b>Dist:</b> %{x}m<br>"
            "<b>Elevation:</b> %{y:.1f}m<br>"
            "<b>Lat:</b> %{customdata[0]:.5f}<br>"
            "<b>Lon:</b> %{customdata[1]:.5f}<extra></extra>"
        )
    ))

    if df_compare is not None:
        fig.add_trace(go.Scatter(
            x=df_compare['distance_m'],
            y=df_compare['elevation_m'],
            mode='lines',
            opacity=.75,
            line=dict(color='#EA3323'),
            name='Match Elevation'
        ))

    fig.update_layout(
        xaxis_title="Distance (m)",
        # yaxis_title="Elevation (m)",
        showlegend=False,
        margin={"r": 10, "t": 30, "l": 10, "b": 10},
        hovermode="x unified"  # Creates a nice vertical crosshair line for elevation plots
    )
    return fig

def get_activity_df(activity_id, start_dist=None, end_dist=None):
    # st.write(start_dist, end_dist)
    sql = f"""SELECT 
                    ROW_NUMBER() OVER (ORDER BY distance_m asc)-1 as distance_m,
                    latitude,
                    longitude,
                    elevation_m_smooth as elevation_m,
                    m2mi(distance_m) as distance_mi
            FROM activities.activity_details_distance
        where activity_id = {activity_id}
        """

    if start_dist is not None:
        sql = f"""{sql} and distance_m BETWEEN {int(start_dist)} and {int(end_dist)} """

    sql = f"""{sql} ORDER BY distance_m"""

    # st.code(sql, language='sql')

    return pd.read_sql(sql, con=get_conn(alchemy=True))


def render_activity(activity_id=None, matching_activity_id=None):
    if not activity_id:
        return

    if not sse('cc_df'):
        ss.cc_df = get_activity_df(activity_id)
    meta_sql = f"""SELECT start_time_utc, m2mi(distance_m) as distance_mi
            FROM activities.activities WHERE activity_id = {int(activity_id)}"""
    meta_dict = sql_to_dict(meta_sql)[0]

    st.caption(f"""ID# __{activity_id}__ @ {meta_dict.get('start_time_utc')} | {meta_dict.get('distance_mi')} mi""")


    # Setup the two-ended slider for trimming

    max_dist = int(ss.cc_df['distance_m'].max())

    map_container = st.container(width=800)

    with map_container:
        name_col, style_col = st.columns([3, 1], gap="small", border=False)
        with name_col:
            if sse('cc_seg_id'):
                sn_lbl = f"Segment Name (id# {ss.get('cc_seg_id')})"
            else:
                sn_lbl = f"Segment Name"
            segment_name = st.text_input(label=sn_lbl, value="")

        with style_col:
            map_style_widget()


        if ss.get('cc_trim_start'):
            trim_range_start = ss.get('cc_trim_start')
        else:
            trim_range_start = 0

        if ss.get('cc_trim_end'):
            trim_range_end = ss.get('cc_trim_end')
        else:
            trim_range_end = ss.cc_df['distance_m'].max()

        df_filtered = ss.cc_df[(ss.cc_df['distance_m'] >= trim_range_start) &
                               (ss.cc_df['distance_m'] <= trim_range_end)]

        st.plotly_chart(plot_route_map(df_filtered), width=800, height=400, key='c-bev'
                        ,config = {'staticPlot': True})

        start_col, end_col = st.columns(spec=[1,1], gap="small", border=True)

        with start_col:
            trim_range_start = st.number_input(
                "Select start gate",
                min_value=0,
                max_value=max_dist,
                value=0,  # Default to full range
                step=1,
                key='cc_trim_start'
            )
            st.plotly_chart(plot_route_map(df_filtered, focus_type='start'), width=400, height=300, key='c-st')

        with end_col:
            trim_range_end = st.number_input(
                "Select end gate",
                min_value=0,
                max_value=max_dist,
                value=max_dist,  # Default to full range
                step=1,
                key='cc_trim_end'
            )
            st.plotly_chart(plot_route_map(df_filtered, focus_type='end'), width=400, height=300, key='c-end')
            # Filter the dataframe based on slider values

        df_min = df_filtered['distance_m'].min()
        df_filtered['distance_m'] = df_filtered['distance_m'] - df_min
        distance = df_filtered['distance_mi'].max() - df_filtered['distance_mi'].min()

        st.caption(
            f"**Selected Path Bounds:** Start: {df_filtered.iloc[0]['latitude']:.5f}, {df_filtered.iloc[0]['longitude']:.5f} | End: {df_filtered.iloc[-1]['latitude']:.5f}, {df_filtered.iloc[-1]['longitude']:.5f} | Distance: {round(distance,2)} mi")

        c_col, s_col, reset_col = st.columns(spec=[1,1,1], gap="small", border=False)
        with c_col:
            if st.button(label=f":material/wand_stars: Create Course"):
                ss.cc_seg_created, ss.cc_seg_id = new_segment_creation(activity_id,
                                                                       trim_range_start,
                                                                       trim_range_end,
                                                                       True, segment_name)
                st.toast(f'Course __{segment_name}__ created as ID# {ss.cc_seg_id}', duration=5)

        with s_col:
            # st.write(trim_range_start, trim_range_end)
            if st.button(label=f":material/wand_stars: Create Segment"):
                ss.cc_seg_created, ss.cc_seg_id = new_segment_creation(activity_id,
                                                                       trim_range_start,
                                                                       trim_range_end,
                                                                       False, segment_name)
                st.toast(f'Segment __{segment_name}__ created as ID# {ss.cc_seg_id}', duration=5)

        with reset_col:
            if st.button(':material/restart_alt: Choose new activity'):
                ss_pop(['cc_selected_id', 'cc_seg_id', 'cc_df'] )
                st.rerun()

            if st.button(':material/restart_alt: New segment from same activity'):
                ss_pop( 'cc_seg_id')
                st.rerun()

        st.plotly_chart(plot_elevation_profile(df_filtered), width=800)

    return

def init_map_basics():
    if not sse('style_dict'):
        ss.style_dict = {'Open Map': 'open-street-map',
                         'Positron': 'carto-positron',
                         'Dark Matter': 'carto-darkmatter',
                         'Whiteout': 'white-bg'}
        ss.map_style = 'carto-positron'
        ss.cc_color1 = '#000000'
    return

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


def visualize_segment_matches():


    act_list = sql_to_list("SELECT DISTINCT activity_id FROM activities.temp_segment_matching_activities")
    if not sse('result_df'):
        return

    if ss.result_df.empty:
        st.info('No matching segments found')
        return


    result_config = {'confirmed': st.column_config.CheckboxColumn('Match', disabled=False),
    'activity_id': st.column_config.NumberColumn('ID#',
                                                 format='localized'),
    'dist_deviation': st.column_config.ProgressColumn('Distance Delta',
                                                 format='percent',
                                                    min_value=0,
                                                    max_value=1),
     'polygon_deviation_m': st.column_config.ProgressColumn('Polygon',
                                                 format='%d',
                                                    min_value=0,
                                                    max_value=int(ss.result_df['polygon_deviation_m'].max())),
    'hausdorff_deviation_m': st.column_config.ProgressColumn('Hausdorff',
                                                 format='%d',
                                                    min_value=0,
                                                    max_value=int(ss.result_df['hausdorff_deviation_m'].max())),
                     'freschet_deviation_m': st.column_config.ProgressColumn('Freschet',
                                                                              format='%d',
                                                                              min_value=0,
                                                                              max_value=int(ss.result_df[
                                                                                                'freschet_deviation_m'].max())),
                     'ruggedness': st.column_config.ProgressColumn('Ruggedness',
                                                                   min_value=0, max_value=1)}
    cols = ['confirmed', 'activity_id', 'dist_deviation', 'polygon_deviation_m', 'freschet_deviation_m', 'hausdorff_deviation_m','ruggedness']
    st.dataframe(ss.result_df, column_order=cols, column_config=result_config,
                 on_select='rerun',
                 key='sm_df_changes',
                 selection_mode='multi-row',
                 hide_index=True)

    st.write(ss.get('sm_df_changes'))
    if ss.get('sm_df_changes'):
        idx_row = ss.get('sm_df_changes').get('selection')
        if idx_row:
            if idx_row.get('rows'):
                ss.idx_exclusions = []
                for idx in idx_row.get('rows'):
                    ss.idx_exclusions.append(idx)
                idx_row = idx_row.get('rows')[0]


                contender_id = ss.result_df.iloc[idx_row]['activity_id']
                contender_start = ss.result_df.iloc[idx_row]['best_start_dist']
                contender_end = ss.result_df.iloc[idx_row]['best_end_dist']
                ss.contender_dict = {'id': contender_id, 'start': contender_start, 'end': contender_end}
                ss.comp_df = get_activity_df(contender_id, contender_start, contender_end)
                # st.write(ss.comp_df.columns)
                if not sse('prior_idx_row'):
                    ss.prior_idx_row = idx_row

                    st.rerun()
                elif ss.prior_idx_row != idx_row:
                    ss.prior_idx_row = idx_row
                    st.rerun()
    st.write(len(ss.get('comp_df')))
    st.write(ss.get('idx_exclusions'))
    return

def new_segment_creation(activity_id, start_s, end_s, is_course, seg_name):
    ins_sql = f"""CALL activities.segment_matching_segment_creation(%s, %s, %s, %s, %s)"""
    params = [seg_name, int(activity_id), int(start_s), int(end_s), is_course]
    returns = qec(ins_sql, params)
    if returns:
        st.error(returns)
        return False, None


    new_id = one_sql_result("SELECT MAX(segment_id) from activities.segments")

    match_sql = f"""CALL activities.segment_matching_finalize_match(True,
                                                                           %s,
                                                                           %s,
                                                                           %s,
                                                                           %s,
                                                                           0::NUMERIC)"""
    params = [int(activity_id), int(new_id), int(start_s), int(end_s)]
    # st.code(match_sql, language="sql")
    returns = qec(match_sql, params)
    if returns:
        st.error(returns)
        return False, None


    return True, new_id



def insert_possible_activity_matches(segment_id):
    qec("TRUNCATE activities.temp_segment_matching_activities;")
    sql = f"""INSERT INTO activities.temp_segment_matching_activities (activity_id)
    SELECT
    a.activity_id
FROM
    activities.activities a
        JOIN
    activities.segments s ON s.segment_id = {int(segment_id)} and a.activity_type_id = s.activity_type_id
WHERE
    a.activity_path && s.segment_path
  AND ST_DWithin(a.activity_path::geography, s.start_point, 50)
  AND ST_DWithin(a.activity_path::geography, s.end_point, 50)    
    """
    qec(sql)
    return

def select_course_create_activity():
    st.info('Select an activity to map from:')
    type_col, dist_col = st.columns(spec=[1, 1], gap="small", border=False)

    with type_col:
        st.segmented_control(label='Select Activity Type',
                             default=None,
                             key='sc_act_type',
                             options=['Run', 'Trail Run', 'Hike', 'Walk', 'Bike', 'Ski'],
                             on_change=ss_pop,
                             args=('cc_act_df',))
    with dist_col:
        st.number_input(label='Minimum Distance',
                        value=4,
                        key='sc_min_dist',
                        min_value=0,
                        max_value=100,
                        step=1,
                        on_change=ss_pop,
                        args=('cc_act_df',)
                        )

    sql = """SELECT * FROM activities.vw_activity_summary_segment_creation """

    filter_sql = None
    if ss.get('sc_act_type') == 'Run':
        filter_sql = f"""WHERE activity_type_name like '%%run%%' and activity_type_name not like '%%trail%%'"""
    elif ss.get('sc_act_type') == 'Trail Run':
        filter_sql = f"""WHERE activity_type_name like '%%trail%%run%%'"""
    elif ss.get('sc_act_type') == 'Hike':
        filter_sql = f"""WHERE activity_type_name like '%%hik%%'"""
    elif ss.get('sc_act_type') == 'Walk':
        filter_sql = f"""WHERE activity_type_name like '%%walk%%'"""
    elif ss.get('sc_act_type') == 'Bike':
        filter_sql = f"""WHERE activity_type_name like '%%bik%%'"""
    elif ss.get('sc_act_type') == 'Ski':
        filter_sql = f"""WHERE activity_type_name like '%%ski%%'"""

    if ss.get('sc_min_dist') is not None:
        distance_m = int(ss.get('sc_min_dist') * 1609.344)
        if filter_sql:
            filter_sql = f"""{filter_sql} AND distance_m >= {distance_m}"""
        else:
            filter_sql = f"WHERE distance_m >= {distance_m} "

    if filter_sql:
        sql = f"""{sql} {filter_sql} ORDER BY start_time_utc desc limit 50"""

    if not sse('cc_act_df'):
        ss.cc_act_df = pd.read_sql(sql, con=get_conn(alchemy=True))

    dist_max = int(ss.cc_act_df['distance_mi'].max()) + 1
    child_segment_max = int(ss.cc_act_df['child_segments'].max())
    matched_courses = int(ss.cc_act_df['matched_courses'].max())
    matched_segments = int(ss.cc_act_df['matched_segments'].max())



    col_config = {'activity_id': st.column_config.TextColumn(label='#', width="small"),
                  'start_time_utc': st.column_config.DatetimeColumn(label='Start Time',
                                                                    format='yyyy-MM-DD',
                                                                    disabled=True,
                                                                    width='small'),
                  'distance_mi': st.column_config.ProgressColumn(label='Miles',
                                                                 min_value=0,
                                                                 format='localized',
                                                                 max_value=dist_max,
                                                                 width='small'),
                  'child_courses': st.column_config.TextColumn(label='Child Courses',
                                                                 width='medium'),
                  'child_segments': st.column_config.ProgressColumn(label='Child Segments',
                                                                 min_value=0,
                                                                 format='%d',
                                                                 max_value=child_segment_max+1,
                                                                 width='small'),
                  'matched_courses': st.column_config.ProgressColumn(label='Matched Courses',
                                                                 min_value=0,
                                                                 format='%d',
                                                                 max_value=matched_courses+1,
                                                                 width='small'),
                  'matched_segments': st.column_config.ProgressColumn(label='Matched Segments',
                                                                 min_value=0,
                                                                 format='%d',
                                                                 max_value=matched_segments+1,
                                                                 width='small'),
                  }

    cols = ['start_time_utc', 'distance_mi', 'child_courses']

    if child_segment_max>0:
        cols += ['child_segments']

    cols += ['matched_courses', 'matched_segments']

    st.dataframe(ss.cc_act_df, column_config=col_config,
                 column_order=cols,
                 on_select='rerun',
                 selection_mode='single-row',
                 key='sc_activity_selection')

    if ss.get('sc_activity_selection'):
        selected_idx_row = ss.get('sc_activity_selection').get('selection').get('rows')
        if selected_idx_row:
            ss.cc_selected_id = ss.cc_act_df.iloc[selected_idx_row[0]]['activity_id']
            ss_pop(['cc_df','cc_act_df', 'result_df', 'cc_df', 'comp_df'])
            st.rerun()
    return

def render_segment_matches():
    init_map_basics()

    if not sse('sm_seg_act_id'):
        get_segment_id()
        return

    if not sse('seg_df'):
        ss.sm_seg_df = get_activity_df(ss.get('sm_seg_act_id'), start_dist=ss.get('sm_seg_start'), end_dist=ss.get('sm_seg_end'))
        ss.sm_comp_df = None



    w = int(700)
    h = int(900 * (w/1600))
    map_col, param_col = st.columns(spec=[3,3], gap='small', border=False)



    with param_col:
        map_style_widget()

        exist_match_sql = f"""SELECT count(distinct activity_id) as cnt FROM activities.segment_matches WHERE
                                   segment_id = {int(ss.get('sm_seg_id'))}
                                   and activity_id != {int(ss.get('sm_seg_act_id'))}"""
        st.info(f"Existing Matches {int(one_sql_result(exist_match_sql))}")
        potential_match_sql = f"""SELECT * FROM activities.vw_temp_segment_matches_downselect WHERE 
                                               segment_id = {int(ss.get('sm_seg_id'))}
                                               and activity_id != {int(ss.get('sm_seg_act_id'))}"""
        if not sse('p_df'):
            ss.p_df = pd.read_sql(potential_match_sql, con=get_conn(alchemy=True))

        if ss.p_df.empty:
            if st.button(':material/counter_1: Find Matches'):
                with st.spinner('Getting Matches', show_time=True):
                    qec(f"CALL activities.segment_matching_match_activities({int(ss.get('sm_seg_id'))});")
                with st.spinner('Generating Pairs', show_time=True):
                    qec(f"CALL activities.segment_matching_pair_generation({int(ss.get('sm_seg_id'))});")
                with st.spinner('Polygon Matching', show_time=True):
                    qec(f"CALL activities.segment_matches_all_polygon();")
                with st.spinner('Auto-Approving', show_time=True):
                    qec(f"CALL activities.segment_matching_mass_confirmation(1);")
                ss.sm_matches_attempted = True
                ss.p_df = pd.read_sql(potential_match_sql, con=get_conn(alchemy=True))

                st.rerun()
            if ss.get('sm_matches_attempted') is True:
                st.warning(f'No matches seem to exist for {ss.sm_seg_id}')
                # return

        tsm_cols = ['confidence', 'effort_date', 'dist_deviation', 'polygon_deviation_m',
                    'hausdorff_deviation_m',
                    'freschet_deviation_m'
                    ]
        max_dist = ss.p_df['dist_deviation'].max()
        if math.isnan(max_dist) or max_dist == 0:
            max_dist = 1
        max_poly = ss.p_df['polygon_deviation_m'].max()
        if math.isnan(max_poly) or max_poly == 0:
            max_poly = 1
        max_haus = ss.p_df['hausdorff_deviation_m'].max()
        if math.isnan(max_haus) or max_haus == 0:
            max_haus = 1
        max_fresh = ss.p_df['freschet_deviation_m'].max()
        if math.isnan(max_fresh) or max_fresh == 0:
            max_fresh = 1

        tsm_config = {'confidence': st.column_config.NumberColumn('Confidence',
                                                                  format='plain'),
                      'effort_date': st.column_config.DateColumn('From',
                                                                 format='yyyy-MMM-DD'),
                      'dist_deviation': st.column_config.ProgressColumn('Dist',
                                                                        min_value=0,
                                                                        max_value=max_dist,
                                                                        format='percent'),
                      'polygon_deviation_m': st.column_config.ProgressColumn('Poly',
                                                               min_value=0,
                                                               max_value=max_poly,
                                                               format='plain'),
                    'hausdorff_deviation_m': st.column_config.ProgressColumn('Hausdorff',
                                                               min_value=0,
                                                               max_value=max_haus,
                                                               format='plain'),
                    'freschet_deviation_m': st.column_config.ProgressColumn('Freschett',
                                                               min_value=0,
                                                               max_value=max_fresh,
                                                               format='plain')}
        st.dataframe(ss.p_df,
                     hide_index=True,
                     on_select='rerun',
                     selection_mode='single-row',
                     column_order=tsm_cols,
                     column_config=tsm_config,
                     key='sm_comp_dict')
        if ss.get('sm_comp_dict'):
            selected_idx_row = ss.get('sm_comp_dict').get('selection').get('rows')
            if selected_idx_row:
                ss.sm_comp_id = ss.p_df.iloc[selected_idx_row[0]]['activity_id']
                ss.sm_comp_start = ss.p_df.iloc[selected_idx_row[0]]['best_start_dist']
                ss.sm_comp_end = ss.p_df.iloc[selected_idx_row[0]]['best_end_dist']
                ss.sm_comp_confidence = ss.p_df.iloc[selected_idx_row[0]]['confidence']
                ss.sm_comp_df = get_activity_df(ss.sm_comp_id, ss.sm_comp_start, ss.sm_comp_end)

        if st.button(':material/check_circle: Confirm Match'):
            confirm_sql = f"""CALL activities.segment_matching_finalize_match(True,
                                                                       {int(ss.sm_comp_id)},
                                                                       {int(ss.sm_seg_id)},
                                                                       {int(ss.sm_comp_start)},
                                                                       {int(ss.sm_comp_end)},
                                                                       {ss.sm_comp_confidence}::NUMERIC)"""
            qec(confirm_sql)
            ss_pop(['sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', 'p_df', 'sm_seg_df', 'sm_comp_df'])
            st.toast('Match Confirmed', duration=5)
            st.rerun()
        if st.button(':red[:material/cancel: Reject Match]'):
            confirm_sql = f"""CALL activities.segment_matching_finalize_match(False,
                                                                                   {int(ss.sm_comp_id)},
                                                                                   {int(ss.sm_seg_id)},
                                                                                   {int(ss.sm_comp_start)},
                                                                                   {int(ss.sm_comp_end)},
                                                                                   {ss.sm_comp_confidence}::NUMERIC)"""
            qec(confirm_sql)
            ss_pop(['sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', 'p_df', 'sm_seg_df', 'sm_comp_df'])
            st.toast('Match Rejected', duration=5)
            st.rerun()
        if st.button(':green[:material/check_circle: Confirm All Remaining]'):
            with st.spinner('Running Mass Approvals', show_time=True):
                qec(f"CALL activities.segment_matching_mass_confirmation(4);")
            ss_pop(['sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', ])
            ss_pop(['p_df', 'sm_seg_df', 'sm_comp_df'])
            st.rerun()

        if st.button(':material/timelapse: Run Hausdorff Scoring'):
            with st.spinner('Running Hausdorff Scoring', show_time=True):
                qec(f"CALL activities.segment_matches_all_hausdorff();")
            ss_pop(['sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', 'p_df', 'sm_seg_df', 'sm_comp_df'])
            st.rerun()

        if st.button(':material/slow_motion_video: Run Freschett Scoring'):
            with st.spinner('Running Hausdorff Scoring', show_time=True):
                qec(f"CALL activities.segment_matches_all_freschet();")
            ss_pop(['sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', 'p_df', 'sm_seg_df', 'sm_comp_df'])
            st.rerun()

        # -------FINAL clearance button------------
        if st.button(':material/restart_alt: Change Segment'):
            pop_sm_vars()

        if st.button(':red[:material/delete: Delete Segment]'):
            qec(f"DELETE FROM activities.segment_matches where segment_id = {ss.sm_seg_id};")
            qec(f"DELETE FROM activities.segments where segment_id = {ss.sm_seg_id};")
            pop_sm_vars()



    with map_col:
        hd_msg = f"__{ss.get('sm_seg_name')}__ | __{ss.get('sm_seg_dist')}__ mi :material/arrow_range: | __{int(ss.get('sm_seg_elev'))}__ m :material/altitude:    :gray[*id# {ss.sm_seg_id}*]"
        st.write(hd_msg)
        st.plotly_chart(plot_route_map(ss.sm_seg_df, ss.sm_comp_df), height=h, width=w, key='sm_seg_route_map')
        st.plotly_chart(plot_elevation_profile(ss.sm_seg_df, ss.sm_comp_df), height=h, width=w, key='sm_elev_plot')

    return

def pop_sm_vars():
    ss_pop(['sm_matches_attempted', 'p_df', 'sm_comp_id', 'sm_comp_start', 'sm_comp_end', 'sm_comp_df', 'sm_seg_df',
            'sm_seg_act_id', 'sm_seg_start', 'sm_seg_end', 'sm_seg_name', 'sm_seg_dist', 'sm_seg_elev', 'sm_seg_id',
            'sm_matches_attempted'])
    st.rerun()


def get_segment_id():
    sql = """SELECT segment_id, segment_name, activity_reference_id, reference_start_point, reference_end_point,
           last_event_utc, matched_activities, distance_mi, elevation_gain FROM activities.vw_course_review"""

    name_col, dist_col,  act_type_col, type_col = st.columns(spec=[3,1,4,2])

    with name_col:
        st.text_input('Course/Segment Name',
                      key='sm_seg_name_key',
                      value=None)

    with dist_col:
        st.number_input('Distance (mi)',
                        min_value=0,
                        max_value=30,
                        step=1,
                        key='sm_dist_min')

    with type_col:
        st.segmented_control('Type',
                             options=['Course', 'Segment'],
                             key='sm_seg_type')

    with act_type_col:
            st.segmented_control(label='Select Activity Type',
                                 default=None,
                                 key='sm_act_type',
                                 options=['Run', 'Trail Run', 'Hike', 'Walk', 'Bike', 'Ski'],
                                 on_change=ss_pop,
                                 args=('cc_act_df',))

    if ss.get('sm_act_type') == 'Run':
        filter_sql = f"""WHERE activity_type_name like '%%run%%' and activity_type_name not like '%%trail%%'"""
    elif ss.get('sm_act_type') == 'Trail Run':
        filter_sql = f"""WHERE activity_type_name like '%%trail%%run%%'"""
    elif ss.get('sm_act_type') == 'Hike':
        filter_sql = f"""WHERE activity_type_name like '%%hik%%'"""
    elif ss.get('sm_act_type') == 'Walk':
        filter_sql = f"""WHERE activity_type_name like '%%walk%%'"""
    elif ss.get('sm_act_type') == 'Bike':
        filter_sql = f"""WHERE activity_type_name like '%%bik%%'"""
    elif ss.get('sm_act_type') == 'Ski':
        filter_sql = f"""WHERE activity_type_name like '%%ski%%'"""
    else:
        filter_sql = f"""WHERE 1=1 """

    if ss.get('sm_seg_name_key'):
        filter_sql = f"""{filter_sql} AND lower(activity_type_name) like LOWER('{ss.get('sm_seg_name_key')}') """


    if ss.get('sm_seg_type'):
        filter_sql = f"""{filter_sql} AND '{ss.get('sm_seg_type')}' = effort_label """


    if ss.get('sm_dist_min'):
        filter_sql = f"""{filter_sql} AND distance_mi >= {int(ss.get('sm_dist_min'))} """

    final_sql = f"{sql} {filter_sql}"

    df = pd.read_sql(final_sql, con=get_conn(alchemy=True))
    cols = ['segment_name', 'last_event_utc', 'matched_activities', 'distance_mi', 'elevation_gain']
    col_config = {'segment_name': st.column_config.TextColumn('Name'),
                  'last_event_utc': st.column_config.DateColumn('Last Effort', format='yyyy-MMM-DD'),
                  'matched_activities': st.column_config.ProgressColumn('# Matched',
                                                                        min_value=0,
                                                                        max_value=int(df['matched_activities'].max()),
                                                                        format='%d'),
                  'distance_mi': st.column_config.ProgressColumn('Miles',
                                                                        min_value=0,
                                                                        max_value=df['distance_mi'].max(),
                                                                        format='plain'),
                  'elevation_gain': st.column_config.ProgressColumn('Elevation',
                                                                        min_value=0,
                                                                        max_value=int(df['elevation_gain'].max())+1,
                                                                        format='%d')
                  }

    st.dataframe(df,
                 hide_index=True,
                 on_select='rerun',
                 selection_mode='single-row',
                 column_order=cols,
                 column_config=col_config,
                 key='sm_seg_select_dict')

    if ss.get('sm_seg_select_dict'):
        selected_idx_row = ss.get('sm_seg_select_dict').get('selection').get('rows')
        if selected_idx_row:
            ss.sm_seg_act_id = df.iloc[selected_idx_row[0]]['activity_reference_id']
            ss.sm_seg_start = df.iloc[selected_idx_row[0]]['reference_start_point']
            ss.sm_seg_end = df.iloc[selected_idx_row[0]]['reference_end_point']
            ss.sm_seg_name = df.iloc[selected_idx_row[0]]['segment_name']
            ss.sm_seg_dist = df.iloc[selected_idx_row[0]]['distance_mi']
            ss.sm_seg_elev = df.iloc[selected_idx_row[0]]['elevation_gain']
            ss.sm_seg_id = df.iloc[selected_idx_row[0]]['segment_id']
            st.rerun()

    return

def render_segment_leaderboard():
    st.divider()
    init_map_basics()

    if not sse('sm_seg_act_id'):
        get_segment_id()
        return

    if not sse('seg_df'):
        ss.sm_seg_df = get_activity_df(ss.get('sm_seg_act_id'), start_dist=ss.get('sm_seg_start'),
                                       end_dist=ss.get('sm_seg_end'))
        ss.sm_comp_df = None
        leaderboard_update(segment_id=ss.get('sm_seg_id'))
        leader_sql = f"""SELECT * FROM activities.vw_segment_leaderboard WHERE segment_id = {ss.get('sm_seg_id')}"""
        ss.sm_leaderboard_df = pd.read_sql(leader_sql, con=get_conn(alchemy=True))
    hd_msg = f"__{ss.get('sm_seg_name')}__ | __{ss.get('sm_seg_dist')}__ mi :material/arrow_range: | __{int(ss.get('sm_seg_elev'))}__ m :material/altitude:    :gray[*id# {ss.sm_seg_id}*]"
    st.write(hd_msg)

    map_col, board_col = st.columns(spec=[3,3], gap="small", border=True)
    w = int(700)
    h = int(900 * (w/1600))
    with board_col:
        render_leaderboard(ss.sm_leaderboard_df)

    with map_col:
        st.plotly_chart(plot_route_map(ss.sm_seg_df, ss.sm_comp_df), height=h, width=w, key='sm_seg_route_map')
    if st.button(':material/restart_alt: Change Segment'):
        ss_pop(['sm_seg_act_id', 'sm_seg_id', 'sm_seg_start', 'sm_seg_end', 'sm_leaderboard_df'])
        st.rerun()

    return

def running_display(activity_id=22115130889):
    df=get_activity_df(activity_id)
    map_col, info_col = st.columns(spec=[3,3], gap="small", border=True)
    w = int(700)
    h = int(900 * (w/1600))
    with info_col:
        st.info('info here')

    with map_col:
        st.plotly_chart(plot_route_map(df, None), height=h, width=w, key='sm_seg_route_map', config=None)






