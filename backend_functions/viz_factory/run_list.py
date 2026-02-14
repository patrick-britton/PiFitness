import math
from math import radians, cos, sin, asin, sqrt
import time

from backend_functions.database_functions import sql_to_dict, qec
from shapely import wkb
import plotly.graph_objects as go
import streamlit as st
from streamlit import session_state as ss

from frontend_functions.streamlit_helpers import sse, ss_pop


def reset_idx():
    ss_pop('course_idx')
    return

def render_course_list():

    n_col, min_col, max_col = st.columns(spec=[2,1,1], gap=None, border=False)
    with n_col:
        name_search = st.text_input(label='Name Search',
                                value=None,
                                    on_change=reset_idx)
    with min_col:
        length_min = st.number_input(label='Length Min',
                                 value=0,
                                    on_change=reset_idx)
    with max_col:
        length_max = st.number_input(label='Length Max',
                                 value=999,
                                    on_change=reset_idx)
    st.divider()

    sql = """SELECT * FROM activities.vw_course_review"""
    sql = f"{sql} WHERE distance_mi >= {length_min} AND distance_mi < {length_max}"
    if name_search:
        sql = f"{sql} AND lower(segment_name) LIKE LOWER('%{name_search}%');"

    rd = sql_to_dict(sql)

    total_course_count = len(rd)
    new_course_count = rd[0].get('new_count')

    msg = f"__{total_course_count}__ Total courses"
    if new_course_count > 0:
        msg = f"{msg} :blue[{new_course_count} new courses]"

    st.subheader(msg)

    if not sse('course_idx'):
        ss.course_idx = 0
        ss.r = rd[ss.course_idx]

    prior_col, next_col, fake_col = st.columns(spec=[1,1,3], border=False, gap=None)

    with prior_col:
        if st.button(':material/keyboard_double_arrow_left: Prior'):
            if ss.course_idx == 0:
                ss.course_idx = len(rd)-1
            else:
                ss.course_idx -= 1
            ss.r = rd[ss.course_idx]
            st.rerun()

    with next_col:
        if st.button('Next :material/keyboard_double_arrow_right:'):
            if ss.course_idx == len(rd)-1:
                ss.course_idx = 0
            else:
                ss.course_idx += 1
            ss.r = rd[ss.course_idx]
            st.rerun()

    ## DISPLAY THE COURSE


    st.subheader(f"__{ss.r.get('segment_name')}__")
    st.write(f":blue[Last Run]: {ss.r.get('last_event_utc')}")
    st.write(f":blue[Dist/Attempts]: {ss.r.get('distance_mi')} | {ss.r.get('matched_activities')}")

    rhex = ss.r.get('segment_path')
    trajectory = wkb.loads(bytes.fromhex(rhex))
    bounds = trajectory.bounds
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2

    coords = list(trajectory.coords)
    lons, lats, elevs = zip(*[(c[0], c[1], c[2]) for c in coords])

    if ss.is_mobile:
        # Downsample to every nth point
        step = len(lons) // 150
        lons = lons[::step]
        lats = lats[::step]
        elevs = elevs[::step]
        ui_rev = 'static'
        map_width = 200
        map_height = 200
        zoom = get_auto_zoom(trajectory, 2)
        zoom = max(1, min(zoom, 18))

    else:
        ui_rev = 'default'
        map_width = 800
        map_height = 450
        zoom = get_auto_zoom(trajectory, 0)
        zoom = max(1, min(zoom, 18))


    # Create plotly figure
    fig = go.Figure(go.Scattermapbox(
        mode='lines',
        lon=list(lons),
        lat=list(lats),
        line=dict(width=2, color='blue'),
        hoverinfo='skip'
    ))

    fig.update_traces(
        line=dict(width=2),
        connectgaps=False
    )

    fig.update_layout(
        mapbox=dict(
            style='carto-positron',
            uirevision=ui_rev,
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        autosize=False,
        showlegend=False,
        height=map_height,
        width=map_width
    )

    map_col, elev_col = st.columns(spec=[1, 1], gap=None, border=False)

    with map_col:
        st.plotly_chart(fig, key=f"pc_{ss.r.get('activity_id')}",
                        config={
                            'displayModeBar': False,  # Hide toolbar on mobile
                            'staticPlot': True,  # Keep interactive
                            'responsive': False,
                            'doubleClick': False,
                            'scrollZoom': False,
                            'height': map_height,
                            'width': map_width
                        },
                        width='content'
                        )

    with elev_col:
        # 3. GENERATE ELEVATION PROFILE

        # Calculate X-axis (Distance)
        dist_miles = get_cumulative_distance_miles(lons, lats)

        # Determine min/max for dynamic Y-axis scaling
        min_elev = min(elevs) if elevs else 0
        max_elev = max(elevs) if elevs else 100
        elev_buffer = (max_elev - min_elev) * 0.1

        # Create Area Chart
        elev_fig = go.Figure()

        elev_fig.add_trace(go.Scatter(
            x=dist_miles,
            y=elevs,
            mode='lines',
            fill='tozeroy',  # Creates the "Area" chart effect
            name='Elevation',
            line=dict(color='#1f77b4', width=2),  # Matches your map blue
            fillcolor='rgba(31, 119, 180, 0.3)',  # Semi-transparent blue fill
            hoverinfo='x+y',
            hovertemplate='<b>%{x:.2f} mi</b><br>%{y:.0f} m<extra></extra>'
        ))

        elev_fig.update_layout(
            margin=dict(l=0, r=0, t=20, b=0),  # Tight margins
            height=map_height,  # Match the map height
            xaxis=dict(
                title=None,
                showgrid=False,
                zeroline=False,
                showticklabels=True,
                tickformat=".1f",  # Show distance decimals
                fixedrange=True  # Disable zoom for consistency
            ),
            yaxis=dict(
                title=None,
                showgrid=True,
                gridcolor='rgba(200,200,200,0.2)',  # Subtle grid
                zeroline=False,
                showticklabels=True,
                # Dynamically range the Y-axis so the graph doesn't look flat
                range=[min_elev - elev_buffer, max_elev + elev_buffer],
                fixedrange=True
            ),
            paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False
        )

        st.plotly_chart(elev_fig,
                        key=f"elev_{ss.r.get('activity_id')}",
                        width='content',
                        config={'displayModeBar': False, 'staticPlot': True})

    st.divider()
    new_name = st.text_input(f"Update Name for course # {ss.r.get('segment_id')}",
                  value=None,
                  key=f'new_course_name_btn')

    if new_name:
        if st.button(':material/save: Save Name'):
            update_course_name(ss.r.get('segment_id'), new_name)
            st.toast('Name Saved!', duration=3)
            ss_pop(['r', 'course_idx'])
            st.rerun()

    return


def get_auto_zoom(geometry, padding=0.1):
    """Get optimal zoom from shapely geometry with safety checks"""
    try:
        bounds = geometry.bounds  # (minx, miny, maxx, maxy)

        lon_span = (bounds[2] - bounds[0]) * (1 + padding)
        lat_span = (bounds[3] - bounds[1]) * (1 + padding)

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

        # Clamp to valid range (some map styles break outside this)
        return max(1, min(zoom, 18))

    except Exception as e:
        # Fallback to middle zoom
        return 13


def update_course_name(id, new_name):
    new_course_name = new_name
    if not new_course_name:
        return

    sql = "UPDATE activities.segments SET segment_name=%s WHERE segment_id=%s"
    params = [new_course_name, id]
    qec(sql, params)
    return


def get_cumulative_distance_miles(lons, lats):
    """Calculates cumulative distance in miles from coordinate lists."""
    dists = [0.0]
    total_dist = 0.0

    for i in range(1, len(lons)):
        # Haversine formula for distance between two points
        lon1, lat1, lon2, lat2 = map(radians, [lons[i - 1], lats[i - 1], lons[i], lats[i]])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 3956  # Radius of earth in miles

        step_dist = c * r
        total_dist += step_dist
        dists.append(total_dist)

    return dists