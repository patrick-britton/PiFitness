import math

from backend_functions.database_functions import sql_to_dict, qec
from shapely import wkb
import plotly.graph_objects as go
import streamlit as st
from streamlit import session_state as ss

def render_course_list():
    sql = """SELECT * FROM activities.vw_runs where course_id is not null and single_course=1
            ORDER BY course_name like '%Course Created%' DESC;"""
    rd = sql_to_dict(sql)

    max_runs = len(rd)
    offset = st.number_input("Offset", value=0, min_value=0, max_value=len(rd), step=10)
    ctr = offset
    while ctr < min(max_runs, offset+10):
        r = rd[ctr]

        course_name = st.text_input(f"Course # {r.get('course_id')}",
                                    value=r.get('course_name'),
                                    key=f'new_course_name_{r.get('activity_id')}',
                                    on_change=update_course_name,
                                    args=(r.get('course_id'),f'new_course_name_{r.get('activity_id')}'))

        st.write(f":blue[Last Run]: {r.get('start_date')}")
        st.write(f":blue[Dist/Time]: {r.get('distance_mi')} | {r.get('duration_mmss')}")

        rhex = r.get('trajectory')
        trajectory = wkb.loads(bytes.fromhex(rhex))
        bounds = trajectory.bounds
        center_lon = (bounds[0] + bounds[2]) / 2
        center_lat = (bounds[1] + bounds[3]) / 2
        zoom = get_auto_zoom(trajectory, 3)


        lons, lats = zip(*[(lon, lat) for lon, lat, *_ in trajectory.coords])


        # Create plotly figure
        fig = go.Figure(go.Scattermapbox(
            mode='lines',
            lon=lons,
            lat=lats,
            line=dict(width=3, color='blue')
        ))

        fig.update_layout(
            mapbox=dict(
                style='carto-positron',
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            height=150,
            width=150
        )

        st.plotly_chart(fig, key=f"pc_{r.get('activity_id')}",
                        config={
                            'displayModeBar': False,  # Hide toolbar on mobile
                            'staticPlot': True,  # Keep interactive
                            'responsive': True,
                            'height': 150,
                            'width': 150
                        },
                        use_container_width=False
                        )
        ctr += 1
        st.divider()

    return


def get_auto_zoom(geometry, padding=0.1):
    """Get optimal zoom from shapely geometry"""
    bounds = geometry.bounds  # (minx, miny, maxx, maxy)

    lon_span = (bounds[2] - bounds[0]) * (1 + padding)
    lat_span = (bounds[3] - bounds[1]) * (1 + padding)

    # Handle edge case of single point or very small area
    if lon_span < 0.0001:
        lon_span = 0.001
    if lat_span < 0.0001:
        lat_span = 0.001

    zoom = min(
        math.log2(360 / lon_span),
        math.log2(180 / lat_span)
    )

    return zoom


def update_course_name(id, key_val):
    new_course_name = ss.get(key_val)
    if not new_course_name:
        return

    sql = "UPDATE activities.courses SET course_name=%s WHERE course_id=%s"
    params = [new_course_name, id]
    qec(sql, params)
    return