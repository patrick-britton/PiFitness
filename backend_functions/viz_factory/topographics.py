import streamlit as st
import pandas as pd
import pydeck as pdk
import matplotlib.cm as cm
import matplotlib.colors as colors
from sqlalchemy import create_engine

from backend_functions.database_functions import get_conn





# Regional Bounding Boxes (Min Lon, Min Lat, Max Lon, Max Lat)
# REGIONS = {
#     "San Diego (Local)": (-117.3, 32.5, -116.8, 33.1),
#     "North America": (-125.0, 24.0, -66.0, 49.0),
#     "Europe": (-10.0, 35.0, 40.0, 70.0)
# }

def render_topo():

    st.title("3D Elevation Grid Viewer")

    regions = {
        "San Diego": (-117.3, 32.5, -116.8, 33.1),
        "North America": (-125.0, 24.0, -66.0, 49.0),
        "Europe": (-10.0, 35.0, 40.0, 70.0)
    }


    # selected_region = st.selectbox("Select Region", list(regions.keys()))
    selected_region = 'San Diego'

    min_lon, min_lat, max_lon, max_lat = regions[selected_region]


    color_metric = st.selectbox(
        "Color Grids By:",
        ["avg_elevation", "elevation_hammered", "obs_count", "elev_stddev"]
    )

    df = load_data(min_lon, min_lat, max_lon, max_lat)
    if df.empty:
        st.warning("No data found for this region.")
        return

    # Normalize the chosen metric for coloring
    metric_min = df[color_metric].min()
    metric_max = df[color_metric].max()

    # Apply a Matplotlib colormap (Viridis) and convert to PyDeck RGB format [R, G, B]
    norm = colors.Normalize(vmin=metric_min, vmax=metric_max)
    colormap = cm.ScalarMappable(norm=norm, cmap='viridis')
    df['color'] = df[color_metric].apply(
        lambda x: [int(c * 255) for c in colormap.to_rgba(x)[:3]]
    )

    # Define the 3D PyDeck Column Layer
    layer = pdk.Layer(
        "ColumnLayer",
        data=df,
        get_position=["lon", "lat"],
        get_elevation="elevation_hammered",  # Physical 3D height
        elevation_scale=2,  # Exaggerate height for visibility
        radius=2.5,  # 5m grid = 2.5m radius
        get_fill_color="color",  # Dynamic color based on UI selection
        pickable=True,
        auto_highlight=True,
    )

    # Set the initial viewport
    view_state = pdk.ViewState(
        longitude=df['lon'].median(),
        latitude=df['lat'].median(),
        zoom=13,
        pitch=45,  # Tilt the camera for 3D effect
        bearing=0
    )

    # Render the map
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Grid ID:</b> {grid_5m_00_id} <br/>"
                    "<b>Raw Avg:</b> {avg_elevation}m <br/>"
                    "<b>Hammered:</b> {elevation_hammered}m <br/>"
                    "<b>Obs Count:</b> {obs_count} <br/>"
                    "<b>StdDev:</b> {elev_stddev}m",
            "style": {"color": "white"}
        }
    ))
    return



def load_data(min_lon, min_lat, max_lon, max_lat):
    """Loads grid data constrained by the selected bounding box."""

    query = f"""
        SELECT 
            grid_5m_00_id,
            ST_X(grid_5m_00_id) AS lon,
            ST_Y(grid_5m_00_id) AS lat,
            avg_elevation,
            elevation_hammered,
            obs_count,
            elev_stddev
        FROM activities.elevation_reference
        WHERE ST_X(grid_5m_00_id) BETWEEN {min_lon} AND {max_lon}
          AND ST_Y(grid_5m_00_id) BETWEEN {min_lat} AND {max_lat}
        LIMIT 100000; -- Safety limit for browser rendering
    """
    return pd.read_sql(query, con=get_conn(alchemy=True))




