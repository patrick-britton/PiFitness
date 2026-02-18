import os
import requests
import subprocess
from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def reconcile_elevation_tiles():
    sql = "SELECT * FROM activities.vw_required_elevation_tiles"
    tile_list = sql_to_dict(sql)

    tile_storage_path = elevation_tile_path()

    for tile in tile_list:
        tile_name = tile.get('tile_name')

        # 1. Name the destination file
        # We save as .tif for raster2pgsql to consume later
        local_filename = f"usgs_19_{tile_name}.tif"
        full_path = os.path.join(tile_storage_path, local_filename)

        # 2. Check to see if file has already been downloaded
        if os.path.exists(full_path):
            st.info(f"File {local_filename} already exists. Skipping download.")
        else:
            # 3. Search for precise file name via USGS API
            # 1/9 arc-second is approx 3.4 meters
            st.info(f"Searching for {tile_name} via USGS TNM API...")
            download_url = get_usgs_3dep_url(tile_name)

            if not download_url:
                st.info(f"Could not find 1/9 arc-second data for {tile_name}. Skipping.")
                continue

            # 4. download & save the correct file
            st.info(f"Downloading {tile_name} from {download_url}...")
            if download_file(download_url, full_path):
                # 5. Process the raster into Postgres using raster2pgsql
                # -a: Append to table
                # -I: Create index
                # -C: Apply constraints
                # -t 100x100: Tile into 100px chunks (crucial for Pi 5 performance)
                cmd = (
                    f"raster2pgsql -a -I -C -M -t 100x100 {full_path} activities.elevation_rasters | "
                    f"psql -d your_db_name"
                )
                subprocess.run(cmd, shell=True, check=True)

                # Update the catalog so the View stops showing this tile
                catalog_sql = f"""
                    INSERT INTO activities.elevation_file_catalog (tile_name, import_status)
                    VALUES ('{tile_name}', 'imported')
                    ON CONFLICT (tile_name) DO UPDATE SET import_status = 'imported';
                """
                qec(catalog_sql)

    # 6. Run the backlog update to populate elevation_reference in activity_details
    st.info("Running database elevation update...")
    qec("CALL activities.process_elevation_backlog()")


def get_usgs_3dep_url(tile_id):
    """Queries USGS API for 1/9 arc-second (3 meter) GeoTIFFs"""
    base_url = "https://tnmaccess.nationalmap.gov/api/v1/products"
    params = {
        'datasets': 'Standard-3rd arc-second',  # This matches 1/9" (3 meters)
        'q': tile_id,
        'outputFormat': 'JSON'
    }

    try:
        response = requests.get(base_url, params=params)
        data = response.json()

        # Filter items to find the best GeoTIFF download link
        for item in data.get('items', []):
            if 'IMG' in item.get('formats', []) or 'GeoTIFF' in item.get('formats', []):
                return item.get('downloadURL')
    except Exception as e:
        st.info(f"API Error: {e}")
    return None


def download_file(url, destination):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(destination, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return True

