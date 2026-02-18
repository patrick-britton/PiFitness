import os
import requests
import subprocess
from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def reconcile_elevation_tiles():
    # 1. Get tiles needing download from the updated view
    sql = "SELECT * FROM activities.vw_required_elevation_tiles"
    tile_list = sql_to_dict(sql)

    tile_storage_path = elevation_tile_path()

    for tile in tile_list:
        tile_name = tile.get('tile_name')
        bbox = tile.get('bbox_coords')  # New column from the updated view

        # 2. Search using the Bounding Box instead of the Name
        st.info(f"Searching for data in {tile_name} ({bbox})...")

        # --- THIS IS THE REPLACEMENT LINE ---
        download_urls = get_usgs_by_bbox(bbox)
        # -------------------------------------

        if not download_urls:
            st.info(f"  No products found for {tile_name}. Skipping.")
            continue

        # Handle the list of URLs (USGS often breaks 1m data into multiple chunks per degree)
        for url in download_urls:
            # Extract a unique filename from the USGS URL to avoid collisions
            remote_filename = url.split('/')[-1]
            full_path = os.path.join(tile_storage_path, remote_filename)

            if os.path.exists(full_path):
                st.info(f"  File {remote_filename} exists. Skipping download.")
            else:
                st.info(f"  Downloading: {remote_filename}")
                if download_file(url, full_path):
                    # 3. Import to Postgres
                    # Note: We use -s 4269 (NAD83) as it is the USGS standard for 3DEP
                    cmd = (
                        f"raster2pgsql -a -I -C -M -t 50x50 -s 4269 {full_path} activities.elevation_rasters | "
                        f"psql -d personal_fitness"
                    )
                    subprocess.run(cmd, shell=True, check=True)

        # 4. Mark this 1-degree square as 'imported' in your catalog
        catalog_sql = f"""
            INSERT INTO activities.elevation_file_catalog (tile_name, import_status)
            VALUES ('{tile_name}', 'imported')
            ON CONFLICT (tile_name) DO UPDATE SET import_status = 'imported';
        """
        qec(catalog_sql)

    # 5. Final Step: Run the spatial join update
    qec("CALL activities.process_elevation_backlog()")
    return


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


def get_usgs_by_bbox(bbox):
    base_url = "https://tnmaccess.nationalmap.gov/api/v1/products"

    # We remove 'datasets' to avoid being filtered out by naming changes
    params = {
        'bbox': bbox,
        'prodFormats': 'GeoTIFF',
        'outputFormat': 'JSON',
        'max': 10  # Get a few options to choose from
    }

    try:
        response = requests.get(base_url, params=params, timeout=20)
        data = response.json()
        items = data.get('items', [])

        if not items:
            return []

        scored_items = []
        for item in items:
            title = item.get('title', '').lower()
            url = item.get('downloadURL')
            if not url: continue

            # SCORING LOGIC: Higher is better
            score = 0
            if '1 meter' in title:
                score = 100
            elif '1/9' in title or '9th' in title:
                score = 80
            elif '1/3' in title or '3rd' in title:
                score = 60
            elif 'elevation' in title or 'dem' in title:
                score += 10

            # Skip imagery or other non-elevation products
            if 'imagery' in title or 'topo map' in title: score = -1

            if score > 0:
                scored_items.append((score, url, title))

        # Sort by score descending
        scored_items.sort(key=lambda x: x[0], reverse=True)

        if scored_items:
            print(f"  Found: {scored_items[0][2]}")  # Log what we found
            return [scored_items[0][1]]  # Return the best URL in a list

    except Exception as e:
        print(f"  API Error: {e}")
    return []

