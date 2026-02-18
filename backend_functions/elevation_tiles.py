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
                    import_to_postgres(full_path, db_name='personal_fitness')

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


def download_file(url, destination):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(destination, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return True


def get_usgs_by_bbox(bbox):
    base_url = "https://tnmaccess.nationalmap.gov/api/v1/products"

    # We broaden the search to include all standard 3DEP products
    params = {
        'bbox': bbox,
        'datasets': '1 meter,1/3 arc-second,National Elevation Dataset (NED) 1/9 arc-second',
        'prodFormats': 'GeoTIFF,IMG',  # Include IMG for those legacy tiles
        'outputFormat': 'JSON',
        'max': 100
    }

    try:
        response = requests.get(base_url, params=params, timeout=20)
        data = response.json()
        items = data.get('items', [])

        if not items:
            print(f"  No elevation products found for {bbox}.")
            return []

        # We group items by their resolution to pick the best available "Tier"
        # Tier 1: 1-meter (Best)
        # Tier 2: 1/9 arc-second (~3m)
        # Tier 3: 1/3 arc-second (~10m)

        tier_1 = [i.get('downloadURL') for i in items if '1 meter' in i.get('title', '')]
        tier_2 = [i.get('downloadURL') for i in items if '1/9' in i.get('title', '') or '9th' in i.get('title', '')]
        tier_3 = [i.get('downloadURL') for i in items if '1/3' in i.get('title', '') or '3rd' in i.get('title', '')]

        # Return the best tier that actually has data
        if tier_1: return tier_1
        if tier_2: return tier_2
        return tier_3

    except Exception as e:
        print(f"  API Error: {e}")
        return []


def import_to_postgres(file_path, db_name):
    """
    Standardizes the import whether the file is .img or .tif
    """
    # -a: Append, -F: Add filename, -I: Index, -C: Constraints, -M: Analyze
    # -t 100x100: Good middle-ground tile size for Pi 5 RAM
    # -s 4269: Use NAD83 (USGS Standard)

    cmd = (
        f'raster2pgsql -a -F -I -C -M -t 100x100 -s 4269 "{file_path}" activities.elevation_rasters | '
        f'psql -d {db_name} -q'
    )

    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Import failed for {file_path}: {e}")
        return False
