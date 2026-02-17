#!/usr/bin/env python3
"""
Auto-download SRTM elevation tiles for activity locations
"""

import subprocess
from pathlib import Path
import zipfile
import requests
from backend_functions.database_functions import get_conn
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def get_tiles_needing_download(conn):
    """
    Query for tiles that need to be downloaded or upgraded.
    Returns list of (tile_id, min_lat, max_lat, min_lon, max_lon, point_count, current_resolution)
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                tile_id, 
                min_lat, 
                max_lat, 
                min_lon, 
                max_lon, 
                point_count,
                current_resolution
            FROM activities.vw_missing_tiles
        """)
        return cur.fetchall()


def find_best_available_tile(tile_id):
    """
    Check which resolution we already have downloaded locally.
    Returns (file_path, resolution) or (None, None) if none exist.
    """
    for resolution in ['3m', '10m', '30m']:
        file_path = Path(elevation_tile_path()) / f"{tile_id}_{resolution}.tif"
        if file_path.exists():
            return file_path, resolution
    return None, None


def download_tile(tile_id, min_lat, max_lat, min_lon, max_lon, current_resolution=None):
    """
    Download highest resolution tile available from USGS.
    If current_resolution is provided, only tries to upgrade to better resolution.
    Returns (file_path, resolution) or (None, None) if download failed.
    """
    # Define resolution hierarchy
    resolution_priority = {
        '3m': ('National Elevation Dataset (NED) 1/9 arc-second', 1),
        '10m': ('National Elevation Dataset (NED) 1/3 arc-second', 2),
        '30m': ('National Elevation Dataset (NED) 1 arc-second', 3)
    }

    # Determine which resolutions to try
    if current_resolution and current_resolution in resolution_priority:
        current_priority = resolution_priority[current_resolution][1]
        resolutions_to_try = [
            (res, data[0]) for res, data in resolution_priority.items()
            if data[1] < current_priority
        ]
        if resolutions_to_try:
            st.info(f"Attempting to upgrade {tile_id} from {current_resolution} to higher resolution")
        else:
            st.info(f"{tile_id} already at best available resolution ({current_resolution})")
            existing_file = Path(elevation_tile_path()) / f"{tile_id}_{current_resolution}.tif"
            return existing_file, current_resolution
    else:
        # Try all resolutions in priority order
        resolutions_to_try = [
            ('3m', resolution_priority['3m'][0]),
            ('10m', resolution_priority['10m'][0]),
            ('30m', resolution_priority['30m'][0])
        ]

    # Try each resolution
    for resolution, dataset_name in resolutions_to_try:
        output_file = Path(elevation_tile_path()) / f"{tile_id}_{resolution}.tif"

        # Check if we already have this resolution
        if output_file.exists():
            file_size = output_file.stat().st_size
            st.success(f"✓ {tile_id} at {resolution} already exists ({file_size / 1024 / 1024:.1f} MB)")
            return output_file, resolution

        st.info(f"Checking USGS for {resolution} data...")

        try:
            # Query USGS API
            api_url = "https://tnmaccess.nationalmap.gov/api/v1/products"
            params = {
                'datasets': dataset_name,
                'bbox': f"{min_lon},{min_lat},{max_lon},{max_lat}",
                'outputFormat': 'JSON'
            }

            response = requests.get(api_url, params=params, timeout=30)
            data = response.json()

            if not data.get('items'):
                st.warning(f"No {resolution} data available from USGS")
                continue

            # Found data - download it
            download_url = data['items'][0]['downloadURL']
            st.info(f"Downloading {resolution} data ({download_url.split('/')[-1][:30]}...)")

            temp_file = Path(elevation_tile_path()) / f"{tile_id}_{resolution}.download"

            with requests.get(download_url, stream=True, timeout=300) as r:
                r.raise_for_status()
                total_size = 0
                with open(temp_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        total_size += len(chunk)

            st.info(f"Downloaded {total_size / 1024 / 1024:.1f} MB, processing...")

            # Extract if zip, otherwise just rename
            if zipfile.is_zipfile(temp_file):
                with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                    elevation_files = [
                        f for f in zip_ref.namelist()
                        if f.lower().endswith(('.tif', '.tiff', '.img'))
                    ]

                    if not elevation_files:
                        st.error(f"No elevation files found in zip")
                        temp_file.unlink()
                        continue

                    zip_ref.extract(elevation_files[0], elevation_tile_path())
                    extracted_path = Path(elevation_tile_path()) / elevation_files[0]
                    extracted_path.rename(output_file)

                temp_file.unlink()
            else:
                temp_file.rename(output_file)

            if output_file.exists():
                file_size = output_file.stat().st_size
                st.success(f"✓ {tile_id} at {resolution} ({file_size / 1024 / 1024:.1f} MB)")
                return output_file, resolution

        except Exception as e:
            st.warning(f"Failed to get {resolution}: {e}")
            continue

    st.error(f"Could not download {tile_id} at any resolution")
    return None, None


def load_tile_to_postgres(tile_file, tile_id, resolution, conn):
    """Load tile into PostgreSQL using raster2pgsql"""
    st.info(f"Loading {tile_id} ({resolution}) into PostgreSQL...")

    try:
        # Check if this tile_id already exists in the database
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM activities.elevation_tiles WHERE tile_id = %s
            """, (tile_id,))
            tile_exists = cur.fetchone()[0] > 0

        if tile_exists:
            st.info(f"Tile {tile_id} exists in database, removing old data...")
            with conn.cursor() as cur:
                cur.execute("DELETE FROM activities.elevation_tiles WHERE tile_id = %s", (tile_id,))
                conn.commit()

        # Check if table structure exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'activities' 
                    AND table_name = 'elevation_tiles'
                )
            """)
            table_exists = cur.fetchone()[0]

        # Generate SQL with raster2pgsql
        if table_exists:
            cmd = [
                'raster2pgsql',
                '-s', '4326',
                '-a',  # Append
                '-t', '100x100',
                '-F',
                str(tile_file),
                'activities.elevation_tiles'
            ]
        else:
            cmd = [
                'raster2pgsql',
                '-s', '4326',
                '-I',  # Create index
                '-C',  # Add constraints
                '-t', '100x100',
                '-F',
                str(tile_file),
                'activities.elevation_tiles'
            ]

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        sql = result.stdout

        with conn.cursor() as cur:
            cur.execute(sql)

            # Set tile_id for newly inserted rows
            cur.execute("""
                UPDATE activities.elevation_tiles 
                SET tile_id = %s 
                WHERE tile_id IS NULL
            """, (tile_id,))

            conn.commit()

        st.success(f"✓ Loaded {tile_id} into database")
        return True

    except subprocess.CalledProcessError as e:
        st.error(f"raster2pgsql error: {e.stderr}")
        conn.rollback()
        return False
    except Exception as e:
        st.error(f"Database error: {e}")
        conn.rollback()
        return False


def register_tile_metadata(tile_id, min_lat, max_lat, min_lon, max_lon, file_path, resolution, conn):
    """Register or update tile metadata"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO activities.elevation_tiles_metadata 
                (tile_id, min_lat, max_lat, min_lon, max_lon, file_path, resolution, is_downloaded)
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (tile_id) 
            DO UPDATE SET 
                download_date = CURRENT_TIMESTAMP,
                file_path = EXCLUDED.file_path,
                resolution = EXCLUDED.resolution,
                is_downloaded = TRUE
        """, (tile_id, min_lat, max_lat, min_lon, max_lon, str(file_path), resolution))
        conn.commit()
    return

def update_activity_elevations_from_tile(tile_id, conn):
    """Update activity_details with elevations from a specific tile"""
    st.info(f"Updating activity elevations from {tile_id}...")

    with conn.cursor() as cur:
        cur.execute("""
            WITH tile_lookups AS (
                SELECT 
                    ad.activity_id,
                    ad.elapsed_duration_s,
                    ST_Value(et.rast, ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326)) AS elevation
                FROM activities.activity_details ad
                JOIN activities.elevation_tiles et 
                    ON et.tile_id = %s
                    AND ST_Intersects(et.rast, ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326))
                WHERE ad.elevation_tiles IS NULL
            )
            UPDATE activities.activity_details ad
            SET elevation_tiles = tl.elevation
            FROM tile_lookups tl
            WHERE ad.activity_id = tl.activity_id
              AND ad.elapsed_duration_s = tl.elapsed_duration_s
        """, (tile_id,))

        rows_updated = cur.rowcount
        conn.commit()

    st.success(f"✓ Updated {rows_updated} activity points from {tile_id}")
    return


def reconcile_elevation_tiles():
    """Main workflow: download tiles and update database"""
    st.info("Starting elevation tile reconciliation...")

    conn = get_conn()

    try:
        # Step 1: Identify tiles that need downloading
        tiles_needed = get_tiles_needing_download(conn)

        if not tiles_needed:
            st.success("✓ All tiles are up to date!")
            return

        st.info(f"Found {len(tiles_needed)} tiles to process")

        # Step 2: Download each tile
        downloaded_tiles = []

        for tile_id, min_lat, max_lat, min_lon, max_lon, point_count, current_resolution in tiles_needed:
            st.write(f"--- Processing {tile_id} (covers {point_count} points) ---")

            # Check what we have locally first
            existing_file, existing_resolution = find_best_available_tile(tile_id)

            if existing_file:
                st.info(f"Found local file: {existing_file.name}")
                tile_file, resolution = existing_file, existing_resolution
            else:
                # Download the tile
                tile_file, resolution = download_tile(tile_id, min_lat, max_lat, min_lon, max_lon, current_resolution)

            if not tile_file:
                st.warning(f"Skipping {tile_id} - download failed")
                continue

            downloaded_tiles.append((tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, resolution))

        # Step 3: Load tiles into PostgreSQL
        st.write("--- Loading tiles into PostgreSQL ---")
        loaded_tiles = []

        for tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, resolution in downloaded_tiles:
            if load_tile_to_postgres(tile_file, tile_id, resolution, conn):
                register_tile_metadata(tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, resolution, conn)
                loaded_tiles.append(tile_id)
            else:
                st.warning(f"Failed to load {tile_id}")

        # Step 4: Update activity elevations
        st.write("--- Updating activity elevations ---")
        for tile_id in loaded_tiles:
            update_activity_elevations_from_tile(tile_id, conn)

        st.success(f"✓ Reconciliation complete! Processed {len(loaded_tiles)} tiles")

    except Exception as e:
        st.error(f"Error during reconciliation: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
    return