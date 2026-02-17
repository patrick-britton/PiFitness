#!/usr/bin/env python3
"""
Auto-download SRTM elevation tiles for activity locations
"""

import os
import subprocess
import psycopg2
from pathlib import Path
import logging
import zipfile

import requests

from backend_functions.database_functions import get_conn
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def get_missing_tiles(conn):
    """Query for tiles that need to be downloaded"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tile_id, min_lat, max_lat, min_lon, max_lon, point_count
            FROM activities.vw_missing_tiles
        """)
        return cur.fetchall()


def download_tile(tile_id, min_lat, max_lat, min_lon, max_lon):
    """Download high-resolution tile from USGS (3m or 10m)"""
    filename = f"{tile_id}.tif"
    output_file = Path(elevation_tile_path()) / filename

    if output_file.exists():
        st.info(f"Tile {tile_id} already exists")
        return output_file

    st.info(f"Downloading tile {tile_id}...")

    # Try 3m first, fall back to 10m, then 30m
    datasets = [
        'National Elevation Dataset (NED) 1/9 arc-second',  # 3m
        'National Elevation Dataset (NED) 1/3 arc-second',  # 10m
        'National Elevation Dataset (NED) 1 arc-second'  # 30m
    ]

    for dataset in datasets:
        try:
            api_url = "https://tnmaccess.nationalmap.gov/api/v1/products"
            params = {
                'datasets': dataset,
                'bbox': f"{min_lon},{min_lat},{max_lon},{max_lat}",
                'outputFormat': 'JSON'
            }

            response = requests.get(api_url, params=params, timeout=30)
            data = response.json()

            if data.get('items'):
                download_url = data['items'][0]['downloadURL']
                resolution = '3m' if '1/9' in dataset else '10m' if '1/3' in dataset else '30m'
                st.info(f"Found {resolution} data, downloading from USGS...")

                # Download the file (might be zip or tif)
                temp_file = output_file.with_suffix('.download')
                with requests.get(download_url, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    with open(temp_file, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                # Check if it's a zip file
                if zipfile.is_zipfile(temp_file):
                    with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                        tif_files = [f for f in zip_ref.namelist() if f.endswith('.tif')]
                        if tif_files:
                            zip_ref.extract(tif_files[0], elevation_tile_path())
                            extracted = Path(elevation_tile_path()) / tif_files[0]
                            extracted.rename(output_file)
                    temp_file.unlink()
                else:
                    # It's already a .tif, just rename
                    temp_file.rename(output_file)

                st.info(f"Successfully downloaded {tile_id} at {resolution}")
                return output_file

        except Exception as e:
            st.warning(f"Failed to get {dataset}: {e}")
            continue

    st.error(f"No data found for {tile_id}")
    return None


def load_tile_to_postgres(tile_file, tile_id, conn):
    """Load tile into PostgreSQL using raster2pgsql"""
    st.write(f"Loading {tile_id} into PostgreSQL...")

    try:
        # Generate SQL with raster2pgsql
        cmd = [
            'raster2pgsql',
            '-s', '4326',  # SRID
            '-I',  # Create spatial index
            '-t', '100x100',  # Tile size
            '-F',  # Add filename column
            str(tile_file),
            'activities.elevation_tiles'
        ]

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        sql = result.stdout

        # Execute the generated SQL
        with conn.cursor() as cur:
            cur.execute(sql)

            # Update all rows with the tile_id
            cur.execute("""
                UPDATE activities.elevation_tiles 
                SET tile_id = %s 
                WHERE tile_id IS NULL
            """, (tile_id,))

            conn.commit()

        st.write(f"Successfully loaded {tile_id} into database")
        return True

    except subprocess.CalledProcessError as e:
        st.error(f"Failed to load {tile_id}: {e.stderr}")
        conn.rollback()
        return False


def register_tile_metadata(tile_id, min_lat, max_lat, min_lon, max_lon, file_path, conn):
    """Register tile in metadata table"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO activities.elevation_tiles_metadata 
                (tile_id, min_lat, max_lat, min_lon, max_lon, file_path, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'loaded')
            ON CONFLICT (tile_id) 
            DO UPDATE SET 
                download_date = CURRENT_TIMESTAMP,
                file_path = EXCLUDED.file_path,
                is_downloaded = TRUE
        """, (tile_id, min_lat, max_lat, min_lon, max_lon, str(file_path)))
        conn.commit()


def update_activity_elevations(conn):
    """Update activity_details with elevations from tiles"""
    st.write("Updating activity elevations from tiles...")

    with conn.cursor() as cur:
        cur.execute("""
            WITH tile_lookups AS (
                SELECT 
                    ad.activity_id,
                    ad.elapsed_duration_s,
                    ST_Value(et.rast, ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326)) AS elevation
                FROM activities.activity_details ad
                JOIN activities.elevation_tiles et 
                    ON ST_Intersects(et.rast, ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326))
                WHERE ad.elevation_tiles IS NULL
            )
            UPDATE activities.activity_details ad
            SET elevation_tiles = tl.elevation
            FROM tile_lookups tl
            WHERE ad.activity_id = tl.activity_id
              AND ad.elapsed_duration_s = tl.elapsed_duration_s
        """)

        rows_updated = cur.rowcount
        conn.commit()

    st.info(f"Updated {rows_updated} activity points with tile elevations")


def reconcile_elevation_tiles():
    """Main execution flow"""
    st.info("Starting elevation tile download process...")

    # Connect to database
    conn = get_conn()

    try:
        # Get list of missing tiles
        missing_tiles = get_missing_tiles(conn)

        if not missing_tiles:
            st.info("No missing tiles found!")
            return

        st.info(f"Found {len(missing_tiles)} tiles to download")

        # Download and load each tile
        for tile_id, min_lat, max_lat, min_lon, max_lon, point_count in missing_tiles:
            st.info(f"Processing {tile_id} (covers {point_count} points)")

            # Download
            tile_file = download_tile(tile_id, min_lat, max_lat, min_lon, max_lon)
            # if not tile_file:
            #     st.info(f"Skipping {tile_id} due to download failure")
            #     continue
            #
            # # Load into PostgreSQL
            # if load_tile_to_postgres(tile_file, tile_id, conn):
            #     # Register in metadata
            #     register_tile_metadata(
            #         tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, conn
            #     )
            # else:
            #     st.info(f"Failed to load {tile_id} into database")

        # Update all activities with new elevation data
        # update_activity_elevations(conn)

        st.info("Elevation tile process completed successfully!")

    except Exception as e:
        st.error(f"Error during execution: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
    return


