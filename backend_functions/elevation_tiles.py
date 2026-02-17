#!/usr/bin/env python3
"""
Auto-download SRTM elevation tiles for activity locations
"""

import os
import subprocess
import psycopg2
from pathlib import Path
import logging

from backend_functions.database_functions import get_conn
from backend_functions.file_handlers import elevation_tile_path


def get_missing_tiles(conn):
    """Query for tiles that need to be downloaded"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tile_id, min_lat, max_lat, min_lon, max_lon, point_count
            FROM activities.vw_missing_tiles
        """)
        return cur.fetchall()


def download_tile(tile_id, min_lat, max_lat, min_lon, max_lon, resolution='3m'):
    """Download a single SRTM tile using elevation library"""

    filename = f"{tile_id}.tif"
    output_file = os.path.join(elevation_tile_path(), filename)

    if os.path.exists(output_file):
        logging.info(f"Tile {tile_id} at {resolution} already exists")
        return output_file

    logging.info(f"Downloading {resolution} tile {tile_id}")

    try:
        if resolution == '10m':
            # USGS 3DEP 1/3 arc-second
            product = 'ned13'
        elif resolution == '3m':
            # USGS 3DEP 1/9 arc-second
            product = 'ned19'
        else:
            # Default to SRTM 30m
            product = 'srtm30m'

        # Use elevation library with specific product
        cmd = [
            'eio', 'clip',
            '-o', str(output_file),
            '--product', product,
            '--bounds', f"{min_lon}", f"{min_lat}", f"{max_lon}", f"{max_lat}"
        ]

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"Successfully downloaded {tile_id} at {resolution}")
        return output_file

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to download {tile_id} at {resolution}: {e.stderr}")

        # Fallback to lower resolution if high-res not available
        if resolution == '3m':
            logging.info(f"Falling back to 10m for {tile_id}")
            return download_tile(tile_id, min_lat, max_lat, min_lon, max_lon, '10m')
        elif resolution == '10m':
            logging.info(f"Falling back to 30m for {tile_id}")
            return download_tile(tile_id, min_lat, max_lat, min_lon, max_lon, '30m')

        return None


def load_tile_to_postgres(tile_file, tile_id, conn):
    """Load tile into PostgreSQL using raster2pgsql"""
    logging.info(f"Loading {tile_id} into PostgreSQL...")

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

        logging.info(f"Successfully loaded {tile_id} into database")
        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to load {tile_id}: {e.stderr}")
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
    logging.info("Updating activity elevations from tiles...")

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

    print(f"Updated {rows_updated} activity points with tile elevations")


def reconcile_elevation_tiles():
    """Main execution flow"""
    print("Starting elevation tile download process...")

    # Connect to database
    conn = get_conn()

    try:
        # Get list of missing tiles
        missing_tiles = get_missing_tiles(conn)

        if not missing_tiles:
            print("No missing tiles found!")
            return

        print(f"Found {len(missing_tiles)} tiles to download")

        # Download and load each tile
        for tile_id, min_lat, max_lat, min_lon, max_lon, point_count in missing_tiles:
            print(f"Processing {tile_id} (covers {point_count} points)")

            # Download
            tile_file = download_tile(tile_id, min_lat, max_lat, min_lon, max_lon)
            if not tile_file:
                print(f"Skipping {tile_id} due to download failure")
                continue

            # Load into PostgreSQL
            if load_tile_to_postgres(tile_file, tile_id, conn):
                # Register in metadata
                register_tile_metadata(
                    tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, conn
                )
            else:
                print(f"Failed to load {tile_id} into database")

        # Update all activities with new elevation data
        update_activity_elevations(conn)

        print("Elevation tile process completed successfully!")

    except Exception as e:
        logging.error(f"Error during execution: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
    return


