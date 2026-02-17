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
    """
    Download highest resolution tile available from USGS.
    Tries 3m first, then 10m, then 30m.
    Always attempts to upgrade to higher resolution if available.
    """
    filename = f"{tile_id}.tif"
    output_file = Path(elevation_tile_path()) / filename

    # Check what resolution we currently have (if any)
    current_resolution = None
    if output_file.exists():
        # Try to determine current resolution from metadata table
        # For now, we'll just try to upgrade anyway
        st.info(f"Tile {tile_id} exists, checking for higher resolution...")

    st.info(f"Searching for tile {tile_id}...")

    # Try 3m first, fall back to 10m, then 30m
    datasets = [
        ('National Elevation Dataset (NED) 1/9 arc-second', '3m'),
        ('National Elevation Dataset (NED) 1/3 arc-second', '10m'),
        ('National Elevation Dataset (NED) 1 arc-second', '30m')
    ]

    for dataset_name, resolution in datasets:
        try:
            # Query USGS API
            api_url = "https://tnmaccess.nationalmap.gov/api/v1/products"
            params = {
                'datasets': dataset_name,
                'bbox': f"{min_lon},{min_lat},{max_lon},{max_lat}",
                'outputFormat': 'JSON'
            }

            st.info(f"Checking for {resolution} data...")
            response = requests.get(api_url, params=params, timeout=30)
            data = response.json()

            if not data.get('items'):
                st.warning(f"No {resolution} data available, trying next resolution...")
                continue

            # Found data at this resolution
            download_url = data['items'][0]['downloadURL']
            st.info(f"Found {resolution} data! Downloading from USGS...")

            # Use resolution-specific temp file
            temp_file = Path(elevation_tile_path()) / f"{tile_id}_{resolution}.download"

            # Download the file
            with requests.get(download_url, stream=True, timeout=300) as r:
                r.raise_for_status()
                total_size = 0
                with open(temp_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        total_size += len(chunk)

            st.info(f"Downloaded {total_size / 1024 / 1024:.1f} MB")

            # Process the downloaded file
            if zipfile.is_zipfile(temp_file):
                st.info("Extracting zip archive...")

                with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                    all_files = zip_ref.namelist()

                    # Look for elevation files (.tif, .tiff, or .img)
                    elevation_files = [
                        f for f in all_files
                        if f.lower().endswith(('.tif', '.tiff', '.img'))
                    ]

                    if not elevation_files:
                        st.error(f"No elevation files (.tif/.img) found in zip. Contents: {all_files[:5]}")
                        temp_file.unlink()
                        continue

                    # Extract the first elevation file found
                    elevation_file = elevation_files[0]
                    st.info(f"Extracting {elevation_file}...")

                    zip_ref.extract(elevation_file, elevation_tile_path())
                    extracted_path = Path(elevation_tile_path()) / elevation_file

                    # Verify extraction succeeded
                    if not extracted_path.exists():
                        st.error(f"Extraction failed - file not found at {extracted_path}")
                        temp_file.unlink()
                        continue

                    # Move to final location
                    if output_file.exists():
                        output_file.unlink()

                    extracted_path.rename(output_file)
                    st.info(f"Extracted and renamed to {output_file.name}")

                # Clean up zip file (comment out to keep for debugging)
                # temp_file.unlink()

            else:
                # File is already a .tif, just rename it
                st.info("File is already in GeoTIFF format")

                if output_file.exists():
                    output_file.unlink()

                temp_file.rename(output_file)

            # Verify final file exists and has content
            if output_file.exists():
                file_size = output_file.stat().st_size
                st.success(
                    f"✓ Successfully saved {tile_id} at {resolution} resolution ({file_size / 1024 / 1024:.1f} MB)")
                return output_file
            else:
                st.error(f"File verification failed - {output_file} does not exist")
                continue

        except requests.exceptions.RequestException as e:
            st.warning(f"Network error downloading {resolution} data: {e}")
            continue
        except zipfile.BadZipFile as e:
            st.warning(f"Invalid zip file for {resolution} data: {e}")
            # Clean up bad file
            if temp_file.exists():
                temp_file.unlink()
            continue
        except Exception as e:
            st.error(f"Unexpected error with {resolution} data: {e}")
            # Clean up on error
            temp_file = Path(elevation_tile_path()) / f"{tile_id}_{resolution}.download"
            if temp_file.exists():
                temp_file.unlink()
            continue

    # If we get here, all resolutions failed
    st.error(f"Failed to download tile {tile_id} at any resolution (3m/10m/30m)")
    return None


def load_tile_to_postgres(tile_file, tile_id, conn):
    """Load tile into PostgreSQL using raster2pgsql"""
    st.write(f"Loading {tile_id} into PostgreSQL...")

    try:
        # Check if table exists to determine mode
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
            # Append mode - table already exists
            cmd = [
                'raster2pgsql',
                '-s', '4326',  # SRID
                '-a',  # Append to existing table
                '-t', '100x100',  # Tile size
                '-F',  # Add filename column
                str(tile_file),
                'activities.elevation_tiles'
            ]
        else:
            # Create mode - first time loading
            cmd = [
                'raster2pgsql',
                '-s', '4326',  # SRID
                '-I',  # Create spatial index
                '-C',  # Add raster constraints
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
    except Exception as e:
        st.error(f"Database error loading {tile_id}: {e}")
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
            if not tile_file:
                st.info(f"Skipping {tile_id} due to download failure")
                continue

            # Load into PostgreSQL
            if load_tile_to_postgres(tile_file, tile_id, conn):
                # Register in metadata
                register_tile_metadata(
                    tile_id, min_lat, max_lat, min_lon, max_lon, tile_file, conn
                )
            else:
                st.info(f"Failed to load {tile_id} into database")

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


