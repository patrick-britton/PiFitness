import os
import subprocess
from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def reconcile_elevation_tiles():
    tile_dir = elevation_tile_path()
    db_name = "personal_fitness"

    files = [f for f in os.listdir(tile_dir) if f.lower().endswith('.tif')]
    if not files:
        st.info("No .tif files found.")
        return

    # Check if the table already exists to decide between Create (-c) or Append (-a)
    table_exists_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'activities' 
            AND table_name = 'elevation_rasters'
        );
    """
    table_exists = sql_to_dict(table_exists_query)[0]['exists']

    for index, filename in enumerate(files):
        file_path = os.path.join(tile_dir, filename)

        # Check metadata to avoid duplicates
        check_sql = f"SELECT 1 FROM activities.elevation_tiles_metadata WHERE filename = '{filename}'"
        if sql_to_dict(check_sql):
            continue

        st.info(f"Processing: {filename}")

        # Use -c (Create) for the very first file if table doesn't exist,
        # otherwise use -a (Append).
        mode = "-a" if table_exists or index > 0 else "-c"

        # REMOVED -C: Strict constraints often fail on USGS tiles due to alignment.
        # ADDED -e: Use individual transactions (helps debugging).
        cmd = (
            f'raster2pgsql {mode} -F -I -M -t 100x100 -s 4269 "{file_path}" activities.elevation_rasters | '
            f'psql -d {db_name} -q'
        )

        try:
            # Capture stderr to see the actual Postgres/Raster error if it fails
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)

            # Calculate and store metadata
            metadata_sql = f"""
                INSERT INTO activities.elevation_tiles_metadata (filename, bbox)
                SELECT '{filename}', ST_SetSRID(ST_Extent(rast::geometry), 4269)
                FROM activities.elevation_rasters WHERE filename = '{filename}'
                ON CONFLICT (filename) DO NOTHING;
            """
            qec(metadata_sql)
            st.success(f"Ingested {filename}")

            # After the first successful file, switch to append mode
            table_exists = True

        except subprocess.CalledProcessError as e:
            st.error(f"Error ingesting {filename}")
            st.code(e.stderr) # This will show the ACTUAL reason (e.g., 'column "filename" does not exist')

    return

