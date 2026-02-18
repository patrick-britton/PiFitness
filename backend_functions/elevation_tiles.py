import os
import subprocess

from dotenv import load_dotenv

from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st

# Load your .env file
load_dotenv()

# Map the environment variables from your specific .env keys
PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")


def reconcile_elevation_tiles():
    """
    Scans elevation_tile_path(), checks against metadata, and ingests new .tif files.
    """
    tile_dir = elevation_tile_path()

    # 1. Identify .tif files in the directory
    files = [f for f in os.listdir(tile_dir) if f.lower().endswith('.tif')]
    if not files:
        st.info("No .tif files found in the elevation directory.")
        return


    # 3. Setup the environment for the subprocess (Authentication)
    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PASS

    progress_text = st.empty()
    bar = st.progress(0)

    for i, filename in enumerate(files):
        file_path = os.path.join(tile_dir, filename)

        # 4. Check if already ingested in metadata
        check_sql = f"SELECT 1 FROM activities.elevation_tiles_metadata WHERE filename = '{filename}'"
        if sql_to_dict(check_sql):
            continue

        progress_text.text(f"Ingesting {i + 1}/{len(files)}: {filename}")

        # Use Create (-c) for the first file if table doesn't exist, otherwise Append (-a)
        mode = "-a"

        # Command explanation:
        # -F: Adds a 'filename' column to the raster table (critical for BBOX calculation)
        # -I: Creates the spatial index automatically
        # -M: Vacuums and analyzes the table for performance
        # -t 100x100: Breaks the massive TIF into smaller internal chunks for faster querying
        cmd = (
            f'raster2pgsql {mode} -F -I -M -t 100x100 -s 4269 "{file_path}" activities.elevation_rasters | '
            f'psql -h {PG_HOST} -p {PG_PORT} -U {PG_USER} -d {PG_DB} -q'
        )

        try:
            # Run the ingestion
            subprocess.run(cmd, shell=True, check=True, env=env, capture_output=True, text=True)

            # 5. Record metadata and calculate the Bounding Box from the ingested data
            metadata_sql = f"""
                INSERT INTO activities.elevation_tiles_metadata (filename, bbox)
                SELECT 
                    '{filename}', 
                    ST_SetSRID(ST_Extent(rast::geometry), 4269)
                FROM activities.elevation_rasters 
                WHERE filename = '{filename}'
                ON CONFLICT (filename) DO NOTHING;
            """
            returns = qec(metadata_sql)
            if returns:
                st.warning(returns)


        except subprocess.CalledProcessError as e:
            st.error(f"Failed to ingest {filename}")
            st.code(e.stderr)  # Shows the specific database error
            break

        bar.progress((i + 1) / len(files))

    progress_text.text("Ingestion complete.")
    return