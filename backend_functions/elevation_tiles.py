import os
import subprocess
from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path
import streamlit as st


def reconcile_elevation_tiles():
    """
    Scans the local directory for .tif files and ingests them if 
    they haven't been processed yet.
    """
    tile_dir = elevation_tile_path()
    db_name = "personal_fitness"

    # 1. Scan contents of the elevation path
    files = [f for f in os.listdir(tile_dir) if f.lower().endswith('.tif')]

    if not files:
        st.info("No .tif files found in the elevation directory.")
        return

    for filename in files:
        file_path = os.path.join(tile_dir, filename)

        # 2. Check if already ingested
        check_sql = f"SELECT 1 FROM activities.elevation_tiles_metadata WHERE filename = '{filename}'"
        exists = sql_to_dict(check_sql)

        if exists:
            # st.write(f"Skipping {filename}: Already ingested.")
            continue

        st.info(f"Processing new file: {filename}")

        # 3. Ingest the raster data
        # -a: Append to table
        # -F: Add a column 'filename' to the raster table (Crucial for BBOX calculation)
        # -I: Create spatial index
        # -C: Apply raster constraints
        # -t 100x100: Tile size for performance
        # -s 4269: NAD83 SRID
        cmd = (
            f'raster2pgsql -a -F -I -C -M -t 100x100 -s 4269 "{file_path}" activities.elevation_rasters | '
            f'psql -d {db_name} -q'
        )

        try:
            subprocess.run(cmd, shell=True, check=True)

            # 4. Record metadata and calculate Bounding Box
            # We calculate the BBOX by looking at the envelope of all tiles 
            # associated with this filename in the raster table.
            metadata_sql = f"""
                INSERT INTO activities.elevation_tiles_metadata (filename, bbox)
                SELECT 
                    '{filename}', 
                    ST_SetSRID(ST_Extent(rast::geometry), 4269)
                FROM activities.elevation_rasters
                WHERE filename = '{filename}';
            """
            qec(metadata_sql)
            st.success(f"Successfully ingested {filename}")

        except subprocess.CalledProcessError as e:
            st.error(f"Failed to ingest {filename}: {e}")
        except Exception as e:
            st.error(f"Error updating metadata for {filename}: {e}")

    return

