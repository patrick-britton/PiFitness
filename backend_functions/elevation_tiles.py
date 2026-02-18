# Comprehensive Elevation Reference Pipeline
# ------------------------------------------------------------
# Goals addressed:
# 1) Download only REQUIRED USGS elevation tiles at highest available resolution
# 2) Avoid re-downloads and re-imports
# 3) Correctly unzip / normalize rasters
# 4) Persist raster + metadata in PostGIS
# 5) Extract missing elevations for activity lat/lon points
# 6) Support incremental operation for each new activity
# ------------------------------------------------------------

import os
import json
import zipfile
import subprocess
import hashlib
import requests
from pathlib import Path
from typing import List, Dict, Tuple
import streamlit as st

from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.file_handlers import elevation_tile_path

# -----------------------------
# Configuration
# -----------------------------

USGS_API = "https://tnmaccess.nationalmap.gov/api/v1/products"
DATASET_PRIORITY = [
    "1 meter",
    "National Elevation Dataset (NED) 1/9 arc-second",
    "1/3 arc-second",
]
SRID = 4269            # NAD83 (USGS standard)
RASTER_TABLE = "activities.elevation_rasters"
CATALOG_TABLE = "activities.elevation_file_catalog"
TILE_ROOT = Path(elevation_tile_path())
TILE_ROOT.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Public Orchestrator
# -----------------------------
def reconcile_elevation_tiles():
    """
    Public entry point.
    Idempotent.
    Safe to run per activity ingest.
    """

    ingest_missing_elevation_tiles()
    qec("CALL activities.process_elevation_backlog();")
    return


def ingest_missing_elevation_tiles():
    required_tiles = get_required_tiles()

    for tile in required_tiles:
        st.info(f"Processing {tile}")
        process_tile(tile)
    return

# -----------------------------
# Tile Resolution
# -----------------------------

def get_required_tiles() -> List[Dict]:
    """
    Each tile represents a 1-degree bbox required by activities
    View must already exclude tiles fully covered by imported rasters
    """
    return sql_to_dict("SELECT * FROM activities.vw_required_elevation_tiles")

# -----------------------------
# USGS Discovery
# -----------------------------

def discover_best_usgs_products(bbox: str) -> List[Dict]:
    """
    Returns ordered list of products (best resolution first)
    bbox: xmin,ymin,xmax,ymax (lon/lat)
    """
    params = {
        "bbox": bbox,
        "datasets": ",".join(DATASET_PRIORITY),
        "prodFormats": "GeoTIFF,IMG",
        "outputFormat": "JSON",
    }

    r = requests.get(USGS_API, params=params, timeout=30)
    r.raise_for_status()

    items = r.json().get("items", [])
    valid = []

    xmin, ymin, xmax, ymax = map(float, bbox.split(","))

    for i in items:
        bb = i.get("boundingBox") or {}
        if bb.get("minX") in (None, 0):
            continue

        # true spatial intersection check
        if not (
            bb["minX"] > xmax or bb["maxX"] < xmin or
            bb["minY"] > ymax or bb["maxY"] < ymin
        ):
            valid.append(i)

    # resolution prioritization
    ordered = []
    for tier in DATASET_PRIORITY:
        ordered.extend([i for i in valid if tier in i.get("title", "")])

    return ordered

# -----------------------------
# Tile Processing
# -----------------------------

def process_tile(tile: Dict):
    tile_name = tile["tile_name"]
    bbox = f"{tile['xmin']},{tile['ymin']},{tile['xmax']},{tile['ymax']}"
    st.info(f"Tile BBOX: {bbox}")

    if tile_already_imported(tile_name):
        return

    products = discover_best_usgs_products(bbox)
    if not products:
        mark_tile_failed(tile_name, "no_products")
        st.error(f'No products for {tile}')
        return

    for p in products:
        url = p["downloadURL"]
        st.info(f'Checking download status of {tile}')
        local_file = download_if_needed(url)
        rasters = extract_rasters(local_file)

        for r in rasters:
            st.info(f"Importing raster {r}")
            import_raster(r)
            st.info(f"Recording metadata {tile_name}")
            record_metadata(tile_name, p, r)

    mark_tile_imported(tile_name)
    st.info('Tile marked as imported')
    return

# -----------------------------
# Download / Extraction
# -----------------------------

def download_if_needed(url: str) -> Path:
    fname = url.split("/")[-1]
    dest = TILE_ROOT / fname

    if dest.exists():
        st.info(f"{url} already exists")
        return dest

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for c in r.iter_content(8192):
                f.write(c)

    st.success(f"{url} downloaded")
    return dest


def extract_rasters(path: Path) -> List[Path]:
    """
    Extract GeoTIFF / IMG rasters from a USGS download.
    Uses environment-specific elevation tile storage.
    Safe to call multiple times.
    """
    outputs: List[Path] = []

    tile_root = Path(elevation_tile_path())
    tile_root.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            for member in z.infolist():
                name = member.filename

                if not name.lower().endswith((".tif", ".img")):
                    continue

                out_path = tile_root / Path(name).name

                # Avoid re-extracting identical files
                if not out_path.exists():
                    with z.open(member) as src, open(out_path, "wb") as dst:
                        dst.write(src.read())

                outputs.append(out_path)
    else:
        # Non-zip raster already lives in tile_root
        outputs.append(path)
    st.success(f"Rasters Extracted")
    return outputs
# -----------------------------
# PostGIS Import
# -----------------------------

def import_raster(raster_path: Path):
    cmd = (
        f'raster2pgsql -a -F -I -C -M '
        f'-t 100x100 -s {SRID} "{raster_path}" {RASTER_TABLE} | '
        f'psql -q'
    )

    subprocess.run(cmd, shell=True, check=True)

# -----------------------------
# Metadata Catalog
# -----------------------------

def record_metadata(tile_name: str, product: Dict, raster: Path):
    h = sha256_file(raster)

    sql = f"""
        INSERT INTO {CATALOG_TABLE}
        (tile_name, product_id, title, source_url, file_hash)
        VALUES (
            '{tile_name}',
            '{product.get('id')}',
            '{product.get('title')}',
            '{product.get('downloadURL')}',
            '{h}'
        )
        ON CONFLICT (file_hash) DO NOTHING;
    """
    qec(sql)


def tile_already_imported(tile_name: str) -> bool:
    sql = f"""
        SELECT 1 FROM {CATALOG_TABLE}
        WHERE tile_name = '{tile_name}' AND import_status = 'imported'
        LIMIT 1;
    """
    return bool(sql_to_dict(sql))


def mark_tile_imported(tile_name: str):
    qec(f"""
        UPDATE {CATALOG_TABLE}
        SET import_status = 'imported'
        WHERE tile_name = '{tile_name}';
    """)


def mark_tile_failed(tile_name: str, reason: str):
    qec(f"""
        INSERT INTO {CATALOG_TABLE} (tile_name, import_status, notes)
        VALUES ('{tile_name}', 'failed', '{reason}')
        ON CONFLICT (tile_name)
        DO UPDATE SET import_status='failed', notes='{reason}';
    """)

# -----------------------------
# Utilities
# -----------------------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()
