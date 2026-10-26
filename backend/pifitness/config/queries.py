"""
App-wide configuration endpoints (009-002 T08).

Serves client-safe configuration the React frontend needs to render
interactive surfaces. Currently exposes the Mapbox access token so the
Leaderboards visuals replay can build Mapbox-style tile URLs and gate the
style dropdown on token presence (AC-13).

The Mapbox token is an inherently client-side web-tile secret (Mapbox GL JS
presents it directly to api.mapbox.com when loading vector/raster tiles), so
it is served to the browser, mirroring how Mapbox web maps work everywhere.
The backend stays the single source of truth (resolved from the encrypted
`api_services.credentials` store) so there is no duplicate secret in the
frontend build environment.
"""

from fastapi import APIRouter

from backend_functions.service_logins import mapbox_token

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("/map")
async def get_map_config():
    """Return client-safe map configuration.

    Returns:
        {"mapboxToken": str | null} — the Mapbox access token when a Mapbox
        credential is configured, otherwise null.
    """
    try:
        token = mapbox_token()
    except Exception:
        # No/undecodable Mapbox credential -> treat as absent; UI degrades to
        # the free OSM/Carto + blank vector styles only (AC-13).
        token = None
    return {"mapboxToken": token or None}