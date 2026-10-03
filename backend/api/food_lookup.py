"""Food API Raw-JSON Diagnostic Proxy (010-001).
=============================================

Dev-only diagnostic endpoints that forward a query straight to the upstream
external API and return the raw response body unmodified. No local database
lookup (besides the FR-5 USDA credential read), no persistence, no flattening.

Endpoints (contract from .features/designs_active/010-001_design.md):
    GET /api/food/lookup/usda?query=<text>&pageSize=<n>&dataType=<csv>
        -> {upstreamStatus, body, rateLimitRemaining}
    GET /api/food/lookup/off?barcode=<code>
        -> {upstreamStatus, body}

Non-2xx upstream responses still return HTTP 200 from this proxy with
`upstreamStatus` set, so the raw error body stays displayable. Local proxy
failures (validation / upstream unreachable / missing credential) return the
local-error shape {detail, status, type}.
"""

import re
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend_functions.service_logins import load_api_credentials

router = APIRouter(prefix="/api/food/lookup", tags=["food-lookup"])

_FDC_SEARCH_URL = "https://api.nal.usda.gov/fdc/v1/foods/search"
_OFF_PRODUCT_URL = "https://world.openfoodfacts.org/api/v2/product/{barcode}.json"

_TIMEOUT_S = 15.0
_PAGE_SIZE_DEFAULT = 10
_PAGE_SIZE_MAX = 100

_BARCODE_RE = re.compile(r"^[0-9A-Za-z\-]{1,32}$")


def _local_error(status: int, err_type: str, detail: str) -> JSONResponse:
    """Consistent local-error envelope (proxy itself failed)."""
    return JSONResponse(
        status_code=status,
        content={"detail": detail, "status": status, "type": err_type},
    )


def _passthrough(upstream: httpx.Response, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Shape an upstream response into the contract envelope without altering the body."""
    try:
        body = upstream.json()
    except Exception:
        body = {"_nonJsonBody": upstream.text}
    payload: Dict[str, Any] = {"upstreamStatus": upstream.status_code, "body": body}
    if extra:
        payload.update(extra)
    return payload


def _resolve_usda_key() -> Optional[str]:
    """Resolve the USDA key server-side via the canonical credential store.

    Returns None when the USDA service row (or its api_key field) is absent;
    the caller then fails closed with a ConfigError. Key material never
    leaves the backend.
    """
    try:
        creds = load_api_credentials("USDA")
    except Exception:
        return None
    if not isinstance(creds, dict):
        return None
    key = creds.get("api_key")
    return key or None


@router.get("/usda")
async def lookup_usda(
    query: str = "",
    pageSize: int = _PAGE_SIZE_DEFAULT,
    dataType: Optional[str] = None,
):
    """Proxy a food-name query to USDA FDC /foods/search, raw passthrough."""
    query = (query or "").strip()
    if not query:
        return _local_error(400, "ValidationError", "query must be a non-empty string")
    page_size = max(1, min(pageSize or _PAGE_SIZE_DEFAULT, _PAGE_SIZE_MAX))

    api_key = _resolve_usda_key()
    if not api_key:
        return _local_error(
            500, "ConfigError", "USDA credential (api_key) is not configured in the credential store"
        )

    params: Dict[str, Any] = {"api_key": api_key, "query": query, "pageSize": page_size}
    data_types = (dataType or "").strip()
    if data_types:
        params["dataType"] = data_types

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
            upstream = await client.get(_FDC_SEARCH_URL, params=params)
    except httpx.TimeoutException:
        return _local_error(502, "UpstreamError", "USDA FDC request timed out")
    except httpx.HTTPError as e:
        return _local_error(502, "UpstreamError", f"USDA FDC request failed: {e}")

    # Rate-limit visibility is returned in-band (no DB logging per AC-5).
    return _passthrough(upstream, {"rateLimitRemaining": upstream.headers.get("X-RateLimit-Remaining")})


@router.get("/off")
async def lookup_off(barcode: str = ""):
    """Proxy a barcode to Open Food Facts /api/v2/product/{barcode}.json, raw passthrough."""
    barcode = (barcode or "").strip()
    if not _BARCODE_RE.match(barcode):
        return _local_error(400, "ValidationError", "barcode must be 1-32 alphanumeric characters")
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
            upstream = await client.get(_OFF_PRODUCT_URL.format(barcode=barcode))
    except httpx.TimeoutException:
        return _local_error(502, "UpstreamError", "Open Food Facts request timed out")
    except httpx.HTTPError as e:
        return _local_error(502, "UpstreamError", f"Open Food Facts request failed: {e}")
    return _passthrough(upstream)
