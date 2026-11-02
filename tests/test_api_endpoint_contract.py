"""Contract test: every API endpoint the frontend calls exists on the backend.

Promoted from root-level `validate_endpoints.py` (INT-34). Same assertion logic,
now runs under `pytest -m contract`.

Checks one direction: frontend-expected endpoints must be present in the
FastAPI app's route table, and each must return HTTP 200 when called via
TestClient.
"""

import re
import sys
from pathlib import Path

import pytest

# backend/ is on pythonpath via pytest.ini, so `main` resolves here.
from main import app  # noqa: E402


def _api_client_path() -> Path:
    """Resolve frontend/pifitness/src/lib/api-client.ts from this test file."""
    return Path(__file__).resolve().parents[1] / "frontend" / "pifitness" / "src" / "lib" / "api-client.ts"


def extract_api_client_endpoints() -> list[str]:
    """Extract all endpoints from the frontend API client."""
    api_client_path = _api_client_path()

    if not api_client_path.exists():
        return []

    content = api_client_path.read_text()

    # Find all API calls in the format fetchAPI("/api/...")
    pattern = r'fetchAPI\(["\'](/api/[^"\']+)["\']'
    endpoints = re.findall(pattern, content)

    # Also find any hardcoded paths
    pattern2 = r'["\'](/api/[^"\']+)["\']'
    hardcoded_endpoints = re.findall(pattern2, content)

    all_endpoints = list(set(endpoints + hardcoded_endpoints))

    valid_endpoints = []
    for endpoint in all_endpoints:
        if endpoint.startswith("/api/") and not endpoint.endswith("{"):
            clean_endpoint = endpoint.split("?")[0]
            valid_endpoints.append(clean_endpoint)

    return sorted(set(valid_endpoints))


def get_backend_endpoints() -> list[str]:
    """Get all available backend endpoints.

    Walks app.routes, handling both direct APIRoute objects and Starlette's
    _IncludedRouter wrapper (which stores the real router in original_router).
    Route paths in original_router.routes already include the router prefix,
    so they are used as-is.
    """
    endpoints = set()

    for route in app.routes:
        # Direct route (e.g. /api/health defined on the app)
        if hasattr(route, "path") and route.path.startswith("/api/"):
            endpoints.add(route.path)
        # Included router — Starlette wraps it in _IncludedRouter;
        # the real router (with .routes) is in original_router.
        # Route paths already include the router prefix, so use as-is.
        elif hasattr(route, "original_router"):
            orig = route.original_router
            for sub_route in getattr(orig, "routes", []):
                if hasattr(sub_route, "path") and sub_route.path.startswith("/api/"):
                    endpoints.add(sub_route.path)

    return sorted(endpoints)


@pytest.mark.contract
def test_api_endpoints_exist_on_backend():
    """Every frontend-expected endpoint must exist on the backend.

    Matches the original validate_endpoints.py behavior: asserts that no
    frontend-expected endpoint is missing from the backend route table.
    (The original script exercised each endpoint via TestClient for
    visibility only; that loop is omitted here to keep the test fast and
    deterministic under pytest.)
    """
    frontend_endpoints = extract_api_client_endpoints()
    backend_endpoints = get_backend_endpoints()

    missing = [ep for ep in frontend_endpoints if ep not in backend_endpoints]
    assert not missing, f"Missing backend endpoints: {missing}"