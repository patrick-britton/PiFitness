"""Smoke checks that the read-only API routes are wired and answer with their standard
envelope against the live database.
"""

import pytest
from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)

ENVELOPE_ROUTES = [
    "/api/activities?limit=1",
    "/api/admin/tasks",
    "/api/admin/tasks/schedule",
    "/api/admin/tasks/names",
    "/api/admin/events?limit=1",
    "/api/admin/events?search=test&errors_only=true&ignore_skips=true&event_type=INFO&limit=50",
    "/api/admin/db-info/task-summary",
    "/api/admin/db-info/db-size-chart",
    "/api/admin/db-info/db-size-breakdown",
    "/api/admin/logs/tables",
    "/api/admin/db-sessions",
    "/api/admin/services",
    "/api/admin/functions",
    "/api/admin/credentials/requirements",
]


# VERIFIED: Smoke — each read-only API route returns 200 with data envelope and integer count
@pytest.mark.smoke
@pytest.mark.live
@pytest.mark.parametrize("path", ENVELOPE_ROUTES)
def test_get_route_returns_data_envelope(path):
    """Each read-only API route in the list answers 200 with a data field and an integer
    count against the live database.
    """
    response = client.get(path)
    assert response.status_code == 200, f"{path} returned {response.status_code}"
    body = response.json()
    assert "data" in body
    assert isinstance(body["count"], int)


# VERIFIED: Smoke — /api/admin/events row count respects requested limit
@pytest.mark.smoke
@pytest.mark.live
def test_admin_events_limit_is_respected():
    """GET /api/admin/events with filters returns no more rows than the requested limit.
    """
    response = client.get("/api/admin/events?search=test&errors_only=true&ignore_skips=true&event_type=INFO&limit=50")
    assert response.status_code == 200
    assert len(response.json()["data"]) <= 50


# VERIFIED: Smoke — activity detail/telemetry/segments routes return 200 with data field for latest activity
@pytest.mark.smoke
@pytest.mark.live
@pytest.mark.parametrize("suffix", ["", "/telemetry", "/segments"])
def test_activity_detail_routes_respond_for_latest_activity(suffix):
    """The by-id, telemetry and segments routes answer 200 with a data field for the
    most recent activity, skipping explicitly when none exist.
    """
    listing = client.get("/api/activities?limit=1")
    assert listing.status_code == 200
    rows = listing.json()["data"]
    if not rows:
        pytest.skip("no activities in the database")
    activity_id = rows[0]["activity_id"]
    response = client.get(f"/api/activities/{activity_id}{suffix}")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    if suffix == "":
        assert body["data"]["activity_id"] == activity_id
