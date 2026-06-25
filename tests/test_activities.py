"""
Test Activities API Endpoints
============================

Basic smoke tests for activities API endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app)

def test_activities_list():
    """Test GET /api/activities returns successfully with schema-aware columns."""
    response = client.get("/api/activities")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0
        first_activity = data["data"][0]
        # These columns should always exist
        assert "activity_id" in first_activity
        assert "activity_name" in first_activity
        assert "start_timestamp_utc" in first_activity
        # These columns may or may not exist (schema-aware)
        if "duration_moving_s" in first_activity:
            assert isinstance(first_activity["duration_moving_s"], int)
        if "distance_m" in first_activity:
            assert isinstance(first_activity["distance_m"], (int, float))

def test_activity_by_id():
    """Test GET /api/activities/{id} returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["data"]["activity_id"] == activity_id

def test_activity_telemetry():
    """Test GET /api/activities/{id}/telemetry returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}/telemetry")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        if len(data["data"]) > 0:
            first_point = data["data"][0]
            assert "timestamp_utc" in first_point
            assert "latitude_deg" in first_point

def test_activity_segments():
    """Test GET /api/activities/{id}/segments returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}/segments")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "count" in data