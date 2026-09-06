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
            assert "latitude" in first_point
            assert "longitude" in first_point
            assert "elevation_m" in first_point

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


def test_activity_report_contract_shape():
    """Test GET /api/activities/report returns the cross-surface ActivityReport
    shape (009-001 T02), with query helpers mocked so no DB is required."""
    import backend.api.activities as api_activities

    header = {
        "start_utc": "2026-09-01T12:00:00+00:00",
        "distance_mi": 5.0,
        "total_time_s": 2700.0,
    }
    efforts = [
        {
            "segment_id": 11,
            "name": "My Course",
            "is_course": True,
            "all_time_rank": 2,
            "prior_delta_s": -12.5,
            "best_delta_s": 8.25,
            "total_attempts": 47,
        },
        {
            "segment_id": 22,
            "name": "Hill Sprint",
            "is_course": False,
            "all_time_rank": 15,
            "prior_delta_s": 3.0,
            "best_delta_s": 20.5,
            "total_attempts": 47,
        },
    ]

    import unittest.mock as mock
    with mock.patch.object(api_activities, "resolve_latest_activity_id", return_value=1234), \
         mock.patch.object(api_activities, "get_activity_report_header", return_value=header), \
         mock.patch.object(api_activities, "get_activity_percentile_hr", side_effect=[150.0, 165.0, 182.0]), \
         mock.patch.object(api_activities, "get_activity_report_efforts", return_value=efforts):
        response = client.get("/api/activities/report?activity_type=Run")
        assert response.status_code == 200
        data = response.json()
        assert data["activity_id"] == 1234
        assert data["activity_type"] == "Run"
        assert data["has_segments"] is True
        assert data["header"]["total_time_text"] == "0:45:00.000"
        assert data["header"]["pace_text"] == "9:00.00/mi"
        assert data["header"]["hr_median"] == 150.0
        assert data["header"]["hr_p75"] == 165.0
        assert data["header"]["hr_max"] == 182.0
        assert data["header"]["show_efficiency_placeholder"] is True
        # Course mapped from the is_course row; segments exclude the course row.
        assert data["course"]["name"] == "My Course"
        assert data["course"]["all_time_rank"] == 2
        assert data["course"]["total_attempts"] == 47
        assert len(data["segments"]) == 1
        assert data["segments"][0]["name"] == "Hill Sprint"
        assert data["segments"][0]["is_course"] is False
        assert data["segments"][0]["prior_delta_s"] == 3.0


def test_activity_report_invalid_type():
    """Test GET /api/activities/report rejects an unknown activity_type."""
    response = client.get("/api/activities/report?activity_type=Ride")
    assert response.status_code == 422


def test_activity_report_walk_no_placeholder():
    """Test a Walk report does not show the running-efficiency placeholder."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    header = {
        "start_utc": "2026-09-01T10:00:00+00:00",
        "distance_mi": 2.0,
        "total_time_s": 1800.0,
    }
    with mock.patch.object(api_activities, "resolve_latest_activity_id", return_value=999), \
         mock.patch.object(api_activities, "get_activity_report_header", return_value=header), \
         mock.patch.object(api_activities, "get_activity_percentile_hr", side_effect=[120.0, 140.0, 160.0]), \
         mock.patch.object(api_activities, "get_activity_report_efforts", return_value=[]):
        response = client.get("/api/activities/report?activity_type=Walk")
        assert response.status_code == 200
        data = response.json()
        assert data["activity_type"] == "Walk"
        assert data["header"]["show_efficiency_placeholder"] is False
        assert data["course"] is None
        assert data["segments"] == []
        assert data["has_segments"] is False