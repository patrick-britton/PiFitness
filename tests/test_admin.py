"""
Test Admin API Endpoints
========================

Basic smoke tests for admin API endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app)

def test_admin_tasks():
    """Test GET /api/admin/tasks returns successfully."""
    response = client.get("/api/admin/tasks")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0
        first_task = data["data"][0]
        assert "task_name" in first_task
        assert "task_description" in first_task

def test_admin_task_schedule():
    """Test GET /api/admin/tasks/schedule returns successfully."""
    response = client.get("/api/admin/tasks/schedule")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)

def test_admin_task_names():
    """Test GET /api/admin/tasks/names returns successfully."""
    response = client.get("/api/admin/tasks/names")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0

def test_admin_events_no_filter():
    """Test GET /api/admin/events returns successfully without filters."""
    response = client.get("/api/admin/events")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0

def test_admin_events_with_filters():
    """Test GET /api/admin/events with filter parameters."""
    response = client.get("/api/admin/events?search=test&errors_only=true&ignore_skips=true&event_type=INFO&limit=50")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    # With filters, count should be <= limit
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) <= 50
