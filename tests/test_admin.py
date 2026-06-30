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

def test_admin_db_info_task_summary():
    """Test GET /api/admin/db-info/task-summary returns successfully."""
    response = client.get("/api/admin/db-info/task-summary")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0

def test_admin_db_info_db_size_chart():
    """Test GET /api/admin/db-info/db-size-chart returns successfully."""
    response = client.get("/api/admin/db-info/db-size-chart")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0

def test_admin_db_info_db_size_breakdown():
    """Test GET /api/admin/db-info/db-size-breakdown returns successfully."""
    response = client.get("/api/admin/db-info/db-size-breakdown")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0


# ---------------------------------------------------------------------------
# BUG 005.001 — Raw Logs Fix Verification
# All logging schema tables should return 200, not 500
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table_name", [
    "api_logins",
    "application_events",
    "task_executions",
    "db_size_log",
    "db_stats",
])
def test_admin_logs_data_all_tables(table_name: str):
    """Test GET /api/admin/logs/data/{table_name} for each logging table.
    
    This test verifies BUG 005.001 is fixed — all logging schema tables
    must return 200, not 500 Internal Server Error.
    """
    response = client.get(f"/api/admin/logs/data/{table_name}?limit=5")
    assert response.status_code == 200, (
        f"BUG 005.001: {table_name} returned {response.status_code}, expected 200"
    )
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        # Verify each row has event_time_utc (common column across all log tables)
        first_row = data["data"][0]
        assert "event_time_utc" in first_row, (
            f"Expected event_time_utc in {table_name} row columns: {list(first_row.keys())}"
        )


def test_admin_logs_tables():
    """Test GET /api/admin/logs/tables returns list of log tables."""
    response = client.get("/api/admin/logs/tables")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        # Verify known tables are present (BUG 005.001 context)
        expected_tables = {"api_logins", "application_events", "task_executions"}
        found_tables = set(data["data"])
        missing = expected_tables - found_tables
        assert not missing, (
            f"BUG 005.001: Expected log tables not found: {missing}"
        )


def test_admin_db_sessions():
    """Test GET /api/admin/db-sessions returns successfully."""
    response = client.get("/api/admin/db-sessions")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)


def test_admin_services():
    """Test GET /api/admin/services returns successfully."""
    response = client.get("/api/admin/services")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)


def test_admin_functions():
    """Test GET /api/admin/functions returns successfully."""
    response = client.get("/api/admin/functions")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)


def test_admin_credential_requirements():
    """Test GET /api/admin/credentials/requirements returns successfully."""
    response = client.get("/api/admin/credentials/requirements")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
