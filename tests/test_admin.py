"""Admin API checks against the live database."""

import pytest
from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# BUG 005.001 — Raw Logs Fix Verification
# All logging schema tables should return 200, not 500
# ---------------------------------------------------------------------------

@pytest.mark.regression
@pytest.mark.live
@pytest.mark.parametrize("table_name", [
    "api_logins",
    "application_events",
    "task_executions",
    "db_size_log",
    "db_stats",
])
def test_admin_logs_data_all_tables(table_name: str):
    """GET /api/admin/logs/data/{table} answers 200 with the data and count envelope for
    every logging-schema table instead of a 500.
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
