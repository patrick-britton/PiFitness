"""failed_run_message turns failed task results into the error message the admin API
reports.
"""
import pytest
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
            override=False)

from backend.pifitness.admin.services import failed_run_message


@pytest.mark.unit
def test_failed_run_message_carries_underlying_error():
    """failed_run_message includes the failure count, the failed task's name and its
    underlying error text.
    """
    failed = [{"task_id": 19, "task_name": "Activity Details",
               "success": False, "error": "No API response to load"}]
    msg = failed_run_message(failed)
    assert "1 failure(s)" in msg
    assert "Activity Details" in msg
    assert "No API response to load" in msg


@pytest.mark.unit
def test_failed_run_message_aggregates_multiple():
    """failed_run_message reports the total failure count and every failed task's error
    when several tasks fail.
    """
    failed = [
        {"task_id": 19, "task_name": "Activity Details",
         "success": False, "error": "No API response to load"},
        {"task_id": 4, "task_name": "Activity Sync",
         "success": False, "error": "boom"},
    ]
    msg = failed_run_message(failed)
    assert "2 failure(s)" in msg
    assert "No API response to load" in msg
    assert "boom" in msg
