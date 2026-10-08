"""T11 regression — 004-005 Bug 004-005-5 (backend half).

The async execute endpoint records only a count
("Task execution completed with 1 failure(s)") while the real per-task
reason sits unused in the result JSONB. The endpoint must aggregate the
underlying `<task_name>: <error>` detail into `error_message`.

Pure unit test — no DB, no network, no pandas.
"""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
            override=False)

from backend.api.admin import _failed_run_message


def test_failed_run_message_carries_underlying_error():
    failed = [{"task_id": 19, "task_name": "Activity Details",
               "success": False, "error": "No API response to load"}]
    msg = _failed_run_message(failed)
    assert "1 failure(s)" in msg
    assert "Activity Details" in msg
    assert "No API response to load" in msg


def test_failed_run_message_aggregates_multiple():
    failed = [
        {"task_id": 19, "task_name": "Activity Details",
         "success": False, "error": "No API response to load"},
        {"task_id": 4, "task_name": "Activity Sync",
         "success": False, "error": "boom"},
    ]
    msg = _failed_run_message(failed)
    assert "2 failure(s)" in msg
    assert "No API response to load" in msg
    assert "boom" in msg
