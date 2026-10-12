"""
004-007 T01 — read-only diagnosis: what are forced tasks 4/19
(sync_activities / sync_details) actually executed by
ultimate_task_executioner(force_task_id=4/19)?

SELECT-only against PRD. Run from repo root:
  .venv/Scripts/python.exe scripts/diag_004_007_sync_tasks.py
"""
import json

from backend_functions.database_functions import sql_to_dict

print(json.dumps(sql_to_dict(
    "SELECT task_id, task_name, run_python, run_extract, python_execution_function "
    "FROM tasks.vw_task_info WHERE task_id IN (4,19)"),
    default=str, indent=1))

print(json.dumps(sql_to_dict(
    "SELECT pg_get_viewdef('activities.vw_last_activity_id_by_type', true) AS viewdef"),
    default=str, indent=1))

print(json.dumps(sql_to_dict(
    "SELECT task_name, api_function, api_loop_type, api_post_processing, api_parameters "
    "FROM tasks.task_config ORDER BY task_name"),
    default=str, indent=1))

print(json.dumps(sql_to_dict(
    "SELECT activity_id, activity_type_name, distance_m, is_downloaded "
    "FROM activities.activities WHERE activity_id IN (22447361779, 21133164016, 17325964906) "
    "ORDER BY activity_id"),
    default=str, indent=1))
