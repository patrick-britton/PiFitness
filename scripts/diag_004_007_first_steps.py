"""
Read-only diagnosis — 004-007 human report (2026-10-11): first two steps of
activity processing (sync_activities / sync_details) vs activity 24689141056.

Theories to test (never guess):
  T1) UI-initiated download doesn't stick (is_downloaded never flips / row absent)
  T2) resolve doesn't pick up the hiking activity as 'Walk'

SELECT-only against PRD. Run from repo root:
  .venv/Scripts/python.exe scripts/diag_004_007_first_steps.py
"""
import json

from backend_functions.database_functions import sql_to_dict


def p(label, rows):
    print(f"\n=== {label} ===")
    print(json.dumps(rows, default=str, indent=1))


AID = 24689141056

# 1. Does the activity exist locally at all? (task 4 writes activities.activities)
p("TARGET_ACTIVITY", sql_to_dict(
    "SELECT activity_id, activity_type_name, distance_m, duration_elapsed_s, "
    "is_downloaded, start_time_utc "
    "FROM activities.activities WHERE activity_id = %s", [AID]))

# 2. Most recent 10 activities in the table (what the list sync has ingested)
p("RECENT_ACTIVITIES", sql_to_dict(
    "SELECT activity_id, activity_type_name, distance_m, is_downloaded, start_time_utc "
    "FROM activities.activities ORDER BY start_time_utc DESC NULLS LAST LIMIT 10"))

# 3. What task 19 (details download) would pick up RIGHT NOW
p("SYNC_VIEW_COUNT", sql_to_dict(
    "SELECT count(*) AS n FROM activities.vw_activity_ids_to_sync"))
p("SYNC_VIEW_ROWS", sql_to_dict(
    "SELECT activity_id FROM activities.vw_activity_ids_to_sync ORDER BY activity_id DESC"))

# 4. What resolve_activity (step 3) would return for Walk / Run right now
p("LAST_BY_TYPE", sql_to_dict(
    "SELECT * FROM activities.vw_last_activity_id_by_type"))
p("LAST_BY_TYPE_VIEWDEF", sql_to_dict(
    "SELECT pg_get_viewdef('activities.vw_last_activity_id_by_type', true) AS viewdef"))

# 5. Why might the target be excluded from either view? (int literal inlined —
#    no params, so the LIKE '%%' patterns don't collide with psycopg2 placeholders)
p("TARGET_EXCLUSION_PROBE", sql_to_dict(
    "SELECT activity_id, distance_m, is_downloaded, activity_type_name, "
    "(distance_m > 1000) AS pass_1000, (distance_m > 1500) AS pass_1500, "
    "(activity_type_name ILIKE '%treadmill%') AS is_treadmill, "
    "(activity_type_name ILIKE '%walk%') AS like_walk, "
    "(activity_type_name ILIKE '%hike%') AS like_hike, "
    "(activity_type_name ILIKE '%hik%') AS like_hik "
    "FROM activities.activities WHERE activity_id = 24689141056"))

# 5b. Do detail rows exist for the target (did step 2 ever deliver data)?
p("TARGET_DETAIL_ROWS", sql_to_dict(
    "SELECT count(*) AS n FROM activities.activity_details "
    "WHERE activity_id = 24689141056"))

# 5c. The two candidate Walk rows the view could return, side by side
p("WALK_CANDIDATES", sql_to_dict(
    "SELECT activity_id, activity_type_name, distance_m, is_downloaded "
    "FROM activities.activities "
    "WHERE (activity_type_name LIKE '%walk%' OR activity_type_name LIKE '%hike%') "
    "AND distance_m > 1500 ORDER BY activity_id DESC LIMIT 5"))

# 6. Scheduler / failure state of the two sync tasks (lockout check)
p("TASK_STATE", sql_to_dict(
    "SELECT task_id, task_name, should_execute, consecutive_failures, "
    "next_planned_execution_utc "
    "FROM tasks.vw_task_info WHERE task_id IN (4, 19)"))
p("TASK_CONFIG", sql_to_dict(
    "SELECT task_id, task_name, last_executed_utc, last_succeeded_utc, "
    "last_failed_utc, consecutive_failures, last_failure_message "
    "FROM tasks.task_configuration WHERE task_id IN (4, 19)"))

# 7. Recent task-run history for tasks 4/19 (did the UI clicks run them?)
p("RECENT_RUNS", sql_to_dict(
    "SELECT execution_id, task_id, task_name, status, started_at, completed_at, "
    "error_message "
    "FROM tasks.task_executions WHERE task_id IN (4, 19) "
    "ORDER BY execution_id DESC LIMIT 10"))

# 8. Blast radius: which OTHER live views share the '%hike%' pattern that
#    fails to match the raw type string 'hiking'? (no params → single %)
p("VIEWS_WITH_HIKE_PATTERN", sql_to_dict(
    "SELECT schemaname, viewname FROM pg_views "
    "WHERE definition LIKE '%hike%' ORDER BY schemaname, viewname"))

# 9. Full def of the second affected view (report header path)
p("HEADER_STATS_VIEWDEF", sql_to_dict(
    "SELECT pg_get_viewdef('activities.vw_activity_header_stats', true) AS viewdef"))
