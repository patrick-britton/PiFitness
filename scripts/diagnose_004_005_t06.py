"""
004-005 T06 — read-only diagnosis of Bug 004-005-6 ('No API response to load'
on Activity Details sync) and Bug 004-005-5 (count-only failure reporting).

SELECT-only against PRD; mirrors investigate_004_005_t03.py.

Run from repo root:  .venv/Scripts/python.exe scripts/diagnose_004_005_t06.py
"""
import json

from backend_functions.database_functions import sql_to_dict


def p(label, rows):
    print(f"\n=== {label} ===")
    print(json.dumps(rows, default=str, indent=1))


# 1. Task 19 (Sync Activity Details) + task 4 configuration: which extractor,
#    which login, which service?
p("TASK_CONFIG_4_19", sql_to_dict(
    "SELECT task_id, task_name, run_extract, run_python, python_extraction_function, "
    "python_login_function, python_execution_function, flatten_sproc, api_function_name "
    "FROM tasks.vw_task_info WHERE task_id IN (4, 19)"))

# 2. Reconcile state: what did the failed runs write?
p("TASK_CONFIGURATION_4_19", sql_to_dict(
    "SELECT task_id, task_name, last_executed_utc, last_succeeded_utc, last_failed_utc, "
    "consecutive_failures, last_failure_message "
    "FROM tasks.task_configuration WHERE task_id IN (4, 19)"))

# 3. Recent task_executions for 4/19: does error_message hold only the count
#    while result JSONB carries the detail? (Bug 5 evidence.)
p("TASK_EXECUTIONS_4_19", sql_to_dict(
    "SELECT execution_id, task_id, task_name, status, started_at, completed_at, "
    "error_message, left(result::text, 1200) AS result_preview "
    "FROM tasks.task_executions WHERE task_id IN (4, 19) "
    "ORDER BY execution_id DESC LIMIT 6"))

# 4. The exact failure events: 'Loading ignored' / 'No API response to load'.
p("NO_API_RESPONSE_EVENTS", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description, task_id, error_text "
    "FROM logging.application_events "
    "WHERE event_description ILIKE %s OR error_text ILIKE %s "
    "ORDER BY event_time_utc DESC LIMIT 10",
    ["%Loading ignored%", "%No API response to load%"]))

# 5. All recent events for the Activity Detail sync (Task #19) — extract vs
#    load vs login stages: did extraction succeed/return empty?
p("TASK19_RECENT_EVENTS", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description, error_text, data_event "
    "FROM logging.application_events "
    "WHERE event_category ILIKE %s "
    "ORDER BY event_time_utc DESC LIMIT 30", ["%Task #19%"]))

# 6. Activity Detail Sync events (extractor's own logging).
p("ACTIVITY_DETAIL_SYNC_EVENTS", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description, error_text "
    "FROM logging.application_events "
    "WHERE event_category = %s "
    "ORDER BY event_time_utc DESC LIMIT 20", ["Activity Detail Sync"]))

# 7. Is the sync view empty? (If empty, extractor returns `client` — a truthy
#    dict — vs `[]` when calls return falsy; both route to 'No API response'?
p("SYNC_VIEW_COUNT", sql_to_dict(
    "SELECT count(*) AS n, min(activity_id) AS min_id, max(activity_id) AS max_id "
    "FROM activities.vw_activity_ids_to_sync"))

p("SYNC_VIEW_ROWS", sql_to_dict(
    "SELECT * FROM activities.vw_activity_ids_to_sync ORDER BY activity_id DESC LIMIT 10"))

# 8. Control: are details already present for recent activities?
p("RECENT_ACTIVITY_DETAILS", sql_to_dict(
    "SELECT activity_id, count(*) AS detail_rows "
    "FROM activities.activity_details GROUP BY activity_id "
    "ORDER BY activity_id DESC LIMIT 5"))

# 9. Login/rate-limit state for the Garmin service.
p("GARMIN_SERVICE_STATE", sql_to_dict(
    "SELECT * FROM api_services.api_service_list WHERE api_service_name ILIKE %s",
    ["%garmin%"]))

# --- Bug 004-005-6 follow-up -------------------------------------------------

# 10. Loop metadata driving extract_pirate_universal for tasks 4/19.
p("LOOP_METADATA", sql_to_dict(
    "SELECT task_id, task_name, api_function_name, loop_strategy, iter_path_keys, "
    "iter_query_keys, value_recency, static_path_params, static_query_params "
    "FROM tasks.vw_task_info WHERE task_id IN (4, 19)"))

# 11. Definition of the activity sync view (why is it empty / when does it fill?).
p("SYNC_VIEW_DEF", sql_to_dict(
    "SELECT left(pg_get_viewdef(%s::regclass, true), 4000) AS def",
    ["activities.vw_activity_ids_to_sync"]))

# 12. Did the failing runs log extraction failures at all? (Errors are logged
#     with desc 'Extraction Failure for : ...' — absent means the loop never
#     ran or every call returned None silently.)
p("EXTRACTION_FAILURE_EVENTS", sql_to_dict(
    "SELECT event_time_utc, event_description, data_event "
    "FROM logging.application_events "
    "WHERE event_description ILIKE %s AND event_time_utc > %s "
    "ORDER BY event_time_utc DESC LIMIT 20",
    ["%Extraction Failure%", "2026-10-07"]))

# 13. Why is the sync view empty while 297 activities are not downloaded?
p("SYNC_VIEW_FUNNEL", sql_to_dict(
    "SELECT count(*) FILTER (WHERE NOT is_downloaded) AS not_downloaded, "
    "count(*) FILTER (WHERE NOT is_downloaded AND distance_m > 1000) AS not_dl_gt1k, "
    "count(*) FILTER (WHERE NOT is_downloaded AND distance_m > 1000 "
    "  AND activity_type_name NOT ILIKE '%treadmill%') AS eligible "
    "FROM activities.activities"))

# 14. The most recent not-downloaded rows and their filter attributes.
p("NOT_DOWNLOADED_SAMPLE", sql_to_dict(
    "SELECT activity_id, activity_type_name, distance_m, is_downloaded, start_time_utc "
    "FROM activities.activities WHERE NOT is_downloaded "
    "ORDER BY start_time_utc DESC LIMIT 10"))

# 15. When did the last activity arrive vs. the last successful detail download?
p("LAST_ARRIVALS", sql_to_dict(
    "SELECT max(start_time_utc) AS last_activity_start, "
    "max(start_time_utc) FILTER (WHERE is_downloaded) AS last_downloaded_start, "
    "max(start_time_utc) FILTER (WHERE NOT is_downloaded) AS last_not_downloaded_start "
    "FROM activities.activities"))

# 16. Scheduling impact: is task 19 about to be locked out of should_execute?
p("ACCEPTABLE_FAILURES", sql_to_dict(
    "SELECT * FROM tasks.acceptable_failures"))

p("SHOULD_EXECUTE_NOW", sql_to_dict(
    "SELECT task_id, task_name, should_execute, consecutive_failures, "
    "next_planned_execution_utc "
    "FROM tasks.vw_task_info WHERE task_id IN (4, 19, 21)"))

# 17. What did the 05:30 successful run actually download? (Proves the queue
#     was non-empty then and identifies the activity that emptied it.)
p("STG_IMPORTS_TASK19_TODAY", sql_to_dict(
    "SELECT * FROM staging.api_imports WHERE task_id = 19 ORDER BY 1 DESC LIMIT 3"))

# 18. What ran between the last success (05:30) and first failure (08:00)?
#     Did post-processing (build_activity_path → is_downloaded=TRUE) empty the view?
p("EVENTS_0530_0800", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description "
    "FROM logging.application_events "
    "WHERE event_time_utc BETWEEN %s AND %s "
    "ORDER BY event_time_utc LIMIT 40",
    ["2026-10-08 05:30:00-07", "2026-10-08 08:01:00-07"]))

# 19. Build-path / post-processing activity this morning.
p("PATH_EVENTS", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description "
    "FROM logging.application_events "
    "WHERE event_description ILIKE %s AND event_time_utc > %s "
    "ORDER BY event_time_utc DESC LIMIT 15",
    ["%path%", "2026-10-07"]))





