"""
004-005 T03 — read-only diagnosis of Bug 004-005-3 (silent false-success +
missing segment/course match). SELECT-only against PRD, mirroring T01's
investigate_004_005_resample_duplicates.py evidence pattern.

Run from repo root:  .venv/Scripts/python.exe scripts/investigate_004_005_t03.py
"""
import json

from backend_functions.database_functions import sql_to_dict

# Phase-1 output (captured in the T03 progress log) is silenced so phase-3
# evidence is not pushed out of the console window; queries still execute.
QUIET = True


def p(label, rows):
    if QUIET:
        return
    print(f"\n=== {label} ===")
    print(json.dumps(rows, default=str, indent=1))


# 1. What are forced tasks 4 / 19 (the /process sync steps)?
p("TASKS 4/19", sql_to_dict(
    "SELECT task_id, task_name, run_python, run_extract, python_execution_function "
    "FROM tasks.vw_task_info WHERE task_id IN (4,19)"))

# 2. What is queued for post-processing right now?
p("QUEUE", sql_to_dict(
    "SELECT activity_id FROM activities.activity_processing_queue "
    "ORDER BY activity_id DESC LIMIT 10"))

# 3. Resolved target activities for last_walk / last_run.
p("LAST_ACTIVITY_BY_TYPE", sql_to_dict(
    "SELECT * FROM activities.vw_last_activity_id_by_type"))

# 4. Recent post-processing sub-step events (errors prove the silent failure).
p("POST_PROCESSING_ERRORS", sql_to_dict(
    "SELECT * FROM logging.application_events "
    "WHERE error_text IS NOT NULL AND event_description ILIKE %s "
    "ORDER BY 1 DESC LIMIT 15", ["%Post Processing%"]))

# 5. Recent task execution records (did runs report success despite failure?).
p("TASK_EXECUTIONS", sql_to_dict(
    "SELECT * FROM tasks.task_executions ORDER BY execution_id DESC LIMIT 10"))

# 6. Which tasks recently reconciled as failed / succeeded (task_configuration).
p("TASK_CONFIG_RECENT", sql_to_dict(
    "SELECT task_id, task_name, last_executed_utc, last_succeeded_utc, last_failed_utc, "
    "consecutive_failures, last_failure_message "
    "FROM tasks.task_configuration WHERE task_id IN (4,19) "
    "OR task_name ILIKE %s ORDER BY task_id", ["%Post Processing%"]))

# 7. Segment-match storage tables (for the missing-match evidence).
p("MATCH_TABLES", sql_to_dict(
    "SELECT table_schema, table_name FROM information_schema.tables "
    "WHERE table_name ILIKE %s ORDER BY 1,2", ["%match%"]))

# 8. Segment/course matches for the most recent activities (missing match?).
try:
    p("RECENT_ACTIVITY_MATCH_COUNTS", sql_to_dict(
        "SELECT a.activity_id, "
        "(SELECT count(*) FROM activities.activity_segment_matches m "
        " WHERE m.activity_id = a.activity_id) AS match_rows "
        "FROM activities.activities a ORDER BY a.activity_id DESC LIMIT 5"
    ))
except Exception as e:
    print(f"\n=== RECENT_ACTIVITY_MATCH_COUNTS ===\nERROR: {e}")


def p2(label, rows):
    if QUIET:
        return
    print(f"\n=== {label} ===")
    print(json.dumps(rows, default=str, indent=1))


def p3(label, rows):
    print(f"\n=== {label} ===")
    print(json.dumps(rows, default=str, indent=1))


# --- T03 phase 2 ----------------------------------------------------------------

# 9. All post-processing error rows (key columns only — error_text is huge).
p2("PP_ERROR_TIMELINE", sql_to_dict(
    "SELECT event_time_utc, event_description FROM logging.application_events "
    "WHERE error_text IS NOT NULL AND event_description ILIKE %s "
    "ORDER BY 1 DESC LIMIT 20", ["%Post Processing%"]))

# 10. Everything logged in the most recent run window (reconstruct the UI run).
p2("EVENTS_SINCE_1440", sql_to_dict(
    "SELECT event_time_utc, event_category, event_description, "
    "CASE WHEN error_text IS NULL THEN NULL ELSE left(error_text, 120) END AS err "
    "FROM logging.application_events WHERE event_time_utc >= %s "
    "ORDER BY 1 ASC LIMIT 60", ["2026-10-07 14:40:00-07"]))

# 11. Admin task_executions for task 21 (Activity Detail Post Processing).
p2("TASK21_EXECUTIONS", sql_to_dict(
    "SELECT execution_id, task_id, task_name, status, started_at, completed_at, "
    "error_message FROM tasks.task_executions WHERE task_id = 21 "
    "ORDER BY execution_id DESC LIMIT 5"))

# 12. Segment matches for the resolved activities + the 3 stuck queue activities.
p2("MATCH_COUNTS_KEY_ACTIVITIES", sql_to_dict(
    "SELECT v.activity_id, "
    "(SELECT count(*) FROM activities.segment_matches m "
    " WHERE m.activity_id = v.activity_id) AS match_rows "
    "FROM (VALUES (24640204951), (24358931863), (3262162962), "
    "      (3259650230), (3252286731)) AS v(activity_id)"))

# 13. Latest activities overall with match counts (is anything ever matching?).
p2("LATEST_MATCH_COUNTS", sql_to_dict(
    "SELECT a.activity_id, "
    "(SELECT count(*) FROM activities.segment_matches m "
    " WHERE m.activity_id = a.activity_id) AS match_rows "
    "FROM activities.activities a ORDER BY a.activity_id DESC LIMIT 8"))

# 14. Server clock (correlate the run windows).
p2("DB_NOW", sql_to_dict("SELECT now() AS db_now"))


# --- T03 phase 3: why did segment matching produce zero rows for 24640204951? ---

SHOW_FNS = True  # need FN_MASS_CONFIRM: where does the candidate die?

if SHOW_FNS == "all":
    p3("FN_MATCH_SEGMENTS", sql_to_dict(
        "SELECT left(pg_get_functiondef(oid), 7000) AS def FROM pg_proc "
        "WHERE proname = %s AND pronamespace = %s::regnamespace",
        ["segment_matching_match_segments", "activities"]))

    p3("FN_PAIR_GENERATION", sql_to_dict(
        "SELECT left(pg_get_functiondef(oid), 7000) AS def FROM pg_proc "
        "WHERE proname = %s AND pronamespace = %s::regnamespace",
        ["segment_matching_activity_pair_generation", "activities"]))

p3("SEGMENT_BASE_TABLES", sql_to_dict(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
    "AND (table_name ILIKE %s OR table_name ILIKE %s) "
    "ORDER BY 1", ["activities", "%segment%", "%course%"]))

p3("ACT_24640204951", sql_to_dict(
    "SELECT activity_id, activity_type_id, is_downloaded "
    "FROM activities.activities WHERE activity_id = 24640204951"))

try:
    p3("EXCLUSIONS_24640204951", sql_to_dict(
        "SELECT * FROM activities.segment_match_exclusions "
        "WHERE activity_id = 24640204951"))
except Exception as e:
    print(f"\n=== EXCLUSIONS_24640204951 ===\nERROR: {e}")

try:
    p3("SEGMENT_TYPES", sql_to_dict(
        "SELECT activity_type_id, count(*) AS n FROM activities.segments "
        "GROUP BY 1 ORDER BY 2 DESC"))
except Exception as e:
    print(f"\n=== SEGMENT_TYPES ===\nERROR: {e}")


# --- T03 phase 4: replicate the match_segments gates for 24640204951 (SELECT) --

# Gate A (segment_matching_match_segments): same activity_type_id + bbox
# overlap + within 50 m of both segment endpoints.
p3("GATE_A_CANDIDATE_SEGMENTS", sql_to_dict(
    "SELECT s.segment_id, s.activity_type_id, "
    "a.activity_path && s.segment_path AS bbox_hit, "
    "ST_DWithin(a.activity_path::geography, s.start_point, 50) AS near_start, "
    "ST_DWithin(a.activity_path::geography, s.end_point, 50) AS near_end "
    "FROM activities.segments s "
    "JOIN activities.activities a ON a.activity_id = 24640204951 "
    "WHERE s.activity_type_id = 9 ORDER BY s.segment_id"))

# Gate B (pair_generation): trackpoints within 25 m of start / end gates and
# length fingerprint within 50 m.
p3("GATE_B_START_END_PASS_COUNTS", sql_to_dict(
    "SELECT s.segment_id, "
    "(SELECT count(*) FROM activities.activity_details_distance d "
    " WHERE d.activity_id = 24640204951 "
    "   AND ST_DWithin(d.loc_point::geography, s.start_point, 25)) AS start_passes, "
    "(SELECT count(*) FROM activities.activity_details_distance d "
    " WHERE d.activity_id = 24640204951 "
    "   AND ST_DWithin(d.loc_point::geography, s.end_point, 25)) AS end_passes, "
    "s.distance_m AS seg_len_m "
    "FROM activities.segments s WHERE s.activity_type_id = 9 ORDER BY s.segment_id"))

# What did the activity's own route length / point count look like?
p3("ACT_ROUTE_SUMMARY", sql_to_dict(
    "SELECT count(*) AS detail_rows, min(distance_m) AS min_m, "
    "max(distance_m) AS max_m "
    "FROM activities.activity_details_distance WHERE activity_id = 24640204951"))

# What does a SUCCESSFUL recent match look like (control)? 24358931863 has 5.
p3("CONTROL_MATCHES_RUN", sql_to_dict(
    "SELECT activity_id, segment_id FROM activities.segment_matches "
    "WHERE activity_id = 24358931863 ORDER BY segment_id LIMIT 10"))

# --- T03 phase 5: where do the candidates die? ----------------------------------

# State left behind by the 14:42 run (temp tables persist until next truncate).
p3("TEMP_MATCHING_SEGMENTS", sql_to_dict(
    "SELECT * FROM activities.temp_segment_matching_segments ORDER BY 1"))

p3("TEMP_MATCHES_DOWNSELECT", sql_to_dict(
    "SELECT activity_id, segment_id, best_start_dist, best_end_dist, "
    "dist_deviation, ruggedness "
    "FROM activities.temp_segment_matches_downselect ORDER BY 1 LIMIT 20"))

# Length-fingerprint gate (potential_pairs): best achievable |seg_len - span|.
p3("LENGTH_GATE_BEST_DEVIATION", sql_to_dict(
    "SELECT s.segment_id, s.distance_m AS seg_len_m, "
    "min(abs(s.distance_m - (ep.distance_m - sp.distance_m))) AS best_dev_m "
    "FROM activities.segments s "
    "JOIN activities.activity_details_distance sp "
    "  ON sp.activity_id = 24640204951 "
    " AND ST_DWithin(sp.loc_point::geography, s.start_point, 25) "
    "JOIN activities.activity_details_distance ep "
    "  ON ep.activity_id = 24640204951 "
    " AND ST_DWithin(ep.loc_point::geography, s.end_point, 25) "
    " AND ep.distance_m > sp.distance_m "
    "WHERE s.activity_type_id = 9 "
    "GROUP BY s.segment_id, s.distance_m ORDER BY s.segment_id"))

# Which procedure writes the final activities.segment_matches rows?
if SHOW_FNS:
    p3("FN_MASS_CONFIRM", sql_to_dict(
        "SELECT left(pg_get_functiondef(oid), 6000) AS def FROM pg_proc "
        "WHERE proname = %s AND pronamespace = %s::regnamespace",
        ["segment_matching_mass_confirmation", "activities"]))

# --- T03 phase 6: mass-confirmation tail + the writer of segment_matches -------

p3("FN_MASS_CONFIRM_PART2", sql_to_dict(
    "SELECT substr(pg_get_functiondef(oid), 5500, 6500) AS def FROM pg_proc "
    "WHERE proname = %s AND pronamespace = %s::regnamespace",
    ["segment_matching_mass_confirmation", "activities"]))

p3("WRITERS_OF_SEGMENT_MATCHES", sql_to_dict(
    "SELECT n.nspname AS schema, p.proname FROM pg_proc p "
    "JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE p.prosrc ILIKE %s ORDER BY 1,2",
    ["%into activities.segment_matches%"]))

p3("FN_AUTO_APPROVE_VIEW", sql_to_dict(
    "SELECT left(pg_get_viewdef(%s::regclass, true), 5000) AS def",
    ["activities.vw_segment_create_auto_approve"]))




# --- T03 phase 7: why was the segment-22 candidate not approved? ----------------

p3("AUTO_APPROVE_ROW", sql_to_dict(
    "SELECT * FROM activities.vw_segment_create_auto_approve "
    "WHERE activity_id = 24640204951"))

p3("SEGMENT_REFERENCES", sql_to_dict(
    "SELECT segment_id, activity_reference_id, is_course "
    "FROM activities.segments WHERE segment_id IN (22, 94, 95) ORDER BY 1"))

# Control: what do score rows look like (any row at all in the pipeline)?
p3("AUTO_APPROVE_ALL", sql_to_dict(
    "SELECT activity_id, segment_id, dist_deviation, polygon_deviation_m, "
    "hausdorff_deviation_m, freschet_deviation_m, confidence, approval_phase "
    "FROM activities.vw_segment_create_auto_approve ORDER BY activity_id LIMIT 15"))
