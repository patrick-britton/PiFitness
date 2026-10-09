"""
004-005 Bug 004-005-7 — read-only diagnosis (SELECT-only, no writes).

Morning walk activity 24652871872 x course 96 ("Carson's Northside Walk"):
  1. activity row (start time, type, distance_m)
  2. segment row 96 (name, is_course, reference activity, reference distance)
  3. segment_matches rows for the pair (confirmed? confidence?)
  4. auto-approve view row for the pair (gates passed? approval_phase?)
  5. vw_activity_summary rows for the activity (what the report sees)
  6. leaderboard rows for course 96 (what the leaderboard sees)
  7. distance sources side by side (activities.distance_m vs
     segments.distance_m vs vw_activity_header_stats vs header view)

Usage: python scripts/diagnose_004_005_07.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         'backend', '.env'))

from backend_functions.database_functions import sql_to_dict

ACT = 24652871872
SEG = 96


def p(name, fn):
    try:
        rows = fn()
        print(f"\n=== {name} ({len(rows)} rows) ===")
        print(json.dumps(rows, indent=2, default=str)[:4000])
    except Exception as e:
        print(f"\n=== {name} ===\nERROR: {e}")


p("ACTIVITY", lambda: sql_to_dict(
    "SELECT activity_id, activity_type_name, start_time_utc, distance_m, "
    "is_downloaded FROM activities.activities WHERE activity_id = %s", (ACT,)))

p("SEGMENT_96", lambda: sql_to_dict(
    "SELECT segment_id, segment_name, is_course, activity_reference_id, "
    "distance_m, reference_start_point, reference_end_point "
    "FROM activities.segments WHERE segment_id = %s", (SEG,)))

p("MATCH_ROWS", lambda: sql_to_dict(
    "SELECT activity_id, segment_id, match_confirmed, match_confidence, "
    "match_ts_utc FROM activities.segment_matches "
    "WHERE activity_id = %s AND segment_id = %s", (ACT, SEG)))

p("ALL_MATCHES_FOR_ACTIVITY", lambda: sql_to_dict(
    "SELECT activity_id, segment_id, match_confirmed, match_confidence "
    "FROM activities.segment_matches WHERE activity_id = %s", (ACT,)))

p("AUTO_APPROVE", lambda: sql_to_dict(
    "SELECT * FROM activities.vw_segment_create_auto_approve "
    "WHERE activity_id = %s AND segment_id = %s", (ACT, SEG)))

p("ACTIVITY_SUMMARY", lambda: sql_to_dict(
    "SELECT * FROM activities.vw_activity_summary WHERE activity_id = %s", (ACT,)))

p("REPORT_EFFORTS", lambda: sql_to_dict(
    "SELECT s.segment_id, s.segment_name AS name, s.is_course, "
    "s.all_time_rank, s.best_delta_s, s.prior_delta_s "
    "FROM activities.vw_activity_summary s WHERE s.activity_id = %s "
    "ORDER BY s.is_course DESC", (ACT,)))

p("LEADERBOARD_96", lambda: sql_to_dict(
    "SELECT segment_id, activity_id, all_time_rank, activity_start_utc "
    "FROM activities.vw_segment_leaderboard WHERE segment_id = %s "
    "ORDER BY all_time_rank LIMIT 10", (SEG,)))

p("DISTANCE_SOURCES", lambda: sql_to_dict(
    "SELECT a.activity_id, a.distance_m AS act_distance_m, "
    "s.segment_id, s.distance_m AS seg_distance_m "
    "FROM activities.activities a CROSS JOIN activities.segments s "
    "WHERE a.activity_id = %s AND s.segment_id = %s", (ACT, SEG)))

p("HEADER_STATS", lambda: sql_to_dict(
    "SELECT * FROM activities.vw_activity_header_stats "
    "WHERE activity_id = %s", (ACT,)))

p("COURSE_REVIEW_96", lambda: sql_to_dict(
    "SELECT * FROM activities.vw_course_review WHERE segment_id = %s", (SEG,)))
