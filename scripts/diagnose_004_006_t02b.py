"""T02 (004-006) live read-only execution of the migration file's statements
(wrappers stripped) — proves the exact file content runs on PRD and captures
EXPLAIN evidence. Read-only. Usage: python scripts/diagnose_004_006_t02b.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backend_functions.database_functions import sql_to_dict  # noqa: E402

TEXT = Path(__file__).resolve().parent.parent.joinpath(
    '.features', 'migrations', '004-006_report_perf_and_nav.sql'
).read_text(encoding='utf-8')

BODY = re.search(r'AS \$\$(.*?)\$\$;', TEXT, re.DOTALL).group(1).strip().rstrip(';')
VBODY = re.search(
    r'CREATE OR REPLACE VIEW activities\.vw_report_segment_efforts AS\s+(.*);',
    TEXT, re.DOTALL,
).group(1).strip()


def p(title):
    print(f"\n=== {title} ===")


p("S1 live: default (newest) target")
# p_activity_id is a function parameter; substitute NULL::bigint for the live run
body_default = BODY.replace("p_activity_id,", "NULL::bigint,")
r = sql_to_dict("SELECT * FROM (" + body_default + ") fn", ())[0]
for k, v in r.items():
    print(f"  {k} = {v}")
act = int(r["activity_id"])

p("S1 live: explicit prev/next ids")
for t in (r["prev_activity_id"], r["next_activity_id"]):
    if t is None:
        print("  (None at boundary)")
        continue
    b2 = BODY.replace("p_activity_id,", str(int(t)) + "::bigint,")
    rr = sql_to_dict(b2 + " LIMIT 1", ())[0]
    print(f"  id={rr['activity_id']} prev={rr['prev_activity_id']} "
          f"next={rr['next_activity_id']} dist={rr['distance_display']} "
          f"pace={rr['pace_display']} hr_med={rr['hr_median']}")

p(f"S2 live: rows for activity {act}")
for e in sql_to_dict(
    "SELECT * FROM (" + VBODY + ") v WHERE v.activity_id = %s "
    "ORDER BY v.is_course DESC, v.start_time_utc", (act,),
):
    print(f"  seg={e['segment_id']} course={e['is_course']} "
          f"rank={e['all_time_rank']}/{e['total_attempts']} "
          f"prior_d={e['prior_delta_s']} best_d={e['best_delta_s']} "
          f"p10_cad={e['prior10_avg_cadence']} cad={e['avg_cadence']}")

p("S2 live: newest COURSE efforts (gold-case sanity: best_delta_s > 0 expected)")
q = ("SELECT v.activity_id, v.segment_id, v.all_time_rank, v.total_attempts, v.best_delta_s "
     "FROM (" + VBODY + ") v "
     "JOIN activities.segments s ON s.segment_id = v.segment_id AND s.is_course "
     "ORDER BY v.activity_id DESC LIMIT 3")
for c in sql_to_dict(q, ()):
    print(f"  act={c['activity_id']} seg={c['segment_id']} "
          f"rank={c['all_time_rank']}/{c['total_attempts']} best_d={c['best_delta_s']}")

p("EXPLAIN S2 scoped")
for row in sql_to_dict(
    "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM (" + VBODY + ") v "
    "WHERE v.activity_id = " + str(act), (),
):
    plan = row["QUERY PLAN"]
    if "Execution Time" in plan or "Seq Scan" in plan:
        print(" ", plan)
