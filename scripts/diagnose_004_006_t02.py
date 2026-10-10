"""
T02 (004-006) read-only reconnaissance.

1. Indexes / PK on activities.segments_details, activities.activity_details,
   activities.activities (report hot-path predicates).
2. Row counts for sizing.
3. EXPLAIN (ANALYZE, BUFFERS) of the candidate hot queries that the new
   004-006 view will rely on (scoped per-segment laterals).

Read-only: SELECT / EXPLAIN only. Usage: python scripts/diagnose_004_006_t02.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from backend_functions.database_functions import sql_to_dict  # noqa: E402


def p(title, fn):
    print(f"\n=== {title} ===")
    try:
        rows = fn()
        for r in rows:
            print(r)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: {e}")


def indexes(tbl):
    return sql_to_dict(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'activities' AND tablename = %s
        ORDER BY indexname
        """,
        (tbl,),
    )


p("INDEXES segments_details", lambda: indexes("segments_details"))
p("INDEXES activity_details", lambda: indexes("activity_details"))
p("INDEXES activities", lambda: indexes("activities"))

p(
    "COUNTS",
    lambda: sql_to_dict(
        """
        SELECT
            (SELECT count(*) FROM activities.segments_details) AS segments_details_rows,
            (SELECT count(*) FROM activities.activities) AS activities_rows,
            (SELECT count(*) FROM activities.segments) AS segments_rows
        """
    ),
)

# Representative activity with the most segment efforts (hot-path sizing).
p(
    "TOP ACTIVITY BY SEGMENT EFFORTS",
    lambda: sql_to_dict(
        """
        SELECT activity_id, count(*) AS effort_rows
        FROM activities.segments_details
        GROUP BY activity_id
        ORDER BY effort_rows DESC
        LIMIT 3
        """
    ),
)

SAMPLE = sql_to_dict(
    """
    SELECT activity_id FROM activities.segments_details
    GROUP BY activity_id ORDER BY count(*) DESC LIMIT 1
"""
)
ACT = int(SAMPLE[0]["activity_id"]) if SAMPLE else None
print(f"\n(sample activity for EXPLAINs: {ACT})")

p(
    "EXPLAIN base filtered scan (segments_details by activity_id)",
    lambda: sql_to_dict(
        f"""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT segment_id, activity_id, elapsed_duration_s
        FROM activities.segments_details
        WHERE activity_id = {ACT}
        """
    ),
)

p(
    "EXPLAIN scoped laterals (per-segment best/prior/count/p10 for one activity)",
    lambda: sql_to_dict(
        f"""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT sd.segment_id, sd.activity_id, sd.elapsed_duration_s,
               (SELECT min(x.elapsed_duration_s)
                  FROM activities.segments_details x
                 WHERE x.segment_id = sd.segment_id
                   AND x.activity_id <> sd.activity_id) AS best_excl_s,
               (SELECT count(*) + 1
                  FROM activities.segments_details y
                 WHERE y.segment_id = sd.segment_id
                   AND y.elapsed_duration_s < sd.elapsed_duration_s) AS all_time_rank,
               (SELECT count(*)
                  FROM activities.segments_details z
                 WHERE z.segment_id = sd.segment_id) AS total_attempts,
               (SELECT avg(w.avg_cadence)
                  FROM (SELECT avg_cadence
                          FROM activities.segments_details v
                         WHERE v.segment_id = sd.segment_id
                           AND v.start_time_utc < sd.start_time_utc
                         ORDER BY v.start_time_utc DESC
                         LIMIT 10) w) AS p10_cadence
          FROM activities.segments_details sd
         WHERE sd.activity_id = {ACT}
        """
    ),
)

p(
    "EXPLAIN HR percentile for one activity",
    lambda: sql_to_dict(
        f"""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY heartrate_bpm) AS hr_median,
               max(heartrate_bpm) AS hr_max
          FROM activities.activity_details
         WHERE activity_id = {ACT} AND heartrate_bpm IS NOT NULL
        """
    ),
)
