"""T09 charts EXPLAIN (read-only): confirm the narrowed aggregate keeps the
(activity_id, ts_utc) PK prefix, with no seq scan and no work_mem spill."""
from backend_functions.database_functions import sql_to_dict

SQL = """
    EXPLAIN (ANALYZE, BUFFERS)
    WITH pre_aggs AS (
        SELECT activity_id,
               floor(elapsed_duration_s / 60) AS minute_of_activity,
               round(avg(heartrate_bpm), 1)   AS heartrate_bpm,
               round(avg(elevation_m), 0)     AS elevation_m,
               max(distance_mm)               AS distance_mm
        FROM activities.activity_details
        WHERE activity_id = %s AND elapsed_duration_s IS NOT NULL
        GROUP BY activity_id, minute_of_activity
    )
    SELECT
        heartrate_bpm,
        elevation_m,
        round(distance_mm / 1000.0, 1) AS distance_m
    FROM pre_aggs
    ORDER BY minute_of_activity ASC
"""

rows = sql_to_dict(SQL, (24663109355,))
plan = "\n".join(r["QUERY PLAN"] for r in rows)
print(plan)
assert "Seq Scan" not in plan, "seq scan on hot path"
assert "Sort Method" in plan and "external merge" not in plan, "work_mem spill detected"
print("EXPLAIN_OK: no seq scan, no spill")
