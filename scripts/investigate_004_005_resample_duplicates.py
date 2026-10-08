#!/usr/bin/env python3
"""
004-005 T01 / Bug 004-005-1 — forensics: why does activities.resample_activity_to_distance
raise "ON CONFLICT DO UPDATE command cannot affect row a second time" on
(activity_id, distance_m)?
================================================================================

Read-only. Evidence gathered:

1. live function body via pg_get_functiondef (repo docs are stale — the cached
   schema_documentation.json variant targets activity_resampled_1m, not
   activity_details_distance);
2. full constraint + index set on activities.activity_details_distance (what is
   the ON CONFLICT arbiter, and did a restore change it?);
3. candidate affected activities: the id from the error log (20678335982) plus
   everything currently in activities.activity_processing_queue;
4. source-row probes on activities.activity_details for those candidates:
   duplicate (activity_id, elapsed_duration_s), duplicate distance_mm, and
   non-monotonic distance_mm ordering — the three ways the grid JOIN in the
   failing INSERT can propose the same (activity_id, distance_m) twice;
5. exact replication of the failing INSERT's source as a SELECT, counting
   duplicate target_dist values per activity.

No writes: every statement is a SELECT.
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / 'backend' / '.env')

from backend_functions.database_functions import sql_to_dict

ERROR_LOG_ACTIVITY = 20678335982  # id in the WHERE comment of the logged failure


def section(title):
    print(f"\n=== {title} ===")


def q(sql, params=None):
    return sql_to_dict(sql, params)


def live_function_body():
    section("1. live activities.resample_activity_to_distance body")
    rows = q(
        """
        SELECT n.nspname AS schema, p.proname, p.oid,
               pg_get_functiondef(p.oid) AS def
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'resample_activity_to_distance'
        ORDER BY 1, 2
        """
    )
    if not rows:
        print("  NOT FOUND — function missing from live database")
        return None
    for r in rows:
        print(f"  -- {r['schema']}.{r['proname']} (oid {r['oid']})")
        print(r['def'])
    return rows[0]['def'] if len(rows) == 1 else None


def table_constraints_and_indexes():
    section("2. constraints + indexes on activities.activity_details_distance")
    cons = q(
        """
        SELECT c.conname, c.contype, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'activities' AND r.relname = 'activity_details_distance'
        ORDER BY c.contype, c.conname
        """
    )
    print(f"  constraints ({len(cons)}):")
    for c in cons:
        print(f"    [{c['contype']}] {c['conname']}: {c['def']}")
    idx = q(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'activities' AND tablename = 'activity_details_distance'
        ORDER BY indexname
        """
    )
    print(f"  indexes ({len(idx)}):")
    for i in idx:
        print(f"    {i['indexdef']}")


def candidate_activities():
    section("3. candidate activities (error-log id + processing queue)")
    ids = {ERROR_LOG_ACTIVITY}
    try:
        rows = q(
            "SELECT DISTINCT activity_id FROM activities.activity_processing_queue "
            "ORDER BY activity_id DESC"
        )
        ids.update(int(r['activity_id']) for r in rows)
    except Exception as e:
        print(f"  queue read failed: {e}")
    print(f"  candidates: {sorted(ids, reverse=True)}")
    return sorted(ids, reverse=True)


def source_row_probes(ids):
    section("4. source-row probes on activities.activity_details")
    ph = ",".join(["%s"] * len(ids))

    dup_ts = q(
        f"""
        SELECT activity_id, elapsed_duration_s, count(*) AS n
        FROM activities.activity_details
        WHERE activity_id IN ({ph})
        GROUP BY 1, 2
        HAVING count(*) > 1
        ORDER BY 1, 2
        LIMIT 50
        """, ids)
    print(f"  duplicate (activity_id, elapsed_duration_s) rows: {len(dup_ts)}")
    for r in dup_ts[:10]:
        print(f"    act {r['activity_id']} @ {r['elapsed_duration_s']}s x {r['n']}")

    dup_dist = q(
        f"""
        SELECT activity_id, distance_mm, count(*) AS n
        FROM activities.activity_details
        WHERE activity_id IN ({ph})
        GROUP BY 1, 2
        HAVING count(*) > 1
        ORDER BY 1, 2
        LIMIT 50
        """, ids)
    print(f"  duplicate (activity_id, distance_mm) rows: {len(dup_dist)}")
    for r in dup_dist[:10]:
        print(f"    act {r['activity_id']} @ {r['distance_mm']}mm x {r['n']}")

    nonmono = q(
        f"""
        SELECT activity_id, count(*) AS backwards_steps
        FROM (
            SELECT activity_id, elapsed_duration_s, distance_mm,
                   LAG(distance_mm) OVER (PARTITION BY activity_id
                                          ORDER BY elapsed_duration_s) AS prev_mm
            FROM activities.activity_details
            WHERE activity_id IN ({ph})
        ) x
        WHERE prev_mm IS NOT NULL AND distance_mm < prev_mm
        GROUP BY 1
        ORDER BY 1
        """, ids)
    print("  non-monotonic distance_mm (ordered by elapsed_duration_s):")
    for r in nonmono:
        print(f"    act {r['activity_id']}: {r['backwards_steps']} backwards steps")
    if not nonmono:
        print("    none")


def grid_duplicates(ids):
    """Replicate the failing INSERT source as a SELECT; count duplicate
    target_dist values — direct proof of the ON CONFLICT violation."""
    section("5. duplicate target_dist in the failing INSERT source (replicated as SELECT)")
    for act in ids:
        try:
            rows = q(
                """
                WITH base AS (
                    SELECT activity_type_id, activity_id, distance_mm,
                           elapsed_duration_s, latitude, longitude,
                           elevation_m_smooth AS elevation_m, heartrate_bpm,
                           cadence_spm, air_temp_c, speed_mmps, vertical_speed_mps,
                           vert_oscillation_cm, stride_length_cm, vert_ratio,
                           ground_contact_time_ms, ground_contact_balance_left,
                           performance_condition
                    FROM activities.activity_details
                    WHERE activity_id = %s
                ),
                normalized_base AS (
                    SELECT activity_type_id, activity_id,
                           distance_mm - (SELECT MIN(distance_mm) FROM base) AS distance_mm,
                           elapsed_duration_s, latitude, longitude, elevation_m,
                           heartrate_bpm, cadence_spm, air_temp_c, speed_mmps,
                           vertical_speed_mps, vert_oscillation_cm, stride_length_cm,
                           vert_ratio, ground_contact_time_ms,
                           ground_contact_balance_left, performance_condition
                    FROM base
                ),
                segment_bounds AS (
                    SELECT activity_type_id, activity_id,
                           (distance_mm / 1000.0) AS d1,
                           LEAD(distance_mm / 1000.0) OVER (ORDER BY elapsed_duration_s) AS d2,
                           (elapsed_duration_s * 1000) AS t1,
                           LEAD(elapsed_duration_s * 1000) OVER (ORDER BY elapsed_duration_s) AS t2,
                           latitude AS lat1, LEAD(latitude) OVER (ORDER BY elapsed_duration_s) AS lat2,
                           longitude AS lon1, LEAD(longitude) OVER (ORDER BY elapsed_duration_s) AS lon2,
                           elevation_m::float AS ele1, LEAD(elevation_m::float) OVER (ORDER BY elapsed_duration_s) AS ele2,
                           heartrate_bpm::float AS hr1, LEAD(heartrate_bpm::float) OVER (ORDER BY elapsed_duration_s) AS hr2,
                           cadence_spm::float AS cad1, LEAD(cadence_spm::float) OVER (ORDER BY elapsed_duration_s) AS cad2,
                           air_temp_c::float AS tmp1, LEAD(air_temp_c::float) OVER (ORDER BY elapsed_duration_s) AS tmp2,
                           speed_mmps::float AS spd1, LEAD(speed_mmps::float) OVER (ORDER BY elapsed_duration_s) AS spd2,
                           vertical_speed_mps::float AS v_spd1, LEAD(vertical_speed_mps::float) OVER (ORDER BY elapsed_duration_s) AS v_spd2,
                           vert_oscillation_cm::float AS v_osc1, LEAD(vert_oscillation_cm::float) OVER (ORDER BY elapsed_duration_s) AS v_osc2,
                           stride_length_cm::float AS strd1, LEAD(stride_length_cm::float) OVER (ORDER BY elapsed_duration_s) AS strd2,
                           vert_ratio::float AS v_rat1, LEAD(vert_ratio::float) OVER (ORDER BY elapsed_duration_s) AS v_rat2,
                           ground_contact_time_ms::float AS gct1, LEAD(ground_contact_time_ms::float) OVER (ORDER BY elapsed_duration_s) AS gct2,
                           ground_contact_balance_left::float AS gcb1, LEAD(ground_contact_balance_left::float) OVER (ORDER BY elapsed_duration_s) AS gcb2,
                           performance_condition::float AS perf1, LEAD(performance_condition::float) OVER (ORDER BY elapsed_duration_s) AS perf2
                    FROM normalized_base
                ),
                distance_grid AS (
                    SELECT generate_series(0, (SELECT MAX(distance_mm)::int / 1000 FROM normalized_base)) AS dist_m
                ),
                interpolated AS (
                    SELECT s.*, g.dist_m AS target_dist,
                           CASE
                               WHEN s.d2 IS NULL THEN 0
                               WHEN s.d2 = s.d1 THEN 0
                               ELSE (g.dist_m - s.d1) / (s.d2 - s.d1)
                           END AS f
                    FROM distance_grid g
                    JOIN segment_bounds s
                      ON g.dist_m >= s.d1
                     AND (g.dist_m < s.d2 OR (s.d2 IS NULL AND g.dist_m = s.d1))
                )
                SELECT target_dist, count(*) AS n
                FROM interpolated
                GROUP BY 1
                HAVING count(*) > 1
                ORDER BY 1
                LIMIT 20
                """, (act,))
            if rows:
                print(f"  act {act}: {len(rows)}+ duplicate target_dist values "
                      f"(first: {[(float(r['target_dist']), r['n']) for r in rows[:5]]})")
            else:
                print(f"  act {act}: no duplicate target_dist - source is clean")
        except Exception as e:
            print(f"  act {act}: probe failed: {e}")


def timing_probes(ids):
    """Was this a recent (restore-window) regression? When did the queue
    entries and activities originate, and what do duplicate-distance rows
    look like (spread timestamps = drift, identical = double import)?"""
    section("6. timing probes (restore-window hypothesis)")
    ph = ",".join(["%s"] * len(ids))
    qrows = q(
        f"""
        SELECT activity_id, min(created_at_utc) AS queued_at, count(*) AS n
        FROM activities.activity_processing_queue
        WHERE activity_id IN ({ph})
        GROUP BY 1 ORDER BY 1
        """, ids)
    print("  queue entries:")
    for r in qrows:
        print(f"    act {r['activity_id']}: queued {r['queued_at']} x{r['n']}")
    arows = q(
        f"""
        SELECT activity_id, min(start_time_utc) AS start_time
        FROM activities.activities
        WHERE activity_id IN ({ph})
        GROUP BY 1 ORDER BY 1
        """, ids)
    print("  activity start times:")
    for r in arows:
        print(f"    act {r['activity_id']}: {r['start_time']}")
    samples = q(
        f"""
        SELECT activity_id, distance_mm, count(*) AS n,
               min(ts_utc) AS first_ts, max(ts_utc) AS last_ts,
               count(DISTINCT ts_utc) AS distinct_ts
        FROM activities.activity_details
        WHERE activity_id IN ({ph})
        GROUP BY 1, 2
        HAVING count(*) > 1
        ORDER BY 1, 3 DESC
        LIMIT 12
        """, ids)
    print("  duplicate-distance groups (timestamp spread):")
    for r in samples:
        spread = "SAME-TS(double import)" if r['distinct_ts'] == 1 else "spread(GPS drift/pauses)"
        print(f"    act {r['activity_id']} @ {r['distance_mm']}mm x{r['n']} "
              f"[{r['first_ts']} .. {r['last_ts']}] {spread}")


def main():
    live_function_body()
    table_constraints_and_indexes()
    ids = candidate_activities()
    source_row_probes(ids)
    grid_duplicates(ids)
    timing_probes(ids)
    section("done - read-only; no writes issued")


if __name__ == '__main__':
    main()


