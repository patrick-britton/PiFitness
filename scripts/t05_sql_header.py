"""004-005 T05 header comment + monotonic CTE text for build_004_005_t05_sql.py."""

OUT_NAME = '004-005_T05_resample_fix.sql'

HEADER = """-- ============================================================
-- 004-005 T05 / Bug 004-005-1 corrective SQL (HUMAN-APPLIED)
-- Procedure: activities.resample_activity_to_distance
-- ============================================================
-- Root cause (T01): INSERT source builds segment_bounds ordered by
-- elapsed_duration_s and joins each grid meter to every segment whose
-- [d1, d2) range contains it. Non-monotonic distance_mm (backwards
-- steps: 3262162962 x3, 3259650230 x1, 3252286731 x1) and repeated
-- values (GPS drift/pauses, up to 6 rows at one distance_mm) make
-- ranges overlap, so one meter matches two segments and the single
-- INSERT proposes the same (activity_id, distance_m) twice -> PK
-- activity_details_distance_pkey raises "cannot affect row a 2nd time".
-- Proof (scratch_004_005_resample_fix.py, SELECT-only): live shape
-- yields 759/15/303 duplicate meters on the 3 queue activities;
-- monotonic-frontier shape yields 0 on all three.
--
-- Fix: monotonic_base keeps rows whose distance_mm strictly exceeds
-- every earlier row in time order (tiebreak elapsed, distance), so
-- ranges are disjoint and each meter matches at most one segment.
-- f stays in [0,1]; d2=d1 guard retained; all else byte-identical to
-- live body oid 19690 (dumped 2026-10-07 via pg_get_functiondef).
--
-- Post-apply human verify:
--   CALL activities.resample_activity_to_distance(3262162962);
--   CALL activities.resample_activity_to_distance(3262162962);
--   CALL activities.resample_activity_to_distance(3259650230);
--   CALL activities.resample_activity_to_distance(3252286731);
-- Rollback: snapshot pg_get_functiondef output before applying;
-- re-apply that text to revert. Same target table/PK, no migration.
-- ============================================================
"""

MONOTONIC_CTE = """
        -- 004-005 T05 monotonic-distance frontier (see header).
        , monotonic_base as (
            SELECT * FROM (
                SELECT nb.*,
                       MAX(nb.distance_mm) OVER (
                           ORDER BY nb.elapsed_duration_s, nb.distance_mm
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                       ) AS prev_max_distance_mm
                FROM normalized_base nb
            ) frontier
            WHERE prev_max_distance_mm IS NULL
               OR distance_mm > prev_max_distance_mm
        )
"""
