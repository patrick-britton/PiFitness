# PiFitness Backlog

Out-of-scope findings and deferred enhancements. Agents append new items at the bottom with `python tools/workflow/fd.py backlog add <item>`.

| ID | Item | Source | Suggested Phase | Size | Status | Phase |
|---|---|---|---|---|---|---|
| B27 | Scope segment matching procedures to current activity ID | fix-activity-processing-fixes | Backlog | Small | Open | - |
| B28 | Optimize SQL non-equi join in resample_activity_to_distance using range indexing or LATERAL lookups | fix-activity-processing-fixes | Backlog | Medium | Open | - |
| B29 | Vectorize linear distance resampling in Python using NumPy and PostgreSQL binary COPY | fix-activity-processing-fixes | Backlog | Large | Open | - |
| B30 | Consolidate elevation smoothing and batch updates in smoothing.py | fix-activity-processing-fixes | Backlog | Small | Open | - |

---

## Detailed Specifications for Backlog Items

### B27: Scope Segment Matching Procedures to Current Activity ID

- **Surface:** Backend / Database (`backend/pifitness/activities/smoothing.py`, PostgreSQL stored procedures)
- **Priority / Estimated Impact:** P1 — High Impact (saves ~10s–25s per processed activity)
- **Target Files & Locations:**
  - `backend/pifitness/activities/smoothing.py:261-285`
  - `docs/lineage/defs/procedure.activities.segment_matches_all_polygon.sql`
  - `docs/lineage/defs/procedure.activities.segment_matches_all_hausdorff.sql`
  - `docs/lineage/defs/procedure.activities.segment_matches_all_freschet.sql`
  - `docs/lineage/defs/procedure.activities.segment_matching_mass_confirmation.sql`
- **Root Cause & Current Code:**
  In `smoothing.py`, the segment matching sub-steps accept `int_a: int` but discard the argument to invoke global stored procedures:
  ```python
  def call_segment_polygon_match(int_a: int):
      _run_sql_step("CALL activities.segment_matches_all_polygon();")

  def call_segment_hausdorff_match(int_a: int):
      _run_sql_step("CALL activities.segment_matches_all_hausdorff();")

  def call_segment_frechet_match(int_a: int):
      _run_sql_step("CALL activities.segment_matches_all_freschet();")
  ```
  Each `_all_` procedure iterates through a cursor:
  ```sql
  FOR v_activity_id IN SELECT DISTINCT activity_id FROM activities.temp_segment_matches_downselect WHERE activity_id IS NOT NULL LOOP
      CALL activities.update_segment_matches_polygon(v_activity_id);
  END LOOP;
  ```
  Consequently, on every new activity processed, the pipeline re-computes polygon intersections, Hausdorff distances, and Fréchet metrics across *every* activity currently present in `temp_segment_matches_downselect`. This causes unbounded $O(N)$ spatial computations that severely degrade performance on the Raspberry Pi.
- **Detailed Implementation Instructions:**
  1. In `backend/pifitness/activities/smoothing.py`, update `call_segment_polygon_match`, `call_segment_hausdorff_match`, and `call_segment_frechet_match` to bypass the `_all_` wrapper and directly call the existing per-activity stored procedures:
     ```python
     def call_segment_polygon_match(int_a: int):
         _run_sql_step(f"CALL activities.update_segment_matches_polygon({int_a});")

     def call_segment_hausdorff_match(int_a: int):
         _run_sql_step(f"CALL activities.update_segment_matches_hausdorff({int_a});")

     def call_segment_frechet_match(int_a: int):
         _run_sql_step(f"CALL activities.update_segment_matches_freschet({int_a});")
     ```
  2. For `segment_matching_mass_confirmation(IN p_approval_phase integer)`:
     Update the procedure definition (or create an overload `segment_matching_mass_confirmation(IN p_approval_phase integer, IN p_activity_id bigint DEFAULT NULL)`) to filter candidate segment efforts by `scaa.activity_id = p_activity_id` when `p_activity_id IS NOT NULL`.
  3. In `smoothing.py`, pass `int_a` into `call_segment_mass_confirm_1`, `2`, and `3`.
- **Invariants & Constraints:**
  - Must not break matching for activities currently in progress.
  - Matches must continue to populate `activities.segment_matches` identically.
- **Verification Plan:**
  - Execute a single activity post-processing run. Verify `activities.segment_matches` contains the expected matched segments for that activity. Verify runtime for segment matching steps drops significantly.

---

### B28: Optimize SQL Non-Equi Join in `resample_activity_to_distance`

- **Surface:** Database (PostgreSQL / PostGIS stored procedures)
- **Priority / Estimated Impact:** P1 — High Impact (reduces DB resampling time by 2x–3x)
- **Target Files & Locations:**
  - `docs/lineage/defs/procedure.activities.resample_activity_to_distance.sql`
- **Root Cause & Current Code:**
  `activities.resample_activity_to_distance` builds a 1-meter grid (`distance_grid`) using `generate_series(0, (SELECT MAX(distance_mm) / 1000 FROM monotonic_base))` (producing 5,000 to 42,000+ rows) and matches it against segment boundaries using a non-equi join:
  ```sql
  FROM distance_grid g
  JOIN segment_bounds s
    ON g.dist_m >= s.d1
   AND (g.dist_m < s.d2 OR (s.d2 IS NULL AND g.dist_m = s.d1))
  ```
  PostgreSQL cannot execute a hash join on range inequalities. The query planner executes either a nested loop with repeated index/seq scans or an expensive merge join across large intermediate CTE result sets, evaluating multiple expressions per meter mark.
- **Detailed Implementation Instructions:**
  1. **Option A (Ordered LATERAL lookup):**
     Since `monotonic_base` guarantees strictly monotonic distance ordering (`distance_mm > prev_max_distance_mm`), replace the non-equi join with a `LATERAL` lookup:
     ```sql
     FROM distance_grid g
     CROSS JOIN LATERAL (
         SELECT *
         FROM segment_bounds s
         WHERE s.d1 <= g.dist_m
         ORDER BY s.d1 DESC
         LIMIT 1
     ) s
     ```
     Ensure `s.d1` is indexed or materialized in a temporary table with an index on `(d1 DESC)` if necessary.
  2. **Option B (PostgreSQL `numrange` with GiST):**
     Construct `numrange(s.d1, COALESCE(s.d2, s.d1 + 0.0001), '[)') AS seg_range` in `segment_bounds` and join using the contains operator `seg_range @> g.dist_m::numeric`.
  3. Maintain the 1-meter output resolution invariant strictly: `activities.activity_details_distance` must output every meter point from 0 to `max_dist_m` with identical interpolated coordinate fidelity.
- **Invariants & Constraints:**
  - **Fidelity Invariant:** Resolution MUST remain 1 meter. No decimation to 5m/10m.
  - Linear interpolation formula: `f = (g.dist_m - s.d1) / (s.d2 - s.d1)` must evaluate identically.
- **Verification Plan:**
  - Compare the output of `activities.activity_details_distance` for a benchmark activity before and after optimization. Verify row counts and values match to 4 decimal places. Run `EXPLAIN (ANALYZE, BUFFERS)` to confirm index/lateral plan efficiency.

---

### B29: Vectorize Linear Distance Resampling in Python using NumPy and PostgreSQL Binary COPY

- **Surface:** Backend / Database (`backend/pifitness/activities/smoothing.py`, `backend/pifitness/activities/queries.py`)
- **Priority / Estimated Impact:** P2 — Transformative (reduces resampling from ~15s–20s to <1s)
- **Target Files & Locations:**
  - `backend/pifitness/activities/smoothing.py:168-212`
  - `backend/pifitness/activities/queries.py`
- **Root Cause & Current Code:**
  Executing procedural linear interpolation across 15+ metrics for 40,000 points row-by-row inside PostgreSQL generates massive CPU load, temporary disk storage, and WAL write churn. Python's scientific stack (NumPy) can perform 1D linear interpolation (`numpy.interp`) over contiguous memory blocks in C at multi-gigabyte/sec throughput.
- **Detailed Implementation Instructions:**
  1. Fetch raw activity track points from `activities.activity_details` for `p_activity_id` into Python (only 1,500 to 4,000 original GPS points).
  2. Filter to monotonic distance frontier in Python:
     ```python
     distances_m = raw_df['distance_mm'].to_numpy() / 1000.0
     monotonic_mask = np.maximum.accumulate(distances_m) == distances_m
     # Ensure strictly increasing for interpolation:
     strict_mask = np.insert(np.diff(distances_m) > 0, 0, True) & monotonic_mask
     ```
  3. Create target 1-meter distance array:
     ```python
     max_dist_m = int(distances_m[strict_mask][-1])
     grid_m = np.arange(0, max_dist_m + 1, dtype=np.int32)
     ```
  4. Perform vectorized interpolation for all channels:
     ```python
     lat_resampled = np.interp(grid_m, orig_dist, orig_lat)
     lon_resampled = np.interp(grid_m, orig_dist, orig_lon)
     ele_resampled = np.interp(grid_m, orig_dist, orig_ele)
     hr_resampled = np.round(np.interp(grid_m, orig_dist, orig_hr)).astype(np.int16)
     speed_resampled = np.round(np.interp(grid_m, orig_dist, orig_spd)).astype(np.int64)
     # Repeat for cadence, temp, vert metrics, etc.
     ```
  5. Bulk stream into PostgreSQL:
     - Clear existing rows: `DELETE FROM activities.activity_details_distance WHERE activity_id = %s`.
     - Use `psycopg2`'s `cursor.copy_expert` with CSV/binary format to stream interpolated records directly into `activities.activity_details_distance`.
     - Compute spatial geometries via `ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)` in batch or during `COPY`.
  6. Call `activities.assign_elevation_reference_distance(p_activity_id)` after bulk insertion completes.
- **Invariants & Constraints:**
  - Must handle edge cases (activities with zero distance or identical consecutive timestamps).
  - Types and nullability must match table schema for `activity_details_distance`.
- **Verification Plan:**
  - Create a regression test verifying that all columns in `activities.activity_details_distance` produced by Python match the SQL stored procedure's outputs within 0.01% numerical tolerance. Benchmark runtime (<1.5s target).

---

### B30: Consolidate Elevation Smoothing and Batch Updates in `smoothing.py`

- **Surface:** Backend (`backend/pifitness/activities/smoothing.py`)
- **Priority / Estimated Impact:** P2 — Medium Impact (saves 5s–10s of repeated DB I/O)
- **Target Files & Locations:**
  - `backend/pifitness/activities/smoothing.py:60-140, 313-326`
- **Root Cause & Current Code:**
  In `activity_post_processing_steps`, smoothing is split into two separate passes:
  - `smooth_elevation_python_distance` (sets `elevation_m_smooth`)
  - `smooth_elevation_python_reference` (sets `elevation_reference`)
  Each step calls `process_all_activities([int_a], ...)`. This executes:
  1. `SELECT activity_id, distance_m, COALESCE(...) FROM activities.activity_details_distance WHERE activity_id = %s ORDER BY distance_m;`
  2. Data smoothing calculations in Python.
  3. `execute_values(cur, query, smoothed_records, page_size=1000)`: loops through chunks of 1,000 records, sending dozens of individual SQL `UPDATE ... FROM (VALUES ...)` statements to PostgreSQL.
  Executing this sequence twice doubles round-trip database queries, network latency, and index update lock contention.
- **Detailed Implementation Instructions:**
  1. Consolidate into a unified function `smooth_elevation_python_all(activity_id: int)`:
     - Fetch `(activity_id, distance_m, elevation_m, elevation_m_smooth, elevation_reference)` once.
     - Calculate both smoothed elevation and reference elevation series in memory.
     - Execute a single batched update setting both columns simultaneously:
       ```sql
       UPDATE activities.activity_details_distance AS t
       SET elevation_m_smooth = v.elevation_m_smooth,
           elevation_reference = v.elevation_reference
       FROM (VALUES %s) AS v(activity_id, distance_m, elevation_m_smooth, elevation_reference)
       WHERE t.activity_id = v.activity_id::bigint
         AND t.distance_m = v.distance_m::int;
       ```
  2. In `activity_post_processing_steps`, if both UI substeps are preserved for visual progress bar tracking, have `smooth_elevation_python_distance` perform the combined calculation and mark `smooth_elevation_python_reference` as an instantaneous completion / no-op.
- **Invariants & Constraints:**
  - Must not modify smoothing mathematical algorithms (must produce identical smoothed curve outputs).
  - Must maintain transaction safety and connection handling.
- **Verification Plan:**
  - Verify that both `elevation_m_smooth` and `elevation_reference` columns in `activities.activity_details_distance` are populated correctly. Run `test_process_generator_events.py` to confirm generator compatibility.
