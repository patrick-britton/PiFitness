import time

import numpy as np
from scipy.signal import savgol_filter
from psycopg2.extras import execute_values

from backend_functions.database_functions import get_conn, sql_to_list, qec, one_sql_result
from backend_functions.logging_functions import start_timer, elapsed_ms, log_app_event


def apply_savgol_filter(records, polyorder=3, is_time=False):
    """
    Applies the Savitzky-Golay filter to a list of activity records.
    records format: [(activity_id, elapsed_duration_s, elevation_m), ...]
    """
    if is_time:
        window_length = 31
    else:
        window_length = 101

    # Edge case: If the activity is shorter than the window length
    if len(records) < window_length:
        window_length = len(records) if len(records) % 2 != 0 else len(records) - 1
        if window_length <= polyorder:
            # Too short to smooth, return as-is
            return [(r[0], r[1], r[2]) for r in records]

    # Extract elevations into a numpy array for C-level speed
    elevations = np.array([r[2] for r in records], dtype=float)

    # Apply the filter
    smoothed_elev = savgol_filter(elevations, window_length, polyorder)

    # Repackage into a list of tuples with the new smoothed value
    # Format: [(activity_id, elapsed_duration_s, elevation_m_smooth), ...]
    smoothed_records = [
        (records[i][0], records[i][1], float(smoothed_elev[i]))
        for i in range(len(records))
    ]

    return smoothed_records


def update_smoothed_elevation(conn, smoothed_records, field_name, is_time=False):
    """
    Efficiently merges the smoothed elevations back into the database.
    """
    # The 'AS v(...)' part creates a temporary virtual table in memory
    # that we join against the actual activity_details table.
    if is_time:
        table_appendix = ''
        pk_field = 'elapsed_duration_s'
    else:
        table_appendix = '_distance'
        pk_field = 'distance_m'

    query = f"""
        UPDATE activities.activity_details{table_appendix} AS t
        SET {field_name} = v.elevation_m_smooth
        FROM (VALUES %s) AS v(activity_id, {pk_field}, elevation_m_smooth)
        WHERE t.activity_id = v.activity_id::bigint 
          AND t.{pk_field} = v.{pk_field}::int;
    """

    with conn.cursor() as cur:
        # page_size=1000 chunks the update within the single query to protect Pi's RAM
        execute_values(cur, query, smoothed_records, page_size=1000)

    conn.commit()


def process_all_activities(activity_ids, is_reference=False, is_time=False):

    start_time = time.time()

    # Connect to the database
    conn = get_conn()


    if is_reference:
        field_name = 'elevation_reference'
    else:
        field_name = 'elevation_m_smooth'

    if is_time:
        table_appendix=''
        order_field = 'elapsed_duration_s'
    else:
        table_appendix='_distance'
        order_field = 'distance_m'

    for index, act_id in enumerate(activity_ids, start=1):
        try:
            with conn.cursor() as cur:
                # 2. Fetch the raw data for this activity
                # We use COALESCE to ensure we don't pass NULLs into the math function
                cur.execute(f"""
                    SELECT activity_id, {order_field}, COALESCE({field_name}, 0) 
                    FROM activities.activity_details{table_appendix}
                    WHERE activity_id = %s 
                    ORDER BY {order_field};
                """, (act_id,))

                raw_records = cur.fetchall()

            if not raw_records:
                continue

            # 3. Apply the math
            smoothed_records = apply_savgol_filter(raw_records, is_time=is_time)

            # 4. Push it back to the database
            update_smoothed_elevation(conn, smoothed_records, field_name, is_time=is_time)


        except Exception as e:
            print(f"\nERROR on activity {act_id}: {e}")
            conn.rollback()  # Important: reset the transaction block if an error occurs
            raise e  # Propagate the error so atomic step functions can fail properly

    conn.close()

    total_time = time.time() - start_time


# ---------------------------------------------------------------------------
# Helper: Run SQL via qec and raise on error to prevent silent failures
# ---------------------------------------------------------------------------

def _run_sql_step(sql: str, params=None):
    res = qec(sql, params)
    if res is not None:
        raise RuntimeError(f"SQL Execution Error: {res[0]}")


# ---------------------------------------------------------------------------
# Atomic Step Functions for Activity Post Processing
# ---------------------------------------------------------------------------

def insert_heartrate_for_activity(int_a: int):
    hr_sql = f"""INSERT INTO health.heartrate_raw (ts_utc, heartrate_bpm)
            SELECT ts_utc, heartrate_bpm FROM activities.activity_details
            WHERE activity_id = {int_a} AND heartrate_bpm IS NOT NULL
            ON CONFLICT(ts_utc) DO UPDATE SET heartrate_bpm = EXCLUDED.heartrate_bpm
            WHERE health.heartrate_raw.heartrate_bpm IS DISTINCT FROM EXCLUDED.heartrate_bpm;"""
    _run_sql_step(hr_sql)


def call_assign_elevation_reference_time(int_a: int):
    _run_sql_step(f"CALL activities.assign_elevation_reference_time({int_a});")


def call_smooth_elevation_spikes_by_time(int_a: int):
    _run_sql_step(f"CALL activities.smooth_elevation_spikes_by_time({int_a});")


def smooth_elevation_python_time(int_a: int):
    process_all_activities([int_a], is_reference=False, is_time=True)


def call_update_elevation_reference_by_time(int_a: int):
    _run_sql_step(f"CALL activities.update_elevation_reference_by_time({int_a});")


def call_resample_activity_to_distance(int_a: int):
    _run_sql_step(f"CALL activities.resample_activity_to_distance({int_a});")


def call_smooth_elevation_spikes_by_distance(int_a: int):
    _run_sql_step(f"CALL activities.smooth_elevation_spikes_by_distance({int_a});")


def smooth_elevation_python_distance(int_a: int):
    process_all_activities([int_a], is_reference=False, is_time=False)


def smooth_elevation_python_reference(int_a: int):
    process_all_activities([int_a], is_reference=True, is_time=False)


def call_update_elevation_reference_by_distance(int_a: int):
    _run_sql_step(f"CALL activities.update_elevation_reference_by_distance({int_a});")


def build_activity_path(int_a: int):
    path_sql = f"""UPDATE activities.activities
            SET activity_path = sub.path, is_downloaded=TRUE
            FROM (
                     SELECT
                         rd.activity_id,
                         ST_SetSRID(
                                 ST_MakeLine(
                                         ST_MakePoint(longitude, latitude, elevation_m_smooth, distance_m)
                                         ORDER BY distance_m ASC
                                 ),
                                 4326
                         ) AS path
                     FROM activities.activity_details_distance rd
                     WHERE activity_id = {int_a} 
                     GROUP BY rd.activity_id
                 ) AS sub
            WHERE activities.activity_id = sub.activity_id;"""
    _run_sql_step(path_sql)


# Segment matching steps:
def call_segment_match_segments(int_a: int):
    _run_sql_step(f"CALL activities.segment_matching_match_segments({int_a});")


def call_segment_pair_generation(int_a: int):
    _run_sql_step(f"CALL activities.segment_matching_activity_pair_generation({int_a});")


def call_segment_polygon_match(int_a: int):
    _run_sql_step("CALL activities.segment_matches_all_polygon();")


def call_segment_mass_confirm_1(int_a: int):
    _run_sql_step("CALL activities.segment_matching_mass_confirmation(1);")


def call_segment_hausdorff_match(int_a: int):
    _run_sql_step("CALL activities.segment_matches_all_hausdorff();")


def call_segment_mass_confirm_2(int_a: int):
    _run_sql_step("CALL activities.segment_matching_mass_confirmation(2);")


def call_segment_frechet_match(int_a: int):
    _run_sql_step("CALL activities.segment_matches_all_freschet();")


def call_segment_mass_confirm_3(int_a: int):
    _run_sql_step("CALL activities.segment_matching_mass_confirmation(3);")


def call_segment_update_details(int_a: int):
    _run_sql_step(f"CALL staging.update_segment_details({int_a});")


def delete_queue_for_activity(int_a: int):
    _run_sql_step(f"DELETE FROM activities.activity_processing_queue WHERE activity_id = {int_a};")


# ---------------------------------------------------------------------------
# Generator Orchestrator yielding (step_id, elapsed_ms, error)
# ---------------------------------------------------------------------------

def activity_post_processing_steps(activity_id: int):
    """
    Generator that yields (step_id, elapsed_ms, error) for each sub-step of post processing for a single activity.
    """
    int_a = int(activity_id)

    # Elevation & Smoothing pipeline (runs for any activity type)
    steps = [
        ('insert_heartrate', insert_heartrate_for_activity),
        ('assign_elevation_reference_time', call_assign_elevation_reference_time),
        ('smooth_elevation_spikes_by_time', call_smooth_elevation_spikes_by_time),
        ('smooth_elevation_python_time', smooth_elevation_python_time),
        ('update_elevation_reference_by_time', call_update_elevation_reference_by_time),
        ('resample_activity_to_distance', call_resample_activity_to_distance),
        ('smooth_elevation_spikes_by_distance', call_smooth_elevation_spikes_by_distance),
        ('smooth_elevation_python_distance', smooth_elevation_python_distance),
        ('smooth_elevation_python_reference', smooth_elevation_python_reference),
        ('update_elevation_reference_by_distance', call_update_elevation_reference_by_distance),
        ('build_activity_path', build_activity_path),
    ]

    for step_id, fn in steps:
        t0 = start_timer()
        error = None
        try:
            fn(int_a)
            try:
                log_app_event(cat='Task Executioner',
                              desc=f'Activity Post Processing Substep: {step_id} for {int_a}',
                              err=None,
                              data_event=None)
            except Exception as log_err:
                print(f"\nLOGGING ERROR after step {step_id} for {int_a}: {log_err}")
        except Exception as e:
            error = str(e)
            try:
                log_app_event(cat='Task Executioner',
                              desc=f'Activity Post Processing Substep Error: {step_id} for {int_a}',
                              err=error,
                              data_event=None)
            except Exception as log_err:
                print(f"\nLOGGING ERROR after step {step_id} for {int_a}: {log_err}")

        ms = elapsed_ms(t0)
        yield (step_id, ms, error)
        if error:
            return  # Halt pipeline on first error

    # Segment matching (running, trail_running, treadmill_running, walking, hiking only)
    # The DB layer enforces same-activity-type matching (segment_matching_match_segments:
    # "Rule 1: Match the activity_type_id of the target segment"), so widening this
    # gate to include walks/hikes is safe (OQ-6 / Bug T10-2 fix). Per the feature
    # owner: walk = 'walking'(9)/'hiking'(3); run = 'running'(1)/'trail_running'(4)/
    # 'treadmill_running'(18).
    do_segment_matching = False
    try:
        activity_type_row = one_sql_result(
            "SELECT activity_type_name FROM activities.activities WHERE activity_id = %s", (int_a,)
        )
        if activity_type_row and activity_type_row in (
            'running', 'trail_running', 'treadmill_running', 'walking', 'hiking',
        ):
            do_segment_matching = True
    except Exception as e:
        log_app_event(cat='Task Executioner',
                      desc=f'Activity Post Processing: failed to get activity type for {int_a}',
                      err=str(e),
                      data_event=None)

    if do_segment_matching:
        segment_steps = [
            ('segment_match_segments', call_segment_match_segments),
            ('segment_pair_generation', call_segment_pair_generation),
            ('segment_polygon_match', call_segment_polygon_match),
            ('segment_mass_confirm_1', call_segment_mass_confirm_1),
            ('segment_hausdorff_match', call_segment_hausdorff_match),
            ('segment_mass_confirm_2', call_segment_mass_confirm_2),
            ('segment_frechet_match', call_segment_frechet_match),
            ('segment_mass_confirm_3', call_segment_mass_confirm_3),
            ('segment_update_details', call_segment_update_details),
        ]

        for step_id, fn in segment_steps:
            t0 = start_timer()
            error = None
            try:
                fn(int_a)
                try:
                    log_app_event(cat='Task Executioner',
                                  desc=f'Activity Post Processing Substep: {step_id} for {int_a}',
                                  err=None,
                                  data_event=None)
                except Exception as log_err:
                    print(f"\nLOGGING ERROR after step {step_id} for {int_a}: {log_err}")
            except Exception as e:
                error = str(e)
                try:
                    log_app_event(cat='Task Executioner',
                                  desc=f'Activity Post Processing Substep Error: {step_id} for {int_a}',
                                  err=error,
                                  data_event=None)
                except Exception as log_err:
                    print(f"\nLOGGING ERROR after step {step_id} for {int_a}: {log_err}")

            ms = elapsed_ms(t0)
            yield (step_id, ms, error)
            if error:
                return  # Halt pipeline on first error

    # Delete queue entry on successful completion of all steps
    try:
        delete_queue_for_activity(int_a)
        log_app_event(cat='Task Executioner',
                      desc=f'Activity Post Processing Queue Deletion: {int_a}',
                      err=None,
                      data_event=None)
    except Exception as e:
        log_app_event(cat='Task Executioner',
                      desc=f'Activity Post Processing Queue Deletion Error: {int_a}',
                      err=str(e),
                      data_event=None)


# Backwards compatibility wrapper
def activity_post_processing(manual_list=None):
    if manual_list:
        activity_list = manual_list
    else:
        activity_list = sql_to_list(
            "SELECT DISTINCT activity_id from activities.activity_processing_queue order by activity_id desc LIMIT 5"
        )

    if not activity_list:
        log_app_event(cat='Task Executioner',
                      desc='Activity Post Processing: Early Exit with no activities',
                      err=None,
                      data_event=None)
        return True

    ac = len(activity_list)
    ctr = 1

    log_app_event(cat='Task Executioner',
                  desc=f'Activity Post Processing: List has {ac} activities.',
                  err=None,
                  data_event=None)

    for a in activity_list:
        int_a = int(a)
        t0 = start_timer()
        
        has_error = False
        for step_id, ms, error in activity_post_processing_steps(int_a):
            if error:
                has_error = True
                break
        
        if not has_error:
            print(f"{ctr}/{ac} | ID# {int_a} | {elapsed_ms(t0)} ms")
        else:
            print(f"{ctr}/{ac} | ID# {int_a} | FAILED")
        ctr += 1

    return True
