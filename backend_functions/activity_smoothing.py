import time

import numpy as np
from scipy.signal import savgol_filter
from psycopg2.extras import execute_values

from backend_functions.database_functions import get_conn, sql_to_list, qec, one_sql_result


def apply_savgol_filter(records, window_length=31, polyorder=3):
    """
    Applies the Savitzky-Golay filter to a list of activity records.
    records format: [(activity_id, elapsed_duration_s, elevation_m), ...]
    """
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


def update_smoothed_elevation(conn, smoothed_records, field_name):
    """
    Efficiently merges the smoothed elevations back into the database.
    """
    # The 'AS v(...)' part creates a temporary virtual table in memory
    # that we join against the actual activity_details table.
    query = f"""
        UPDATE activities.activity_details AS t
        SET {field_name} = v.elevation_m_smooth
        FROM (VALUES %s) AS v(activity_id, elapsed_duration_s, elevation_m_smooth)
        WHERE t.activity_id = v.activity_id::bigint 
          AND t.elapsed_duration_s = v.elapsed_duration_s::int;
    """

    with conn.cursor() as cur:
        # page_size=1000 chunks the update within the single query to protect Pi's RAM
        execute_values(cur, query, smoothed_records, page_size=1000)

    conn.commit()


def process_all_activities(activity_ids, is_reference=False):
    print("Starting Savitzky-Golay smoothing pipeline...")
    start_time = time.time()

    # Connect to the database
    conn = get_conn()

    total_activities = len(activity_ids)
    print(f"Found {total_activities} activities to process.\n" + "-" * 40)

    if is_reference:
        field_name = 'elevation_reference'
    else:
        field_name = 'elevation_m_smooth'

    for index, act_id in enumerate(activity_ids, start=1):
        try:
            with conn.cursor() as cur:
                # 2. Fetch the raw data for this activity
                # We use COALESCE to ensure we don't pass NULLs into the math function
                cur.execute(f"""
                    SELECT activity_id, elapsed_duration_s, COALESCE({field_name}, 0) 
                    FROM activities.activity_details 
                    WHERE activity_id = %s 
                    ORDER BY elapsed_duration_s;
                """, (act_id,))

                raw_records = cur.fetchall()

            if not raw_records:
                continue

            # 3. Apply the math
            smoothed_records = apply_savgol_filter(raw_records)

            # 4. Push it back to the database
            update_smoothed_elevation(conn, smoothed_records, field_name)

            # 5. Print progress every 10 activities to avoid terminal spam
            if index % 10 == 0 or index == total_activities:
                elapsed = time.time() - start_time
                rate = index / elapsed
                percent = (index / total_activities) * 100
                print(
                    f"[{percent:5.1f}%] Processed {index}/{total_activities} | ID: {act_id} | Rate: {rate:.1f} act/sec")

        except Exception as e:
            print(f"\nERROR on activity {act_id}: {e}")
            conn.rollback()  # Important: reset the transaction block if an error occurs

    conn.close()

    total_time = time.time() - start_time
    print("-" * 40)
    print(f"Pipeline complete! Processed {total_activities} activities in {total_time:.1f} seconds.")

def activity_post_processing():
    activity_list = sql_to_list(
        f"SELECT DISTINCT activity_id from activities.activity_processing_queue order by activity_id desc")

    if not activity_list:
        return

    for a in activity_list:
        int_a = int(a)


        # Insert heartrate values:
        hr_sql = f"""INSERT INTO health.heartrate_raw (ts_utc, heartrate_bpm)
                SELECT ts_utc, heartrate_bpm FROM activities.activity_details
                WHERE activity_id = {int_a} AND heartrate_bpm IS NOT NULL
                ON CONFLICT(ts_utc) DO UPDATE SET heartrate_bpm = EXCLUDED.heartrate_bpm
                WHERE health.heartrate_raw.heartrate_bpm IS DISTINCT FROM EXCLUDED.heartrate_bpm;"""
        qec(hr_sql)

        # Smooth Elevation Spikes
        qec(f"CALL activities.smooth_elevation_spikes2({int_a});")

        # Smooth again in python
        process_all_activities([int_a], False)
        process_all_activities([int_a], True)

        # Build the path
        path_sql = f"""UPDATE activities.activities
                SET activity_path = sub.path, is_downloaded=TRUE
                FROM (
                         SELECT
                             rd.activity_id,
                             ST_SetSRID(
                                     ST_MakeLine(
                                             ST_MakePoint(longitude, latitude, elevation_m_smooth, elapsed_duration_s)
                                             ORDER BY elapsed_duration_s ASC
                                     ),
                                     4326
                             ) AS path
                         FROM activities.activity_details rd
                         WHERE activity_id = {int_a} 
                         GROUP BY rd.activity_id
                     ) AS sub
                WHERE activities.activity_id = sub.activity_id;"""
        qec(path_sql)

        # Update Reference Values
        qec(f"CALL activities.update_elevation_reference({int_a});")


        # Segment matching
        max_id = one_sql_result(f"""SELECT
                    MAX(elapsed_duration_s)
                    FROM
                    activities.activity_details
                    where
                    activity_id = {int_a}""")
        qec(f"""CALL staging.match_activity_to_segment({int_a}, 0, {int(max_id)}, TRUE);""")
        qec(f"""DELETE FROM activities.activity_processing_queue WHERE activity_id = {int_a}""")

    qec("""CALL activities.refresh_overlaps();""")
    qec("""CALL activities.repath_segment_matches();""")
    qec("""CALL activities.repath_segments();""")

    return
