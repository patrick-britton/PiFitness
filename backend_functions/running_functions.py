from backend_functions.database_functions import qec


def leaderboard_update(segment_id=None, activity_id=None):
    if not segment_id and not activity_id:
        return

    if segment_id:
        where_sql = f"WHERE segment_id = {segment_id}"
        where_sql2 = ''
    else:
        where_sql = f"WHERE activity_id = {activity_id}"
        where_sql2 = f"WHERE f.activity_id = {activity_id}"
    up_sql = f"""
                WITH valid_segs as (
                SELECT DISTINCT segment_id
                FROM activities.segment_matches
                {where_sql}
                
                )
    ,seg_details AS (SELECT sm.segment_id,
                            s.segment_name,
                            s.is_course,
                            sm.activity_start_point,
                            sm.activity_end_point,
                            add.activity_id,
                            a.start_time_utc +
                            min((add.elapsed_duration_ms / 1000) || ' seconds'::text)::interval                       AS start_time_utc,
                            max(add.distance_m) - min(add.distance_m)                                                 AS distance_m,
                            round(((max(add.elapsed_duration_ms) - min(add.elapsed_duration_ms)) / 1000)::numeric,
                                  2)                                                                                  AS elapsed_duration_s,
                            max(add.heartrate_bpm)                                                                    AS max_hr,
                            avg(add.heartrate_bpm)                                                                    AS avg_hr,
                            avg(add.cadence_spm)                                                                      AS avg_cadence,
                            avg(c2f(add.air_temp_c::numeric))                                                         AS avg_temp,
                            avg(add.vert_oscillation_cm)                                                              AS avg_vert_osc,
                            avg(add.vert_ratio)                                                                       AS avg_vert_ratio,
                            avg(add.ground_contact_time_ms)                                                           AS avg_gct,
                            avg(add.performance_condition)                                                            AS avg_perf,
                            avg(add.ground_contact_balance_left) as avg_balance
                     FROM activities.segment_matches sm
                         INNER JOIN valid_segs vs on vs.segment_id = sm.segment_id
                              JOIN activities.segments s ON sm.segment_id = s.segment_id
                              JOIN activities.activity_details_distance add ON sm.activity_id = add.activity_id
                              JOIN activities.activities a
                                   ON a.activity_id = add.activity_id AND add.distance_m >= sm.activity_start_point AND
                                      add.distance_m <= sm.activity_end_point

                     GROUP BY sm.segment_id, s.is_course, add.activity_id, a.start_time_utc, s.segment_name, sm.activity_start_point,
                              sm.activity_end_point),
     seg_stats AS (SELECT sm.segment_id,
                          sm.is_course,
                          sm.segment_name,
                          sm.activity_id,
                          sm.activity_start_point,
                          sm.activity_end_point,
                          m2mi(sm.distance_m::numeric)                                                  AS distance_mi,
                          sm.elapsed_duration_s,
                          sm.start_time_utc,
                          sm.max_hr,
                          sm.avg_hr,
                          sm.avg_cadence,
                          sm.avg_temp,
                          sm.avg_vert_osc,
                          sm.avg_vert_ratio,
                          sm.avg_gct,
                          sm.avg_perf,
                          sm.avg_balance,
                          row_number()
                          OVER (PARTITION BY sm.segment_id ORDER BY sm.elapsed_duration_s)              AS all_time_rank,
                          sm.start_time_utc > (CURRENT_TIMESTAMP - '365 days'::interval)                AS is_last_365,
                          sm.start_time_utc > ((SELECT max(training_cycle_dates.cycle_start_utc) AS max
                                                FROM activities.training_cycle_dates))                  AS is_current_cycle
                   FROM seg_details sm),
     seg_ranks AS (SELECT seg_stats.segment_id,
                          seg_stats.is_course,
                          seg_stats.segment_name,
                          seg_stats.activity_id,
                          seg_stats.activity_start_point,
                          seg_stats.activity_end_point,
                          seg_stats.distance_mi,
                          seg_stats.elapsed_duration_s,
                          seg_stats.start_time_utc,
                          seg_stats.all_time_rank,
                          seg_stats.max_hr,
                          seg_stats.avg_hr,
                          seg_stats.avg_cadence,
                          seg_stats.avg_temp,
                          seg_stats.avg_vert_osc,
                          seg_stats.avg_vert_ratio,
                          seg_stats.avg_gct,
                          seg_stats.avg_perf,
                          seg_stats.avg_balance,
                          CASE
                              WHEN seg_stats.is_last_365 THEN row_number()
                                                              OVER (PARTITION BY seg_stats.segment_id, seg_stats.is_last_365 ORDER BY seg_stats.elapsed_duration_s)
                              ELSE NULL::bigint
                              END                                                                         AS last_365_rank,
                          CASE
                              WHEN seg_stats.is_current_cycle THEN row_number()
                                                                   OVER (PARTITION BY seg_stats.segment_id, seg_stats.is_current_cycle ORDER BY seg_stats.elapsed_duration_s)
                              ELSE NULL::bigint
                              END                                                                         AS current_cycle_rank,
                          row_number()
                          OVER (PARTITION BY seg_stats.segment_id ORDER BY seg_stats.start_time_utc DESC) AS recency_rank,
                          sum(1) OVER (PARTITION BY seg_stats.segment_id)                                 AS all_time_effort_count,
                          sum(
                          CASE
                              WHEN seg_stats.is_last_365 THEN 1
                              ELSE 0
                              END)
                          OVER (PARTITION BY seg_stats.segment_id)                                        AS last_365_effort_count,
                          sum(
                          CASE
                              WHEN seg_stats.is_current_cycle THEN 1
                              ELSE 0
                              END)
                          OVER (PARTITION BY seg_stats.segment_id)                                        AS this_cycle_effort_count,
                          max(seg_stats.start_time_utc)
                          OVER (PARTITION BY seg_stats.segment_id)                                        AS last_effort_utc,
                          seg_stats.start_time_utc = max(seg_stats.start_time_utc)
                                                     OVER (PARTITION BY seg_stats.segment_id)             AS is_last_effort
                   FROM seg_stats),
     labels AS (SELECT r.segment_id,
                       r.is_course,
                       r.segment_name,
                       r.activity_id,
                       r.activity_start_point,
                       r.activity_end_point,
                       w.weight_lb,
                       w.muscle_lb,
                       w.fat_lb,
                       r.distance_mi,
                       r.elapsed_duration_s,
                       r.start_time_utc,
                       r.all_time_rank,
                       COALESCE(r.last_365_rank, r.last_365_effort_count + r.all_time_rank)        AS last_365_rank,
                       COALESCE(r.current_cycle_rank, r.this_cycle_effort_count +
                                                      r.all_time_rank)                             AS current_cycle_rank,
                       r.recency_rank,
                       r.all_time_effort_count,
                       r.last_365_effort_count,
                       r.this_cycle_effort_count,
                       r.last_effort_utc,
                       r.is_last_effort,
                       CASE
                           WHEN r.recency_rank = 1 THEN 'most_recent'::text
                           WHEN r.recency_rank = 2 THEN 'prior_attempt'::text
                           WHEN r.current_cycle_rank = 1 AND r.recency_rank > 2 THEN 'best_of_cycle'::text
                           WHEN r.last_365_rank = 1 AND r.recency_rank > 2 AND
                                COALESCE(r.current_cycle_rank, 999::bigint) <> 1 THEN 'best_in_last_year'::text
                           WHEN r.all_time_rank = 1 AND r.recency_rank > 2 AND
                                COALESCE(r.current_cycle_rank, 999::bigint) <> 1 AND
                                COALESCE(r.last_365_rank, 999::bigint) <> 1 THEN 'best_all_time'::text
                           ELSE 'other'::text
                           END                                                                     AS effort_label,
                       ts.vo2_max_value,
                       ts.altitude_acclimation_m,
                       ts.heat_acclimation_pct,
                       ts.training_load_acute,
                       ts.training_load_pct,
                       ts.resting_hr_asleep,
                       ts.resting_hr_awake,
                       ts.sleep_duration_s,
                       ts.sleep_score,
                       ts.light_sleep_s,
                       ts.deep_sleep_s,
                       ts.awake_sleep_s,
                       ts.rem_sleep_s,
                       ts.awake_s_before_activity,
                       r.max_hr,
                       r.avg_hr,
                       r.avg_cadence,
                       r.avg_temp,
                       r.avg_vert_osc,
                       r.avg_vert_ratio,
                       r.avg_gct,
                       r.avg_perf,
                       r.avg_balance
                FROM seg_ranks r
                         LEFT JOIN activities.vw_activity_weights w ON w.activity_id = r.activity_id
                         LEFT JOIN activities.vw_activity_training_status ts ON r.activity_id = ts.activity_id),
     final_sql AS (SELECT l.segment_id,
                          l.is_course,
                          l.segment_name,
                          l.activity_id,
                          l.activity_start_point,
                          l.activity_end_point,
                          l.weight_lb,
                          l.muscle_lb,
                          l.fat_lb,
                          l.distance_mi,
                          l.elapsed_duration_s,
                          pace_as_text(l.distance_mi, l.elapsed_duration_s) AS pace_str,
                          l.start_time_utc,
                          l.all_time_rank,
                          l.last_365_rank,
                          l.current_cycle_rank,
                          l.recency_rank,
                          l.all_time_effort_count,
                          l.last_365_effort_count,
                          l.this_cycle_effort_count,
                          l.last_effort_utc,
                          l.is_last_effort,
                          l.effort_label,
                          l.vo2_max_value,
                          l.altitude_acclimation_m,
                          l.heat_acclimation_pct,
                          l.training_load_acute,
                          l.training_load_pct,
                          l.resting_hr_asleep,
                          l.resting_hr_awake,
                          l.sleep_duration_s,
                          l.sleep_score,
                          l.light_sleep_s,
                          l.deep_sleep_s,
                          l.awake_sleep_s,
                          l.rem_sleep_s,
                          l.awake_s_before_activity,
                          l.max_hr,
                          l.avg_hr,
                          l.avg_cadence,
                          l.avg_temp,
                          l.avg_vert_osc,
                          l.avg_vert_ratio,
                          l.avg_gct,
                          l.avg_perf,
                          l.avg_balance
                   FROM labels l)

    SELECT
         f.segment_id,
         f.is_course,
         f.segment_name,
         f.activity_id,
         f.activity_start_point,
         f.activity_end_point,
         f.distance_mi,
         f.start_time_utc,
         f.all_time_rank,
         f.last_365_rank,
         f.current_cycle_rank,
         f.recency_rank,
         f.all_time_effort_count,
         f.last_365_effort_count,
         f.this_cycle_effort_count,
         f.last_effort_utc,
         f.is_last_effort,
         f.effort_label,
         f.elapsed_duration_s,
         f.pace_str,
         f.weight_lb,
         f.muscle_lb,
         f.fat_lb,
         f.vo2_max_value,
         f.altitude_acclimation_m,
         f.heat_acclimation_pct,
         f.training_load_acute,
         f.training_load_pct,
         f.resting_hr_asleep,
         f.resting_hr_awake,
         f.sleep_duration_s,
         f.sleep_score,
         f.light_sleep_s,
         f.deep_sleep_s,
         f.awake_sleep_s,
         f.rem_sleep_s,
         f.awake_s_before_activity,
         f.max_hr,
         f.avg_hr,
         f.avg_cadence,
         f.avg_temp,
         f.avg_vert_osc,
         f.avg_vert_ratio,
         f.avg_gct,
         f.avg_perf,
         f.avg_balance
     FROM final_sql f
     {where_sql2}
     ;"""

    return up_sql