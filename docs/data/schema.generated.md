# PiFitness database schema (generated)

Do not edit by hand. Regenerate with python tools/db/schema_map.py.

## Schema activities

### activities.activities (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| activity_name | text | YES |
| activity_type_id | smallint | YES |
| activity_type_name | text | YES |
| distance_m | numeric | YES |
| duration_s | numeric | YES |
| duration_elapsed_s | numeric | YES |
| duration_moving_s | integer | YES |
| calories | integer | YES |
| calories_bmr | integer | YES |
| steps | integer | YES |
| stride_length_cm_avg | integer | YES |
| cadence_double_spm_max | integer | YES |
| cadence_running_spm_max | integer | YES |
| cadence_running_spm_avg | numeric | YES |
| speed_mps_max | numeric | YES |
| speed_mps_avg | numeric | YES |
| heartrate_max | integer | YES |
| heartrate_avg | integer | YES |
| start_timestamp_utc | bigint | YES |
| start_time_utc | timestamp with time zone | YES |
| end_time_utc | timestamp with time zone | YES |
| latitude_start | numeric | YES |
| latitude_end | numeric | YES |
| longitude_start | numeric | YES |
| longitude_end | numeric | YES |
| elevation_m_min | numeric | YES |
| elevation_m_max | numeric | YES |
| elevation_m_gain | numeric | YES |
| elevation_m_loss | numeric | YES |
| temperature_c_min | smallint | YES |
| temperature_c_max | smallint | YES |
| water_ml_estimated | integer | YES |
| training_load | numeric | YES |
| training_effect_aerobic | numeric | YES |
| training_effect_anerobic | numeric | YES |
| intensity_minutes_moderate | integer | YES |
| intensity_minutes_vigorous | integer | YES |
| is_downloaded | boolean | YES |
| activity_path | USER-DEFINED | YES |

**Indexes:**
- `activities_pkey`: CREATE UNIQUE INDEX activities_pkey ON activities.activities USING btree (activity_id)
- `idx_activities_path_gist`: CREATE INDEX idx_activities_path_gist ON activities.activities USING gist (activity_path)
- `idx_activities_range`: CREATE INDEX idx_activities_range ON activities.activities USING gist (tstzrange(start_time_utc, end_time_utc))

### activities.activity_details (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| ts_utc | timestamp with time zone | NO |
| moving_duration_s | integer | YES |
| elapsed_duration_s | integer | YES |
| duration_s | integer | YES |
| distance_mm | bigint | YES |
| latitude | numeric | YES |
| longitude | numeric | YES |
| heartrate_bpm | smallint | YES |
| cadence_spm | smallint | YES |
| air_temp_c | smallint | YES |
| elevation_m | numeric | YES |
| speed_mmps | bigint | YES |
| vertical_speed_mps | numeric | YES |
| vert_oscillation_cm | numeric | YES |
| stride_length_cm | numeric | YES |
| vert_ratio | numeric | YES |
| ground_contact_time_ms | smallint | YES |
| ground_contact_balance_left | numeric | YES |
| performance_condition | smallint | YES |
| activity_type_id | smallint | YES |
| elevation_m_smooth | numeric | YES |
| elevation_reference | numeric | YES |
| loc_point | USER-DEFINED | YES |
| grid_5m_00_id | USER-DEFINED | YES |

**Indexes:**
- `activity_details_pkey`: CREATE UNIQUE INDEX activity_details_pkey ON activities.activity_details USING btree (activity_id, ts_utc)
- `activity_details_activity_id_distance_mm_idx`: CREATE INDEX activity_details_activity_id_distance_mm_idx ON activities.activity_details USING btree (activity_id, distance_mm)
- `idx_act_detail_lat_lon`: CREATE INDEX idx_act_detail_lat_lon ON activities.activity_details USING btree (latitude, longitude)
- `idx_large_table_loc_point`: CREATE INDEX idx_large_table_loc_point ON activities.activity_details USING gist (loc_point)
- `idx_loc_point_grid`: CREATE INDEX idx_loc_point_grid ON activities.activity_details USING gist (grid_5m_00_id)

### activities.activity_details_distance (TABLE)
| column | type | nullable |
|---|---|---|
| activity_type_id | smallint | YES |
| activity_id | bigint | NO |
| distance_m | integer | NO |
| elapsed_duration_ms | integer | YES |
| latitude | numeric | YES |
| longitude | numeric | YES |
| heartrate_bpm | smallint | YES |
| cadence_spm | smallint | YES |
| air_temp_c | smallint | YES |
| elevation_m | numeric | YES |
| speed_mmps | bigint | YES |
| vertical_speed_mps | numeric | YES |
| vert_oscillation_cm | numeric | YES |
| stride_length_cm | numeric | YES |
| vert_ratio | numeric | YES |
| ground_contact_time_ms | smallint | YES |
| ground_contact_balance_left | numeric | YES |
| performance_condition | smallint | YES |
| elevation_m_smooth | numeric | YES |
| elevation_reference | numeric | YES |
| loc_point | USER-DEFINED | YES |
| grid_5m_00_id | USER-DEFINED | YES |

**Indexes:**
- `activity_details_distance_pkey`: CREATE UNIQUE INDEX activity_details_distance_pkey ON activities.activity_details_distance USING btree (activity_id, distance_m)
- `activity_details_activity_id_distance_mm_idx_dist`: CREATE INDEX activity_details_activity_id_distance_mm_idx_dist ON activities.activity_details_distance USING btree (activity_id, distance_m)
- `idx_act_detail_lat_lon_dist`: CREATE INDEX idx_act_detail_lat_lon_dist ON activities.activity_details_distance USING btree (latitude, longitude)
- `idx_large_table_loc_point_dist`: CREATE INDEX idx_large_table_loc_point_dist ON activities.activity_details_distance USING gist (loc_point)
- `idx_loc_point_grid_dist`: CREATE INDEX idx_loc_point_grid_dist ON activities.activity_details_distance USING gist (grid_5m_00_id)

### activities.activity_processing_queue (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| created_at_utc | timestamp with time zone | YES |

**Indexes:**
- `activity_processing_queue_pkey`: CREATE UNIQUE INDEX activity_processing_queue_pkey ON activities.activity_processing_queue USING btree (activity_id)

### activities.activity_resampled_1m (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| distance_m | integer | YES |
| latitude | double precision | YES |
| longitude | double precision | YES |
| elevation_m | double precision | YES |
| elapsed_duration_s | double precision | YES |

### activities.courses (TABLE)
| column | type | nullable |
|---|---|---|
| course_id | integer | NO |
| course_name | text | YES |
| created_at | timestamp with time zone | YES |
| course_path | USER-DEFINED | YES |
| course_distance_m | numeric | YES |
| course_elevation_gain_m | numeric | YES |

**Indexes:**
- `courses_pkey`: CREATE UNIQUE INDEX courses_pkey ON activities.courses USING btree (course_id)
- `idx_courses_end_point`: CREATE INDEX idx_courses_end_point ON activities.courses USING gist (st_endpoint(course_path))
- `idx_courses_path`: CREATE INDEX idx_courses_path ON activities.courses USING gist (course_path)
- `idx_courses_start_point`: CREATE INDEX idx_courses_start_point ON activities.courses USING gist (st_startpoint(course_path))

### activities.distinct_segments (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | NO |
| match_seg_id | integer | NO |

**Indexes:**
- `distinct_segments_pkey`: CREATE UNIQUE INDEX distinct_segments_pkey ON activities.distinct_segments USING btree (segment_id, match_seg_id)

### activities.elevation_reference (TABLE)
| column | type | nullable |
|---|---|---|
| grid_5m_00_id | USER-DEFINED | NO |
| avg_elevation | double precision | NO |
| obs_count | integer | NO |
| last_updated | timestamp with time zone | YES |
| elev_stddev | numeric | YES |
| elevation_hammered | numeric | YES |
| elevation_sanded | numeric | YES |

**Indexes:**
- `elevation_reference_pkey`: CREATE UNIQUE INDEX elevation_reference_pkey ON activities.elevation_reference USING btree (grid_5m_00_id)
- `idx_elevation_ref_gist`: CREATE INDEX idx_elevation_ref_gist ON activities.elevation_reference USING gist (grid_5m_00_id)

### activities.metric_list (TABLE)
| column | type | nullable |
|---|---|---|
| activity_type_id | smallint | NO |
| metric_name | text | NO |
| metric_unit | text | YES |
| last_seen_utc | timestamp with time zone | YES |

**Indexes:**
- `metric_list_pkey`: CREATE UNIQUE INDEX metric_list_pkey ON activities.metric_list USING btree (activity_type_id, metric_name)

### activities.overlap_queue (TABLE)
| column | type | nullable |
|---|---|---|
| queue_id | integer | NO |
| segment_id_a | integer | NO |
| segment_id_b | integer | NO |
| match_confidence | numeric | YES |
| created_at | timestamp with time zone | YES |

**Indexes:**
- `overlap_queue_pkey`: CREATE UNIQUE INDEX overlap_queue_pkey ON activities.overlap_queue USING btree (queue_id)
- `unique_overlap_pair`: CREATE UNIQUE INDEX unique_overlap_pair ON activities.overlap_queue USING btree (segment_id_a, segment_id_b)

### activities.race_predictions_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| prediction_5k_s | integer | YES |
| prediction_10k_s | integer | YES |
| prediction_half_marathon_s | integer | YES |
| prediction_marathon_s | integer | YES |

**Indexes:**
- `race_predictions_raw_pkey`: CREATE UNIQUE INDEX race_predictions_raw_pkey ON activities.race_predictions_raw USING btree (ts_utc)

### activities.segment_match_exclusions (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_id | bigint | YES |

**Foreign keys:**
- `activity_id` -> `activities.activities.activity_id` (segment_match_exclusions_activity_id_fkey)
- `segment_id` -> `activities.segments.segment_id` (segment_match_exclusions_segment_id_fkey)

### activities.segment_matches (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | NO |
| activity_id | bigint | NO |
| activity_start_point | integer | YES |
| activity_end_point | integer | YES |
| activity_path | USER-DEFINED | YES |
| match_confirmed | boolean | YES |
| match_confidence | numeric | YES |
| match_ts_utc | timestamp with time zone | YES |

**Foreign keys:**
- `activity_id` -> `activities.activities.activity_id` (segment_matches_activity_id_fkey)

**Indexes:**
- `segment_matches_pkey`: CREATE UNIQUE INDEX segment_matches_pkey ON activities.segment_matches USING btree (segment_id, activity_id)
- `uc_segment_match_act_seg_ids`: CREATE UNIQUE INDEX uc_segment_match_act_seg_ids ON activities.segment_matches USING btree (segment_id, activity_id)
- `idx_segment_activities_path_gist`: CREATE INDEX idx_segment_activities_path_gist ON activities.segment_matches USING gist (activity_path)

### activities.segments (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | NO |
| activity_reference_id | bigint | YES |
| reference_start_point | integer | YES |
| reference_end_point | integer | YES |
| is_course | boolean | YES |
| segment_name | text | YES |
| segment_path | USER-DEFINED | YES |
| distance_m | numeric | YES |
| elevation_gain | numeric | YES |
| elevation_loss | numeric | YES |
| start_point | USER-DEFINED | YES |
| end_point | USER-DEFINED | YES |
| activity_type_id | smallint | YES |
| utm_srid | integer | YES |
| segment_path_transformed | USER-DEFINED | YES |

**Indexes:**
- `segments_pkey`: CREATE UNIQUE INDEX segments_pkey ON activities.segments USING btree (segment_id)
- `idx_segments_path_gist`: CREATE INDEX idx_segments_path_gist ON activities.segments USING gist (segment_path)

### activities.segments_details (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | NO |
| activity_id | bigint | NO |
| activity_start_point | integer | NO |
| activity_end_point | integer | NO |
| start_time_utc | timestamp with time zone | YES |
| distance_m | integer | YES |
| elapsed_duration_s | numeric | YES |
| max_hr | numeric | YES |
| avg_hr | numeric | YES |
| avg_cadence | numeric | YES |
| avg_temp | numeric | YES |
| avg_vert_osc | numeric | YES |
| avg_vert_ratio | numeric | YES |
| avg_gct | numeric | YES |
| avg_perf | numeric | YES |
| avg_balance | numeric | YES |
| avg_stride_length | numeric | YES |
| segment_path | USER-DEFINED | YES |

**Indexes:**
- `segments_details_pkey`: CREATE UNIQUE INDEX segments_details_pkey ON activities.segments_details USING btree (segment_id, activity_id, activity_start_point, activity_end_point)

### activities.steps_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| step_goal | integer | YES |
| total_steps | integer | YES |
| total_distance_m | integer | YES |

**Indexes:**
- `steps_raw_pkey`: CREATE UNIQUE INDEX steps_raw_pkey ON activities.steps_raw USING btree (ts_utc)

### activities.temp_activity_segment_metas (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| start_point | integer | YES |
| end_point | integer | YES |

### activities.temp_segment_details (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_type_id | smallint | YES |
| is_course | boolean | YES |
| segment_name | text | YES |
| row_asc | integer | YES |
| row_desc | integer | YES |
| elapsed_duration_s | integer | YES |
| latitude | numeric | YES |
| longitude | numeric | YES |
| elevation_m | numeric | YES |
| distance_m | numeric | YES |
| elevation_delta | numeric | YES |

### activities.temp_segment_matches_downselect (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| best_start_dist | integer | YES |
| best_end_dist | integer | YES |
| full_path | USER-DEFINED | YES |
| confidence | numeric | YES |
| ruggedness | numeric | YES |
| utm_srid | integer | YES |
| short_path | USER-DEFINED | YES |
| segment_id | integer | YES |
| dist_deviation | numeric | YES |
| full_path_transformed | USER-DEFINED | YES |
| freschet_deviation_m | numeric | YES |
| polygon_deviation_m | numeric | YES |
| hausdorff_deviation_m | numeric | YES |
| heading_deviation | numeric | YES |

### activities.temp_segment_matches_v1 (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_id | bigint | YES |
| activity_start_point | integer | YES |
| activity_end_point | integer | YES |
| activity_path | USER-DEFINED | YES |
| match_confirmed | boolean | YES |
| match_confidence | numeric | YES |
| match_ts_utc | timestamp with time zone | YES |

### activities.temp_segment_matching_activities (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |

### activities.temp_segment_matching_segments (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |

### activities.temp_segments_v1 (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_reference_id | bigint | YES |
| reference_start_point | integer | YES |
| reference_end_point | integer | YES |
| is_course | boolean | YES |
| segment_name | text | YES |
| segment_path | USER-DEFINED | YES |
| distance_m | numeric | YES |
| elevation_gain | numeric | YES |
| elevation_loss | numeric | YES |
| start_point | USER-DEFINED | YES |
| end_point | USER-DEFINED | YES |
| activity_type_id | smallint | YES |

### activities.training_cycle_dates (TABLE)
| column | type | nullable |
|---|---|---|
| cycle_start_utc | timestamp with time zone | NO |
| cycle_name | text | YES |

**Indexes:**
- `training_cycle_dates_pkey`: CREATE UNIQUE INDEX training_cycle_dates_pkey ON activities.training_cycle_dates USING btree (cycle_start_utc)

### activities.vw_activity_elevation_deltas (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| elapsed_duration_s | integer | YES |
| elevation_delta | numeric | YES |

### activities.vw_activity_header_stats (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_type_basic | text | YES |
| distance_mi | text | YES |
| duration_display | text | YES |
| pace_display | text | YES |
| start_time_utc | timestamp with time zone | YES |
| activity_path | USER-DEFINED | YES |
| hr_median | smallint | YES |
| hr_maximum | smallint | YES |

### activities.vw_activity_ids_to_sync (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| max_points | numeric | YES |

### activities.vw_activity_path_updates (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_path | USER-DEFINED | YES |

### activities.vw_activity_segments_aggregated (VIEW)
| column | type | nullable |
|---|---|---|
| id_val | text | YES |
| meta_rank | bigint | YES |
| path_coords | json | YES |
| map_timestamps | json | YES |
| x_time | ARRAY | YES |
| x_distance | ARRAY | YES |
| y_time | ARRAY | YES |
| y_time_delta | ARRAY | YES |
| y_hr | ARRAY | YES |
| y_hr_delta | ARRAY | YES |
| y_elevation | ARRAY | YES |
| y_cadence | ARRAY | YES |
| y_balance | ARRAY | YES |
| y_performance | ARRAY | YES |
| y_vert_ratio | ARRAY | YES |
| y_osc | ARRAY | YES |
| y_temp | ARRAY | YES |
| y_stride | ARRAY | YES |
| y_gct | ARRAY | YES |
| y_speed | ARRAY | YES |

### activities.vw_activity_sleep (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| awake_hours_prior | numeric | YES |
| sleep_score | integer | YES |
| avg_sleep_hours | numeric | YES |
| one_day_sleep_hours | numeric | YES |

### activities.vw_activity_summary (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_start_utc | timestamp with time zone | YES |
| distance_mi | numeric | YES |
| activity_pace | text | YES |
| segment_name | text | YES |
| segment_id | integer | YES |
| is_course | boolean | YES |
| activity_time_s | numeric | YES |
| segment_distance_mi | numeric | YES |
| all_time_rank | bigint | YES |
| best_time_s | numeric | YES |
| prior_time_s | numeric | YES |
| best_delta_s | numeric | YES |
| prior_delta_s | numeric | YES |
| segment_pace | text | YES |

### activities.vw_activity_summary_segment_creation (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_type_name | text | YES |
| start_time_utc | timestamp with time zone | YES |
| distance_mi | numeric | YES |
| distance_m | numeric | YES |
| child_courses | text | YES |
| child_segments | bigint | YES |
| matched_courses | bigint | YES |
| matched_segments | bigint | YES |

### activities.vw_activity_training_status (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| vo2_max_value | numeric | YES |
| altitude_acclimation_m | integer | YES |
| heat_acclimation_pct | numeric | YES |
| training_load_acute | numeric | YES |
| training_load_pct | numeric | YES |
| training_readiness | numeric | YES |
| training_load_mid | numeric | YES |
| training_load_max | numeric | YES |
| training_load_min | numeric | YES |
| resting_hr_asleep | double precision | YES |
| resting_hr_awake | double precision | YES |
| sleep_duration_s | integer | YES |
| sleep_score | integer | YES |
| light_sleep_s | integer | YES |
| deep_sleep_s | integer | YES |
| awake_sleep_s | integer | YES |
| rem_sleep_s | integer | YES |
| awake_s_before_activity | numeric | YES |
| start_time_utc | timestamp with time zone | YES |
| sleep_end_utc | timestamp with time zone | YES |
| sleep_end_date | date | YES |
| activity_start_date | date | YES |

### activities.vw_activity_weights (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| weight_lb | numeric | YES |
| muscle_lb | numeric | YES |
| fat_lb | numeric | YES |

### activities.vw_course_review (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| activity_reference_id | bigint | YES |
| reference_start_point | integer | YES |
| reference_end_point | integer | YES |
| effort_label | text | YES |
| activity_type_name | text | YES |
| activity_path | USER-DEFINED | YES |
| distance_mi | numeric | YES |
| elevation_gain | numeric | YES |
| elevation_loss | numeric | YES |
| matched_activities | bigint | YES |
| last_event_utc | date | YES |
| course_idx | bigint | YES |
| max_course_idx | bigint | YES |
| new_count | bigint | YES |

### activities.vw_elevation_smooth (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| latitude | numeric | YES |
| longitude | numeric | YES |
| lat5 | numeric | YES |
| long5 | numeric | YES |
| elevation_raw | numeric | YES |
| elevation_m | numeric | YES |
| geog | USER-DEFINED | YES |

### activities.vw_last_activity_id_by_type (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_type | text | YES |

### activities.vw_matched_segment_path_updates (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_id | bigint | YES |
| activity_path | USER-DEFINED | YES |

### activities.vw_overlapped_segments (VIEW)
| column | type | nullable |
|---|---|---|
| match_rank | integer | YES |
| match_confidence | numeric | YES |
| segment_id | integer | YES |
| segment_name | text | YES |
| effort_label | text | YES |
| activity_path | USER-DEFINED | YES |
| matched_activities | ARRAY | YES |

### activities.vw_possible_segment_matches (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| activity_id | bigint | YES |
| activity_date | date | YES |
| match_confidence | numeric | YES |
| match_rank | bigint | YES |
| is_course | boolean | YES |
| activity_path | USER-DEFINED | YES |
| effort_label | text | YES |

### activities.vw_pr_hunter (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| vs_date | date | YES |
| attempt_label | text | YES |
| total_attempts | bigint | YES |
| predicted_time_s | numeric | YES |
| vs_pace | text | YES |
| estimated_pace | text | YES |
| prestige_score | numeric | YES |
| weight_delta | numeric | YES |
| vo2_delta | numeric | YES |
| acute_load_delta | numeric | YES |
| load_max_delta | numeric | YES |

### activities.vw_report_segment_efforts (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| is_course | boolean | YES |
| activity_id | bigint | YES |
| start_time_utc | timestamp with time zone | YES |
| elapsed_duration_s | numeric | YES |
| avg_cadence | numeric | YES |
| avg_stride_length | numeric | YES |
| avg_gct | numeric | YES |
| avg_vert_ratio | numeric | YES |
| all_time_rank | bigint | YES |
| total_attempts | bigint | YES |
| prior_time_s | numeric | YES |
| best_time_excl_s | numeric | YES |
| prior_delta_s | numeric | YES |
| best_delta_s | numeric | YES |
| prior10_avg_cadence | numeric | YES |
| prior10_avg_stride_length | numeric | YES |
| prior10_avg_gct | numeric | YES |
| prior10_avg_vert_ratio | numeric | YES |

### activities.vw_run_timing (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| run_age | text | YES |
| music_start | timestamp with time zone | YES |
| music_end | timestamp with time zone | YES |
| music_duration_ms | numeric | YES |

### activities.vw_segment_comps (VIEW)
| column | type | nullable |
|---|---|---|
| id_val | text | YES |
| meta_rank | bigint | YES |
| elevation_m_smooth | numeric | YES |
| distance_m | integer | YES |
| elapsed_ms | integer | YES |
| elapsed_ms_delta | numeric | YES |
| longitude | numeric | YES |
| latitude | numeric | YES |
| heartrate_bpm | smallint | YES |
| heartrate_bpm_delta | numeric | YES |
| cadence_spm | smallint | YES |
| ground_contact_balance_left | numeric | YES |
| performance_condition | smallint | YES |
| vert_ratio | numeric | YES |
| vert_oscillation_cm | numeric | YES |
| air_temp_c | smallint | YES |
| stride_length_cm | numeric | YES |
| ground_contact_time_ms | smallint | YES |
| speed_mmps | bigint | YES |

### activities.vw_segment_create_auto_approve (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| segment_id | integer | YES |
| best_start_dist | integer | YES |
| best_end_dist | integer | YES |
| dist_deviation | numeric | YES |
| polygon_deviation_m | numeric | YES |
| hausdorff_deviation_m | numeric | YES |
| freschet_deviation_m | numeric | YES |
| confidence | numeric | YES |
| approval_phase | integer | YES |

### activities.vw_segment_create_from_details (VIEW)
| column | type | nullable |
|---|---|---|
| activity_reference_id | bigint | YES |
| reference_start_point | integer | YES |
| reference_end_point | integer | YES |
| is_course | boolean | YES |
| segment_name | text | YES |
| segment_path | USER-DEFINED | YES |
| distance_m | numeric | YES |
| elevation_gain | numeric | YES |
| elevation_loss | numeric | YES |
| start_point | USER-DEFINED | YES |
| end_point | USER-DEFINED | YES |
| activity_type_id | smallint | YES |

### activities.vw_segment_leaderboard (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| is_course | boolean | YES |
| segment_name | text | YES |
| cycle_name | text | YES |
| activity_id | bigint | YES |
| activity_start_point | integer | YES |
| activity_end_point | integer | YES |
| distance_mi | numeric | YES |
| start_time_utc | timestamp with time zone | YES |
| all_time_rank | bigint | YES |
| last_365_rank | bigint | YES |
| current_cycle_rank | bigint | YES |
| recency_rank | bigint | YES |
| elapsed_duration_s | numeric | YES |
| pace_str | text | YES |
| weight_lb | numeric | YES |
| muscle_pct | numeric | YES |
| fat_pct | numeric | YES |
| vo2_max_value | numeric | YES |
| altitude_acclimation_m | integer | YES |
| heat_acclimation_pct | numeric | YES |
| training_load_acute | numeric | YES |
| training_load_pct | numeric | YES |
| training_readiness | numeric | YES |
| training_load_mid | numeric | YES |
| training_load_max | numeric | YES |
| training_load_min | numeric | YES |
| resting_hr_asleep | double precision | YES |
| resting_hr_awake | double precision | YES |
| sleep_duration_s | integer | YES |
| sleep_hours | numeric | YES |
| sleep_score | integer | YES |
| light_sleep_s | integer | YES |
| deep_sleep_s | integer | YES |
| awake_sleep_s | integer | YES |
| rem_sleep_s | integer | YES |
| awake_hours | numeric | YES |
| max_hr | numeric | YES |
| avg_hr | numeric | YES |
| avg_cadence | numeric | YES |
| avg_temp | numeric | YES |
| avg_vert_osc | numeric | YES |
| avg_vert_ratio | numeric | YES |
| avg_gct | numeric | YES |
| avg_perf | numeric | YES |
| avg_balance | numeric | YES |
| avg_stride_length | numeric | YES |

### activities.vw_segment_matching_downselect_confidence (VIEW)
| column | type | nullable |
|---|---|---|
| confirmed | boolean | YES |
| activity_id | bigint | YES |
| best_start_dist | integer | YES |
| best_end_dist | integer | YES |
| dist_deviation | numeric | YES |
| polygon_deviation_m | numeric | YES |
| freschet_deviation_m | numeric | YES |
| hausdorff_deviation_m | numeric | YES |
| confidence_score | integer | YES |

### activities.vw_segment_matching_downselect_scores (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| best_start_dist | integer | YES |
| best_end_dist | integer | YES |
| dist_deviation | numeric | YES |
| ruggedness | double precision | YES |

### activities.vw_segment_path_updates (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_reference_id | bigint | YES |
| activity_type_id | smallint | YES |
| reference_start_point | integer | YES |
| reference_end_point | integer | YES |
| segment_path | USER-DEFINED | YES |
| distance_m | numeric | YES |
| ascent_m | numeric | YES |
| descent_m | numeric | YES |
| start_point | USER-DEFINED | YES |
| end_point | USER-DEFINED | YES |

### activities.vw_segment_racing (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| distance_mi | numeric | YES |
| effort_count | bigint | YES |
| last_effort | timestamp with time zone | YES |
| activity_id | bigint | YES |
| is_course | boolean | YES |

### activities.vw_segments_details_ranked (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_id | bigint | YES |
| cycle_name | text | YES |
| activity_start_point | integer | YES |
| activity_end_point | integer | YES |
| recency_rank | bigint | YES |
| all_time_rank | bigint | YES |
| last_365_rank | bigint | YES |
| current_cycle_rank | bigint | YES |
| start_time_utc | timestamp with time zone | YES |
| distance_mi | numeric | YES |
| elapsed_duration_s | numeric | YES |
| max_hr | numeric | YES |
| avg_hr | numeric | YES |
| avg_cadence | numeric | YES |
| avg_temp | numeric | YES |
| avg_vert_osc | numeric | YES |
| avg_vert_ratio | numeric | YES |
| avg_gct | numeric | YES |
| avg_perf | numeric | YES |
| avg_balance | numeric | YES |
| avg_stride_length | numeric | YES |
| segment_path | USER-DEFINED | YES |

### activities.vw_segments_effort_stats (VIEW)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| segment_name | text | YES |
| is_course | boolean | YES |
| activity_type_name | text | YES |
| distance_mi | numeric | YES |
| elevation_gain | numeric | YES |
| last_effort | date | YES |
| matched_activity_count | bigint | YES |

### activities.vw_temp_segment_matches_downselect (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| segment_id | integer | YES |
| effort_date | date | YES |
| best_start_dist | integer | YES |
| best_end_dist | integer | YES |
| ruggedness | numeric | YES |
| dist_deviation | numeric | YES |
| polygon_deviation_m | numeric | YES |
| hausdorff_deviation_m | numeric | YES |
| freschet_deviation_m | numeric | YES |
| confidence | numeric | YES |

### activities.vw_unmatched_activitites (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |

### activities.vw_watch_music_heard (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |
| track_id | text | YES |
| isrc | text | YES |
| track_order | integer | YES |
| track_name_clean | text | YES |
| artist_display_name | text | YES |
| duration_ms | integer | YES |
| music_start | timestamp with time zone | YES |
| start_cumulative_ms | bigint | YES |
| end_cumulative_ms | bigint | YES |
| pct_listened | numeric | YES |
| total_listen_ms | numeric | YES |
| played_at_utc | timestamp with time zone | YES |

## Schema api_services

### api_services.api_service_list (TABLE)
| column | type | nullable |
|---|---|---|
| api_service_name | text | NO |
| api_service_function | text | YES |
| api_credential_requirements | text | YES |
| rate_limit_detected_utc | timestamp with time zone | YES |
| rate_limit_cleared_utc | timestamp with time zone | YES |

**Indexes:**
- `api_service_list_pkey`: CREATE UNIQUE INDEX api_service_list_pkey ON api_services.api_service_list USING btree (api_service_name)

### api_services.api_tokens (TABLE)
| column | type | nullable |
|---|---|---|
| id | integer | NO |
| service_name | text | NO |
| token_data | text | NO |
| created_at_utc | timestamp with time zone | YES |
| updated_at_utc | timestamp with time zone | YES |
| is_active | boolean | NO |
| refresh_token_expires_at_utc | timestamp with time zone | YES |

**Indexes:**
- `api_tokens_pkey`: CREATE UNIQUE INDEX api_tokens_pkey ON api_services.api_tokens USING btree (id)
- `api_tokens_service_name_is_active_idx`: CREATE INDEX api_tokens_service_name_is_active_idx ON api_services.api_tokens USING btree (service_name, is_active)

### api_services.credentials (TABLE)
| column | type | nullable |
|---|---|---|
| api_service_name | text | NO |
| api_credentials | text | YES |

**Indexes:**
- `credentials_pkey`: CREATE UNIQUE INDEX credentials_pkey ON api_services.credentials USING btree (api_service_name)

### api_services.function_library (TABLE)
| column | type | nullable |
|---|---|---|
| friendly_name | text | NO |
| api_service_name | text | YES |
| api_function_name | text | YES |
| python_extraction_function | text | YES |
| flatten_sproc | text | YES |
| value_recency | timestamp with time zone | YES |
| loop_strategy | text | YES |
| iter_path_keys | text | YES |
| iter_query_keys | text | YES |
| static_path_params | text | YES |
| static_query_params | text | YES |

**Foreign keys:**
- `api_service_name` -> `api_services.api_service_list.api_service_name` (fk_svc_function_to_service)

**Indexes:**
- `function_library_pkey`: CREATE UNIQUE INDEX function_library_pkey ON api_services.function_library USING btree (friendly_name)

### api_services.spotify_auth_metadata (TABLE)
| column | type | nullable |
|---|---|---|
| id | integer | NO |
| authorized_at_utc | timestamp with time zone | NO |
| refresh_token_hash | text | YES |
| expires_at_utc | timestamp with time zone | NO |
| is_active | boolean | YES |
| created_at | timestamp with time zone | YES |

**Indexes:**
- `spotify_auth_metadata_pkey`: CREATE UNIQUE INDEX spotify_auth_metadata_pkey ON api_services.spotify_auth_metadata USING btree (id)
- `spotify_auth_metadata_refresh_token_hash_key`: CREATE UNIQUE INDEX spotify_auth_metadata_refresh_token_hash_key ON api_services.spotify_auth_metadata USING btree (refresh_token_hash)

## Schema exercises

### exercises.exercise_attempts (TABLE)
| column | type | nullable |
|---|---|---|
| attempt_id | integer | NO |
| exercise_id | integer | NO |
| interval_seconds_used | numeric | NO |
| started_at | timestamp with time zone | NO |
| ended_at | timestamp with time zone | NO |
| paced_count | integer | NO |
| total_count | integer | NO |
| notes | text | YES |
| created_at | timestamp with time zone | NO |

**Foreign keys:**
- `exercise_id` -> `exercises.exercise_timers.exercise_id` (exercise_attempts_exercise_id_fkey)

**Indexes:**
- `exercise_attempts_pkey`: CREATE UNIQUE INDEX exercise_attempts_pkey ON exercises.exercise_attempts USING btree (attempt_id)

### exercises.exercise_timers (TABLE)
| column | type | nullable |
|---|---|---|
| exercise_id | integer | NO |
| name | text | NO |
| interval_seconds | numeric | NO |
| notes | text | YES |
| created_at | timestamp with time zone | NO |
| updated_at | timestamp with time zone | NO |

**Indexes:**
- `exercise_timers_pkey`: CREATE UNIQUE INDEX exercise_timers_pkey ON exercises.exercise_timers USING btree (exercise_id)
- `idx_exercise_timers_name`: CREATE UNIQUE INDEX idx_exercise_timers_name ON exercises.exercise_timers USING btree (lower(name))

## Schema food

### food.diary_entries (TABLE)
| column | type | nullable |
|---|---|---|
| entry_id | integer | NO |
| food_id | integer | YES |
| name_snapshot | text | NO |
| kcal_per_100g | double precision | YES |
| protein_g | double precision | YES |
| fat_g | double precision | YES |
| carbs_g | double precision | YES |
| fiber_g | double precision | YES |
| sugars_g | double precision | YES |
| sodium_mg | double precision | YES |
| caffeine_mg | double precision | YES |
| is_partial | boolean | NO |
| qty | double precision | NO |
| unit | text | NO |
| kcal | double precision | YES |
| logged_at | timestamp with time zone | NO |
| created_at | timestamp with time zone | NO |

**Foreign keys:**
- `food_id` -> `food.foods.food_id` (diary_entries_food_id_fkey)

**Indexes:**
- `diary_entries_pkey`: CREATE UNIQUE INDEX diary_entries_pkey ON food.diary_entries USING btree (entry_id)
- `ix_diary_day`: CREATE INDEX ix_diary_day ON food.diary_entries USING btree (logged_at DESC, entry_id DESC)
- `ix_diary_logged_day_kcal`: CREATE INDEX ix_diary_logged_day_kcal ON food.diary_entries USING btree ((((logged_at AT TIME ZONE 'UTC'::text))::date), kcal) WHERE (kcal IS NOT NULL)

### food.foods (TABLE)
| column | type | nullable |
|---|---|---|
| food_id | integer | NO |
| name | text | NO |
| source | text | NO |
| fdc_id | integer | YES |
| barcode | text | YES |
| kcal_per_100g | double precision | YES |
| protein_g | double precision | YES |
| fat_g | double precision | YES |
| carbs_g | double precision | YES |
| fiber_g | double precision | YES |
| sugars_g | double precision | YES |
| sodium_mg | double precision | YES |
| caffeine_mg | double precision | YES |
| serving_size | double precision | YES |
| serving_unit | text | YES |
| serving_text | text | YES |
| nutrients_raw | jsonb | YES |
| normalized_from | text | YES |
| needs_review | boolean | NO |
| review_reason | text | YES |
| quality_score | double precision | NO |
| is_deleted | boolean | NO |
| deleted_at | timestamp with time zone | YES |
| created_at | timestamp with time zone | NO |

**Indexes:**
- `foods_pkey`: CREATE UNIQUE INDEX foods_pkey ON food.foods USING btree (food_id)
- `ix_foods_barcode`: CREATE INDEX ix_foods_barcode ON food.foods USING btree (barcode) WHERE (barcode IS NOT NULL)
- `ix_foods_name_trgm`: CREATE INDEX ix_foods_name_trgm ON food.foods USING gin (name gin_trgm_ops)
- `ix_foods_needs_review`: CREATE INDEX ix_foods_needs_review ON food.foods USING btree (needs_review) WHERE needs_review
- `ix_foods_source_alive`: CREATE INDEX ix_foods_source_alive ON food.foods USING btree (source) WHERE (NOT is_deleted)

### food.recipe_cook_states (TABLE)
| column | type | nullable |
|---|---|---|
| recipe_id | integer | NO |
| checked_steps | jsonb | NO |
| checked_foods | jsonb | NO |
| timers | jsonb | NO |
| updated_at | timestamp with time zone | NO |

**Foreign keys:**
- `recipe_id` -> `food.recipes.recipe_id` (recipe_cook_states_recipe_id_fkey)

**Indexes:**
- `recipe_cook_states_pkey`: CREATE UNIQUE INDEX recipe_cook_states_pkey ON food.recipe_cook_states USING btree (recipe_id)

### food.recipe_ingredients (TABLE)
| column | type | nullable |
|---|---|---|
| recipe_id | integer | NO |
| ingredient_id | integer | NO |
| food_id | integer | NO |
| qty | double precision | NO |
| unit | text | NO |

**Foreign keys:**
- `food_id` -> `food.foods.food_id` (recipe_ingredients_food_id_fkey)
- `recipe_id` -> `food.recipes.recipe_id` (recipe_ingredients_recipe_id_fkey)

**Indexes:**
- `recipe_ingredients_pkey`: CREATE UNIQUE INDEX recipe_ingredients_pkey ON food.recipe_ingredients USING btree (recipe_id, ingredient_id)

### food.recipe_step_foods (TABLE)
| column | type | nullable |
|---|---|---|
| recipe_id | integer | NO |
| step_id | integer | NO |
| food_id | integer | NO |
| qty | double precision | NO |
| unit | text | NO |

**Foreign keys:**
- `food_id` -> `food.foods.food_id` (recipe_step_foods_food_id_fkey)
- `recipe_id` -> `food.recipes.recipe_id` (recipe_step_foods_recipe_id_fkey)
- `step_id` -> `food.recipe_steps.step_id` (recipe_step_foods_step_id_fkey)

**Indexes:**
- `recipe_step_foods_pkey`: CREATE UNIQUE INDEX recipe_step_foods_pkey ON food.recipe_step_foods USING btree (recipe_id, step_id, food_id, unit)

### food.recipe_steps (TABLE)
| column | type | nullable |
|---|---|---|
| step_id | integer | NO |
| recipe_id | integer | NO |
| step_no | integer | NO |
| instruction | text | NO |
| timer_seconds | integer | YES |

**Foreign keys:**
- `recipe_id` -> `food.recipes.recipe_id` (recipe_steps_recipe_id_fkey)

**Indexes:**
- `recipe_steps_pkey`: CREATE UNIQUE INDEX recipe_steps_pkey ON food.recipe_steps USING btree (step_id)
- `recipe_steps_recipe_id_step_no_key`: CREATE UNIQUE INDEX recipe_steps_recipe_id_step_no_key ON food.recipe_steps USING btree (recipe_id, step_no)

### food.recipes (TABLE)
| column | type | nullable |
|---|---|---|
| recipe_id | integer | NO |
| title | text | NO |
| category | text | NO |
| times_prepared | integer | NO |
| food_id | integer | YES |
| is_deleted | boolean | NO |
| deleted_at | timestamp with time zone | YES |
| created_at | timestamp with time zone | NO |
| servings_count | integer | YES |
| serving_unit | text | YES |

**Foreign keys:**
- `food_id` -> `food.foods.food_id` (recipes_food_id_fkey)

**Indexes:**
- `recipes_pkey`: CREATE UNIQUE INDEX recipes_pkey ON food.recipes USING btree (recipe_id)
- `ix_recipes_alive_rank`: CREATE INDEX ix_recipes_alive_rank ON food.recipes USING btree (times_prepared DESC, title) WHERE (NOT is_deleted)
- `ix_recipes_cat_alive`: CREATE INDEX ix_recipes_cat_alive ON food.recipes USING btree (category) WHERE (NOT is_deleted)
- `ix_recipes_food`: CREATE INDEX ix_recipes_food ON food.recipes USING btree (food_id) WHERE (food_id IS NOT NULL)

### food.tri_tip_events (TABLE)
| column | type | nullable |
|---|---|---|
| tri_tip_id | integer | NO |
| weight_lbs | numeric | NO |
| shape | text | NO |
| target_internal_temp_f | numeric | NO |
| status | text | NO |
| started_at | timestamp with time zone | YES |
| completed_at | timestamp with time zone | YES |
| label | text | YES |
| notes | text | YES |
| created_at | timestamp with time zone | NO |

**Indexes:**
- `tri_tip_events_pkey`: CREATE UNIQUE INDEX tri_tip_events_pkey ON food.tri_tip_events USING btree (tri_tip_id)

### food.tri_tip_readings (TABLE)
| column | type | nullable |
|---|---|---|
| reading_id | integer | NO |
| tri_tip_id | integer | NO |
| recorded_at | timestamp with time zone | NO |
| grill_temp_f | numeric | NO |
| internal_temp_f | numeric | NO |
| note | text | YES |
| created_at | timestamp with time zone | NO |

**Foreign keys:**
- `tri_tip_id` -> `food.tri_tip_events.tri_tip_id` (tri_tip_readings_tri_tip_id_fkey)

**Indexes:**
- `tri_tip_readings_pkey`: CREATE UNIQUE INDEX tri_tip_readings_pkey ON food.tri_tip_readings USING btree (reading_id)

### food.vw_food_search (VIEW)
| column | type | nullable |
|---|---|---|
| food_id | integer | YES |
| name | text | YES |
| source | text | YES |
| kcal_per_100g | double precision | YES |
| caffeine_mg | double precision | YES |
| quality_score | double precision | YES |
| needs_review | boolean | YES |
| rank_score | double precision | YES |

### food.vw_usual_foods (VIEW)
| column | type | nullable |
|---|---|---|
| food_id | integer | YES |
| name | text | YES |
| source | text | YES |
| kcal_per_100g | double precision | YES |
| quality_score | double precision | YES |
| times_eaten | integer | YES |
| hour_weight | double precision | YES |
| mode_qty | double precision | YES |
| mode_unit | text | YES |

## Schema health

### health.body_battery_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| body_battery_value | integer | YES |

**Indexes:**
- `body_battery_raw_pkey`: CREATE UNIQUE INDEX body_battery_raw_pkey ON health.body_battery_raw USING btree (ts_utc)

### health.body_composition_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| weight_total_g | integer | YES |
| weight_bone_g | integer | YES |
| weight_muscle_g | integer | YES |
| fat_pct | numeric | YES |
| water_pct | numeric | YES |
| body_mass_index | numeric | YES |

**Indexes:**
- `body_composition_raw_pkey`: CREATE UNIQUE INDEX body_composition_raw_pkey ON health.body_composition_raw USING btree (ts_utc)

### health.body_dimensions (TABLE)
| column | type | nullable |
|---|---|---|
| captured_at_utc | timestamp with time zone | NO |
| butt_cm | numeric | YES |
| waist_cm | numeric | YES |
| stomach_cm | numeric | YES |
| chest_cm | numeric | YES |
| neck_cm | numeric | YES |

**Indexes:**
- `body_dimensions_pkey`: CREATE UNIQUE INDEX body_dimensions_pkey ON health.body_dimensions USING btree (captured_at_utc)

### health.fitness_age_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| human_age | numeric | YES |
| fitness_age_measured | numeric | YES |
| fitness_age_potential | numeric | YES |

**Indexes:**
- `fitness_age_raw_pkey`: CREATE UNIQUE INDEX fitness_age_raw_pkey ON health.fitness_age_raw USING btree (ts_utc)

### health.heartrate_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| heartrate_bpm | smallint | YES |
| activity_label | text | YES |
| hr_date | date | YES |

**Indexes:**
- `heartrate_raw_pkey`: CREATE UNIQUE INDEX heartrate_raw_pkey ON health.heartrate_raw USING btree (ts_utc)
- `heartrate_raw_hr_date_activity_label_heartrate_bpm_idx`: CREATE INDEX heartrate_raw_hr_date_activity_label_heartrate_bpm_idx ON health.heartrate_raw USING btree (hr_date, activity_label, heartrate_bpm)
- `heartrate_raw_hr_date_idx`: CREATE INDEX heartrate_raw_hr_date_idx ON health.heartrate_raw USING btree (hr_date)
- `idx_heartrate_raw_ts_utc_date`: CREATE INDEX idx_heartrate_raw_ts_utc_date ON health.heartrate_raw USING btree ((((ts_utc AT TIME ZONE 'UTC'::text))::date), ts_utc)

### health.hrv_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| hrv_value | smallint | YES |

**Indexes:**
- `hrv_raw_pkey`: CREATE UNIQUE INDEX hrv_raw_pkey ON health.hrv_raw USING btree (ts_utc)

### health.photo_metadata (TABLE)
| column | type | nullable |
|---|---|---|
| captured_at_utc | timestamp with time zone | NO |
| photo_type | text | YES |
| file_name | text | YES |

**Indexes:**
- `photo_metadata_pkey`: CREATE UNIQUE INDEX photo_metadata_pkey ON health.photo_metadata USING btree (captured_at_utc)

### health.respiration_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| breaths_per_minute | smallint | YES |

**Indexes:**
- `respiration_raw_pkey`: CREATE UNIQUE INDEX respiration_raw_pkey ON health.respiration_raw USING btree (ts_utc)

### health.sleep_movement_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| sleep_movement | numeric | YES |

**Indexes:**
- `sleep_movement_raw_pkey`: CREATE UNIQUE INDEX sleep_movement_raw_pkey ON health.sleep_movement_raw USING btree (ts_utc)

### health.sleep_totals (TABLE)
| column | type | nullable |
|---|---|---|
| sleep_end_date | date | YES |
| sleep_start_utc | timestamp with time zone | NO |
| sleep_end_utc | timestamp with time zone | YES |
| sleep_score | integer | YES |
| heartrate_bpm | numeric | YES |
| spo2 | numeric | YES |
| breaths_per_min | numeric | YES |
| hrv_value | smallint | YES |
| sleep_duration_s | integer | YES |
| rem_sleep_s | integer | YES |
| light_sleep_s | integer | YES |
| awake_sleep_s | integer | YES |
| deep_sleep_s | integer | YES |
| score_label | text | YES |

**Indexes:**
- `sleep_totals_pk`: CREATE UNIQUE INDEX sleep_totals_pk ON health.sleep_totals USING btree (sleep_start_utc)
- `idx_sleep_end`: CREATE INDEX idx_sleep_end ON health.sleep_totals USING btree (sleep_end_utc DESC)
- `idx_sleep_totals_range`: CREATE INDEX idx_sleep_totals_range ON health.sleep_totals USING gist (tstzrange(sleep_start_utc, sleep_end_utc))

### health.spo2_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| spo2_reading | smallint | YES |
| spo2_confidence | smallint | YES |

**Indexes:**
- `spo2_raw_pkey`: CREATE UNIQUE INDEX spo2_raw_pkey ON health.spo2_raw USING btree (ts_utc)

### health.stress_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| stress_value | smallint | YES |

**Indexes:**
- `stress_raw_pkey`: CREATE UNIQUE INDEX stress_raw_pkey ON health.stress_raw USING btree (ts_utc)

### health.training_status_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| vo2_max_value | numeric | YES |
| altitude_current_m | integer | YES |
| altitude_acclimation_m | integer | YES |
| altitude_acclimation_pct | numeric | YES |
| heat_acclimation_pct | numeric | YES |
| training_load_acute | numeric | YES |
| training_load_min | numeric | YES |
| training_load_max | numeric | YES |

**Indexes:**
- `training_status_raw_pkey`: CREATE UNIQUE INDEX training_status_raw_pkey ON health.training_status_raw USING btree (ts_utc)

### health.vw_activity_weights (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| weight_total_g | numeric | YES |
| weight_bone_g | numeric | YES |
| weight_muscle_g | numeric | YES |
| weight_fat_g | numeric | YES |
| fat_pct | numeric | YES |
| water_pct | numeric | YES |

### health.vw_body_comp_interpolation_precalc (VIEW)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | YES |
| next_ts_utc | timestamp with time zone | YES |
| weight_total_g | integer | YES |
| delta_s | integer | YES |
| weight_delta_per_s | numeric | YES |
| weight_bone_g | integer | YES |
| bone_delta_per_s | numeric | YES |
| weight_muscle_g | integer | YES |
| muscle_delta_per_s | numeric | YES |
| fat_pct | numeric | YES |
| fat_delta_per_s | numeric | YES |
| water_pct | numeric | YES |
| water_delta_per_s | numeric | YES |

### health.vw_heart_rates (VIEW)
| column | type | nullable |
|---|---|---|
| hr_date | date | YES |
| resting_hr_asleep | double precision | YES |
| max_hr | double precision | YES |
| resting_hr_awake | double precision | YES |

### health.weight_target (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| weight_total_g | integer | YES |

**Indexes:**
- `weight_target_pkey`: CREATE UNIQUE INDEX weight_target_pkey ON health.weight_target USING btree (ts_utc)

## Schema logging

### logging.api_logins (TABLE)
| column | type | nullable |
|---|---|---|
| event_time_utc | timestamp with time zone | NO |
| api_service_name | text | YES |
| event_name | text | YES |
| token_age_s | smallint | YES |
| error_text | text | YES |

**Foreign keys:**
- `api_service_name` -> `api_services.api_service_list.api_service_name` (api_logins_api_service_name_fkey)

**Indexes:**
- `api_logins_pkey`: CREATE UNIQUE INDEX api_logins_pkey ON logging.api_logins USING btree (event_time_utc)

### logging.application_events (TABLE)
| column | type | nullable |
|---|---|---|
| event_time_utc | timestamp with time zone | NO |
| event_category | text | YES |
| event_description | text | YES |
| execution_time_ms | integer | YES |
| error_text | text | YES |
| task_id | integer | YES |
| data_event | text | YES |

**Indexes:**
- `application_events_pkey`: CREATE UNIQUE INDEX application_events_pkey ON logging.application_events USING btree (event_time_utc)

### logging.db_size_log (TABLE)
| column | type | nullable |
|---|---|---|
| event_time_utc | timestamp with time zone | YES |
| table_name | text | YES |
| total_size_mb | numeric | YES |
| table_size_mb | numeric | YES |
| index_size_mb | numeric | YES |

### logging.db_stats (TABLE)
| column | type | nullable |
|---|---|---|
| event_time_utc | timestamp with time zone | YES |
| size_before_mb | numeric | YES |
| size_after_mb | numeric | YES |
| maintenance_time_ms | integer | YES |
| total_time_ms | integer | YES |
| maintenance_type | text | YES |

### logging.health_checks (TABLE)
| column | type | nullable |
|---|---|---|
| check_id | bigint | NO |
| check_time_utc | timestamp with time zone | NO |
| check_name | text | NO |
| status | text | NO |
| value | numeric | YES |
| message | text | YES |

**Indexes:**
- `health_checks_pkey`: CREATE UNIQUE INDEX health_checks_pkey ON logging.health_checks USING btree (check_id)
- `idx_health_checks_time`: CREATE INDEX idx_health_checks_time ON logging.health_checks USING btree (check_time_utc DESC)

### logging.music_db_info (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| track_id_count | integer | YES |
| isrc_id_count | integer | YES |
| mapped_isrc_id_count | integer | YES |
| artist_id_count | integer | YES |
| album_id_count | integer | YES |

**Indexes:**
- `music_db_info_pkey`: CREATE UNIQUE INDEX music_db_info_pkey ON logging.music_db_info USING btree (ts_utc)

### logging.task_executions (TABLE)
| column | type | nullable |
|---|---|---|
| record_id | bigint | NO |
| event_time_utc | timestamp with time zone | YES |
| task_name | text | YES |
| extract_time_ms | integer | YES |
| load_time_ms | integer | YES |
| transform_time_ms | integer | YES |
| failure_type | text | YES |
| error_text | text | YES |
| task_id | integer | YES |
| interpolation_time_ms | integer | YES |
| forecast_time_ms | integer | YES |

**Indexes:**
- `task_executions_pkey`: CREATE UNIQUE INDEX task_executions_pkey ON logging.task_executions USING btree (record_id)

### logging.vw_all_event_history (VIEW)
| column | type | nullable |
|---|---|---|
| event_time_utc | timestamp with time zone | YES |
| event_type | text | YES |
| description | text | YES |
| error_text | text | YES |
| is_error | boolean | YES |
| not_skip_row | boolean | YES |

### logging.vw_db_biggest_table_chart (VIEW)
| column | type | nullable |
|---|---|---|
| date_utc | date | YES |
| db_table | text | YES |
| total_size_mb | numeric | YES |

### logging.vw_db_size (VIEW)
| column | type | nullable |
|---|---|---|
| table_name | text | YES |
| total_size_mb | numeric | YES |
| table_size_mb | numeric | YES |
| index_size_mb | numeric | YES |
| other_size_mb | numeric | YES |

### logging.vw_db_size_chart (VIEW)
| column | type | nullable |
|---|---|---|
| date_utc | date | YES |
| table_size_mb | numeric | YES |
| index_size_mb | numeric | YES |
| other_size_mb | numeric | YES |

### logging.vw_event_analysis (VIEW)
| column | type | nullable |
|---|---|---|
| task_age | text | YES |
| event_category | text | YES |
| event_description | text | YES |
| error_text | text | YES |
| execution_time_ms | integer | YES |
| event_time_utc | timestamp with time zone | YES |
| is_error | boolean | YES |

### logging.vw_last_login (VIEW)
| column | type | nullable |
|---|---|---|
| api_service_name | text | YES |
| seconds_since_last_event | integer | YES |

### logging.vw_task_analysis (VIEW)
| column | type | nullable |
|---|---|---|
| task_age | text | YES |
| task_name | text | YES |
| is_failure | boolean | YES |
| error_text | text | YES |

## Schema music

### music.all_albums (TABLE)
| column | type | nullable |
|---|---|---|
| album_id | text | NO |
| album_name | text | YES |
| album_name_clean | text | YES |
| album_name_search | text | YES |
| updated_at_utc | timestamp with time zone | YES |

**Indexes:**
- `all_albums_pkey`: CREATE UNIQUE INDEX all_albums_pkey ON music.all_albums USING btree (album_id)
- `trgm_idx_album_name`: CREATE INDEX trgm_idx_album_name ON music.all_albums USING gin (album_name_search gin_trgm_ops)

### music.all_artists (TABLE)
| column | type | nullable |
|---|---|---|
| artist_id | text | NO |
| artist_name | text | YES |
| artist_name_search | text | YES |
| updated_at_utc | timestamp with time zone | YES |

**Indexes:**
- `all_artists_pkey`: CREATE UNIQUE INDEX all_artists_pkey ON music.all_artists USING btree (artist_id)
- `trgm_idx_artist_name`: CREATE INDEX trgm_idx_artist_name ON music.all_artists USING gin (artist_name_search gin_trgm_ops)

### music.all_tracks (TABLE)
| column | type | nullable |
|---|---|---|
| track_id | text | NO |
| track_name | text | YES |
| track_name_clean | text | YES |
| track_name_search | text | YES |
| track_isrc | text | YES |
| duration_ms | integer | YES |
| artist_id | text | YES |
| artist_display_name | text | YES |
| album_id | text | YES |
| release_date | date | YES |
| available_in_us | boolean | YES |
| total_markets | integer | YES |
| popularity | integer | YES |
| updated_at_utc | timestamp with time zone | YES |
| id_synced_at_utc | timestamp with time zone | YES |

**Indexes:**
- `all_tracks_pkey`: CREATE UNIQUE INDEX all_tracks_pkey ON music.all_tracks USING btree (track_id)
- `trgm_idx_track_name`: CREATE INDEX trgm_idx_track_name ON music.all_tracks USING gin (track_name_search gin_trgm_ops)

### music.isrc_non_swaps (TABLE)
| column | type | nullable |
|---|---|---|
| original_isrc | text | YES |
| mapped_isrc | text | YES |
| mapping_time_utc | timestamp with time zone | YES |

**Indexes:**
- `uq_original_mapped_isrc`: CREATE UNIQUE INDEX uq_original_mapped_isrc ON music.isrc_non_swaps USING btree (original_isrc, mapped_isrc)

### music.isrc_possible_dupes (TABLE)
| column | type | nullable |
|---|---|---|
| isrc1 | text | YES |
| isrc2 | text | YES |
| match_score | numeric | YES |
| track_score | numeric | YES |
| artist_score | numeric | YES |
| album_score | numeric | YES |

### music.isrc_swaps (TABLE)
| column | type | nullable |
|---|---|---|
| original_isrc | text | NO |
| mapped_isrc | text | YES |
| mapping_time_utc | timestamp with time zone | YES |
| valid_mapping | boolean | YES |

**Indexes:**
- `isrc_swaps_pkey`: CREATE UNIQUE INDEX isrc_swaps_pkey ON music.isrc_swaps USING btree (original_isrc)

### music.listening_history (TABLE)
| column | type | nullable |
|---|---|---|
| played_at_utc | timestamp with time zone | NO |
| isrc | text | NO |
| playlist_id | text | YES |

**Indexes:**
- `listening_history_pk`: CREATE UNIQUE INDEX listening_history_pk ON music.listening_history USING btree (isrc, played_at_utc)

### music.missing_relationships (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |
| needs_auto | boolean | YES |
| needs_manual | boolean | YES |
| needs_recs | boolean | YES |

### music.playlist_config (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | NO |
| playlist_name | text | YES |
| playlist_description | text | YES |
| track_count | integer | YES |
| auto_shuffle | boolean | YES |
| seeds_only | boolean | YES |
| make_recs | boolean | YES |
| ratings_weight | numeric | YES |
| recency_weight | numeric | YES |
| randomness_weight | numeric | YES |
| is_active | boolean | YES |
| minutes_to_sync | integer | YES |
| manual_shuffle | boolean | YES |
| last_synced_utc | timestamp with time zone | YES |
| last_auto_shuffled_utc | timestamp with time zone | YES |
| last_verified_utc | timestamp with time zone | YES |
| prior_track_count | integer | YES |

**Indexes:**
- `playlist_config_pkey`: CREATE UNIQUE INDEX playlist_config_pkey ON music.playlist_config USING btree (playlist_id)

### music.playlist_isrcs (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| track_order | integer | YES |
| updated_at_utc | timestamp with time zone | YES |

### music.playlist_recommendation_exclusions (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| elo_track_predicted | text | YES |
| decision_timestamp | timestamp with time zone | YES |

### music.playlist_relationships (TABLE)
| column | type | nullable |
|---|---|---|
| parent_playlist_id | text | NO |
| child_playlist_id | text | NO |
| child_playlist_type | text | YES |

**Indexes:**
- `pk_parent_child_playlist`: CREATE UNIQUE INDEX pk_parent_child_playlist ON music.playlist_relationships USING btree (parent_playlist_id, child_playlist_id)

### music.ratings (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | NO |
| isrc | text | NO |
| elo_rating | integer | YES |
| rating_count | integer | YES |
| rating_wins | integer | YES |
| rating_losses | integer | YES |
| last_rated_utc | timestamp with time zone | YES |

**Indexes:**
- `isrc_ratings`: CREATE UNIQUE INDEX isrc_ratings ON music.ratings USING btree (playlist_id, isrc)

### music.ratings_history (TABLE)
| column | type | nullable |
|---|---|---|
| rating_time_utc | timestamp with time zone | NO |
| playlist_id | text | NO |
| isrc | text | NO |
| isrc_vs | text | YES |
| elo_old | integer | YES |
| elo_new | integer | YES |
| rating_result | smallint | YES |

**Indexes:**
- `pk_rating_history`: CREATE UNIQUE INDEX pk_rating_history ON music.ratings_history USING btree (rating_time_utc, playlist_id, isrc)

### music.temp_listening_history (TABLE)
| column | type | nullable |
|---|---|---|
| played_at_utc | timestamp with time zone | YES |
| isrc | text | YES |
| playlist_id | text | YES |

### music.track_recommendation_decisions (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| elo_track_linear | numeric | YES |
| elo_track_rf | numeric | YES |
| elo_track_neural_net | numeric | YES |
| elo_track_pairwise | numeric | YES |
| elo_track_predicted | numeric | YES |
| elo_artist_snap | numeric | YES |
| elo_genre_snap | numeric | YES |
| popularity_snap | numeric | YES |
| was_promoted | boolean | YES |
| decision_time_utc | timestamp with time zone | YES |
| model_version | text | YES |
| notes | text | YES |

### music.track_recommendations (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| artist_id | text | YES |
| genre | text | YES |
| possible_genres | text | YES |
| artist_elo | numeric | YES |
| genre_elo | numeric | YES |
| playlist_genre_rank | integer | YES |
| is_best_genre | boolean | YES |
| track_name_clean | text | YES |
| popularity | integer | YES |
| m_acousticness | numeric | YES |
| m_energy | numeric | YES |
| m_instrumentalness | numeric | YES |
| m_key | numeric | YES |
| m_liveness | numeric | YES |
| m_mode | numeric | YES |
| m_speechiness | numeric | YES |
| m_tempo | numeric | YES |
| m_valence | numeric | YES |
| m_danceability | numeric | YES |
| elo_artist_norm | numeric | YES |
| elo_genre_norm | numeric | YES |
| elo_track_linear | numeric | YES |
| elo_track_random_forest | numeric | YES |
| elo_track_neural_net | numeric | YES |
| elo_track_pairwise | numeric | YES |
| elo_track_predicted | numeric | YES |

### music.vw_all_tracks (VIEW)
| column | type | nullable |
|---|---|---|
| track_id | text | YES |
| track_name | text | YES |
| track_name_clean | text | YES |
| track_name_search | text | YES |
| track_isrc | text | YES |
| duration_ms | integer | YES |
| artist_id | text | YES |
| artist_display_name | text | YES |
| album_id | text | YES |
| release_date | date | YES |
| available_in_us | boolean | YES |
| total_markets | integer | YES |
| popularity | integer | YES |
| updated_at_utc | timestamp with time zone | YES |
| album_name | text | YES |
| best_track_id | text | YES |
| is_best_track_id | boolean | YES |
| best_isrc | text | YES |
| is_best_isrc | boolean | YES |

### music.vw_best_track_id (VIEW)
| column | type | nullable |
|---|---|---|
| isrc | text | YES |
| best_track_id | text | YES |
| isrc_preference | bigint | YES |

### music.vw_isrc_dupe_review (VIEW)
| column | type | nullable |
|---|---|---|
| isrc1 | text | YES |
| pref1 | bigint | YES |
| track_name1 | text | YES |
| artist_name1 | text | YES |
| album_name1 | text | YES |
| duration_1 | integer | YES |
| isrc2 | text | YES |
| pref2 | bigint | YES |
| track_name2 | text | YES |
| artist_name2 | text | YES |
| album_name2 | text | YES |
| duration_2 | integer | YES |
| match_score | numeric | YES |
| track_score | numeric | YES |
| artist_score | numeric | YES |
| album_score | numeric | YES |
| duration_score | numeric | YES |
| preferred_isrc | text | YES |
| preferred_logic | text | YES |

### music.vw_listening_history (VIEW)
| column | type | nullable |
|---|---|---|
| played_at_utc | timestamp with time zone | YES |
| isrc | text | YES |
| playlist_id | text | YES |

### music.vw_playlist_config (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |
| playlist_description | text | YES |
| track_count | integer | YES |
| auto_shuffle | boolean | YES |
| manual_shuffle | boolean | YES |
| seeds_only | boolean | YES |
| make_recs | boolean | YES |
| ratings_weight | numeric | YES |
| recency_weight | numeric | YES |
| randomness_weight | numeric | YES |
| minutes_to_sync | integer | YES |
| playlist_type | text | YES |
| last_played_utc | timestamp with time zone | YES |

### music.vw_playlist_detail_sync_logic (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |
| next_valid_sync | timestamp with time zone | YES |
| last_played_utc | timestamp with time zone | YES |
| rval | bigint | YES |

### music.vw_playlist_families (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| child_playlist_id | text | YES |
| playlist_type | text | YES |

### music.vw_playlist_isrc_stats (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| target_playlist_id | text | YES |
| isrc | text | YES |
| track_id | text | YES |
| track_artist | text | YES |
| album_name | text | YES |
| duration_s | integer | YES |
| recency_pct | double precision | YES |
| ratings_pct | double precision | YES |
| random_pct | double precision | YES |
| recency_weight | numeric | YES |
| ratings_weight | numeric | YES |
| randomness_weight | numeric | YES |
| elo_rating | integer | YES |
| play_count_on_playlist | bigint | YES |
| recency_pressure | double precision | YES |
| ratings_pressure | double precision | YES |
| randomness_pressure | double precision | YES |
| total_weight | double precision | YES |
| minutes_to_sync | integer | YES |
| default_new_order | bigint | YES |
| cumulative_playlist_minutes | numeric | YES |

### music.vw_playlist_isrcs (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| track_order | integer | YES |
| updated_at_utc | timestamp with time zone | YES |

### music.vw_playlist_last_heard (VIEW)
| column | type | nullable |
|---|---|---|
| last_played_utc | timestamp with time zone | YES |
| playlist_id | text | YES |

### music.vw_playlist_shuffle_eligibility (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |
| last_played_utc | timestamp with time zone | YES |
| last_auto_shuffled_utc | timestamp with time zone | YES |

### music.vw_playlist_shuffle_targets (VIEW)
| column | type | nullable |
|---|---|---|
| parent_playlist_id | text | YES |
| target_playlist_id | text | YES |
| target_playlist_type | text | YES |

### music.vw_playlist_with_mismatched_isrc (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |

### music.vw_rating_eligible (VIEW)
| column | type | nullable |
|---|---|---|
| isrc | text | YES |
| playlist_id | text | YES |
| playlist_name | text | YES |
| last_played_utc | timestamp with time zone | YES |
| last_rated_utc | timestamp with time zone | YES |
| needs_rating | boolean | YES |
| played_in_last_24 | boolean | YES |
| never_rated | boolean | YES |

### music.vw_rating_eligible_playlists (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| playlist_name | text | YES |

### music.vw_rating_update (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| elo_rating | integer | YES |
| rating_count | bigint | YES |
| rating_wins | bigint | YES |
| rating_losses | bigint | YES |
| last_rated_utc | timestamp with time zone | YES |

### music.vw_ratings (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| elo_rating | integer | YES |
| rating_count | integer | YES |
| rating_wins | integer | YES |
| rating_losses | integer | YES |
| last_rated_utc | timestamp with time zone | YES |

### music.vw_ratings_history (VIEW)
| column | type | nullable |
|---|---|---|
| rating_time_utc | timestamp with time zone | YES |
| playlist_id | text | YES |
| isrc | text | YES |
| isrc_vs | text | YES |
| elo_old | integer | YES |
| elo_new | integer | YES |
| rating_result | smallint | YES |
| row_num | bigint | YES |

### music.vw_ratings_update (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| elo_current | integer | YES |
| rating_count | bigint | YES |
| wins | bigint | YES |
| losses | bigint | YES |
| last_rated_utc | timestamp with time zone | YES |

### music.vw_recent_plays (VIEW)
| column | type | nullable |
|---|---|---|
| last_played_at_utc | timestamp with time zone | YES |
| playcount_last_30 | bigint | YES |
| playcount_total | bigint | YES |
| track_artist | text | YES |
| rating | integer | YES |
| max_playcount_last_30 | bigint | YES |
| max_playcount_total | bigint | YES |
| max_elo | integer | YES |
| min_elo | integer | YES |

### music.vw_track_id_finder (VIEW)
| column | type | nullable |
|---|---|---|
| track_isrc | text | YES |

### music.vw_track_id_to_isrc (VIEW)
| column | type | nullable |
|---|---|---|
| track_id | text | YES |
| isrc | text | YES |

## Schema public

### public.dim_date (TABLE)
| column | type | nullable |
|---|---|---|
| date_key | integer | NO |
| date_utc | date | NO |
| year | smallint | NO |
| quarter | smallint | NO |
| month | smallint | NO |
| day_of_month | smallint | NO |
| day_of_week | smallint | NO |
| day_name | text | NO |
| month_name | text | NO |
| week_of_year | smallint | NO |
| day_of_year | smallint | YES |

**Indexes:**
- `dim_date_date_utc_key`: CREATE UNIQUE INDEX dim_date_date_utc_key ON public.dim_date USING btree (date_utc)
- `dim_date_pkey`: CREATE UNIQUE INDEX dim_date_pkey ON public.dim_date USING btree (date_key)

### public.dim_hour (TABLE)
| column | type | nullable |
|---|---|---|
| hour_key | integer | NO |
| ts_utc | timestamp without time zone | NO |
| date_key | integer | NO |
| hour | smallint | NO |

**Foreign keys:**
- `date_key` -> `public.dim_date.date_key` (dim_hour_date_key_fkey)

**Indexes:**
- `dim_hour_pkey`: CREATE UNIQUE INDEX dim_hour_pkey ON public.dim_hour USING btree (hour_key)
- `dim_hour_ts_utc_key`: CREATE UNIQUE INDEX dim_hour_ts_utc_key ON public.dim_hour USING btree (ts_utc)

### public.geography_columns (VIEW)
| column | type | nullable |
|---|---|---|
| f_table_catalog | name | YES |
| f_table_schema | name | YES |
| f_table_name | name | YES |
| f_geography_column | name | YES |
| coord_dimension | integer | YES |
| srid | integer | YES |
| type | text | YES |

### public.geometry_columns (VIEW)
| column | type | nullable |
|---|---|---|
| f_table_catalog | character varying | YES |
| f_table_schema | name | YES |
| f_table_name | name | YES |
| f_geometry_column | name | YES |
| coord_dimension | integer | YES |
| srid | integer | YES |
| type | character varying | YES |

### public.pg_stat_statements (VIEW)
| column | type | nullable |
|---|---|---|
| userid | oid | YES |
| dbid | oid | YES |
| toplevel | boolean | YES |
| queryid | bigint | YES |
| query | text | YES |
| plans | bigint | YES |
| total_plan_time | double precision | YES |
| min_plan_time | double precision | YES |
| max_plan_time | double precision | YES |
| mean_plan_time | double precision | YES |
| stddev_plan_time | double precision | YES |
| calls | bigint | YES |
| total_exec_time | double precision | YES |
| min_exec_time | double precision | YES |
| max_exec_time | double precision | YES |
| mean_exec_time | double precision | YES |
| stddev_exec_time | double precision | YES |
| rows | bigint | YES |
| shared_blks_hit | bigint | YES |
| shared_blks_read | bigint | YES |
| shared_blks_dirtied | bigint | YES |
| shared_blks_written | bigint | YES |
| local_blks_hit | bigint | YES |
| local_blks_read | bigint | YES |
| local_blks_dirtied | bigint | YES |
| local_blks_written | bigint | YES |
| temp_blks_read | bigint | YES |
| temp_blks_written | bigint | YES |
| shared_blk_read_time | double precision | YES |
| shared_blk_write_time | double precision | YES |
| local_blk_read_time | double precision | YES |
| local_blk_write_time | double precision | YES |
| temp_blk_read_time | double precision | YES |
| temp_blk_write_time | double precision | YES |
| wal_records | bigint | YES |
| wal_fpi | bigint | YES |
| wal_bytes | numeric | YES |
| wal_buffers_full | bigint | YES |
| jit_functions | bigint | YES |
| jit_generation_time | double precision | YES |
| jit_inlining_count | bigint | YES |
| jit_inlining_time | double precision | YES |
| jit_optimization_count | bigint | YES |
| jit_optimization_time | double precision | YES |
| jit_emission_count | bigint | YES |
| jit_emission_time | double precision | YES |
| jit_deform_count | bigint | YES |
| jit_deform_time | double precision | YES |
| parallel_workers_to_launch | bigint | YES |
| parallel_workers_launched | bigint | YES |
| stats_since | timestamp with time zone | YES |
| minmax_stats_since | timestamp with time zone | YES |

### public.pg_stat_statements_info (VIEW)
| column | type | nullable |
|---|---|---|
| dealloc | bigint | YES |
| stats_reset | timestamp with time zone | YES |

### public.raster_columns (VIEW)
| column | type | nullable |
|---|---|---|
| r_table_catalog | name | YES |
| r_table_schema | name | YES |
| r_table_name | name | YES |
| r_raster_column | name | YES |
| srid | integer | YES |
| scale_x | double precision | YES |
| scale_y | double precision | YES |
| blocksize_x | integer | YES |
| blocksize_y | integer | YES |
| same_alignment | boolean | YES |
| regular_blocking | boolean | YES |
| num_bands | integer | YES |
| pixel_types | ARRAY | YES |
| nodata_values | ARRAY | YES |
| out_db | ARRAY | YES |
| extent | USER-DEFINED | YES |
| spatial_index | boolean | YES |

### public.raster_overviews (VIEW)
| column | type | nullable |
|---|---|---|
| o_table_catalog | name | YES |
| o_table_schema | name | YES |
| o_table_name | name | YES |
| o_raster_column | name | YES |
| r_table_catalog | name | YES |
| r_table_schema | name | YES |
| r_table_name | name | YES |
| r_raster_column | name | YES |
| overview_factor | integer | YES |

### public.schema_migrations (TABLE)
| column | type | nullable |
|---|---|---|
| version | bigint | NO |
| applied_at | timestamp with time zone | NO |

**Indexes:**
- `schema_migrations_pkey`: CREATE UNIQUE INDEX schema_migrations_pkey ON public.schema_migrations USING btree (version)

### public.spatial_ref_sys (TABLE)
| column | type | nullable |
|---|---|---|
| srid | integer | NO |
| auth_name | character varying | YES |
| auth_srid | integer | YES |
| srtext | character varying | YES |
| proj4text | character varying | YES |

**Indexes:**
- `spatial_ref_sys_pkey`: CREATE UNIQUE INDEX spatial_ref_sys_pkey ON public.spatial_ref_sys USING btree (srid)

### public.staging.temp_bc_import (TABLE)
| column | type | nullable |
|---|---|---|
| ROW_ID | bigint | YES |
| ts_utc | text | YES |
| Weight | double precision | YES |
| BMI | double precision | YES |
| Body Fat | double precision | YES |
| Skeletal Muscle Mass | double precision | YES |
| Bone Mass | double precision | YES |
| Body Water | double precision | YES |

### public.v_end_elev (TABLE)
| column | type | nullable |
|---|---|---|
| smoothed_elev | double precision | YES |

### public.v_is_loop (TABLE)
| column | type | nullable |
|---|---|---|
| ?column? | boolean | YES |

### public.v_start_elev (TABLE)
| column | type | nullable |
|---|---|---|
| smoothed_elev | double precision | YES |

### public.v_total_distance (TABLE)
| column | type | nullable |
|---|---|---|
| max | integer | YES |

### public.vw_date (VIEW)
| column | type | nullable |
|---|---|---|
| date_key | integer | YES |
| date_utc | date | YES |
| year | smallint | YES |
| quarter | smallint | YES |
| month | smallint | YES |
| day_of_month | smallint | YES |
| day_of_week | smallint | YES |
| day_name | text | YES |
| month_name | text | YES |
| week_of_year | smallint | YES |
| relative_day | integer | YES |
| relative_week | integer | YES |
| relative_month | integer | YES |
| relative_year | integer | YES |
| day_of_year | smallint | YES |
| relative_30 | double precision | YES |
| relative_90 | double precision | YES |
| dm30 | double precision | YES |
| dm90 | double precision | YES |

### public.vw_date_all (VIEW)
| column | type | nullable |
|---|---|---|
| date_key | integer | YES |
| date_utc | date | YES |
| year | smallint | YES |
| quarter | smallint | YES |
| month | smallint | YES |
| day_of_month | smallint | YES |
| day_of_week | smallint | YES |
| day_name | text | YES |
| month_name | text | YES |
| week_of_year | smallint | YES |
| relative_day | integer | YES |
| relative_week | integer | YES |
| relative_month | integer | YES |
| relative_year | integer | YES |

### public.vw_hour (VIEW)
| column | type | nullable |
|---|---|---|
| date_key | integer | YES |
| date_utc | date | YES |
| date_hour_utc | timestamp without time zone | YES |
| year | smallint | YES |
| quarter | smallint | YES |
| month | smallint | YES |
| day_of_month | smallint | YES |
| day_of_week | smallint | YES |
| day_name | text | YES |
| month_name | text | YES |
| week_of_year | smallint | YES |
| relative_day | integer | YES |
| relative_week | integer | YES |
| relative_month | integer | YES |
| relative_year | integer | YES |

## Schema staging

### staging.activity_metric_descriptors (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| metric_idx | smallint | NO |
| metric_name | text | YES |
| metric_unit | text | YES |
| activity_type_id | smallint | YES |

**Indexes:**
- `activity_metric_descriptors_pkey`: CREATE UNIQUE INDEX activity_metric_descriptors_pkey ON staging.activity_metric_descriptors USING btree (activity_id, metric_idx)

### staging.activity_metrics_flat (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| activity_detail_index | integer | NO |
| metric_index | integer | NO |
| metric_value | text | YES |

**Indexes:**
- `activity_metrics_flat_pkey`: CREATE UNIQUE INDEX activity_metrics_flat_pkey ON staging.activity_metrics_flat USING btree (activity_id, activity_detail_index, metric_index)
- `idx_activity_metrics_activity`: CREATE INDEX idx_activity_metrics_activity ON staging.activity_metrics_flat USING btree (activity_id)

### staging.activity_metrics_matched (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_detail_index | integer | YES |
| metric_name | text | YES |
| metric_value | text | YES |
| activity_type_id | smallint | YES |

**Foreign keys:**
- `activity_id` -> `staging.activity_payload.activity_id` (activity_metrics_matched_activity_id_fkey)

### staging.activity_payload (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| payload | jsonb | YES |

**Indexes:**
- `activity_payload_pkey`: CREATE UNIQUE INDEX activity_payload_pkey ON staging.activity_payload USING btree (activity_id)

### staging.api_imports (TABLE)
| column | type | nullable |
|---|---|---|
| load_id | bigint | NO |
| event_time_utc | timestamp with time zone | YES |
| api_function_name | text | YES |
| payload | jsonb | NO |
| task_id | integer | YES |

**Indexes:**
- `api_imports_pkey`: CREATE UNIQUE INDEX api_imports_pkey ON staging.api_imports USING btree (load_id)

### staging.bounds (TABLE)
| column | type | nullable |
|---|---|---|
| id | boolean | NO |
| min_ts | timestamp with time zone | YES |
| max_ts | timestamp with time zone | YES |

**Indexes:**
- `bounds_pkey`: CREATE UNIQUE INDEX bounds_pkey ON staging.bounds USING btree (id)

### staging.interpolation_anchors (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| load_value | numeric | YES |
| prev_value | numeric | YES |
| prev_ts | timestamp with time zone | YES |
| next_value | numeric | YES |
| next_ts | timestamp with time zone | YES |

**Indexes:**
- `interpolation_anchors_pkey`: CREATE UNIQUE INDEX interpolation_anchors_pkey ON staging.interpolation_anchors USING btree (ts_utc)

### staging.interpolation_load_values (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | NO |
| load_value | numeric | YES |

**Indexes:**
- `interpolation_load_values_pkey`: CREATE UNIQUE INDEX interpolation_load_values_pkey ON staging.interpolation_load_values USING btree (ts_utc)

### staging.isrc_dupe_pairs (TABLE)
| column | type | nullable |
|---|---|---|
| isrc1 | text | YES |
| isrc2 | text | YES |
| track1 | text | YES |
| track2 | text | YES |
| artist1 | text | YES |
| artist2 | text | YES |
| artist_id1 | text | YES |
| artist_id2 | text | YES |
| album1 | text | YES |
| album2 | text | YES |
| album_id1 | text | YES |
| album_id2 | text | YES |
| duration1 | integer | YES |
| duration2 | integer | YES |

**Indexes:**
- `trgm_idx_pair_track1`: CREATE INDEX trgm_idx_pair_track1 ON staging.isrc_dupe_pairs USING gin (track1 gin_trgm_ops)
- `trgm_idx_pair_track2`: CREATE INDEX trgm_idx_pair_track2 ON staging.isrc_dupe_pairs USING gin (track2 gin_trgm_ops)

### staging.isrc_dupe_seeds (TABLE)
| column | type | nullable |
|---|---|---|
| isrc | text | NO |
| track_name_search | text | YES |
| artist_name_search | text | YES |
| album_name_search | text | YES |
| artist_id | text | YES |
| album_id | text | YES |
| duration_ms | integer | YES |
| prior_comparison | text | YES |

**Indexes:**
- `isrc_dupe_seeds_pkey`: CREATE UNIQUE INDEX isrc_dupe_seeds_pkey ON staging.isrc_dupe_seeds USING btree (isrc)
- `idx_isrc_seeds_duration`: CREATE INDEX idx_isrc_seeds_duration ON staging.isrc_dupe_seeds USING btree (duration_ms)
- `idx_isrc_seeds_duration_gist`: CREATE INDEX idx_isrc_seeds_duration_gist ON staging.isrc_dupe_seeds USING gist (duration_ms)
- `trgm_idx_track_seeds`: CREATE INDEX trgm_idx_track_seeds ON staging.isrc_dupe_seeds USING gin (track_name_search gin_trgm_ops)

### staging.json_examples (TABLE)
| column | type | nullable |
|---|---|---|
| raw_table_name | text | NO |
| json_sample | jsonb | YES |
| updated_at_utc | timestamp with time zone | YES |

**Indexes:**
- `json_examples_pkey`: CREATE UNIQUE INDEX json_examples_pkey ON staging.json_examples USING btree (raw_table_name)

### staging.stg_listen_history (TABLE)
| column | type | nullable |
|---|---|---|
| track_id | text | NO |
| track_name | text | YES |
| album_id | text | YES |
| album_name | text | YES |
| artist_id | text | YES |
| artist_name | text | YES |
| popularity | integer | YES |
| duration_ms | integer | YES |
| incoming_isrc | text | YES |
| available_market_count | integer | YES |
| available_in_usa | boolean | YES |
| context_type | text | YES |
| context_uri | text | YES |
| played_at_utc | timestamp with time zone | NO |
| track_release | text | YES |
| track_release_precision | text | YES |

**Indexes:**
- `stg_listen_history_pk`: CREATE UNIQUE INDEX stg_listen_history_pk ON staging.stg_listen_history USING btree (track_id, played_at_utc)

### staging.stg_now_playing (TABLE)
| column | type | nullable |
|---|---|---|
| track_id | text | NO |
| track_name | text | YES |
| album_id | text | YES |
| album_name | text | YES |
| artist_id | text | YES |
| artist_name | text | YES |
| popularity | integer | YES |
| duration_ms | integer | YES |
| incoming_isrc | text | YES |
| available_market_count | integer | YES |
| available_in_usa | boolean | YES |
| context_type | text | YES |
| context_uri | text | YES |
| played_at_utc | timestamp with time zone | NO |
| track_release | text | YES |
| track_release_precision | text | YES |

**Indexes:**
- `stg_now_playing_pk`: CREATE UNIQUE INDEX stg_now_playing_pk ON staging.stg_now_playing USING btree (track_id, played_at_utc)

### staging.stg_playlist_details (TABLE)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| track_id | text | YES |
| track_name | text | YES |
| track_order | integer | YES |
| album_id | text | YES |
| album_name | text | YES |
| artist_id | text | YES |
| artist_name | text | YES |
| track_release | text | YES |
| track_release_precision | text | YES |
| track_markets | text | YES |
| popularity | integer | YES |
| incoming_isrc | text | YES |
| duration_ms | integer | YES |
| fetched_at_utc | timestamp with time zone | YES |

### staging.temp_activity_locations (TABLE)
| column | type | nullable |
|---|---|---|
| latitude | numeric | YES |
| longitude | numeric | YES |
| loc_point | USER-DEFINED | YES |
| elevation_m_smooth | numeric | YES |
| grid_5m_00 | USER-DEFINED | YES |
| grid_5m_25_00 | USER-DEFINED | YES |
| grid_5m_00_25 | USER-DEFINED | YES |
| grid_5m_25_25 | USER-DEFINED | YES |

### staging.temp_activity_matched_locations (TABLE)
| column | type | nullable |
|---|---|---|
| latitude | numeric | YES |
| longitude | numeric | YES |
| loc_point | USER-DEFINED | YES |
| loc_point_matched | USER-DEFINED | YES |
| elevation_m_smooth | numeric | YES |

**Indexes:**
- `idx_sm_activity_details_lat_lon`: CREATE INDEX idx_sm_activity_details_lat_lon ON staging.temp_activity_matched_locations USING btree (latitude, longitude)
- `idx_sm_activity_details_p2p`: CREATE INDEX idx_sm_activity_details_p2p ON staging.temp_activity_matched_locations USING btree (loc_point, loc_point_matched)

### staging.temp_activity_metas (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| activity_type_id | smallint | YES |
| activity_path | USER-DEFINED | YES |

**Indexes:**
- `temp_activity_metas_pkey`: CREATE UNIQUE INDEX temp_activity_metas_pkey ON staging.temp_activity_metas USING btree (activity_id)

### staging.temp_activity_trackpoints (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| track_point | integer | YES |
| longitude | numeric | YES |
| latitude | numeric | YES |
| elevation_m | numeric | YES |
| activity_type_id | smallint | YES |

### staging.temp_bc_import (TABLE)
| column | type | nullable |
|---|---|---|
| ROW_ID | bigint | YES |
| ts_utc | text | YES |
| Weight | double precision | YES |
| BMI | double precision | YES |
| Body Fat | double precision | YES |
| Skeletal Muscle Mass | double precision | YES |
| Bone Mass | double precision | YES |
| Body Water | double precision | YES |

### staging.temp_csv_import (TABLE)
| column | type | nullable |
|---|---|---|
| Sleep Score 4 Weeks | text | YES |
| Score | text | YES |
| Resting Heart Rate | text | YES |
| Body Battery | text | YES |
| Pulse Ox | text | YES |
| Respiration | text | YES |
| HRV Status | text | YES |
| Quality | text | YES |
| Duration | text | YES |
| Bedtime | text | YES |
| Wake Time | text | YES |

### staging.temp_elevation_deltas (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | NO |
| ascent_m | numeric | YES |
| descent_m | numeric | YES |

**Indexes:**
- `temp_elevation_deltas_pkey`: CREATE UNIQUE INDEX temp_elevation_deltas_pkey ON staging.temp_elevation_deltas USING btree (activity_id)

### staging.temp_elevation_medians (TABLE)
| column | type | nullable |
|---|---|---|
| loc_point | USER-DEFINED | NO |
| median_elev | numeric | YES |

**Indexes:**
- `temp_elevation_medians_pkey`: CREATE UNIQUE INDEX temp_elevation_medians_pkey ON staging.temp_elevation_medians USING btree (loc_point)
- `idx_sm_activity_details_medians`: CREATE INDEX idx_sm_activity_details_medians ON staging.temp_elevation_medians USING btree (loc_point)

### staging.temp_elevation_smoothed (TABLE)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| track_point | integer | YES |
| elevation_m | numeric | YES |

### staging.temp_elevation_smoothing (TABLE)
| column | type | nullable |
|---|---|---|
| elapsed_duration_s | numeric | NO |
| elevation_m_raw | numeric | YES |
| distance_m | numeric | YES |

**Indexes:**
- `temp_elevation_smoothing_pkey`: CREATE UNIQUE INDEX temp_elevation_smoothing_pkey ON staging.temp_elevation_smoothing USING btree (elapsed_duration_s)

### staging.temp_elevation_smoothing_m (TABLE)
| column | type | nullable |
|---|---|---|
| meter | integer | NO |
| elapsed_duration_s | numeric | YES |
| elevation_m_raw | numeric | YES |
| elevation_m_smooth | numeric | YES |

**Indexes:**
- `temp_elevation_smoothing_m_pkey`: CREATE UNIQUE INDEX temp_elevation_smoothing_m_pkey ON staging.temp_elevation_smoothing_m USING btree (meter)

### staging.temp_isrc_swaps (TABLE)
| column | type | nullable |
|---|---|---|
| original_isrc | text | YES |
| mapped_isrc | text | YES |
| mapping_time_utc | timestamp with time zone | YES |
| valid_mapping | boolean | YES |

### staging.temp_performance_profiling (TABLE)
| column | type | nullable |
|---|---|---|
| ts | timestamp with time zone | YES |
| segment | text | YES |
| code | text | YES |
| time_ms | integer | YES |

### staging.temp_segment_matches (TABLE)
| column | type | nullable |
|---|---|---|
| segment_id | integer | YES |
| activity_id | bigint | YES |
| activity_start_point | integer | YES |
| activity_end_point | integer | YES |
| activity_path | USER-DEFINED | YES |
| match_confirmed | boolean | YES |
| match_confidence | numeric | YES |
| match_ts_utc | timestamp with time zone | YES |

### staging.temp_training_status_raw (TABLE)
| column | type | nullable |
|---|---|---|
| ts_utc | timestamp with time zone | YES |
| vo2_max_value | numeric | YES |
| altitude_current_m | integer | YES |
| altitude_acclimation_m | integer | YES |
| altitude_acclimation_pct | numeric | YES |
| heat_acclimation_pct | numeric | YES |
| training_load_acute | numeric | YES |
| training_load_min | numeric | YES |
| training_load_max | numeric | YES |

### staging.vw_activity_by_distance (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| elapsed_duration_s | numeric | YES |
| elev_raw | numeric | YES |

### staging.vw_activity_flatten (VIEW)
| column | type | nullable |
|---|---|---|
| activity_id | bigint | YES |
| activity_type_id | smallint | YES |
| activity_detail_index | integer | YES |
| ts_utc | timestamp with time zone | YES |
| moving_duration_s | integer | YES |
| elapsed_duration_s | integer | YES |
| duration_s | integer | YES |
| distance_mm | bigint | YES |
| latitude | numeric | YES |
| longitude | numeric | YES |
| heartrate_bpm | integer | YES |
| cadence_spm | smallint | YES |
| air_temp_c | smallint | YES |
| elevation_m | numeric | YES |
| speed_mmps | bigint | YES |
| vertical_speed_mps | numeric | YES |
| vert_oscillation_cm | numeric | YES |
| stride_length_cm | numeric | YES |
| vert_ratio | numeric | YES |
| ground_contact_time_ms | smallint | YES |
| ground_contact_balance_left | numeric | YES |
| performance_condition | smallint | YES |
| loc_point | USER-DEFINED | YES |
| elevation_smooth_m | numeric | YES |

### staging.vw_album_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| album_id | text | YES |
| album_name | text | YES |
| album_name_clean | text | YES |
| album_name_search | text | YES |
| updated_at_utc | timestamp with time zone | YES |

### staging.vw_artist_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| artist_id | text | YES |
| artist_name | text | YES |
| artist_name_search | text | YES |
| updated_at_utc | timestamp with time zone | YES |

### staging.vw_elevation_backward (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_elevation_bumps (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_elevation_forward (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_elevation_median (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_elevation_medians (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |

### staging.vw_elevation_neighbors (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| prior_elev | numeric | YES |
| next_elev | numeric | YES |
| inc_delta | numeric | YES |
| out_delta | numeric | YES |
| valid_point | boolean | YES |
| extreme_point | boolean | YES |

### staging.vw_elevation_weighted_avg (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_large_pull (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_listening_history_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| played_at_utc | timestamp with time zone | YES |
| isrc | text | YES |
| playlist_id | text | YES |

### staging.vw_now_playing_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| played_at_utc | timestamp with time zone | YES |
| isrc | text | YES |
| playlist_id | text | YES |

### staging.vw_playlist_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| playlist_id | text | YES |
| isrc | text | YES |
| track_order | bigint | YES |

### staging.vw_small_pull (VIEW)
| column | type | nullable |
|---|---|---|
| meter | integer | YES |
| curr_elev | numeric | YES |
| new_elev | numeric | YES |

### staging.vw_track_inserts (VIEW)
| column | type | nullable |
|---|---|---|
| track_id | text | YES |
| track_name | text | YES |
| track_name_clean | text | YES |
| track_name_search | text | YES |
| track_isrc | text | YES |
| artist_id | text | YES |
| artist_display_name | text | YES |
| album_id | text | YES |
| release_date | date | YES |
| available_in_us | boolean | YES |
| total_markets | integer | YES |
| popularity | integer | YES |
| duration_ms | integer | YES |
| updated_at_utc | timestamp with time zone | YES |

### staging.vw_track_inserts_from_listen_history (VIEW)
| column | type | nullable |
|---|---|---|
| track_id | text | YES |
| track_name | text | YES |
| track_name_clean | text | YES |
| track_name_search | text | YES |
| track_isrc | text | YES |
| artist_id | text | YES |
| artist_display_name | text | YES |
| album_id | text | YES |
| release_date | date | YES |
| available_in_us | boolean | YES |
| total_markets | integer | YES |
| popularity | integer | YES |
| duration_ms | integer | YES |
| updated_at_utc | timestamp with time zone | YES |

## Schema tasks

### tasks.acceptable_failures (TABLE)
| column | type | nullable |
|---|---|---|
| acceptable_failures | smallint | YES |

### tasks.fact_configuration (TABLE)
| column | type | nullable |
|---|---|---|
| fact_id | integer | NO |
| staging_id | integer | YES |
| task_id | integer | YES |
| fact_name | text | YES |
| data_type | text | YES |
| extraction_sql | text | YES |
| infer_values | boolean | YES |
| forecast_values | boolean | YES |
| is_unique_constraint | boolean | YES |
| interpolation_ts | boolean | YES |
| interpolation_destination_table | text | YES |
| interpolate_values | boolean | YES |
| value_recency | timestamp with time zone | YES |

**Foreign keys:**
- `staging_id` -> `tasks.staging_configuration.staging_id` (fk_fact_to_staging)
- `task_id` -> `tasks.task_configuration.task_id` (fk_fact_to_task)

**Indexes:**
- `fact_configuration_pkey`: CREATE UNIQUE INDEX fact_configuration_pkey ON tasks.fact_configuration USING btree (fact_id)

### tasks.staging_configuration (TABLE)
| column | type | nullable |
|---|---|---|
| staging_id | integer | NO |
| task_id | integer | YES |
| staging_name | text | YES |
| staging_description | text | YES |
| destination_table | text | YES |
| cross_join_condition | text | YES |
| filter_condition | text | YES |
| source_table | text | YES |

**Foreign keys:**
- `task_id` -> `tasks.task_configuration.task_id` (fk_staging_to_task)

**Indexes:**
- `staging_configuration_pkey`: CREATE UNIQUE INDEX staging_configuration_pkey ON tasks.staging_configuration USING btree (staging_id)

### tasks.task_config (TABLE)
| column | type | nullable |
|---|---|---|
| task_name | text | NO |
| task_description | text | YES |
| task_priority | integer | YES |
| task_frequency | text | YES |
| task_interval | numeric | YES |
| api_function | text | YES |
| api_service_name | text | YES |
| api_loop_type | text | YES |
| api_post_processing | text | YES |
| python_function | text | YES |
| last_calendar_field | text | YES |
| updated_through_date | date | YES |
| api_parameters | text | YES |

**Foreign keys:**
- `api_service_name` -> `api_services.api_service_list.api_service_name` (task_config_api_service_name_fkey)

**Indexes:**
- `task_config_pkey`: CREATE UNIQUE INDEX task_config_pkey ON tasks.task_config USING btree (task_name)

### tasks.task_configuration (TABLE)
| column | type | nullable |
|---|---|---|
| task_id | integer | NO |
| task_name | text | YES |
| last_executed_utc | timestamp with time zone | YES |
| last_succeeded_utc | timestamp with time zone | YES |
| last_failed_utc | timestamp with time zone | YES |
| next_planned_execution_utc | timestamp with time zone | YES |
| last_failure_message | text | YES |
| display_icon | text | YES |
| task_description | text | YES |
| task_priority | integer | YES |
| task_frequency | text | YES |
| task_start_hour | integer | YES |
| task_stop_hour | integer | YES |
| task_interval | integer | YES |
| api_service_name | text | YES |
| api_function_name | text | YES |
| python_execution_function | text | YES |
| consecutive_failures | smallint | YES |

**Indexes:**
- `task_configuration_pkey`: CREATE UNIQUE INDEX task_configuration_pkey ON tasks.task_configuration USING btree (task_id)

### tasks.task_executions (TABLE)
| column | type | nullable |
|---|---|---|
| execution_id | integer | NO |
| task_id | integer | NO |
| task_name | character varying | NO |
| status | character varying | YES |
| started_at | timestamp without time zone | YES |
| completed_at | timestamp without time zone | YES |
| result | jsonb | YES |
| error_message | text | YES |

**Foreign keys:**
- `task_id` -> `tasks.task_configuration.task_id` (task_executions_task_id_fkey)

**Indexes:**
- `task_executions_pkey`: CREATE UNIQUE INDEX task_executions_pkey ON tasks.task_executions USING btree (execution_id)

### tasks.vw_task_execution (VIEW)
| column | type | nullable |
|---|---|---|
| task_name | text | YES |
| task_description | text | YES |
| task_frequency | text | YES |
| task_interval | numeric | YES |
| task_priority | integer | YES |
| api_function | text | YES |
| api_service_name | text | YES |
| api_loop_type | text | YES |
| api_post_processing | text | YES |
| api_parameters | text | YES |
| api_service_function | text | YES |
| python_function | text | YES |
| last_calendar_field | text | YES |
| total_attempts | bigint | YES |
| consecutive_successes | bigint | YES |
| consecutive_failures | bigint | YES |
| last_execution_utc | timestamp with time zone | YES |
| last_success_utc | timestamp with time zone | YES |
| updated_through_utc | timestamp with time zone | YES |
| data_requires_catchup | boolean | YES |
| next_execution_utc | timestamp with time zone | YES |
| do_execute | boolean | YES |
| execution_logic | text | YES |

### tasks.vw_task_info (VIEW)
| column | type | nullable |
|---|---|---|
| task_id | integer | YES |
| task_name | text | YES |
| task_frequency | text | YES |
| task_interval | integer | YES |
| task_start_hour | integer | YES |
| task_stop_hour | integer | YES |
| next_planned_execution_utc | timestamp with time zone | YES |
| should_execute | boolean | YES |
| consecutive_failures | integer | YES |
| api_service_name | text | YES |
| api_function_name | text | YES |
| loop_strategy | text | YES |
| iter_path_keys | text | YES |
| iter_query_keys | text | YES |
| static_path_params | text | YES |
| static_query_params | text | YES |
| friendly_name | text | YES |
| run_extract | boolean | YES |
| python_login_function | text | YES |
| python_extraction_function | text | YES |
| flatten_sproc | text | YES |
| value_recency | timestamp with time zone | YES |
| python_execution_function | text | YES |
| run_python | boolean | YES |
| api_under_rate_limits | boolean | YES |

### tasks.vw_task_performance (VIEW)
| column | type | nullable |
|---|---|---|
| task_id | integer | YES |
| event_date | date | YES |
| login_ms | numeric | YES |
| extract_ms | numeric | YES |
| load_ms | numeric | YES |
| flatten_ms | numeric | YES |
| parse_ms | numeric | YES |
| interpolation_ms | numeric | YES |
| forecasting_ms | numeric | YES |
| python_ms | numeric | YES |
| admin_ms | numeric | YES |
| total_ms | numeric | YES |
| is_failure | boolean | YES |

### tasks.vw_task_summary_chart (VIEW)
| column | type | nullable |
|---|---|---|
| task_id | integer | YES |
| task_name | text | YES |
| task_frequency | text | YES |
| consecutive_failures | smallint | YES |
| time_ago_execution | text | YES |
| time_ago_next | text | YES |
| time_ago_value | text | YES |
| execution_minutes_ago | numeric | YES |
| value_recency_minutes_ago | numeric | YES |
| next_planned_execution_minutes | numeric | YES |
| value_recency | timestamp with time zone | YES |
| last_executed_utc | timestamp with time zone | YES |
| last_failed_utc | timestamp with time zone | YES |
| next_planned_execution_utc | timestamp with time zone | YES |
| last_failure_message | text | YES |
| is_active_failure | boolean | YES |
| execution_count | bigint | YES |
| success_count | bigint | YES |
| success_percentage | numeric | YES |
| max_executions | bigint | YES |
| login_ms | numeric | YES |
| extract_ms | numeric | YES |
| flatten_ms | numeric | YES |
| parse_ms | numeric | YES |
| interpolation_ms | numeric | YES |
| forecasting_ms | numeric | YES |
| python_ms | numeric | YES |
| admin_ms | numeric | YES |
| total_ms | numeric | YES |
| increasing_steps | text | YES |

## Schema volleyball

### volleyball.game_events (TABLE)
| column | type | nullable |
|---|---|---|
| event_id | integer | NO |
| game_id | integer | NO |
| kind | text | NO |
| message | text | YES |
| recorded_at | timestamp with time zone | NO |
| created_at | timestamp with time zone | NO |

**Foreign keys:**
- `game_id` -> `volleyball.games.game_id` (game_events_game_id_fkey)

**Indexes:**
- `game_events_pkey`: CREATE UNIQUE INDEX game_events_pkey ON volleyball.game_events USING btree (event_id)
- `ix_game_events_game_recorded`: CREATE INDEX ix_game_events_game_recorded ON volleyball.game_events USING btree (game_id, recorded_at DESC)

### volleyball.games (TABLE)
| column | type | nullable |
|---|---|---|
| game_id | integer | NO |
| team_b_name | text | NO |
| status | text | NO |
| started_at | timestamp with time zone | YES |
| completed_at | timestamp with time zone | YES |
| label | text | YES |
| notes | text | YES |
| created_at | timestamp with time zone | NO |
| partner_number | integer | YES |
| partner_name | text | YES |
| first_entry_at | timestamp with time zone | YES |

**Indexes:**
- `games_pkey`: CREATE UNIQUE INDEX games_pkey ON volleyball.games USING btree (game_id)

### volleyball.points (TABLE)
| column | type | nullable |
|---|---|---|
| point_id | integer | NO |
| game_id | integer | NO |
| scoring_team | text | NO |
| recorded_at | timestamp with time zone | NO |
| created_at | timestamp with time zone | NO |
| event_type | text | YES |

**Foreign keys:**
- `game_id` -> `volleyball.games.game_id` (points_game_id_fkey)

**Indexes:**
- `points_pkey`: CREATE UNIQUE INDEX points_pkey ON volleyball.points USING btree (point_id)

