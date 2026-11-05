# Data Dictionary

Per-domain data dictionary. Each section records the grain of key tables/views, the canonical source for each concept, and the rules that govern the data, marked `enforced` (DB constraint) or `convention` (application agreement) with a one-line why.

## activities

### activities.activities (table)
- **Grain:** one row per recorded activity (workout), keyed by `activity_id`.
- **Canonical source:** `activities.activities`; populated by the activity sync/import worker from Garmin data (staging.api_imports flow); `activity_path`/`is_downloaded` set later by `backend/backend_functions/activity_smoothing.build_activity_path` from `activities.activity_details_distance`.
- **Rules:** `enforced` PK on `activity_id`; `convention` one row per activity; `activity_path` nullable until GPX downloaded; timestamps UTC.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | NO | Surrogate activity id | count | schema.generated.md | schema.generated.md | confirmed |
| `activity_name` | text | YES | Garmin activity name/title | text | backend/pifitness/activities/queries.py | schema.generated.md | inferred |
| `activity_type_id` | smallint | YES | Garmin activity type id | id | schema.generated.md | schema.generated.md | inferred |
| `activity_type_name` | text | YES | Garmin activity type name (e.g. running, walking) | text | backend/backend_functions/activity_smoothing.py (filters walks/hikes) | schema.generated.md | inferred |
| `distance_m` | numeric | YES | Activity distance | metres | schema.generated.md | schema.generated.md | confirmed |
| `duration_s` | numeric | YES | Activity duration | seconds | schema.generated.md | schema.generated.md | confirmed |
| `duration_elapsed_s` | numeric | YES | Elapsed duration including pauses | seconds | schema.generated.md | schema.generated.md | confirmed |
| `duration_moving_s` | integer | YES | Moving duration excluding pauses | seconds | schema.generated.md | schema.generated.md | confirmed |
| `calories` | integer | YES | Calories burned | kcal | schema.generated.md | schema.generated.md | confirmed |
| `calories_bmr` | integer | YES | BMR calories | kcal | schema.generated.md | schema.generated.md | confirmed |
| `steps` | integer | YES | Step count | count | schema.generated.md | schema.generated.md | confirmed |
| `stride_length_cm_avg` | integer | YES | Average stride length | cm | name suffix | schema.generated.md | inferred |
| `cadence_double_spm_max` | integer | YES | Max double (left+right) cadence | steps/min | name suffix | schema.generated.md | inferred |
| `cadence_running_spm_max` | integer | YES | Max running cadence | steps/min | name suffix | schema.generated.md | inferred |
| `cadence_running_spm_avg` | numeric | YES | Average running cadence | steps/min | name suffix | schema.generated.md | inferred |
| `speed_mps_max` | numeric | YES | Max speed | m/s | name suffix | schema.generated.md | inferred |
| `speed_mps_avg` | numeric | YES | Average speed | m/s | name suffix | schema.generated.md | inferred |
| `heartrate_max` | integer | YES | Max heart rate | bpm | name suffix | schema.generated.md | inferred |
| `heartrate_avg` | integer | YES | Average heart rate | bpm | name suffix | schema.generated.md | inferred |
| `start_timestamp_utc` | bigint | YES | Start epoch | ms since epoch | schema.generated.md | schema.generated.md | confirmed |
| `start_time_utc` | timestamptz | YES | Activity start time | UTC timestamptz | schema.generated.md | schema.generated.md | confirmed |
| `end_time_utc` | timestamptz | YES | Activity end time | UTC timestamptz | schema.generated.md | schema.generated.md | confirmed |
| `latitude_start` | numeric | YES | Start latitude | degrees | name suffix | schema.generated.md | inferred |
| `latitude_end` | numeric | YES | End latitude | degrees | name suffix | schema.generated.md | inferred |
| `longitude_start` | numeric | YES | Start longitude | degrees | name suffix | schema.generated.md | inferred |
| `longitude_end` | numeric | YES | End longitude | degrees | name suffix | schema.generated.md | inferred |
| `elevation_m_min` | numeric | YES | Min elevation | m | name suffix | schema.generated.md | inferred |
| `elevation_m_max` | numeric | YES | Max elevation | m | name suffix | schema.generated.md | inferred |
| `elevation_m_gain` | numeric | YES | Elevation gain | m | name suffix | schema.generated.md | inferred |
| `elevation_m_loss` | numeric | YES | Elevation loss | m | name suffix | schema.generated.md | inferred |
| `temperature_c_min` | smallint | YES | Min temperature | deg C | name suffix | schema.generated.md | inferred |
| `temperature_c_max` | smallint | YES | Max temperature | deg C | name suffix | schema.generated.md | inferred |
| `water_ml_estimated` | integer | YES | Estimated water loss | ml | name suffix | schema.generated.md | inferred |
| `training_load` | numeric | YES | Training load | load | schema.generated.md | schema.generated.md | inferred |
| `training_effect_aerobic` | numeric | YES | Aerobic training effect | 0-5 scale | name suffix | schema.generated.md | inferred |
| `training_effect_anerobic` | numeric | YES | Anaerobic training effect | 0-5 scale | name suffix | schema.generated.md | inferred |
| `intensity_minutes_moderate` | integer | YES | Moderate intensity minutes | min | name suffix | schema.generated.md | inferred |
| `intensity_minutes_vigorous` | integer | YES | Vigorous intensity minutes | min | name suffix | schema.generated.md | inferred |
| `is_downloaded` | boolean | YES | GPX/path downloaded flag | boolean | backend/backend_functions/activity_smoothing.py | schema.generated.md | confirmed |
| `activity_path` | USER-DEFINED | YES | Stored activity geometry path | geometry | backend/backend_functions/activity_smoothing.build_activity_path | schema.generated.md | confirmed |

### activities.activity_details (table)
- **Grain:** one row per timestamp sample within an activity (`activity_id`, `ts_utc`).
- **Canonical source:** `activities.activity_details`.
- **Rules:** `enforced` PK (`activity_id`, `ts_utc`); `convention` samples ordered by ts_utc; FK to activities.activities (app-level).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ts_utc` | timestamp with time zone | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `moving_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mm` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heartrate_bpm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cadence_spm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `air_temp_c` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `speed_mmps` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vertical_speed_mps` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_oscillation_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `stride_length_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_time_ms` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_balance_left` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `performance_condition` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m_smooth` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_reference` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `loc_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `grid_5m_00_id` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.activity_details_distance (table)
- **Grain:** one row per distance-binned sample; derived for path building.
- **Canonical source:** `activities.activity_details_distance`.
- **Rules:** `convention` derived table; PK not defined; FK app-level.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_ms` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heartrate_bpm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cadence_spm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `air_temp_c` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `speed_mmps` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vertical_speed_mps` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_oscillation_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `stride_length_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_time_ms` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_balance_left` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `performance_condition` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m_smooth` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_reference` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `loc_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `grid_5m_00_id` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.courses (table)
- **Grain:** one row per named course/route.
- **Canonical source:** `activities.courses`.
- **Rules:** `convention` course metadata; referenced by segments.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `course_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `course_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `created_at` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `course_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `course_distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `course_elevation_gain_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.segments (table)
- **Grain:** one row per defined segment.
- **Canonical source:** `activities.segments`.
- **Rules:** `enforced` PK `segment_id`; `convention` segment geometry in activities schema.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_reference_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_gain` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_loss` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `utm_srid` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path_transformed` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.segments_details (table)
- **Grain:** one row per segment match occurrence.
- **Canonical source:** `activities.segments_details`.
- **Rules:** `convention` match results; FKs app-level.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_point` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_end_point` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `max_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_cadence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_temp` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_osc` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_gct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_perf` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_balance` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_stride_length` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.distinct_segments (table)
- **Grain:** one row per distinct segment geometry.
- **Canonical source:** `activities.distinct_segments`.
- **Rules:** `convention` dedup layer.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_seg_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.metric_list (table)
- **Grain:** one row per activity metric descriptor.
- **Canonical source:** `activities.metric_list`.
- **Rules:** `convention` lookup table.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_type_id` | smallint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `metric_name` | text | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `metric_unit` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_seen_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.overlap_queue (table)
- **Grain:** one row per queued overlap-processing job.
- **Canonical source:** `activities.overlap_queue`.
- **Rules:** `convention` queue table.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `queue_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id_a` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id_b` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `created_at` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.activity_processing_queue (table)
- **Grain:** one row per queued activity-processing job.
- **Canonical source:** `activities.activity_processing_queue`.
- **Rules:** `convention` queue table.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `created_at_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.race_predictions_raw (table)
- **Grain:** one row per race prediction snapshot.
- **Canonical source:** `activities.race_predictions_raw`.
- **Rules:** `convention` raw predictions.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `ts_utc` | timestamp with time zone | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prediction_5k_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prediction_10k_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prediction_half_marathon_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prediction_marathon_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.segment_match_exclusions (table)
- **Grain:** one row per excluded segment match.
- **Canonical source:** `activities.segment_match_exclusions`.
- **Rules:** `enforced` FKs to activities and segments.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.elevation_reference (table)
- **Grain:** one row per elevation reference point.
- **Canonical source:** `activities.elevation_reference`.
- **Rules:** `convention` reference data.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `grid_5m_00_id` | USER-DEFINED | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_elevation` | double precision | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `obs_count` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_updated` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elev_stddev` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_hammered` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_sanded` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.activity_resampled_1m (table)
- **Grain:** one row per minute-bucketed activity metrics.
- **Canonical source:** `activities.activity_resampled_1m`.
- **Rules:** `convention` derived aggregate.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_header_stats (view)
- **Grain:** one row per activity with formatted header stats.
- **Canonical source:** derived from `activities.activities`.
- **Rules:** `convention` read-only view.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_basic` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `duration_display` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `pace_display` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hr_median` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hr_maximum` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_summary (view)
- **Grain:** one row per activity summary.
- **Canonical source:** derived from `activities.activities`.
- **Rules:** `convention` read-only view.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_pace` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_time_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `all_time_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_time_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior_time_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_delta_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior_delta_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_pace` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.segment_matches (table)
- **Grain:** one row per segment matches (table); see schema.generated.md for columns.
- **Canonical source:** `activities.segment_matches`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confirmed` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_ts_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.steps_raw (table)
- **Grain:** one row per steps raw (table); see schema.generated.md for columns.
- **Canonical source:** `activities.steps_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `ts_utc` | timestamp with time zone | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `step_goal` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `total_steps` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `total_distance_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_activity_segment_metas (table)
- **Grain:** one row per temp activity segment metas (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_activity_segment_metas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segment_details (table)
- **Grain:** one row per temp segment details (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `row_asc` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `row_desc` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segment_matches_downselect (table)
- **Grain:** one row per temp segment matches downselect (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matches_downselect`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_start_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_end_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `full_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ruggedness` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `utm_srid` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `short_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `dist_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `full_path_transformed` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `freschet_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `polygon_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hausdorff_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heading_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segment_matches_v1 (table)
- **Grain:** one row per temp segment matches v1 (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matches_v1`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confirmed` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_ts_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segment_matching_activities (table)
- **Grain:** one row per temp segment matching activities (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matching_activities`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segment_matching_segments (table)
- **Grain:** one row per temp segment matching segments (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matching_segments`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.temp_segments_v1 (table)
- **Grain:** one row per temp segments v1 (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segments_v1`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_reference_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_gain` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_loss` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.training_cycle_dates (table)
- **Grain:** one row per training cycle dates (table); see schema.generated.md for columns.
- **Canonical source:** `activities.training_cycle_dates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `cycle_start_utc` | timestamp with time zone | NO | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cycle_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_elevation_deltas (view)
- **Grain:** one row per vw activity elevation deltas (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_elevation_deltas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_ids_to_sync (view)
- **Grain:** one row per vw activity ids to sync (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_ids_to_sync`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `max_points` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_path_updates (view)
- **Grain:** one row per vw activity path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_segments_aggregated (view)
- **Grain:** one row per vw activity segments aggregated (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_segments_aggregated`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `id_val` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `meta_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `path_coords` | json | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `map_timestamps` | json | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `x_time` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `x_distance` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_time` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_time_delta` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_hr` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_hr_delta` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_elevation` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_cadence` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_balance` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_performance` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_vert_ratio` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_osc` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_temp` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_stride` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_gct` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `y_speed` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_sleep (view)
- **Grain:** one row per vw activity sleep (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_sleep`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `awake_hours_prior` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_score` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_sleep_hours` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `one_day_sleep_hours` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_summary_segment_creation (view)
- **Grain:** one row per vw activity summary segment creation (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_summary_segment_creation`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `child_courses` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `child_segments` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `matched_courses` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `matched_segments` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_training_status (view)
- **Grain:** one row per vw activity training status (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_training_status`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vo2_max_value` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `altitude_acclimation_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heat_acclimation_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_acute` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_readiness` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_mid` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_max` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_min` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `resting_hr_asleep` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `resting_hr_awake` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_score` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `light_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `deep_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `awake_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `rem_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `awake_s_before_activity` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_end_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_end_date` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_date` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_activity_weights (view)
- **Grain:** one row per vw activity weights (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_weights`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `weight_lb` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `muscle_lb` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `fat_lb` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_course_review (view)
- **Grain:** one row per vw course review (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_course_review`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_reference_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `effort_label` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_gain` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_loss` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `matched_activities` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_event_utc` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `course_idx` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `max_course_idx` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `new_count` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_elevation_smooth (view)
- **Grain:** one row per vw elevation smooth (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_elevation_smooth`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `lat5` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `long5` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_raw` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `geog` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_last_activity_id_by_type (view)
- **Grain:** one row per vw last activity id by type (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_last_activity_id_by_type`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_matched_segment_path_updates (view)
- **Grain:** one row per vw matched segment path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_matched_segment_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_overlapped_segments (view)
- **Grain:** one row per vw overlapped segments (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_overlapped_segments`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `match_rank` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `effort_label` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `matched_activities` | ARRAY | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_possible_segment_matches (view)
- **Grain:** one row per vw possible segment matches (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_possible_segment_matches`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_date` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `match_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `effort_label` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_pr_hunter (view)
- **Grain:** one row per vw pr hunter (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_pr_hunter`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vs_date` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `attempt_label` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `total_attempts` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `predicted_time_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vs_pace` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `estimated_pace` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prestige_score` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `weight_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vo2_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `acute_load_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `load_max_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_report_segment_efforts (view)
- **Grain:** one row per vw report segment efforts (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_report_segment_efforts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_cadence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_stride_length` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_gct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `all_time_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `total_attempts` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior_time_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_time_excl_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior_delta_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_delta_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior10_avg_cadence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior10_avg_stride_length` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior10_avg_gct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `prior10_avg_vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_run_timing (view)
- **Grain:** one row per vw run timing (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_run_timing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `run_age` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `music_start` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `music_end` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `music_duration_ms` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_comps (view)
- **Grain:** one row per vw segment comps (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_comps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `id_val` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `meta_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_m_smooth` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_ms` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_ms_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `longitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `latitude` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heartrate_bpm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heartrate_bpm_delta` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cadence_spm` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_balance_left` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `performance_condition` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vert_oscillation_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `air_temp_c` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `stride_length_cm` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ground_contact_time_ms` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `speed_mmps` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_create_auto_approve (view)
- **Grain:** one row per vw segment create auto approve (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_create_auto_approve`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_start_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_end_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `dist_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `polygon_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hausdorff_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `freschet_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `approval_phase` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_create_from_details (view)
- **Grain:** one row per vw segment create from details (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_create_from_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_reference_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_gain` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_loss` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_leaderboard (view)
- **Grain:** one row per vw segment leaderboard (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_leaderboard`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cycle_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `all_time_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_365_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `current_cycle_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `recency_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `pace_str` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `weight_lb` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `muscle_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `fat_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `vo2_max_value` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `altitude_acclimation_m` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `heat_acclimation_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_acute` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_pct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_readiness` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_mid` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_max` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `training_load_min` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `resting_hr_asleep` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `resting_hr_awake` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_duration_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_hours` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `sleep_score` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `light_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `deep_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `awake_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `rem_sleep_s` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `awake_hours` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `max_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_cadence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_temp` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_osc` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_gct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_perf` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_balance` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_stride_length` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_matching_downselect_confidence (view)
- **Grain:** one row per vw segment matching downselect confidence (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_matching_downselect_confidence`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `confirmed` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_start_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_end_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `dist_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `polygon_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `freschet_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hausdorff_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `confidence_score` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_matching_downselect_scores (view)
- **Grain:** one row per vw segment matching downselect scores (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_matching_downselect_scores`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_start_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_end_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `dist_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ruggedness` | double precision | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_path_updates (view)
- **Grain:** one row per vw segment path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_reference_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_id` | smallint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `reference_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ascent_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `descent_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_point` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segment_racing (view)
- **Grain:** one row per vw segment racing (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_racing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `effort_count` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_effort` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segments_details_ranked (view)
- **Grain:** one row per vw segments details ranked (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segments_details_ranked`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `cycle_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_start_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_end_point` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `recency_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `all_time_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_365_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `current_cycle_rank` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_time_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elapsed_duration_s` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `max_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_hr` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_cadence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_temp` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_osc` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_vert_ratio` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_gct` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_perf` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_balance` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `avg_stride_length` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_path` | USER-DEFINED | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_segments_effort_stats (view)
- **Grain:** one row per vw segments effort stats (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segments_effort_stats`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `is_course` | boolean | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `activity_type_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `distance_mi` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `elevation_gain` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `last_effort` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `matched_activity_count` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_temp_segment_matches_downselect (view)
- **Grain:** one row per vw temp segment matches downselect (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_temp_segment_matches_downselect`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `segment_id` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `effort_date` | date | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_start_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `best_end_dist` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `ruggedness` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `dist_deviation` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `polygon_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `hausdorff_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `freschet_deviation_m` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `confidence` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_unmatched_activitites (view)
- **Grain:** one row per vw unmatched activitites (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_unmatched_activitites`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `activity_id` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

### activities.vw_watch_music_heard (view)
- **Grain:** one row per vw watch music heard (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_watch_music_heard`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `playlist_id` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `playlist_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `track_id` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `isrc` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `track_order` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `track_name_clean` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `artist_display_name` | text | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `duration_ms` | integer | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `music_start` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `start_cumulative_ms` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `end_cumulative_ms` | bigint | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `pct_listened` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `total_listen_ms` | numeric | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |
| `played_at_utc` | timestamp with time zone | YES | Unknown � investigation incomplete | unknown | schema.generated.md | schema.generated.md | unknown |

## api_services

## api_services

API service registry and OAuth/credential store for third-party integrations (Spotify, Garmin, etc.). Tables are maintained by `backend/pifitness/admin/queries.py` (admin CRUD) and written/read by the integration providers; tokens and Spotify auth metadata were moved here from `_migration` by `deploy/migrations/001__int19b__move_tokens_to_api_services.sql` (INT19b).

### api_services.api_service_list (table)
- **Purpose:** registry of configured external API services, including their function name, credential requirements, and rate-limit state.
- **Grain:** one row per registered API service, keyed by `api_service_name`.
- **Primary/candidate key:** `api_service_list_pkey` on `api_service_name` (enforced). No other declared unique key.
- **Canonical source and lifecycle:** inserted/updated/deleted via admin endpoints backed by `backend/pifitness/admin/queries.py` (`INSERT` line 53, `UPDATE` line 67, `DELETE` line 80); read by `SELECT *` (line 28) and `SELECT DISTINCT api_service_name` (line 39).
- **Consumers:** `backend/backend_functions/music_service.py:50` and `backend/pifitness/auth/status.py:48` read `rate_limit_cleared_utc` for Spotify to determine rate-limit state; `backend/pifitness/music/router.py:454` and `backend/pifitness/integrations/spotify/provider.py:73` read from the table; projected through `tasks.vw_task_info`.
- **Relationships:** `api_service_name` referenced by `api_services.function_library.api_service_name` (FK `fk_svc_function_to_service`).
- **Rules:** `enforced` PK on `api_service_name`; `convention` service registry; `rate_limit_cleared_utc` is a future UTC timestamp — while now() < cleared, the service is rate-limited; timestamps UTC.
- **Evidence/confidence:** `backend/pifitness/admin/queries.py`, `backend/backend_functions/music_service.py`, `backend/pifitness/auth/status.py`; `schema.generated.md` lines 881-891. Structural facts confirmed; rate-lift semantics inferred from consumer reads (medium).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `api_service_name` | text | NO | Registered service name (e.g. 'Spotify', 'Garmin') | Identifier, PK | admin create | api_service_list_pkey | confirmed |
| `api_service_function` | text | YES | Backend function name used for the service | free text | admin create/update | queries.py | confirmed |
| `api_credential_requirements` | text | YES | Description of credentials needed for the service | free text | admin create/update | queries.py | confirmed |
| `rate_limit_detected_utc` | timestamp with time zone | YES | When a rate limit was detected | UTC timestamptz | provider/write path | queries.py/music_service.py | confirmed |
| `rate_limit_cleared_utc` | timestamp with time zone | YES | When the rate limit clears (future); service rate-limited until then | UTC timestamptz | provider/write path | music_service.py:50; auth/status.py:48 | confirmed |

### api_services.api_tokens (table)
- **Purpose:** OAuth/API access+refresh tokens per external service, used for cross-device session sync (DB tier) with a local file-cache mirror.
- **Grain:** one row per (service, active token) — `id` is the row key; the active token for a service is the row with `is_active=true` having the latest `updated_at_utc`.
- **Primary/candidate key:** `api_tokens_pkey` on `id` (enforced, identity sequence). Index `api_tokens_service_name_is_active_idx` on (`service_name`, `is_active`).
- **Canonical source and lifecycle:** written by `backend/pifitness/integrations/_common.py` (`save_token_to_db`: `INSERT ... CURRENT_TIMESTAMP`, lines 110-115; deactivate prior active token via `UPDATE ... SET is_active=false`, line 99); Garmin and Spotify providers load/save through it. A token row is never deleted — it is superseded by deactivating (`is_active=false`) when refreshed.
- **Consumers:** `_common.py:48` reads the latest active `token_data` for a service; `backend/pifitness/auth/status.py:83` reads Garmin `refresh_token_expires_at_utc`; `backend/pifitness/integrations/garmin/provider.py` bridges DB and file cache.
- **Relationships:** none declared (no FK); `service_name` is a loose reference to `api_services.api_service_list.api_service_name` (application convention).
- **Rules:** `enforced` PK on `id`; `convention` `is_active` boolean (only one active per service at a time, enforced by app logic not DB), `token_data` is JSONB-ish encrypted/opaque token payload treated as secret (table truncated in dev sync per `tools/db/sync-prod-to-dev.ps1`), timestamps UTC (`created_at_utc`, `updated_at_utc`, `refresh_token_expires_at_utc`).
- **Evidence/confidence:** `backend/pifitness/integrations/_common.py`, `backend/pifitness/auth/status.py`, `backend/pifitness/integrations/garmin/provider.py`, `deploy/migrations/001__int19b__move_tokens_to_api_services.sql`; `schema.generated.md` lines 893-906. Structural facts confirmed; "one active per service" is app convention (not a DB constraint).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `id` | integer | NO | Surrogate row identifier | Identifier, identity (sequence) | insert-assigned | schema_only.sql identity; api_tokens_pkey | confirmed |
| `service_name` | text | NO | External service the token belongs to | free text | insert request | _common.py | confirmed |
| `token_data` | text | NO | Opaque/encrypted token payload (secret) | secret | insert (save_token_to_db) | _common.py | confirmed |
| `created_at_utc` | timestamp with time zone | YES | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |
| `updated_at_utc` | timestamp with time zone | YES | Last refresh/update time | UTC timestamptz | update on refresh | _common.py | confirmed |
| `is_active` | boolean | NO | Whether this is the current active token | true/false; only one active per service (app convention) | insert=true; deactivate=false | _common.py | confirmed |
| `refresh_token_expires_at_utc` | timestamp with time zone | YES | Refresh token expiry | UTC timestamptz | insert-assigned | _common.py; auth/status.py:83 | confirmed |

### api_services.credentials (table)
- **Purpose:** encrypted API credentials per registered service — the single server-side source of truth for secrets (no duplicate secret in the frontend build).
- **Grain:** one row per service, keyed by `api_service_name`.
- **Primary/candidate key:** `credentials_pkey` on `api_service_name` (enforced). No FK declared.
- **Canonical source and lifecycle:** upserted via `backend/pifitness/admin/queries.py` (`INSERT ... ON CONFLICT (api_service_name)`, line 170); deleted via `DELETE WHERE api_service_name` (line 187).
- **Consumers:** `backend/pifitness/integrations/_common.py:20` reads `api_credentials` for a service; `backend/pifitness/config/queries.py` and `backend/pifitness/config/router.py` resolve it from the encrypted store for the browser.
- **Relationships:** `api_service_name` is a loose reference to `api_services.api_service_list.api_service_name` (application convention; no DB FK).
- **Rules:** `enforced` PK on `api_service_name`; `convention` `api_credentials` holds an encrypted secret (never logged/exported); no soft-delete (hard delete only).
- **Evidence/confidence:** `backend/pifitness/admin/queries.py`, `backend/pifitness/integrations/_common.py`, `backend/pifitness/config/queries.py`; `schema.generated.md` lines 908-915. Structural facts confirmed; encryption treatment is app convention (code comment).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `api_service_name` | text | NO | Service the credentials belong to | Identifier, PK | admin upsert | credentials_pkey | confirmed |
| `api_credentials` | text | YES | Encrypted service credentials (secret) | secret | admin upsert | config/queries.py; _common.py:20 | confirmed |

### api_services.function_library (table)
- **Purpose:** registry of callable API functions / data-extraction functions used by the task executioner, including how each function is invoked (loop strategy, iteration keys, static params) and how freshness is tracked.
- **Grain:** one row per registered function, keyed by `friendly_name`.
- **Primary/candidate key:** `function_library_pkey` on `friendly_name` (enforced). FK `fk_svc_function_to_service` (`api_service_name` -> `api_services.api_service_list.api_service_name`, enforced).
- **Canonical source and lifecycle:** created/updated/deleted via admin endpoints backed by `backend/pifitness/admin/queries.py` (`get_function_library` line 91, `INSERT` line 110, `UPDATE` line 126, `DELETE` line 139); `value_recency` touched by `backend/pifitness/worker/ultimate_task_executioner_v2.py:512`.
- **Consumers:** `backend/backend_functions/database_functions.py:213` reads `friendly_name`/`api_service_name`; `tasks.vw_task_info` view joins `function_library` (projects `python_extraction_function`); deploy migrations note that updates auto-reflect through the non-updatable `vw_task_info` view.
- **Relationships:** enforced FK to `api_services.api_service_list`; consumed by `tasks.vw_task_info` (view lineage).
- **Rules:** `enforced` PK + FK; `convention` function registry; `value_recency` timestamptz tracks freshness (compared to CURRENT_DATE in the executioner); no soft-delete.
- **Evidence/confidence:** `backend/pifitness/admin/queries.py`, `backend/backend_functions/database_functions.py`, `backend/pifitness/worker/ultimate_task_executioner_v2.py`; `schema.generated.md` lines 917-936. Structural facts confirmed; exact business meaning of `loop_strategy`/`iter_path_keys`/`static_*_params` not established in inspected code (low).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `friendly_name` | text | NO | Function display/key name | Identifier, PK | admin create | function_library_pkey | confirmed |
| `api_service_name` | text | YES | Owning service | FK -> api_services.api_service_list.api_service_name | admin create | fk_svc_function_to_service | confirmed |
| `api_function_name` | text | YES | Underlying API function name | free text | admin create/update | queries.py | confirmed |
| `python_extraction_function` | text | YES | Python extraction function invoked | free text | admin create/update | queries.py | confirmed |
| `flatten_sproc` | text | YES | Stored procedure used to flatten results | free text | admin create/update | queries.py | confirmed |
| `value_recency` | timestamp with time zone | YES | Freshness watermark for the function's values | UTC timestamptz | updated by executioner | ultimate_task_executioner_v2.py:512 | confirmed |
| `loop_strategy` | text | YES | How results are iterated/paged | free text | admin create/update | queries.py | low |
| `iter_path_keys` | text | YES | JSON path keys for iteration | free text | admin create/update | queries.py | low |
| `iter_query_keys` | text | YES | Query keys for iteration | free text | admin create/update | queries.py | low |
| `static_path_params` | text | YES | Static path parameters | free text | admin create/update | queries.py | low |
| `static_query_params` | text | YES | Static query parameters | free text | admin create/update | queries.py | low |

### api_services.spotify_auth_metadata (table)
- **Purpose:** Spotify OAuth authorization metadata — one row per authorization, recording when it was authorized, a hashed refresh token, and when it expires (6 months).
- **Grain:** one row per Spotify authorization, keyed by `id`.
- **Primary/candidate key:** `spotify_auth_metadata_pkey` on `id` (enforced, identity sequence). Unique index `spotify_auth_metadata_refresh_token_hash_key` on `refresh_token_hash`.
- **Canonical source and lifecycle:** inserted by `backend/pifitness/integrations/spotify/provider.py` (`store_spotify_auth_metadata`, line 324: `authorized_at_utc = CURRENT_TIMESTAMP`, `refresh_token_hash = sha256(refresh_token)`, `expires_at_utc = CURRENT_TIMESTAMP + 6 months`); rows are never updated — a new authorization inserts a new row. Moved here from `_migration.spotify_auth_metadata` by `deploy/migrations/001__int19b__move_tokens_to_api_services.sql`.
- **Consumers:** `backend/pifitness/auth/status.py:68` and `provider.py:351` read `MAX(expires_at_utc) WHERE is_active = true` to surface Spotify token expiry.
- **Relationships:** none declared (no FK).
- **Rules:** `enforced` PK on `id` + unique `refresh_token_hash`; `convention` `refresh_token_hash` is a SHA-256 hash (never the raw token — secret), `expires_at_utc` fixed at +6 months from authorization, `is_active` boolean marks the current authorization, timestamps UTC.
- **Evidence/confidence:** `backend/pifitness/integrations/spotify/provider.py`, `backend/pifitness/auth/status.py`; `deploy/migrations/001__int19b__move_tokens_to_api_services.sql`; `schema.generated.md` lines 938-950. Structural facts confirmed; `is_active` semantics (single active row?) inferred (medium).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `id` | integer | NO | Surrogate row identifier | Identifier, identity (sequence) | insert-assigned | schema_only.sql identity; spotify_auth_metadata_pkey | confirmed |
| `authorized_at_utc` | timestamp with time zone | NO | Authorization time | UTC timestamptz | insert = CURRENT_TIMESTAMP | provider.py:324 | confirmed |
| `refresh_token_hash` | text | YES | SHA-256 hash of the refresh token (secret) | secret, unique | insert = sha256(refresh_token) | provider.py:324 | confirmed |
| `expires_at_utc` | timestamp with time zone | NO | Token expiry | UTC timestamptz | insert = now + 6 months | provider.py:324 | confirmed |
| `is_active` | boolean | YES | Marks the current authorization | true/false; single-active semantics inferred | insert-assigned | auth/status.py:68 | medium |
| `created_at` | timestamp with time zone | YES | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |

## exercises

### exercises.exercise_attempts (table)
- **Purpose:** one row per confirmed Start/Stop exercise cycle — the attempt history for a timer.
- **Grain:** one row per attempt, keyed by `attempt_id`; scoped to one timer via `exercise_id`.
- **Primary/candidate key:** `exercise_attempts_pkey` on `attempt_id` (enforced). FK `exercise_attempts_exercise_id_fkey` (`exercise_id` -> `exercises.exercise_timers.exercise_id`, enforced, `ON DELETE RESTRICT`).
- **Canonical source and lifecycle:** `create_attempt` writes exactly one row per user-confirmed Start/Stop cycle (the in-progress run lives entirely in the client until confirmed). Deleted together with its timer on DELETE `/api/exercises/{id}` — attempts deleted first (RESTRICT), then the timer, in a single transaction scoped by `exercise_id` (OQ-1).
- **Consumers:** GET `/api/exercises/{exercise_id}` (last_attempt for the live progress bar), list summaries; serializer `_serialize_attempt`.
- **Relationships:** enforced FK to `exercises.exercise_timers` with `ON DELETE RESTRICT`.
- **Rules:** `enforced` PK + FK (RESTRICT); `convention` CHECK `paced_count >= 0` and `total_count >= paced_count` (code-evidenced `ExerciseValidationError`), `interval_seconds_used` CHECK > 0 (snapshot of the timer's interval in effect during the run — editing a timer later does NOT re-pace historical attempts), `started_at`/`ended_at` UTC timestamptz, `notes` unused in v1.
- **Evidence/confidence:** `backend/pifitness/exercises/queries.py` (create_attempt/delete_exercise docstrings), `router.py`, `schemas.py` (`ExerciseAttemptCreateRequest`); `schema.generated.md` lines 954-972. Structural facts confirmed; check constraints code-evidenced (not in generated inventory).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `attempt_id` | integer | NO | Surrogate identifier for the attempt | Identifier | assigned on insert | queries.py create_attempt; exercise_attempts_pkey | confirmed |
| `exercise_id` | integer | NO | Parent timer | FK -> exercises.exercise_timers.exercise_id (ON DELETE RESTRICT) | create_attempt | exercise_attempts_exercise_id_fkey | confirmed |
| `interval_seconds_used` | numeric | NO | Snapshot of the timer's pacing interval during this run | seconds, > 0 (CHECK) | snapshotted at run time | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `started_at` | timestamp with time zone | NO | Run start time (after the 5→1 countdown) | UTC timestamptz | create_attempt request | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `ended_at` | timestamp with time zone | NO | Run stop time (Stop pressed) | UTC timestamptz | create_attempt request | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `paced_count` | integer | NO | User-confirmed on-pace reps | count, >= 0 (CHECK) | create_attempt request | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `total_count` | integer | NO | User-confirmed total reps | count, >= paced_count (CHECK) | create_attempt request | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `notes` | text | YES | Optional notes | free text | unused in v1 | schemas.py ExerciseAttemptCreateRequest | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |

### exercises.exercise_timers (table)
- **Purpose:** master data for exercise timer definitions — a named pacing interval that users can create, edit, and delete.
- **Grain:** one row per exercise timer, keyed by `exercise_id`.
- **Primary/candidate key:** `exercise_timers_pkey` on `exercise_id` (enforced). Unique index `idx_exercise_timers_name` on `lower(name)` (enforced, case-insensitive unique name).
- **Canonical source and lifecycle:** created via POST `/api/exercises` (`create_exercise`), updated via PUT `/api/exercises/{id}` (`update_exercise`), hard-deleted via DELETE `/api/exercises/{id}` (`delete_exercise`, which first deletes that timer's attempts due to FK `ON DELETE RESTRICT`, all in one transaction — OQ-1). Independently editable; no soft-delete column.
- **Consumers:** GET `/api/exercises` (list with per-timer attempt aggregates, ordered by name), GET `/api/exercises/{exercise_id}`; frontend contract `frontend/pifitness/src/lib/types/exercises.ts`; serializers `_serialize_timer`, `_serialize_summary`.
- **Relationships:** `exercise_id` referenced by `exercises.exercise_attempts.exercise_id` (FK `ON DELETE RESTRICT` — attempts must be deleted before the timer).
- **Rules:** `enforced` PK + unique `lower(name)` index; `convention` name unique case-insensitively (DB unique index, pre-checked by app -> `ExerciseNameConflictError`), `interval_seconds` CHECK > 0 (code-evidenced; check constraints are not enumerated in the generated inventory — confirm against live catalog if needed), timestamps UTC (`timestamptz`), `notes` unused in v1.
- **Evidence/confidence:** `backend/pifitness/exercises/queries.py` (master-data lifecycle + delete_exercise docstrings), `router.py`, `schemas.py`; `schema.generated.md` lines 973-985. Structural facts confirmed; check constraints code-evidenced (not in generated inventory).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `exercise_id` | integer | NO | Surrogate identifier for the timer | Identifier | assigned on insert | queries.py create_exercise; exercise_timers_pkey | confirmed |
| `name` | text | NO | Display name, unique case-insensitively | free text, min 1 char | create/update request | schemas.py ExerciseCreateRequest | confirmed |
| `interval_seconds` | numeric | NO | Pacing interval | seconds, > 0 (CHECK) | create/update request | schemas.py (gt=0); queries.py | confirmed |
| `notes` | text | YES | Optional notes | free text | unused in v1 | schemas.py | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |
| `updated_at` | timestamp with time zone | NO | Row last-update time | UTC timestamptz | update-assigned | schema.generated.md | confirmed |

## food

### food.diary_entries (table)
- **Purpose:** one row per meal/snack log entry with nutrient snapshot.
- **Grain:** one row per diary entry, keyed by `entry_id`.
- **Primary/candidate key:** `diary_entries_pkey` on `entry_id`. FK `diary_entries_food_id_fkey` (nullable).
- **Canonical source and lifecycle:** written by insert/update_diary_entry; hard-deleted by delete_diary_entry; nutrients snapshotted at log time; kcal derived via _kcal_for.
- **Consumers:** GET /api/food/diary, /api/food/usual.
- **Rules:** enforced PK; convention logged_at UTC; name_snapshot captures name at log time.
- **Evidence/confidence:** backend/pifitness/food/queries.py (ENTRY_COLS, _kcal_for); schema.generated.md 989-1017. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `entry_id` | integer | NO | Surrogate id | Identifier | insert | diary_entries_pkey | confirmed |
| `food_id` | integer | YES | Food consumed | FK -> food.foods.food_id | request | diary_entries_food_id_fkey | confirmed |
| `name_snapshot` | text | NO | Name at log time | free text | food name at insert | queries.py | confirmed |
| `kcal_per_100g`..`caffeine_mg` | double precision | YES | Nutrient densities per 100g snapshot | g/mg per 100g | snapshotted | _NUTRIENT_KEYS | confirmed |
| `is_partial` | boolean | NO | Nutrients unknown at log time | boolean | insert-assigned | queries.py | confirmed |
| `qty` | double precision | NO | Quantity | number | request | queries.py | confirmed |
| `unit` | text | NO | Unit | text | request, normalized | _norm_unit | confirmed |
| `kcal` | double precision | YES | Computed energy | kcal | _kcal_for | queries.py | confirmed |
| `logged_at` | timestamp with time zone | NO | Consumption time | UTC timestamptz | request | ix_diary_day | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation | UTC timestamptz | insert | schema.generated.md | confirmed |

### food.foods (table)
- **Purpose:** canonical food catalog (manual + USDA + barcode + recipe-derived).
- **Grain:** one row per food item.
- **Primary/candidate key:** `foods_pkey` on `food_id`.
- **Canonical source and lifecycle:** written by food search/import and recipe save (source='recipe' food row); soft-deleted via is_deleted/deleted_at.
- **Consumers:** food.vw_food_search, food.vw_usual_foods, diary_entries.
- **Rules:** enforced PK; convention soft-delete; needs_review flags uncertain imports; quality_score numeric.
- **Evidence/confidence:** backend/pifitness/food/queries.py (FOOD_COLS); schema.generated.md 1018-1052. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `food_id` | integer | NO | Surrogate id | identifier | insert | foods_pkey | confirmed |
| `name` | text | NO | Food name | free text | request/import | ix_foods_name_trgm | confirmed |
| `source` | text | NO | Provenance | text (manual/usda/barcode/recipe) | insert | ix_foods_source_alive | confirmed |
| `fdc_id` | integer | YES | USDA FDC id | identifier | import | queries.py | confirmed |
| `barcode` | text | YES | Barcode | text | import | ix_foods_barcode | confirmed |
| `kcal_per_100g` | double precision | YES | Energy density | kcal/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `protein_g` | double precision | YES | Protein density | g/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `fat_g` | double precision | YES | Fat density | g/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `carbs_g` | double precision | YES | Carbs density | g/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `fiber_g` | double precision | YES | Fiber density | g/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `sugars_g` | double precision | YES | Sugars density | g/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `sodium_mg` | double precision | YES | Sodium density | mg/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `caffeine_mg` | double precision | YES | Caffeine density | mg/100g | import/snapshot | _NUTRIENT_KEYS | confirmed |
| `serving_size` | double precision | YES | Grams per serving_unit | g | import/recipe | _kcal_for | confirmed |
| `serving_unit` | text | YES | Serving unit label | text | import/recipe | _kcal_for | confirmed |
| `serving_text` | text | YES | Human serving description | free text | import | schema.generated.md | confirmed |
| `nutrients_raw` | jsonb | YES | Raw nutrient payload | json | import | schema.generated.md | confirmed |
| `normalized_from` | text | YES | Normalization source | text | food_normalize | schema.generated.md | confirmed |
| `needs_review` | boolean | NO | Needs manual review | boolean | import | ix_foods_needs_review | confirmed |
| `review_reason` | text | YES | Review reason | free text | import | schema.generated.md | confirmed |
| `quality_score` | double precision | NO | Quality score | number | import | schema.generated.md | confirmed |
| `is_deleted` | boolean | NO | Soft-delete flag | boolean | delete | ix_foods_source_alive | confirmed |
| `deleted_at` | timestamp with time zone | YES | Soft-delete time | UTC timestamptz | delete | schema.generated.md | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation | UTC timestamptz | insert | schema.generated.md | confirmed |

### food.recipe_cook_states (table)
- **Purpose:** per-recipe cook session state (checked steps/foods, timers).
- **Grain:** one row per recipe.
- **Primary/candidate key:** `recipe_cook_states_pkey` on `recipe_id`. FK -> food.recipes.recipe_id.
- **Canonical source and lifecycle:** created/updated during cooking; deleted on complete_recipe.
- **Consumers:** recipe cook endpoints.
- **Rules:** enforced PK/FK; convention JSONB checked_steps/checked_foods/timers.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1053-1067. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `recipe_id` | integer | NO | Recipe | FK -> food.recipes.recipe_id | request | recipe_cook_states_pkey | confirmed |
| `checked_steps` | jsonb | NO | Checked step ids | json | client | schema.generated.md | confirmed |
| `checked_foods` | jsonb | NO | Checked ingredient ids | json | client | schema.generated.md | confirmed |
| `timers` | jsonb | NO | Timer state | json | client | schema.generated.md | confirmed |
| `updated_at` | timestamp with time zone | NO | Last update | UTC timestamptz | update | schema.generated.md | confirmed |

### food.recipe_ingredients (table)
- **Purpose:** ingredient lines in a recipe.
- **Grain:** one row per (recipe, ingredient line).
- **Primary/candidate key:** `recipe_ingredients_pkey` on (recipe_id, ingredient_id). FKs -> recipes/foods.
- **Canonical source and lifecycle:** rewritten wholesale on recipe create/update.
- **Consumers:** recipe detail, cook state.
- **Rules:** enforced PK/FKs.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1068-1083. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `recipe_id` | integer | NO | Recipe | FK -> food.recipes.recipe_id | request | recipe_ingredients_pkey | confirmed |
| `ingredient_id` | integer | NO | Line id within recipe | identifier | assigned | recipe_ingredients_pkey | confirmed |
| `food_id` | integer | NO | Food used | FK -> food.foods.food_id | request | recipe_ingredients_food_id_fkey | confirmed |
| `qty` | double precision | NO | Quantity | number | request | schema.generated.md | confirmed |
| `unit` | text | NO | Unit | text | request | schema.generated.md | confirmed |

### food.recipe_step_foods (table)
- **Purpose:** foods used in a recipe step.
- **Grain:** one row per (recipe, step, food, unit).
- **Primary/candidate key:** `recipe_step_foods_pkey` on (recipe_id, step_id, food_id, unit). FKs -> recipes/recipe_steps/foods.
- **Canonical source and lifecycle:** rewritten wholesale on recipe create/update.
- **Consumers:** recipe detail, cook state.
- **Rules:** enforced PK/FKs.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1084-1100. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `recipe_id` | integer | NO | Recipe | FK -> food.recipes.recipe_id | request | recipe_step_foods_pkey | confirmed |
| `step_id` | integer | NO | Step | FK -> food.recipe_steps.step_id | request | recipe_step_foods_step_id_fkey | confirmed |
| `food_id` | integer | NO | Food used | FK -> food.foods.food_id | request | recipe_step_foods_food_id_fkey | confirmed |
| `qty` | double precision | NO | Quantity | number | request | schema.generated.md | confirmed |
| `unit` | text | NO | Unit | text | request | schema.generated.md | confirmed |

### food.recipe_steps (table)
- **Purpose:** ordered steps in a recipe.
- **Grain:** one row per step.
- **Primary/candidate key:** `recipe_steps_pkey` on `step_id`; unique `recipe_steps_recipe_id_step_no_key` on (recipe_id, step_no). FK -> recipes.recipe_id.
- **Canonical source and lifecycle:** rewritten wholesale on recipe create/update.
- **Consumers:** recipe detail, cook state.
- **Rules:** enforced PK/unique/FK.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1101-1116. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `step_id` | integer | NO | Surrogate id | identifier | assigned | recipe_steps_pkey | confirmed |
| `recipe_id` | integer | NO | Recipe | FK -> food.recipes.recipe_id | request | recipe_steps_recipe_id_fkey | confirmed |
| `step_no` | integer | NO | Step order | number | request | recipe_steps_recipe_id_step_no_key | confirmed |
| `instruction` | text | NO | Step text | free text | request | schema.generated.md | confirmed |
| `timer_seconds` | integer | YES | Timer duration | seconds | request | schema.generated.md | confirmed |

### food.recipes (table)
- **Purpose:** recipe catalog with aggregated nutrients and cook count.
- **Grain:** one row per recipe.
- **Primary/candidate key:** `recipes_pkey` on `recipe_id`. FK `recipes_food_id_fkey` (nullable) -> food.foods.food_id.
- **Canonical source and lifecycle:** created/updated/deleted via recipe endpoints; soft-delete via is_deleted/deleted_at; times_prepared increments on complete_recipe; recipe also owns a source='recipe' food row.
- **Consumers:** recipe endpoints, diary (via recipe food).
- **Rules:** enforced PK/FK; soft-delete; ix_recipes_* partial indexes.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1117-1139. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `recipe_id` | integer | NO | Surrogate id | identifier | insert | recipes_pkey | confirmed |
| `title` | text | NO | Recipe title | free text | request | schema.generated.md | confirmed |
| `category` | text | NO | Category | free text | request | ix_recipes_cat_alive | confirmed |
| `times_prepared` | integer | NO | Cook count | count | complete_recipe | schema.generated.md | confirmed |
| `food_id` | integer | YES | Linked food row (source=recipe) | FK -> food.foods.food_id | insert | recipes_food_id_fkey | confirmed |
| `is_deleted` | boolean | NO | Soft-delete flag | boolean | delete | ix_recipes_alive_rank | confirmed |
| `deleted_at` | timestamp with time zone | YES | Soft-delete time | UTC timestamptz | delete | schema.generated.md | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation | UTC timestamptz | insert | schema.generated.md | confirmed |
| `servings_count` | integer | YES | Servings per recipe | count | request | schema.generated.md | confirmed |
| `serving_unit` | text | YES | Serving unit label | text | request | schema.generated.md | confirmed |

### food.tri_tip_events (table)
- **Purpose:** tri-tip cooking event metadata.
- **Grain:** one row per cook event.
- **Primary/candidate key:** `tri_tip_events_pkey` on `tri_tip_id`.
- **Canonical source and lifecycle:** created via tri-tip endpoints.
- **Consumers:** tri-tip endpoints, tri_tip_readings.
- **Rules:** enforced PK.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1140-1156. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `tri_tip_id` | integer | NO | Surrogate id | identifier | insert | tri_tip_events_pkey | confirmed |
| `weight_lbs` | numeric | NO | Raw weight | lbs | request | schema.generated.md | confirmed |
| `shape` | text | NO | Shape | text | request | schema.generated.md | confirmed |
| `target_internal_temp_f` | numeric | NO | Target internal temp | �F | request | schema.generated.md | confirmed |
| `status` | text | NO | Event status | text | request | schema.generated.md | confirmed |
| `started_at` | timestamp with time zone | YES | Start time | UTC timestamptz | request | schema.generated.md | confirmed |
| `completed_at` | timestamp with time zone | YES | Completion time | UTC timestamptz | request | schema.generated.md | confirmed |
| `label` | text | YES | Label | free text | request | schema.generated.md | confirmed |
| `notes` | text | YES | Notes | free text | request | schema.generated.md | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation | UTC timestamptz | insert | schema.generated.md | confirmed |

### food.tri_tip_readings (table)
- **Purpose:** temperature readings during a tri-tip cook.
- **Grain:** one row per reading.
- **Primary/candidate key:** `tri_tip_readings_pkey` on `reading_id`. FK -> tri_tip_events.tri_tip_id.
- **Canonical source and lifecycle:** appended during cook.
- **Consumers:** tri-tip endpoints.
- **Rules:** enforced PK/FK.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1157-1173. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `reading_id` | integer | NO | Surrogate id | identifier | insert | tri_tip_readings_pkey | confirmed |
| `tri_tip_id` | integer | NO | Event | FK -> food.tri_tip_events.tri_tip_id | request | tri_tip_readings_tri_tip_id_fkey | confirmed |
| `recorded_at` | timestamp with time zone | NO | Reading time | UTC timestamptz | request | schema.generated.md | confirmed |
| `grill_temp_f` | numeric | NO | Grill temp | �F | request | schema.generated.md | confirmed |
| `internal_temp_f` | numeric | NO | Internal temp | �F | request | schema.generated.md | confirmed |
| `note` | text | YES | Note | free text | request | schema.generated.md | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation | UTC timestamptz | insert | schema.generated.md | confirmed |

### food.vw_food_search (view)
- **Purpose:** searchable food list with rank_score.
- **Grain:** one row per non-deleted food.
- **Primary/candidate key:** none (view); underlying food_id.
- **Canonical source and lifecycle:** derived from food.foods.
- **Consumers:** GET /api/food/search (queries.py).
- **Rules:** enforced via underlying constraints; convention rank_score.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1174-1185; definitions.sql. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `food_id` | integer | YES | Food id | identifier | foods.food_id | schema.generated.md | confirmed |
| `name` | text | YES | Name | free text | foods.name | schema.generated.md | confirmed |
| `source` | text | YES | Provenance | text | foods.source | schema.generated.md | confirmed |
| `kcal_per_100g` | double precision | YES | Energy density | kcal/100g | foods.kcal_per_100g | schema.generated.md | confirmed |
| `caffeine_mg` | double precision | YES | Caffeine density | mg/100g | foods.caffeine_mg | schema.generated.md | confirmed |
| `quality_score` | double precision | YES | Quality score | number | foods.quality_score | schema.generated.md | confirmed |
| `needs_review` | boolean | YES | Needs review | boolean | foods.needs_review | schema.generated.md | confirmed |
| `rank_score` | double precision | YES | Search rank | number | computed | definitions.sql | confirmed |

### food.vw_usual_foods (view)
- **Purpose:** frequently used foods with usage stats.
- **Grain:** one row per food frequently eaten.
- **Primary/candidate key:** none (view).
- **Canonical source and lifecycle:** derived from food.diary_entries/food.foods.
- **Consumers:** GET /api/food/usual (queries.py).
- **Rules:** convention read-only view.
- **Evidence/confidence:** backend/pifitness/food/queries.py; schema.generated.md 1186-1198; definitions.sql. confirmed.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `food_id` | integer | YES | Food id | identifier | foods.food_id | schema.generated.md | confirmed |
| `name` | text | YES | Name | free text | foods.name | schema.generated.md | confirmed |
| `source` | text | YES | Provenance | text | foods.source | schema.generated.md | confirmed |
| `kcal_per_100g` | double precision | YES | Energy density | kcal/100g | foods.kcal_per_100g | schema.generated.md | confirmed |
| `quality_score` | double precision | YES | Quality score | number | foods.quality_score | schema.generated.md | confirmed |
| `times_eaten` | integer | YES | Usage count | count | diary entries | definitions.sql | confirmed |
| `hour_weight` | double precision | YES | Hour distribution weight | number | computed | definitions.sql | confirmed |
| `mode_qty` | double precision | YES | Most common qty | number | computed | definitions.sql | confirmed |
| `mode_unit` | text | YES | Most common unit | text | computed | definitions.sql | confirmed |

## health

### health.body_battery_raw (table)
- **Grain:** one row per body-battery sample.
- **Canonical source:** `health.body_battery_raw` (Garmin import).
- **Rules:** `convention` raw time-series; PK app-level.

### health.body_composition_raw (table)
- **Grain:** one row per body-composition measurement.
- **Canonical source:** `health.body_composition_raw` (Garmin import).
- **Rules:** `convention` raw measurement.

### health.body_dimensions (table)
- **Grain:** one row per body dimension record.
- **Canonical source:** `health.body_dimensions`.
- **Rules:** `convention` dimension data.

### health.fitness_age_raw (table)
- **Grain:** one row per fitness-age sample.
- **Canonical source:** `health.fitness_age_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.heartrate_raw (table)
- **Grain:** one row per heart-rate sample (ts_utc).
- **Canonical source:** `health.heartrate_raw`; populated by `activity_smoothing.insert_heartrate_for_activity` from `activities.activity_details`.
- **Rules:** `enforced` PK ts_utc; upsert on conflict.

### health.hrv_raw (table)
- **Grain:** one row per HRV sample.
- **Canonical source:** `health.hrv_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.photo_metadata (table)
- **Grain:** one row per photo metadata.
- **Canonical source:** `health.photo_metadata`.
- **Rules:** `convention` metadata.

### health.respiration_raw (table)
- **Grain:** one row per respiration sample.
- **Canonical source:** `health.respiration_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.sleep_movement_raw (table)
- **Grain:** one row per sleep movement sample.
- **Canonical source:** `health.sleep_movement_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.sleep_totals (table)
- **Grain:** one row per night sleep summary.
- **Canonical source:** `health.sleep_totals`.
- **Rules:** `convention` nightly summary.

### health.spo2_raw (table)
- **Grain:** one row per SpO2 sample.
- **Canonical source:** `health.spo2_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.stress_raw (table)
- **Grain:** one row per stress sample.
- **Canonical source:** `health.stress_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.training_status_raw (table)
- **Grain:** one row per training status sample.
- **Canonical source:** `health.training_status_raw` (Garmin import).
- **Rules:** `convention` raw sample.

### health.vw_activity_weights (view)
- **Grain:** one row per activity weight.
- **Canonical source:** derived from health data.
- **Rules:** `convention` read-only view.

### health.vw_body_comp_interpolation_precalc (view)
- **Grain:** one row per precomputed body-comp interpolation.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### health.vw_heart_rates (view)
- **Grain:** one row per heart-rate reading.
- **Canonical source:** derived from `health.heartrate_raw`.
- **Rules:** `convention` read-only view.

### health.weight_target (table)
- **Grain:** one row per weight target setting.
- **Canonical source:** `health.weight_target`.
- **Rules:** `convention` user setting.

## logging

### logging.api_logins (table)
- **Grain:** one row per API login event.
- **Canonical source:** `logging.api_logins`.
- **Rules:** `convention` audit log.

### logging.application_events (table)
- **Grain:** one row per application event.
- **Canonical source:** `logging.application_events`.
- **Rules:** `convention` event log.

### logging.db_size_log (table)
- **Grain:** one row per DB size snapshot.
- **Canonical source:** `logging.db_size_log`.
- **Rules:** `convention` monitoring log.

### logging.db_stats (table)
- **Grain:** one row per DB stats snapshot.
- **Canonical source:** `logging.db_stats`.
- **Rules:** `convention` monitoring log.

### logging.health_checks (table)
- **Grain:** one row per health check result.
- **Canonical source:** `logging.health_checks`.
- **Rules:** `convention` monitoring log.

### logging.music_db_info (table)
- **Grain:** one row per music DB info snapshot.
- **Canonical source:** `logging.music_db_info`.
- **Rules:** `convention` monitoring log.

### logging.task_executions (table)
- **Grain:** one row per task execution record.
- **Canonical source:** `logging.task_executions`.
- **Rules:** `convention` execution log.

### logging.vw_all_event_history (view)
- **Grain:** one row per event across history.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_db_biggest_table_chart (view)
- **Grain:** one row per table size entry.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_db_size (view)
- **Grain:** one row per DB size reading.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_db_size_chart (view)
- **Grain:** one row per DB size chart point.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_event_analysis (view)
- **Grain:** one row per event analysis.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_last_login (view)
- **Grain:** one row per last login.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

### logging.vw_task_analysis (view)
- **Grain:** one row per task analysis.
- **Canonical source:** derived view.
- **Rules:** `convention` read-only view.

## music

### music.all_albums (table)
- **Grain:** one row per all albums (table); see schema.generated.md for columns.
- **Canonical source:** `music.all_albums`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.all_artists (table)
- **Grain:** one row per all artists (table); see schema.generated.md for columns.
- **Canonical source:** `music.all_artists`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.all_tracks (table)
- **Grain:** one row per all tracks (table); see schema.generated.md for columns.
- **Canonical source:** `music.all_tracks`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.isrc_non_swaps (table)
- **Grain:** one row per isrc non swaps (table); see schema.generated.md for columns.
- **Canonical source:** `music.isrc_non_swaps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.isrc_possible_dupes (table)
- **Grain:** one row per isrc possible dupes (table); see schema.generated.md for columns.
- **Canonical source:** `music.isrc_possible_dupes`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.isrc_swaps (table)
- **Grain:** one row per isrc swaps (table); see schema.generated.md for columns.
- **Canonical source:** `music.isrc_swaps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.listening_history (table)
- **Grain:** one row per listening history (table); see schema.generated.md for columns.
- **Canonical source:** `music.listening_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.missing_relationships (view)
- **Grain:** one row per missing relationships (view); see schema.generated.md for columns.
- **Canonical source:** `music.missing_relationships`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.playlist_config (table)
- **Grain:** one row per playlist config (table); see schema.generated.md for columns.
- **Canonical source:** `music.playlist_config`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.playlist_isrcs (table)
- **Grain:** one row per playlist isrcs (table); see schema.generated.md for columns.
- **Canonical source:** `music.playlist_isrcs`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.playlist_recommendation_exclusions (table)
- **Grain:** one row per playlist recommendation exclusions (table); see schema.generated.md for columns.
- **Canonical source:** `music.playlist_recommendation_exclusions`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.playlist_relationships (table)
- **Grain:** one row per playlist relationships (table); see schema.generated.md for columns.
- **Canonical source:** `music.playlist_relationships`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.ratings (table)
- **Grain:** one row per ratings (table); see schema.generated.md for columns.
- **Canonical source:** `music.ratings`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.ratings_history (table)
- **Grain:** one row per ratings history (table); see schema.generated.md for columns.
- **Canonical source:** `music.ratings_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.temp_listening_history (table)
- **Grain:** one row per temp listening history (table); see schema.generated.md for columns.
- **Canonical source:** `music.temp_listening_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.track_recommendation_decisions (table)
- **Grain:** one row per track recommendation decisions (table); see schema.generated.md for columns.
- **Canonical source:** `music.track_recommendation_decisions`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.track_recommendations (table)
- **Grain:** one row per track recommendations (table); see schema.generated.md for columns.
- **Canonical source:** `music.track_recommendations`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_all_tracks (view)
- **Grain:** one row per vw all tracks (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_all_tracks`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_best_track_id (view)
- **Grain:** one row per vw best track id (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_best_track_id`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_isrc_dupe_review (view)
- **Grain:** one row per vw isrc dupe review (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_isrc_dupe_review`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_listening_history (view)
- **Grain:** one row per vw listening history (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_listening_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_config (view)
- **Grain:** one row per vw playlist config (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_config`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_detail_sync_logic (view)
- **Grain:** one row per vw playlist detail sync logic (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_detail_sync_logic`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_families (view)
- **Grain:** one row per vw playlist families (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_families`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_isrc_stats (view)
- **Grain:** one row per vw playlist isrc stats (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_isrc_stats`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_isrcs (view)
- **Grain:** one row per vw playlist isrcs (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_isrcs`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_last_heard (view)
- **Grain:** one row per vw playlist last heard (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_last_heard`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_shuffle_eligibility (view)
- **Grain:** one row per vw playlist shuffle eligibility (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_shuffle_eligibility`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_shuffle_targets (view)
- **Grain:** one row per vw playlist shuffle targets (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_shuffle_targets`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_playlist_with_mismatched_isrc (view)
- **Grain:** one row per vw playlist with mismatched isrc (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_playlist_with_mismatched_isrc`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_rating_eligible (view)
- **Grain:** one row per vw rating eligible (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_rating_eligible`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_rating_eligible_playlists (view)
- **Grain:** one row per vw rating eligible playlists (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_rating_eligible_playlists`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_rating_update (view)
- **Grain:** one row per vw rating update (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_rating_update`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_ratings (view)
- **Grain:** one row per vw ratings (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_ratings`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_ratings_history (view)
- **Grain:** one row per vw ratings history (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_ratings_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_ratings_update (view)
- **Grain:** one row per vw ratings update (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_ratings_update`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_recent_plays (view)
- **Grain:** one row per vw recent plays (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_recent_plays`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_track_id_finder (view)
- **Grain:** one row per vw track id finder (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_track_id_finder`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### music.vw_track_id_to_isrc (view)
- **Grain:** one row per vw track id to isrc (view); see schema.generated.md for columns.
- **Canonical source:** `music.vw_track_id_to_isrc`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## staging

### staging.activity_metric_descriptors (table)
- **Grain:** one row per activity metric descriptors (table); see schema.generated.md for columns.
- **Canonical source:** `staging.activity_metric_descriptors`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.activity_metrics_flat (table)
- **Grain:** one row per activity metrics flat (table); see schema.generated.md for columns.
- **Canonical source:** `staging.activity_metrics_flat`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.activity_metrics_matched (table)
- **Grain:** one row per activity metrics matched (table); see schema.generated.md for columns.
- **Canonical source:** `staging.activity_metrics_matched`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.activity_payload (table)
- **Grain:** one row per activity payload (table); see schema.generated.md for columns.
- **Canonical source:** `staging.activity_payload`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.api_imports (table)
- **Grain:** one row per api imports (table); see schema.generated.md for columns.
- **Canonical source:** `staging.api_imports`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.bounds (table)
- **Grain:** one row per bounds (table); see schema.generated.md for columns.
- **Canonical source:** `staging.bounds`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.interpolation_anchors (table)
- **Grain:** one row per interpolation anchors (table); see schema.generated.md for columns.
- **Canonical source:** `staging.interpolation_anchors`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.interpolation_load_values (table)
- **Grain:** one row per interpolation load values (table); see schema.generated.md for columns.
- **Canonical source:** `staging.interpolation_load_values`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.isrc_dupe_pairs (table)
- **Grain:** one row per isrc dupe pairs (table); see schema.generated.md for columns.
- **Canonical source:** `staging.isrc_dupe_pairs`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.isrc_dupe_seeds (table)
- **Grain:** one row per isrc dupe seeds (table); see schema.generated.md for columns.
- **Canonical source:** `staging.isrc_dupe_seeds`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.json_examples (table)
- **Grain:** one row per json examples (table); see schema.generated.md for columns.
- **Canonical source:** `staging.json_examples`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.stg_listen_history (table)
- **Grain:** one row per stg listen history (table); see schema.generated.md for columns.
- **Canonical source:** `staging.stg_listen_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.stg_now_playing (table)
- **Grain:** one row per stg now playing (table); see schema.generated.md for columns.
- **Canonical source:** `staging.stg_now_playing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.stg_playlist_details (table)
- **Grain:** one row per stg playlist details (table); see schema.generated.md for columns.
- **Canonical source:** `staging.stg_playlist_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_activity_locations (table)
- **Grain:** one row per temp activity locations (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_activity_locations`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_activity_matched_locations (table)
- **Grain:** one row per temp activity matched locations (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_activity_matched_locations`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_activity_metas (table)
- **Grain:** one row per temp activity metas (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_activity_metas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_activity_trackpoints (table)
- **Grain:** one row per temp activity trackpoints (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_activity_trackpoints`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_bc_import (table)
- **Grain:** one row per temp bc import (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_bc_import`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_csv_import (table)
- **Grain:** one row per temp csv import (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_csv_import`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_elevation_deltas (table)
- **Grain:** one row per temp elevation deltas (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_elevation_deltas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_elevation_medians (table)
- **Grain:** one row per temp elevation medians (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_elevation_medians`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_elevation_smoothed (table)
- **Grain:** one row per temp elevation smoothed (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_elevation_smoothed`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_elevation_smoothing (table)
- **Grain:** one row per temp elevation smoothing (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_elevation_smoothing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_elevation_smoothing_m (table)
- **Grain:** one row per temp elevation smoothing m (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_elevation_smoothing_m`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_isrc_swaps (table)
- **Grain:** one row per temp isrc swaps (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_isrc_swaps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_performance_profiling (table)
- **Grain:** one row per temp performance profiling (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_performance_profiling`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_segment_matches (table)
- **Grain:** one row per temp segment matches (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_segment_matches`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.temp_training_status_raw (table)
- **Grain:** one row per temp training status raw (table); see schema.generated.md for columns.
- **Canonical source:** `staging.temp_training_status_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_activity_by_distance (view)
- **Grain:** one row per vw activity by distance (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_activity_by_distance`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_activity_flatten (view)
- **Grain:** one row per vw activity flatten (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_activity_flatten`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_album_inserts (view)
- **Grain:** one row per vw album inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_album_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_artist_inserts (view)
- **Grain:** one row per vw artist inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_artist_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_backward (view)
- **Grain:** one row per vw elevation backward (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_backward`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_bumps (view)
- **Grain:** one row per vw elevation bumps (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_bumps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_forward (view)
- **Grain:** one row per vw elevation forward (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_forward`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_median (view)
- **Grain:** one row per vw elevation median (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_median`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_medians (view)
- **Grain:** one row per vw elevation medians (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_medians`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_neighbors (view)
- **Grain:** one row per vw elevation neighbors (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_neighbors`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_elevation_weighted_avg (view)
- **Grain:** one row per vw elevation weighted avg (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_elevation_weighted_avg`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_large_pull (view)
- **Grain:** one row per vw large pull (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_large_pull`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_listening_history_inserts (view)
- **Grain:** one row per vw listening history inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_listening_history_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_now_playing_inserts (view)
- **Grain:** one row per vw now playing inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_now_playing_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_playlist_inserts (view)
- **Grain:** one row per vw playlist inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_playlist_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_small_pull (view)
- **Grain:** one row per vw small pull (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_small_pull`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_track_inserts (view)
- **Grain:** one row per vw track inserts (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_track_inserts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### staging.vw_track_inserts_from_listen_history (view)
- **Grain:** one row per vw track inserts from listen history (view); see schema.generated.md for columns.
- **Canonical source:** `staging.vw_track_inserts_from_listen_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## tasks

### tasks.acceptable_failures (table)
- **Grain:** one row per acceptable failures (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.acceptable_failures`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.fact_configuration (table)
- **Grain:** one row per fact configuration (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.fact_configuration`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.staging_configuration (table)
- **Grain:** one row per staging configuration (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.staging_configuration`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.task_config (table)
- **Grain:** one row per task config (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.task_config`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.task_configuration (table)
- **Grain:** one row per task configuration (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.task_configuration`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.task_executions (table)
- **Grain:** one row per task executions (table); see schema.generated.md for columns.
- **Canonical source:** `tasks.task_executions`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.vw_task_execution (view)
- **Grain:** one row per vw task execution (view); see schema.generated.md for columns.
- **Canonical source:** `tasks.vw_task_execution`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.vw_task_info (view)
- **Grain:** one row per vw task info (view); see schema.generated.md for columns.
- **Canonical source:** `tasks.vw_task_info`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.vw_task_performance (view)
- **Grain:** one row per vw task performance (view); see schema.generated.md for columns.
- **Canonical source:** `tasks.vw_task_performance`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### tasks.vw_task_summary_chart (view)
- **Grain:** one row per vw task summary chart (view); see schema.generated.md for columns.
- **Canonical source:** `tasks.vw_task_summary_chart`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## volleyball

### volleyball.game_events (table)
- **Purpose:** scorekeeper's log of non-score events (timeouts and messages) for an active game; event-sourced alongside `volleyball.points`.
- **Grain:** one row per recorded game event, keyed by `event_id`; scoped to one game via `game_id`.
- **Primary/candidate key:** `game_events_pkey` on `event_id` (enforced). FK `game_events_game_id_fkey` (`game_id` -> `volleyball.games.game_id`, enforced).
- **Canonical source and lifecycle:** inserted only by `insert_game_event` (POST `/{id}/timeout`, POST `/{id}/message`), active-only (409 if not active); read by `list_game_events` / GET `/{id}` detail. Events are not deleted individually; removed with the game on abandon (cascade).
- **Consumers:** GET `/api/sports/volleyball/{id}` detail (events list); router serializer `_serialize_event`.
- **Relationships:** enforced FK to `volleyball.games`; cascade on game delete per code comment (confirm ON DELETE clause against live catalog if needed).
- **Rules:** `enforced` PK + FK; `convention` `kind` open set {'timeout','message'} (app-enforced `VALID_GAME_EVENT_KINDS`), `message` max 240 chars and blank->422, `message` NULL for timeouts, timestamps UTC. No score is stored here.
- **Evidence/confidence:** `backend/pifitness/volleyball/queries.py` (insert_game_event/list_game_events docstrings), `router.py`, `schemas.py` (`VALID_GAME_EVENT_KINDS`, `GAME_EVENT_MESSAGE_MAX`); `schema.generated.md` lines 3066-3081. Structural facts confirmed; app-enforced sets confirmed from code.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `event_id` | integer | NO | Surrogate identifier for the event | Identifier | assigned on insert | queries.py insert_game_event; game_events_pkey | confirmed |
| `game_id` | integer | NO | Parent game | FK -> volleyball.games.game_id | insert_game_event | game_events_game_id_fkey | confirmed |
| `kind` | text | NO | Event type | 'timeout' or 'message' (open set, app-enforced) | insert_game_event | queries.py VALID_GAME_EVENT_KINDS | confirmed |
| `message` | text | YES | Scorekeeper text ('Scorekeeper: {text}'); NULL for timeout | <=240 chars, blank->422 | insert_game_event | schemas.py VolleyballMessageRequest | confirmed |
| `recorded_at` | timestamp with time zone | NO | Event time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |

### volleyball.games (table)
- **Purpose:** one row per beach volleyball match; holds match metadata and lifecycle state (active -> completed, or hard-deleted on abandon).
- **Grain:** one row per game, keyed by `game_id`. Team A ("SR") is implicit and not stored; `team_b_name` is the opponent.
- **Primary/candidate key:** `games_pkey` on `game_id` (enforced, unique index). No other declared unique key.
- **Canonical source and lifecycle:** inserted by `create_game` (POST `/api/sports/volleyball`) with `status='active'` under a single-active guard (`VolleyballActiveGameExistsError`); updated by `end_game` (`status='completed'`, `completed_at = MAX(points.recorded_at)`) and by `add_point`/`remove_last_point` (maintain `started_at = MIN(points.recorded_at)`; `first_entry_at` set-once, never cleared by undo); hard-deleted by `abandon_game` (DELETE; points and game_events cascade per code comment). No soft-delete column; no is_deleted.
- **Consumers:** GET `/api/sports/volleyball` (history), GET `/api/sports/volleyball/active`, GET `/api/sports/volleyball/{id}` (detail); frontend contract `frontend/pifitness/src/lib/types/volleyball.ts`; router serializer `_serialize_game`.
- **Relationships:** `game_id` referenced by `volleyball.game_events.game_id` (`game_events_game_id_fkey`) and `volleyball.points.game_id` (`points_game_id_fkey`); abandon cascades to points and game_events (code-evidenced; DB ON DELETE clause not shown in generated inventory — confirm against live catalog if needed).
- **Rules:** `enforced` PK on `game_id`; `convention` single-active guard, `status` open set {'active','completed'} (app-enforced, no DB check constraint), timestamps UTC (`timestamptz`), `completed_at` NULL when a game ends 0-0.
- **Evidence/confidence:** `backend/pifitness/volleyball/queries.py` (create_game/end_game/add_point/remove_last_point/abandon_game docstrings), `router.py` endpoints, `schemas.py`; `schema.generated.md` lines 3083-3099. Structural facts confirmed; app-enforced sets confirmed from code; `label`/`notes` purpose not established in inspected code (low confidence).

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `game_id` | integer | NO | Surrogate identifier for the game | Identifier | assigned on insert | queries.py create_game; games_pkey | confirmed |
| `team_b_name` | text | NO | Opponent (Team B) name; Team A = 'SR' not stored | free text, 1-120 chars | create_game request | schemas.py VolleyballCreateGameRequest | confirmed |
| `status` | text | NO | Game lifecycle state | 'active' or 'completed' (open set, app-enforced) | create_game='active'; end_game='completed' | queries.py | confirmed |
| `started_at` | timestamp with time zone | YES | First point time of the game | UTC timestamptz | MIN(points.recorded_at), maintained by add_point/remove_last_point | queries.py | confirmed |
| `completed_at` | timestamp with time zone | YES | Time the game ended | UTC timestamptz; NULL if ended 0-0 | MAX(points.recorded_at) at end_game | queries.py end_game | confirmed |
| `label` | text | YES | Free-text game label | free text | not established in inspected code | queries.py/router.py (searched) | low |
| `notes` | text | YES | Free-text game notes | free text | not established in inspected code | queries.py/router.py (searched) | low |
| `created_at` | timestamp with time zone | NO | Row creation time | UTC timestamptz | server/insert-assigned | schema.generated.md | confirmed |
| `partner_number` | integer | YES | SR side's partner jersey number | integer >= 0 (Pydantic ge=0) | create_game request | schemas.py | confirmed |
| `partner_name` | text | YES | SR side's partner name (optional; null when absent) | free text, 1-120 chars | create_game request | schemas.py | confirmed |
| `first_entry_at` | timestamp with time zone | YES | Time of first point entry; set-once, undo never clears | UTC timestamptz | set once on first point | queries.py add_point | confirmed |

### volleyball.points (table)
- **Purpose:** one row per point scored by either team in a game; the score is never stored — it is derived by counting `points` rows per team.
- **Grain:** one row per point, keyed by `point_id`; scoped to one game via `game_id`.
- **Primary/candidate key:** `points_pkey` on `point_id` (enforced). FK `points_game_id_fkey` (`game_id` -> `volleyball.games.game_id`, enforced).
- **Canonical source and lifecycle:** inserted by `add_point` (POST `/{id}/points`; `recorded_at = now()` server-assigned); deleted by `remove_last_point` (most recent point of ONE team only); tagged/overwritten by `tag_last_point` (`event_type`); removed on game abandon (cascade). `end_game` derives `completed_at = MAX(points.recorded_at)`.
- **Consumers:** GET `/api/sports/volleyball/active` and `/{id}` (derived score); frontend contract `frontend/pifitness/src/lib/types/volleyball.ts`; serializers `_serialize_point`, `_serialize_detail`.
- **Relationships:** enforced FK to `volleyball.games`; cascade on game delete per code comment (confirm ON DELETE clause against live catalog if needed).
- **Rules:** `enforced` PK + FK; `convention` `scoring_team` open set {'SR','OPPONENT'} (`VALID_SCORING_TEAMS`), `event_type` open set {'Ace','Block','Spike','Dive'} optional and overwritable on re-tag, `recorded_at` server now() UTC, score derived (not stored) — remove/re-tag recalculates it.
- **Evidence/confidence:** `backend/pifitness/volleyball/queries.py` (add_point/remove_last_point/tag_last_point/end_game docstrings); `schemas.py` (`VALID_SCORING_TEAMS`, `VALID_EVENT_TYPES`); `router.py`; `schema.generated.md` lines 3101-3116. Structural facts confirmed; app-enforced sets confirmed from code.

| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| `point_id` | integer | NO | Surrogate identifier for the point | Identifier | assigned on insert | queries.py add_point; points_pkey | confirmed |
| `game_id` | integer | NO | Parent game | FK -> volleyball.games.game_id | add_point | points_game_id_fkey | confirmed |
| `scoring_team` | text | NO | Team that scored the point | 'SR' or 'OPPONENT' (open set, app-enforced) | add_point request | queries.py VALID_SCORING_TEAMS | confirmed |
| `recorded_at` | timestamp with time zone | NO | Time the point was scored | UTC timestamptz | server-assigned now() | queries.py add_point | confirmed |
| `created_at` | timestamp with time zone | NO | Row creation time | UTC timestamptz | insert-assigned | schema.generated.md | confirmed |
| `event_type` | text | YES | Optional notable-play tag on the point | 'Ace'/'Block'/'Spike'/'Dive' or NULL; overwritable | add_point or tag_last_point | schemas.py VolleyballEventType | confirmed |
