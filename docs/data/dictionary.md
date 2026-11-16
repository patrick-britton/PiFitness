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





- **Purpose:** per-sample telemetry stream for a recorded activity (heart rate, cadence, elevation, air temperature, location, speed); the raw detail behind `activities.activities`.





- **Grain:** one row per timestamped sample within an activity; key (`activity_id`, `ts_utc`).





- **Canonical source:** populated by the Garmin sync/import flow (staging.api_imports path); `elevation_m_smooth`/`elevation_reference` backfilled later by `backend/backend_functions/activity_smoothing.py` (Savitzky-Golay smoothing and elevation-reference lookup).





- **Rules:** `enforced` PK (`activity_id`, `ts_utc`) and NOT NULL on both key columns; no declared DB FK to `activities.activities` (app-level convention); `ts_utc` is UTC timestamptz; `distance_mm` stores millimetres (distinct from activities.distance_m).





- **Consumers:** `backend/backend_functions/activity_smoothing.py` (reads samples, writes smoothed elevation); views `vw_activity_elevation_deltas`, `vw_activity_header_stats`, `vw_segment_comps`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity the sample belongs to; references `activities.activities.activity_id` (app-level, no declared DB FK) | id | schema.generated.md | schema.generated.md; catalog constraints | inferred |





| `ts_utc` | timestamptz | NO | Sample timestamp | UTC timestamptz | schema.generated.md | schema.generated.md | confirmed |





| `moving_duration_s` | integer | YES | Moving duration accumulated up to this sample | seconds | name suffix | schema.generated.md | inferred |





| `elapsed_duration_s` | integer | YES | Elapsed time accumulated up to this sample (includes pauses) | seconds | name suffix | schema.generated.md | inferred |





| `duration_s` | integer | YES | Duration delta for this sample interval | seconds | name suffix | schema.generated.md | inferred |





| `distance_mm` | bigint | YES | Cumulative distance at this sample | millimetres | name suffix; distinct from activities.distance_m | schema.generated.md | inferred |





| `latitude` | numeric | YES | Sample latitude | degrees | name suffix | schema.generated.md | inferred |





| `longitude` | numeric | YES | Sample longitude | degrees | name suffix | schema.generated.md | inferred |





| `heartrate_bpm` | smallint | YES | Sample heart rate | beats per minute | name suffix | schema.generated.md | inferred |





| `cadence_spm` | smallint | YES | Sample cadence | steps per minute | name suffix | schema.generated.md | inferred |





| `air_temp_c` | smallint | YES | Sample air temperature | degrees Celsius | name suffix | schema.generated.md | inferred |





| `elevation_m` | numeric | YES | Sample elevation | metres | name suffix | schema.generated.md | inferred |





| `speed_mmps` | bigint | YES | Sample speed | millimetres per second | name suffix | schema.generated.md | inferred |





| `vertical_speed_mps` | numeric | YES | Sample vertical speed | metres per second | name suffix | schema.generated.md | inferred |





| `vert_oscillation_cm` | numeric | YES | Vertical oscillation | centimetres | name suffix | schema.generated.md | inferred |





| `stride_length_cm` | numeric | YES | Stride length | centimetres | name suffix | schema.generated.md | inferred |





| `vert_ratio` | numeric | YES | Vertical oscillation ratio | ratio | name suffix | schema.generated.md | inferred |





| `ground_contact_time_ms` | smallint | YES | Ground contact time | milliseconds | name suffix | schema.generated.md | inferred |





| `ground_contact_balance_left` | numeric | YES | Ground contact balance (left side) | percent | name suffix | schema.generated.md | inferred |





| `performance_condition` | smallint | YES | Garmin performance condition score at sample time | score (Garmin scale) | name matches Garmin field | schema.generated.md | inferred |





| `activity_type_id` | smallint | YES | Garmin activity type code recorded at sample | id | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |





| `elevation_m_smooth` | numeric | YES | Smoothed elevation (Savitzky-Golay) written back by the activity smoothing worker | metres | `backend/backend_functions/activity_smoothing.py` (`update_smoothed_elevation`) | backend/backend_functions/activity_smoothing.py | confirmed |





| `elevation_reference` | numeric | YES | Reference elevation for the sample from the elevation grid lookup | metres | `backend/backend_functions/activity_smoothing.py` (procedures `activities.assign_elevation_reference_time`, `activities.update_elevation_reference_by_time|by_distance`) | backend/backend_functions/activity_smoothing.py | inferred |





| `loc_point` | USER-DEFINED | YES | Sample location point (spatial type); underlying type not established from DDL | geometry-like | schema.generated.md | schema.generated.md | unknown |





| `grid_5m_00_id` | USER-DEFINED | YES | 5 m elevation grid cell id for the sample; meaning not established beyond name | grid id | name only; no code/DDL semantics evidence | schema.generated.md; deploy/migrations/007__comment_backfill_activities.sql | unknown |











### activities.activity_details_distance (table)




- **Purpose:** distance-binned per-sample telemetry working table used to build/smooth activity paths.




- **Grain:** one row per distance-binned sample within an activity; key (`activity_id`, `distance_m`); unique index `activity_details_distance_pkey`.




- **Canonical source:** pipeline working table loaded ahead of `activity_details` smoothing (migration comment: 'Temporary table for 1 activity prior to load to activity_details_distance'); `elevation_m_smooth`/`elevation_reference` written by `backend/backend_functions/activity_smoothing.py`.




- **Rules:** `enforced` unique index on (`activity_id`, `distance_m`) (no declared PK constraint); NOT NULL `activity_id`/`distance_m`; derived/table-convention contents; `elapsed_duration_ms` stores milliseconds (distinct from `activity_details.elapsed_duration_s` seconds).




- **Consumers:** `backend/backend_functions/activity_smoothing.py` (reads raw elevation, writes smoothed values).









| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |




|---|---|---|---|---|---|---|---|




| `activity_type_id` | smallint | YES | Garmin activity type code for the binned sample | id | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |




| `activity_id` | bigint | NO | Activity the bin belongs to; references `activities.activities.activity_id` (app-level) | id | schema.generated.md | schema.generated.md | inferred |




| `distance_m` | integer | NO | Distance bin edge for the row | metres | schema.generated.md | schema.generated.md | confirmed |




| `elapsed_duration_ms` | integer | YES | Elapsed duration at the bin | milliseconds | name suffix; distinct from `activity_details.elapsed_duration_s` (seconds) | schema.generated.md | inferred |




| `latitude` | numeric | YES | Bin latitude | degrees | name suffix | schema.generated.md | inferred |




| `longitude` | numeric | YES | Bin longitude | degrees | name suffix | schema.generated.md | inferred |




| `heartrate_bpm` | smallint | YES | Bin heart rate | beats per minute | name suffix | schema.generated.md | inferred |




| `cadence_spm` | smallint | YES | Bin cadence | steps per minute | name suffix | schema.generated.md | inferred |




| `air_temp_c` | smallint | YES | Bin air temperature | degrees Celsius | name suffix | schema.generated.md | inferred |




| `elevation_m` | numeric | YES | Bin elevation | metres | name suffix | schema.generated.md | inferred |




| `speed_mmps` | bigint | YES | Bin speed | millimetres per second | name suffix | schema.generated.md | inferred |




| `vertical_speed_mps` | numeric | YES | Bin vertical speed | metres per second | name suffix | schema.generated.md | inferred |




| `vert_oscillation_cm` | numeric | YES | Bin vertical oscillation | centimetres | name suffix | schema.generated.md | inferred |




| `stride_length_cm` | numeric | YES | Bin stride length | centimetres | name suffix | schema.generated.md | inferred |




| `vert_ratio` | numeric | YES | Bin vertical oscillation ratio | ratio | name suffix | schema.generated.md | inferred |




| `ground_contact_time_ms` | smallint | YES | Bin ground contact time | milliseconds | name suffix | schema.generated.md | inferred |




| `ground_contact_balance_left` | numeric | YES | Bin ground contact balance (left) | percent | name suffix | schema.generated.md | inferred |




| `performance_condition` | smallint | YES | Bin performance condition score | Garmin scale | name matches Garmin field | schema.generated.md | inferred |




| `elevation_m_smooth` | numeric | YES | Smoothed elevation for the bin, written by the activity smoothing worker | metres | `backend/backend_functions/activity_smoothing.py` (`update_smoothed_elevation`, pk_field `distance_m`) | backend/backend_functions/activity_smoothing.py | confirmed |




| `elevation_reference` | numeric | YES | Reference elevation for the bin from the elevation grid lookup | metres | `backend/backend_functions/activity_smoothing.py` (elevation-reference procedures) | backend/backend_functions/activity_smoothing.py | inferred |




| `loc_point` | USER-DEFINED | YES | Bin location point (spatial type; underlying type not established from DDL) | geometry-like | schema.generated.md | schema.generated.md | unknown |




| `grid_5m_00_id` | USER-DEFINED | YES | 5 m elevation grid cell id for the bin; meaning not established beyond name | grid id | name only | schema.generated.md; deploy/migrations/007__comment_backfill_activities.sql | unknown |









### activities.courses (table)



- **Purpose:** named courses/routes derived from activities, used as reference geometry for segment/course matching.



- **Grain:** one row per named course/route; key `course_id`.



- **Canonical source:** `activities.courses`; derived from activity data (migration comment: 'Specific courses derived from activities, required for segment/course matching').



- **Rules:** `enforced` PK `course_id`; `course_path` geometry is GiST-indexed (`idx_courses_path`, plus start/end-point indexes); referenced by `activities.segments.activity_reference_id` at the application level.



- **Consumers:** segment/course matching flows; `activities.vw_course_review`.







| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |



|---|---|---|---|---|---|---|---|



| `course_id` | integer | NO | Course identifier | id | schema.generated.md | schema.generated.md | confirmed |



| `course_name` | text | YES | Human-readable course/route name | text | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |



| `created_at` | timestamp with time zone | YES | Course creation timestamp | timestamptz | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |



| `course_path` | USER-DEFINED | YES | Course geometry path (GiST-indexed) | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |



| `course_distance_m` | numeric | YES | Course distance | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |



| `course_elevation_gain_m` | numeric | YES | Course elevation gain | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |







### activities.segments (table)


- **Purpose:** defined segments/courses -- user-relevant route sections against which efforts are compared (PRs, leaderboards).


- **Grain:** one row per defined segment; key `segment_id`.


- **Canonical source:** `activities.segments`; created from activity geometry (segment creation flow); `activity_reference_id` references the source activity.


- **Rules:** `enforced` PK `segment_id`; GiST index on `segment_path`; `activity_type_id`/`utm_srid` describe the spatial reference of transformed geometry; `is_course` distinguishes courses from plain segments.


- **Consumers:** `activities.segment_matches`, `activities.segments_details`, leaderboard views (`vw_segment_leaderboard`, `vw_segments_effort_stats`), `vw_segment_create_*` views.





| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |


|---|---|---|---|---|---|---|---|


| `segment_id` | integer | NO | Segment identifier | id | schema.generated.md | schema.generated.md | confirmed |


| `activity_reference_id` | bigint | YES | Source activity the segment was derived from (references activities.activities.activity_id, app-level) | id | schema.generated.md | schema.generated.md | inferred |


| `reference_start_point` | integer | YES | Start point reference on the source activity | point index | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `reference_end_point` | integer | YES | End point reference on the source activity | point index | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `is_course` | boolean | YES | Whether the segment is treated as a course | boolean | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `segment_name` | text | YES | Human-readable segment name | text | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `segment_path` | USER-DEFINED | YES | Segment geometry path (GiST-indexed) | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |


| `distance_m` | numeric | YES | Segment distance | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `elevation_gain` | numeric | YES | Segment elevation gain | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `elevation_loss` | numeric | YES | Segment elevation loss | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `start_point` | USER-DEFINED | YES | Segment start geometry | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |


| `end_point` | USER-DEFINED | YES | Segment end geometry | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |


| `activity_type_id` | smallint | YES | Activity type associated with the segment | id | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |


| `utm_srid` | integer | YES | Spatial reference system id for transformed geometry | srid | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |


| `segment_path_transformed` | USER-DEFINED | YES | Transformed segment geometry (UTM projection) | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |





### activities.segments_details (table)

- **Purpose:** per-effort rollups when an activity is matched against a segment (effort stats feeding leaderboards and reports).

- **Grain:** one row per segment-effort match occurrence; key (`segment_id`, `activity_id`, `activity_start_point`, `activity_end_point`).

- **Canonical source:** `activities.segments_details`; written by the segment matching/update flow (router `segment_update_details` step).

- **Rules:** `enforced` PK (`segment_id`, `activity_id`, `activity_start_point`, `activity_end_point`) and NOT NULL on all key columns; FKs to `activities`/`segments` are application-level convention (no declared DB FKs); `start_time_utc` is UTC.

- **Consumers:** `backend/pifitness/activities/router.py` (segments/segments_details join for activity reports); views `vw_segments_details_ranked`, `vw_segments_effort_stats`, `vw_report_segment_efforts`.



| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |

|---|---|---|---|---|---|---|---|

| `segment_id` | integer | NO | Segment the effort was matched to (references activities.segments.segment_id, app-level) | id | schema.generated.md | schema.generated.md; catalog constraints | inferred |

| `activity_id` | bigint | NO | Activity the effort belongs to (references activities.activities.activity_id, app-level) | id | schema.generated.md | schema.generated.md; catalog constraints | inferred |

| `activity_start_point` | integer | NO | Start point reference of the matched effort within the activity path | point index | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `activity_end_point` | integer | NO | End point reference of the matched effort within the activity path | point index | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `start_time_utc` | timestamp with time zone | YES | Effort start timestamp | UTC timestamptz | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `distance_m` | integer | YES | Effort distance | metres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `elapsed_duration_s` | numeric | YES | Effort elapsed duration | seconds | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `max_hr` | numeric | YES | Maximum heart rate during the effort | beats per minute | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_hr` | numeric | YES | Average heart rate during the effort | beats per minute | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_cadence` | numeric | YES | Average cadence during the effort | steps per minute | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_temp` | numeric | YES | Average air temperature during the effort | degrees Celsius | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_vert_osc` | numeric | YES | Average vertical oscillation during the effort | centimetres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_vert_ratio` | numeric | YES | Average vertical ratio during the effort | ratio | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_gct` | numeric | YES | Average ground contact time during the effort | milliseconds | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_perf` | numeric | YES | Average performance condition during the effort | score | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_balance` | numeric | YES | Average ground contact balance (left) during the effort | percent | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `avg_stride_length` | numeric | YES | Average stride length during the effort | centimetres | migration comment | deploy/migrations/007__comment_backfill_activities.sql | inferred |

| `segment_path` | USER-DEFINED | YES | Segment geometry at match time | geometry-like | migration comment | deploy/migrations/007__comment_backfill_activities.sql | unknown |



### activities.distinct_segments (table)





- **Grain:** one row per distinct segment geometry.





- **Canonical source:** `activities.distinct_segments`.





- **Rules:** `convention` dedup layer.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | NO | Distinct-segment surrogate id | id | schema.generated.md | schema.generated.md | inferred |





| `match_seg_id` | integer | NO | Matched segment id paired with segment_id in this distinct-segment record | id | schema.generated.md | schema.generated.md | inferred |











### activities.metric_list (table)





- **Grain:** one row per activity metric descriptor.





- **Canonical source:** `activities.metric_list`.





- **Rules:** `convention` lookup table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_type_id` | smallint | NO | Garmin activity type code this metric applies to | id | schema.generated.md | schema.generated.md | inferred |





| `metric_name` | text | NO | Name of the activity metric tracked (e.g. heart rate, cadence) | text | schema.generated.md | schema.generated.md | inferred |





| `metric_unit` | text | YES | Unit of the metric (e.g. bpm, spm) | text | schema.generated.md | schema.generated.md | inferred |





| `last_seen_utc` | timestamp with time zone | YES | Last time this metric was observed for the activity type | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |











### activities.overlap_queue (table)





- **Grain:** one row per queued overlap-processing job.





- **Canonical source:** `activities.overlap_queue`.





- **Rules:** `convention` queue table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `queue_id` | integer | NO | Overlap queue entry identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_id_a` | integer | NO | First segment id in the queued overlap pair | id | schema.generated.md | schema.generated.md | inferred |





| `segment_id_b` | integer | NO | Second segment id in the queued overlap pair | id | schema.generated.md | schema.generated.md | inferred |





| `match_confidence` | numeric | YES | Match confidence score for the segment pair | 0..1 | schema.generated.md | schema.generated.md | inferred |





| `created_at` | timestamp with time zone | YES | Queue entry creation time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |











### activities.activity_processing_queue (table)





- **Grain:** one row per queued activity-processing job.





- **Canonical source:** `activities.activity_processing_queue`.





- **Rules:** `convention` queue table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `created_at_utc` | timestamp with time zone | YES | Time the activity was queued for processing (UTC) | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |











### activities.race_predictions_raw (table)





- **Grain:** one row per race prediction snapshot.





- **Canonical source:** `activities.race_predictions_raw`.





- **Rules:** `convention` raw predictions.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Snapshot timestamp (UTC) of the Garmin race prediction. | timestamp, UTC | Garmin import / ETL | deploy/migrations/007__comment_backfill_activities.sql:132; schema.generated.md | inferred, high |





| `prediction_5k_s` | integer | YES | Predicted time to cover 5 km. | seconds | Garmin import / ETL | deploy/migrations/007__comment_backfill_activities.sql:133; schema.generated.md | inferred, high |





| `prediction_10k_s` | integer | YES | Predicted time to cover 10 km. | seconds | Garmin import / ETL | deploy/migrations/007__comment_backfill_activities.sql:134; schema.generated.md | inferred, high |





| `prediction_half_marathon_s` | integer | YES | Predicted time to cover a half marathon (21.0975 km). | seconds | Garmin import / ETL | deploy/migrations/007__comment_backfill_activities.sql:135; schema.generated.md | inferred, high |





| `prediction_marathon_s` | integer | YES | Predicted time to cover a marathon (42.195 km). | seconds | Garmin import / ETL | deploy/migrations/007__comment_backfill_activities.sql:136; schema.generated.md | inferred, high |











### activities.segment_match_exclusions (table)





- **Grain:** one row per excluded segment match.





- **Canonical source:** `activities.segment_match_exclusions`.





- **Rules:** `enforced` FKs to activities and segments.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Excluded segment (FK to activities.segments). | integer | segment matching exclusion list | deploy/migrations/007__comment_backfill_activities.sql:138; backend/pifitness/activities/queries.py | confirmed, high |





| `activity_id` | bigint | YES | Excluded activity (FK to activities.activities). | integer | segment matching exclusion list | deploy/migrations/007__comment_backfill_activities.sql:139; backend/pifitness/activities/queries.py | confirmed, high |











### activities.elevation_reference (table)





- **Grain:** one row per elevation reference point.





- **Canonical source:** `activities.elevation_reference`.





- **Rules:** `convention` reference data.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `grid_5m_00_id` | USER-DEFINED | NO | 5m grid cell id | id | schema.generated.md | schema.generated.md | inferred |





| `avg_elevation` | double precision | NO | Average elevation | metres | schema.generated.md | schema.generated.md | inferred |





| `obs_count` | integer | NO | Observation count | count | schema.generated.md | schema.generated.md | inferred |





| `last_updated` | timestamp with time zone | YES | Last updated | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `elev_stddev` | numeric | YES | Elevation standard deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `elevation_hammered` | numeric | YES | Hammered elevation | metres | schema.generated.md | schema.generated.md | inferred |





| `elevation_sanded` | numeric | YES | Sanded elevation | metres | schema.generated.md | schema.generated.md | inferred |











### activities.activity_resampled_1m (table)





- **Grain:** one row per minute-bucketed activity metrics.





- **Canonical source:** `activities.activity_resampled_1m`.





- **Rules:** `convention` derived aggregate.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `distance_m` | integer | YES | Distance | metres | schema.generated.md | schema.generated.md | inferred |





| `latitude` | double precision | YES | Latitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `longitude` | double precision | YES | Longitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `elevation_m` | double precision | YES | Elevation | metres | schema.generated.md | schema.generated.md | inferred |





| `elapsed_duration_s` | double precision | YES | Elapsed duration | seconds | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_header_stats (view)





- **Purpose:** per-activity header stats for run/walk/hike activities > 1000 m with a path: basic activity type, distance in miles, pace, max/median HR, and path.





- **Grain:** one row per activity type (run | walk | other) — `GROUP BY activity_type_basic`, picking `max(activity_id)` per type.





- **Canonical source:** derived view over `activities.activities` (CTE `most_recent`) joined to `activities.activity_details` (CTE `hr_values`).





- **Derived logic:** `activity_type_basic` = 'run' if type like %run% else 'walk' if like %walk%/hik% else 'other'; `distance_mi = m2mi(distance_m)::text || ' mi'`; `pace_spm = duration_elapsed_s / m2mi(distance_m)`; `hr_maximum = max(heartrate_bpm)`; `hr_median = percentile_disc(0.5)` of heartrate_bpm.





- **Dependencies:** `activities.activities`, `activities.activity_details`; uses `m2mi()`, `pace_as_text()`.





- **Consumers:** activity header display.





- **Rules:** read-only view; `most_recent` filters walk/hike/run with distance > 1000 m and path NOT NULL, grouped by type.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `max(activities.activity_id)` per type | schema_only.sql:9104 | inferred |





| `activity_type_basic` | text | YES | Basic activity type | 'run' | 'walk' | 'other' | CASE on activity_type_name | schema_only.sql:9105-9109 | inferred |





| `distance_mi` | text | YES | Distance | miles text | `m2mi(distance_m) || ' mi'` | schema_only.sql:9129 | inferred |





| `duration_display` | text | YES | Duration display | text | formatted duration | schema.generated.md | inferred |





| `pace_display` | text | YES | Pace display | text | `pace_as_text(...)` | schema.generated.md | inferred |





| `start_time_utc` | timestamp with time zone | YES | Activity start | UTC timestamptz | `activities.start_time_utc` | schema.generated.md | confirmed |





| `activity_path` | USER-DEFINED | YES | Activity path | USER-DEFINED | `activities.activity_path` | schema.generated.md | confirmed |





| `hr_median` | smallint | YES | Median heart rate | bpm | `percentile_disc(0.5)` of heartrate_bpm | schema_only.sql:9122 | inferred |





| `hr_maximum` | smallint | YES | Max heart rate | bpm | `max(heartrate_bpm)` | schema_only.sql:9121 | inferred |











### activities.vw_activity_summary (view)





- **Purpose:** per-activity summary: distance/pace, segment context (name/id/course), activity time, segment distance, all-time/best/prior ranks and deltas, and segment pace.





- **Grain:** one row per activity.





- **Canonical source:** derived view over `activities.activities` left-joined 3x to `activities.vw_segment_leaderboard` (current, best all-time-rank=1, prior recency_rank+1).





- **Derived logic:** `distance_mi = m2mi(distance_m)`; `activity_pace = pace_as_text(m2mi(distance_m), duration_elapsed_s)`; `best_delta_s = best - activity_time`; `prior_delta_s = prior - activity_time`; `segment_pace = pace_as_text(segment_distance_mi, activity_time_s)`.





- **Dependencies:** `activities.activities`, `activities.vw_segment_leaderboard` (3x).





- **Consumers:** activity summary UI.





- **Rules:** read-only view; ordered by `is_course DESC, start_time_utc`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema_only.sql:11172 | confirmed |





| `activity_start_utc` | timestamp with time zone | YES | Activity start | UTC timestamptz | `activities.start_time_utc` | schema_only.sql:11173 | confirmed |





| `distance_mi` | numeric | YES | Distance | miles | `m2mi(distance_m)` | schema_only.sql:11174 | inferred |





| `activity_pace` | text | YES | Activity pace | text | `pace_as_text(m2mi(distance_m), duration_elapsed_s)` | schema_only.sql:11175 | inferred |





| `segment_name` | text | YES | Segment name | text | `vw_segment_leaderboard.segment_name` | schema_only.sql:11176 | confirmed |





| `segment_id` | integer | YES | Segment id | id | `vw_segment_leaderboard.segment_id` | schema_only.sql:11177 | confirmed |





| `is_course` | boolean | YES | Is course flag | boolean | `vw_segment_leaderboard.is_course` | schema_only.sql:11178 | confirmed |





| `activity_time_s` | numeric | YES | Activity time | seconds | `vw_segment_leaderboard.elapsed_duration_s` | schema_only.sql:11179 | confirmed |





| `segment_distance_mi` | numeric | YES | Segment distance | miles | `vw_segment_leaderboard.distance_mi` | schema_only.sql:11180 | confirmed |





| `all_time_rank` | bigint | YES | All-time rank | ordinal | `vw_segment_leaderboard.all_time_rank` | schema_only.sql:11181 | confirmed |





| `best_time_s` | numeric | YES | Best time | seconds | `vw_segment_leaderboard` (all_time_rank=1) | schema_only.sql:11182 | confirmed |





| `prior_time_s` | numeric | YES | Prior time | seconds | `vw_segment_leaderboard` (recency_rank+1) | schema_only.sql:11183 | confirmed |





| `best_delta_s` | numeric | YES | Best delta | seconds | `best - activity_time` | schema_only.sql:11184 | inferred |





| `prior_delta_s` | numeric | YES | Prior delta | seconds | `prior - activity_time` | schema_only.sql:11185 | inferred |





| `segment_pace` | text | YES | Segment pace | text | `pace_as_text(segment_distance_mi, activity_time_s)` | schema_only.sql:11186 | inferred |











### activities.segment_matches (table)





- **Purpose:** confirmed segment matches linking a segment to an activity with start/end points and the matched activity path.





- **Grain:** one row per `(segment_id, activity_id)` match.





- **Canonical source:** `activities.segment_matches`; write path not evidenced in application code searched (populated by the segment-matching pipeline).





- **Consumers:** `activities.vw_segment_comps` (joins temp_activity_segment_metas to segments_details), segment leaderboard/effort views.





- **Relationships:** `enforced` composite PK `(segment_id, activity_id)` (`segment_matches_pkey`); `enforced` FK `activity_id` -> `activities.activities.activity_id` (`segment_matches_activity_id_fkey`); `segment_id` references `activities.segments.segment_id` at application level.





- **Rules:** `enforced` composite PK + unique index `uc_segment_match_act_seg_ids`; GIST index `idx_segment_activities_path_gist` on `activity_path`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | NO | Segment id | id | PK part; references `activities.segments.segment_id` | schema.generated.md:230-247 | confirmed |





| `activity_id` | bigint | NO | Activity id | id | PK part; FK to `activities.activities.activity_id` | schema.generated.md:230-247 | confirmed |





| `activity_start_point` | integer | YES | Activity start point | ordinal | segment-match result | schema.generated.md:233 | confirmed |





| `activity_end_point` | integer | YES | Activity end point | ordinal | segment-match result | schema.generated.md:234 | confirmed |





| `activity_path` | USER-DEFINED | YES | Matched activity path | geometry/geography | segment-match result | schema.generated.md:235 | confirmed |





| `match_confirmed` | boolean | YES | Match confirmed flag | boolean | segment-match result | schema.generated.md:236 | confirmed |





| `match_confidence` | numeric | YES | Match confidence | score | segment-match result | schema.generated.md:237 | confirmed |





| `match_ts_utc` | timestamp with time zone | YES | Match timestamp | UTC timestamptz | segment-match result | schema.generated.md:238 | confirmed |













### activities.steps_raw (table)





- **Purpose:** raw per-second step samples keyed by timestamp; used to derive step cadence and daily step totals.





- **Grain:** one row per timestamp, keyed by `ts_utc`.





- **Canonical source:** `activities.steps_raw`; write path not evidenced in application code searched (populated by activity import/ETL).





- **Consumers:** step/cadence analytics views (not individually evidenced).





- **Relationships:** `enforced` PK `ts_utc` (`steps_raw_pkey`).





- **Rules:** `enforced` PK; `ts_utc` NOT NULL; `step_goal`/`total_steps`/`total_distance_m` nullable integers.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | PK | schema.generated.md:298-307 | confirmed |





| `step_goal` | integer | YES | Daily step goal | steps | source | schema.generated.md:299 | confirmed |





| `total_steps` | integer | YES | Total steps | steps | source | schema.generated.md:300 | confirmed |





| `total_distance_m` | integer | YES | Total distance | metres | source | schema.generated.md:301 | confirmed |



















### activities.temp_activity_segment_metas (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch per-activity segment metadata (`activity_id`, `start_point`, `end_point`) used by segment matching; working table rebuilt per run.





- **Rules:** no declared PK; transient scratch table; columns marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md:346-352 | unknown |





| `start_point` | integer | YES | Segment start point | ordinal | scratch | schema.generated.md:347-352 | unknown |





| `end_point` | integer | YES | Segment end point | ordinal | scratch | schema.generated.md:348-352 | unknown |











### activities.temp_segment_details (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch per-activity segment detail rows (`activity_id`, `activity_type_id`, `is_course`, `segment_name`, lat/lon/elevation/distance/deltas) used by segment matching; working table rebuilt per run.





- **Rules:** no declared PK; transient scratch table; columns marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md | unknown |





| `activity_type_id` | smallint | YES | Activity type id | id | scratch | schema.generated.md | unknown |





| `is_course` | boolean | YES | Is course flag | boolean | scratch | schema.generated.md | unknown |





| `segment_name` | text | YES | Segment name | text | scratch | schema.generated.md | unknown |





| `row_asc` | integer | YES | Ascending row index | ordinal | scratch | schema.generated.md | unknown |





| `row_desc` | integer | YES | Descending row index | ordinal | scratch | schema.generated.md | unknown |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | scratch | schema.generated.md | unknown |





| `latitude` | numeric | YES | Latitude | degrees | scratch | schema.generated.md | unknown |





| `longitude` | numeric | YES | Longitude | degrees | scratch | schema.generated.md | unknown |





| `elevation_m` | numeric | YES | Elevation | metres | scratch | schema.generated.md | unknown |





| `distance_m` | numeric | YES | Distance | metres | scratch | schema.generated.md | unknown |





| `elevation_delta` | numeric | YES | Elevation delta | metres | scratch | schema.generated.md | unknown |











### activities.temp_segment_matches_downselect (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch segment-match downselect candidates (`activity_id`, best start/end distances, `full_path`/`short_path` paths, `segment_id`, `confidence`, `ruggedness`, various deviation metrics) used by segment matching; working table rebuilt per run.





- **Rules:** no declared PK; transient scratch table; columns marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md | unknown |





| `best_start_dist` | integer | YES | Best start distance | distance | scratch | schema.generated.md | unknown |





| `best_end_dist` | integer | YES | Best end distance | distance | scratch | schema.generated.md | unknown |





| `full_path` | USER-DEFINED | YES | Full path geometry | USER-DEFINED | scratch | schema.generated.md | unknown |





| `confidence` | numeric | YES | Match confidence | score | scratch | schema.generated.md | unknown |





| `ruggedness` | numeric | YES | Path ruggedness | score | scratch | schema.generated.md | unknown |





| `utm_srid` | integer | YES | UTM spatial reference id | id | scratch | schema.generated.md | unknown |





| `short_path` | USER-DEFINED | YES | Short path geometry | USER-DEFINED | scratch | schema.generated.md | unknown |





| `segment_id` | integer | YES | Segment id | id | scratch | schema.generated.md | unknown |





| `dist_deviation` | numeric | YES | Distance deviation | metres | scratch | schema.generated.md | unknown |





| `full_path_transformed` | USER-DEFINED | YES | Transformed full path | USER-DEFINED | scratch | schema.generated.md | unknown |





| `freschet_deviation_m` | numeric | YES | Fréchet deviation | metres | scratch | schema.generated.md | unknown |





| `polygon_deviation_m` | numeric | YES | Polygon deviation | metres | scratch | schema.generated.md | unknown |





| `hausdorff_deviation_m` | numeric | YES | Hausdorff deviation | metres | scratch | schema.generated.md | unknown |





| `heading_deviation` | numeric | YES | Heading deviation | degrees | scratch | schema.generated.md | unknown |











### activities.temp_segment_matches_v1 (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch segment-match candidates v1 (`segment_id`, `activity_id`, start/end points, `activity_path`, `match_confirmed`/`match_confidence`, `match_ts_utc`) used by segment matching; working table rebuilt per run.





- **Rules:** no declared PK; transient scratch table; columns marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | scratch | schema.generated.md | unknown |





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md | unknown |





| `activity_start_point` | integer | YES | Activity start point | ordinal | scratch | schema.generated.md | unknown |





| `activity_end_point` | integer | YES | Activity end point | ordinal | scratch | schema.generated.md | unknown |





| `activity_path` | USER-DEFINED | YES | Activity path | USER-DEFINED | scratch | schema.generated.md | unknown |





| `match_confirmed` | boolean | YES | Match confirmed flag | boolean | scratch | schema.generated.md | unknown |





| `match_confidence` | numeric | YES | Match confidence | score | scratch | schema.generated.md | unknown |





| `match_ts_utc` | timestamp with time zone | YES | Match timestamp | UTC timestamptz | scratch | schema.generated.md | unknown |











### activities.temp_segment_matching_activities (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch list of activity ids pending segment matching; working table rebuilt per run.





- **Rules:** `activity_id` NOT NULL; transient scratch table; column marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | scratch | schema.generated.md | unknown |











### activities.temp_segment_matching_segments (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch list of segment ids pending segment matching; working table rebuilt per run.





- **Rules:** transient scratch table; column marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | scratch | schema.generated.md | unknown |











### activities.temp_segments_v1 (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `activities.temp_*` tables); existence documented only.





- **Purpose:** scratch segment candidate rows v1 (`segment_id`, `activity_reference_id`, start/end reference points, `is_course`, `segment_name`/`segment_path`, distance/elevation deltas, start/end points, `activity_type_id`) used by segment matching; working table rebuilt per run.





- **Rules:** no declared PK; transient scratch table; columns marked unknown (not application data).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | scratch | schema.generated.md | unknown |





| `activity_reference_id` | bigint | YES | Reference activity id | id | scratch | schema.generated.md | unknown |





| `reference_start_point` | integer | YES | Reference start point | ordinal | scratch | schema.generated.md | unknown |





| `reference_end_point` | integer | YES | Reference end point | ordinal | scratch | schema.generated.md | unknown |





| `is_course` | boolean | YES | Is course flag | boolean | scratch | schema.generated.md | unknown |





| `segment_name` | text | YES | Segment name | text | scratch | schema.generated.md | unknown |





| `segment_path` | USER-DEFINED | YES | Segment path | USER-DEFINED | scratch | schema.generated.md | unknown |





| `distance_m` | numeric | YES | Distance | metres | scratch | schema.generated.md | unknown |





| `elevation_gain` | numeric | YES | Elevation gain | metres | scratch | schema.generated.md | unknown |





| `elevation_loss` | numeric | YES | Elevation loss | metres | scratch | schema.generated.md | unknown |





| `start_point` | USER-DEFINED | YES | Start point | USER-DEFINED | scratch | schema.generated.md | unknown |





| `end_point` | USER-DEFINED | YES | End point | USER-DEFINED | scratch | schema.generated.md | unknown |





| `activity_type_id` | smallint | YES | Activity type id | id | scratch | schema.generated.md | unknown |











### activities.training_cycle_dates (table)





- **Purpose:** training-cycle date windows (cycle start + name) used to segment activity history into cycles (e.g. current cycle vs last 365 days vs all time).





- **Grain:** one row per training cycle, keyed by `cycle_start_utc`.





- **Canonical source:** `activities.training_cycle_dates`; write path not evidenced in application code searched.





- **Consumers:** `activities.vw_segments_details_ranked` (computes `cycle_name`), segment ranking views.





- **Relationships:** `enforced` PK `cycle_start_utc` (`training_cycle_dates_pkey`).





- **Rules:** `enforced` PK; `cycle_start_utc` NOT NULL; `cycle_name` free text.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `cycle_start_utc` | timestamp with time zone | NO | Cycle start timestamp | UTC timestamptz | PK | schema.generated.md:390-397 | confirmed |





| `cycle_name` | text | YES | Cycle name | free text | source | schema.generated.md:393 | confirmed |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `cycle_start_utc` | timestamp with time zone | NO | Cycle start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `cycle_name` | text | YES | Training cycle name | text | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_elevation_deltas (view)





- **Purpose:** per-sample elevation deltas computed from smoothed elevation via a 3-point average minus the lagged value.





- **Grain:** one row per `activities.activity_details` sample.





- **Canonical source:** derived view over `activities.activity_details`.





- **Derived logic:** `elevation_delta = round(((elevation_m_smooth + COALESCE(lag(elevation_m_smooth), elevation_m_smooth) + COALESCE(lead(elevation_m_smooth), elevation_m_smooth)) / 3 - COALESCE(lag(elevation_m_smooth), elevation_m_smooth)), 1)` partitioned by activity_id ordered by elapsed_duration_s.





- **Consumers:** elevation-smoothing analysis.





- **Rules:** read-only view.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activity_details.activity_id` | schema.generated.md:399-404 | confirmed |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | `activity_details.elapsed_duration_s` | schema.generated.md:400 | confirmed |





| `elevation_delta` | numeric | YES | Elevation delta | metres | 3-point smooth minus lag | schema_only.sql:9073 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `elevation_delta` | numeric | YES | Elevation delta | metres | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_ids_to_sync (view)





- **Purpose:** candidate activities needing GPX/path sync (not downloaded, not treadmill, distance > 1000 m), limited to 15, prioritizing running activities.





- **Grain:** one row per candidate activity.





- **Canonical source:** derived view over `activities.activities`.





- **Derived logic:** `max_points = round(duration_elapsed_s, 0) * 2`; filter `NOT is_downloaded AND NOT treadmill AND distance_m > 1000`; ordered by running-first then start desc; LIMIT 15.





- **Consumers:** activity sync worker (GPX download).





- **Rules:** read-only view; `max_points` heuristic (2 points per elapsed second).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema.generated.md:9230-9236 | confirmed |





| `max_points` | numeric | YES | Max path points | count (points) | `round(duration_elapsed_s, 0) * 2` | schema_only.sql:9232 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `max_points` | numeric | YES | Max points | count | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_path_updates (view)





- **Purpose:** rebuilt GPX activity path (`activity_path` geography linestring) per activity from smoothed activity detail points.





- **Grain:** one row per activity.





- **Canonical source:** derived view joining `activities.activities` to `activities.activity_details`.





- **Derived logic:** `activity_path = ST_SetSRID(ST_MakeLine(ST_MakePoint(longitude, latitude, elevation_m_smooth, elapsed_duration_s) ORDER BY elapsed_duration_s), 4326)` grouped by activity.





- **Consumers:** `build_activity_path` sets `activities.activities.activity_path`/`is_downloaded`.





- **Rules:** read-only view; `activity_path` is PostGIS geography SRID 4326 with M (elapsed duration) dimension.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema.generated.md:9257-9262 | confirmed |





| `activity_path` | USER-DEFINED | YES | Activity path | geography SRID 4326 | `ST_MakeLine(...)` of smoothed points | schema_only.sql:9259 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_path` | USER-DEFINED | YES | Activity geometry/path | geometry | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_segments_aggregated (view)





- **Purpose:** downsampled per-segment telemetry arrays (path coords, timestamps, HR, cadence, elevation, etc.) for segment detail charts.





- **Grain:** one row per `(id_val, meta_rank)` segment composite.





- **Canonical source:** derived view over `activities.vw_segment_comps`; downsamples telemetry via `DISTINCT ON (meta_rank, id_val, elapsed_duration_s)` then aggregates into JSON/arrays ordered by elapsed duration.





- **Dependencies:** `activities.vw_segment_comps`; uses `c2f()` for temperature.





- **Consumers:** segment detail chart rendering.





- **Rules:** read-only view; `elapsed_ms` converted to seconds (`/1000`); air temp converted Celsius→Fahrenheit via `c2f()`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `id_val` | text | YES | Segment composite id | text | `vw_segment_comps.id_val` | schema.generated.md:9554 | confirmed |





| `meta_rank` | integer | YES | Meta rank | ordinal | `vw_segment_comps.meta_rank` | schema.generated.md:9555 | confirmed |





| `path_coords` | json | YES | Path coordinates | json array | `json_agg([lon, lat])` ordered by elapsed | schema_only.sql:9556 | inferred |





| `map_timestamps` | json | YES | Timestamps | json array | `json_agg(elapsed_duration_s)` | schema_only.sql:9557 | inferred |





| `x_time` | json | YES | X time array | json array | `array_agg(elapsed_duration_s)` | schema_only.sql:9558 | inferred |





| `x_distance` | json | YES | X distance array | json array | `array_agg(distance_m)` | schema_only.sql:9559 | inferred |





| `y_time` | json | YES | Y time (s) array | json array | `round(elapsed_ms/1000.0, 2)` | schema_only.sql:9560 | inferred |





| `y_time_delta` | json | YES | Y time-delta array | json array | `round(elapsed_ms_delta/1000.0, 2)` | schema_only.sql:9561 | inferred |





| `y_hr` | json | YES | HR array | json array | `array_agg(heartrate_bpm)` | schema_only.sql:9562 | inferred |





| `y_hr_delta` | json | YES | HR delta array | json array | `array_agg(heartrate_bpm_delta)` | schema_only.sql:9563 | inferred |





| `y_elevation` | json | YES | Elevation array | json array | `array_agg(elevation_m_smooth)` | schema_only.sql:9564 | inferred |





| `y_cadence` | json | YES | Cadence array | json array | `array_agg(cadence_spm)` | schema_only.sql:9565 | inferred |





| `y_balance` | json | YES | GCT balance array | json array | `array_agg(ground_contact_balance_left)` | schema_only.sql:9566 | inferred |





| `y_performance` | json | YES | Performance array | json array | `array_agg(performance_condition)` | schema_only.sql:9567 | inferred |





| `y_vert_ratio` | json | YES | Vert ratio array | json array | `array_agg(vert_ratio)` | schema_only.sql:9568 | inferred |





| `y_osc` | json | YES | Vert oscillation array | json array | `array_agg(vert_oscillation_cm)` | schema_only.sql:9569 | inferred |





| `y_temp` | json | YES | Temp array (°F) | json array | `c2f(air_temp_c)` | schema_only.sql:9570 | inferred |





| `y_stride` | json | YES | Stride array | json array | `array_agg(stride_length_cm)` | schema_only.sql:9571 | inferred |





| `y_gct` | json | YES | GCT array | json array | `array_agg(ground_contact_time_ms)` | schema_only.sql:9572 | inferred |





| `y_speed` | json | YES | Speed array | json array | `array_agg(speed_mmps)` | schema_only.sql:9573 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `id_val` | text | YES | Identifier value | id | schema.generated.md | schema.generated.md | inferred |





| `meta_rank` | bigint | YES | Metadata rank | rank | schema.generated.md | schema.generated.md | inferred |





| `path_coords` | json | YES | Path coordinates | geometry | schema.generated.md | schema.generated.md | inferred |





| `map_timestamps` | json | YES | Mapped timestamps | timestamp | schema.generated.md | schema.generated.md | inferred |





| `x_time` | ARRAY | YES | X time | seconds | schema.generated.md | schema.generated.md | inferred |





| `x_distance` | ARRAY | YES | X distance | metres | schema.generated.md | schema.generated.md | inferred |





| `y_time` | ARRAY | YES | Y time | seconds | schema.generated.md | schema.generated.md | inferred |





| `y_time_delta` | ARRAY | YES | Y time delta | seconds | schema.generated.md | schema.generated.md | inferred |





| `y_hr` | ARRAY | YES | Y heart rate | bpm | schema.generated.md | schema.generated.md | inferred |





| `y_hr_delta` | ARRAY | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `y_elevation` | ARRAY | YES | Y elevation | metres | schema.generated.md | schema.generated.md | inferred |





| `y_cadence` | ARRAY | YES | Y cadence | spm | schema.generated.md | schema.generated.md | inferred |





| `y_balance` | ARRAY | YES | Y balance | percent | schema.generated.md | schema.generated.md | inferred |





| `y_performance` | ARRAY | YES | Y performance | score | schema.generated.md | schema.generated.md | inferred |





| `y_vert_ratio` | ARRAY | YES | Y vertical ratio | percent | schema.generated.md | schema.generated.md | inferred |





| `y_osc` | ARRAY | YES | Y oscillation | cm | schema.generated.md | schema.generated.md | inferred |





| `y_temp` | ARRAY | YES | Y temperature | celsius | schema.generated.md | schema.generated.md | inferred |





| `y_stride` | ARRAY | YES | Y stride length | metres | schema.generated.md | schema.generated.md | inferred |





| `y_gct` | ARRAY | YES | Y ground contact time | ms | schema.generated.md | schema.generated.md | inferred |





| `y_speed` | ARRAY | YES | Y speed | m/s | schema.generated.md | schema.generated.md | inferred |











### activities.vw_activity_sleep (view)





- **Purpose:** sleep context for each activity (sleep score, hours slept, hours awake prior) using the most recent sleep ending on the activity date.





- **Grain:** one row per activity.





- **Canonical source:** derived view over `activities.activities` and `health.sleep_totals`.





- **Derived logic:** `awake_hours_prior = floor(epoch(start_time_utc - sleep_end_utc)) / 3600.0`; `one_day_sleep_hours = round(sleep_duration_s/3600, 1)`; `avg_sleep_hours = round(avg over prior 3 days, 1)`.





- **Dependencies:** `health.sleep_totals`.





- **Consumers:** activity summary / fatigue context.





- **Rules:** read-only view; `awake_hours_prior` relative to most recent sleep end.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema.generated.md:9850-9872 | confirmed |





| `awake_hours_prior` | numeric | YES | Hours awake before activity | hours | `epoch(start - sleep_end)/3600` | schema_only.sql:9867 | inferred |





| `sleep_score` | integer | YES | Sleep score | score points | `health.sleep_totals.sleep_score` | schema.generated.md:9868 | confirmed |





| `avg_sleep_hours` | numeric | YES | Average sleep hours | hours | 3-day avg of sleep_duration_s/3600 | schema_only.sql:9861 | inferred |





| `one_day_sleep_hours` | numeric | YES | One-day sleep hours | hours | `sleep_duration_s/3600` | schema_only.sql:9860 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `awake_hours_prior` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `sleep_score` | integer | YES | Sleep score | score | schema.generated.md | schema.generated.md | inferred |





| `avg_sleep_hours` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `one_day_sleep_hours` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_activity_summary_segment_creation (view)





- **Grain:** one row per vw activity summary segment creation (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_activity_summary_segment_creation`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_type_name` | text | YES | Activity type name | text | schema.generated.md | schema.generated.md | inferred |





| `start_time_utc` | timestamp with time zone | YES | Activity start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `distance_mi` | numeric | YES | Distance | miles | schema.generated.md | schema.generated.md | inferred |





| `distance_m` | numeric | YES | Distance | metres | schema.generated.md | schema.generated.md | inferred |





| `child_courses` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `child_segments` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `matched_courses` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `matched_segments` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_activity_training_status (view)





- **Purpose:** per-activity training status context: latest pre-activity VO2 max, acclimation, training load, resting HR, and sleep.





- **Grain:** one row per activity.





- **Canonical source:** derived view joining `activities.activities` to `health.vw_heart_rates`, `health.sleep_totals` (latest before activity), and `health.training_status_raw` (latest before activity).





- **Derived logic:** `training_load_mid = (max+min)/2`; `training_readiness = round((min-acute)/min, 3)`; `training_load_pct = round(acute/max, 3)`; `awake_s_before_activity = epoch(start - sleep_end)`.





- **Dependencies:** `health.vw_heart_rates`, `health.sleep_totals`, `health.training_status_raw`.





- **Consumers:** training-readiness dashboards.





- **Rules:** read-only view; uses `NOW()`-independent lateral latest-before lookups.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema.generated.md:10095-10118 | confirmed |





| `vo2_max_value` | numeric | YES | VO2 max | score points | `training_status_raw.vo2_max_value` | schema.generated.md:10097 | confirmed |





| `altitude_acclimation_m` | integer | YES | Altitude acclimation | metres | `training_status_raw.altitude_acclimation_m` | schema.generated.md:10098 | confirmed |





| `heat_acclimation_pct` | numeric | YES | Heat acclimation pct | percent | `training_status_raw.heat_acclimation_pct` | schema.generated.md:10099 | confirmed |





| `training_load_acute` | numeric | YES | Acute training load | score | `training_status_raw.training_load_acute` | schema.generated.md:10100 | confirmed |





| `training_load_pct` | numeric | YES | Load pct of max | ratio (0-1) | `round(acute/max, 3)` | schema_only.sql:10147 | inferred |





| `training_readiness` | numeric | YES | Readiness score | ratio (0-1) | `round((min-acute)/min, 3)` | schema_only.sql:10147 | inferred |





| `training_load_mid` | numeric | YES | Mid training load | score | `(max+min)/2` | schema_only.sql:10146 | inferred |





| `training_load_max` | numeric | YES | Max training load | score | `training_status_raw.training_load_max` | schema.generated.md:10104 | confirmed |





| `training_load_min` | numeric | YES | Min training load | score | `training_status_raw.training_load_min` | schema.generated.md:10105 | confirmed |





| `resting_hr_asleep` | integer | YES | Resting HR asleep | bpm | `health.vw_heart_rates.resting_hr_asleep` | schema.generated.md:10106 | confirmed |





| `resting_hr_awake` | integer | YES | Resting HR awake | bpm | `health.vw_heart_rates.resting_hr_awake` | schema.generated.md:10107 | confirmed |





| `sleep_duration_s` | integer | YES | Sleep duration | seconds | `health.sleep_totals.sleep_duration_s` | schema.generated.md:10108 | confirmed |





| `sleep_score` | integer | YES | Sleep score | score points | `health.sleep_totals.sleep_score` | schema.generated.md:10109 | confirmed |





| `light_sleep_s` | integer | YES | Light sleep | seconds | `health.sleep_totals.light_sleep_s` | schema.generated.md:10110 | confirmed |





| `deep_sleep_s` | integer | YES | Deep sleep | seconds | `health.sleep_totals.deep_sleep_s` | schema.generated.md:10111 | confirmed |





| `awake_sleep_s` | integer | YES | Awake sleep | seconds | `health.sleep_totals.awake_sleep_s` | schema.generated.md:10112 | confirmed |





| `rem_sleep_s` | integer | YES | REM sleep | seconds | `health.sleep_totals.rem_sleep_s` | schema.generated.md:10113 | confirmed |





| `awake_s_before_activity` | numeric | YES | Seconds awake before activity | seconds | `epoch(start - sleep_end)` | schema_only.sql:10114 | inferred |





| `start_time_utc` | timestamp with time zone | YES | Activity start | UTC timestamptz | `activities.start_time_utc` | schema.generated.md:10115 | confirmed |





| `sleep_end_utc` | timestamp with time zone | YES | Sleep end | UTC timestamptz | `health.sleep_totals.sleep_end_utc` | schema.generated.md:10116 | confirmed |





| `sleep_end_date` | date | YES | Sleep end date | calendar date | `health.sleep_totals.sleep_end_date` | schema.generated.md:10117 | confirmed |





| `activity_start_date` | date | YES | Activity start date | calendar date | `activities.start_time_utc::date` | schema_only.sql:10118 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `vo2_max_value` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `altitude_acclimation_m` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `heat_acclimation_pct` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_load_acute` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_load_pct` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_readiness` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_load_mid` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_load_max` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `training_load_min` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `resting_hr_asleep` | double precision | YES | Resting heart rate while asleep | bpm | schema.generated.md | schema.generated.md | inferred |





| `resting_hr_awake` | double precision | YES | Resting heart rate while awake | bpm | schema.generated.md | schema.generated.md | inferred |





| `sleep_duration_s` | integer | YES | Sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `sleep_score` | integer | YES | Sleep score | score | schema.generated.md | schema.generated.md | inferred |





| `light_sleep_s` | integer | YES | Light sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `deep_sleep_s` | integer | YES | Deep sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `awake_sleep_s` | integer | YES | Awake time during sleep window | seconds | schema.generated.md | schema.generated.md | inferred |





| `rem_sleep_s` | integer | YES | REM sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `awake_s_before_activity` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `start_time_utc` | timestamp with time zone | YES | Activity start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `sleep_end_utc` | timestamp with time zone | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `sleep_end_date` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `activity_start_date` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_activity_weights (view)





- **Purpose:** per-activity interpolated body weight/muscle/fat in lbs, bracketing the activity start with the nearest prior and next body-composition samples.





- **Grain:** one row per activity with both prior and next composition samples.





- **Canonical source:** derived view over `activities.activities` and `health.body_composition_raw` (two lateral latest-before/after lookups), interpolating weight/muscle/fat.





- **Dependencies:** `health.body_composition_raw`.





- **Consumers:** body-composition context for activities.





- **Rules:** read-only view; `g2lb()` converts grams to pounds; interpolation is linear between bracketing samples.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activities.activity_id` | schema_only.sql (~10446) | confirmed |





| `weight_lb` | numeric | YES | Interpolated weight | pounds | `g2lb(interpolated_weight_g)` | schema_only.sql:10439 | inferred |





| `muscle_lb` | numeric | YES | Interpolated muscle | pounds | `g2lb(interpolated_muscle_g)` | schema_only.sql:10440 | inferred |





| `fat_lb` | numeric | YES | Interpolated fat | pounds | `g2lb(weight_g * fat_pct)` | schema_only.sql:10441 | inferred |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `weight_lb` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `muscle_lb` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `fat_lb` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_course_review (view)





- **Grain:** one row per vw course review (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_course_review`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `activity_reference_id` | bigint | YES | Reference activity id | id | schema.generated.md | schema.generated.md | inferred |





| `reference_start_point` | integer | YES | Reference segment start point index | index | schema.generated.md | schema.generated.md | inferred |





| `reference_end_point` | integer | YES | Reference segment end point index | index | schema.generated.md | schema.generated.md | inferred |





| `effort_label` | text | YES | Effort label | text | schema.generated.md | schema.generated.md | inferred |





| `activity_type_name` | text | YES | Activity type name | text | schema.generated.md | schema.generated.md | inferred |





| `activity_path` | USER-DEFINED | YES | Activity geometry/path | geometry | schema.generated.md | schema.generated.md | inferred |





| `distance_mi` | numeric | YES | Distance | miles | schema.generated.md | schema.generated.md | inferred |





| `elevation_gain` | numeric | YES | Elevation gain | metres | schema.generated.md | schema.generated.md | inferred |





| `elevation_loss` | numeric | YES | Elevation loss | metres | schema.generated.md | schema.generated.md | inferred |





| `matched_activities` | bigint | YES | Count of matched activities | count | schema.generated.md | schema.generated.md | inferred |





| `last_event_utc` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `course_idx` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `max_course_idx` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `new_count` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_elevation_smooth (view)





- **Grain:** one row per vw elevation smooth (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_elevation_smooth`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `latitude` | numeric | YES | Latitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `longitude` | numeric | YES | Longitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `lat5` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `long5` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `elevation_raw` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `elevation_m` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `geog` | USER-DEFINED | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_last_activity_id_by_type (view)





- **Grain:** one row per vw last activity id by type (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_last_activity_id_by_type`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_type` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_matched_segment_path_updates (view)





- **Grain:** one row per vw matched segment path updates (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_matched_segment_path_updates`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_path` | USER-DEFINED | YES | Activity geometry/path | geometry | schema.generated.md | schema.generated.md | inferred |











### activities.vw_overlapped_segments (view)





- **Grain:** one row per vw overlapped segments (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_overlapped_segments`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `match_rank` | integer | YES | Match rank | rank | schema.generated.md | schema.generated.md | inferred |





| `match_confidence` | numeric | YES | Match confidence score | 0..1 | schema.generated.md | schema.generated.md | inferred |





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `effort_label` | text | YES | Effort label | text | schema.generated.md | schema.generated.md | inferred |





| `activity_path` | USER-DEFINED | YES | Activity geometry/path | geometry | schema.generated.md | schema.generated.md | inferred |





| `matched_activities` | ARRAY | YES | Count of matched activities | count | schema.generated.md | schema.generated.md | inferred |











### activities.vw_possible_segment_matches (view)





- **Grain:** one row per vw possible segment matches (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_possible_segment_matches`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_date` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `match_confidence` | numeric | YES | Match confidence score | 0..1 | schema.generated.md | schema.generated.md | inferred |





| `match_rank` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `is_course` | boolean | YES | Whether activity is a course | boolean | schema.generated.md | schema.generated.md | inferred |





| `activity_path` | USER-DEFINED | YES | Activity geometry/path | geometry | schema.generated.md | schema.generated.md | inferred |





| `effort_label` | text | YES | Effort label | text | schema.generated.md | schema.generated.md | inferred |











### activities.vw_pr_hunter (view)





- **Grain:** one row per vw pr hunter (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_pr_hunter`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `vs_date` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `attempt_label` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `total_attempts` | bigint | YES | Total attempts | count | schema.generated.md | schema.generated.md | inferred |





| `predicted_time_s` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `vs_pace` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `estimated_pace` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prestige_score` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `weight_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `vo2_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `acute_load_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `load_max_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_report_segment_efforts (view)





- **Grain:** one row per vw report segment efforts (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_report_segment_efforts`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `is_course` | boolean | YES | Whether activity is a course | boolean | schema.generated.md | schema.generated.md | inferred |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `start_time_utc` | timestamp with time zone | YES | Activity start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `elapsed_duration_s` | numeric | YES | Elapsed duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `avg_cadence` | numeric | YES | Average cadence | spm | schema.generated.md | schema.generated.md | inferred |





| `avg_stride_length` | numeric | YES | Average stride length | metres | schema.generated.md | schema.generated.md | inferred |





| `avg_gct` | numeric | YES | Average ground contact time | ms | schema.generated.md | schema.generated.md | inferred |





| `avg_vert_ratio` | numeric | YES | Average vertical ratio | percent | schema.generated.md | schema.generated.md | inferred |





| `all_time_rank` | bigint | YES | All-time rank | rank | schema.generated.md | schema.generated.md | inferred |





| `total_attempts` | bigint | YES | Total attempts | count | schema.generated.md | schema.generated.md | inferred |





| `prior_time_s` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `best_time_excl_s` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prior_delta_s` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `best_delta_s` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prior10_avg_cadence` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prior10_avg_stride_length` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prior10_avg_gct` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `prior10_avg_vert_ratio` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_run_timing (view)





- **Grain:** one row per vw run timing (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_run_timing`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `run_age` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `music_start` | timestamp with time zone | YES | Music start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `music_end` | timestamp with time zone | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `music_duration_ms` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_segment_comps (view)





- **Grain:** one row per vw segment comps (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_comps`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `id_val` | text | YES | Identifier value | id | schema.generated.md | schema.generated.md | inferred |





| `meta_rank` | bigint | YES | Metadata rank | rank | schema.generated.md | schema.generated.md | inferred |





| `elevation_m_smooth` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `distance_m` | integer | YES | Distance | metres | schema.generated.md | schema.generated.md | inferred |





| `elapsed_ms` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `elapsed_ms_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `longitude` | numeric | YES | Longitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `latitude` | numeric | YES | Latitude | decimal degrees | schema.generated.md | schema.generated.md | inferred |





| `heartrate_bpm` | smallint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `heartrate_bpm_delta` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `cadence_spm` | smallint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `ground_contact_balance_left` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `performance_condition` | smallint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `vert_ratio` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `vert_oscillation_cm` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `air_temp_c` | smallint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `stride_length_cm` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `ground_contact_time_ms` | smallint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `speed_mmps` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_segment_create_auto_approve (view)





- **Grain:** one row per vw segment create auto approve (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_create_auto_approve`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `best_start_dist` | integer | YES | Best-match start distance | metres | schema.generated.md | schema.generated.md | inferred |





| `best_end_dist` | integer | YES | Best-match end distance | metres | schema.generated.md | schema.generated.md | inferred |





| `dist_deviation` | numeric | YES | Distance deviation between matched segments | metres | schema.generated.md | schema.generated.md | inferred |





| `polygon_deviation_m` | numeric | YES | Polygon deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `hausdorff_deviation_m` | numeric | YES | Hausdorff deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `freschet_deviation_m` | numeric | YES | Frechet deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `confidence` | numeric | YES | Match confidence | 0..1 | schema.generated.md | schema.generated.md | inferred |





| `approval_phase` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_segment_create_from_details (view)





- **Grain:** one row per vw segment create from details (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_create_from_details`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_reference_id` | bigint | YES | Reference activity id | id | schema.generated.md | schema.generated.md | inferred |





| `reference_start_point` | integer | YES | Reference segment start point index | index | schema.generated.md | schema.generated.md | inferred |





| `reference_end_point` | integer | YES | Reference segment end point index | index | schema.generated.md | schema.generated.md | inferred |





| `is_course` | boolean | YES | Whether activity is a course | boolean | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `segment_path` | USER-DEFINED | YES | Segment geometry path | geometry | schema.generated.md | schema.generated.md | inferred |





| `distance_m` | numeric | YES | Distance | metres | schema.generated.md | schema.generated.md | inferred |





| `elevation_gain` | numeric | YES | Elevation gain | metres | schema.generated.md | schema.generated.md | inferred |





| `elevation_loss` | numeric | YES | Elevation loss | metres | schema.generated.md | schema.generated.md | inferred |





| `start_point` | USER-DEFINED | YES | Segment start point index | index | schema.generated.md | schema.generated.md | inferred |





| `end_point` | USER-DEFINED | YES | Segment end point index | index | schema.generated.md | schema.generated.md | inferred |





| `activity_type_id` | smallint | YES | Activity type id | id | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segment_leaderboard (view)





- **Purpose:** per-segment effort leaderboard with all-time/last-365/current-cycle ranks, weights, VO2, and training context for run activities.





- **Grain:** one row per `(segment_id, activity_id)` effort.





- **Canonical source:** complex derived view with CTEs over `activities.vw_segments_details_ranked`, `activities.activities`, `health.*` raw tables; filters `is_course` courses.





- **Derived logic:** computes per-segment best/last-365/current-cycle effort weights, VO2, and acute training load via conditional `max()`; converts grams→lbs via `g2lb()`; `muscle_pct`/`fat_pct` from weight; `pace_str` from elapsed/distance.





- **Dependencies:** `activities.vw_segments_details_ranked`, `activities.activities`, health raw tables.





- **Consumers:** `vw_activity_summary`, `vw_report_segment_efforts`, segment leaderboard UI.





- **Rules:** read-only view; `is_course` filter; aggregates keyed on rank=1 rows.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | leaderboard source | schema_only.sql:10839 | confirmed |





| `is_course` | boolean | YES | Is course flag | boolean | leaderboard source | schema_only.sql:10846 | confirmed |





| `segment_name` | text | YES | Segment name | text | leaderboard source | schema_only.sql:10853 | confirmed |





| `cycle_name` | text | YES | Cycle label | text | leaderboard source | schema_only.sql:10860 | confirmed |





| `activity_id` | bigint | YES | Activity id | id | leaderboard source | schema_only.sql:10867 | confirmed |





| `activity_start_point` | integer | YES | Activity start point | ordinal | leaderboard source | schema_only.sql:10874 | confirmed |





| `activity_end_point` | integer | YES | Activity end point | ordinal | leaderboard source | schema_only.sql:10881 | confirmed |





| `distance_mi` | numeric | YES | Distance | miles | leaderboard source | schema_only.sql:10888 | confirmed |





| `start_time_utc` | timestamp with time zone | YES | Start time | UTC timestamptz | leaderboard source | schema_only.sql:10895 | confirmed |





| `all_time_rank` | bigint | YES | All-time rank | ordinal | leaderboard source | schema_only.sql (~10791) | confirmed |





| `last_365_rank` | bigint | YES | Last-365 rank | ordinal | leaderboard source | schema_only.sql (~10792) | confirmed |





| `current_cycle_rank` | bigint | YES | Current-cycle rank | ordinal | leaderboard source | schema_only.sql (~10793) | confirmed |





| `recency_rank` | bigint | YES | Recency rank | ordinal | leaderboard source | schema_only.sql (~10794) | confirmed |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | leaderboard source | schema_only.sql (~10795) | confirmed |





| `pace_str` | text | YES | Pace string | text | derived pace | schema_only.sql:10796 | inferred |





| `weight_lb` | numeric | YES | Weight | pounds | `g2lb(...)` | schema_only.sql:10797 | inferred |





| `muscle_pct` | numeric | YES | Muscle pct | ratio | `muscle_lb/weight_lb` | schema_only.sql:10799 | inferred |





| `fat_pct` | numeric | YES | Fat pct | ratio | `fat_lb/weight_lb` | schema_only.sql:10803 | inferred |





| `vo2_max_value` | numeric | YES | VO2 max | score points | leaderboard source | schema_only.sql:10806 | confirmed |





| `altitude_acclimation_m` | integer | YES | Altitude acclimation | metres | leaderboard source | schema_only.sql:10807 | confirmed |





| `heat_acclimation_pct` | numeric | YES | Heat acclimation pct | percent | leaderboard source | schema_only.sql:10808 | confirmed |





| `training_load_acute` | numeric | YES | Acute training load | score | leaderboard source | schema_only.sql:10809 | confirmed |





| `training_load_pct` | numeric | YES | Load pct | ratio | leaderboard source | schema_only.sql:10810 | confirmed |





| `training_readiness` | numeric | YES | Readiness score | ratio | leaderboard source | schema_only.sql:10811 | confirmed |





| `training_load_mid` | numeric | YES | Mid training load | score | leaderboard source | schema_only.sql:10812 | confirmed |





| `training_load_max` | numeric | YES | Max training load | score | leaderboard source | schema_only.sql:10813 | confirmed |





| `training_load_min` | numeric | YES | Min training load | score | leaderboard source | schema_only.sql:10814 | confirmed |





| `resting_hr_asleep` | double precision | YES | Resting heart rate while asleep | bpm | schema.generated.md | schema.generated.md | inferred |





| `resting_hr_awake` | double precision | YES | Resting heart rate while awake | bpm | schema.generated.md | schema.generated.md | inferred |





| `sleep_duration_s` | integer | YES | Sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `sleep_hours` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `sleep_score` | integer | YES | Sleep score | score | schema.generated.md | schema.generated.md | inferred |





| `light_sleep_s` | integer | YES | Light sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `deep_sleep_s` | integer | YES | Deep sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `awake_sleep_s` | integer | YES | Awake time during sleep window | seconds | schema.generated.md | schema.generated.md | inferred |





| `rem_sleep_s` | integer | YES | REM sleep duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `awake_hours` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `max_hr` | numeric | YES | Maximum heart rate | bpm | schema.generated.md | schema.generated.md | inferred |





| `avg_hr` | numeric | YES | Average heart rate | bpm | schema.generated.md | schema.generated.md | inferred |





| `avg_cadence` | numeric | YES | Average cadence | spm | schema.generated.md | schema.generated.md | inferred |





| `avg_temp` | numeric | YES | Average temperature | celsius | schema.generated.md | schema.generated.md | inferred |





| `avg_vert_osc` | numeric | YES | Average vertical oscillation | cm | schema.generated.md | schema.generated.md | inferred |





| `avg_vert_ratio` | numeric | YES | Average vertical ratio | percent | schema.generated.md | schema.generated.md | inferred |





| `avg_gct` | numeric | YES | Average ground contact time | ms | schema.generated.md | schema.generated.md | inferred |





| `avg_perf` | numeric | YES | Average performance condition | score | schema.generated.md | schema.generated.md | inferred |





| `avg_balance` | numeric | YES | Average left/right balance | percent | schema.generated.md | schema.generated.md | inferred |





| `avg_stride_length` | numeric | YES | Average stride length | metres | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segment_matching_downselect_confidence (view)





- **Grain:** one row per vw segment matching downselect confidence (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_matching_downselect_confidence`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `confirmed` | boolean | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `best_start_dist` | integer | YES | Best-match start distance | metres | schema.generated.md | schema.generated.md | inferred |





| `best_end_dist` | integer | YES | Best-match end distance | metres | schema.generated.md | schema.generated.md | inferred |





| `dist_deviation` | numeric | YES | Distance deviation between matched segments | metres | schema.generated.md | schema.generated.md | inferred |





| `polygon_deviation_m` | numeric | YES | Polygon deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `freschet_deviation_m` | numeric | YES | Frechet deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `hausdorff_deviation_m` | numeric | YES | Hausdorff deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `confidence_score` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_segment_matching_downselect_scores (view)





- **Grain:** one row per vw segment matching downselect scores (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_matching_downselect_scores`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `best_start_dist` | integer | YES | Best-match start distance | metres | schema.generated.md | schema.generated.md | inferred |





| `best_end_dist` | integer | YES | Best-match end distance | metres | schema.generated.md | schema.generated.md | inferred |





| `dist_deviation` | numeric | YES | Distance deviation between matched segments | metres | schema.generated.md | schema.generated.md | inferred |





| `ruggedness` | double precision | YES | Terrain ruggedness | index | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segment_path_updates (view)





- **Grain:** one row per vw segment path updates (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_path_updates`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_reference_id` | bigint | YES | Reference activity id | id | schema.generated.md | schema.generated.md | inferred |





| `activity_type_id` | smallint | YES | Activity type id | id | schema.generated.md | schema.generated.md | inferred |





| `reference_start_point` | integer | YES | Reference segment start point index | index | schema.generated.md | schema.generated.md | inferred |





| `reference_end_point` | integer | YES | Reference segment end point index | index | schema.generated.md | schema.generated.md | inferred |





| `segment_path` | USER-DEFINED | YES | Segment geometry path | geometry | schema.generated.md | schema.generated.md | inferred |





| `distance_m` | numeric | YES | Distance | metres | schema.generated.md | schema.generated.md | inferred |





| `ascent_m` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `descent_m` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `start_point` | USER-DEFINED | YES | Segment start point index | index | schema.generated.md | schema.generated.md | inferred |





| `end_point` | USER-DEFINED | YES | Segment end point index | index | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segment_racing (view)





- **Grain:** one row per vw segment racing (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segment_racing`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `distance_mi` | numeric | YES | Distance | miles | schema.generated.md | schema.generated.md | inferred |





| `effort_count` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `last_effort` | timestamp with time zone | YES | Last effort time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `is_course` | boolean | YES | Whether activity is a course | boolean | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segments_details_ranked (view)





- **Purpose:** per-segment-attempt details with cycle assignment (current cycle / last 365 / all time) and recency/all-time/current-cycle ranks.





- **Grain:** one row per segment attempt (`activities.segments_details` row).





- **Canonical source:** derived view over `activities.segments_details` and `activities.training_cycle_dates`.





- **Derived logic:** `cycle_name` CASE on start_time vs most-recent cycle start / 365-day window; `recency_rank`/`all_time_rank`/`last_365_rank`/`current_cycle_rank` via `row_number()` per segment; `distance_mi = m2mi(distance_m)`.





- **Dependencies:** `activities.segments_details`, `activities.training_cycle_dates`; uses `CURRENT_TIMESTAMP` for the 365-day window (query-time relative).





- **Consumers:** `vw_segment_leaderboard`, `vw_activity_summary`.





- **Rules:** read-only view; ranks reset per segment partition.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | `segments_details.segment_id` | schema.generated.md:10477-10514 | confirmed |





| `activity_id` | bigint | YES | Activity id | id | `segments_details.activity_id` | schema.generated.md:10483 | confirmed |





| `cycle_name` | text | YES | Cycle label | 'Current Cycle' | 'Last 365' | 'All Time' | CASE expression | schema_only.sql:10484-10488 | inferred |





| `activity_start_point` | integer | YES | Activity start point | ordinal | `segments_details.activity_start_point` | schema.generated.md:10490 | confirmed |





| `activity_end_point` | integer | YES | Activity end point | ordinal | `segments_details.activity_end_point` | schema.generated.md:10491 | confirmed |





| `recency_rank` | bigint | YES | Recency rank | ordinal | `row_number()` per segment by start desc | schema.generated.md:10492 | confirmed |





| `all_time_rank` | bigint | YES | All-time duration rank | ordinal | `row_number()` per segment by elapsed asc | schema.generated.md:10493 | confirmed |





| `last_365_rank` | bigint | YES | Last-365-days rank | ordinal | `row_number()` with 365 filter | schema.generated.md:10494 | confirmed |





| `current_cycle_rank` | bigint | YES | Current-cycle rank | ordinal | `row_number()` with cycle filter | schema.generated.md:10495 | confirmed |





| `start_time_utc` | timestamp with time zone | YES | Attempt start | UTC timestamptz | `segments_details.start_time_utc` | schema.generated.md:10497 | confirmed |





| `distance_mi` | numeric | YES | Segment distance | miles | `m2mi(distance_m)` | schema_only.sql:10498 | inferred |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | `segments_details.elapsed_duration_s` | schema.generated.md:10499 | confirmed |





| `max_hr` | integer | YES | Max HR | bpm | `segments_details.max_hr` | schema.generated.md:10500 | confirmed |





| `avg_hr` | integer | YES | Avg HR | bpm | `segments_details.avg_hr` | schema.generated.md:10501 | confirmed |





| `avg_cadence` | integer | YES | Avg cadence | steps/min | `segments_details.avg_cadence` | schema.generated.md:10502 | confirmed |





| `avg_temp` | smallint | YES | Avg temp | °C | `segments_details.avg_temp` | schema.generated.md:10503 | confirmed |





| `avg_vert_osc` | numeric | YES | Avg vertical oscillation | cm | `segments_details.avg_vert_osc` | schema.generated.md:10504 | confirmed |





| `avg_vert_ratio` | numeric | YES | Avg vertical ratio | ratio | CASE nulls to NULL | schema_only.sql:10505-10508 | inferred |





| `avg_gct` | integer | YES | Avg ground contact time | ms | `segments_details.avg_gct` | schema.generated.md:10509 | confirmed |





| `avg_perf` | smallint | YES | Avg performance condition | score | `segments_details.avg_perf` | schema.generated.md:10510 | confirmed |





| `avg_balance` | numeric | YES | Avg ground contact balance | ratio | `segments_details.avg_balance` | schema.generated.md:10511 | confirmed |





| `avg_stride_length` | numeric | YES | Avg stride length | cm | `segments_details.avg_stride_length` | schema.generated.md:10512 | confirmed |





| `segment_path` | USER-DEFINED | YES | Segment path | USER-DEFINED | `segments_details.segment_path` | schema.generated.md:10513 | confirmed |











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `cycle_name` | text | YES | Training cycle name | text | schema.generated.md | schema.generated.md | inferred |





| `activity_start_point` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `activity_end_point` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `recency_rank` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `all_time_rank` | bigint | YES | All-time rank | rank | schema.generated.md | schema.generated.md | inferred |





| `last_365_rank` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `current_cycle_rank` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `start_time_utc` | timestamp with time zone | YES | Activity start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `distance_mi` | numeric | YES | Distance | miles | schema.generated.md | schema.generated.md | inferred |





| `elapsed_duration_s` | numeric | YES | Elapsed duration | seconds | schema.generated.md | schema.generated.md | inferred |





| `max_hr` | numeric | YES | Maximum heart rate | bpm | schema.generated.md | schema.generated.md | inferred |





| `avg_hr` | numeric | YES | Average heart rate | bpm | schema.generated.md | schema.generated.md | inferred |





| `avg_cadence` | numeric | YES | Average cadence | spm | schema.generated.md | schema.generated.md | inferred |





| `avg_temp` | numeric | YES | Average temperature | celsius | schema.generated.md | schema.generated.md | inferred |





| `avg_vert_osc` | numeric | YES | Average vertical oscillation | cm | schema.generated.md | schema.generated.md | inferred |





| `avg_vert_ratio` | numeric | YES | Average vertical ratio | percent | schema.generated.md | schema.generated.md | inferred |





| `avg_gct` | numeric | YES | Average ground contact time | ms | schema.generated.md | schema.generated.md | inferred |





| `avg_perf` | numeric | YES | Average performance condition | score | schema.generated.md | schema.generated.md | inferred |





| `avg_balance` | numeric | YES | Average left/right balance | percent | schema.generated.md | schema.generated.md | inferred |





| `avg_stride_length` | numeric | YES | Average stride length | metres | schema.generated.md | schema.generated.md | inferred |





| `segment_path` | USER-DEFINED | YES | Segment geometry path | geometry | schema.generated.md | schema.generated.md | inferred |











### activities.vw_segments_effort_stats (view)





- **Grain:** one row per vw segments effort stats (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_segments_effort_stats`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_name` | text | YES | Segment display name | text | schema.generated.md | schema.generated.md | inferred |





| `is_course` | boolean | YES | Whether activity is a course | boolean | schema.generated.md | schema.generated.md | inferred |





| `activity_type_name` | text | YES | Activity type name | text | schema.generated.md | schema.generated.md | inferred |





| `distance_mi` | numeric | YES | Distance | miles | schema.generated.md | schema.generated.md | inferred |





| `elevation_gain` | numeric | YES | Elevation gain | metres | schema.generated.md | schema.generated.md | inferred |





| `last_effort` | date | YES | Last effort time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `matched_activity_count` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











### activities.vw_temp_segment_matches_downselect (view)





- **Grain:** one row per vw temp segment matches downselect (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_temp_segment_matches_downselect`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |





| `segment_id` | integer | YES | Segment identifier | id | schema.generated.md | schema.generated.md | inferred |





| `effort_date` | date | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `best_start_dist` | integer | YES | Best-match start distance | metres | schema.generated.md | schema.generated.md | inferred |





| `best_end_dist` | integer | YES | Best-match end distance | metres | schema.generated.md | schema.generated.md | inferred |





| `ruggedness` | numeric | YES | Terrain ruggedness | index | schema.generated.md | schema.generated.md | inferred |





| `dist_deviation` | numeric | YES | Distance deviation between matched segments | metres | schema.generated.md | schema.generated.md | inferred |





| `polygon_deviation_m` | numeric | YES | Polygon deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `hausdorff_deviation_m` | numeric | YES | Hausdorff deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `freschet_deviation_m` | numeric | YES | Frechet deviation | metres | schema.generated.md | schema.generated.md | inferred |





| `confidence` | numeric | YES | Match confidence | 0..1 | schema.generated.md | schema.generated.md | inferred |











### activities.vw_unmatched_activitites (view)





- **Grain:** one row per vw unmatched activitites (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_unmatched_activitites`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Related activity identifier | id | schema.generated.md | schema.generated.md | inferred |











### activities.vw_watch_music_heard (view)





- **Grain:** one row per vw watch music heard (view); see schema.generated.md for columns.





- **Canonical source:** `activities.vw_watch_music_heard`.





- **Rules:** `convention` — DB constraints per schema.generated.md (individual cataloguing not completed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `playlist_name` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `track_id` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `isrc` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `track_order` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `track_name_clean` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `artist_display_name` | text | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `duration_ms` | integer | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `music_start` | timestamp with time zone | YES | Music start time | UTC timestamptz | schema.generated.md | schema.generated.md | inferred |





| `start_cumulative_ms` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `end_cumulative_ms` | bigint | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `pct_listened` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `total_listen_ms` | numeric | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |





| `played_at_utc` | timestamp with time zone | YES | Not established from available evidence (view column) | unknown | schema.generated.md | schema.generated.md | unknown |











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





| `kcal_per_100g` | double precision | YES | Energy density snapshot | kcal/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `protein_g` | double precision | YES | Protein density snapshot | g/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `fat_g` | double precision | YES | Fat density snapshot | g/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `carbs_g` | double precision | YES | Carbohydrate density snapshot | g/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `fiber_g` | double precision | YES | Fiber density snapshot | g/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `sugars_g` | double precision | YES | Sugar density snapshot | g/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `sodium_mg` | double precision | YES | Sodium density snapshot | mg/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





| `caffeine_mg` | double precision | YES | Caffeine density snapshot | mg/100g | snapshotted | _NUTRIENT_KEYS | confirmed |





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





| `target_internal_temp_f` | numeric | NO | Target internal temp | —F | request | schema.generated.md | confirmed |





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





| `grill_temp_f` | numeric | NO | Grill temp | —F | request | schema.generated.md | confirmed |





| `internal_temp_f` | numeric | NO | Internal temp | —F | request | schema.generated.md | confirmed |





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











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `body_battery_value` | integer | YES | Body battery level | index 0-100 | Garmin import | schema.generated.md | confirmed |











### health.body_composition_raw (table)





- **Grain:** one row per body-composition measurement.





- **Canonical source:** `health.body_composition_raw` (Garmin import).





- **Rules:** `convention` raw measurement.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Measurement timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `weight_total_g` | integer | YES | Total body weight | grams | Garmin import | schema.generated.md | confirmed |





| `weight_bone_g` | integer | YES | Bone mass | grams | Garmin import | schema.generated.md | confirmed |





| `weight_muscle_g` | integer | YES | Muscle mass | grams | Garmin import | schema.generated.md | confirmed |





| `fat_pct` | numeric | YES | Body fat percentage | percent | Garmin import | schema.generated.md | confirmed |





| `water_pct` | numeric | YES | Body water percentage | percent | Garmin import | schema.generated.md | confirmed |





| `body_mass_index` | numeric | YES | BMI | kg/m^2 | Garmin import | schema.generated.md | confirmed |

















### health.body_dimensions (table)





- **Grain:** one row per body dimension record.





- **Canonical source:** `health.body_dimensions`.





- **Rules:** `convention` dimension data.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `captured_at_utc` | timestamp with time zone | NO | Measurement timestamp | UTC timestamptz | manual entry | schema.generated.md | confirmed |





| `butt_cm` | numeric | YES | Buttocks circumference | cm | manual entry | schema.generated.md | confirmed |





| `waist_cm` | numeric | YES | Waist circumference | cm | manual entry | schema.generated.md | confirmed |





| `stomach_cm` | numeric | YES | Stomach circumference | cm | manual entry | schema.generated.md | confirmed |





| `chest_cm` | numeric | YES | Chest circumference | cm | manual entry | schema.generated.md | confirmed |





| `neck_cm` | numeric | YES | Neck circumference | cm | manual entry | schema.generated.md | confirmed |











### health.fitness_age_raw (table)





- **Grain:** one row per fitness-age sample.





- **Canonical source:** `health.fitness_age_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `human_age` | numeric | YES | Chronological age | years | Garmin import | schema.generated.md | confirmed |





| `fitness_age_measured` | numeric | YES | Measured fitness age | years | Garmin import | schema.generated.md | confirmed |





| `fitness_age_potential` | numeric | YES | Potential fitness age | years | Garmin import | schema.generated.md | confirmed |











### health.heartrate_raw (table)





- **Purpose:** raw heart-rate samples from Garmin activity details, flattened into a per-timestamp series.





- **Grain:** one row per heart-rate sample, keyed by `ts_utc`.





- **Canonical source:** `health.heartrate_raw`; populated by `activity_smoothing.insert_heartrate_for_activity` (`backend/backend_functions/activity_smoothing.py:140`) from `activities.activity_details` rows where `heartrate_bpm IS NOT NULL`; `activity_label` and `hr_date` are backfilled by SQL in `docs/data/_evidence/database/definitions.sql` (activity-window overlap and sleep-window rules).





- **Consumers:** `backend/pifitness/health/queries.py:150` (`SELECT ts_utc, heartrate_bpm, activity_label, hr_date`); `backend/pifitness/health/router.py`; `backend/pifitness/health/schemas.py` (`HeartRate` pydantic model).





- **Relationships:** no declared FK; `ts_utc` aligns with `activities.activity_details.ts_utc`.





- **Rules:** `enforced` PK on `ts_utc` (`heartrate_raw_pkey`); upsert — `ON CONFLICT(ts_utc) DO UPDATE SET heartrate_bpm = EXCLUDED.heartrate_bpm WHERE health.heartrate_raw.heartrate_bpm IS DISTINCT FROM EXCLUDED.heartrate_bpm`; `activity_label` backfilled to 'active' for non-run/non-walk activity windows and 'asleep' within `health.sleep_totals` windows (guard conditions also reference 'running'/'walking'); timestamps UTC.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | `activities.activity_details.ts_utc`; PK; upsert on conflict | schema.generated.md:1248-1260; `activity_smoothing.py:141` | confirmed |





| `heartrate_bpm` | smallint | YES | Heart rate | bpm | `activities.activity_details.heartrate_bpm`, filtered `IS NOT NULL`; updated only when value changed | schema.generated.md:1252; `activity_smoothing.py:141-145` | confirmed |





| `activity_label` | text | YES | Activity context label | free text; evidenced closed set 'running' | 'walking' | 'active' | 'asleep' | backfilled per activity type: 'running' for run-type windows, 'walking' for walk-type windows, 'active' for other activity windows, 'asleep' within `health.sleep_totals` windows | schema.generated.md:1253; `definitions.sql:24693,24698,24703,26987` | confirmed |





| `hr_date` | date | YES | Sample date | date | stored date column; populate path not evidenced in application code searched | schema.generated.md:1254; `health/queries.py:150` returns `hr_date` | unknown |











### health.hrv_raw (table)





- **Grain:** one row per HRV sample.





- **Canonical source:** `health.hrv_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `hrv_value` | smallint | YES | HRV (ms) | ms | Garmin import | schema.generated.md | confirmed |











### health.photo_metadata (table)





- **Grain:** one row per photo metadata.





- **Canonical source:** `health.photo_metadata`.





- **Rules:** `convention` metadata.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `captured_at_utc` | timestamp with time zone | NO | Photo capture timestamp | UTC timestamptz | manual entry | schema.generated.md | confirmed |





| `photo_type` | text | YES | Photo category/type | free text | manual entry | schema.generated.md | confirmed |





| `file_name` | text | YES | Stored file name | free text | manual entry | schema.generated.md | confirmed |

















- **Rules:** `convention` raw sample.











### health.respiration_raw (table)





- **Grain:** one row per respiration sample.





- **Canonical source:** `health.respiration_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `breaths_per_minute` | smallint | YES | Respiration rate | breaths/min | Garmin import | schema.generated.md | confirmed |











### health.sleep_movement_raw (table)





- **Grain:** one row per sleep movement sample.





- **Canonical source:** `health.sleep_movement_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `sleep_movement` | numeric | YES | Sleep movement metric | Garmin units | Garmin import | schema.generated.md | confirmed |

















### health.sleep_totals (table)





- **Grain:** one row per night sleep summary.





- **Canonical source:** `health.sleep_totals`.





- **Rules:** `convention` nightly summary.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `sleep_end_date` | date | YES | Sleep end date | date | Garmin import | schema.generated.md | confirmed |





| `sleep_start_utc` | timestamp with time zone | NO | Sleep start | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `sleep_end_utc` | timestamp with time zone | YES | Sleep end | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `sleep_score` | integer | YES | Sleep score | index 0-100 | Garmin import | schema.generated.md | confirmed |





| `heartrate_bpm` | numeric | YES | Average heart rate | bpm | Garmin import | schema.generated.md | confirmed |





| `spo2` | numeric | YES | Average SpO2 | percent | Garmin import | schema.generated.md | confirmed |





| `breaths_per_min` | numeric | YES | Average respiration | breaths/min | Garmin import | schema.generated.md | confirmed |





| `hrv_value` | smallint | YES | HRV | ms | Garmin import | schema.generated.md | confirmed |





| `sleep_duration_s` | integer | YES | Total sleep duration | seconds | Garmin import | schema.generated.md | confirmed |





| `rem_sleep_s` | integer | YES | REM sleep duration | seconds | Garmin import | schema.generated.md | confirmed |





| `light_sleep_s` | integer | YES | Light sleep duration | seconds | Garmin import | schema.generated.md | confirmed |





| `awake_sleep_s` | integer | YES | Awake duration | seconds | Garmin import | schema.generated.md | confirmed |





| `deep_sleep_s` | integer | YES | Deep sleep duration | seconds | Garmin import | schema.generated.md | confirmed |





| `score_label` | text | YES | Score label | free text | Garmin import | schema.generated.md | confirmed |

















### health.spo2_raw (table)





- **Grain:** one row per SpO2 sample.





- **Canonical source:** `health.spo2_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `spo2_reading` | smallint | YES | SpO2 reading | percent | Garmin import | schema.generated.md | confirmed |





| `spo2_confidence` | smallint | YES | Reading confidence | percent | Garmin import | schema.generated.md | confirmed |

















### health.stress_raw (table)





- **Grain:** one row per stress sample.





- **Canonical source:** `health.stress_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `stress_value` | smallint | YES | Stress level | index 0-100 | Garmin import | schema.generated.md | confirmed |

















### health.training_status_raw (table)





- **Grain:** one row per training status sample.





- **Canonical source:** `health.training_status_raw` (Garmin import).





- **Rules:** `convention` raw sample.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | Garmin import | schema.generated.md | confirmed |





| `vo2_max_value` | numeric | YES | VO2 max | ml/kg/min | Garmin import | schema.generated.md | confirmed |





| `altitude_current_m` | integer | YES | Current altitude | metres | Garmin import | schema.generated.md | confirmed |





| `altitude_acclimation_m` | integer | YES | Acclimation altitude | metres | Garmin import | schema.generated.md | confirmed |





| `altitude_acclimation_pct` | numeric | YES | Acclimation percentage | percent | Garmin import | schema.generated.md | confirmed |





| `heat_acclimation_pct` | numeric | YES | Heat acclimation percentage | percent | Garmin import | schema.generated.md | confirmed |





| `training_load_acute` | numeric | YES | Acute training load | Garmin load units | Garmin import | schema.generated.md | confirmed |





| `training_load_min` | numeric | YES | Min training load | Garmin load units | Garmin import | schema.generated.md | confirmed |





| `training_load_max` | numeric | YES | Max training load | Garmin load units | Garmin import | schema.generated.md | confirmed |

















### health.vw_activity_weights (view)





- **Grain:** one row per activity weight.





- **Canonical source:** derived from health data.





- **Rules:** `convention` read-only view.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | identifier | derived | schema.generated.md | inferred |





| `weight_total_g` | numeric | YES | Total weight | grams | derived | schema.generated.md | inferred |





| `weight_bone_g` | numeric | YES | Bone mass | grams | derived | schema.generated.md | inferred |





| `weight_muscle_g` | numeric | YES | Muscle mass | grams | derived | schema.generated.md | inferred |





| `weight_fat_g` | numeric | YES | Fat mass | grams | derived | schema.generated.md | inferred |





| `fat_pct` | numeric | YES | Fat percentage | percent | derived | schema.generated.md | inferred |





| `water_pct` | numeric | YES | Water percentage | percent | derived | schema.generated.md | inferred |

















### health.vw_body_comp_interpolation_precalc (view)





- **Grain:** one row per precomputed body-comp interpolation.





- **Canonical source:** derived view.





- **Rules:** `convention` read-only view.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | YES | Start timestamp | UTC timestamptz | derived | schema.generated.md | inferred |





| `next_ts_utc` | timestamp with time zone | YES | Next timestamp | UTC timestamptz | derived | schema.generated.md | inferred |





| `weight_total_g` | integer | YES | Weight at start | grams | derived | schema.generated.md | inferred |





| `delta_s` | integer | YES | Seconds between samples | seconds | derived | schema.generated.md | inferred |





| `weight_delta_per_s` | numeric | YES | Weight delta per second | g/s | derived | schema.generated.md | inferred |





| `weight_bone_g` | integer | YES | Bone mass at start | grams | derived | schema.generated.md | inferred |





| `bone_delta_per_s` | numeric | YES | Bone delta per second | g/s | derived | schema.generated.md | inferred |





| `weight_muscle_g` | integer | YES | Muscle mass at start | grams | derived | schema.generated.md | inferred |





| `muscle_delta_per_s` | numeric | YES | Muscle delta per second | g/s | derived | schema.generated.md | inferred |





| `fat_pct` | numeric | YES | Fat pct at start | percent | derived | schema.generated.md | inferred |





| `fat_delta_per_s` | numeric | YES | Fat delta per second | %/s | derived | schema.generated.md | inferred |





| `water_pct` | numeric | YES | Water pct at start | percent | derived | schema.generated.md | inferred |





| `water_delta_per_s` | numeric | YES | Water delta per second | %/s | derived | schema.generated.md | inferred |

















### health.vw_heart_rates (view)





- **Grain:** one row per heart-rate reading.





- **Canonical source:** derived from `health.heartrate_raw`.





- **Rules:** `convention` read-only view.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `hr_date` | date | YES | Heart-rate date | date | derived | schema.generated.md | inferred |





| `resting_hr_asleep` | double precision | YES | Resting HR asleep | bpm | derived | schema.generated.md | inferred |





| `max_hr` | double precision | YES | Max HR | bpm | derived | schema.generated.md | inferred |





| `resting_hr_awake` | double precision | YES | Resting HR awake | bpm | derived | schema.generated.md | inferred |

















### health.weight_target (table)





- **Grain:** one row per weight target setting.





- **Canonical source:** `health.weight_target`.





- **Rules:** `convention` user setting.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Target timestamp | UTC timestamptz | manual entry | schema.generated.md | confirmed |





| `weight_total_g` | integer | YES | Target weight | grams | manual entry | schema.generated.md | confirmed |

















## logging











### logging.api_logins (table)





- **Purpose:** audit log of API login/token events for each connected external service (e.g. Garmin, Spotify).





- **Grain:** one row per API login event, keyed by `event_time_utc`.





- **Canonical source:** `logging.api_logins`; written by `log_api_event` in `backend/backend_functions/logging_functions.py:48`.





- **Consumers:** `backend/pifitness/auth/status.py:54,73,78` (last login and last error per service).





- **Relationships:** `enforced` FK `api_service_name` -> `api_services.api_service_list.api_service_name` (`api_logins_api_service_name_fkey`).





- **Rules:** `enforced` PK `event_time_utc` (`api_logins_pkey`); `event_time_utc` DEFAULT `now()` (insert path omits it). `token_age_s` in seconds.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `event_time_utc` | timestamp with time zone | NO | Event timestamp | UTC timestamptz, DEFAULT now() | `log_api_event` (omitted from insert → default) | `logging_functions.py:48-52`; schema_only.sql:16295 | confirmed |





| `api_service_name` | text | YES | External service name | service identifier (e.g. 'Garmin', 'Spotify'); FK | `log_api_event` param `service` | `logging_functions.py:49` | confirmed |





| `event_name` | text | YES | Event label | free text (e.g. 'New Token') | `log_api_event` param `event` | `logging_functions.py:49` | confirmed |





| `token_age_s` | smallint | YES | Token age | seconds | `log_api_event` param `token_age` | `logging_functions.py:49` | confirmed |





| `error_text` | text | YES | Error message | free text | `log_api_event` param `err` | `logging_functions.py:50` | confirmed |











### logging.application_events (table)





- **Purpose:** application event log capturing task executioner, DB maintenance, backup, and other operational events.





- **Grain:** one row per logged application event, keyed by `event_time_utc`.





- **Canonical source:** `logging.application_events`; written by `log_app_event` in `backend/backend_functions/logging_functions.py:6` (dynamic INSERT of whichever optional fields are supplied).





- **Consumers:** `backend/pifitness/admin/queries.py:584` (task execution log records filtered by `event_category`); `tools/probe/probe4.py` round-trip test inserts a probe row and reads it back.





- **Relationships:** none declared.





- **Rules:** `enforced` PK `event_time_utc` (`application_events_pkey`); DEFAULT `now()`; no FKs. `execution_time_ms` is milliseconds per code (`int(exec_time)`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `event_time_utc` | timestamp with time zone | NO | Event timestamp | UTC timestamptz, DEFAULT now() | `log_app_event` (omitted from insert → default) | `logging_functions.py:6-44`; schema_only.sql:16350 | confirmed |





| `event_category` | text | YES | Event category | free text (e.g. 'Task Executioner', 'DB Maintenance', 'DB Backup') | `log_app_event` param `cat` | `logging_functions.py:13` | confirmed |





| `event_description` | text | YES | Event description | free text | `log_app_event` param `desc` | `logging_functions.py:13` | confirmed |





| `execution_time_ms` | integer | YES | Duration | milliseconds | `log_app_event` param `exec_time` cast to int | `logging_functions.py:26-28` | confirmed |





| `error_text` | text | YES | Error message | free text | `log_app_event` param `err` | `logging_functions.py:18-23` | confirmed |





| `task_id` | integer | YES | Related task id | id | `log_app_event` param `task_id` | `logging_functions.py:30-33` | confirmed |





| `data_event` | text | YES | Event payload tag | free text ('probe' in tests) | `log_app_event` param `data_event` | `logging_functions.py:35-40` | confirmed |











### logging.db_size_log (table)





- **Purpose:** per-table database size snapshots recorded during nightly maintenance.





- **Grain:** one row per table per snapshot; no declared PK (`event_time_utc` nullable).





- **Canonical source:** `logging.db_size_log`; written by `backend/pifitness/worker/backend_tasks.py:96` — `INSERT INTO logging.db_size_log ... SELECT ... FROM logging.vw_db_size`.





- **Consumers:** `logging.vw_db_biggest_table_chart`, `logging.vw_db_size`, `logging.vw_db_size_chart`.





- **Relationships:** none declared.





- **Rules:** `convention` monitoring snapshot; sizes stored in MB as `numeric(5,1)`; `event_time_utc` DEFAULT `now()`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `event_time_utc` | timestamp with time zone | YES | Snapshot timestamp | UTC timestamptz, DEFAULT now() | `backend_tasks.py:96` (omitted from insert → default) | `backend_tasks.py:96`; schema_only.sql:16421 | confirmed |





| `table_name` | text | YES | Table name | text | `logging.vw_db_size` | schema_only.sql:16422 | confirmed |





| `total_size_mb` | numeric(5,1) | YES | Total relation size | MB | `pg_total_relation_size / 1024 / 1024` via `vw_db_size` | schema_only.sql:16423; `schema_only.sql:17017` | confirmed |





| `table_size_mb` | numeric(5,1) | YES | Table (main fork) size | MB | `pg_relation_size / 1024 / 1024` via `vw_db_size` | schema_only.sql:16424; `schema_only.sql:17018` | confirmed |





| `index_size_mb` | numeric(5,1) | YES | Index size | MB | `pg_indexes_size / 1024 / 1024` via `vw_db_size` | schema_only.sql:16425; `schema_only.sql:17019` | confirmed |











### logging.db_stats (table)





- **Purpose:** nightly database maintenance statistics (migration `011__comment_backfill_logging.sql`: 'Various statistics recorded from database nightly maintenance.').





- **Grain:** one row per maintenance run; no declared PK.





- **Canonical source:** `logging.db_stats`; written by `backend/pifitness/worker/backend_tasks.py:109` — `INSERT into logging.db_stats (size_before_mb, size_after_mb, maintenance_time_ms, total_time_ms, maintenance_type)`.





- **Relationships:** none declared.





- **Rules:** `convention` maintenance log; sizes in MB; duration columns named `_ms` and passed as `int(...)` milliseconds in code, so treat as milliseconds (the migration comment 'Duration in seconds/milliseconds' is ambiguous and not authoritative for units). `event_time_utc` DEFAULT `now()`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `event_time_utc` | timestamp with time zone | YES | Run timestamp | UTC timestamptz, DEFAULT now() | `backend_tasks.py:109` (omitted from insert → default) | `backend_tasks.py:109`; schema_only.sql:16476 | confirmed |





| `size_before_mb` | numeric(5,1) | YES | DB size before maintenance | MB | `backend_tasks.py:109` param | schema_only.sql:16477 | confirmed |





| `size_after_mb` | numeric(5,1) | YES | DB size after maintenance | MB | `backend_tasks.py:109` param | schema_only.sql:16478 | confirmed |





| `maintenance_time_ms` | integer | YES | Maintenance duration | milliseconds (column name + int cast) | `backend_tasks.py:109` param | schema_only.sql:16479 | confirmed |





| `total_time_ms` | integer | YES | Total run duration | milliseconds (column name + int cast) | `backend_tasks.py:109` param | schema_only.sql:16480 | confirmed |





| `maintenance_type` | text | YES | Maintenance type | free text | `backend_tasks.py:109` param | schema_only.sql:16481 | confirmed |











### logging.health_checks (table)





- **Purpose:** results of database/backup/hardware health checks, one row per check per run.





- **Grain:** one row per check result, keyed by `check_id`.





- **Canonical source:** `logging.health_checks`; written by `run_health_checks` in `backend/pifitness/worker/backend_tasks.py:206` (insert at line 343); results are also written to `/home/god/Documents/PiFitness_Local/health_status.json` for the frontend.





- **Relationships:** none declared.





- **Rules:** `enforced` PK `check_id` (`health_checks_pkey`, sequence `logging.health_checks_check_id_seq`); `enforced` CHECK `status IN ('ok','warn','critical')` (`health_checks_status_check`); `check_name` and `status` NOT NULL; `check_time_utc` DEFAULT `now()` NOT NULL.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `check_id` | bigint | NO | Check row id | sequence-backed identity | `run_health_checks` insert omits it (sequence default) | `backend_tasks.py:343`; schema_only.sql:16538-16545 | confirmed |





| `check_time_utc` | timestamp with time zone | NO | Check timestamp | UTC timestamptz, DEFAULT now() | `run_health_checks` insert omits it → default | schema_only.sql:16540 | confirmed |





| `check_name` | text | NO | Check name | free text | `run_health_checks` param `c['name']` | `backend_tasks.py:344` | confirmed |





| `status` | text | NO | Check status | 'ok' | 'warn' | 'critical' (CHECK constraint enforced) | `run_health_checks` param `c['status']` | confirmed |





| `value` | numeric | YES | Check metric value | unit depends on the check | `run_health_checks` param `c['value']` | `backend_tasks.py:345` | confirmed |





| `message` | text | YES | Check detail message | free text | `run_health_checks` param `c['message']` | `backend_tasks.py:345` | confirmed |











### logging.music_db_info (table)





- **Purpose:** snapshot of music database cardinality (migration `011__comment_backfill_logging.sql`: 'Tracking the state of the musical tracks known by the application').





- **Grain:** one row per snapshot, keyed by `ts_utc`.





- **Canonical source:** `logging.music_db_info`; written by procedure `music.isrc_duplicate_finder()` (`docs/data/_evidence/database/schema_only.sql:2866-2879`), which aggregates `music.all_tracks` left-joined to `music.isrc_swaps` and stores the counts; `ts_utc` DEFAULT `CURRENT_TIMESTAMP` (the insert omits it, so the default applies).





- **Relationships:** none declared.





- **Rules:** `enforced` PK `ts_utc` (`music_db_info_pkey`). All count columns are plain counts with no declared allowed-value set (open set).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Snapshot timestamp | UTC timestamptz, DEFAULT CURRENT_TIMESTAMP | PK; `isrc_duplicate_finder()` insert omits it → default applies | schema_only.sql:16622,2866-2879 | confirmed |





| `track_id_count` | integer | YES | Distinct track count | count | `COUNT(DISTINCT track_id)` FROM `music.all_tracks` | schema_only.sql:2873 | confirmed |





| `isrc_id_count` | integer | YES | Distinct ISRC count | count | `COUNT(DISTINCT track_isrc)` | schema_only.sql:2874 | confirmed |





| `mapped_isrc_id_count` | integer | YES | Distinct mapped/preferred ISRC count | count | `COUNT(DISTINCT COALESCE(mapped_isrc, track_isrc))` (aliased `preferred_isrc_count` in source) | schema_only.sql:2875 | confirmed |





| `artist_id_count` | integer | YES | Distinct artist count | count | `COUNT(DISTINCT artist_id)` | schema_only.sql:2876 | confirmed |





| `album_id_count` | integer | YES | Distinct album count | count | `COUNT(DISTINCT album_id)` | schema_only.sql:2877 | confirmed |











### logging.task_executions (table)





- **Purpose:** fine-grained per-phase task execution timing log (distinct from `tasks.task_executions`, which is a coarse execution log).





- **Grain:** one row per task execution record, keyed by `record_id`.





- **Canonical source:** `logging.task_executions`; written by `task_log` in `backend/pifitness/worker/task_execution.py:300`, which inserts `task_id, task_name, extract_time_ms, load_time_ms, transform_time_ms, failure_type, error_text` (other columns remain NULL).





- **Consumers:** frontend `frontend/pifitness/src/app/admin/components/TaskLogView.tsx:20` (maps DB columns to display fields); `tasks.vw_task_execution` consumes aggregated data.





- **Relationships:** none declared. Do not confuse with `tasks.task_executions` (different schema, coarse log; both happen to have an index named `task_executions_pkey`).





- **Rules:** `enforced` PK `record_id` (`task_executions_pkey`, sequence-backed); `event_time_utc` DEFAULT `now()`; phase duration columns are milliseconds per their `_ms` names and the `task_log` parameter names.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `record_id` | bigint | NO | Log row id | sequence-backed identity | `task_log` insert omits it (sequence default) | `task_execution.py:300`; schema_only.sql:16684 | confirmed |





| `event_time_utc` | timestamp with time zone | YES | Event timestamp | UTC timestamptz, DEFAULT now() | `task_log` insert omits it → default | schema_only.sql:16686 | confirmed |





| `task_name` | text | YES | Task name | free text | `task_log` param `task_name` | `task_execution.py:301` | confirmed |





| `extract_time_ms` | integer | YES | Extract phase duration | milliseconds | `task_log` param `e_time` | `task_execution.py:303` | confirmed |





| `load_time_ms` | integer | YES | Load phase duration | milliseconds | `task_log` param `l_time` | `task_execution.py:304` | confirmed |





| `transform_time_ms` | integer | YES | Transform phase duration | milliseconds | `task_log` param `t_time` | `task_execution.py:305` | confirmed |





| `failure_type` | text | YES | Failure category | free text; non-NULL indicates a failed execution | `task_log` param `fail_type` | `task_execution.py:306` | confirmed |





| `error_text` | text | YES | Error message | free text | `task_log` param `fail_text` | `task_execution.py:307` | confirmed |





| `task_id` | integer | YES | Related task id | id | `task_log` param `task_id` | `task_execution.py:301` | confirmed |





| `interpolation_time_ms` | integer | YES | Interpolation phase duration | milliseconds | not inserted by `task_log` (remains NULL) | schema_only.sql:16694 | inferred |





| `forecast_time_ms` | integer | YES | Forecast phase duration | milliseconds | not inserted by `task_log` (remains NULL) | schema_only.sql:16695 | inferred |











### logging.vw_all_event_history (view)





- **Purpose:** unified event history backing the admin event-log viewer (searchable, filterable, errors-only toggle).





- **Grain:** one row per event across `logging.api_logins`, `logging.application_events`, and `logging.task_executions`; no key (order by `event_time_utc DESC`).





- **Canonical source:** derived view — `UNION ALL` of three typed streams.





- **Dependencies:** `logging.api_logins` (filtered `event_name IS NOT NULL`, event_type 'Login'), `logging.application_events` (event_type 'App Event'), `logging.task_executions` (event_type 'Task Execution').





- **Derived logic:** `description` is a composite (service + event name + token age, or category + description + execution seconds, or task name); task-event `error_text` combines `error_text || ' @ ' || failure_type`; `is_error = error_text IS NOT NULL`; `not_skip_row = description NOT ILIKE '%skipped%'`; ordered `event_time_utc DESC`.





- **Consumers:** `backend/pifitness/admin/queries.py:641` (`SELECT * FROM logging.vw_all_event_history WHERE 1=1`); frontend `frontend/pifitness/src/app/admin/components/EventHistory.tsx`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `event_time_utc` | timestamp with time zone | YES | Event timestamp | UTC timestamptz | derived from source tables | schema.generated.md:1500 | confirmed |





| `event_type` | text | YES | Event stream label | 'Login' | 'App Event' | 'Task Execution' | derived literal per CTE | schema_only.sql:16808,16817,16823 | confirmed |





| `description` | text | YES | Human-readable description | composite text | derived COALESCE chain | schema_only.sql:16810,16817,16824 | confirmed |





| `error_text` | text | YES | Error message | free text | derived; for task events `error_text || ' @ ' || failure_type` | schema_only.sql:16812,16819,16825 | confirmed |





| `is_error` | boolean | YES | Error flag | boolean | `error_text IS NOT NULL` | schema_only.sql:16849 | confirmed |





| `not_skip_row` | boolean | YES | Skip-row filter | boolean | `description NOT ILIKE '%skipped%'` | schema_only.sql:16857 | confirmed |











### logging.vw_db_biggest_table_chart (view)





- **Purpose:** chart of the largest tables by size over time (top tables).





- **Grain:** one row per date per table, for the top tables.





- **Canonical source:** derived view.





- **Dependencies:** `logging.db_size_log`, `public.vw_date`; filters `date_utc > '2025-11-16'`; identifies top tables via `row_number() OVER (ORDER BY max(total_size_mb) DESC) < 4`, then joins back per date.





- **Consumers:** none evidenced in application code (dashboard chart data).





- **Rules:** read-only view; `total_size_mb` is the max per table on that date.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_utc` | date | YES | Chart date | date | `public.vw_date.date_utc` | schema.generated.md:1510; schema_only.sql:16979 | confirmed |





| `db_table` | text | YES | Table name | text | `logging.db_size_log.table_name` | schema_only.sql:16980 | confirmed |





| `total_size_mb` | numeric | YES | Total size | MB | `max(sl.total_size_mb)` per date/table | schema_only.sql:16981 | confirmed |











### logging.vw_db_size (view)





- **Purpose:** current per-table size snapshot from PostgreSQL statistics.





- **Grain:** one row per user table with `total_size_mb > 0`.





- **Canonical source:** derived view over `pg_stat_user_tables`.





- **Dependencies:** `pg_stat_user_tables` (system view); sizes computed as bytes / 1024 / 1024 → MB (`numeric(10,1)`); `other_size_mb = total_size_mb - table_size_mb - index_size_mb` derived.





- **Consumers:** `logging.db_size_log` is populated from this view (`backend_tasks.py:96`); `logging.vw_db_biggest_table_chart` and `logging.vw_db_size_chart` read `logging.db_size_log`, not this view directly.





- **Rules:** read-only view; ordered `total_size_mb DESC`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `table_name` | text | YES | Table name | `schema.table` text | `schemaname || '.' || relname` from `pg_stat_user_tables` | schema.generated.md:1517; schema_only.sql:17016 | confirmed |





| `total_size_mb` | numeric | YES | Total relation size | MB | `pg_total_relation_size(relid) / 1024 / 1024` | schema_only.sql:17017 | confirmed |





| `table_size_mb` | numeric | YES | Table (main fork) size | MB | `pg_relation_size(relid) / 1024 / 1024` | schema_only.sql:17018 | confirmed |





| `index_size_mb` | numeric | YES | Index size | MB | `pg_indexes_size(relid) / 1024 / 1024` | schema_only.sql:17019 | confirmed |





| `other_size_mb` | numeric | YES | Other (toast, etc.) size | MB | derived `total - table - index` | schema_only.sql:17026 | inferred |











### logging.vw_db_size_chart (view)





- **Purpose:** daily aggregated database size chart across all tables.





- **Grain:** one row per date; sums across tables.





- **Canonical source:** derived view.





- **Dependencies:** `logging.db_size_log`, `public.vw_date`; latest size record per table per date (`row_number() = 1` ordered by `event_time_utc DESC`); filters `date_utc > '2025-11-16'`; `other_size_mb = total_size_mb - table_size_mb - index_size_mb` per record.





- **Consumers:** none evidenced in application code (dashboard chart data).





- **Rules:** read-only view; sums `table_size_mb`, `index_size_mb`, `other_size_mb` grouped by `date_utc`; ordered by date.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_utc` | date | YES | Chart date | date | `public.vw_date.date_utc` | schema.generated.md:1526; schema_only.sql:17097 | confirmed |





| `table_size_mb` | numeric | YES | Sum of table sizes | MB | `sum(table_size_mb)` per date | schema_only.sql:17098 | confirmed |





| `index_size_mb` | numeric | YES | Sum of index sizes | MB | `sum(index_size_mb)` per date | schema_only.sql:17099 | confirmed |





| `other_size_mb` | numeric | YES | Sum of other sizes | MB | `sum(total - table - index)` per date | schema_only.sql:17100 | confirmed |











### logging.vw_event_analysis (view)





- **Purpose:** application-event analysis with human-readable age.





- **Grain:** one row per `logging.application_events` row.





- **Canonical source:** derived view over `logging.application_events`.





- **Dependencies:** `logging.application_events`, `public.time_ago()`; `task_age = public.time_ago(event_time_utc)`; `is_error = error_text IS NOT NULL`; ordered `event_time_utc DESC`.





- **Consumers:** none evidenced in application code.





- **Rules:** read-only view; `time_ago` is `IMMUTABLE` and uses `NOW()` at call time, so `task_age` is relative to when the view is queried.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_age` | text | YES | Human-readable age | free text (e.g. '2 hours ago') | `public.time_ago(event_time_utc)` | schema.generated.md:1534; schema_only.sql:17139 | confirmed |





| `event_category` | text | YES | Event category | free text | `logging.application_events.event_category` | schema_only.sql:17140 | confirmed |





| `event_description` | text | YES | Event description | free text | `logging.application_events.event_description` | schema_only.sql:17141 | confirmed |





| `error_text` | text | YES | Error message | free text | `logging.application_events.error_text` | schema_only.sql:17142 | confirmed |





| `execution_time_ms` | integer | YES | Duration | milliseconds | `logging.application_events.execution_time_ms` | schema_only.sql:17143 | confirmed |





| `event_time_utc` | timestamp with time zone | YES | Event timestamp | UTC timestamptz | `logging.application_events.event_time_utc` | schema_only.sql:17144 | confirmed |





| `is_error` | boolean | YES | Error flag | boolean | `error_text IS NOT NULL` | schema_only.sql:17145 | confirmed |











### logging.vw_last_login (view)





- **Purpose:** most recent login per API service with seconds since that login.





- **Grain:** one row per `api_service_name`.





- **Canonical source:** derived view over `logging.api_logins`.





- **Dependencies:** `logging.api_logins` (filtered `event_name LIKE 'login%'`), `public.utc_now()`; `DISTINCT ON (api_service_name)` ordered by `event_time_utc DESC`; `seconds_since_last_event = round(extract(epoch from utc_now() - event_time_utc), 0)::integer` seconds.





- **Consumers:** `backend/pifitness/auth/status.py` reads `logging.api_logins` directly for last login/error (this view not evidenced as consumed).





- **Rules:** read-only view; `utc_now()` is `NOW() AT TIME ZONE 'UTC'` (STABLE), so seconds are relative to query time.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `api_service_name` | text | YES | External service name | text | `DISTINCT ON (api_service_name)` from `logging.api_logins` | schema.generated.md:1545; schema_only.sql:17204 | confirmed |





| `seconds_since_last_event` | integer | YES | Seconds since last login | integer seconds | `round(extract(epoch from public.utc_now() - event_time_utc), 0)::integer` | schema_only.sql:17205 | confirmed |











### logging.vw_task_analysis (view)





- **Purpose:** task-execution analysis with human-readable age and failure flag.





- **Grain:** one row per `logging.task_executions` row.





- **Canonical source:** derived view over `logging.task_executions`.





- **Dependencies:** `logging.task_executions`, `public.time_ago()`; `task_age = public.time_ago(event_time_utc)`; `is_failure = failure_type IS NOT NULL`; ordered `event_time_utc DESC`.





- **Consumers:** none evidenced in application code.





- **Rules:** read-only view; `time_ago` uses `NOW()` at call time, so `task_age` is relative to when the view is queried.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_age` | text | YES | Human-readable age | free text (e.g. '2 hours ago') | `public.time_ago(event_time_utc)` | schema.generated.md:1551; schema_only.sql:17230 | confirmed |





| `task_name` | text | YES | Task name | free text | `logging.task_executions.task_name` | schema_only.sql:17231 | confirmed |





| `is_failure` | boolean | YES | Failure flag | boolean | `failure_type IS NOT NULL` | schema_only.sql:17232 | confirmed |





| `error_text` | text | YES | Error message | free text | `logging.task_executions.error_text` | schema_only.sql:17233 | confirmed |











## music











### music.all_albums (table)





- **Purpose:** Spotify album master data (names + search vectors) used by track resolution and playlist features.





- **Grain:** one row per album, keyed by `album_id`.





- **Canonical source:** `music.all_albums`; populated by the Spotify JSON extraction worker (`backend/pifitness/worker/json_extractors.py`) and upserted by SQL in `deploy/migrations`/`schema_only.sql`.





- **Consumers:** `music.all_tracks` (via `album_id` FK-like join), `music_functions.py:393` (best-track lookup joins `all_albums`).





- **Relationships:** no declared FK; `album_id` is referenced by `music.all_tracks.album_id` at the application level.





- **Rules:** `enforced` PK `album_id` (`all_albums_pkey`); trgm index `trgm_idx_album_name` on `album_name_search` for fuzzy search. `updated_at_utc` is refreshed on each sync/upsert.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `album_id` | text | NO | Album id | Spotify album id | PK; extraction/upsert | schema.generated.md:1561; json_extractors.py | confirmed |





| `album_name` | text | YES | Album name | text | extraction | schema.generated.md:1562 | confirmed |





| `album_name_clean` | text | YES | Cleaned album name | text | normalized from `album_name` | schema.generated.md:1563 | inferred |





| `album_name_search` | text | YES | Search-optimized name | text | indexed with `gin_trgm_ops` | schema.generated.md:1564; index `trgm_idx_album_name` | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last sync timestamp | UTC timestamptz | set on upsert | schema.generated.md:1565 | confirmed |











### music.all_artists (table)





- **Purpose:** Spotify artist master data (names + search vectors) used by track resolution and playlist features.





- **Grain:** one row per artist, keyed by `artist_id`.





- **Canonical source:** `music.all_artists`; populated by the Spotify JSON extraction worker (`backend/pifitness/worker/json_extractors.py`) and upserted alongside `all_albums` in SQL (`schema_only.sql`/`definitions.sql` upsert block).





- **Consumers:** `music.all_tracks` (via `artist_id` join).





- **Relationships:** no declared FK; `artist_id` referenced by `music.all_tracks.artist_id` at application level.





- **Rules:** `enforced` PK `artist_id` (`all_artists_pkey`); trgm index `trgm_idx_artist_name` on `artist_name_search`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `artist_id` | text | NO | Artist id | Spotify artist id | PK; extraction/upsert | schema.generated.md:1574; json_extractors.py | confirmed |





| `artist_name` | text | YES | Artist name | text | extraction | schema.generated.md:1575 | confirmed |





| `artist_name_clean` | text | YES | Cleaned artist name | text | normalized from `artist_name` | schema.generated.md:1576 | inferred |





| `artist_name_search` | text | YES | Search-optimized name | text | indexed with `gin_trgm_ops` | schema.generated.md:1577; index `trgm_idx_artist_name` | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last sync timestamp | UTC timestamptz | set on upsert | schema.generated.md:1578 | confirmed |











### music.all_tracks (table)





- **Purpose:** Spotify track master data with core audio/metadata fields, the central track dimension for the music domain.





- **Grain:** one row per track, keyed by `track_id`.





- **Canonical source:** `music.all_tracks`; populated by `backend/pifitness/worker/json_extractors.py` (Spotify API extraction); upserted by SQL in `schema_only.sql` (`ON CONFLICT (track_id) DO UPDATE` when name/search/release/availability/markets/popularity/duration changed).





- **Consumers:** `music.track_recommendations`, `music.vw_all_tracks`, `music.vw_best_track_id`, `music_functions.py`, `music_service.py` (`get_track_ids_by_isrc`).





- **Relationships:** no declared FKs; `artist_id` -> `music.all_artists.artist_id` and `album_id` -> `music.all_albums.album_id` are application-level joins.





- **Rules:** `enforced` PK `track_id` (`all_tracks_pkey`); trgm index `trgm_idx_track_name` on `track_name_search`. `duration_ms` is milliseconds (column-name convention). `track_isrc` is the ISRC identifier (open set, not a declared enum).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | NO | Track id | Spotify track id | PK; extraction/upsert | schema.generated.md:1586 | confirmed |





| `track_name` | text | YES | Track name | text | extraction | schema.generated.md:1587 | confirmed |





| `track_name_clean` | text | YES | Cleaned track name | text | normalized from `track_name` | schema.generated.md:1588 | inferred |





| `track_name_search` | text | YES | Search-optimized name | text | indexed with `gin_trgm_ops` | schema.generated.md:1589; index `trgm_idx_track_name` | confirmed |





| `track_isrc` | text | YES | ISRC identifier | ISRC code (open set) | extraction | schema.generated.md:1590 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | extraction (column-name convention) | schema.generated.md:1591 | inferred |





| `artist_id` | text | YES | Artist id | references `all_artists.artist_id` (app-level) | extraction | schema.generated.md:1592 | confirmed |





| `artist_display_name` | text | YES | Artist display name | text | extraction | schema.generated.md:1593 | confirmed |





| `album_id` | text | YES | Album id | references `all_albums.album_id` (app-level) | extraction | schema.generated.md:1594 | confirmed |





| `release_date` | date | YES | Release date | calendar date | extraction | schema.generated.md:1595 | confirmed |





| `available_in_us` | boolean | YES | Available in US flag | boolean | extraction | schema.generated.md:1596 | confirmed |





| `total_markets` | integer | YES | Market count | count | extraction | schema.generated.md:1597 | confirmed |





| `popularity` | integer | YES | Popularity score | 0-100 scale (Spotify convention, not DB-enforced) | extraction | schema.generated.md:1598 | inferred |





| `updated_at_utc` | timestamp with time zone | YES | Last sync timestamp | UTC timestamptz | set on upsert | schema.generated.md:1599 | confirmed |





| `id_synced_at_utc` | timestamp with time zone | YES | ID sync timestamp | UTC timestamptz | set on sync | schema.generated.md:1600 | confirmed |











### music.isrc_non_swaps (table)





- **Purpose:** ISRC pairs explicitly rejected as mappings (low-confidence duplicates), i.e. "do not swap" decisions.





- **Grain:** one row per `(original_isrc, mapped_isrc)` rejected pair.





- **Canonical source:** `music.isrc_non_swaps`; written by procedure `music.isrc_duplicate_finder()` (`schema_only.sql:2847-2864`) for candidates where `duration_score < 0.99 AND artist_score < 0.25 AND album_score < 0.25`; `mapping_time_utc = CURRENT_TIMESTAMP`.





- **Consumers:** none evidenced in application code (audit/review reference only).





- **Relationships:** none declared.





- **Rules:** `enforced` uniqueness via `uq_original_mapped_isrc` on `(original_isrc, mapped_isrc)`; no PK declared.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `original_isrc` | text | YES | Original ISRC | ISRC code | `isrc_duplicate_finder()` | schema.generated.md:1609; schema_only.sql:2847 | confirmed |





| `mapped_isrc` | text | YES | Rejected mapped ISRC | ISRC code | `isrc_duplicate_finder()` | schema.generated.md:1610; schema_only.sql:2848 | confirmed |





| `mapping_time_utc` | timestamp with time zone | YES | Mapping timestamp | UTC timestamptz, DEFAULT CURRENT_TIMESTAMP | `isrc_duplicate_finder()` | schema.generated.md:1611; schema_only.sql:2854 | confirmed |











### music.isrc_possible_dupes (table)





- **Purpose:** candidate ISRC duplicate pairs scored by similarity, used for manual review before creating swaps/non-swaps.





- **Grain:** one row per candidate pair `(isrc1, isrc2)`.





- **Canonical source:** `music.isrc_possible_dupes`; populated by the duplicate-finder scoring logic (`music.isrc_duplicate_finder()` and related SQL in `schema_only.sql`); columns are similarity scores.





- **Consumers:** duplicate-review workflow (no direct application consumer evidenced).





- **Relationships:** none declared.





- **Rules:** no PK/index declared. Score columns are numeric similarity values (0-1 range implied by SIMILARITY usage, not DB-enforced — treat as open set).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc1` | text | YES | First ISRC of pair | ISRC code | duplicate-finder | schema.generated.md:1619 | confirmed |





| `isrc2` | text | YES | Second ISRC of pair | ISRC code | duplicate-finder | schema.generated.md:1620 | confirmed |





| `match_score` | numeric | YES | Overall match score | numeric similarity | duplicate-finder | schema.generated.md:1621 | confirmed |





| `track_score` | numeric | YES | Track-name similarity score | numeric similarity | duplicate-finder | schema.generated.md:1622 | confirmed |





| `artist_score` | numeric | YES | Artist-name similarity score | numeric similarity | duplicate-finder | schema.generated.md:1623 | confirmed |





| `album_score` | numeric | YES | Album-name similarity score | numeric similarity | duplicate-finder | schema.generated.md:1624 | confirmed |











### music.isrc_swaps (table)





- **Purpose:** curated ISRC remappings (original -> preferred/mapped ISRC) used to normalize track identity across listening history.





- **Grain:** one row per `original_isrc`, keyed by `original_isrc`.





- **Canonical source:** `music.isrc_swaps`; written by procedure `music.isrc_duplicate_finder()` (`schema_only.sql:2842-2845`) with `mapping_time_utc = CURRENT_TIMESTAMP`.





- **Consumers:** `music.missing_relationships` view (joins `isrc_swaps` on `valid_mapping`), `music.listening_history` reconciliation (coalesces `mapped_isrc`).





- **Relationships:** none declared.





- **Rules:** `enforced` PK `original_isrc` (`isrc_swaps_pkey`); `valid_mapping` boolean flags whether the swap is active; `mapping_time_utc` records when the swap was created/refreshed.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `original_isrc` | text | NO | Original ISRC | ISRC code | PK; duplicate-finder | schema.generated.md:1629 | confirmed |





| `mapped_isrc` | text | YES | Preferred/mapped ISRC | ISRC code | duplicate-finder | schema.generated.md:1630 | confirmed |





| `mapping_time_utc` | timestamp with time zone | YES | Mapping timestamp | UTC timestamptz, DEFAULT CURRENT_TIMESTAMP | duplicate-finder | schema.generated.md:1631 | confirmed |





| `valid_mapping` | boolean | YES | Swap active flag | boolean | duplicate-finder | schema.generated.md:1632 | confirmed |











### music.listening_history (table)





- **Purpose:** reconciled play history — which track (ISRC) was played in which playlist and when.





- **Grain:** one row per play event, keyed by composite `(isrc, played_at_utc)`.





- **Canonical source:** `music.listening_history`; written by `_insert_listening_history` in `backend/pifitness/activities/router.py:526` from `activities.vw_watch_music_heard` (`INSERT ... ON CONFLICT(played_at_utc, isrc) DO NOTHING`); `music.temp_listening_history` is truncated before each reload.





- **Consumers:** `music.missing_relationships`, `music.vw_listening_history`, `music.vw_playlist_last_heard`, `music.vw_playlist_isrc_stats`, rating-eligibility views.





- **Relationships:** no declared FKs; `isrc` relates to `music.all_tracks.track_isrc` / `music.isrc_swaps.original_isrc` at application level.





- **Rules:** `enforced` composite PK `(isrc, played_at_utc)` (`listening_history_pk`); `played_at_utc` and `isrc` NOT NULL; duplicates are ignored on conflict. Timestamps UTC.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `played_at_utc` | timestamp with time zone | NO | Play timestamp | UTC timestamptz | PK part; `vw_watch_music_heard` | schema.generated.md:1640 | confirmed |





| `isrc` | text | NO | Track ISRC | ISRC code | PK part; `vw_watch_music_heard` | schema.generated.md:1641 | confirmed |





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | `vw_watch_music_heard` | schema.generated.md:1642 | confirmed |











### music.missing_relationships (view)





- **Purpose:** playlists that are configured for a shuffle/recommendation type but still lack a corresponding child-playlist relationship — drives relationship creation.





- **Grain:** one row per playlist that needs at least one relationship (auto/manual/recommendation).





- **Canonical source:** derived view over `music.playlist_config` left-joined to `music.playlist_relationships` by `child_playlist_type`.





- **Dependencies:** `music.playlist_config`, `music.playlist_relationships` (joined 3x, once per type: 'manual', 'auto', 'recommendation').





- **Derived logic:** `needs_auto = pc.auto_shuffle AND NOT EXISTS(auto child)`, `needs_manual = pc.manual_shuffle AND NOT EXISTS(manual child)`, `needs_recs = pc.make_recs AND NOT EXISTS(recommendation child)`; filtered to rows where any flag is true.





- **Consumers:** `ensure_playlist_relationships` in `backend/backend_functions/music_functions.py:193` (selects `playlist_id, playlist_name, needs_auto, needs_manual, needs_recs`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | from `playlist_config.playlist_id` | derived | schema.generated.md:1650 | confirmed |





| `playlist_name` | text | YES | Playlist name | from `playlist_config.playlist_name` | derived | schema.generated.md:1651 | confirmed |





| `needs_auto` | boolean | YES | Needs auto-shuffle child | boolean | `pc.auto_shuffle AND child_playlist_id IS NULL` (type 'auto') | schema_only.sql:17521 | confirmed |





| `needs_manual` | boolean | YES | Needs manual-shuffle child | boolean | `pc.manual_shuffle AND child_playlist_id IS NULL` (type 'manual') | schema_only.sql:17522 | confirmed |





| `needs_recs` | boolean | YES | Needs recommendations child | boolean | `pc.make_recs AND child_playlist_id IS NULL` (type 'recommendation') | schema_only.sql:17523 | confirmed |











### music.playlist_config (table)





- **Purpose:** per-playlist shuffle/recommendation configuration (weights, flags, sync timestamps).





- **Grain:** one row per playlist, keyed by `playlist_id`.





- **Canonical source:** `music.playlist_config`; upserted from `staging.api_imports` (`schema_only.sql:5600-5633`, `ON CONFLICT (playlist_id) DO UPDATE` when name/description/track_count changed or inactive); `update_playlist_config` in `backend/backend_functions/music_service.py:98` persists shuffle weights.





- **Consumers:** `music_service.py:38` (`list_playlists` reads all rows), `music_functions.py`, `music.vw_playlist_config`, `missing_relationships`, rating/shuffle views.





- **Relationships:** no declared FKs; `playlist_id` is referenced by `music.playlist_isrcs`, `music.ratings`, etc. at application level.





- **Rules:** `enforced` PK `playlist_id` (`playlist_config_pkey`); weights are numeric (application convention, no range check); booleans `auto_shuffle`/`seeds_only`/`make_recs`/`is_active`/`manual_shuffle` control shuffle behavior; `last_verified_utc` marks last external verification.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | NO | Playlist id | Spotify playlist id | PK; upsert from staging | schema.generated.md:1659 | confirmed |





| `playlist_name` | text | YES | Playlist name | text | upsert | schema.generated.md:1660 | confirmed |





| `playlist_description` | text | YES | Playlist description | text | upsert | schema.generated.md:1661 | confirmed |





| `track_count` | integer | YES | Track count | count | upsert | schema.generated.md:1662 | confirmed |





| `auto_shuffle` | boolean | YES | Auto-shuffle enabled | boolean | config flag | schema.generated.md:1663 | confirmed |





| `seeds_only` | boolean | YES | Seeds-only mode | boolean | config flag | schema.generated.md:1664 | confirmed |





| `make_recs` | boolean | YES | Generate recommendations | boolean | config flag | schema.generated.md:1665 | confirmed |





| `ratings_weight` | numeric | YES | Rating weight | weight (numeric) | `update_playlist_config` | schema.generated.md:1666 | confirmed |





| `recency_weight` | numeric | YES | Recency weight | weight (numeric) | `update_playlist_config` | schema.generated.md:1667 | confirmed |





| `randomness_weight` | numeric | YES | Randomness weight | weight (numeric) | `update_playlist_config` | schema.generated.md:1668 | confirmed |





| `is_active` | boolean | YES | Playlist active flag | boolean | upsert; deactivated when absent from staging | schema.generated.md:1669 | confirmed |





| `minutes_to_sync` | integer | YES | Sync interval | minutes | config value | schema.generated.md:1670 | confirmed |





| `manual_shuffle` | boolean | YES | Manual shuffle enabled | boolean | config flag | schema.generated.md:1671 | confirmed |





| `last_synced_utc` | timestamp with time zone | YES | Last sync timestamp | UTC timestamptz | `music_functions.py:133` sets `CURRENT_TIMESTAMP` | schema.generated.md:1672 | confirmed |





| `last_auto_shuffled_utc` | timestamp with time zone | YES | Last auto-shuffle timestamp | UTC timestamptz | `update_playlist_config` / `music_functions.py:315` | schema.generated.md:1673 | confirmed |





| `last_verified_utc` | timestamp with time zone | YES | Last verification timestamp | UTC timestamptz | upsert from staging | schema.generated.md:1674 | confirmed |





| `prior_track_count` | integer | YES | Previous track count | count | snapshot before sync | schema.generated.md:1675 | confirmed |











### music.playlist_isrcs (table)





- **Purpose:** ordered track membership of playlists (ISRC sequence per playlist).





- **Grain:** one row per `(playlist_id, isrc)` position, ordered by `track_order`.





- **Canonical source:** `music.playlist_isrcs`; written by `backend/pifitness/music/queries.py` and `music_service.py` (DELETE then re-insert on playlist changes); loaded from `staging.stg_playlist_details` in SQL (`schema_only.sql:5541`).





- **Consumers:** playlist track endpoints (`/api/music/playlists/{playlist_id}/tracks`), shuffle logic views.





- **Relationships:** no declared FKs; relates to `playlist_config.playlist_id` and `all_tracks.track_isrc` at application level.





- **Rules:** no PK/index declared in inventory; `track_order` defines play sequence; `updated_at_utc` marks last membership change.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | insert from staging/queries | schema.generated.md:1683 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | insert from staging/queries | schema.generated.md:1684 | confirmed |





| `track_order` | integer | YES | Position in playlist | ordinal | insert from staging/queries | schema.generated.md:1685 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last change timestamp | UTC timestamptz | set on insert | schema.generated.md:1686 | confirmed |











### music.playlist_recommendation_exclusions (table)





- **Purpose:** ISRCs explicitly excluded from recommendations for a playlist ("remove recommendation" actions).





- **Grain:** one row per `(playlist_id, isrc)` exclusion.





- **Canonical source:** `music.playlist_recommendation_exclusions`; written by `remove_recommendation` in `backend/backend_functions/music_service.py:70` (`INSERT ... ON CONFLICT (playlist_id, isrc) DO NOTHING`).





- **Consumers:** recommendation-generation logic (excludes these ISRCs).





- **Relationships:** none declared.





- **Rules:** no PK declared; composite uniqueness implied by application (`ON CONFLICT (playlist_id, isrc)`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | `music_service.py:70` | schema.generated.md:1691 | confirmed |





| `isrc` | text | YES | Excluded ISRC | ISRC code | `music_service.py:70` | schema.generated.md:1692 | confirmed |





| `elo_track_predicted` | text | YES | Predicted Elo at exclusion | text (stores a score) | recorded at exclusion time | schema.generated.md:1693 | inferred |





| `decision_timestamp` | timestamp with time zone | YES | Exclusion timestamp | UTC timestamptz | set at exclusion | schema.generated.md:1694 | confirmed |











### music.playlist_relationships (table)





- **Purpose:** parent->child playlist relationships: auto-shuffled copies, manual-shuffled copies, and recommendation playlists derived from a parent playlist.





- **Grain:** one row per `(parent_playlist_id, child_playlist_id)` relationship.





- **Canonical source:** `music.playlist_relationships`; written by `ensure_playlist_relationships` / playlist generation in `backend/backend_functions/music_functions.py:212-228` when `needs_auto`/`needs_manual`/`needs_recs` (from `missing_relationships`) are true.





- **Consumers:** `missing_relationships` (joins by type), `vw_playlist_families`, `vw_playlist_shuffle_targets`.





- **Relationships:** no declared FKs; both columns reference `playlist_config.playlist_id` at application level.





- **Rules:** `enforced` composite PK `(parent_playlist_id, child_playlist_id)` (`pk_parent_child_playlist`); `child_playlist_type` is 'auto' | 'manual' | 'recommendation' (application convention, not a DB enum).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `parent_playlist_id` | text | NO | Parent playlist id | references `playlist_config.playlist_id` (app-level) | PK part; `music_functions.py:212` | schema.generated.md:1699 | confirmed |





| `child_playlist_id` | text | NO | Child playlist id | references `playlist_config.playlist_id` (app-level) | PK part; newly generated playlist | schema.generated.md:1700 | confirmed |





| `child_playlist_type` | text | YES | Relationship type | 'auto' | 'manual' | 'recommendation' | `music_functions.py:210-226` | schema.generated.md:1701 | confirmed |











### music.ratings (table)





- **Purpose:** per-playlist-per-ISRC Elo rating state (wins/losses/count) for the music matchup/rating feature.





- **Grain:** one row per `(playlist_id, isrc)` rated pair, keyed by composite PK.





- **Canonical source:** `music.ratings`; upserted from `music.vw_ratings_update` by `backend/backend_functions/music_functions.py:746` (`ON CONFLICT (playlist_id, isrc) DO UPDATE` when `last_rated_utc` changed); also written by `backend/pifitness/music/queries.py:443,530`.





- **Consumers:** rating endpoints (`GET/POST /api/music/ratings`), `vw_ratings`, `vw_rating_update`, matchup logic.





- **Relationships:** no declared FKs; references `playlist_config.playlist_id` and `all_tracks.track_isrc` at application level.





- **Rules:** `enforced` composite PK `(playlist_id, isrc)` (`isrc_ratings`); `elo_rating` is an application-managed Elo score (no DB range check); `rating_wins` + `rating_losses` <= `rating_count` is an application invariant, not DB-enforced.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | NO | Playlist id | references `playlist_config.playlist_id` (app-level) | PK part | schema.generated.md:1709 | confirmed |





| `isrc` | text | NO | Track ISRC | ISRC code | PK part | schema.generated.md:1710 | confirmed |





| `elo_rating` | integer | YES | Elo rating | score points | upsert from `vw_ratings_update` | schema.generated.md:1711 | confirmed |





| `rating_count` | integer | YES | Total ratings | count | upsert | schema.generated.md:1712 | confirmed |





| `rating_wins` | integer | YES | Wins | count | upsert | schema.generated.md:1713 | confirmed |





| `rating_losses` | integer | YES | Losses | count | upsert | schema.generated.md:1714 | confirmed |





| `last_rated_utc` | timestamp with time zone | YES | Last rating timestamp | UTC timestamptz | upsert conflict condition | schema.generated.md:1715 | confirmed |











### music.ratings_history (table)





- **Purpose:** append-only snapshot history of rating events (Elo before/after and result) for audit and the ratings-history view.





- **Grain:** one row per rating event, keyed by composite `(rating_time_utc, playlist_id, isrc)`.





- **Canonical source:** `music.ratings_history`; written by the rating write path (`music_functions.py`/`music/queries.py`) alongside `music.ratings` updates.





- **Consumers:** `music.vw_ratings_history` (adds `row_num` ranking).





- **Relationships:** no declared FKs.





- **Rules:** `enforced` composite PK `(rating_time_utc, playlist_id, isrc)` (`pk_rating_history`); `rating_result` smallint (outcome code, application convention); append-only (no update path evidenced).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `rating_time_utc` | timestamp with time zone | NO | Rating timestamp | UTC timestamptz | PK part | schema.generated.md:1723 | confirmed |





| `playlist_id` | text | NO | Playlist id | references `playlist_config.playlist_id` (app-level) | PK part | schema.generated.md:1724 | confirmed |





| `isrc` | text | NO | Track ISRC | ISRC code | PK part | schema.generated.md:1725 | confirmed |





| `isrc_vs` | text | YES | Opponent ISRC | ISRC code | the matched opponent | schema.generated.md:1726 | confirmed |





| `elo_old` | integer | YES | Elo before event | score points | rating write path | schema.generated.md:1727 | confirmed |





| `elo_new` | integer | YES | Elo after event | score points | rating write path | schema.generated.md:1728 | confirmed |





| `rating_result` | smallint | YES | Rating outcome | result code (app convention) | rating write path | schema.generated.md:1729 | confirmed |











### music.temp_listening_history (table)





- **Purpose:** scratch/staging table for listening-history reconciliation; truncated and reloaded before each `_insert_listening_history` run.





- **Grain:** one row per `(played_at_utc, isrc, playlist_id)` staging row; no PK declared (temp working table).





- **Canonical source:** `music.temp_listening_history`; truncated then populated from `activities.vw_watch_music_heard` by `_insert_listening_history` (`backend/pifitness/activities/router.py:526` docstring), then reconciled into `music.listening_history`.





- **Consumers:** `_insert_listening_history` only (application-level staging).





- **Relationships:** none declared.





- **Rules:** `convention` temp table — scratch working table; document existence but not full lifecycle beyond the reconcile job (per worklog classification of `temp_*` tables). Columns mirror `listening_history`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `played_at_utc` | timestamp with time zone | YES | Play timestamp | UTC timestamptz | staging | schema.generated.md:1737 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | staging | schema.generated.md:1738 | confirmed |





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | staging | schema.generated.md:1739 | confirmed |











### music.track_recommendation_decisions (table)





- **Purpose:** per-playlist-per-ISRC recommendation model decision snapshots — multiple Elo model predictions plus artist/genre/popularity snapshots at decision time.





- **Grain:** one row per `(playlist_id, isrc)` decision.





- **Canonical source:** `music.track_recommendation_decisions`; written by the recommendation generation path (`backend/pifitness/music/queries.py`, `backend/backend_functions/music_functions.py`).





- **Consumers:** `track_recommendations`, recommendation/playlist endpoints.





- **Relationships:** no declared FKs; references `playlist_config.playlist_id` and `all_tracks.track_isrc` at application level.





- **Rules:** no PK declared in inventory. All `elo_*` / `popularity_snap` are numeric model outputs (open set, not DB-enforced); `was_promoted` boolean flags whether the track was promoted; `decision_time_utc` is the decision timestamp (UTC); `model_version`/`notes` are free text.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | recommendation path | schema.generated.md:1744 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | recommendation path | schema.generated.md:1745 | confirmed |





| `elo_track_linear` | numeric | YES | Linear-model track Elo | score points | model output | schema.generated.md:1746 | confirmed |





| `elo_track_rf` | numeric | YES | Random-forest track Elo | score points | model output | schema.generated.md:1747 | confirmed |





| `elo_track_neural_net` | numeric | YES | Neural-net track Elo | score points | model output | schema.generated.md:1748 | confirmed |





| `elo_track_pairwise` | numeric | YES | Pairwise track Elo | score points | model output | schema.generated.md:1749 | confirmed |





| `elo_track_predicted` | numeric | YES | Predicted track Elo | score points | model output | schema.generated.md:1750 | confirmed |





| `elo_artist_snap` | numeric | YES | Artist Elo snapshot | score points | model output | schema.generated.md:1751 | confirmed |





| `elo_genre_snap` | numeric | YES | Genre Elo snapshot | score points | model output | schema.generated.md:1752 | confirmed |





| `popularity_snap` | numeric | YES | Popularity snapshot | score points | model output | schema.generated.md:1753 | confirmed |





| `was_promoted` | boolean | YES | Promotion flag | boolean | model output | schema.generated.md:1754 | confirmed |





| `decision_time_utc` | timestamp with time zone | YES | Decision timestamp | UTC timestamptz | set at decision | schema.generated.md:1755 | confirmed |





| `model_version` | text | YES | Model version | text | recommendation path | schema.generated.md:1756 | confirmed |





| `notes` | text | YES | Decision notes | free text | recommendation path | schema.generated.md:1757 | confirmed |











### music.track_recommendations (table)





- **Purpose:** per-playlist track recommendation candidates with audio features, Elo norms, and genre ranks used to build shuffle/recommendation playlists.





- **Grain:** one row per `(playlist_id, isrc)` recommendation candidate.





- **Canonical source:** `music.track_recommendations`; populated by the recommendation path (`backend/pifitness/music/queries.py`, `backend/backend_functions/music_functions.py`) joining `all_tracks` + `track_recommendation_decisions` + audio features.





- **Consumers:** recommendation endpoints, `vw_rating_eligible`, shuffle-target views.





- **Relationships:** no declared FKs; references `playlist_config.playlist_id` and `all_tracks.track_isrc` at application level.





- **Rules:** no PK declared in inventory. `m_*` columns are Spotify audio-feature values (0-1 scale convention, not DB-enforced); `is_best_genre` boolean flags the best-genre pick; `playlist_genre_rank` ordinal within genre.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | references `playlist_config.playlist_id` (app-level) | recommendation path | schema.generated.md:1762 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | recommendation path | schema.generated.md:1763 | confirmed |





| `artist_id` | text | YES | Artist id | references `all_artists.artist_id` (app-level) | recommendation path | schema.generated.md:1764 | confirmed |





| `genre` | text | YES | Track genre | text | recommendation path | schema.generated.md:1765 | confirmed |





| `possible_genres` | text | YES | Candidate genres | text | recommendation path | schema.generated.md:1766 | confirmed |





| `artist_elo` | numeric | YES | Artist Elo | score points | recommendation path | schema.generated.md:1767 | confirmed |





| `genre_elo` | numeric | YES | Genre Elo | score points | recommendation path | schema.generated.md:1768 | confirmed |





| `playlist_genre_rank` | integer | YES | Genre rank in playlist | ordinal | recommendation path | schema.generated.md:1769 | confirmed |





| `is_best_genre` | boolean | YES | Best-genre flag | boolean | recommendation path | schema.generated.md:1770 | confirmed |





| `track_name_clean` | text | YES | Cleaned track name | text | recommendation path | schema.generated.md:1771 | confirmed |





| `popularity` | integer | YES | Popularity | score points | recommendation path | schema.generated.md:1772 | confirmed |





| `m_acousticness` | numeric | YES | Acousticness feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1773 | inferred |





| `m_energy` | numeric | YES | Energy feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1774 | inferred |





| `m_instrumentalness` | numeric | YES | Instrumentalness feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1775 | inferred |





| `m_key` | numeric | YES | Key feature | pitch class | audio features | schema.generated.md:1776 | inferred |





| `m_liveness` | numeric | YES | Liveness feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1777 | inferred |





| `m_mode` | numeric | YES | Mode feature | major/minor | audio features | schema.generated.md:1778 | inferred |





| `m_speechiness` | numeric | YES | Speechiness feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1779 | inferred |





| `m_tempo` | numeric | YES | Tempo feature | BPM | audio features | schema.generated.md:1780 | inferred |





| `m_valence` | numeric | YES | Valence feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1781 | inferred |





| `m_danceability` | numeric | YES | Danceability feature | 0-1 (Spotify convention) | audio features | schema.generated.md:1782 | inferred |





| `elo_artist_norm` | numeric | YES | Normalized artist Elo | score points | model output | schema.generated.md:1783 | confirmed |





| `elo_genre_norm` | numeric | YES | Normalized genre Elo | score points | model output | schema.generated.md:1784 | confirmed |





| `elo_track_linear` | numeric | YES | Linear-model track Elo | score points | model output | schema.generated.md:1785 | confirmed |





| `elo_track_random_forest` | numeric | YES | RF track Elo | score points | model output | schema.generated.md:1786 | confirmed |





| `elo_track_neural_net` | numeric | YES | NN track Elo | score points | model output | schema.generated.md:1787 | confirmed |





| `elo_track_pairwise` | numeric | YES | Pairwise track Elo | score points | model output | schema.generated.md:1788 | confirmed |





| `elo_track_predicted` | numeric | YES | Predicted track Elo | score points | model output | schema.generated.md:1789 | confirmed |











### music.vw_all_tracks (view)





- **Purpose:** enriched track dimension: every track with its album name, best-track flag, and ISRC-normalized best ISRC.





- **Grain:** one row per track (from `music.all_tracks`).





- **Canonical source:** derived view over `music.all_tracks`, `music.all_albums`, `music.isrc_swaps`, `music.vw_best_track_id`.





- **Dependencies:** `music.all_tracks` (JOIN `all_albums` on album_id), `music.isrc_swaps` (valid_mapping on track_isrc), `music.vw_best_track_id` (on coalesced isrc).





- **Derived logic:** `album_name = all_albums.album_name_clean`; `best_track_id` from `vw_best_track_id`; `is_best_track_id = track_id = best_track_id`; `best_isrc = COALESCE(mapped_isrc, track_isrc)`; `is_best_isrc = best_isrc = track_isrc`.





- **Consumers:** track detail/search features, `vw_isrc_dupe_review`, `vw_recent_plays`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | YES | Track id | Spotify track id | `all_tracks.track_id` | schema.generated.md:1794 | confirmed |





| `track_name` | text | YES | Track name | text | `all_tracks.track_name` | schema.generated.md:1795 | confirmed |





| `track_name_clean` | text | YES | Cleaned track name | text | `all_tracks.track_name_clean` | schema.generated.md:1796 | confirmed |





| `track_name_search` | text | YES | Search-optimized name | text | `all_tracks.track_name_search` | schema.generated.md:1797 | confirmed |





| `track_isrc` | text | YES | Track ISRC | ISRC code | `all_tracks.track_isrc` | schema.generated.md:1798 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | `all_tracks.duration_ms` | schema.generated.md:1799 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | `all_tracks.artist_id` | schema.generated.md:1800 | confirmed |





| `artist_display_name` | text | YES | Artist display name | text | `all_tracks.artist_display_name` | schema.generated.md:1801 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | `all_tracks.album_id` | schema.generated.md:1802 | confirmed |





| `release_date` | date | YES | Release date | calendar date | `all_tracks.release_date` | schema.generated.md:1803 | confirmed |





| `available_in_us` | boolean | YES | Available in US flag | boolean | `all_tracks.available_in_us` | schema.generated.md:1804 | confirmed |





| `total_markets` | integer | YES | Market count | count | `all_tracks.total_markets` | schema.generated.md:1805 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | `all_tracks.popularity` | schema.generated.md:1806 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last sync timestamp | UTC timestamptz | `all_tracks.updated_at_utc` | schema.generated.md:1807 | confirmed |





| `album_name` | text | YES | Album name | text | `all_albums.album_name_clean` (JOIN) | schema_only.sql:18179 | confirmed |





| `best_track_id` | text | YES | Best track id for isrc | Spotify track id | `vw_best_track_id.best_track_id` | schema_only.sql:18180 | confirmed |





| `is_best_track_id` | boolean | YES | Is this the best track | boolean | `track_id = best_track_id` | schema_only.sql:18181 | confirmed |





| `best_isrc` | text | YES | Best ISRC | ISRC code | `COALESCE(mapped_isrc, track_isrc)` | schema_only.sql:18182 | confirmed |





| `is_best_isrc` | boolean | YES | Is this the best ISRC | boolean | `best_isrc = track_isrc` | schema_only.sql:18183 | confirmed |











### music.vw_best_track_id (view)





- **Purpose:** picks the canonical ("best") track per ISRC for dedup across swap variants.





- **Grain:** one row per coalesced ISRC (`isrc`).





- **Canonical source:** derived view over `music.all_tracks` left-joined to `music.isrc_swaps` (valid mapping).





- **Derived logic:** `isrc = COALESCE(mapped_isrc, track_isrc)`; `isrc_preference = row_number() OVER (PARTITION BY isrc ORDER BY available_in_us DESC, release_date, total_markets DESC, popularity DESC)`; keeps `isrc_preference = 1` (best = available in US, earliest release, most markets, highest popularity).





- **Consumers:** `vw_all_tracks`, `vw_isrc_dupe_review`, `vw_recent_plays`, `music_functions.py:392,532,862`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc` | text | YES | Coalesced ISRC | ISRC code | `COALESCE(mapped_isrc, track_isrc)` | schema.generated.md:1817 | confirmed |





| `best_track_id` | text | YES | Best track id | Spotify track id | `all_tracks.track_id` of preferred row | schema.generated.md:1818 | confirmed |





| `isrc_preference` | bigint | YES | Preference rank | ordinal (1 = best) | `row_number()` per isrc | schema.generated.md:1819 | confirmed |











### music.vw_isrc_dupe_review (view)





- **Purpose:** scored candidate ISRC duplicate pairs for manual review, with a preferred-ISRC recommendation and rationale.





- **Grain:** one row per candidate pair `(isrc1, isrc2)` from `music.isrc_possible_dupes`.





- **Canonical source:** derived view over `music.isrc_possible_dupes`, `music.vw_best_track_id`, `music.all_tracks`, `music.all_albums`.





- **Derived logic:** `duration_score = least/greatest(duration_ms)` ratio rounded; `preferred_isrc` chosen by CASE (available_in_us, then release_date, then total_markets, then popularity, then duration_ms); `preferred_logic` text code ('avail'|'release'|'markets'|'popularity'|'duration'|'No Preference'); ordered by match/track/album/artist score DESC.





- **Consumers:** duplicate-review workflow; `isrc_duplicate_finder` consumes review outcomes.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc1` | text | YES | First ISRC | ISRC code | `isrc_possible_dupes.isrc1` | schema.generated.md:1824 | confirmed |





| `pref1` | bigint | YES | First track preference rank | ordinal | `vw_best_track_id.isrc_preference` | schema.generated.md:1825 | confirmed |





| `track_name1` | text | YES | First track name | text | `all_tracks.track_name` | schema.generated.md:1826 | confirmed |





| `artist_name1` | text | YES | First artist name | text | `all_tracks.artist_display_name` | schema.generated.md:1827 | confirmed |





| `album_name1` | text | YES | First album name | text | `all_albums.album_name` | schema.generated.md:1828 | confirmed |





| `duration_1` | integer | YES | First track duration | minutes | `round(duration_ms/60)` | schema.generated.md:1829 | confirmed |





| `isrc2` | text | YES | Second ISRC | ISRC code | `isrc_possible_dupes.isrc2` | schema.generated.md:1830 | confirmed |





| `pref2` | bigint | YES | Second track preference rank | ordinal | `vw_best_track_id.isrc_preference` | schema.generated.md:1831 | confirmed |





| `track_name2` | text | YES | Second track name | text | `all_tracks.track_name` | schema.generated.md:1832 | confirmed |





| `artist_name2` | text | YES | Second artist name | text | `all_tracks.artist_display_name` | schema.generated.md:1833 | confirmed |





| `album_name2` | text | YES | Second album name | text | `all_albums.album_name` | schema.generated.md:1834 | confirmed |





| `duration_2` | integer | YES | Second track duration | minutes | `round(duration_ms/60)` | schema.generated.md:1835 | confirmed |





| `match_score` | numeric | YES | Overall match score | similarity score | `isrc_possible_dupes.match_score` | schema.generated.md:1836 | confirmed |





| `track_score` | numeric | YES | Track-name similarity | similarity score | `isrc_possible_dupes.track_score` | schema.generated.md:1837 | confirmed |





| `artist_score` | numeric | YES | Artist similarity | similarity score | `isrc_possible_dupes.artist_score` | schema.generated.md:1838 | confirmed |





| `album_score` | numeric | YES | Album similarity | similarity score | `isrc_possible_dupes.album_score` | schema.generated.md:1839 | confirmed |





| `duration_score` | numeric | YES | Duration similarity | ratio (0-1) | `least/greatest(duration_ms)` | schema_only.sql:18344 | confirmed |





| `preferred_isrc` | text | YES | Recommended ISRC | ISRC code | CASE priority: avail > release > markets > popularity > duration | schema_only.sql:18345-18357 | confirmed |





| `preferred_logic` | text | YES | Preference rationale | 'avail'|'release'|'markets'|'popularity'|'duration'|'No Preference' | CASE expression | schema_only.sql:18358-18365 | confirmed |











### music.vw_listening_history (view)





- **Purpose:** ISRC-normalized, parent-playlist-mapped listening history.





- **Grain:** one row per play event from `music.listening_history`.





- **Canonical source:** derived view over `music.listening_history`.





- **Dependencies:** `music.listening_history`, `music.isrc_swaps` (valid_mapping), `music.playlist_relationships` (parent map).





- **Derived logic:** `isrc = COALESCE(mapped_isrc, lh.isrc)`; `playlist_id = COALESCE(parent_playlist_id, lh.playlist_id)`.





- **Consumers:** `vw_playlist_last_heard`, `vw_playlist_isrc_stats`, `vw_rating_eligible`, `vw_recent_plays`, `vw_playlist_shuffle_eligibility`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `played_at_utc` | timestamp with time zone | YES | Play timestamp | UTC timestamptz | `listening_history.played_at_utc` | schema.generated.md:1847 | confirmed |





| `isrc` | text | YES | Normalized ISRC | ISRC code | `COALESCE(mapped_isrc, lh.isrc)` | schema_only.sql:18515 | confirmed |





| `playlist_id` | text | YES | Parent playlist id | references `playlist_config.playlist_id` (app-level) | `COALESCE(pr.parent_playlist_id, lh.playlist_id)` | schema_only.sql:18516 | confirmed |











### music.vw_playlist_config (view)





- **Purpose:** playlist listing with derived playlist type and last-played timestamp, excluding playlists that are themselves children of another playlist.





- **Grain:** one row per non-child playlist (where no `playlist_relationships` row has it as `child_playlist_id`).





- **Canonical source:** derived view over `music.playlist_config`, `music.playlist_relationships`, `music.vw_playlist_last_heard`.





- **Derived logic:** `playlist_type = 'Parents' if has parent else 'Seeds' if seeds_only else 'Other'`; `last_played_utc` from `vw_playlist_last_heard`; ordered by last played, active, seeds, auto, recs, track count.





- **Consumers:** playlist listing UI, `vw_playlist_detail_sync_logic`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `playlist_config.playlist_id` | schema.generated.md:1854 | confirmed |





| `playlist_name` | text | YES | Playlist name | text | `playlist_config.playlist_name` | schema.generated.md:1855 | confirmed |





| `playlist_description` | text | YES | Playlist description | text | `playlist_config.playlist_description` | schema.generated.md:1856 | confirmed |





| `track_count` | integer | YES | Track count | count | `playlist_config.track_count` | schema.generated.md:1857 | confirmed |





| `auto_shuffle` | boolean | YES | Auto-shuffle enabled | boolean | `playlist_config.auto_shuffle` | schema.generated.md:1858 | confirmed |





| `manual_shuffle` | boolean | YES | Manual shuffle enabled | boolean | `playlist_config.manual_shuffle` | schema.generated.md:1859 | confirmed |





| `seeds_only` | boolean | YES | Seeds-only mode | boolean | `playlist_config.seeds_only` | schema.generated.md:1860 | confirmed |





| `make_recs` | boolean | YES | Generate recommendations | boolean | `playlist_config.make_recs` | schema.generated.md:1861 | confirmed |





| `ratings_weight` | numeric | YES | Rating weight | weight | `playlist_config.ratings_weight` | schema.generated.md:1862 | confirmed |





| `recency_weight` | numeric | YES | Recency weight | weight | `playlist_config.recency_weight` | schema.generated.md:1863 | confirmed |





| `randomness_weight` | numeric | YES | Randomness weight | weight | `playlist_config.randomness_weight` | schema.generated.md:1864 | confirmed |





| `minutes_to_sync` | integer | YES | Sync interval | minutes | `playlist_config.minutes_to_sync` | schema.generated.md:1865 | confirmed |





| `playlist_type` | text | YES | Derived type | 'Parents' | 'Seeds' | 'Other' | CASE expression | schema_only.sql:18587-18591 | confirmed |





| `last_played_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `vw_playlist_last_heard.last_played_utc` | schema_only.sql:18592 | confirmed |











### music.vw_playlist_detail_sync_logic (view)





- **Purpose:** sync-priority ranking of playlists that need attention (manual/shuffle/seed/master types), capped at top 5 (`rval` 1-5).





- **Grain:** one row per candidate playlist with a computed `next_valid_sync` and rank.





- **Canonical source:** derived view over `music.playlist_relationships`, `music.playlist_config`, `music.vw_playlist_last_heard`, `music.vw_playlist_with_mismatched_isrc`.





- **Derived logic:** classifies `list_type` ('manual' for manual-type children; 'updated'|'seeds'|'rare seeds'|'masters'|'No Type' for active parents); `next_valid_sync` = mismatch? now-1d : 'updated'? now : 'masters'? last_synced+3d : 'seeds'? +7d : 'rare seeds'? +30d : NULL; `rval = row_number()` ordered by next_valid_sync and last_played; filter `0 < rval < 6`.





- **Consumers:** sync/scheduler logic deciding which playlists to sync next.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | derived from relationships/config | schema.generated.md:1872 | confirmed |





| `playlist_name` | text | YES | Playlist name | text | `playlist_config.playlist_name` | schema.generated.md:1873 | confirmed |





| `next_valid_sync` | timestamp with time zone | YES | Next valid sync time | UTC timestamptz | type-based offset from `last_synced`/`now()` | schema_only.sql:18767-18773 | confirmed |





| `last_played_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `vw_playlist_last_heard` | schema_only.sql:18782 | confirmed |





| `rval` | bigint | YES | Sync rank | ordinal (1-5) | `row_number()` ordered by next_valid_sync, last_played | schema_only.sql:18785 | confirmed |











### music.vw_playlist_families (view)





- **Purpose:** playlist family graph: self, parent, and sibling relationships derived from `music.playlist_relationships`.





- **Grain:** one row per (playlist, related-playlist) pair with a `playlist_type` role.





- **Canonical source:** derived view: `UNION` of self-rows from `music.playlist_config` and parent/sibling rows from self-joining `music.playlist_relationships` on `parent_playlist_id`.





- **Derived logic:** `playlist_type` = 'self' for own row; 'parent' when the joined relationship shares the same child; 'sibling' otherwise.





- **Consumers:** family/playlist navigation features.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `playlist_config.playlist_id` or relationship child | schema.generated.md:1881 | confirmed |





| `child_playlist_id` | text | YES | Related playlist id | text | self or sibling/parent child id | schema.generated.md:1882 | confirmed |





| `playlist_type` | text | YES | Relationship role | 'self' | 'parent' | 'sibling' | CASE expression | schema_only.sql:18841-18852 | confirmed |











### music.vw_playlist_isrc_stats (view)





- **Purpose:** per-playlist-per-ISRC shuffle recommendation scores (recency, ratings, randomness pressures and total weight) used to order tracks in shuffle/recommendation playlists.





- **Grain:** one row per `(parent_playlist_id, target_playlist_id, isrc)` catalog row.





- **Canonical source:** complex derived view with CTEs (`catalog`, `isrc_last_played_anywhere`, `isrc_last_played_on_playlist`, `playlist_last_played_any_track`, `elo`, `config`, `recency_inputs`, `pressures`) over shuffle targets, listening history, ratings, and playlist config.





- **Dependencies:** `music.vw_playlist_shuffle_targets`, `music.vw_playlist_isrcs`, `music.vw_listening_history`, `music.vw_ratings`, `music.playlist_config`.





- **Derived logic:** `recency_pct`/`ratings_pct`/`random_pct` = `percent_rank()` per parent playlist; default Elo 1500 when missing; `days_since_played_*` squared (CURRENT_DATE anchor, 730-day fallback); `total_weight` combines the three weights; `default_new_order` and `cumulative_playlist_minutes` for ordering. Uses `CURRENT_TIMESTAMP`/`CURRENT_DATE` (query-time relative).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Parent playlist id | text | shuffle target parent | schema.generated.md:1888 | confirmed |





| `target_playlist_id` | text | YES | Target playlist id | text | shuffle target | schema.generated.md:1889 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | `vw_playlist_isrcs.isrc` | schema.generated.md:1890 | confirmed |





| `track_id` | text | YES | Track id | Spotify track id | via best-track mapping | schema.generated.md:1891 | confirmed |





| `track_artist` | text | YES | Track artist | text | derived | schema.generated.md:1892 | confirmed |





| `album_name` | text | YES | Album name | text | derived | schema.generated.md:1893 | confirmed |





| `duration_s` | integer | YES | Duration | seconds | derived | schema.generated.md:1894 | confirmed |





| `recency_pct` | double precision | YES | Recency percentile | 0-1 | `percent_rank()` per playlist | schema_only.sql:19079 | confirmed |





| `ratings_pct` | double precision | YES | Ratings percentile | 0-1 | `percent_rank()` on Elo | schema_only.sql:19080 | confirmed |





| `random_pct` | double precision | YES | Random percentile | 0-1 | `percent_rank()` on `random()` | schema_only.sql:19081 | confirmed |





| `recency_weight` | numeric | YES | Recency weight | weight | `playlist_config.recency_weight` (default 1) | schema_only.sql:19057 | confirmed |





| `ratings_weight` | numeric | YES | Ratings weight | weight | `playlist_config.ratings_weight` (default 1) | schema_only.sql:19058 | confirmed |





| `randomness_weight` | numeric | YES | Randomness weight | weight | `playlist_config.randomness_weight` (default 1) | schema_only.sql:19059 | confirmed |





| `elo_rating` | integer | YES | Elo rating | score points | `vw_ratings.elo_rating`, default 1500 | schema_only.sql:19078 | confirmed |





| `play_count_on_playlist` | bigint | YES | Play count on playlist | count | per-parent-playlist play count | schema_only.sql:19067 | confirmed |





| `recency_pressure` | double precision | YES | Recency pressure | derived score | pressure computation | schema_only.sql:19082+ | inferred |





| `ratings_pressure` | double precision | YES | Ratings pressure | derived score | pressure computation | schema_only.sql:19082+ | inferred |





| `randomness_pressure` | double precision | YES | Randomness pressure | derived score | pressure computation | schema_only.sql:19082+ | inferred |





| `total_weight` | double precision | YES | Combined weight | derived score | weighted combination | schema_only.sql:19082+ | inferred |





| `minutes_to_sync` | integer | YES | Sync interval | minutes | `playlist_config.minutes_to_sync` (default 9999) | schema_only.sql:19060 | confirmed |





| `default_new_order` | bigint | YES | Default new order | ordinal | derived ordering | schema_only.sql:19082+ | inferred |





| `cumulative_playlist_minutes` | numeric | YES | Cumulative minutes | minutes | cumulative duration | schema_only.sql:19082+ | inferred |











### music.vw_playlist_isrcs (view)





- **Purpose:** ISRC-normalized ordered track membership of playlists.





- **Grain:** one row per `(playlist_id, isrc)` position from `music.playlist_isrcs`.





- **Canonical source:** derived view over `music.playlist_isrcs` left-joined to `music.isrc_swaps`.





- **Derived logic:** `isrc = COALESCE(mapped_isrc, pi.isrc)` (applies swaps even when `valid_mapping` is not constrained).





- **Consumers:** `vw_playlist_isrc_stats`, playlist track endpoints.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `playlist_isrcs.playlist_id` | schema.generated.md:1914 | confirmed |





| `isrc` | text | YES | Normalized ISRC | ISRC code | `COALESCE(s.mapped_isrc, pi.isrc)` | schema_only.sql:18884 | confirmed |





| `track_order` | integer | YES | Position in playlist | ordinal | `playlist_isrcs.track_order` | schema.generated.md:1916 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last change timestamp | UTC timestamptz | `playlist_isrcs.updated_at_utc` | schema.generated.md:1917 | confirmed |











### music.vw_playlist_last_heard (view)





- **Purpose:** most recent play timestamp per parent playlist.





- **Grain:** one row per parent playlist (where mapped playlist_id IS NOT NULL).





- **Canonical source:** derived view: `max(listening_history.played_at_utc)` grouped by COALESCE(parent_playlist_id, playlist_id), over `music.listening_history` left-joined to `music.playlist_relationships`.





- **Consumers:** `vw_playlist_config`, `vw_playlist_detail_sync_logic`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `last_played_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `max(lh.played_at_utc)` per playlist | schema.generated.md:1922 | confirmed |





| `playlist_id` | text | YES | Parent playlist id | text | `COALESCE(pr.parent_playlist_id, lh.playlist_id)` | schema.generated.md:1923 | confirmed |











### music.vw_playlist_shuffle_eligibility (view)





- **Purpose:** active playlists eligible for auto-shuffle (played since last shuffle, or have mismatched ISRCs).





- **Grain:** one row per eligible playlist.





- **Canonical source:** derived view over `music.playlist_config`, `music.vw_playlist_with_mismatched_isrc`, `music.listening_history`, `music.playlist_relationships`.





- **Derived logic:** filter `is_active AND (auto_shuffle OR mismatched)`; `last_played_utc = max(lh.played_at_utc)` per parent playlist; keep only where `last_played_utc > last_auto_shuffled_utc` (default anchor '2025-12-01').





- **Consumers:** shuffle scheduler deciding which playlists to reshuffle.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `playlist_config.playlist_id` | schema.generated.md:1928 | confirmed |





| `playlist_name` | text | YES | Playlist name | text | `playlist_config.playlist_name` | schema.generated.md:1929 | confirmed |





| `last_played_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `max(lh.played_at_utc)` per playlist | schema.generated.md:1930 | confirmed |





| `last_auto_shuffled_utc` | timestamp with time zone | YES | Last auto-shuffle timestamp | UTC timestamptz | `playlist_config.last_auto_shuffled_utc` (default '2025-12-01') | schema_only.sql:19295 | confirmed |











### music.vw_playlist_shuffle_targets (view)





- **Purpose:** shuffle-eligible parent->child relationships (auto or manual type) used as shuffle targets.





- **Grain:** one row per relationship of type 'auto' or 'manual'.





- **Canonical source:** derived view over `music.playlist_relationships` filtered `child_playlist_type IN ('auto','manual')`.





- **Consumers:** `vw_playlist_isrc_stats`, `vw_playlist_shuffle_eligibility`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `parent_playlist_id` | text | YES | Parent playlist id | text | `playlist_relationships.parent_playlist_id` | schema.generated.md:1936 | confirmed |





| `target_playlist_id` | text | YES | Target (child) playlist id | text | `playlist_relationships.child_playlist_id` | schema.generated.md:1937 | confirmed |





| `target_playlist_type` | text | YES | Target relationship type | 'auto' | 'manual' | `playlist_relationships.child_playlist_type` | schema.generated.md:1938 | confirmed |











### music.vw_playlist_with_mismatched_isrc (view)





- **Purpose:** playlists containing at least one ISRC that differs from its mapped (preferred) ISRC — flags playlists needing re-normalization.





- **Grain:** one distinct `playlist_id` per mismatched membership.





- **Canonical source:** derived view over `music.playlist_isrcs` left-joined to `music.isrc_swaps` (valid_mapping).





- **Derived logic:** `WHERE pd.isrc <> COALESCE(s.mapped_isrc, pd.isrc)`.





- **Consumers:** `vw_playlist_shuffle_eligibility`, `vw_playlist_detail_sync_logic`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id with mismatch | text | `playlist_isrcs.playlist_id` | schema.generated.md:1943 | confirmed |











### music.vw_rating_eligible (view)





- **Purpose:** tracks due for a user rating (played but not yet rated, or never rated), per playlist.





- **Grain:** one row per `(isrc, playlist_id)` candidate.





- **Canonical source:** derived view over `music.listening_history`, `music.isrc_swaps`, `music.playlist_relationships`, `music.vw_playlist_isrcs`, `music.playlist_config`, `music.vw_ratings`.





- **Derived logic:** `needs_rating = COALESCE(last_rated_utc, '1980-07-18') < last_played_utc` (sentinel '1980-07-18' = never rated); `played_in_last_24 = last_played_utc > now() - 24h`; `never_rated = last_rated_utc IS NULL`; filter `needs_rating AND (played_in_last_24 OR rating_count = 0)`. Uses `CURRENT_TIMESTAMP` (query-time relative).





- **Consumers:** `vw_rating_eligible_playlists`, rating-matchup UI.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc` | text | YES | Track ISRC | ISRC code | normalized via `isrc_swaps` | schema.generated.md:1948 | confirmed |





| `playlist_id` | text | YES | Playlist id | text | parent-mapped playlist | schema.generated.md:1949 | confirmed |





| `playlist_name` | text | YES | Playlist name | text | `playlist_config.playlist_name` | schema.generated.md:1950 | confirmed |





| `last_played_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `max(lh.played_at_utc)` per (isrc, playlist) | schema.generated.md:1951 | confirmed |





| `last_rated_utc` | timestamp with time zone | YES | Last rated timestamp | UTC timestamptz | `vw_ratings.last_rated_utc` | schema.generated.md:1952 | confirmed |





| `needs_rating` | boolean | YES | Rating due flag | boolean | `COALESCE(last_rated_utc,'1980-07-18') < last_played_utc` | schema_only.sql:19366 | confirmed |





| `played_in_last_24` | boolean | YES | Played in last 24h | boolean | `last_played_utc > CURRENT_TIMESTAMP - interval '24:00:00'` | schema_only.sql:19367 | confirmed |





| `never_rated` | boolean | YES | Never rated flag | boolean | `last_rated_utc IS NULL` | schema_only.sql:19368 | confirmed |











### music.vw_rating_eligible_playlists (view)





- **Purpose:** playlists that have at least one track due for rating, with a count-appended display name.





- **Grain:** one row per eligible playlist.





- **Canonical source:** derived view: `GROUP BY playlist_id, playlist_name` over `music.vw_rating_eligible`.





- **Derived logic:** `playlist_name = name || ' (' || count(DISTINCT isrc) || ')'`.





- **Consumers:** `GET /api/music/ratings` (`list_rating_eligible_playlists`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `vw_rating_eligible.playlist_id` | schema.generated.md:1960 | confirmed |





| `playlist_name` | text | YES | Display name with count | text | `name || ' (' || count || ')'` | schema_only.sql:19438 | confirmed |











### music.vw_rating_update (view)





- **Purpose:** latest rating state per `(playlist_id, isrc)` computed from rating history — the upsert source for `music.ratings`.





- **Grain:** one row per `(playlist_id, isrc)` (latest event only, `row_num = 1`).





- **Canonical source:** derived view over `music.vw_ratings_history`.





- **Derived logic:** `elo_rating = elo_new` of latest event; `rating_count = count(*)` over partition; `rating_wins = count(rating_result > 0)`; `rating_losses = count(rating_result < 0)`; `last_rated_utc = latest rating_time_utc`; `rating_result > 0` = win, `< 0` = loss (application convention).





- **Consumers:** `music_functions.py:746` upserts into `music.ratings`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `ratings_history.playlist_id` | schema.generated.md:1966 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | `ratings_history.isrc` | schema.generated.md:1967 | confirmed |





| `elo_rating` | integer | YES | Latest Elo | score points | `elo_new` of latest event | schema_only.sql:19554 | confirmed |





| `rating_count` | bigint | YES | Total ratings | count | `count(*)` per partition | schema_only.sql:19555 | confirmed |





| `rating_wins` | bigint | YES | Wins | count | `count(rating_result > 0)` | schema_only.sql:19556-19560 | confirmed |





| `rating_losses` | bigint | YES | Losses | count | `count(rating_result < 0)` | schema_only.sql:19561-19565 | confirmed |





| `last_rated_utc` | timestamp with time zone | YES | Last rating timestamp | UTC timestamptz | latest `rating_time_utc` | schema_only.sql:19566 | confirmed |











### music.vw_ratings (view)





- **Purpose:** per-(playlist, normalized-ISRC) aggregated rating state (max Elo, wins, losses, last rated).





- **Grain:** one row per `(playlist_id, isrc)` after ISRC normalization.





- **Canonical source:** derived view over `music.ratings` left-joined to `music.isrc_swaps` (valid_mapping), grouping by `playlist_id, COALESCE(mapped_isrc, isrc)`.





- **Derived logic:** `isrc` normalized via swaps; aggregates `max(elo_rating)`, `max(rating_count)`, `max(rating_wins)`, `max(rating_losses)`, `max(last_rated_utc)`.





- **Consumers:** `vw_rating_eligible`, `vw_recent_plays`, rating UI.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `ratings.playlist_id` | schema.generated.md:1977 | confirmed |





| `isrc` | text | YES | Normalized ISRC | ISRC code | `COALESCE(mapped_isrc, r.isrc)` | schema_only.sql:18958 | confirmed |





| `elo_rating` | integer | YES | Max Elo | score points | `max(r.elo_rating)` | schema.generated.md:1978 | confirmed |





| `rating_count` | integer | YES | Max rating count | count | `max(r.rating_count)` | schema.generated.md:1979 | confirmed |





| `rating_wins` | integer | YES | Max wins | count | `max(r.rating_wins)` | schema.generated.md:1980 | confirmed |





| `rating_losses` | integer | YES | Max losses | count | `max(r.rating_losses)` | schema.generated.md:1981 | confirmed |





| `last_rated_utc` | timestamp with time zone | YES | Max last-rated timestamp | UTC timestamptz | `max(r.last_rated_utc)` | schema.generated.md:1982 | confirmed |











### music.vw_ratings_history (view)





- **Purpose:** latest rating event per `(playlist_id, isrc, isrc_vs)` after ISRC normalization, with a recency rank.





- **Grain:** one row per normalized triple (`row_num = 1`, latest by `rating_time_utc`).





- **Canonical source:** derived view over `music.ratings_history` left-joined twice to `music.isrc_swaps` (for `isrc` and `isrc_vs`).





- **Derived logic:** `isrc`/`isrc_vs` normalized via `isrc_swaps` (valid_mapping); `row_number() OVER (PARTITION BY playlist_id, normalized isrc, normalized isrc_vs ORDER BY rating_time_utc DESC)`; filter `row_num = 1`.





- **Consumers:** `vw_rating_update`, `vw_ratings_update`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `rating_time_utc` | timestamp with time zone | YES | Rating timestamp | UTC timestamptz | `ratings_history.rating_time_utc` | schema.generated.md:1988 | confirmed |





| `playlist_id` | text | YES | Playlist id | text | `ratings_history.playlist_id` | schema.generated.md:1989 | confirmed |





| `isrc` | text | YES | Normalized ISRC | ISRC code | `COALESCE(s1.mapped_isrc, h.isrc)` | schema_only.sql:19472 | confirmed |





| `isrc_vs` | text | YES | Normalized opponent ISRC | ISRC code | `COALESCE(s2.mapped_isrc, h.isrc_vs)` | schema_only.sql:19473 | confirmed |





| `elo_old` | integer | YES | Elo before | score points | `ratings_history.elo_old` | schema.generated.md:1991 | confirmed |





| `elo_new` | integer | YES | Elo after | score points | `ratings_history.elo_new` | schema.generated.md:1992 | confirmed |





| `rating_result` | smallint | YES | Rating outcome | result code (>0 win, <0 loss) | `ratings_history.rating_result` | schema.generated.md:1993 | confirmed |





| `row_num` | bigint | YES | Recency rank | ordinal (1 = latest) | `row_number()` per partition | schema.generated.md:1994 | confirmed |











### music.vw_ratings_update (view)





- **Purpose:** aggregate rating summary per `(playlist_id, isrc)`: current Elo at last rating, distinct win/loss counts, and last-rated timestamp.





- **Grain:** one row per `(playlist_id, isrc)`.





- **Canonical source:** derived view over `music.vw_ratings_history` with a `lr` CTE (max `rating_time_utc` per pair).





- **Derived logic:** `elo_current = elo_new` at the last-rated time; `rating_count = count(DISTINCT rating_time_utc)`; `wins = count(DISTINCT rating_result > 0)`; `losses = count(DISTINCT rating_result < 0)`; `last_rated_utc = max(rating_time_utc)`. Default Elo 1500 applied downstream in `vw_recent_plays`.





- **Consumers:** not directly consumed by application code evidenced (sister view `vw_rating_update` feeds `music.ratings`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | text | `vw_ratings_history.playlist_id` | schema.generated.md:1998 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | `vw_ratings_history.isrc` | schema.generated.md:1999 | confirmed |





| `elo_current` | integer | YES | Elo at last rating | score points | `elo_new` at `last_rated` | schema.generated.md:2002 | confirmed |





| `rating_count` | bigint | YES | Distinct rating times | count | `count(DISTINCT rating_time_utc)` | schema.generated.md:2003 | confirmed |





| `wins` | bigint | YES | Distinct wins | count | `count(rating_result > 0)` | schema.generated.md:2004 | confirmed |





| `losses` | bigint | YES | Distinct losses | count | `count(rating_result < 0)` | schema.generated.md:2005 | confirmed |





| `last_rated_utc` | timestamp with time zone | YES | Last rated timestamp | UTC timestamptz | `max(rating_time_utc)` | schema.generated.md:2006 | confirmed |











### music.vw_recent_plays (view)





- **Purpose:** recent-play summary per ISRC: last played, 30-day/total play counts, track artist line, and current Elo with global max/min context.





- **Grain:** one row per ISRC with at least one play.





- **Canonical source:** derived view: `play_stats` CTE over `music.vw_listening_history` joined to `vw_best_track_id`, `all_tracks`, `playlist_relationships`, `vw_ratings`, `playlist_config`.





- **Derived logic:** `playcount_last_30 = count(played_at_utc > now() - 30 days)`; `playcount_total = count(*)`; `track_artist = 'name by artist [ from playlist]'`, default Elo 1500 when unrated; window `max`/`min` over playcounts and Elo for global context. Uses `CURRENT_TIMESTAMP` (query-time relative).





- **Consumers:** `GET /api/music/recent-plays` (`list_recent_plays`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `last_played_at_utc` | timestamp with time zone | YES | Last played timestamp | UTC timestamptz | `max(played_at_utc)` per isrc | schema.generated.md:2011 | confirmed |





| `playcount_last_30` | bigint | YES | Plays in last 30 days | count | `count(now()-30d filter)` | schema.generated.md:2012 | confirmed |





| `playcount_total` | bigint | YES | Total plays | count | `count(*)` per isrc | schema.generated.md:2013 | confirmed |





| `track_artist` | text | YES | Display label | text | `'name by artist [ from playlist]'` | schema_only.sql:19726 | confirmed |





| `rating` | integer | YES | Current Elo | score points | `COALESCE(vw_ratings.elo_rating, 1500)` | schema_only.sql:19727 | confirmed |





| `max_playcount_last_30` | bigint | YES | Global max 30d count | count | `max() OVER ()` | schema_only.sql:19728 | confirmed |





| `max_playcount_total` | bigint | YES | Global max total count | count | `max() OVER ()` | schema_only.sql:19729 | confirmed |





| `max_elo` | integer | YES | Global max Elo | score points | `max(COALESCE(elo,1500)) OVER ()` | schema_only.sql:19730 | confirmed |





| `min_elo` | integer | YES | Global min Elo | score points | `min(COALESCE(elo,1500)) OVER ()` | schema_only.sql:19731 | confirmed |











### music.vw_track_id_finder (view)





- **Purpose:** distinct track ISRCs ordered by most recently synced (lookup helper).





- **Grain:** one row per distinct `track_isrc` in `music.all_tracks`.





- **Canonical source:** derived view: `SELECT DISTINCT track_isrc, id_synced_at_utc FROM music.all_tracks ORDER BY id_synced_at_utc` (only `track_isrc` projected).





- **Consumers:** track-id lookup flows.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_isrc` | text | YES | Track ISRC | ISRC code | `all_tracks.track_isrc` distinct | schema.generated.md:2024 | confirmed |











### music.vw_track_id_to_isrc (view)





- **Purpose:** track id -> normalized ISRC mapping helper.





- **Grain:** one row per track in `music.all_tracks`.





- **Canonical source:** derived view over `music.all_tracks` left-joined to `music.isrc_swaps`.





- **Derived logic:** `isrc = COALESCE(mapped_isrc, track_isrc)` (applies swaps without requiring `valid_mapping`).





- **Consumers:** ISRC resolution flows.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | YES | Track id | Spotify track id | `all_tracks.track_id` | schema.generated.md:2029 | confirmed |





| `isrc` | text | YES | Normalized ISRC | ISRC code | `COALESCE(s.mapped_isrc, t.track_isrc)` | schema_only.sql:19830 | confirmed |











## public











The `public` schema holds the calendar date dimension, schema-migration bookkeeping, and PostgreSQL/PostGIS system catalog objects. It is not the application's primary data schema: application tables live under `activities`, `api_services`, `exercises`, `food`, `health`, `logging`, `music`, `staging`, `tasks`, and `volleyball`.











Objects documented in this section (in-scope): `public.dim_date`, `public.dim_hour`, `public.schema_migrations`, `public.vw_date`, `public.vw_date_all`, `public.vw_hour`.











Excluded from semantic documentation (reasons below): PostGIS system catalog views `public.geography_columns`, `public.geometry_columns`, `public.raster_columns`, `public.raster_overviews`, `public.spatial_ref_sys`; PostgreSQL statistics views `public.pg_stat_statements`, `public.pg_stat_statements_info`; scratch/work tables `public.staging.temp_bc_import`, `public.v_end_elev`, `public.v_is_loop`, `public.v_start_elev`, `public.v_total_distance`. The sibling scratch table `staging.temp_bc_import` is excluded in the `staging` section.











### public.dim_date (table)





- **Purpose:** Calendar date dimension used for relative-date reporting on the logging dashboard views.





- **Grain:** one row per calendar date, keyed by `date_key`; time grain is one day (`date_utc` is a `date`, no time component).





- **Primary/candidate key:** `dim_date_pkey` on `date_key` (enforced); `dim_date_date_utc_key` unique on `date_utc` (enforced).





- **Canonical source and lifecycle:** reference data; no application write path was found in repository search (`repo_references.json` reports 0 references for this object). Population/update/delete lifecycle is not evidenced in the repository.





- **Consumers:** `public.dim_hour` via enforced FK `date_key`; views `public.vw_date`, `public.vw_date_all`, `public.vw_hour`; logging dashboard views `logging.vw_db_biggest_table_chart` and `logging.vw_db_size` (their catalog definitions join `vw_date`).





- **Relationships:** enforced FK into this table: `public.dim_hour.date_key` -> `public.dim_date.date_key` (`dim_hour_date_key_fkey`). No FKs are declared on `dim_date` itself.





- **Rules:** `enforced` primary key and unique constraint on `date_utc`; `convention` treated as append-only reference calendar (not evidenced in application code).





| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_key` | integer | NO | Surrogate date key | integer count | reference data load (load path not evidenced) | schema.generated.md | confirmed |





| `date_utc` | date | NO | Calendar date | date value, no time component or time zone | reference data load (load path not evidenced) | schema.generated.md | confirmed |





| `year` | smallint | NO | Calendar year of `date_utc` | integer, e.g. 2026 | derived from `date_utc` | schema.generated.md | confirmed |





| `quarter` | smallint | NO | Quarter of year | 1-4 | derived from `date_utc` | schema.generated.md | confirmed |





| `month` | smallint | NO | Month of year | 1-12 | derived from `date_utc` | schema.generated.md | confirmed |





| `day_of_month` | smallint | NO | Day of month | 1-31 | derived from `date_utc` | schema.generated.md | confirmed |





| `day_of_week` | smallint | NO | Day of week | numeric day-of-week; numbering scheme not evidenced | derived from `date_utc` | schema.generated.md | confirmed |





| `day_name` | text | NO | Day name | free text, e.g. 'Monday' | derived from `date_utc` | schema.generated.md | confirmed |





| `month_name` | text | NO | Month name | free text, e.g. 'November' | derived from `date_utc` | schema.generated.md | confirmed |





| `week_of_year` | smallint | NO | Week of year | smallint; numbering scheme not evidenced | derived from `date_utc` | schema.generated.md | confirmed |





| `day_of_year` | smallint | YES | Day of year | 1-366; nullable though populated per view definition | derived from `date_utc` | schema.generated.md | confirmed |











### public.dim_hour (table)





- **Purpose:** Hour dimension extending `public.dim_date` to individual hours, used for hourly relative-date reporting.





- **Grain:** one row per hour, keyed by `hour_key`; scoped to one date via `date_key`.





- **Primary/candidate key:** `dim_hour_pkey` on `hour_key` (enforced); `dim_hour_ts_utc_key` unique on `ts_utc` (enforced).





- **Canonical source and lifecycle:** reference data; no application write path found (0 code references); population lifecycle not evidenced.





- **Consumers:** view `public.vw_hour` (joins on `date_key`); no direct application consumers found.





- **Relationships:** enforced FK `dim_hour_date_key_fkey`: `date_key` -> `public.dim_date.date_key`.





- **Rules:** `enforced` primary key and unique constraint on `ts_utc`; `convention` `ts_utc` is `timestamp without time zone` (naive timestamp, not `timestamptz`) — consumers must interpret it consistently with `dim_date.date_utc`.





- **Evidence/confidence:** `schema.generated.md` lines 2053-2067; `repo_references.json` (0 references). Structural facts confirmed; write path unknown.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `hour_key` | integer | NO | Surrogate hour key | integer count | reference data load (load path not evidenced) | schema.generated.md | confirmed |





| `ts_utc` | timestamp without time zone | NO | Hour timestamp | naive timestamp, no time zone declared | reference data load (load path not evidenced) | schema.generated.md | confirmed |





| `date_key` | integer | NO | Parent date | FK -> `public.dim_date.date_key` | reference data load | dim_hour_date_key_fkey | confirmed |





| `hour` | smallint | NO | Hour of day | 0-23 (range not declared in DB) | derived from `ts_utc` | schema.generated.md | confirmed |











### public.schema_migrations (table)





- **Purpose:** Bookkeeping table recording which numbered schema migrations have been applied to a database; used by the `tools/db/migrate.py` migration tool, not by the FastAPI application runtime.





- **Grain:** one row per applied migration version, keyed by `version`.





- **Primary/candidate key:** `schema_migrations_pkey` on `version` (enforced).





- **Canonical source and lifecycle:** written only by `tools/db/migrate.py` (`INSERT INTO schema_migrations (version)` after applying a migration's up SQL; `DELETE FROM schema_migrations` on rollback). `applied_at` defaults to `now()`. Read by `_applied_versions` to decide which migrations remain pending.





- **Consumers:** `tools/db/migrate.py`; no application or frontend consumers.





- **Relationships:** no FKs declared to or from this table.





- **Rules:** `enforced` PK on `version`; `convention` versions are applied in ascending migration filename order (see `tools/db/migrate.py`).





- **Evidence/confidence:** `schema.generated.md` lines 2186-2194; `tools/db/migrate.py` (`SCHEMA_MIGRATIONS_SQL`, `_ensure_schema_migrations`, `_applied_versions`). Structural facts confirmed; write behavior confirmed from tool source.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `version` | bigint | NO | Migration version identifier | bigint count | `INSERT` by migrate.py after applying up SQL | tools/db/migrate.py; schema_migrations_pkey | confirmed |





| `applied_at` | timestamp with time zone | NO | Time the migration was recorded as applied | UTC timestamptz | `now()` default, insert-assigned | tools/db/migrate.py `SCHEMA_MIGRATIONS_SQL` | confirmed |











### public.vw_date (view)





- **Purpose:** Read-only date-dimension view over `public.dim_date` restricted to dates up to today and ordered newest first; supports logging dashboard relative-date charts.





- **Grain:** one row per calendar date (past dates and today only), keyed by `date_key`; derived view, recomputed from `dim_date` on each query — not a stored base table.





- **Primary/candidate key:** none declared on the view; logical key `date_key` is unique by derivation from `dim_date`.





- **Canonical source and lifecycle:** derived view; definition stored in the database catalog; no refresh process (read-only derivation).





- **Consumers:** `logging.vw_db_biggest_table_chart` and `logging.vw_db_size` (their catalog view definitions reference `vw_date`); no direct application code references (0 repo references).





- **Relationships:** underlying dependency `public.dim_date`; views carry no declared FKs.





- **Rules:** `convention` the anchor CTE uses `CURRENT_DATE` at query time, so `relative_day`/`relative_week`/… shift with the query day; `WHERE d.date_utc <= a.today` excludes future dates.





- **Evidence/confidence:** `schema.generated.md` lines 2239-2261; `docs/data/_evidence/database/definitions.sql` (`-- public.vw_date (view)`); `catalog.json`. Transformation logic confirmed from view definition.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_key` | integer | YES | Surrogate date key (from dim_date) | integer | `dim_date.date_key` | definitions.sql | confirmed |





| `date_utc` | date | YES | Calendar date | date | `dim_date.date_utc` | definitions.sql | confirmed |





| `year` | smallint | YES | Calendar year | smallint | `dim_date.year` | definitions.sql | confirmed |





| `quarter` | smallint | YES | Quarter of year | 1-4 | `dim_date.quarter` | definitions.sql | confirmed |





| `month` | smallint | YES | Month of year | 1-12 | `dim_date.month` | definitions.sql | confirmed |





| `day_of_month` | smallint | YES | Day of month | 1-31 | `dim_date.day_of_month` | definitions.sql | confirmed |





| `day_of_week` | smallint | YES | Day of week | numeric | `dim_date.day_of_week` | definitions.sql | confirmed |





| `day_name` | text | YES | Day name | free text | `dim_date.day_name` | definitions.sql | confirmed |





| `month_name` | text | YES | Month name | free text | `dim_date.month_name` | definitions.sql | confirmed |





| `week_of_year` | smallint | YES | Week of year | smallint | `dim_date.week_of_year` | definitions.sql | confirmed |





| `relative_day` | integer | YES | Days from query date | integer (<=0) | `date_utc - CURRENT_DATE` | definitions.sql | confirmed |





| `relative_week` | integer | YES | Weeks from query date | integer | `date_trunc('week')` difference vs anchor | definitions.sql | confirmed |





| `relative_month` | integer | YES | Months from query date | integer | month arithmetic vs anchor | definitions.sql | confirmed |





| `relative_year` | integer | YES | Years from query date | integer | year arithmetic vs anchor | definitions.sql | confirmed |





| `day_of_year` | smallint | YES | Day of year | 1-366 | `dim_date.day_of_year` | definitions.sql | confirmed |





| `relative_30` | double precision | YES | Position within current 30-day bucket | double precision | floor expression vs anchor | definitions.sql | confirmed |





| `relative_90` | double precision | YES | Position within current 90-day bucket | double precision | floor expression vs anchor | definitions.sql | confirmed |





| `dm30` | double precision | YES | Offset within 30-day bucket | double precision | modulo expression vs anchor | definitions.sql | confirmed |





| `dm90` | double precision | YES | Offset within 90-day bucket | double precision | modulo expression vs anchor | definitions.sql | confirmed |





### public.vw_date_all (view)





- **Purpose:** Read-only view over all of `public.dim_date` without the future-date filter, with a smaller column set than `vw_date`; reference for reporting joins.





- **Grain:** one row per calendar date in `dim_date`, keyed by `date_key`; derived, recomputed per query.





- **Primary/candidate key:** none declared; logical key `date_key`.





- **Canonical source and lifecycle:** derived view over `dim_date`; no refresh process.





- **Consumers:** no direct application code references found (0 repo references); available for dashboard/reporting queries.





- **Relationships:** underlying dependency `public.dim_date`.





- **Rules:** `convention` no `WHERE` filter — includes future dates unlike `vw_date`; omits `day_of_year`, `relative_30`, `relative_90`, `dm30`, `dm90`.





- **Evidence/confidence:** `schema.generated.md` lines 2262-2279; `definitions.sql`. Transformation logic confirmed from view definition.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_key` | integer | YES | Surrogate date key | integer | `dim_date.date_key` | definitions.sql | confirmed |





| `date_utc` | date | YES | Calendar date | date | `dim_date.date_utc` | definitions.sql | confirmed |





| `year` | smallint | YES | Calendar year | smallint | `dim_date.year` | definitions.sql | confirmed |





| `quarter` | smallint | YES | Quarter of year | 1-4 | `dim_date.quarter` | definitions.sql | confirmed |





| `month` | smallint | YES | Month of year | 1-12 | `dim_date.month` | definitions.sql | confirmed |





| `day_of_month` | smallint | YES | Day of month | 1-31 | `dim_date.day_of_month` | definitions.sql | confirmed |





| `day_of_week` | smallint | YES | Day of week | numeric | `dim_date.day_of_week` | definitions.sql | confirmed |





| `day_name` | text | YES | Day name | free text | `dim_date.day_name` | definitions.sql | confirmed |





| `month_name` | text | YES | Month name | free text | `dim_date.month_name` | definitions.sql | confirmed |





| `week_of_year` | smallint | YES | Week of year | smallint | `dim_date.week_of_year` | definitions.sql | confirmed |





| `relative_day` | integer | YES | Days from query date | integer | `date_utc - CURRENT_DATE` | definitions.sql | confirmed |





| `relative_week` | integer | YES | Weeks from query date | integer | `date_trunc('week')` difference vs anchor | definitions.sql | confirmed |





| `relative_month` | integer | YES | Months from query date | integer | month arithmetic vs anchor | definitions.sql | confirmed |





| `relative_year` | integer | YES | Years from query date | integer | year arithmetic vs anchor | definitions.sql | confirmed |











### public.vw_hour (view)





- **Purpose:** Read-only date-and-hour dimension view joining `public.dim_date` to `public.dim_hour`, restricted to dates up to today.





- **Grain:** one row per date-hour combination (one row per hour in `dim_hour` per date), logical key (`date_key`, hour); derived, recomputed per query.





- **Primary/candidate key:** none declared.





- **Canonical source and lifecycle:** derived view over `dim_date` JOIN `dim_hour` ON `date_key`; no refresh process.





- **Consumers:** no direct application code references found (0 repo references); available for hourly dashboard/reporting queries.





- **Relationships:** underlying dependencies `public.dim_date` and `public.dim_hour`.





- **Rules:** `convention` `date_hour_utc` is `dim_hour.ts_utc` — naive `timestamp without time zone`; `WHERE d.date_utc <= CURRENT_DATE` excludes future dates.





- **Evidence/confidence:** `schema.generated.md` line 2280; `definitions.sql`; `catalog.json`. Transformation logic confirmed from view definition.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `date_key` | integer | YES | Surrogate date key | integer | `dim_date.date_key` | definitions.sql | confirmed |





| `date_utc` | date | YES | Calendar date | date | `dim_date.date_utc` | definitions.sql | confirmed |





| `date_hour_utc` | timestamp without time zone | YES | Hour timestamp | naive timestamp | `dim_hour.ts_utc` | definitions.sql | confirmed |





| `year` | smallint | YES | Calendar year | smallint | `dim_date.year` | definitions.sql | confirmed |





| `quarter` | smallint | YES | Quarter of year | 1-4 | `dim_date.quarter` | definitions.sql | confirmed |





| `month` | smallint | YES | Month of year | 1-12 | `dim_date.month` | definitions.sql | confirmed |





| `day_of_month` | smallint | YES | Day of month | 1-31 | `dim_date.day_of_month` | definitions.sql | confirmed |





| `day_of_week` | smallint | YES | Day of week | numeric | `dim_date.day_of_week` | definitions.sql | confirmed |





| `day_name` | text | YES | Day name | free text | `dim_date.day_name` | definitions.sql | confirmed |





| `month_name` | text | YES | Month name | free text | `dim_date.month_name` | definitions.sql | confirmed |





| `week_of_year` | smallint | YES | Week of year | smallint | `dim_date.week_of_year` | definitions.sql | confirmed |





| `relative_day` | integer | YES | Days from query date | integer | `date_utc - CURRENT_DATE` | definitions.sql | confirmed |





| `relative_week` | integer | YES | Weeks from query date | integer | `date_trunc('week')` difference vs anchor | definitions.sql | confirmed |





| `relative_month` | integer | YES | Months from query date | integer | month arithmetic vs anchor | definitions.sql | confirmed |





| `relative_year` | integer | YES | Years from query date | integer | year arithmetic vs anchor | definitions.sql | confirmed |











### Excluded objects in `public` (not application data)











The following objects appear in the generated inventory under schema `public` but are excluded from semantic documentation in this dictionary: they are PostgreSQL/PostGIS system catalogs or scratch tables and carry no application semantics. Each is represented once with a `### ... (TABLE|VIEW)` heading so every inventory object is accounted for exactly once; their columns are not individually documented because they are not application data. Column structures are per `schema.generated.md`.











### public.geography_columns (view)





- **Excluded:** yes — PostGIS system catalog view listing tables/views that contain a geography column; maintained by PostGIS, not by the application.





- **Reason:** system catalog metadata; zero application code references (`repo_references.json`: 0 references).





- **Columns:** 7 (`f_table_catalog`, `f_table_schema`, `f_table_name`, `f_geography_column`, `coord_dimension`, `srid`, `type`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `f_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_geography_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `coord_dimension` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `type` | text | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `f_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_geography_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `coord_dimension` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `type` | text | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` lines 2068-2080 (structure); absence of code references confirmed. Not application data — confirmed by exclusion analysis.











### public.geometry_columns (view)





- **Excluded:** yes — PostGIS system catalog view listing geometry columns; maintained by PostGIS, not by the application.





- **Reason:** system catalog metadata; zero application code references.





- **Columns:** 7 (`f_table_catalog` `character varying`, `f_table_schema`, `f_table_name`, `f_geometry_column`, `coord_dimension`, `srid`, `type` `character varying`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `f_table_catalog` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_geometry_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `coord_dimension` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `type` | character varying | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `f_table_catalog` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `f_geometry_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `coord_dimension` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `type` | character varying | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` lines 2081-2092 (structure); not application data — confirmed by exclusion analysis.











### public.raster_columns (view)





- **Excluded:** yes — PostGIS system catalog view for raster columns; maintained by PostGIS, not by the application.





- **Reason:** system catalog metadata; zero application code references.





- **Columns:** 17 (`r_table_catalog` ... `spatial_index`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `r_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `scale_x` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `scale_y` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `blocksize_x` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `blocksize_y` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `same_alignment` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `regular_blocking` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `num_bands` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `pixel_types` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `nodata_values` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `out_db` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `extent` | USER-DEFINED | YES | Excluded system/scratch object — column not semantically documented. |
| `spatial_index` | boolean | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `r_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `scale_x` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `scale_y` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `blocksize_x` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `blocksize_y` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `same_alignment` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `regular_blocking` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `num_bands` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `pixel_types` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `nodata_values` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `out_db` | ARRAY | YES | Excluded system/scratch object — column not semantically documented. |
| `extent` | USER-DEFINED | YES | Excluded system/scratch object — column not semantically documented. |
| `spatial_index` | boolean | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` lines 2093-2114 (structure); not application data — confirmed by exclusion analysis.











### public.raster_overviews (view)





- **Excluded:** yes — PostGIS system catalog view for raster overviews; maintained by PostGIS, not by the application.





- **Reason:** system catalog metadata; zero application code references.





- **Columns:** 9 (`o_table_catalog` ... `overview_factor`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `o_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `overview_factor` | integer | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `o_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `o_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_catalog` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_schema` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_table_name` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `r_raster_column` | name | YES | Excluded system/scratch object — column not semantically documented. |
| `overview_factor` | integer | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` lines 2115-2126 (structure); not application data — confirmed by exclusion analysis.











### public.spatial_ref_sys (table)





- **Excluded:** yes — PostGIS spatial reference system table; maintained by PostGIS, not by the application.





- **Reason:** reference/system table; zero application code references.





- **Columns:** 5 (`srid`, `auth_name`, `auth_srid`, `srtext`, `proj4text`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `srid` | integer | NO | Excluded system/scratch object — column not semantically documented. |
| `auth_name` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `auth_srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srtext` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `proj4text` | character varying | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `srid` | integer | NO | Excluded system/scratch object — column not semantically documented. |
| `auth_name` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `auth_srid` | integer | YES | Excluded system/scratch object — column not semantically documented. |
| `srtext` | character varying | YES | Excluded system/scratch object — column not semantically documented. |
| `proj4text` | character varying | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` lines 2195-2206 (structure); not application data — confirmed by exclusion analysis.











### public.pg_stat_statements (view)





- **Excluded:** yes — PostgreSQL system statistics view of executed SQL statements; maintained by the `pg_stat_statements` extension, not by the application.





- **Reason:** system statistics view; statement text may contain application SQL but the view itself is not application data; zero direct application code references.





- **Columns:** 52 (`userid` ... `minmax_stats_since`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `userid` | oid | YES | Excluded system/scratch object — column not semantically documented. |
| `dbid` | oid | YES | Excluded system/scratch object — column not semantically documented. |
| `toplevel` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `queryid` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `query` | text | YES | Excluded system/scratch object — column not semantically documented. |
| `plans` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `total_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `min_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `max_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `mean_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `stddev_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `calls` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `total_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `min_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `max_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `mean_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `stddev_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `rows` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_hit` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_dirtied` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_hit` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_dirtied` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_records` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_fpi` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_bytes` | numeric | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_buffers_full` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_functions` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_generation_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_inlining_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_inlining_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_optimization_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_optimization_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_emission_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_emission_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_deform_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_deform_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `parallel_workers_to_launch` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `parallel_workers_launched` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `stats_since` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |
| `minmax_stats_since` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `userid` | oid | YES | Excluded system/scratch object — column not semantically documented. |
| `dbid` | oid | YES | Excluded system/scratch object — column not semantically documented. |
| `toplevel` | boolean | YES | Excluded system/scratch object — column not semantically documented. |
| `queryid` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `query` | text | YES | Excluded system/scratch object — column not semantically documented. |
| `plans` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `total_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `min_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `max_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `mean_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `stddev_plan_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `calls` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `total_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `min_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `max_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `mean_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `stddev_exec_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `rows` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_hit` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_dirtied` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_hit` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_dirtied` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blks_read` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blks_written` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `shared_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `local_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blk_read_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `temp_blk_write_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_records` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_fpi` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_bytes` | numeric | YES | Excluded system/scratch object — column not semantically documented. |
| `wal_buffers_full` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_functions` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_generation_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_inlining_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_inlining_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_optimization_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_optimization_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_emission_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_emission_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_deform_count` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `jit_deform_time` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `parallel_workers_to_launch` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `parallel_workers_launched` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `stats_since` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |
| `minmax_stats_since` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` (structure); not application data — confirmed by exclusion analysis.











### public.pg_stat_statements_info (view)





- **Excluded:** yes — PostgreSQL system view with `pg_stat_statements` internal allocation/reset info; maintained by the extension, not by the application.





- **Reason:** system statistics view; zero application code references.





- **Columns:** 2 (`dealloc`, `stats_reset`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `dealloc` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `stats_reset` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `dealloc` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `stats_reset` | timestamp with time zone | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` (structure); not application data — confirmed by exclusion analysis.











### public.staging.temp_bc_import (table)





- **Excluded:** yes — scratch/import table qualified under `public` in the generated inventory; column names (`Weight`, `BMI`, `Body Fat`, `Skeletal Muscle Mass`, `Bone Mass`, `Body Water`) suggest bioimpedance-scale import rows with a text `ts_utc`.





- **Reason:** temporary scratch/import working table; sibling object `staging.temp_bc_import` exists in the `staging` schema and is likewise scratch.





- **Columns:** 8 (`ROW_ID`, `ts_utc`, `Weight`, `BMI`, `Body Fat`, `Skeletal Muscle Mass`, `Bone Mass`, `Body Water`).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `ROW_ID` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `ts_utc` | text | YES | Excluded system/scratch object — column not semantically documented. |
| `Weight` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `BMI` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Body Fat` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Skeletal Muscle Mass` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Bone Mass` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Body Water` | double precision | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `ROW_ID` | bigint | YES | Excluded system/scratch object — column not semantically documented. |
| `ts_utc` | text | YES | Excluded system/scratch object — column not semantically documented. |
| `Weight` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `BMI` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Body Fat` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Skeletal Muscle Mass` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Bone Mass` | double precision | YES | Excluded system/scratch object — column not semantically documented. |
| `Body Water` | double precision | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` line 2207 (structure); column meaning not corroborated by code (0 references).











### public.v_end_elev (table)





- **Excluded:** yes — single-column scratch table holding a scalar `smoothed_elev` result; working storage for elevation-smoothing procedure(s), not application data.





- **Reason:** scratch work table; zero application code references; no `INSERT INTO public.v_end_elev` found in captured routine DDL (same-named PL/pgSQL local variables are used instead).





- **Columns:** 1 (`smoothed_elev` double precision).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `smoothed_elev` | double precision | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `smoothed_elev` | double precision | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` line 2219 (structure); `docs/data/_evidence/database/schema_only.sql` (table DDL; `activities.smooth_elevation_spikes_by_distance` procedure). Scratch status inferred — persistent-table write path not evidenced.











### public.v_is_loop (table)





- **Excluded:** yes — single-column scratch table holding a boolean loop-detection result for elevation smoothing.





- **Reason:** scratch work table; zero application code references; no persistent-table write path evidenced.





- **Columns:** 1 (`?column?` boolean — unnamed column artifact).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `?column?` | boolean | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `?column?` | boolean | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` line 2224 (structure); `schema_only.sql` table DDL (line 19900).











### public.v_start_elev (table)





- **Excluded:** yes — single-column scratch table holding a scalar `smoothed_elev` result for elevation smoothing.





- **Reason:** scratch work table; zero application code references; no persistent-table write path evidenced.





- **Columns:** 1 (`smoothed_elev` double precision).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `smoothed_elev` | double precision | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `smoothed_elev` | double precision | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` line 2229 (structure); `schema_only.sql` table DDL (line 19909).











### public.v_total_distance (table)





- **Excluded:** yes — single-column scratch table holding a scalar `max` (total distance) result for elevation smoothing.





- **Reason:** scratch work table; zero application code references; no persistent-table write path evidenced.





- **Columns:** 1 (`max` integer).

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `max` | integer | YES | Excluded system/scratch object — column not semantically documented. |

| Column | Type | Nullable | Meaning / semantics |
|---|---|---|---|
| `max` | integer | YES | Excluded system/scratch object — column not semantically documented. |





- **Evidence/confidence:** `schema.generated.md` line 2234 (structure); `schema_only.sql` table DDL (line 19918).











| `relative_year` | integer | YES | Years from query date | integer | year arithmetic vs anchor | definitions.sql | confirmed |

















- **Evidence/confidence:** `schema.generated.md` lines 2034-2052; `docs/data/_evidence/database/catalog.json`; `docs/data/_evidence/database/definitions.sql`; `docs/data/_evidence/repo_references.json`. Structural facts confirmed; write path unknown (absence inferred from code search).











## staging











### staging.activity_metric_descriptors (table)





- **Purpose:** per-activity metric descriptor definitions (name + unit) extracted from activity detail JSON during flatten.





- **Grain:** one row per `(activity_id, metric_idx)` metric descriptor.





- **Canonical source:** `staging.activity_metric_descriptors`; populated from `staging.activity_payload.payload` JSON (`metricDescriptors` array) by the flatten procedure (`schema_only.sql:4442-4446`).





- **Consumers:** `activity_metrics_flat` / `activity_metrics_matched` reference metric names/units.





- **Relationships:** `enforced` composite PK `(activity_id, metric_idx)` (`activity_metric_descriptors_pkey`).





- **Rules:** `enforced` composite PK; derived working table (rebuilt per activity flatten run).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | PK part | schema.generated.md:2301-2304 | confirmed |





| `metric_idx` | smallint | NO | Metric index within activity | ordinal | PK part | schema.generated.md:2305 | confirmed |





| `metric_name` | text | YES | Metric name | text | `payload->'metricDescriptors'` name | schema.generated.md:2306 | confirmed |





| `metric_unit` | text | YES | Metric unit | text | `payload->'metricDescriptors'->'unit'->'key'` | schema.generated.md:2307 | confirmed |





| `activity_type_id` | smallint | YES | Activity type id | id | source | schema.generated.md:2308 | confirmed |











### staging.activity_metrics_flat (table)





- **Purpose:** flattened per-activity-detail metric values (one value per activity/detail/metric index) extracted from activity detail JSON.





- **Grain:** one row per `(activity_id, activity_detail_index, metric_index)` metric sample.





- **Canonical source:** `staging.activity_metrics_flat`; populated from `staging.activity_payload.payload` JSON (`activityDetailMetrics` array) by the flatten procedure (`schema_only.sql:4457-4461`).





- **Relationships:** `enforced` composite PK `(activity_id, activity_detail_index, metric_index)` (`activity_metrics_flat_pkey`); index `idx_activity_metrics_activity` on `activity_id`.





- **Rules:** `enforced` composite PK; derived working table (rebuilt per activity flatten run); `metric_value` stored as text (raw, not typed).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | PK part | schema.generated.md:2313-2316 | confirmed |





| `activity_detail_index` | integer | NO | Activity detail sample index | ordinal | PK part | schema.generated.md:2317 | confirmed |





| `metric_index` | integer | NO | Metric index | ordinal | PK part | schema.generated.md:2318 | confirmed |





| `metric_value` | text | YES | Metric raw value | text (raw) | `payload->'activityDetailMetrics'` value | schema.generated.md:2319 | confirmed |











### staging.activity_metrics_matched (table)





- **Purpose:** matched metric samples keyed to known metric descriptors (name/unit) per activity detail sample, produced by matching `activity_metrics_flat` to `activity_metric_descriptors`.





- **Grain:** one row per `(activity_id, activity_detail_index)` matched metric sample.





- **Canonical source:** `staging.activity_metrics_matched`; derived from `activity_payload`/`activity_metrics_flat` during flatten; `activity_id` references `activity_payload.activity_id` ON DELETE CASCADE.





- **Relationships:** `enforced` FK `activity_id` -> `staging.activity_payload.activity_id` ON DELETE CASCADE (`activity_metrics_matched_activity_id_fkey`).





- **Rules:** no PK declared; working table rebuilt per activity flatten run; `metric_value` stored as text.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | FK to `activity_payload.activity_id` ON DELETE CASCADE | schema.generated.md:2325-2327 | confirmed |





| `activity_detail_index` | integer | YES | Activity detail sample index | ordinal | derived | schema.generated.md:2328 | confirmed |





| `metric_name` | text | YES | Matched metric name | text | derived from descriptors | schema.generated.md:2329 | confirmed |





| `metric_value` | text | YES | Matched metric value | text (raw) | derived | schema.generated.md:2330 | confirmed |





| `activity_type_id` | smallint | YES | Activity type id | id | derived | schema.generated.md:2331 | confirmed |











### staging.activity_payload (table)





- **Purpose:** raw Garmin activity detail JSON payloads held during the flatten/extract stage before they are written to `activities.activity_details`.





- **Grain:** one row per activity, keyed by `activity_id`.





- **Canonical source:** `staging.activity_payload` (UNLOGGED table); `TRUNCATE ... CASCADE` then `INSERT (activity_id, payload)` by `flatten_activity_details` (`schema_only.sql:4415-4419`); referenced by `staging.activity_metrics_matched` via `ON DELETE CASCADE` FK.





- **Consumers:** `flatten_activity_details` writes `activities.activity_details` from it; `activity_metric_descriptors` / `activity_metrics_flat` / `activity_metrics_matched` are derived from its `payload` JSON.





- **Relationships:** `enforced` PK `activity_id`; `activity_metrics_matched.activity_id` -> `activity_payload.activity_id` ON DELETE CASCADE.





- **Rules:** `enforced` PK; `payload` jsonb nullable; no application write path evidenced (SQL procedure only).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | PK; source row key | schema.generated.md:2338-2341 | confirmed |





| `payload` | jsonb | YES | Raw activity detail JSON | JSON document | source JSON from Garmin sync | schema.generated.md:2341 | confirmed |











### staging.api_imports (table)





- **Purpose:** staging landing table for raw Spotify/Garmin API JSON responses fetched by task jobs, keyed by `load_id`; consumed by upsert procedures that populate `music.playlist_config`, `music.playlist_isrcs`, `health.*` raw tables, etc., then deleted after processing.





- **Grain:** one row per API import job payload, keyed by `load_id`.





- **Canonical source:** `staging.api_imports`; written by `backend/pifitness/worker/task_execution.py:434` (`INSERT (api_function_name, payload)`) and `backend/pifitness/worker/ultimate_task_executioner_v2.py:738` (`INSERT (task_id, payload)`); read by `docs/data/_evidence/database/definitions.sql` upsert procedures (`playlist_config` at :5631, `playlist_isrcs` at :5541, health raw tables at :25752/:25802, `json_examples` at :25840); deleted via `DELETE FROM staging.api_imports WHERE task_id = ...` after processing.





- **Consumers:** `music.playlist_config`, `music.playlist_isrcs`, `health.*` raw tables, `staging.json_examples`.





- **Relationships:** `enforced` PK `load_id` (`api_imports_pkey`).





- **Rules:** `enforced` PK; `payload` jsonb NOT NULL; `task_id` nullable FK-like link to tasks. Staging/working table — rows are transient (inserted then deleted per job).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `load_id` | bigint | NO | Import load id | id | PK; insert path | schema.generated.md:2347-2350 | confirmed |





| `event_time_utc` | timestamp with time zone | YES | Event timestamp | UTC timestamptz | insert/default | schema.generated.md:2350 | confirmed |





| `api_function_name` | text | YES | API function name | text | `task_execution.py` param | schema.generated.md:2351 | confirmed |





| `payload` | jsonb | NO | Raw API response JSON | JSON document | NOT NULL; insert path | schema.generated.md:2352 | confirmed |





| `task_id` | integer | YES | Related task id | id | `ultimate_task_executioner_v2.py` param | schema.generated.md:2353 | confirmed |











### staging.bounds (table)





- **Purpose:** singleton time-bounds row holding `min_ts`/`max_ts` for the activity time range (acts as a flag row via `id` boolean).





- **Grain:** exactly one row (`id` defaults to `true` and is NOT NULL — singleton pattern).





- **Canonical source:** `staging.bounds`; write path not evidenced in application code searched (schema/DDL only).





- **Relationships:** `enforced` PK `id` (`bounds_pkey`).





- **Rules:** `enforced` PK; `id` boolean DEFAULT true NOT NULL (enforces singleton via application convention); `min_ts`/`max_ts` nullable timestamptz.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `id` | boolean | NO | Singleton flag (always true) | boolean | DEFAULT true; PK | schema.generated.md:2358-2360; schema_only.sql:20246-20248 | confirmed |





| `min_ts` | timestamp with time zone | YES | Range start timestamp | UTC timestamptz | write path not evidenced | schema.generated.md:2361 | confirmed |





| `max_ts` | timestamp with time zone | YES | Range end timestamp | UTC timestamptz | write path not evidenced | schema.generated.md:2362 | confirmed |











### staging.interpolation_anchors (table)





- **Purpose:** per-timestamp interpolation anchor points (previous/next known values) produced by `build_anchors()` from `staging.interpolation_load_values`, used to fill gaps in time series.





- **Grain:** one row per timestamp, keyed by `ts_utc`.





- **Canonical source:** `staging.interpolation_anchors`; `TRUNCATE` + rebuild by procedure `staging.build_anchors()` (`schema_only.sql:4065-4084` / `definitions.sql:24839-24858`).





- **Dependencies:** `staging.interpolation_load_values` (source); forward/backward window functions locate `prev_ts`/`next_ts`.





- **Relationships:** `enforced` PK `ts_utc` (`interpolation_anchors_pkey`).





- **Rules:** `enforced` PK; working table rebuilt per run (truncate + insert).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Anchor timestamp | UTC timestamptz | PK; from load_values | schema.generated.md:2368-2370 | confirmed |





| `load_value` | numeric | YES | Value at this timestamp | numeric | `interpolation_load_values.load_value` | schema.generated.md:2371 | confirmed |





| `prev_value` | numeric | YES | Previous known value | numeric | forward_pass value at prev_ts | schema.generated.md:2372 | confirmed |





| `prev_ts` | timestamp with time zone | YES | Previous known timestamp | UTC timestamptz | forward_pass window | schema.generated.md:2373 | confirmed |





| `next_value` | numeric | YES | Next known value | numeric | backward_pass value at next_ts | schema.generated.md:2374 | confirmed |





| `next_ts` | timestamp with time zone | YES | Next known timestamp | UTC timestamptz | backward_pass window | schema.generated.md:2375 | confirmed |











### staging.interpolation_load_values (table)





- **Purpose:** raw per-timestamp load values fed to `build_anchors()` to compute interpolation anchors (`prev`/`next` known values).





- **Grain:** one row per timestamp, keyed by `ts_utc`.





- **Canonical source:** `staging.interpolation_load_values`; write path not evidenced in application code searched (populated by import/ETL outside searched code).





- **Relationships:** `enforced` PK `ts_utc` (`interpolation_load_values_pkey`); consumed by `staging.build_anchors()`.





- **Rules:** `enforced` PK; `load_value` nullable numeric.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | NO | Sample timestamp | UTC timestamptz | PK | schema.generated.md:2381-2383 | confirmed |





| `load_value` | numeric | YES | Raw load value | numeric | source measurement | schema.generated.md:2384 | confirmed |











### staging.isrc_dupe_pairs (table)





- **Purpose:** candidate ISRC duplicate pairs (two ISRCs referring to possibly the same track) produced by `isrc_duplicate_finder()` from `staging.isrc_dupe_seeds`, with trgm indexes on track names for fuzzy matching.





- **Grain:** one row per candidate pair `(isrc1, isrc2)`; no PK declared.





- **Canonical source:** `staging.isrc_dupe_pairs`; `TRUNCATE` + `INSERT ... SELECT` by `music.isrc_duplicate_finder()` (`schema_only.sql`/catalog JSON procedure def) joining two seeds on similarity/duration/artist/album thresholds.





- **Consumers:** duplicate-review workflow feeding `music.isrc_swaps` / `music.isrc_non_swaps`.





- **Relationships:** none declared; trgm indexes `trgm_idx_pair_track1`/`trgm_idx_pair_track2` support fuzzy search.





- **Rules:** no PK/index uniqueness declared; working table rebuilt per duplicate-finder run.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc1` | text | YES | First ISRC | ISRC code | `isrc_duplicate_finder()` | schema.generated.md:2391 | confirmed |





| `isrc2` | text | YES | Second ISRC | ISRC code | `isrc_duplicate_finder()` | schema.generated.md:2392 | confirmed |





| `track1` | text | YES | First track name | text | seed track_name_search | schema.generated.md:2393 | confirmed |





| `track2` | text | YES | Second track name | text | seed track_name_search | schema.generated.md:2394 | confirmed |





| `artist1` | text | YES | First artist name | text | seed artist_name_search | schema.generated.md:2395 | confirmed |





| `artist2` | text | YES | Second artist name | text | seed artist_name_search | schema.generated.md:2396 | confirmed |





| `artist_id1` | text | YES | First artist id | text | seed artist_id | schema.generated.md:2397 | confirmed |





| `artist_id2` | text | YES | Second artist id | text | seed artist_id | schema.generated.md:2398 | confirmed |





| `album1` | text | YES | First album name | text | seed album_name_search | schema.generated.md:2399 | confirmed |





| `album2` | text | YES | Second album name | text | seed album_name_search | schema.generated.md:2400 | confirmed |





| `album_id1` | text | YES | First album id | text | seed album_id | schema.generated.md:2401 | confirmed |





| `album_id2` | text | YES | Second album id | text | seed album_id | schema.generated.md:2402 | confirmed |





| `duration1` | integer | YES | First track duration | milliseconds | seed duration_ms | schema.generated.md:2403 | confirmed |





| `duration2` | integer | YES | Second track duration | milliseconds | seed duration_ms | schema.generated.md:2404 | confirmed |











### staging.isrc_dupe_seeds (table)





- **Purpose:** seed rows (one per track) used by `isrc_duplicate_finder()` to build candidate ISRC duplicate pairs; populated from `music.all_tracks` joined to artists/albums/best-track.





- **Grain:** one row per track ISRC, keyed by `isrc`.





- **Canonical source:** `staging.isrc_dupe_seeds`; `TRUNCATE` + insert by `music.isrc_duplicate_finder()` (`schema_only.sql`/catalog JSON procedure def); clustered on `duration_ms` via `idx_isrc_seeds_duration_gist`.





- **Consumers:** `isrc_duplicate_finder()` builds `isrc_dupe_pairs` from it.





- **Relationships:** `enforced` PK `isrc` (`isrc_dupe_seeds_pkey`); trgm index `trgm_idx_track_seeds` on `track_name_search`.





- **Rules:** `enforced` PK; working table rebuilt per run (truncate + insert + ANALYZE + CLUSTER).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `isrc` | text | NO | Track ISRC | ISRC code | PK; `all_tracks.track_isrc` | schema.generated.md:2412-2414 | confirmed |





| `track_name_search` | text | YES | Search-optimized track name | text | `all_tracks.track_name_search` | schema.generated.md:2415 | confirmed |





| `artist_name_search` | text | YES | Search-optimized artist name | text | `all_artists.artist_name_search` | schema.generated.md:2416 | confirmed |





| `album_name_search` | text | YES | Search-optimized album name | text | `all_albums.album_name_search` | schema.generated.md:2417 | confirmed |





| `artist_id` | text | YES | Artist id | text | `all_tracks.artist_id` | schema.generated.md:2418 | confirmed |





| `album_id` | text | YES | Album id | text | `all_tracks.album_id` | schema.generated.md:2419 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | `all_tracks.duration_ms`; used for clustering | schema.generated.md:2420 | confirmed |





| `prior_comparison` | text | YES | Prior comparison tag | text | free text | schema.generated.md:2421 | confirmed |











### staging.json_examples (table)





- **Purpose:** curated JSON sample documents keyed by raw table name, used as reference fixtures for API payload shapes.





- **Grain:** one row per raw table name, keyed by `raw_table_name`.





- **Canonical source:** `staging.json_examples`; written by `definitions.sql:25838-25842` from `staging.api_imports` (`SELECT payload, CURRENT_TIMESTAMP ... ORDER BY event_time_utc DESC LIMIT 1 WHERE task_id = v_task_id`).





- **Consumers:** API payload reference/diagnostics (no direct application consumer evidenced).





- **Relationships:** `enforced` PK `raw_table_name` (`json_examples_pkey`).





- **Rules:** `enforced` PK; `json_sample` jsonb nullable; `updated_at_utc` DEFAULT CURRENT_TIMESTAMP.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `raw_table_name` | text | NO | Raw table identifier | text | PK | schema.generated.md:2430-2433 | confirmed |





| `json_sample` | jsonb | YES | Sample JSON document | JSON document | `api_imports.payload` | schema.generated.md:2433 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Last refresh timestamp | UTC timestamptz | CURRENT_TIMESTAMP | schema.generated.md:2434 | confirmed |











### staging.stg_listen_history (table)





- **Purpose:** raw Spotify listening-history staging rows (track + album + artist + play context + played_at) loaded from API imports before insertion into `music.listening_history`.





- **Grain:** one row per `(track_id, played_at_utc)` listen event.





- **Canonical source:** `staging.stg_listen_history`; TRUNCATED then repopulated by `definitions.sql` import procedures (`:26075` `TRUNCATE staging.stg_listen_history`).





- **Consumers:** `music.listening_history` insert path; `vw_album_inserts`/`vw_artist_inserts` reference it for dedup; `vw_listening_history_inserts`, `vw_track_inserts_from_listen_history` derive from it.





- **Relationships:** `enforced` composite PK `(track_id, played_at_utc)` (`stg_listen_history_pkey`); relates to `music.all_tracks.track_id` / `all_albums.album_id` / `all_artists.artist_id` at application level.





- **Rules:** `enforced` composite PK; `played_at_utc` NOT NULL; `incoming_isrc` holds the raw ISRC that may later be normalized via `isrc_swaps`; `context_type`/`context_uri` describe the playback context.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | NO | Track id | Spotify track id | PK part | schema.generated.md:2440-2443 | confirmed |





| `track_name` | text | YES | Track name | text | source | schema.generated.md:2444 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2445 | confirmed |





| `album_name` | text | YES | Album name | text | source | schema.generated.md:2446 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2447 | confirmed |





| `artist_name` | text | YES | Artist name | text | source | schema.generated.md:2448 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | source | schema.generated.md:2449 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | source | schema.generated.md:2450 | confirmed |





| `incoming_isrc` | text | YES | Raw ISRC from API | ISRC code | source | schema.generated.md:2451 | confirmed |





| `available_market_count` | integer | YES | Market count | count | source | schema.generated.md:2452 | confirmed |





| `available_in_usa` | boolean | YES | Available in US flag | boolean | source | schema.generated.md:2453 | confirmed |





| `context_type` | text | YES | Playback context type | text | source | schema.generated.md:2454 | confirmed |





| `context_uri` | text | YES | Playback context URI | text | source | schema.generated.md:2455 | confirmed |





| `played_at_utc` | timestamp with time zone | NO | Play timestamp | UTC timestamptz | PK part | schema.generated.md:2456 | confirmed |





| `track_release` | text | YES | Release date | calendar date text | source | schema.generated.md:2457 | confirmed |





| `track_release_precision` | text | YES | Release precision | text | source | schema.generated.md:2458 | confirmed |











### staging.stg_now_playing (table)





- **Purpose:** raw Spotify "now playing"/recent-play staging rows with track, album, artist, market, context, and release fields.





- **Grain:** one row per `(track_id, played_at_utc)` play event.





- **Canonical source:** `staging.stg_now_playing`; write path via API import staging (not individually evidenced in application code searched).





- **Relationships:** `enforced` composite PK `(track_id, played_at_utc)` (`stg_now_playing_pk`).





- **Rules:** `enforced` composite PK; `played_at_utc` NOT NULL; `available_in_usa` boolean flag; `incoming_isrc` holds the raw ISRC from the API response.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | NO | Track id | Spotify track id | PK part | schema.generated.md:2463-2465 | confirmed |





| `track_name` | text | YES | Track name | text | source | schema.generated.md:2466 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2467 | confirmed |





| `album_name` | text | YES | Album name | text | source | schema.generated.md:2468 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2469 | confirmed |





| `artist_name` | text | YES | Artist name | text | source | schema.generated.md:2470 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | source | schema.generated.md:2471 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | source | schema.generated.md:2472 | confirmed |





| `incoming_isrc` | text | YES | Raw ISRC from API | ISRC code | source | schema.generated.md:2473 | confirmed |





| `available_market_count` | integer | YES | Market count | count | source | schema.generated.md:2474 | confirmed |





| `available_in_usa` | boolean | YES | Available in US flag | boolean | source | schema.generated.md:2475 | confirmed |





| `context_type` | text | YES | Playback context type | text | source | schema.generated.md:2476 | confirmed |





| `context_uri` | text | YES | Playback context URI | text | source | schema.generated.md:2477 | confirmed |





| `played_at_utc` | timestamp with time zone | NO | Play timestamp | UTC timestamptz | PK part | schema.generated.md:2478 | confirmed |





| `track_release` | text | YES | Release date | calendar date text | source | schema.generated.md:2479 | confirmed |





| `track_release_precision` | text | YES | Release precision | text | source | schema.generated.md:2480 | confirmed |











### staging.stg_playlist_details (table)





- **Purpose:** raw Spotify playlist-track details staging rows (playlist, track, order, album, artist, release, markets) loaded from `api_imports` before insertion into `music.playlist_isrcs`.





- **Grain:** one row per `(playlist_id, track_id, track_order)` membership row.





- **Canonical source:** `staging.stg_playlist_details`; populated by `definitions.sql` playlist import procedure (`:5598-5633` `INSERT INTO music.playlist_config ... FROM staging.api_imports src` then `stg_playlist_details` loaded at `:5541`); `DELETE FROM staging.stg_playlist_details` after processing (`:5593`).





- **Consumers:** `music.playlist_isrcs` insert path; `vw_album_inserts`/`vw_artist_inserts` dedup reference it.





- **Relationships:** no declared FKs; relates to `playlist_config.playlist_id` / `all_tracks.track_id` at application level.





- **Rules:** staging/working table; `fetched_at_utc` marks when the page was fetched; `incoming_isrc` is the raw ISRC that may later be normalized via `isrc_swaps`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | Spotify playlist id | source | schema.generated.md:2487 | confirmed |





| `track_id` | text | YES | Track id | Spotify track id | source | schema.generated.md:2489 | confirmed |





| `track_name` | text | YES | Track name | text | source | schema.generated.md:2490 | confirmed |





| `track_order` | integer | YES | Position in playlist | ordinal | source | schema.generated.md:2491 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2492 | confirmed |





| `album_name` | text | YES | Album name | text | source | schema.generated.md:2493 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2494 | confirmed |





| `artist_name` | text | YES | Artist name | text | source | schema.generated.md:2495 | confirmed |





| `track_release` | text | YES | Release date | calendar date text | source | schema.generated.md:2496 | confirmed |





| `track_release_precision` | text | YES | Release precision | text | source | schema.generated.md:2497 | confirmed |





| `track_markets` | text | YES | Markets list | text | source | schema.generated.md:2498 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | source | schema.generated.md:2499 | confirmed |





| `incoming_isrc` | text | YES | Raw ISRC from API | ISRC code | source | schema.generated.md:2500 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | source | schema.generated.md:2501 | confirmed |





| `fetched_at_utc` | timestamp with time zone | YES | Fetch timestamp | UTC timestamptz | source | schema.generated.md:2502 | confirmed |











### staging.temp_activity_locations (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only, no full column semantics.





- **Purpose:** scratch location points used by the elevation-smoothing/segment-matching pipeline; columns include `latitude`, `longitude`, `loc_point`, `elevation_m_smooth`, and `grid_5m_*` USER-DEFINED grid cells (see `schema.generated.md`).





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `latitude` | numeric | YES | Latitude | degrees | scratch | schema.generated.md:2505-2507 | unknown |





| `longitude` | numeric | YES | Longitude | degrees | scratch | schema.generated.md:2506 | unknown |





| `loc_point` | USER-DEFINED | YES | Location point | geometry/geography | scratch | schema.generated.md:2507 | unknown |





| `elevation_m_smooth` | numeric | YES | Smoothed elevation | metres | scratch | schema.generated.md:2508 | unknown |





| `grid_5m_00` | USER-DEFINED | YES | Grid cell | USER-DEFINED | scratch | schema.generated.md:2509 | unknown |





| `grid_5m_25_00` | USER-DEFINED | YES | Grid cell | USER-DEFINED | scratch | schema.generated.md:2510 | unknown |





| `grid_5m_00_25` | USER-DEFINED | YES | Grid cell | USER-DEFINED | scratch | schema.generated.md:2511 | unknown |





| `grid_5m_25_25` | USER-DEFINED | YES | Grid cell | USER-DEFINED | scratch | schema.generated.md:2512 | unknown |











### staging.temp_activity_matched_locations (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch matched location pairs used by elevation-smoothing/segment matching; columns `latitude`, `longitude`, `loc_point`, `loc_point_matched`, `elevation_m_smooth` (see `schema.generated.md`).





- **Rules:** no declared PK; btree indexes on `(latitude, longitude)` and `(loc_point, loc_point_matched)`; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `latitude` | numeric | YES | Latitude | degrees | scratch | schema.generated.md:2517-2519 | unknown |





| `longitude` | numeric | YES | Longitude | degrees | scratch | schema.generated.md:2518 | unknown |





| `loc_point` | USER-DEFINED | YES | Location point | geometry/geography | scratch | schema.generated.md:2519 | unknown |





| `loc_point_matched` | USER-DEFINED | YES | Matched location point | geometry/geography | scratch | schema.generated.md:2520 | unknown |





| `elevation_m_smooth` | numeric | YES | Smoothed elevation | metres | scratch | schema.generated.md:2521 | unknown |











### staging.temp_activity_metas (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch per-activity metadata used by the activity-smoothing pipeline (`activity_id`, `activity_type_id`, `activity_path`).





- **Rules:** `enforced` PK `activity_id` (`temp_activity_metas_pkey`); transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | PK | schema.generated.md:2530-2533 | unknown |





| `activity_type_id` | smallint | YES | Activity type id | id | scratch | schema.generated.md:2534 | unknown |





| `activity_path` | USER-DEFINED | YES | Activity path | geometry/geography | scratch | schema.generated.md:2535 | unknown |











### staging.temp_activity_trackpoints (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch per-activity track points (longitude/latitude/elevation) used by elevation smoothing; columns `activity_id`, `track_point`, `longitude`, `latitude`, `elevation_m`, `activity_type_id`.





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md:2540-2543 | unknown |





| `track_point` | integer | YES | Track point index | ordinal | scratch | schema.generated.md:2544 | unknown |





| `longitude` | numeric | YES | Longitude | degrees | scratch | schema.generated.md:2545 | unknown |





| `latitude` | numeric | YES | Latitude | degrees | scratch | schema.generated.md:2546 | unknown |





| `elevation_m` | numeric | YES | Elevation | metres | scratch | schema.generated.md:2547 | unknown |





| `activity_type_id` | smallint | YES | Activity type id | id | scratch | schema.generated.md:2548 | unknown |











### staging.temp_bc_import (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables / public scratch `temp_bc_import` pattern); existence documented only.





- **Purpose:** scratch body-composition import staging (columns `Weight`, `BMI`, `Body Fat`, `Skeletal Muscle Mass`, `Bone Mass`, `Body Water` plus `ROW_ID`, `ts_utc`) referenced in the excluded-objects register; no persistent application write path evidenced in searched code.





- **Rules:** no declared PK; transient scratch table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ROW_ID` | bigint | YES | Row id | id | scratch | schema.generated.md:2550-2553 | unknown |





| `ts_utc` | text | YES | Timestamp (text) | text | scratch | schema.generated.md:2554 | unknown |





| `Weight` | double precision | YES | Body weight | unknown units | scratch | schema.generated.md:2555 | unknown |





| `BMI` | double precision | YES | BMI | unknown units | scratch | schema.generated.md:2556 | unknown |





| `Body Fat` | double precision | YES | Body fat | unknown units | scratch | schema.generated.md:2557 | unknown |





| `Skeletal Muscle Mass` | double precision | YES | Skeletal muscle mass | unknown units | scratch | schema.generated.md:2558 | unknown |





| `Bone Mass` | double precision | YES | Bone mass | unknown units | scratch | schema.generated.md:2559 | unknown |





| `Body Water` | double precision | YES | Body water | unknown units | scratch | schema.generated.md:2560 | unknown |











### staging.temp_csv_import (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch CSV health-import staging with text columns for sleep score, resting HR, body battery, pulse ox, respiration, HRV status, quality, duration, bedtime, wake time (see `schema.generated.md`).





- **Rules:** no declared PK; transient scratch table; column names are raw CSV headers (units not established).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `Sleep Score 4 Weeks` | text | YES | Sleep score | text | scratch CSV header | schema.generated.md:2562 | unknown |





| `Score` | text | YES | Score | text | scratch CSV header | schema.generated.md:2563 | unknown |





| `Resting Heart Rate` | text | YES | Resting HR | text | scratch CSV header | schema.generated.md:2564 | unknown |





| `Body Battery` | text | YES | Body battery | text | scratch CSV header | schema.generated.md:2565 | unknown |





| `Pulse Ox` | text | YES | Pulse ox | text | scratch CSV header | schema.generated.md:2566 | unknown |





| `Respiration` | text | YES | Respiration | text | scratch CSV header | schema.generated.md:2567 | unknown |





| `HRV Status` | text | YES | HRV status | text | scratch CSV header | schema.generated.md:2568 | unknown |





| `Quality` | text | YES | Quality | text | scratch CSV header | schema.generated.md:2569 | unknown |





| `Duration` | text | YES | Duration | text | scratch CSV header | schema.generated.md:2570 | unknown |





| `Bedtime` | text | YES | Bedtime | text | scratch CSV header | schema.generated.md:2571 | unknown |





| `Wake Time` | text | YES | Wake time | text | scratch CSV header | schema.generated.md:2572 | unknown |











### staging.temp_elevation_deltas (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch per-activity elevation ascent/descent deltas used by elevation smoothing.





- **Rules:** `enforced` PK `activity_id` (`temp_elevation_deltas_pkey`); transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | NO | Activity id | id | PK | schema.generated.md:2577-2580 | unknown |





| `ascent_m` | numeric | YES | Ascent | metres | scratch | schema.generated.md:2581 | unknown |





| `descent_m` | numeric | YES | Descent | metres | scratch | schema.generated.md:2582 | unknown |











### staging.temp_elevation_medians (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch per-location median elevation used by elevation smoothing (`loc_point` PK, `median_elev`).





- **Rules:** `enforced` PK `loc_point` (`temp_elevation_medians_pkey`); transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `loc_point` | USER-DEFINED | NO | Location point | geometry/geography | PK | schema.generated.md:2587-2589 | unknown |





| `median_elev` | numeric | YES | Median elevation | metres | scratch | schema.generated.md:2590 | unknown |











### staging.temp_elevation_smoothed (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch smoothed per-activity track-point elevations (`activity_id`, `track_point`, `elevation_m`).





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md:2597-2599 | unknown |





| `track_point` | integer | YES | Track point index | ordinal | scratch | schema.generated.md:2600 | unknown |





| `elevation_m` | numeric | YES | Smoothed elevation | metres | scratch | schema.generated.md:2601 | unknown |











### staging.temp_elevation_smoothing (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch elevation-smoothing state keyed by `elapsed_duration_s` (`elapsed_duration_s`, `elevation_m_raw`, `distance_m`).





- **Rules:** `enforced` PK `elapsed_duration_s` (`temp_elevation_smoothing_pkey`); transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `elapsed_duration_s` | numeric | NO | Elapsed duration | seconds | PK | schema.generated.md:2604-2606 | unknown |





| `elevation_m_raw` | numeric | YES | Raw elevation | metres | scratch | schema.generated.md:2607 | unknown |





| `distance_m` | numeric | YES | Distance | metres | scratch | schema.generated.md:2608 | unknown |











### staging.temp_elevation_smoothing_m (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch per-meter elevation smoothing state (`meter`, `elapsed_duration_s`, `elevation_m_raw`, `elevation_m_smooth`).





- **Rules:** `enforced` PK `meter` (`temp_elevation_smoothing_m_pkey`); transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | NO | Meter position | ordinal | PK | schema.generated.md:2614-2616 | unknown |





| `elapsed_duration_s` | numeric | YES | Elapsed duration | seconds | scratch | schema.generated.md:2617 | unknown |





| `elevation_m_raw` | numeric | YES | Raw elevation | metres | scratch | schema.generated.md:2618 | unknown |





| `elevation_m_smooth` | numeric | YES | Smoothed elevation | metres | scratch | schema.generated.md:2619 | unknown |











### staging.temp_isrc_swaps (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch ISRC swap candidates (`original_isrc`, `mapped_isrc`, `mapping_time_utc`, `valid_mapping`) used by the duplicate-finder pipeline.





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `original_isrc` | text | YES | Original ISRC | ISRC code | scratch | schema.generated.md:2625-2628 | unknown |





| `mapped_isrc` | text | YES | Mapped ISRC | ISRC code | scratch | schema.generated.md:2629 | unknown |





| `mapping_time_utc` | timestamp with time zone | YES | Mapping timestamp | UTC timestamptz | scratch | schema.generated.md:2630 | unknown |





| `valid_mapping` | boolean | YES | Valid mapping flag | boolean | scratch | schema.generated.md:2631 | unknown |











### staging.temp_performance_profiling (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch performance-profiling rows (`ts`, `segment`, `code`, `time_ms`) used to time pipeline steps.





- **Rules:** no declared PK; transient scratch table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts` | timestamp with time zone | YES | Sample timestamp | UTC timestamptz | scratch | schema.generated.md:2633-2635 | unknown |





| `segment` | text | YES | Pipeline segment name | text | scratch | schema.generated.md:2636 | unknown |





| `code` | text | YES | Code/step identifier | text | scratch | schema.generated.md:2637 | unknown |





| `time_ms` | integer | YES | Elapsed time | milliseconds | scratch | schema.generated.md:2638 | unknown |











### staging.temp_segment_matches (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch segment-match candidates (`segment_id`, `activity_id`, start/end points, `activity_path`, `match_confirmed`, `match_confidence`, `match_ts_utc`) used by segment matching.





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `segment_id` | integer | YES | Segment id | id | scratch | schema.generated.md:2641-2644 | unknown |





| `activity_id` | bigint | YES | Activity id | id | scratch | schema.generated.md:2645 | unknown |





| `activity_start_point` | integer | YES | Activity start point | ordinal | scratch | schema.generated.md:2646 | unknown |





| `activity_end_point` | integer | YES | Activity end point | ordinal | scratch | schema.generated.md:2647 | unknown |





| `activity_path` | USER-DEFINED | YES | Activity path | geometry/geography | scratch | schema.generated.md:2648 | unknown |





| `match_confirmed` | boolean | YES | Match confirmed flag | boolean | scratch | schema.generated.md:2649 | unknown |





| `match_confidence` | numeric | YES | Match confidence | score | scratch | schema.generated.md:2650 | unknown |





| `match_ts_utc` | timestamp with time zone | YES | Match timestamp | UTC timestamptz | scratch | schema.generated.md:2651 | unknown |











### staging.temp_training_status_raw (table)





- **Status:** `excluded` — temporary working/scratch table (per worklog classification of `staging.temp_*` tables); existence documented only.





- **Purpose:** scratch raw training-status samples (`ts_utc`, vo2 max, altitude/acclimation, heat acclimation, training load acute/min/max) used by health-training-status processing.





- **Rules:** no declared PK; transient working table.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `ts_utc` | timestamp with time zone | YES | Sample timestamp | UTC timestamptz | scratch | schema.generated.md:2653-2656 | unknown |





| `vo2_max_value` | numeric | YES | VO2 max | score points | scratch | schema.generated.md:2657 | unknown |





| `altitude_current_m` | integer | YES | Current altitude | metres | scratch | schema.generated.md:2658 | unknown |





| `altitude_acclimation_m` | integer | YES | Altitude acclimation | metres | scratch | schema.generated.md:2659 | unknown |





| `altitude_acclimation_pct` | numeric | YES | Altitude acclimation pct | percent | scratch | schema.generated.md:2660 | unknown |





| `heat_acclimation_pct` | numeric | YES | Heat acclimation pct | percent | scratch | schema.generated.md:2661 | unknown |





| `training_load_acute` | numeric | YES | Acute training load | score | scratch | schema.generated.md:2662 | unknown |





| `training_load_min` | numeric | YES | Min training load | score | scratch | schema.generated.md:2663 | unknown |





| `training_load_max` | numeric | YES | Max training load | score | scratch | schema.generated.md:2664 | unknown |











### staging.vw_activity_by_distance (view)





- **Purpose:** per-meter interpolated activity elevation/duration series used by the elevation-smoothing pipeline.





- **Grain:** one row per integer meter position (0..ceil(max distance_m)).





- **Canonical source:** derived view over `staging.temp_elevation_smoothing`; uses window `lag()` to get prev_t/prev_elev_raw/prev_d, then `generate_series` over meters and linear interpolation (`elapsed_duration_s`, `elevation_raw`) between bounds, averaged per meter.





- **Dependencies:** `staging.temp_elevation_smoothing`.





- **Consumers:** elevation smoothing pipeline.





- **Rules:** read-only view; `meter`/`elapsed_duration_s`/`elev_raw` derived (interpolated, not raw).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `generate_series` over distance | schema.generated.md:2666-2670 | confirmed |





| `elapsed_duration_s` | numeric | YES | Interpolated elapsed duration | seconds | linear interpolation per meter | schema.generated.md:2667 | confirmed |





| `elev_raw` | numeric | YES | Interpolated raw elevation | metres | linear interpolation per meter | schema.generated.md:2668 | confirmed |











### staging.vw_activity_flatten (view)





- **Purpose:** flattened per-activity-detail sample series (the canonical flattened activity view) with computed location point and smoothed elevation.





- **Grain:** one row per `(activity_id, activity_detail_index)` sample.





- **Canonical source:** derived view over `staging.activity_metrics_matched`; pivots metric_name/metric_value rows into columns via conditional aggregation on `directTimestamp`/`distance`/`heartrate` etc., and computes `loc_point = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography`, `elevation_smooth_m = elevation_m`.





- **Dependencies:** `staging.activity_metrics_matched` (source), `public.st_setsrid`/`st_makepoint` (PostGIS).





- **Consumers:** `activities.activity_details` insert path (`flatten_activity_details`); downstream activity analytics.





- **Rules:** read-only view; `distance_mm`/`speed_mmps` are millimetre/millimetre-per-second raw metric values (text-derived, cast); `loc_point` is PostGIS geography SRID 4326.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `activity_id` | bigint | YES | Activity id | id | `activity_metrics_matched.activity_id` | schema.generated.md:2671 | confirmed |





| `activity_type_id` | smallint | YES | Activity type id | id | `activity_metrics_matched.activity_type_id` | schema.generated.md:2672 | confirmed |





| `activity_detail_index` | integer | YES | Detail sample index | ordinal | `activity_metrics_matched.activity_detail_index` | schema.generated.md:2673 | confirmed |





| `ts_utc` | timestamp with time zone | YES | Sample timestamp | UTC timestamptz | pivoted `directTimestamp` metric | schema.generated.md:2674 | confirmed |





| `moving_duration_s` | integer | YES | Moving duration | seconds | pivoted metric | schema.generated.md:2675 | confirmed |





| `elapsed_duration_s` | integer | YES | Elapsed duration | seconds | pivoted metric | schema.generated.md:2676 | confirmed |





| `duration_s` | integer | YES | Duration | seconds | pivoted metric | schema.generated.md:2677 | confirmed |





| `distance_mm` | bigint | YES | Distance | millimetres | pivoted metric | schema.generated.md:2678 | confirmed |





| `latitude` | numeric | YES | Latitude | degrees | pivoted metric | schema.generated.md:2679 | confirmed |





| `longitude` | numeric | YES | Longitude | degrees | pivoted metric | schema.generated.md:2680 | confirmed |





| `heartrate_bpm` | integer | YES | Heart rate | bpm | pivoted metric | schema.generated.md:2681 | confirmed |





| `cadence_spm` | smallint | YES | Cadence | steps/min | pivoted metric | schema.generated.md:2682 | confirmed |





| `air_temp_c` | smallint | YES | Air temperature | °C | pivoted metric | schema.generated.md:2683 | confirmed |





| `elevation_m` | numeric | YES | Elevation | metres | pivoted metric | schema.generated.md:2684 | confirmed |





| `speed_mmps` | bigint | YES | Speed | mm/s | pivoted metric | schema.generated.md:2685 | confirmed |





| `vertical_speed_mps` | numeric | YES | Vertical speed | m/s | pivoted metric | schema.generated.md:2686 | confirmed |





| `vert_oscillation_cm` | numeric | YES | Vertical oscillation | cm | pivoted metric | schema.generated.md:2687 | confirmed |





| `stride_length_cm` | numeric | YES | Stride length | cm | pivoted metric | schema.generated.md:2688 | confirmed |





| `vert_ratio` | numeric | YES | Vertical ratio | ratio | pivoted metric | schema.generated.md:2689 | confirmed |





| `ground_contact_time_ms` | smallint | YES | Ground contact time | ms | pivoted metric | schema.generated.md:2690 | confirmed |





| `ground_contact_balance_left` | numeric | YES | Ground contact balance left | ratio | pivoted metric | schema.generated.md:2691 | confirmed |





| `performance_condition` | smallint | YES | Performance condition | score | pivoted metric | schema.generated.md:2692 | confirmed |





| `loc_point` | USER-DEFINED | YES | Location point | geography SRID 4326 | `ST_SetSRID(ST_MakePoint(lon,lat),4326)::geography` | schema_only.sql:21992 | confirmed |





| `elevation_smooth_m` | numeric | YES | Smoothed elevation | metres | `elevation_m` passthrough | schema_only.sql:21993 | confirmed |











### staging.vw_album_inserts (view)





- **Purpose:** deduplicated album insert candidates (latest fetched_at_utc per album) with cleaned/search names, feeding `music.all_albums`.





- **Grain:** one row per `album_id` (latest record only, `duplicate_finder = 1`).





- **Canonical source:** derived view over `staging.stg_playlist_details` UNION ALL `staging.stg_listen_history`; `row_number()` dedup by `album_id` ordered by `fetched_at_utc DESC`; `album_name_search` via `public.clean_album_match()`.





- **Dependencies:** `staging.stg_playlist_details`, `staging.stg_listen_history`, `public.clean_album_match`.





- **Consumers:** `music.all_albums` upsert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2700 | confirmed |





| `album_name` | text | YES | Album name | text | source | schema.generated.md:2701 | confirmed |





| `album_name_clean` | text | YES | Cleaned album name | text | `public.clean_track_display(album_name)` | schema.generated.md:2702 | confirmed |





| `album_name_search` | text | YES | Search-optimized album name | text | `public.clean_album_match(album_name_clean)` | schema.generated.md:2703 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Refresh timestamp | UTC timestamptz | `fetched_at_utc` (latest) | schema.generated.md:2704 | confirmed |











### staging.vw_artist_inserts (view)





- **Purpose:** deduplicated artist insert candidates (latest fetched_at_utc per artist) with cleaned/search name, feeding `music.all_artists`.





- **Grain:** one row per `artist_id` (latest record only, `duplicate_finder = 1`).





- **Canonical source:** derived view over `staging.stg_playlist_details` UNION ALL `staging.stg_listen_history`; `row_number()` dedup by `artist_id` ordered by `fetched_at_utc DESC`; `artist_name_search` via `public.clean_artist_match()`.





- **Dependencies:** `staging.stg_playlist_details`, `staging.stg_listen_history`, `public.clean_artist_match`.





- **Consumers:** `music.all_artists` upsert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2709 | confirmed |





| `artist_name` | text | YES | Artist name | text | source | schema.generated.md:2710 | confirmed |





| `artist_name_search` | text | YES | Search-optimized artist name | text | `public.clean_artist_match(artist_name)` | schema.generated.md:2711 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Refresh timestamp | UTC timestamptz | `fetched_at_utc` (latest) | schema.generated.md:2712 | confirmed |











### staging.vw_elevation_backward (view)





- **Purpose:** backward-pass elevation smoothing: adjusts invalid points by comparing to `next_elev` (±2 m), part of the multi-pass elevation-smoothing pipeline.





- **Grain:** one row per invalid point (`valid_point = false`) from `vw_elevation_neighbors`.





- **Canonical source:** derived view over `staging.vw_elevation_neighbors`; `new_elev = next_elev - 2` if `next_elev > curr_elev` else `next_elev + 2`.





- **Dependencies:** `staging.vw_elevation_neighbors`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `vw_elevation_neighbors.meter` | schema.generated.md:2717 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | `vw_elevation_neighbors.curr_elev` | schema.generated.md:2718 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | `next_elev ± 2` case expression | schema_only.sql:22506-22509 | inferred |











### staging.vw_elevation_bumps (view)





- **Purpose:** identifies elevation "bumps" (extreme points) and replaces them with the average elevation of valid neighbors within 500 m.





- **Grain:** one row per extreme point (`extreme_point = true`) from `vw_elevation_neighbors`.





- **Canonical source:** derived view joining `vw_elevation_neighbors` to itself on valid points within 500 m; `new_elev = avg(neighbor curr_elev)`.





- **Dependencies:** `staging.vw_elevation_neighbors`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `vw_elevation_neighbors.meter` | schema.generated.md:2725 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | `vw_elevation_neighbors.curr_elev` | schema.generated.md:2726 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | `avg()` of valid neighbors within 500 m | schema_only.sql:22542 | inferred |











### staging.vw_elevation_forward (view)





- **Purpose:** forward-pass elevation smoothing: adjusts invalid points by comparing to `prior_elev`/`next_elev` (±0.35 m), part of the multi-pass elevation-smoothing pipeline.





- **Grain:** one row per invalid point (`valid_point = false`) from `vw_elevation_neighbors`.





- **Canonical source:** derived view over `staging.vw_elevation_neighbors`; `new_elev = prior_elev - 0.35` if `prior_elev > curr_elev` else `next_elev + 0.35`.





- **Dependencies:** `staging.vw_elevation_neighbors`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `vw_elevation_neighbors.meter` | schema.generated.md:2733 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | `vw_elevation_neighbors.curr_elev` | schema.generated.md:2734 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | `prior_elev/next_elev ± 0.35` case expression | schema_only.sql:22577-22580 | inferred |











### staging.vw_elevation_median (view)





- **Purpose:** median-based elevation smoothing: replaces invalid points with the median elevation of valid neighbors within 45 m (`percentile_disc(0.5)`).





- **Grain:** one row per invalid point (`valid_point = false`) from `vw_elevation_neighbors`.





- **Canonical source:** derived view joining `vw_elevation_neighbors` to itself on valid points within 45 m; `new_elev = percentile_disc(0.5)` of neighbor `curr_elev`.





- **Dependencies:** `staging.vw_elevation_neighbors`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `vw_elevation_neighbors.meter` | schema.generated.md:2739 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | `vw_elevation_neighbors.curr_elev` | schema.generated.md:2740 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | `percentile_disc(0.5)` of neighbors within 45 m | schema_only.sql:22613 | inferred |











### staging.vw_elevation_medians (view)





- **Purpose:** weighted-average elevation smoothing over `temp_elevation_smoothing_m`: flags valid points (neighbor deltas < 2 m), computes distance-weighted elevation within 150 m, then a second validity pass falling back to median.





- **Grain:** one row per meter position.





- **Canonical source:** complex derived view (CTEs `neighbors`, `altered`, `wa`, `wa_neighbor`, `valid_pass_2`) over `staging.temp_elevation_smoothing_m`; `curr_elev` = weighted average or median fallback.





- **Dependencies:** `staging.temp_elevation_smoothing_m`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | `temp_elevation_smoothing_m.meter` | schema.generated.md:2746 | confirmed |





| `curr_elev` | numeric | YES | Smoothed elevation | metres | weighted average / median fallback | schema_only.sql:22689-22693 | inferred |











### staging.vw_elevation_neighbors (view)





- **Purpose:** per-meter elevation neighbor context (prior/next valid elevations and deltas) plus validity/extreme-point flags used by the elevation-smoothing pass views.





- **Grain:** one row per meter position.





- **Canonical source:** derived view over the elevation-smoothing base (temp smoothed points); computes `prior_elev`/`next_elev` via window `lag`/`lead`, `inc_delta`/`out_delta`, and `valid_point`/`extreme_point` flags.





- **Dependencies:** `staging.temp_elevation_smoothing_m` (via upstream CTEs).





- **Consumers:** `vw_elevation_backward`, `vw_elevation_bumps`, `vw_elevation_forward`, `vw_elevation_median`, `vw_small_pull`.





- **Rules:** read-only view; `inc_delta`/`out_delta` are elevation deltas in metres; `valid_point`/`extreme_point` are boolean flags.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | elevation base | schema.generated.md:2752 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | elevation base | schema.generated.md:2753 | confirmed |





| `prior_elev` | numeric | YES | Prior valid elevation | metres | window `lag` | schema.generated.md:2754 | confirmed |





| `next_elev` | numeric | YES | Next valid elevation | metres | window `lead` | schema.generated.md:2755 | confirmed |





| `inc_delta` | numeric | YES | Incoming elevation delta | metres | derived difference | schema.generated.md:2756 | confirmed |





| `out_delta` | numeric | YES | Outgoing elevation delta | metres | derived difference | schema.generated.md:2757 | confirmed |





| `valid_point` | boolean | YES | Valid point flag | boolean | window-based validity (neighbor deltas < 2 m) | schema_only.sql (elevation base) | inferred |





| `extreme_point` | boolean | YES | Extreme point (bump) flag | boolean | flag used by `vw_elevation_bumps` | schema_only.sql (elevation base) | inferred |











### staging.vw_elevation_weighted_avg (view)





- **Purpose:** weighted-average elevation smoothing pass producing `new_elev` per meter from raw/current elevation (part of the multi-pass elevation-smoothing pipeline).





- **Grain:** one row per meter position.





- **Canonical source:** derived view over elevation neighbor data; `new_elev` is a weighted average of neighboring elevations.





- **Dependencies:** `staging.vw_elevation_neighbors` (upstream elevation data).





- **Consumers:** elevation smoothing pipeline.





- **Rules:** read-only view; elevation values in metres (inferred from column names and pipeline context, not DB-enforced).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | elevation base | schema.generated.md:2747 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | elevation base | schema.generated.md:2748 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | weighted average | schema.generated.md:2749 | inferred |











### staging.vw_large_pull (view)





- **Purpose:** elevation-smoothing pass producing `new_elev` per meter (part of the multi-pass elevation-smoothing pipeline); name suggests a large-batch pull of points for smoothing.





- **Grain:** one row per meter position.





- **Canonical source:** derived view over elevation data; exact upstream relation not individually evidenced in searched code beyond the view definition.





- **Dependencies:** elevation smoothing base tables/views (not individually evidenced).





- **Consumers:** elevation smoothing pipeline.





- **Rules:** read-only view; elevation values in metres (inferred, not DB-enforced).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | elevation base | schema.generated.md:2758 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | elevation base | schema.generated.md:2759 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | derived | schema.generated.md:2760 | inferred |











### staging.vw_listening_history_inserts (view)





- **Purpose:** deduplicated listening-history insert candidates (one row per played_at_utc, ordered) with ISRC and playlist_id extracted from context URI, feeding `music.listening_history`.





- **Grain:** one row per `played_at_utc` play event.





- **Canonical source:** derived view over `staging.stg_listen_history`; `playlist_id = split_part(context_uri, 'spotify:playlist:', 2)` when `context_type = 'playlist'`, else NULL; ordered by `played_at_utc`.





- **Dependencies:** `staging.stg_listen_history`.





- **Consumers:** `music.listening_history` insert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `played_at_utc` | timestamp with time zone | YES | Play timestamp | UTC timestamptz | `stg_listen_history.played_at_utc` | schema.generated.md:2764 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | `stg_listen_history.incoming_isrc` | schema_only.sql:22798 | confirmed |





| `playlist_id` | text | YES | Playlist id | Spotify playlist id | `split_part(context_uri,...)` if playlist | schema_only.sql:22799-22802 | confirmed |











### staging.vw_now_playing_inserts (view)





- **Purpose:** distinct now-playing insert candidates (played_at_utc, ISRC, playlist_id) from `staging.stg_now_playing`, feeding `music.listening_history`.





- **Grain:** one row per `(played_at_utc, isrc)` play event.





- **Canonical source:** derived view over `staging.stg_now_playing`; `isrc = incoming_isrc`; `playlist_id = split_part(context_uri, 'spotify:playlist:', 2)` when `context_type = 'playlist'`, else NULL; `DISTINCT`, ordered by `played_at_utc`.





- **Dependencies:** `staging.stg_now_playing`.





- **Consumers:** `music.listening_history` insert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `played_at_utc` | timestamp with time zone | YES | Play timestamp | UTC timestamptz | `stg_now_playing.played_at_utc` | schema.generated.md:2769 | confirmed |





| `isrc` | text | YES | Track ISRC | ISRC code | `stg_now_playing.incoming_isrc` | schema_only.sql:22834 | confirmed |





| `playlist_id` | text | YES | Playlist id | Spotify playlist id | `split_part(context_uri,...)` if playlist | schema_only.sql:22835-22838 | confirmed |











### staging.vw_playlist_inserts (view)





- **Purpose:** deduplicated playlist-track insert candidates with ISRC normalized via `music.isrc_swaps`, feeding `music.playlist_isrcs`.





- **Grain:** one row per `(playlist_id, isrc)` (first occurrence per playlist+isrc, renumbered `track_order`).





- **Canonical source:** derived view over `staging.stg_playlist_details` left-joined to `music.isrc_swaps`; `isrc = COALESCE(mapped_isrc, incoming_isrc)`; dedup `row_number() = 1` per playlist+isrc; `track_order` renumbered per playlist.





- **Dependencies:** `staging.stg_playlist_details`, `music.isrc_swaps`.





- **Consumers:** `music.playlist_isrcs` insert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `playlist_id` | text | YES | Playlist id | Spotify playlist id | `stg_playlist_details.playlist_id` | schema.generated.md:2774 | confirmed |





| `isrc` | text | YES | Normalized track ISRC | ISRC code | `COALESCE(mapped_isrc, incoming_isrc)` | schema_only.sql:22871 | confirmed |





| `track_order` | integer | YES | Position in playlist | ordinal | renumbered `row_number()` per playlist | schema_only.sql:22879 | confirmed |











### staging.vw_small_pull (view)





- **Purpose:** small-batch elevation-smoothing pass: for invalid points, replaces elevation with the average of `(prior_elev + next_elev + curr_elev) / 3` from neighbors 1-10 m away.





- **Grain:** one row per bad point from `vw_elevation_neighbors`.





- **Canonical source:** derived view joining `vw_elevation_neighbors` to bad points within 1-10 m; `new_elev = avg((prior_elev + next_elev + curr_elev) / 3)`.





- **Dependencies:** `staging.vw_elevation_neighbors`.





- **Consumers:** elevation smoothing pipeline.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `meter` | integer | YES | Meter position | ordinal | bad point meter | schema.generated.md:2779 | confirmed |





| `curr_elev` | numeric | YES | Current elevation | metres | `vw_elevation_neighbors.curr_elev` | schema.generated.md:2780 | confirmed |





| `new_elev` | numeric | YES | Smoothed elevation | metres | `avg((prior+next+curr)/3)` of neighbors 1-10 m | schema_only.sql:22918 | inferred |











### staging.vw_track_inserts (view)





- **Purpose:** deduplicated track insert candidates (latest fetched_at_utc per track) with cleaned names, parsed release date, US availability, market count, feeding `music.all_tracks`.





- **Grain:** one row per `track_id` (latest record only, `duplicate_finder = 1`).





- **Canonical source:** derived view over `staging.stg_playlist_details`; `row_number()` dedup by `track_id` ordered by `fetched_at_utc DESC`; `track_name_clean` via `clean_track_display()`, `artist_display_name` via `clean_artist_display()`, `release_date` parsed from `track_release`/`track_release_precision`, `available_in_us = track_markets ILIKE '%US%'`, `total_markets = cardinality(track_markets[])`.





- **Dependencies:** `staging.stg_playlist_details`, `public.clean_track_display`, `public.clean_artist_display`.





- **Consumers:** `music.all_tracks` upsert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | YES | Track id | Spotify track id | source | schema.generated.md:2785 | confirmed |





| `track_name` | text | YES | Track name | text | source | schema.generated.md:2786 | confirmed |





| `track_name_clean` | text | YES | Cleaned track name | text | `public.clean_track_display(track_name)` | schema.generated.md:2787 | confirmed |





| `track_name_search` | text | YES | Search-optimized track name | text | derived | schema.generated.md:2788 | confirmed |





| `track_isrc` | text | YES | Track ISRC | ISRC code | `stg_playlist_details.incoming_isrc` | schema.generated.md:2789 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2790 | confirmed |





| `artist_display_name` | text | YES | Artist display name | text | `public.clean_artist_display(track_name, artist_name)` | schema.generated.md:2791 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2792 | confirmed |





| `release_date` | date | YES | Release date | calendar date | parsed from `track_release`/`precision` | schema.generated.md:2793 | confirmed |





| `available_in_us` | boolean | YES | Available in US flag | boolean | `track_markets ILIKE '%US%'` | schema.generated.md:2794 | confirmed |





| `total_markets` | integer | YES | Market count | count | `cardinality(track_markets[])` | schema.generated.md:2795 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | source | schema.generated.md:2796 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | source | schema.generated.md:2797 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Refresh timestamp | UTC timestamptz | `fetched_at_utc` (latest) | schema.generated.md:2798 | confirmed |











### staging.vw_track_inserts_from_listen_history (view)





- **Purpose:** deduplicated track insert candidates derived from `staging.stg_listen_history` (latest played_at_utc per track), feeding `music.all_tracks` from listen history.





- **Grain:** one row per `track_id` (latest record only).





- **Canonical source:** derived view over `staging.stg_listen_history`; same cleaning/parsing logic as `vw_track_inserts` (`track_name_clean`, `artist_display_name`, `release_date`, `available_in_us`, `total_markets`) but `total_markets` from `available_market_count` and `available_in_us` from `available_in_usa`.





- **Dependencies:** `staging.stg_listen_history`, `public.clean_track_display`, `public.clean_artist_display`.





- **Consumers:** `music.all_tracks` upsert path.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `track_id` | text | YES | Track id | Spotify track id | source | schema.generated.md:2803 | confirmed |





| `track_name` | text | YES | Track name | text | source | schema.generated.md:2804 | confirmed |





| `track_name_clean` | text | YES | Cleaned track name | text | `public.clean_track_display(track_name)` | schema.generated.md:2805 | confirmed |





| `track_name_search` | text | YES | Search-optimized track name | text | derived | schema.generated.md:2806 | confirmed |





| `track_isrc` | text | YES | Track ISRC | ISRC code | `stg_listen_history.incoming_isrc` | schema.generated.md:2807 | confirmed |





| `artist_id` | text | YES | Artist id | Spotify artist id | source | schema.generated.md:2808 | confirmed |





| `artist_display_name` | text | YES | Artist display name | text | `public.clean_artist_display(track_name, artist_name)` | schema.generated.md:2809 | confirmed |





| `album_id` | text | YES | Album id | Spotify album id | source | schema.generated.md:2810 | confirmed |





| `release_date` | date | YES | Release date | calendar date | parsed from `track_release`/`precision` | schema.generated.md:2811 | confirmed |





| `available_in_us` | boolean | YES | Available in US flag | boolean | `stg_listen_history.available_in_usa` | schema.generated.md:2812 | confirmed |





| `total_markets` | integer | YES | Market count | count | `stg_listen_history.available_market_count` | schema.generated.md:2813 | confirmed |





| `popularity` | integer | YES | Popularity score | score points | source | schema.generated.md:2814 | confirmed |





| `duration_ms` | integer | YES | Track duration | milliseconds | source | schema.generated.md:2815 | confirmed |





| `updated_at_utc` | timestamp with time zone | YES | Refresh timestamp | UTC timestamptz | `played_at_utc` as `fetched_at_utc` | schema.generated.md:2816 | confirmed |











## tasks











### tasks.acceptable_failures (table)





- **Purpose:** single-value configuration table storing the maximum tolerated consecutive failures before the scheduler treats a task as failed.





- **Grain:** exactly one row holding one integer threshold. No primary key or unique constraint is declared, so single-row cardinality is convention only.





- **Canonical source:** `tasks.acceptable_failures`; no application INSERT/UPDATE/DELETE path evidenced in repository code (maintained out-of-band).





- **Consumers:** `tasks.vw_task_info` — scalar subquery `(SELECT acceptable_failures FROM tasks.acceptable_failures)` caps `consecutive_failures` in `should_execute`.





- **Relationships:** no foreign keys; not referenced by any foreign key.





- **Rules:** `convention` — `should_execute` requires `consecutive_failures < (SELECT acceptable_failures FROM tasks.acceptable_failures)`; value compared as a count. Migration `deploy/migrations/014__comment_backfill_tasks.sql` carries only generic boilerplate ('Tasks table.', 'Acceptable failures value.').





- **Write path:** none evidenced in application code.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `acceptable_failures` | smallint | YES | Maximum consecutive failures tolerated before a task is skipped | count | schema.generated.md; scalar subquery in `tasks.vw_task_info` definition | schema.generated.md, docs/data/_evidence/database/schema_only.sql | inferred |











### tasks.fact_configuration (table)





- **Purpose:** ETL fact registry: each row declares one derived fact/metric computed by a task, including the extraction SQL and whether values are inferred, forecasted, interpolated, or unique.





- **Grain:** one row per fact, keyed by `fact_id`; each fact belongs to one task (`task_id`) and one staging job (`staging_id`).





- **Primary key:** `fact_id` (enforced; unique index `fact_configuration_pkey`, default from sequence `tasks.fact_configuration_fact_id_seq`).





- **Foreign keys (enforced):** `task_id` -> `tasks.task_configuration.task_id` (`fk_fact_to_task`); `staging_id` -> `tasks.staging_configuration.staging_id` (`fk_fact_to_staging`).





- **Canonical source:** `tasks.fact_configuration`.





- **Consumers:** admin API `POST /api/admin/tasks/facts` (`upsert_fact_configuration_endpoint`) and `DELETE /api/admin/tasks/facts/{fact_id}` (`delete_fact_configuration_endpoint`) via `backend/pifitness/admin/queries.py` (`upsert_fact_configuration`, `delete_fact_configuration`).





- **Relationships:** child of `tasks.task_configuration` and `tasks.staging_configuration`; no downstream consumers evidenced.





- **Rules:** `enforced` PK + 2 FKs; upsert (INSERT or UPDATE by `fact_id`) is application logic, not DB-enforced. Migration `deploy/migrations/014__comment_backfill_tasks.sql` carries only generic boilerplate ('Tasks table.', per-column '... value.').





- **Write path:** `backend/pifitness/admin/queries.py:336` `upsert_fact_configuration`; `:288` `delete_fact_configuration`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `fact_id` | integer | NO | Surrogate fact id | count | sequence default `tasks.fact_configuration_fact_id_seq` | schema.generated.md, docs/data/_evidence/database/catalog.json | confirmed |





| `staging_id` | integer | YES | Owning staging job | id | FK `fk_fact_to_staging` | schema.generated.md, catalog.json | confirmed |





| `task_id` | integer | YES | Owning task | id | FK `fk_fact_to_task` | schema.generated.md, catalog.json | confirmed |





| `fact_name` | text | YES | Fact/metric name | text | upsert fields | backend/pifitness/admin/queries.py:336 | inferred |





| `data_type` | text | YES | Declared data type of the fact | text | upsert fields | backend/pifitness/admin/queries.py:336 | inferred |





| `extraction_sql` | text | YES | SQL used to extract/compute the fact | SQL text | upsert fields | backend/pifitness/admin/queries.py:336 | inferred |





| `infer_values` | boolean | YES | Whether missing values are inferred | yes/no; default false | upsert fields; column default false | catalog.json + queries.py:336 | inferred |





| `forecast_values` | boolean | YES | Whether future values are forecast | yes/no; default false | upsert fields; column default false | catalog.json + queries.py:336 | inferred |





| `is_unique_constraint` | boolean | YES | Whether fact values are expected unique | yes/no; default false | upsert fields; column default false | catalog.json + queries.py:336 | inferred |





| `interpolation_ts` | boolean | YES | Whether interpolation is timestamp-based | yes/no; default false | upsert fields; column default false | catalog.json + queries.py:336 | inferred |





| `interpolation_destination_table` | text | YES | Target table for interpolated values | table name | upsert fields | backend/pifitness/admin/queries.py:336 | inferred |





| `interpolate_values` | boolean | YES | Whether values are interpolated | yes/no; default false | upsert fields; column default false | catalog.json + queries.py:336 | inferred |





| `value_recency` | timestamp with time zone | YES | Timestamp of last known-good value | UTC timestamptz | upsert fields | backend/pifitness/admin/queries.py:336 | inferred |











### tasks.staging_configuration (table)





- **Purpose:** staging-job registry: each row declares one extract/load staging job (source table, destination table, optional filter/cross-join conditions) for a task.





- **Grain:** one row per staging job, keyed by `staging_id`; belongs to one task (`task_id`).





- **Primary key:** `staging_id` (enforced; sequence `tasks.staging_configuration_staging_id_seq`).





- **Foreign keys (enforced):** `task_id` -> `tasks.task_configuration.task_id` (`fk_staging_to_task`).





- **Canonical source:** `tasks.staging_configuration`; no application read/write path evidenced in repository code (referenced only by FKs and migration comments).





- **Consumers:** none evidenced.





- **Relationships:** child of `tasks.task_configuration`.





- **Rules:** `enforced` PK + FK; `source_table` defaults to `staging.api_imports` (live catalog default). Migration comments are generic boilerplate only.





- **Write path:** none evidenced in application code.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `staging_id` | integer | NO | Surrogate staging-job id | count | sequence default `tasks.staging_configuration_staging_id_seq` | schema.generated.md, docs/data/_evidence/database/catalog.json | confirmed |





| `task_id` | integer | YES | Owning task | id | FK `fk_staging_to_task` | schema.generated.md, catalog.json | confirmed |





| `staging_name` | text | YES | Staging job name | text | no write path evidenced | schema.generated.md | inferred |





| `staging_description` | text | YES | Staging job description | text | no write path evidenced | schema.generated.md | inferred |





| `destination_table` | text | YES | Target table for staged data | table name | no write path evidenced | schema.generated.md | inferred |





| `cross_join_condition` | text | YES | Optional cross-join condition for the staging query | SQL fragment | no write path evidenced | schema.generated.md | inferred |





| `filter_condition` | text | YES | Optional filter condition for the staging query | SQL fragment | no write path evidenced | schema.generated.md | inferred |





| `source_table` | text | YES | Source table for the staging job | table name | default `'staging.api_imports'::text` | catalog.json | confirmed |











### tasks.task_config (table)





- **Purpose:** legacy static task schedule configuration, keyed by task name; used by the legacy task executioner (`worker/task_execution.py`) and refreshed by `reset_and_reload`/`update_task_through_date`. Runs in parallel with the newer `tasks.task_configuration` (name-keyed legacy vs id-keyed current runtime state).





- **Grain:** one row per task, keyed by `task_name`.





- **Primary key:** `task_name` (enforced; `task_config_pkey`).





- **Foreign keys (enforced):** `api_service_name` -> `api_services.api_service_list.api_service_name` (`task_config_api_service_name_fkey`).





- **Canonical source:** `tasks.task_config`.





- **Consumers:** `backend/pifitness/worker/task_execution.py` (`task_executioner` resolves `task_name` -> `task_id` against this table); `reset_and_reload` and `update_task_through_date` write `updated_through_date`.





- **Relationships:** references `api_services.api_service_list`; overlaps conceptually with `tasks.task_configuration`.





- **Rules:** `enforced` PK + FK; live-catalog defaults: `task_priority` 999, `task_frequency` 'daily', `task_interval` 23 (`numeric(3,1)`). `task_interval` unit is not evidenced (hours assumed by scheduling helpers; unverified).





- **Write path:** no INSERT path evidenced; `UPDATE tasks.task_config SET updated_through_date` in `backend/pifitness/worker/task_execution.py` (`reset_and_reload` sets '2020-01-01'; `update_task_through_date` sets today's date).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_name` | text | NO | Task name (natural key) | text | key assignment | schema.generated.md, catalog.json | confirmed |





| `task_description` | text | YES | Task description | text | no write path evidenced | schema.generated.md | inferred |





| `task_priority` | integer | YES | Scheduling priority (lower = higher priority assumed, not evidenced) | count | default 999 | catalog.json | inferred |





| `task_frequency` | text | YES | Schedule frequency (e.g. 'daily') | text | default 'daily' | catalog.json | inferred |





| `task_interval` | numeric | YES | Interval for recurrence | unit unknown (hours assumed) | default 23 | catalog.json | inferred |





| `api_function` | text | YES | API function name | text | no write path evidenced | schema.generated.md | inferred |





| `api_service_name` | text | YES | API service | id | FK `task_config_api_service_name_fkey` | schema.generated.md, catalog.json | confirmed |





| `api_loop_type` | text | YES | API pagination/loop strategy | text | no write path evidenced | schema.generated.md | inferred |





| `api_post_processing` | text | YES | Post-processing step name | text | no write path evidenced | schema.generated.md | inferred |





| `python_function` | text | YES | Python entry point to run | function name | no write path evidenced | schema.generated.md | inferred |





| `last_calendar_field` | text | YES | Calendar field used for date boundaries | text | no write path evidenced | schema.generated.md | inferred |





| `updated_through_date` | date | YES | Data watermark (date only, no time zone) | calendar date | `reset_and_reload` ('2020-01-01'), `update_task_through_date` (today) | backend/pifitness/worker/task_execution.py:458-496 | confirmed |





| `api_parameters` | text | YES | API call parameters | text | no write path evidenced | schema.generated.md | inferred |











### tasks.task_configuration (table)





- **Purpose:** current task schedule plus runtime state: last run timestamps, failure message, and next planned execution. Source of truth for the v2 execution engine (`worker/ultimate_task_executioner_v2.py`).





- **Grain:** one row per task, keyed by `task_id`.





- **Primary key:** `task_id` (enforced; sequence `tasks.task_configuration_task_id_seq`).





- **Canonical source:** `tasks.task_configuration`.





- **Consumers:** admin API (`GET /api/admin/tasks`, `/api/admin/tasks/{task_id}/config`, `/api/admin/tasks/schedule`, `/api/admin/tasks/names`, `PUT /api/admin/tasks/{task_id}`, `DELETE /api/admin/tasks/schedule/{task_id}`); `worker/ultimate_task_executioner_v2.py` (`ultimate_task_executioner` reads via `tasks.vw_task_info`; `reconcile_task_dates` writes state); `tasks.vw_task_info` and `tasks.vw_task_summary_chart` derive from it.





- **Relationships:** parent of `tasks.task_executions` (`task_executions_task_id_fkey`), `tasks.fact_configuration` (`fk_fact_to_task`), `tasks.staging_configuration` (`fk_staging_to_task`).





- **Rules:** `enforced` PK; new rows default `task_frequency` = 'Inactive'. Write semantics (from `reconcile_task_dates`, `worker/ultimate_task_executioner_v2.py:483`): on success sets `last_executed_utc = last_succeeded_utc = CURRENT_TIMESTAMP`, `consecutive_failures = 0`, and computes `next_planned_execution_utc` by frequency (Hourly: +`task_interval` hours; Daily/Weekly/Monthly: at America/Los_Angeles midnight + interval hours; otherwise +24 hours; +2 hours when the service value is not current); on failure increments `consecutive_failures`, sets `last_failed_utc` and `last_failure_message`, and backfills `next_planned_execution_utc` by `consecutive_failures * 60 minutes` (exponential backoff in minutes). Sentinel default '1980-07-18 07:00:00+00' on the `last_*`/`next_planned` timestamps means 'never run'. All timestamps are `CURRENT_TIMESTAMP` (database server time; time zone not confirmed as UTC).





- **Write path:** `backend/pifitness/admin/queries.py` (`insert_task_configuration`, `update_task_configuration`, `insert_placeholder_task`) + `worker/ultimate_task_executioner_v2.py:483` `reconcile_task_dates`.











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_id` | integer | NO | Surrogate task id | count | sequence default `tasks.task_configuration_task_id_seq` | schema.generated.md, catalog.json | confirmed |





| `task_name` | text | YES | Task name | text | default 'placeholder' | catalog.json | confirmed |





| `last_executed_utc` | timestamp with time zone | YES | Last run start time | UTC timestamptz; sentinel '1980-07-18' = never run | `reconcile_task_dates` | catalog.json | confirmed |





| `last_succeeded_utc` | timestamp with time zone | YES | Last successful run | UTC timestamptz; sentinel '1980-07-18' = never run | `reconcile_task_dates` | catalog.json | confirmed |





| `last_failed_utc` | timestamp with time zone | YES | Last failed run | UTC timestamptz | `reconcile_task_dates` | catalog.json | confirmed |





| `next_planned_execution_utc` | timestamp with time zone | YES | Next scheduled run | UTC timestamptz; sentinel '1980-07-18' = due immediately | `reconcile_task_dates` by frequency | catalog.json + worker/ultimate_task_executioner_v2.py:483 | confirmed |





| `last_failure_message` | text | YES | Last error message | text | `reconcile_task_dates` | catalog.json | confirmed |





| `display_icon` | text | YES | UI icon name | text | default 'task' | catalog.json | confirmed |





| `task_description` | text | YES | Task description | text | no write path evidenced | schema.generated.md | inferred |





| `task_priority` | integer | YES | Priority | count | default 999 | catalog.json | inferred |





| `task_frequency` | text | YES | Schedule frequency ('Hourly'|'Daily'|'Weekly'|'Monthly'|'Inactive'|other) | text | default 'Inactive' | catalog.json | inferred |





| `task_start_hour` | integer | YES | Start of hourly execution window (hour of day) | hour 0-23 | default 6 | catalog.json | inferred |





| `task_stop_hour` | integer | YES | End of hourly execution window (hour of day) | hour 0-23 | default 22 | catalog.json | inferred |





| `task_interval` | integer | YES | Recurrence interval | hours (per `reconcile_task_dates`) | default 8 | catalog.json | inferred |





| `api_service_name` | text | YES | API service | id | default 'N/A' | catalog.json | confirmed |





| `api_function_name` | text | YES | API/function name | text | default 'N/A' | catalog.json | confirmed |





| `python_execution_function` | text | YES | Python entry point to run | function name | no write path evidenced | schema.generated.md | inferred |





| `consecutive_failures` | smallint | YES | Current consecutive failure count | count | default 0; incremented on failure, zeroed on success | catalog.json + worker/ultimate_task_executioner_v2.py:483 | confirmed |











### tasks.task_executions (table)





- **Purpose:** coarse-grained log of task runs: one row per execution with status, start/complete timestamps, JSON result, and error message.





- **Grain:** one row per execution, keyed by `execution_id` (identity sequence `tasks.task_executions_execution_id_seq`).





- **Primary key:** `execution_id` (enforced; `task_executions_pkey`).





- **Foreign keys (enforced):** `task_id` -> `tasks.task_configuration.task_id` (`task_executions_task_id_fkey`).





- **Canonical source:** `tasks.task_executions`.





- **Consumers:** admin API (`GET /api/admin/tasks/executions/{execution_id}`, recent-executions list) via `backend/pifitness/admin/queries.py` (`get_task_execution`, `get_recent_task_executions`).





- **Relationships:** child of `tasks.task_configuration`. Do not confuse with `logging.task_executions` (fine-grained phase-timing log consumed by `tasks.vw_task_execution`); both tables share index name `task_executions_pkey` in different schemas.





- **Rules:** `enforced` PK + FK; `status` defaults to 'running', `started_at` defaults to CURRENT_TIMESTAMP but the column is `timestamp without time zone` (naive — server local time, not confirmed as UTC). `task_id`/`task_name` are NOT NULL. Completion update sets `completed_at = CURRENT_TIMESTAMP`, `result` (jsonb), `error_message`.





- **Write path:** `backend/pifitness/admin/router.py:223` (`execute_task_v2_async` inserts 'running'); `backend/pifitness/admin/queries.py:449` (`insert_task_execution`) and `:474` (`update_task_execution`).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `execution_id` | integer | NO | Surrogate execution id | count | sequence default `tasks.task_executions_execution_id_seq` | schema.generated.md, catalog.json | confirmed |





| `task_id` | integer | NO | Owning task | id | FK `task_executions_task_id_fkey` | schema.generated.md, catalog.json | confirmed |





| `task_name` | character varying | NO | Task name at execution time | text (255 per catalog) | insert path | schema.generated.md, catalog.json | confirmed |





| `status` | character varying | YES | Execution status ('running'|'success'|'failed') | text | default 'running' | catalog.json | confirmed |





| `started_at` | timestamp without time zone | YES | Start time (naive) | server local time; default CURRENT_TIMESTAMP | insert path; zone not confirmed as UTC | catalog.json | confirmed |





| `completed_at` | timestamp without time zone | YES | Completion time (naive) | server local time | `update_task_execution` | schema.generated.md | inferred |





| `result` | jsonb | YES | Execution result payload | JSON | `update_task_execution` | schema.generated.md | confirmed |





| `error_message` | text | YES | Error text | text | `update_task_execution` | schema.generated.md | confirmed |











### tasks.vw_task_execution (view)





- **Purpose:** per-task dashboard/execution view: merges static config (`tasks.task_config`) with aggregated execution history (`logging.task_executions`) to drive skip/recency logic for the admin UI and the legacy executioner.





- **Grain:** one row per `task_name` in `tasks.task_config`; tasks with no executions still appear with zeroed stats (left-joined aggregates).





- **Output dependencies:** `tasks.task_config`, `logging.task_executions`, `api_services.api_service_list`, SQL functions `public.utc_now()`, `public.requires_catchup()`, `public.calculate_next_execution_utc()`.





- **Derived logic:** `total_attempts`/`consecutive_successes`/`consecutive_failures` are streak aggregates over `logging.task_executions` (partitioned by task_name, latest row first); `updated_through_utc` is `updated_through_date` cast at UTC (or `utc_now()` when `api_function` IS NULL); `data_requires_catchup = public.requires_catchup(...)`; `next_execution_utc = public.calculate_next_execution_utc(...)`; `do_execute` is true unless the task is retired and either catchup is due or the next execution is past; `execution_logic` = 'recency' if the last run was under 1 minute ago, 'failures' if `consecutive_failures > 5`, else 'timing'. NOTE: this view's `consecutive_failures` comes from `logging.task_executions`, not `tasks.task_configuration.consecutive_failures` — two different failure counts.





- **Consumers:** `backend/pifitness/worker/task_execution.py:16` (`task_executioner`); `backend/pifitness/admin/queries.py:368` (`get_distinct_task_names`) and `:377` (`get_task_execution_view`) for the admin UI.





- **Rules:** read-only view; ORDER BY favors due/needed tasks (`next_execution_utc`, `data_requires_catchup DESC`, `do_execute DESC`, ...). Built over `logging.task_executions` — distinct from `tasks.task_executions`.





- **Write path:** none (view).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_name` | text | YES | Task name | text | from `task_config` | schema.generated.md | confirmed |





| `task_description` | text | YES | Task description | text | from `task_config` | schema.generated.md | inferred |





| `task_frequency` | text | YES | Schedule frequency | text | from `task_config` | schema.generated.md | inferred |





| `task_interval` | numeric | YES | Interval for recurrence | unit unknown | from `task_config` | schema.generated.md | inferred |





| `task_priority` | integer | YES | Priority | count | from `task_config` | schema.generated.md | inferred |





| `api_function` | text | YES | API function name | text | from `task_config` | schema.generated.md | inferred |





| `api_service_name` | text | YES | API service | id | from `task_config` | schema.generated.md | inferred |





| `api_loop_type` | text | YES | API loop strategy | text | from `task_config` | schema.generated.md | inferred |





| `api_post_processing` | text | YES | Post-processing step | text | from `task_config` | schema.generated.md | inferred |





| `api_parameters` | text | YES | API call parameters | text | from `task_config` | schema.generated.md | inferred |





| `api_service_function` | text | YES | Resolved service function | function name | join to `api_service_list` | schema.generated.md | inferred |





| `python_function` | text | YES | Python entry point | function name | from `task_config` | schema.generated.md | inferred |





| `last_calendar_field` | text | YES | Calendar boundary field | text | from `task_config` | schema.generated.md | inferred |





| `total_attempts` | bigint | YES | Total execution attempts | count | COUNT over `logging.task_executions` | view definition | derived |





| `consecutive_successes` | bigint | YES | Current success streak | count | window aggregate over `logging.task_executions` | view definition | derived |





| `consecutive_failures` | bigint | YES | Current failure streak | count | window aggregate over `logging.task_executions` | view definition | derived |





| `last_execution_utc` | timestamp with time zone | YES | Most recent execution time | UTC timestamptz | MAX(event_time_utc) | view definition | derived |





| `last_success_utc` | timestamp with time zone | YES | Most recent success | UTC timestamptz | conditional MAX | view definition | derived |





| `updated_through_utc` | timestamp with time zone | YES | Data watermark as UTC | timestamptz | CASE: `updated_through_date` at UTC or `utc_now()` | view definition | derived |





| `data_requires_catchup` | boolean | YES | Whether data is behind schedule | yes/no | `public.requires_catchup()` | view definition | derived |





| `next_execution_utc` | timestamp with time zone | YES | Computed next run | UTC timestamptz | `public.calculate_next_execution_utc()` | view definition | derived |





| `do_execute` | boolean | YES | Whether the task is due to run | yes/no | CASE expression | view definition | derived |





| `execution_logic` | text | YES | Skip-reason category | 'recency'|'failures'|'timing' | CASE expression | view definition | derived |











### tasks.vw_task_info (view)





- **Purpose:** execution-ready task list for the v2 engine: task runtime state enriched with service/rate-limit state and boolean action flags (`run_extract`, `run_python`).





- **Grain:** one row per `tasks.task_configuration` row.





- **Output dependencies:** `tasks.task_configuration`, `tasks.acceptable_failures` (scalar threshold subquery), `api_services.function_library`, `api_services.api_service_list`.





- **Derived logic:** `should_execute` = (frequency = 'Hourly' within `task_start_hour`..`task_stop_hour`, or frequency not in ('Inactive','Hourly')) AND `next_planned_execution_utc` < now AND `consecutive_failures` < the `tasks.acceptable_failures` threshold; `next_planned_execution_utc` defaults to sentinel 1980-07-18 when NULL; `run_extract`/`run_python` = configured function not in ('None','N/A',''); `flatten_sproc` = computed call string `flatten_sproc(task_id, 'friendly_name')`; `api_under_rate_limits` = `rate_limit_cleared_utc` > now; internal `catchup_required` flags staleness vs yesterday.





- **Consumers:** `backend/pifitness/worker/ultimate_task_executioner_v2.py:33` (`ultimate_task_executioner` filters `WHERE should_execute`).





- **Rules:** read-only, non-updatable view — migrations `002`/`003`/`004` document that updates flow through base tables only and no UPDATE against the view is required/supported; `acceptable_failures` is assumed single-row. All view timestamps default-ish to sentinel 1980-07-18 / 2025-07-18 via COALESCE when base values are NULL.





- **Write path:** none (view).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_id` | integer | YES | Task id | count | from `task_configuration` | schema.generated.md | confirmed |





| `task_name` | text | YES | Task name | text | from `task_configuration` | schema.generated.md | inferred |





| `task_frequency` | text | YES | Schedule frequency | text | from `task_configuration` | schema.generated.md | inferred |





| `task_interval` | integer | YES | Recurrence interval | hours assumed | from `task_configuration` | schema.generated.md | inferred |





| `task_start_hour` | integer | YES | Start of hourly window (hour of day) | hour 0-23 | from `task_configuration` | schema.generated.md | inferred |





| `task_stop_hour` | integer | YES | End of hourly window (hour of day) | hour 0-23 | from `task_configuration` | schema.generated.md | inferred |





| `next_planned_execution_utc` | timestamp with time zone | YES | Next scheduled run | UTC timestamptz; coalesce sentinel 1980-07-18 | COALESCE from base table | view definition | derived |





| `should_execute` | boolean | YES | Execution gate | yes/no | compound expression incl. `acceptable_failures` subquery | view definition | derived |





| `consecutive_failures` | integer | YES | Current consecutive failure count | count | cast of base-table column | view definition | derived |





| `api_service_name` | text | YES | API service | id | join `function_library`/`api_service_list` | view definition | inferred |





| `api_function_name` | text | YES | API/function name | text | join `function_library`/`api_service_list` | view definition | inferred |





| `loop_strategy` | text | YES | API loop strategy | text | from `function_library` | view definition | inferred |





| `iter_path_keys` | text | YES | Iteration path keys | text | from `function_library` | view definition | inferred |





| `iter_query_keys` | text | YES | Iteration query keys | text | from `function_library` | view definition | inferred |





| `static_path_params` | text | YES | Static path parameters | text | from `function_library` | view definition | inferred |





| `static_query_params` | text | YES | Static query parameters | text | from `function_library` | view definition | inferred |





| `friendly_name` | text | YES | Service friendly name | text | from `api_service_list` | view definition | inferred |





| `run_extract` | boolean | YES | Whether extract step is configured | yes/no | `api_service_name` not in ('None','N/A','') | view definition | derived |





| `python_login_function` | text | YES | Login function | function name | `api_service_list.api_service_function` | view definition | derived |





| `python_extraction_function` | text | YES | Extraction function | function name | from `function_library` | view definition | inferred |





| `flatten_sproc` | text | YES | Computed flatten stored-proc call | text | string concat `flatten_sproc(task_id, 'friendly_name')` | view definition | derived |





| `value_recency` | timestamp with time zone | YES | Last known-good value timestamp | UTC timestamptz; coalesce default 2025-07-18 | from `function_library` | view definition | derived |





| `python_execution_function` | text | YES | Python entry point | function name | from `task_configuration` | view definition | inferred |





| `run_python` | boolean | YES | Whether Python step is configured | yes/no | `python_execution_function` not in ('None','N/A','') | view definition | derived |





| `api_under_rate_limits` | boolean | YES | Whether service is rate limited | yes/no | `rate_limit_cleared_utc` > now | view definition | derived |











### tasks.vw_task_performance (view)





- **Purpose:** per-task, per-day rollup of phase-level execution timings from application event logs.





- **Grain:** one row per `task_id` + `event_date`; only events of the last 28 days with `task_id IS NOT NULL`.





- **Output dependencies:** `logging.application_events`.





- **Derived logic:** each event's `execution_time_ms` is bucketed by `data_event` ('Login'|'Extract'|'Load'|'Flatten'|'Parsing'|'Interpolation'|'Forecasting'|'Python'); all other events count as `admin_ms`; `total_ms` is the time at 'Complete' events; `is_failure` is true when any event in the day has `error_text`. Outputs are daily averages in milliseconds.





- **Consumers:** none evidenced in repository code (likely admin dashboards; unverified).





- **Rules:** read-only view; units milliseconds; 28-day sliding window is a hard filter in the view definition.





- **Write path:** none (view).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_id` | integer | YES | Task id | count | from `application_events` | schema.generated.md | confirmed |





| `event_date` | date | YES | Event date | calendar date | cast of `event_time_utc` | schema.generated.md | confirmed |





| `login_ms` | numeric | YES | Login phase duration | ms (avg per day) | CASE bucket of execution_time_ms | view definition | derived |





| `extract_ms` | numeric | YES | Extract phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `load_ms` | numeric | YES | Load phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `flatten_ms` | numeric | YES | Flatten phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `parse_ms` | numeric | YES | Parsing phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `interpolation_ms` | numeric | YES | Interpolation phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `forecasting_ms` | numeric | YES | Forecasting phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `python_ms` | numeric | YES | Python phase duration | ms (avg per day) | CASE bucket | view definition | derived |





| `admin_ms` | numeric | YES | All other phases | ms (avg per day) | CASE bucket (non-standard events) | view definition | derived |





| `total_ms` | numeric | YES | Total run duration | ms (avg per day) | 'Complete' events only | view definition | derived |





| `is_failure` | boolean | YES | Whether any event failed that day | yes/no | `error_text IS NOT NULL` | view definition | derived |











### tasks.vw_task_summary_chart (view)





- **Purpose:** admin dashboard chart data: per-task recency, execution counts, success rate, average phase timings, and an `increasing_steps` string flagging phases whose latest 7-day average exceeds the 14-day average by more than one standard deviation.





- **Grain:** one row per task.





- **Output dependencies:** `tasks.task_configuration`, `api_services.function_library`, `logging.application_events`, SQL functions `public.time_ago()`, `public.requires_catchup()`, `public.calculate_next_execution_utc()`.





- **Derived logic:** CTE `te` joins task config with `function_library` (`value_recency`, active-failure flag `last_failed_utc = last_executed_utc`); execution aggregates are split into 7-day (el7), 14-day (el14) and stddev (sd) windows over `logging.application_events`; outputs include `time_ago_*` (text via `public.time_ago`), `*_minutes_ago` (epoch math), `success_percentage`, `max_executions` (window max across tasks), and `increasing_steps` (concatenated phase names whose el7 mean > el14 mean + 1 stddev). `last_failure_message` is truncated to 30 chars.





- **Consumers:** `backend/pifitness/admin/queries.py:569` (`get_task_summary_chart`) for the admin API.





- **Code discrepancy:** `backend/pifitness/worker/task_execution.py:286` issues `REFRESH MATERIALIZED VIEW tasks.vw_task_summary_chart_materialized`; no such relation exists in `schema.generated.md` or the live dev DB (relations are table/view only) — stale or forward-looking code, harmlessly swallowed by try/except. The relation that exists is the regular view `tasks.vw_task_summary_chart`.





- **Rules:** read-only view; timings in milliseconds.





- **Write path:** none (view).











| Column | Type | Nullable | Meaning / semantics | Values / units | Source / transformation | Evidence | Confidence |





|---|---|---|---|---|---|---|---|





| `task_id` | integer | YES | Task id | count | from `task_configuration` | schema.generated.md | confirmed |





| `task_name` | text | YES | Task name | text | from `task_configuration` | schema.generated.md | inferred |





| `task_frequency` | text | YES | Schedule frequency | text | from `task_configuration` | schema.generated.md | inferred |





| `consecutive_failures` | smallint | YES | Current consecutive failure count | count | from `task_configuration` | schema.generated.md | confirmed |





| `time_ago_execution` | text | YES | Human-readable time since last execution | text | `public.time_ago(last_executed_utc)` | view definition | derived |





| `time_ago_next` | text | YES | Human-readable time to next execution | text | `public.time_ago(next_planned_execution_utc)` | view definition | derived |





| `time_ago_value` | text | YES | Human-readable time since value refreshed | text | `public.time_ago(value_recency)` | view definition | derived |





| `execution_minutes_ago` | numeric | YES | Minutes since last execution | minutes | epoch math rounded to 1 decimal | view definition | derived |





| `value_recency_minutes_ago` | numeric | YES | Minutes since value refreshed | minutes | epoch math rounded to 1 decimal | view definition | derived |





| `next_planned_execution_minutes` | numeric | YES | Minutes to next execution | minutes | epoch math rounded to 1 decimal | view definition | derived |





| `value_recency` | timestamp with time zone | YES | Last known-good value timestamp | UTC timestamptz | from `function_library` via `te` | view definition | derived |





| `last_executed_utc` | timestamp with time zone | YES | Last run time | UTC timestamptz | from `task_configuration` | view definition | derived |





| `last_failed_utc` | timestamp with time zone | YES | Last failed run | UTC timestamptz | from `task_configuration` | view definition | derived |





| `next_planned_execution_utc` | timestamp with time zone | YES | Next scheduled run | UTC timestamptz | from `task_configuration` | view definition | derived |





| `last_failure_message` | text | YES | Last failure message | text (first 30 chars) | `left(last_failure_message, 30)` | view definition | derived |





| `is_active_failure` | boolean | YES | Whether the last run was a failure | yes/no | `last_failed_utc = last_executed_utc` | view definition | derived |





| `execution_count` | bigint | YES | Execution count in window | count | 7-day aggregation | view definition | derived |





| `success_count` | bigint | YES | Success count in window | count | 7-day aggregation | view definition | derived |





| `success_percentage` | numeric | YES | Success rate | percent (0-100) | success_count / NULLIF(execution_count,0) * 100 | view definition | derived |





| `max_executions` | bigint | YES | Max execution count across all tasks | count | window MAX | view definition | derived |





| `login_ms` | numeric | YES | Login phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `extract_ms` | numeric | YES | Extract phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `flatten_ms` | numeric | YES | Flatten phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `parse_ms` | numeric | YES | Parsing phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `interpolation_ms` | numeric | YES | Interpolation phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `forecasting_ms` | numeric | YES | Forecasting phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `python_ms` | numeric | YES | Python phase duration | ms (avg) | 7-day aggregation | view definition | derived |





| `admin_ms` | numeric | YES | All other phases | ms (avg) | 7-day aggregation | view definition | derived |





| `total_ms` | numeric | YES | Total run duration | ms (avg) | 7-day aggregation | view definition | derived |





| `increasing_steps` | text | YES | Phases trending worse | text | concatenated phase names | view definition | derived |











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





