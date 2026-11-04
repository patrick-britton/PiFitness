# Data Dictionary

Per-domain data dictionary. Each section records the grain of key tables/views, the canonical source for each concept, and the rules that govern the data, marked `enforced` (DB constraint) or `convention` (application agreement) with a one-line why.

## activities

### activities.activities (table)
- **Grain:** one row per recorded activity (workout), keyed by `activity_id`.
- **Canonical source:** `activities.activities` (Garmin-derived activity summary).
- **Rules:** `enforced` PK on `activity_id`; `convention` one row per activity; `activity_path`/`is_downloaded` track GPX ingestion; timestamps UTC.

### activities.activity_details (table)
- **Grain:** one row per timestamp sample within an activity (`activity_id`, `ts_utc`).
- **Canonical source:** `activities.activity_details`.
- **Rules:** `enforced` PK (`activity_id`, `ts_utc`); `convention` samples ordered by ts_utc; FK to activities.activities (app-level).

### activities.activity_details_distance (table)
- **Grain:** one row per distance-binned sample; derived for path building.
- **Canonical source:** `activities.activity_details_distance`.
- **Rules:** `convention` derived table; PK not defined; FK app-level.

### activities.courses (table)
- **Grain:** one row per named course/route.
- **Canonical source:** `activities.courses`.
- **Rules:** `convention` course metadata; referenced by segments.

### activities.segments (table)
- **Grain:** one row per defined segment.
- **Canonical source:** `activities.segments`.
- **Rules:** `enforced` PK `segment_id`; `convention` segment geometry in activities schema.

### activities.segments_details (table)
- **Grain:** one row per segment match occurrence.
- **Canonical source:** `activities.segments_details`.
- **Rules:** `convention` match results; FKs app-level.

### activities.distinct_segments (table)
- **Grain:** one row per distinct segment geometry.
- **Canonical source:** `activities.distinct_segments`.
- **Rules:** `convention` dedup layer.

### activities.metric_list (table)
- **Grain:** one row per activity metric descriptor.
- **Canonical source:** `activities.metric_list`.
- **Rules:** `convention` lookup table.

### activities.overlap_queue (table)
- **Grain:** one row per queued overlap-processing job.
- **Canonical source:** `activities.overlap_queue`.
- **Rules:** `convention` queue table.

### activities.activity_processing_queue (table)
- **Grain:** one row per queued activity-processing job.
- **Canonical source:** `activities.activity_processing_queue`.
- **Rules:** `convention` queue table.

### activities.race_predictions_raw (table)
- **Grain:** one row per race prediction snapshot.
- **Canonical source:** `activities.race_predictions_raw`.
- **Rules:** `convention` raw predictions.

### activities.segment_match_exclusions (table)
- **Grain:** one row per excluded segment match.
- **Canonical source:** `activities.segment_match_exclusions`.
- **Rules:** `enforced` FKs to activities and segments.

### activities.elevation_reference (table)
- **Grain:** one row per elevation reference point.
- **Canonical source:** `activities.elevation_reference`.
- **Rules:** `convention` reference data.

### activities.activity_resampled_1m (table)
- **Grain:** one row per minute-bucketed activity metrics.
- **Canonical source:** `activities.activity_resampled_1m`.
- **Rules:** `convention` derived aggregate.

### activities.vw_activity_header_stats (view)
- **Grain:** one row per activity with formatted header stats.
- **Canonical source:** derived from `activities.activities`.
- **Rules:** `convention` read-only view.

### activities.vw_activity_summary (view)
- **Grain:** one row per activity summary.
- **Canonical source:** derived from `activities.activities`.
- **Rules:** `convention` read-only view.

### activities.segment_matches (table)
- **Grain:** one row per segment matches (table); see schema.generated.md for columns.
- **Canonical source:** `activities.segment_matches`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.segments (table)
- **Grain:** one row per segments (table); see schema.generated.md for columns.
- **Canonical source:** `activities.segments`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.segments_details (table)
- **Grain:** one row per segments details (table); see schema.generated.md for columns.
- **Canonical source:** `activities.segments_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.steps_raw (table)
- **Grain:** one row per steps raw (table); see schema.generated.md for columns.
- **Canonical source:** `activities.steps_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_activity_segment_metas (table)
- **Grain:** one row per temp activity segment metas (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_activity_segment_metas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segment_details (table)
- **Grain:** one row per temp segment details (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segment_matches_downselect (table)
- **Grain:** one row per temp segment matches downselect (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matches_downselect`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segment_matches_v1 (table)
- **Grain:** one row per temp segment matches v1 (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matches_v1`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segment_matching_activities (table)
- **Grain:** one row per temp segment matching activities (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matching_activities`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segment_matching_segments (table)
- **Grain:** one row per temp segment matching segments (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segment_matching_segments`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.temp_segments_v1 (table)
- **Grain:** one row per temp segments v1 (table); see schema.generated.md for columns.
- **Canonical source:** `activities.temp_segments_v1`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.training_cycle_dates (table)
- **Grain:** one row per training cycle dates (table); see schema.generated.md for columns.
- **Canonical source:** `activities.training_cycle_dates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_elevation_deltas (view)
- **Grain:** one row per vw activity elevation deltas (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_elevation_deltas`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_header_stats (view)
- **Grain:** one row per vw activity header stats (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_header_stats`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_ids_to_sync (view)
- **Grain:** one row per vw activity ids to sync (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_ids_to_sync`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_path_updates (view)
- **Grain:** one row per vw activity path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_segments_aggregated (view)
- **Grain:** one row per vw activity segments aggregated (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_segments_aggregated`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_sleep (view)
- **Grain:** one row per vw activity sleep (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_sleep`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_summary (view)
- **Grain:** one row per vw activity summary (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_summary`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_summary_segment_creation (view)
- **Grain:** one row per vw activity summary segment creation (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_summary_segment_creation`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_training_status (view)
- **Grain:** one row per vw activity training status (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_training_status`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_activity_weights (view)
- **Grain:** one row per vw activity weights (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_activity_weights`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_course_review (view)
- **Grain:** one row per vw course review (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_course_review`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_elevation_smooth (view)
- **Grain:** one row per vw elevation smooth (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_elevation_smooth`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_last_activity_id_by_type (view)
- **Grain:** one row per vw last activity id by type (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_last_activity_id_by_type`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_matched_segment_path_updates (view)
- **Grain:** one row per vw matched segment path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_matched_segment_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_overlapped_segments (view)
- **Grain:** one row per vw overlapped segments (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_overlapped_segments`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_possible_segment_matches (view)
- **Grain:** one row per vw possible segment matches (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_possible_segment_matches`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_pr_hunter (view)
- **Grain:** one row per vw pr hunter (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_pr_hunter`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_report_segment_efforts (view)
- **Grain:** one row per vw report segment efforts (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_report_segment_efforts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_run_timing (view)
- **Grain:** one row per vw run timing (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_run_timing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_comps (view)
- **Grain:** one row per vw segment comps (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_comps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_create_auto_approve (view)
- **Grain:** one row per vw segment create auto approve (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_create_auto_approve`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_create_from_details (view)
- **Grain:** one row per vw segment create from details (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_create_from_details`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_leaderboard (view)
- **Grain:** one row per vw segment leaderboard (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_leaderboard`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_matching_downselect_confidence (view)
- **Grain:** one row per vw segment matching downselect confidence (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_matching_downselect_confidence`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_matching_downselect_scores (view)
- **Grain:** one row per vw segment matching downselect scores (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_matching_downselect_scores`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_path_updates (view)
- **Grain:** one row per vw segment path updates (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_path_updates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segment_racing (view)
- **Grain:** one row per vw segment racing (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segment_racing`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segments_details_ranked (view)
- **Grain:** one row per vw segments details ranked (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segments_details_ranked`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_segments_effort_stats (view)
- **Grain:** one row per vw segments effort stats (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_segments_effort_stats`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_temp_segment_matches_downselect (view)
- **Grain:** one row per vw temp segment matches downselect (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_temp_segment_matches_downselect`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_unmatched_activitites (view)
- **Grain:** one row per vw unmatched activitites (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_unmatched_activitites`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### activities.vw_watch_music_heard (view)
- **Grain:** one row per vw watch music heard (view); see schema.generated.md for columns.
- **Canonical source:** `activities.vw_watch_music_heard`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## api_services

### api_services.api_service_list (table)
- **Grain:** one row per api service list (table); see schema.generated.md for columns.
- **Canonical source:** `api_services.api_service_list`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### api_services.api_tokens (table)
- **Grain:** one row per api tokens (table); see schema.generated.md for columns.
- **Canonical source:** `api_services.api_tokens`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### api_services.credentials (table)
- **Grain:** one row per credentials (table); see schema.generated.md for columns.
- **Canonical source:** `api_services.credentials`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### api_services.function_library (table)
- **Grain:** one row per function library (table); see schema.generated.md for columns.
- **Canonical source:** `api_services.function_library`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### api_services.spotify_auth_metadata (table)
- **Grain:** one row per spotify auth metadata (table); see schema.generated.md for columns.
- **Canonical source:** `api_services.spotify_auth_metadata`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## exercises

### exercises.exercise_attempts (table)
- **Grain:** one row per exercise attempts (table); see schema.generated.md for columns.
- **Canonical source:** `exercises.exercise_attempts`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### exercises.exercise_timers (table)
- **Grain:** one row per exercise timers (table); see schema.generated.md for columns.
- **Canonical source:** `exercises.exercise_timers`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## food

### food.diary_entries (table)
- **Grain:** one row per diary entries (table); see schema.generated.md for columns.
- **Canonical source:** `food.diary_entries`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.foods (table)
- **Grain:** one row per foods (table); see schema.generated.md for columns.
- **Canonical source:** `food.foods`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.recipe_cook_states (table)
- **Grain:** one row per recipe cook states (table); see schema.generated.md for columns.
- **Canonical source:** `food.recipe_cook_states`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.recipe_ingredients (table)
- **Grain:** one row per recipe ingredients (table); see schema.generated.md for columns.
- **Canonical source:** `food.recipe_ingredients`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.recipe_step_foods (table)
- **Grain:** one row per recipe step foods (table); see schema.generated.md for columns.
- **Canonical source:** `food.recipe_step_foods`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.recipe_steps (table)
- **Grain:** one row per recipe steps (table); see schema.generated.md for columns.
- **Canonical source:** `food.recipe_steps`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.recipes (table)
- **Grain:** one row per recipes (table); see schema.generated.md for columns.
- **Canonical source:** `food.recipes`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.tri_tip_events (table)
- **Grain:** one row per tri tip events (table); see schema.generated.md for columns.
- **Canonical source:** `food.tri_tip_events`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.tri_tip_readings (table)
- **Grain:** one row per tri tip readings (table); see schema.generated.md for columns.
- **Canonical source:** `food.tri_tip_readings`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.vw_food_search (view)
- **Grain:** one row per vw food search (view); see schema.generated.md for columns.
- **Canonical source:** `food.vw_food_search`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### food.vw_usual_foods (view)
- **Grain:** one row per vw usual foods (view); see schema.generated.md for columns.
- **Canonical source:** `food.vw_usual_foods`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## health

### health.body_battery_raw (table)
- **Grain:** one row per body battery raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.body_battery_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.body_composition_raw (table)
- **Grain:** one row per body composition raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.body_composition_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.body_dimensions (table)
- **Grain:** one row per body dimensions (table); see schema.generated.md for columns.
- **Canonical source:** `health.body_dimensions`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.fitness_age_raw (table)
- **Grain:** one row per fitness age raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.fitness_age_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.heartrate_raw (table)
- **Grain:** one row per heartrate raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.heartrate_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.hrv_raw (table)
- **Grain:** one row per hrv raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.hrv_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.photo_metadata (table)
- **Grain:** one row per photo metadata (table); see schema.generated.md for columns.
- **Canonical source:** `health.photo_metadata`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.respiration_raw (table)
- **Grain:** one row per respiration raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.respiration_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.sleep_movement_raw (table)
- **Grain:** one row per sleep movement raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.sleep_movement_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.sleep_totals (table)
- **Grain:** one row per sleep totals (table); see schema.generated.md for columns.
- **Canonical source:** `health.sleep_totals`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.spo2_raw (table)
- **Grain:** one row per spo2 raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.spo2_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.stress_raw (table)
- **Grain:** one row per stress raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.stress_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.training_status_raw (table)
- **Grain:** one row per training status raw (table); see schema.generated.md for columns.
- **Canonical source:** `health.training_status_raw`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.vw_activity_weights (view)
- **Grain:** one row per vw activity weights (view); see schema.generated.md for columns.
- **Canonical source:** `health.vw_activity_weights`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.vw_body_comp_interpolation_precalc (view)
- **Grain:** one row per vw body comp interpolation precalc (view); see schema.generated.md for columns.
- **Canonical source:** `health.vw_body_comp_interpolation_precalc`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.vw_heart_rates (view)
- **Grain:** one row per vw heart rates (view); see schema.generated.md for columns.
- **Canonical source:** `health.vw_heart_rates`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### health.weight_target (table)
- **Grain:** one row per weight target (table); see schema.generated.md for columns.
- **Canonical source:** `health.weight_target`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

## logging

### logging.api_logins (table)
- **Grain:** one row per api logins (table); see schema.generated.md for columns.
- **Canonical source:** `logging.api_logins`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.application_events (table)
- **Grain:** one row per application events (table); see schema.generated.md for columns.
- **Canonical source:** `logging.application_events`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.db_size_log (table)
- **Grain:** one row per db size log (table); see schema.generated.md for columns.
- **Canonical source:** `logging.db_size_log`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.db_stats (table)
- **Grain:** one row per db stats (table); see schema.generated.md for columns.
- **Canonical source:** `logging.db_stats`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.health_checks (table)
- **Grain:** one row per health checks (table); see schema.generated.md for columns.
- **Canonical source:** `logging.health_checks`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.music_db_info (table)
- **Grain:** one row per music db info (table); see schema.generated.md for columns.
- **Canonical source:** `logging.music_db_info`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.task_executions (table)
- **Grain:** one row per task executions (table); see schema.generated.md for columns.
- **Canonical source:** `logging.task_executions`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_all_event_history (view)
- **Grain:** one row per vw all event history (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_all_event_history`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_db_biggest_table_chart (view)
- **Grain:** one row per vw db biggest table chart (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_db_biggest_table_chart`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_db_size (view)
- **Grain:** one row per vw db size (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_db_size`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_db_size_chart (view)
- **Grain:** one row per vw db size chart (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_db_size_chart`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_event_analysis (view)
- **Grain:** one row per vw event analysis (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_event_analysis`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_last_login (view)
- **Grain:** one row per vw last login (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_last_login`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### logging.vw_task_analysis (view)
- **Grain:** one row per vw task analysis (view); see schema.generated.md for columns.
- **Canonical source:** `logging.vw_task_analysis`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

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
- **Grain:** one row per game events (table); see schema.generated.md for columns.
- **Canonical source:** `volleyball.game_events`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### volleyball.games (table)
- **Grain:** one row per games (table); see schema.generated.md for columns.
- **Canonical source:** `volleyball.games`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.

### volleyball.points (table)
- **Grain:** one row per points (table); see schema.generated.md for columns.
- **Canonical source:** `volleyball.points`.
- **Rules:** `convention` � not yet individually catalogued; DB constraints per schema.generated.md.
