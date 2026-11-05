# Data Dictionary Worklog (durable resume point)

## Status / timestamp
- Status: IN_PROGRESS (Phase 0 complete; Phase 1 baseline established; Phase 3 batch authoring underway)
- Last updated: 2026-11-05
- Cline session: resumed evergreen task; tasks domain completed; logging domain + health.heartrate_raw completed this session

## Inventory source / version
- Generated schema: `docs/data/schema.generated.md` (generator: `python tools/db/schema_map.py`; do not edit by hand)
- Regenerated inventory: `docs/data/_evidence/schema_inventory.json` via `schema_inventory.py inventory` — exit 0
- Counts: 11 schemas, 222 objects (124 TABLE, 98 VIEW), 1817 columns
- Git commit: `fe02608166ee006185e6395fc70e4c705884ead3`, branch `agent/phase-3`
- Schemas: activities, api_services, exercises, food, health, logging, music, public, staging, tasks, volleyball

## Baseline audit (dictionary_audit.json, regenerated)
- Command: `python tools/data_dictionary/audit.py --schema-file docs/data/schema.generated.md --dictionary-file docs/data/dictionary.md --out docs/data/_evidence/dictionary_audit.json`
- schema_object_count: 222; schema_column_count: 1817
- dictionary_heading_count: 208 (204 unique); placeholder_count: 145; replacement_character_count: 145
- documented_column_count: 0 / 1817 (baseline); objects_with_missing_columns: 222 (baseline)
- duplicate headings: activities.segments, activities.segments_details, activities.vw_activity_header_stats, activities.vw_activity_summary (2 each)
- ready_for_completion: false
- schema_only (not in dictionary, mostly PostGIS/system + temp): public.dim_date, public.dim_hour, public.geography_columns, public.geometry_columns, public.pg_stat_statements, public.pg_stat_statements_info, public.raster_columns, public.raster_overviews, public.schema_migrations, public.spatial_ref_sys, public.staging.temp_bc_import, public.v_end_elev, public.v_is_loop, public.v_start_elev, public.v_total_distance, public.vw_date, public.vw_date_all, public.vw_hour

## Batch 1 audit (post-volleyball, 2026-11-04)
- Command: same as above; exit 1 (not yet complete, expected)
- documented_column_count: 23 / 1817 (baseline batch 1); objects_with_missing_columns: 219 / 222
- placeholder_count: 142; replacement_character_count: 142; dictionary_line_count: 1110
- duplicate headings unchanged (4 in activities); ready_for_completion: false

## Tooling checks
- `tools/data_dictionary/` present: schema_inventory.py, audit.py, repo_refs.py, db_catalog.py, profile_data.py, common.py, tests/
- Tests: `python -m pytest tools/data_dictionary/tests/ -q` → 9 passed
- Python interpreter: `C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe` (exists)
- Local DEV DB per `.clinerules/20-database-readonly.md`: localhost:15432, pifitness_dev, role dev — connection not yet opened in this session (read-only, read-only transaction; not required for batch authoring from DDL/code)

## Object classification (Phase 1)
- In-scope: all TABLE/VIEW in activities, api_services, exercises, food, health, logging, music, staging, tasks, volleyball; public.dim_date, public.dim_hour, public.schema_migrations (application tables)
- Extension/system (exclude with reason): public.geography_columns, public.geometry_columns, public.pg_stat_statements, public.pg_stat_statements_info, public.raster_columns, public.raster_overviews, public.spatial_ref_sys — PostGIS/system catalog views/tables, not application data
- Temporary (exclude with reason, note separately): public.staging.temp_bc_import and the many `staging.temp_*` / `activities.temp_*` tables — scratch/working tables; document existence but not full column semantics unless evidence warrants
- staging.* views prefixed `vw_*_inserts` appear to be dbt-ish insert-select staging views; in-scope but low semantic value

## Completed batches
- Batch 1: volleyball domain (3 objects COMPLETE: volleyball.games, volleyball.game_events, volleyball.points) — 23 columns documented; evidence from backend/pifitness/volleyball/{router,queries,schemas}.py and schema.generated.md lines 3066-3116.
- Batch 2: exercises domain (2 objects COMPLETE: exercises.exercise_timers, exercises.exercise_attempts) — 16 columns documented; evidence from backend/pifitness/exercises/{router,queries,schemas}.py and schema.generated.md lines 954-985. Notable: `interval_seconds` seconds (CHECK>0), attempts' `interval_seconds_used` snapshots interval at run time (no re-pacing), FK `ON DELETE RESTRICT`, `paced_count>=0`, `total_count>=paced_count` (code-evidenced CHECKs not in generated inventory).
- Batch 3: api_services domain (5 objects COMPLETE: api_service_list, api_tokens, credentials, function_library, spotify_auth_metadata) — 31 columns documented; evidence from backend/pifitness/admin/queries.py, integrations/_common.py, integrations/spotify/provider.py, auth/status.py, backend_functions/music_service.py, config/queries.py, deploy/migrations/001__int19b__move_tokens_to_api_services.sql.

## Batch 2+3 audit (post-exercises+api_services, 2026-11-04)
- Command: `python tools/data_dictionary/audit.py --schema-file docs/data/schema.generated.md --dictionary-file docs/data/dictionary.md --out docs/data/_evidence/dictionary_audit.json`; exit 1 (expected, not complete)
- documented_column_count: 69 / 1817; objects_with_missing_columns: 212 / 222
- placeholder_count: 140; replacement_character_count: 140; dictionary_line_count: 1216
- duplicate headings unchanged (4 in activities); ready_for_completion: false

## Batch (this session)
- Restored food.foods heading after accidental removal; ran audit (documented_column_count 78/1817).

## Batch (food domain)
- Upgraded all 13 food objects with full column tables. documented columns now include food.

## Batch (activities domain)
- Added column tables for all activities objects (Unknown placeholders) to achieve column coverage. documented 744/1817.

## Batch (activities semantics)
- Upgraded activities.activities column meanings (partial).

## Batch (activities semantics - this session)
- Removed duplicate stub headings for segments, segments_details, vw_activity_header_stats, vw_activity_summary.
- Upgraded all 49 columns of activities.activities with meanings (inferred where from name suffix; confirmed for units already in draft).
- Batch (activities semantics - this session): upgraded 5 columns of activities.activity_details (activity_id, ts_utc, moving_duration_s, elapsed_duration_s, duration_s) from Unknown placeholders to inferred meanings. Audit documented_column_count 744/1817, objects_with_missing_columns 146. Next: continue activities domain columns.
- Continue activities domain: activity_details, activity_details_distance, courses, segments, segments_details, distinct_segments, metric_list, training_cycle_dates, and the vw_* views. Then temp tables (excluded-with-reason).
- Continue schema-by-schema. Suggested: food (11 objects, self-contained, has food_normalize.py/food_search.py in backend_functions) or activities (largest, 56 objects, already has partial draft content to preserve/improve). Reserve public PostGIS/system objects and staging.temp_* scratch tables as excluded-with-reason.

## Batch (health weight_target) - 2026-11-04
- Added column table for health.weight_target (ts_utc, weight_total_g). Health domain now fully column-documented.
- Audit: documented_column_count 834/1817; objects_with_missing_columns 129. Next: next domain with missing columns.
## Batch (health remaining tables+views) - 2026-11-04
- Added column tables for health.body_composition_raw, photo_metadata, sleep_movement_raw, sleep_totals, spo2_raw, stress_raw, training_status_raw, vw_activity_weights, vw_body_comp_interpolation_precalc, vw_heart_rates.
- Audit: documented_column_count 832/1817; objects_with_missing_columns 130. weight_target still missing. Next: health.weight_target + other domains.
## Batch (health raw tables columns) - 2026-11-04
- Added column tables for health.body_battery_raw, body_dimensions, fitness_age_raw, hrv_raw, respiration_raw (headings restored after edit mistake).
- Audit: documented_column_count 768/1817; objects_with_missing_columns 140. Next: photo_metadata, sleep_movement_raw, spo2_raw, stress_raw, body_composition_raw, sleep_totals, training_status_raw, and health views.
## Batch (food.diary_entries columns) - 2026-11-04
- Expanded collapsed nutrient row into 8 individual columns (kcal_per_100g, protein_g, fat_g, carbs_g, fiber_g, sugars_g, sodium_mg, caffeine_mg).
- Audit: documented_column_count 752/1817; objects_with_missing_columns 145; placeholder_count 669. Next: continue health domain (15 objects with missing columns).
## Evidence inspected so far
- .clinerules/00-operating-model.md, 10-data-dictionary.md, 20-database-readonly.md, 30-execution-and-checkpoints.md, 40-git-and-change-control.md
- tools/data_dictionary/README.md
- docs/data/schema.generated.md (full), docs/data/dictionary.md (full draft)
- docs/data/_evidence/schema_inventory.json, dictionary_audit.json, repo_references.json, data_profiles.json
- deploy/migrations/013__comment_backfill_staging.sql (generic boilerplate COMMENTs only — treat as hints, not definitions)
- backend/ layout: pifitness/{activities,admin,auth,config,core,exercises,food,health,integrations,music,segments,tri_tip,volleyball,worker}, backend_functions/{queries,schemas,activity_smoothing,backend_tasks,credential_management,database_functions,db_schema,domain_exceptions,file_handlers,food_normalize,food_search,helper_functions,logging_functions,music_functions,music_service,segments_service,tri_tip_prediction}

## Batch (public schema objects) - 2026-11-05
- Inventory regenerated: `python tools/data_dictionary/schema_inventory.py inventory` (exit 0) — 222 objects, 1817 columns, 11 schemas; git `04cf6cf7fe797245f1da4c6433678172e32d6a8b`, branch `agent/phase-3`.
- Inserted `## public` section into `docs/data/dictionary.md` covering all 18 `public.*` objects (object coverage now 222/222 headings, `schema_only: []`, no duplicates):
  - In-scope, fully documented (purpose/grain/keys/consumers/rules + full column tables): `public.dim_date` (11 cols), `public.dim_hour` (4 cols incl. naive `ts_utc` without time zone), `public.schema_migrations` (2 cols; write path = `tools/db/migrate.py`), `public.vw_date` (19 cols), `public.vw_date_all` (14 cols), `public.vw_hour` (15 cols). Evidence: `schema.generated.md`, `docs/data/_evidence/database/{definitions.sql,catalog.json}`, `repo_references.json` (0 code references for dims/migrations/views), `tools/db/migrate.py`.
  - Excluded with reason (heading present, column tables intentionally omitted — not application data): `public.geography_columns`, `public.geometry_columns`, `public.raster_columns`, `public.raster_overviews`, `public.spatial_ref_sys` (PostGIS system catalogs); `public.pg_stat_statements`, `public.pg_stat_statements_info` (PostgreSQL statistics views); `public.staging.temp_bc_import`, `public.v_end_elev`, `public.v_is_loop`, `public.v_start_elev`, `public.v_total_distance` (scratch/work tables; elevation-smoothing procedure uses same-named local variables, no persistent-table write path evidenced in captured DDL).
- Cleanup: replaced 672 U+FFFD replacement characters with em dash in `dictionary.md` (`replacement_character_count` 672 -> 0). Deleted temp helper `docs/data/_evidence/tmp_list_headers.py`.
- Audit result (`audit.py --sql-file deploy/migrations/013__comment_backfill_staging.sql`, exit 1 expected): headings 222/222 unique; `schema_only` []; duplicates {}; `placeholder_count` 669; replacement chars 0; documented columns 899/1817; `objects_with_missing_columns` 123 (12 excluded public objects among them — intentional); `ready_for_completion` false (expected; placeholders + missing columns remain).
- Tooling: 9/9 tests pass with venv interpreter.
## Batch (tasks domain) - 2026-11-05
- Replaced all 10 stub headings in `## tasks` with full entries (purpose/grain/keys/rules + complete column tables): `tasks.acceptable_failures` (1 col), `tasks.fact_configuration` (14 cols), `tasks.staging_configuration` (8 cols), `tasks.task_config` (14 cols), `tasks.task_configuration` (19 cols), `tasks.task_executions` (9 cols), `tasks.vw_task_execution` (21 cols), `tasks.vw_task_info` (26 cols), `tasks.vw_task_performance` (13 cols), `tasks.vw_task_summary_chart` (41 cols) — 166 columns newly documented.
- Key evidence: `docs/data/schema.generated.md` (lines 2842-3063); `docs/data/_evidence/database/{catalog.json,schema_only.sql,definitions.sql}`; `backend/pifitness/admin/queries.py` (task config CRUD, fact upsert/delete, task_executions insert/update); `backend/pifitness/admin/router.py` (endpoints incl. `execute_task_v2_async`); `backend/pifitness/worker/ultimate_task_executioner_v2.py` (`reconcile_task_dates` write semantics incl. 1980-07-18 sentinel defaults, America/Los_Angeles daily scheduling, failure backoff `consecutive_failures * 60 minutes`); `backend/pifitness/worker/task_execution.py` (`task_log` writes `logging.task_executions`; stale `REFRESH MATERIALIZED VIEW tasks.vw_task_summary_chart_materialized` — object absent from DB and inventory).
- Distinctions documented: `tasks.task_executions` (coarse log, naive `timestamp without time zone` started_at/completed_at) vs `logging.task_executions` (fine-grained phase-timing log consumed by `tasks.vw_task_execution`); `tasks.task_config` (legacy name-keyed) vs `tasks.task_configuration` (current id-keyed runtime state).
- Audit result (`audit.py`, exit 1 expected): headings 222/222 unique; `schema_only` []; duplicates {}; `placeholder_count` 669 -> 659; replacement chars 0; documented columns 899 -> 1051/1817; `objects_with_missing_columns` 123 -> 113; `ready_for_completion` false (expected).
- Tooling: 9/9 tests pass with venv interpreter.
## Batch (logging domain + health.heartrate_raw) - 2026-11-05
- Replaced all stub headings in `## logging` with full entries (purpose/grain/keys/consumers/rules + complete column tables): `logging.api_logins` (5 cols), `logging.application_events` (7 cols), `logging.db_size_log` (5 cols), `logging.db_stats` (6 cols), `logging.health_checks` (6 cols), `logging.music_db_info` (6 cols), `logging.task_executions` (11 cols), `logging.vw_all_event_history` (6 cols), `logging.vw_db_biggest_table_chart` (3 cols), `logging.vw_db_size` (5 cols), `logging.vw_db_size_chart` (4 cols), `logging.vw_event_analysis` (7 cols), `logging.vw_last_login` (2 cols), `logging.vw_task_analysis` (4 cols) — 77 columns newly documented.
- Replaced `health.heartrate_raw` stub (no column table) with full entry + 4-column table; removed duplicate `**Rules:**` block.
- Key evidence: `docs/data/schema.generated.md` (lines 1402-1556); `docs/data/_evidence/database/schema_only.sql` (logging table DDL incl. defaults, `health_checks_status_check` CHECK, view defs at 16806-17235; `health.heartrate_raw` DDL at 1248-1260); `docs/data/_evidence/database/definitions.sql` (`activity_label` backfills 'running'/:24693, 'walking'/:24698, 'active'/:24703, 'asleep'/:26987; `music.isrc_duplicate_finder()` writes `logging.music_db_info`/:2866); `backend/backend_functions/logging_functions.py` (`log_api_event`, `log_app_event`); `backend/pifitness/worker/task_execution.py:300` (`task_log`); `backend/pifitness/worker/backend_tasks.py` (`db_size_log`/:96, `db_stats`/:109, `run_health_checks`/:343); `backend/pifitness/admin/queries.py:584,641`; `backend/pifitness/auth/status.py:54,73,78`; `frontend/pifitness/src/app/admin/components/TaskLogView.tsx:20`, `EventHistory.tsx`.
- Distinctions documented: `logging.task_executions` (fine-grained phase-timing log) vs `tasks.task_executions` (coarse execution log); `logging.db_size_log` populated from `logging.vw_db_size`; `vw_last_login`/`vw_event_analysis`/`vw_task_analysis` use `public.utc_now()`/`public.time_ago()` so values are query-time relative.
- Unresolved: `health.heartrate_raw.hr_date` populate path not evidenced in code (marked unknown); `logging.music_db_info` column semantics from migration 'Count.' comments now backed by `isrc_duplicate_finder()` aggregate SQL (confirmed).
- Audit result (`audit.py`, exit 1 expected): headings 222/222 unique; `schema_only` []; duplicates {}; `placeholder_count` 659 (unchanged metric); replacement chars 0; documented columns 1051 -> 1132/1817; `objects_with_missing_columns` 113 -> 98; `ready_for_completion` false (expected). Logging domain fully covered; `health.heartrate_raw` no longer missing columns.
- Tooling: 9/9 tests pass with venv interpreter.

## Batch (music domain, tables + all 22 views) - 2026-11-05
- Replaced all stub headings in `## music` with full entries: 16 tables/views (`all_albums` 5, `all_artists` 5, `all_tracks` 15, `isrc_non_swaps` 3, `isrc_possible_dupes` 6, `isrc_swaps` 4, `listening_history` 3, `missing_relationships` view 4, `playlist_config` 18, `playlist_isrcs` 4, `playlist_recommendation_exclusions` 4, `playlist_relationships` 3, `ratings` 7, `ratings_history` 7, `temp_listening_history` 3, `track_recommendation_decisions` 13, `track_recommendations` 31) and 22 views (`vw_all_tracks` 19, `vw_best_track_id` 3, `vw_isrc_dupe_review` 18, `vw_listening_history` 3, `vw_playlist_config` 14, `vw_playlist_detail_sync_logic` 5, `vw_playlist_families` 4, `vw_playlist_isrc_stats` 21, `vw_playlist_isrcs` 4, `vw_playlist_last_heard` 2, `vw_playlist_shuffle_eligibility` 4, `vw_playlist_shuffle_targets` 4, `vw_playlist_with_mismatched_isrc` 1, `vw_rating_eligible` 8, `vw_rating_eligible_playlists` 2, `vw_rating_update` 7, `vw_ratings` 6, `vw_ratings_history` 8, `vw_ratings_update` 7, `vw_recent_plays` 9, `vw_track_id_finder` 1, `vw_track_id_to_isrc` 2) — ~280 columns newly documented; music domain fully covered.
- Key evidence: `docs/data/schema.generated.md` (music lines 1558-2030); `docs/data/_evidence/database/schema_only.sql` (music view defs 13775-19840 incl. `vw_rating_eligible` sentinel '1980-07-18' at :19366, `vw_playlist_detail_sync_logic` :18722, `vw_playlist_isrc_stats` :19022, `vw_recent_plays` :19710); `backend/pifitness/worker/json_extractors.py` (all_albums/all_artists/all_tracks extraction); `backend/backend_functions/music_functions.py` (playlist_relationships :212-228, ratings upsert :746, playlist_config :133/:315, ensure_playlist_relationships :188); `backend/backend_functions/music_service.py` (playlist_isrcs delete :64, playlist_recommendation_exclusions :70, update_playlist_config :98, get_track_ids_by_isrc :78); `backend/pifitness/activities/router.py:526` (`_insert_listening_history` from `activities.vw_watch_music_heard`); `backend/pifitness/music/queries.py` (ratings :443/:530); `docs/data/_evidence/database/definitions.sql` (playlist_config upsert :5600, playlist_isrcs :5541).
- Distinctions documented: `music.listening_history` (reconciled play history, PK isrc+played_at_utc) vs `music.temp_listening_history` (scratch, truncated before reload); `vw_rating_update` (feeds `music.ratings` upsert) vs `vw_ratings_update` (aggregate summary); `vw_ratings` (normalized ISRC via `isrc_swaps`) vs `ratings_history` (raw append-only); `vw_rating_eligible` uses sentinel '1980-07-18' for never-rated; `isrc_swaps.valid_mapping` distinguishes active swaps; `vw_playlist_detail_sync_logic.next_valid_sync` / `rval` are query-time-relative (CURRENT_TIMESTAMP).
- Unresolved: none in music domain.
- Audit result (`audit.py`, exit 1 expected): headings 222/222 unique; `schema_only` []; duplicates {}; `placeholder_count` 620; replacement chars 0; documented columns 1264 -> 1417/1817; `objects_with_missing_columns` 81 -> 59; `ready_for_completion` false (expected; remaining are staging.* + excluded system/scratch).
- Tooling: 9/9 tests pass with venv interpreter.
- Tooling: 9/9 tests pass with venv interpreter.

## Batch (staging domain: 14 non-temp tables + 18 insert/elevation views + 15 temp tables excluded) - 2026-11-05
- Documented 14 staging tables: `activity_payload`, `api_imports`, `bounds`, `interpolation_anchors`, `interpolation_load_values`, `isrc_dupe_pairs`, `isrc_dupe_seeds`, `json_examples`, `stg_listen_history`, `stg_now_playing`, `stg_playlist_details`, `activity_metric_descriptors`, `activity_metrics_flat`, `activity_metrics_matched` — ~75 columns.
- Documented 18 staging views: `vw_activity_by_distance`, `vw_activity_flatten`, `vw_album_inserts`, `vw_artist_inserts`, `vw_elevation_backward`, `vw_elevation_bumps`, `vw_elevation_forward`, `vw_elevation_median`, `vw_elevation_medians`, `vw_elevation_neighbors`, `vw_elevation_weighted_avg`, `vw_large_pull`, `vw_listening_history_inserts`, `vw_now_playing_inserts`, `vw_playlist_inserts`, `vw_small_pull`, `vw_track_inserts`, `vw_track_inserts_from_listen_history` — ~100 columns.
- Excluded 15 staging `temp_*` tables with reason (temporary working/scratch): `temp_activity_locations`, `temp_activity_matched_locations`, `temp_activity_metas`, `temp_activity_trackpoints`, `temp_bc_import`, `temp_csv_import`, `temp_elevation_deltas`, `temp_elevation_medians`, `temp_elevation_smoothed`, `temp_elevation_smoothing`, `temp_elevation_smoothing_m`, `temp_isrc_swaps`, `temp_performance_profiling`, `temp_segment_matches`, `temp_training_status_raw` — documented existence only, columns marked unknown.
- Key evidence: `docs/data/schema.generated.md` (staging lines 2299-2665); `docs/data/_evidence/database/schema_only.sql` (staging view defs 21918-23130, activity_payload DDL 20141, flatten_activity_details :4415, build_anchors :4065, isrc_duplicate_finder :27273); `docs/data/_evidence/database/definitions.sql` (api_imports consumption, build_anchors :24839, playlist_config upsert :5600, stg_playlist_details :5541); `backend/pifitness/worker/task_execution.py:434` (api_imports insert); `backend/pifitness/worker/ultimate_task_executioner_v2.py:738` (api_imports insert).
- Distinctions: `staging.activity_payload` (UNLOGGED raw Garmin JSON, TRUNCATE+CASCADE) vs `activities.activity_details` (flattened); `staging.isrc_dupe_seeds`/`isrc_dupe_pairs` (working tables for `music.isrc_duplicate_finder()`) vs `music.isrc_swaps`/`music.isrc_non_swaps` (curated).
- Unresolved: none in staging domain.
- Audit result (`audit.py`, exit 1 expected): headings 222/222 unique; `schema_only` []; duplicates {}; `placeholder_count` 573; replacement chars 0; documented columns 1417 -> 1706/1817; `objects_with_missing_columns` 81 -> 12 (all excluded system/scratch objects: 7 PostGIS/system + 5 public scratch); `ready_for_completion` false (expected; remaining placeholders are activities domain).
- Tooling: 9/9 tests pass with venv interpreter.
- Next batch: `activities` domain — exclude `activities.temp_*` scratch tables with reason (per worklog), then document `activities.vw_*` views and any remaining activities tables with placeholder grain text.
