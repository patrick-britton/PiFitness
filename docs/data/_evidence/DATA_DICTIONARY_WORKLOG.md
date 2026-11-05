# Data Dictionary Worklog (durable resume point)

## Status / timestamp
- Status: IN_PROGRESS (Phase 0 complete; Phase 1 baseline established; Phase 3 batch authoring begun)
- Last updated: 2026-11-04
- Cline session: first session of this evergreen task

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
- documented_column_count: 0 / 1817; objects_with_missing_columns: 222
- duplicate headings: activities.segments, activities.segments_details, activities.vw_activity_header_stats, activities.vw_activity_summary (2 each)
- ready_for_completion: false
- schema_only (not in dictionary, mostly PostGIS/system + temp): public.dim_date, public.dim_hour, public.geography_columns, public.geometry_columns, public.pg_stat_statements, public.pg_stat_statements_info, public.raster_columns, public.raster_overviews, public.schema_migrations, public.spatial_ref_sys, public.staging.temp_bc_import, public.v_end_elev, public.v_is_loop, public.v_start_elev, public.v_total_distance, public.vw_date, public.vw_date_all, public.vw_hour

## Batch 1 audit (post-volleyball, 2026-11-04)
- Command: same as above; exit 1 (not yet complete, expected)
- documented_column_count: 23 / 1817; objects_with_missing_columns: 219 / 222
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
