================================
       PiFitness Bootstrap
================================

1) PRD
2) TEST
Q) Quit

Select environment: 2

Fetching origin/feature-test...
remote: Enumerating objects: 7, done.
remote: Counting objects: 100% (7/7), done.
remote: Compressing objects: 100% (1/1), done.
remote: Total 4 (delta 3), reused 4 (delta 3), pack-reused 0 (from 0)
Unpacking objects: 100% (4/4), 337 bytes | 337.00 KiB/s, done.
From https://github.com/patrick-britton/PiFitness
   86898b6..5bf97dc  feature-test -> origin/feature-test

Environment: test
Branch:      feature-test
Commit:      5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
Environment: /home/god/.config/pifitness/test.env

Bootstrap and supporting library syntax verified.

Executing test bootstrap...
[autodeploy] starting: /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/autodeploy.sh --environment test --commit 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
[autodeploy] script dir: /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts
[new_run] initialising run ...
[new_run] (1/6) Parse and validate arguments ...
[new_run]     EVIDENCE: environment = test
[new_run]     EVIDENCE: commit = 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
[new_run]     EVIDENCE: through = (not set)
[new_run] (1/6) Parse and validate arguments ... OK (0.0s)
[new_run] (2/6) Locate autodeploy.conf ...
[new_run]     EVIDENCE: script_dir = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts
[new_run]     EVIDENCE: config_file = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/autodeploy.conf
[new_run]     EVIDENCE: config_sha256 = 9d130d09d6305e66...
[new_run]     EVIDENCE: config_bytes = 6493
[new_run] (2/6) Locate autodeploy.conf ... OK (0.0s)
[new_run] (3/6) Load autodeploy.conf ...
[new_run]     EVIDENCE: config_keys_loaded = 22
[new_run] (3/6) Load autodeploy.conf ... OK (0.0s)
[new_run] (4/6) Load Pi env file ...
[new_run]     EVIDENCE: env_file = /home/god/.config/pifitness/test.env
[new_run]     EVIDENCE: env_keys_loaded = 17
[new_run]     EVIDENCE: required_keys_checked = 12 (all present; empty allowed for: PG_PASSWORD)
[new_run]     EVIDENCE: app_env = test (matches test)
[new_run]     EVIDENCE: log_dir = /home/god/pifitness-logs
[new_run] (4/6) Load Pi env file ... OK (0.0s)
[new_run] (5/6) Resolve step list ...
[new_run]     EVIDENCE: step_list_key = TEST_STEPS
[new_run]     EVIDENCE: step_count = 10
[new_run]     EVIDENCE: steps = preflight checkout dependencies db_prepare migrate regression stop_services start_services smoke manual_gate
[new_run] (5/6) Resolve step list ... OK (0.0s)
[new_run] (6/6) Create run directory and state ...
[new_run]     EVIDENCE: log_file = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.log
[new_run]     EVIDENCE: run_dir = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d
[new_run]     EVIDENCE: files_written = run.state, steps
[new_run] (6/6) Create run directory and state ... OK (0.0s)
[new_run] run initialised; handing over to autodeploy.sh
[autodeploy] log file:      /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.log
[autodeploy] run directory: /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d
[startup] startup report ...
[startup] (1/4) Read run state ...
[startup]     EVIDENCE: run_state = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/run.state
[startup]     EVIDENCE: state_keys = 13
[startup]     EVIDENCE: required_keys_present = 13 of 13
[startup]     EVIDENCE: steps_file = 10 step(s)
[startup] (1/4) Read run state ... OK (0.0s)
[startup] (2/4) Run identity ...
[startup]     EVIDENCE: environment = test
[startup]     EVIDENCE: commit = 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
[startup]     EVIDENCE: through = (not set)
[startup]     EVIDENCE: run_started_utc = 2026-11-15T17:43:29Z
[startup]     EVIDENCE: log_file = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.log
[startup]     EVIDENCE: run_dir = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d
[startup]     EVIDENCE: config_file = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/autodeploy.conf
[startup]     EVIDENCE: config_sha256 = 9d130d09d6305e66...
[startup]     EVIDENCE: config_keys_loaded = 22
[startup]     EVIDENCE: env_file = /home/god/.config/pifitness/test.env
[startup]     EVIDENCE: env_keys_loaded = 17
[startup] (2/4) Run identity ... OK (0.0s)
[startup] (3/4) Host ...
[startup]     EVIDENCE: host = pfpi
[startup]     EVIDENCE: user = god
[startup]     EVIDENCE: os = Linux-6.18.50+rpt-rpi-2712-aarch64-with-glibc2.41
[startup]     EVIDENCE: python = 3.13.5
[startup]     EVIDENCE: now_utc = 2026-11-15T17:43:29Z
[startup]     EVIDENCE: terminal_available = yes
[startup] (3/4) Host ... OK (0.0s)
[startup] (4/4) Step plan ...
[startup]     EVIDENCE: step_01/10 = preflight  [built]
[startup]     EVIDENCE: step_02/10 = checkout  [built]
[startup]     EVIDENCE: step_03/10 = dependencies  [built]
[startup]     EVIDENCE: step_04/10 = db_prepare  [built]
[startup]     EVIDENCE: step_05/10 = migrate  [built]
[startup]     EVIDENCE: step_06/10 = regression  [built]
[startup]     EVIDENCE: step_07/10 = stop_services  [built]
[startup]     EVIDENCE: step_08/10 = start_services  [built]
[startup]     EVIDENCE: step_09/10 = smoke  [NOT IMPLEMENTED]
[startup]     EVIDENCE: step_10/10 = manual_gate  [NOT IMPLEMENTED]
[startup]     EVIDENCE: steps_built = 8 of 10
[startup]     EVIDENCE: expected_outcome = stops cleanly at step 9 (smoke): NOT IMPLEMENTED
[startup] (4/4) Step plan ... OK (0.0s)
[startup] startup report complete
[autodeploy] steps to run: 10 (preflight checkout dependencies db_prepare migrate regression stop_services start_services smoke manual_gate)
[STEP 1/10] preflight ...................................... START
    (1/10) Configuration and Pi env file ...
  EVIDENCE: prd_source_env_file = /home/god/.config/pifitness/prd.env (read-only use: DB connection + nightly dumps)
  EVIDENCE: environment = test
  EVIDENCE: app_env = test
  EVIDENCE: commit = 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
  EVIDENCE: config_file = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/autodeploy.conf
  EVIDENCE: env_file = /home/god/.config/pifitness/test.env
  EVIDENCE: tools_file = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/tools.conf
  EVIDENCE: required_keys_checked = 27 (all present)
    (2/10) Terminal ...
  EVIDENCE: terminal_available = yes
    (3/10) Required tools ...
  EVIDENCE: tool_psql = /usr/bin/psql (psql (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2))
  EVIDENCE: tool_pg_dump = /usr/bin/pg_dump (pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2))
  EVIDENCE: tool_pg_restore = /usr/bin/pg_restore (pg_restore (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2))
  EVIDENCE: tool_systemctl = /usr/bin/systemctl (systemd 257 (257.13-1~deb13u1))
  EVIDENCE: tool_git = /usr/bin/git (git version 2.47.3)
  EVIDENCE: tool_uv = /home/god/.local/bin/uv (uv 0.10.11)
  EVIDENCE: tool_node = /usr/bin/node (v24.21.0)
  EVIDENCE: tool_npm = /usr/bin/npm (11.19.0)
  EVIDENCE: tool_dbmate = /usr/local/bin/dbmate (dbmate version 2.36.0)
    (4/10) Paths ...
  EVIDENCE: log_dir = /home/god/pifitness-logs (exists, writable)
  EVIDENCE: backup_dir = /home/god/Documents/DB_Backups (exists, writable)
  EVIDENCE: nightly_dir = /home/god/Documents/DB_Backups (exists, readable)
  EVIDENCE: app_dir_parent = /home/god (exists, writable)
  EVIDENCE: app_dir = /home/god/PiFitness-test (exists)
  EVIDENCE: venv_dir = /home/god/.local/share/pifitness/test-venv (exists; writable)
  EVIDENCE: state_file_dir = /home/god/.config/pifitness (exists; writable)
  EVIDENCE: persistent_resources = none configured
    (5/10) systemd units ...
  EVIDENCE: units_dir = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/systemd (3 service(s), 2 timer(s))
  EVIDENCE: services_list = pifitness-fastapi.service pifitness-hardware-health.service pifitness_agent.service
  EVIDENCE: timers_list = pifitness-hardware-health.timer pifitness_agent.timer
  EVIDENCE: start_units = pifitness-fastapi.service pifitness-hardware-health.timer pifitness_agent.timer
  EVIDENCE: paired_services_not_started_directly = pifitness-hardware-health.service pifitness_agent.service
  EVIDENCE: unit_pifitness-hardware-health.timer = not-found, inactive
  EVIDENCE: unit_pifitness_agent.timer = loaded, active
  EVIDENCE: unit_pifitness-fastapi.service = loaded, active
  EVIDENCE: unit_pifitness-hardware-health.service = not-found, inactive
  EVIDENCE: unit_pifitness_agent.service = loaded, inactive
  EVIDENCE: unit_name_glob = pifitness* (5 loaded unit(s) match)
  EVIDENCE: unit_pifitness-hw-health.service = loaded, inactive
  EVIDENCE: unit_pifitness-hw-health.timer = loaded, active
  EVIDENCE: active_services = pifitness-fastapi.service
  EVIDENCE: active_timers = pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: restore_units = pifitness-fastapi.service pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: unit_install_dir = /etc/systemd/system
  EVIDENCE: privilege_cmd = sudo -n
  EVIDENCE: unit_dir_writable_with_privilege = yes
  EVIDENCE: units_render = ok (placeholders: APP_DIR VENV_DIR)
  EVIDENCE: unit_files_to_move_aside = pifitness-fastapi-test.service pifitness-fastapi.service pifitness-hw-health.service pifitness-hw-health.timer pifitness_agent.service pifitness_agent.timer
    (6/10) PRD database ...
  EVIDENCE: prd_database = localhost:5432/personal_fitness as devuser (PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2))
  EVIDENCE: prd_read_only = off
  EVIDENCE: prd_in_recovery = false
  EVIDENCE: prd_migrations_table = public.schema_migrations absent (treated as 0 applied: first dbmate run)
  EVIDENCE: prd_migrations = latest (none), count 0
  EVIDENCE: test_target = localhost:5432/personal_fitness_test as pifitness_test_app
  EVIDENCE: test_target_is_prd = no
    (7/10) TEST database ...
  EVIDENCE: test_state_at_start = clean
  EVIDENCE: test_database = localhost:5432/personal_fitness_test as pifitness_test_app
  EVIDENCE: test_marker = present
  EVIDENCE: test_migrations_table = public.schema_migrations absent (treated as 0 applied: first dbmate run)
  EVIDENCE: test_clean_baseline = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 18h 4m old)
  EVIDENCE: test_migrations = latest (none), count 0
  EVIDENCE: versions_in_test_not_prd = none
  EVIDENCE: versions_in_prd_not_test = none
  EVIDENCE: schema_diff_lines = 0
    (8/10) Newest nightly snapshot ...
  EVIDENCE: nightly_snapshots_found = 6
  EVIDENCE: newest_snapshot = personal_fitness_20261115_010039.dump (1.2 GiB, 8h 39m old)
  EVIDENCE: snapshot_usable = yes
  EVIDENCE: snapshot_migrations_table = public.schema_migrations absent (treated as 0 applied)
  EVIDENCE: snapshot_migrations = latest (none), count 0
  EVIDENCE: snapshot_matches_live_prd = yes
    (9/10) Decision ...
  EVIDENCE: test_classification = compatible (TEST is identical to PRD)
  EVIDENCE: prompt = database choice [K]eep / [R]estore nightly / [C]lone (waits indefinitely)

TEST database is compatible with PRD (TEST is identical to PRD).
  [K] Keep  [R] Restore most recent nightly  [C] Take a fresh clone
Choice: k
  EVIDENCE: answer = K
  EVIDENCE: decision_source = developer
  EVIDENCE: decision = database action = keep
    (10/10) Plan ...
  EVIDENCE: plan_database = keep the current TEST database
  EVIDENCE: plan_prd_units_stopped_at_step_7 = pifitness-hw-health.timer, pifitness_agent.timer, pifitness-fastapi.service
  EVIDENCE: plan_unit_files_moved_aside_at_step_7 = pifitness-fastapi-test.service pifitness-fastapi.service pifitness-hw-health.service pifitness-hw-health.timer pifitness_agent.service pifitness_agent.timer
  EVIDENCE: plan_units_installed_and_started_at_step_8 = pifitness-fastapi.service, pifitness-hardware-health.timer, pifitness_agent.timer
  EVIDENCE: plan_units_restored_on_exit = pifitness-fastapi.service, pifitness-hw-health.timer, pifitness_agent.timer
  EVIDENCE: plan_input = no further input is needed until step 10 (GO / NO-GO)
  EVIDENCE: preflight = ready: nothing was changed
[STEP 1/10] preflight ...................................... PASS (8.2s)
[STEP 2/10] checkout ....................................... START
    (1/8) Configuration ...
  EVIDENCE: environment = test
  EVIDENCE: requested_sha = 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
  EVIDENCE: app_dir = /home/god/PiFitness-test (previous checkout present)
  EVIDENCE: source_repo = /home/god/PiFitness (contains the commit)
  EVIDENCE: origin_remote = https://github.com/patrick-britton/PiFitness.git (copied onto the new checkout)
  EVIDENCE: staging_dir = /home/god/PiFitness-test.stage-autodeploy-test-20261115T174329Z-5bf97dc
  EVIDENCE: env_file_wiring = symlink backend file backend/.env -> /home/god/.config/pifitness/test.env
  EVIDENCE: previous_checkout_policy = deleted right after the swap
  EVIDENCE: leftover_stage_dirs = 0
    (2/8) Fetch the exact commit ...
      git: From /home/god/PiFitness
      git:  * branch            5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a -> FETCH_HEAD
  EVIDENCE: fetch_method = shallow fetch of the exact SHA from the local repo
  EVIDENCE: origin_remote_set = https://github.com/patrick-britton/PiFitness.git
    (3/8) Verify the checkout ...
  EVIDENCE: actual_sha = 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
  EVIDENCE: clean_tree = yes (git status --porcelain is empty)
  EVIDENCE: expected_paths = backend deploy/scripts (all present)
  EVIDENCE: file_count = 535 on disk, 535 tracked
  EVIDENCE: running_scripts_match_checkout = 36 identical, 0 differ, 0 missing
    (4/8) Persistent resources ...
  EVIDENCE: persistent_resources_preserved = none
    (5/8) Environment file ...
  EVIDENCE: env_file = symlink backend/.env -> /home/god/.config/pifitness/test.env
    (6/8) Final checks ...
  EVIDENCE: untracked_files = 0 (apart from persistent resources and backend/.env)
  EVIDENCE: file_count_final = 536
    (7/8) Swap in ...
  EVIDENCE: swap_method = atomic exchange (renameat2)
  EVIDENCE: live_checkout = /home/god/PiFitness-test at 5bf97dcdd32b7a1f5b094a0fdf678ffc0a30d27a
    (8/8) Previous checkout ...
  WARNING: previous TEST checkout held top-level entries that are NOT in the new one and are being discarded: .pytest_cache
  EVIDENCE: old_checkout_removed = /home/god/PiFitness-test.stage-autodeploy-test-20261115T174329Z-5bf97dc
  EVIDENCE: checkout = ready: /home/god/PiFitness-test is commit 5bf97dcdd32b
    checkout complete (2.2s)
[STEP 2/10] checkout ....................................... PASS (2.3s)
[STEP 3/10] dependencies ................................... START
    (1/5) Configuration and paths ...
  EVIDENCE: app_dir = /home/god/PiFitness-test
  EVIDENCE: venv_dir = /home/god/.local/share/pifitness/test-venv (outside the app dir: yes)
  EVIDENCE: python_project_dir = /home/god/PiFitness-test/backend
  EVIDENCE: frontend_dir = /home/god/PiFitness-test/frontend/pifitness
  EVIDENCE: tools_file = /home/god/pifitness-bootstrap/bootstrap.RXDHmm/deploy/scripts/tools.conf
  EVIDENCE: uv_sync_command = uv sync --locked --no-editable --python /usr/bin/python3 --extra dev --group deployment
    (2/5) Non-pip tools ...
  EVIDENCE: installable_tools = dbmate
  EVIDENCE: dbmate_pinned_version = 2.36.0
  EVIDENCE: dbmate_found = /usr/local/bin/dbmate (2.36.0)
  EVIDENCE: dbmate = already at the pinned version; nothing to install
    (3/5) Python environment ...
  EVIDENCE: uv_version = uv 0.10.11 (/home/god/.local/bin/uv)
  EVIDENCE: uv_lock_sha256 = 700116ff33861ae442a0946475f845a8411e2c4f7476c012344c929a95c7bb8c
  EVIDENCE: venv_before = 51 packages
    uv sync --locked --no-editable --python /usr/bin/python3 --extra dev --group deployment
      Resolved 52 packages in 0.76ms
         Building pifitness @ file:///home/god/PiFitness-test/backend
            Built pifitness @ file:///home/god/PiFitness-test/backend
      Prepared 1 package in 1.25s
      Uninstalled 1 package in 1ms
      Installed 1 package in 1ms
       ~ pifitness==0.1.0 (from file:///home/god/PiFitness-test/backend)
  EVIDENCE: packages_added = 0
  EVIDENCE: packages_removed = 0
  EVIDENCE: packages_changed = 0
  EVIDENCE: venv_after = 51 packages
  EVIDENCE: second_sync = no changes (exit 0, package list identical)
  EVIDENCE: pip_check = All installed packages are compatible
  EVIDENCE: extraneous_packages_after_sync = 0
  EVIDENCE: venv_python = Python 3.13.5 (/home/god/.local/share/pifitness/test-venv/bin/python)
    (4/5) Node dependencies ...
  EVIDENCE: node_version = v24.21.0
  EVIDENCE: npm_version = 11.19.0
  EVIDENCE: package_lock_sha256 = 5bfbafb4ef0e39055a99978cb3e6f47f50910706323a2ccd388eb1ea2c8e538c
    npm ci --no-audit --no-fund --include=dev  (in /home/god/PiFitness-test/frontend/pifitness)

      > pifitness-frontend@0.1.0 postinstall
      > node scripts/copy-zxing-wasm.mjs

      [copy-zxing-wasm] zxing_reader.wasm -> public/wasm/zxing_reader.wasm
      [copy-zxing-wasm] copied 1 file(s) to public/wasm/

      added 270 packages in 17s
  EVIDENCE: npm_ci = ok, /home/god/PiFitness-test/frontend/pifitness/node_modules
    (5/5) Playwright browsers ...
  EVIDENCE: playwright_version = Version 1.60.0
    playwright install chromium
  EVIDENCE: browser_chromium = launches, version 148.0.7778.0
  EVIDENCE: dependencies = reconciled
[STEP 3/10] dependencies ................................... PASS (20.5s)
[STEP 4/10] db_prepare ..................................... START
    (1/8) Decision ...  [elapsed 0s]
  EVIDENCE: db_action = keep
  EVIDENCE: test_state_marker = clean
  EVIDENCE: test_clean_baseline = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 18h 4m old)
  EVIDENCE: prd_database = PRD database was not modified
[STEP 4/10] db_prepare ..................................... SKIPPED (not required)
[STEP 5/10] migrate ........................................ START
    (1/4) Checks ...  [elapsed 0s]
  EVIDENCE: migrations_dir = /home/god/PiFitness-test/deploy/migrations (MIGRATIONS_DIR=deploy/migrations)
  EVIDENCE: migration_files = 16
  EVIDENCE: write_target_is_prd = no (APP_ENV, database name, server and role checked)
  EVIDENCE: test_database = localhost:5432/personal_fitness_test as pifitness_test_app
  EVIDENCE: test_state_marker = clean
    (2/4) Pending migrations ...  [elapsed 0s]
  EVIDENCE: migrations_table = public.schema_migrations absent (first dbmate run: every migration is pending)
  EVIDENCE: applied_before = 0 (latest none)
  EVIDENCE: pending_before = 16
        pending: 20261108000000_int19b_move_tokens_to_api_services.sql
        pending: 20261108000001_int25_repoint_get_spotify_client.sql
        pending: 20261108000002_int31d_repoint_garmin_dispatchers.sql
        pending: 20261108000003_int35_repoint_pirate_dispatchers.sql
        pending: 20261108000004_int38_repoint_worker_tasks.sql
        pending: 20261108000005_comment_backfill_exercises.sql
        pending: 20261108000006_comment_backfill_activities.sql
        pending: 20261108000007_comment_backfill_api_services.sql
        pending: 20261108000008_comment_backfill_food.sql
        pending: 20261108000009_comment_backfill_health.sql
        pending: 20261108000010_comment_backfill_logging.sql
        pending: 20261108000011_comment_backfill_music.sql
        pending: 20261108000012_comment_backfill_staging.sql
        pending: 20261108000013_comment_backfill_tasks.sql
        pending: 20261108000014_comment_backfill_volleyball.sql
        pending: 20261108000015_dev_reset_replay_sentinel.sql
    (3/4) Apply ...  [elapsed 0s]
  EVIDENCE: undo_registered = test_db
  EVIDENCE: test_state_marker = dirty (set before dbmate up)
      Applying: 20261108000000_int19b_move_tokens_to_api_services.sql
      Applied: 20261108000000_int19b_move_tokens_to_api_services.sql in 266.153494ms
      Applying: 20261108000001_int25_repoint_get_spotify_client.sql
      Applied: 20261108000001_int25_repoint_get_spotify_client.sql in 2.036334ms
      Applying: 20261108000002_int31d_repoint_garmin_dispatchers.sql
      Applied: 20261108000002_int31d_repoint_garmin_dispatchers.sql in 1.06225ms
      Applying: 20261108000003_int35_repoint_pirate_dispatchers.sql
      Applied: 20261108000003_int35_repoint_pirate_dispatchers.sql in 1.295544ms
      Applying: 20261108000004_int38_repoint_worker_tasks.sql
      Applied: 20261108000004_int38_repoint_worker_tasks.sql in 1.46595ms
      Applying: 20261108000005_comment_backfill_exercises.sql
      Applied: 20261108000005_comment_backfill_exercises.sql in 2.253721ms
      Applying: 20261108000006_comment_backfill_activities.sql
      Applied: 20261108000006_comment_backfill_activities.sql in 33.036837ms
      Applying: 20261108000007_comment_backfill_api_services.sql
      Applied: 20261108000007_comment_backfill_api_services.sql in 2.236739ms
      Applying: 20261108000008_comment_backfill_food.sql
      Applied: 20261108000008_comment_backfill_food.sql in 4.725087ms
      Applying: 20261108000009_comment_backfill_health.sql
      Applied: 20261108000009_comment_backfill_health.sql in 6.864606ms
      Applying: 20261108000010_comment_backfill_logging.sql
      Applied: 20261108000010_comment_backfill_logging.sql in 4.338369ms
      Applying: 20261108000011_comment_backfill_music.sql
      Applied: 20261108000011_comment_backfill_music.sql in 13.538269ms
      Applying: 20261108000012_comment_backfill_staging.sql
      Applied: 20261108000012_comment_backfill_staging.sql in 15.041293ms
      Applying: 20261108000013_comment_backfill_tasks.sql
      Applied: 20261108000013_comment_backfill_tasks.sql in 7.471323ms
      Applying: 20261108000014_comment_backfill_volleyball.sql
      Applied: 20261108000014_comment_backfill_volleyball.sql in 1.786447ms
      Applying: 20261108000015_dev_reset_replay_sentinel.sql
      Applied: 20261108000015_dev_reset_replay_sentinel.sql in 2.489441ms
  EVIDENCE: migrations_applied = 16 (20261108000000_int19b_move_tokens_to_api_services.sql, 20261108000001_int25_repoint_get_spotify_client.sql, 20261108000002_int31d_repoint_garmin_dispatchers.sql, 20261108000003_int35_repoint_pirate_dispatchers.sql, 20261108000004_int38_repoint_worker_tasks.sql, 20261108000005_comment_backfill_exercises.sql, 20261108000006_comment_backfill_activities.sql, 20261108000007_comment_backfill_api_services.sql, 20261108000008_comment_backfill_food.sql, 20261108000009_comment_backfill_health.sql, 20261108000010_comment_backfill_logging.sql, 20261108000011_comment_backfill_music.sql, 20261108000012_comment_backfill_staging.sql, 20261108000013_comment_backfill_tasks.sql, 20261108000014_comment_backfill_volleyball.sql, 20261108000015_dev_reset_replay_sentinel.sql)
  EVIDENCE: dbmate_up_seconds = 0.4
    (4/4) Verify ...  [elapsed 0s]
      [X] 20261108000000_int19b_move_tokens_to_api_services.sql
      [X] 20261108000001_int25_repoint_get_spotify_client.sql
      [X] 20261108000002_int31d_repoint_garmin_dispatchers.sql
      [X] 20261108000003_int35_repoint_pirate_dispatchers.sql
      [X] 20261108000004_int38_repoint_worker_tasks.sql
      [X] 20261108000005_comment_backfill_exercises.sql
      [X] 20261108000006_comment_backfill_activities.sql
      [X] 20261108000007_comment_backfill_api_services.sql
      [X] 20261108000008_comment_backfill_food.sql
      [X] 20261108000009_comment_backfill_health.sql
      [X] 20261108000010_comment_backfill_logging.sql
      [X] 20261108000011_comment_backfill_music.sql
      [X] 20261108000012_comment_backfill_staging.sql
      [X] 20261108000013_comment_backfill_tasks.sql
      [X] 20261108000014_comment_backfill_volleyball.sql
      [X] 20261108000015_dev_reset_replay_sentinel.sql

      Applied: 16
      Pending: 0
  EVIDENCE: applied_after = 16 (latest 20261108000015)
  EVIDENCE: pending_after = 0
  EVIDENCE: prd_database = PRD database was not modified
[STEP 5/10] migrate ........................................ PASS (0.8s)
[STEP 6/10] regression ..................................... START
    (1/3) Checks ...
  EVIDENCE: tests_conf = /home/god/PiFitness-test/deploy/tests.conf
  EVIDENCE: suites = pytest npm
  EVIDENCE: write_target_is_prd = no (APP_ENV, database name, server and role checked)
  EVIDENCE: test_database = localhost:5432/personal_fitness_test as pifitness_test_app
  EVIDENCE: backend_env = /home/god/PiFitness-test/backend/.env -> /home/god/.config/pifitness/test.env (this run's env file)
    (2/3) Arm: undo action, state marker dirty ...
  EVIDENCE: test_state_marker_before = dirty
  EVIDENCE: undo_registered = test_db
  EVIDENCE: test_state_marker = dirty (the tests write to the TEST database)
    (3/3) Suites ...
    suite pytest: cd $APP_DIR && $VENV_DIR/bin/python -m pytest --junitxml=$REPORT
      ============================= test session starts ==============================
      platform linux -- Python 3.13.5, pytest-9.1.1, pluggy-1.7.0
      rootdir: /home/god/PiFitness-test
      configfile: pytest.ini
      testpaths: tests
      plugins: anyio-4.16.0, asyncio-1.4.0
      asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
      collected 468 items

      tests/test_activities.py .....s.......s.                                 [  3%]
      tests/test_admin.py .....                                                [  4%]
      tests/test_admin_failure_message.py ..                                   [  4%]
      tests/test_api_endpoint_contract.py .                                    [  4%]
      tests/test_auth_status.py .....                                          [  5%]
      tests/test_auth_status_contract.py ....                                  [  6%]
      tests/test_auto_shuffle_pandas_free.py .                                 [  7%]
      tests/test_bootstrap_database_inspection.py ..F.FF.FF...                 [  9%]
      tests/test_bootstrap_database_recovery.py .........                      [ 11%]
      tests/test_config.py ...                                                 [ 12%]
      tests/test_create_segment_details_write.py s..                           [ 12%]
      tests/test_empty_queue_noop.py ...                                       [ 13%]
      tests/test_env_helpers.py .......................                        [ 18%]
      tests/test_failure_propagation.py ..............                         [ 21%]
      tests/test_food_api_contract.py ....................ss.....ss.s........  [ 29%]
      tests/test_food_search_backend.py .............s........sss              [ 35%]
      tests/test_garmin_auth.py ........                                       [ 36%]
      tests/test_garmin_login_flow.py ......s                                  [ 38%]
      tests/test_health.py .                                                   [ 38%]
      tests/test_inspect_test_db.py ............F................              [ 44%]
      tests/test_integration_protocol.py ..                                    [ 45%]
      tests/test_isrc_dupe_endpoints.py ...........                            [ 47%]
      tests/test_isrc_dupes.py ........                                        [ 49%]
      tests/test_json_extractors_garmin_data.py ...                            [ 49%]
      tests/test_music_actions.py ..................s......................... [ 59%]
      .                                                                        [ 59%]
      tests/test_no_prd_default.py F..........                                 [ 61%]
      tests/test_playlist_details_href.py .                                    [ 61%]
      tests/test_playlist_relationships.py ..                                  [ 62%]
      tests/test_process_generator_events.py .ss                               [ 63%]
      tests/test_ratchet.py .......                                            [ 64%]
      tests/test_restore_test_db.py .......F.FFFFFFFFFF..                      [ 69%]
      tests/test_route_smoke.py ..................                             [ 72%]
      tests/test_schema_compare.py ....F.............                          [ 76%]
      tests/test_spotify_oauth_flow.py ...                                     [ 77%]
      tests/test_test_db.py sssssssssss.ssssss                                 [ 81%]
      tests/test_test_db_policy.py .................                           [ 84%]
      tests/test_test_deploy_scripts.py .s...sssssssssssssss                   [ 89%]
      tests/test_test_down.py sssssssF                                         [ 90%]
      tests/test_test_processes.py ssssFFFFs.                                  [ 92%]
      tests/test_test_state.py ......F....                                     [ 95%]
      tests/test_test_up.py .......ss..FFss                                    [ 98%]
      tests/test_token_db_persistence.py ....                                  [ 99%]
      tests/test_verify_spotify_isrc_refresh.py .ss                            [100%]

      =================================== FAILURES ===================================
      ___ test_inspector_is_invoked_read_only_with_prd_authorization_and_evidence ____

          def test_inspector_is_invoked_read_only_with_prd_authorization_and_evidence():
              text = read_script()
              invoke = text.index('inspect_test_db.py')
      >       end = text.index('); then', invoke)
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      E       ValueError: substring not found

      tests/test_bootstrap_database_inspection.py:59: ValueError
      _____ test_policy_actions_are_all_handled_and_route_refreshes_to_recovery ______

          def test_policy_actions_are_all_handled_and_route_refreshes_to_recovery():
              text = read_script()
              decision_block = text[text.index('case "$DB_ACTION" in'):]

              for action in ("reuse_test", "create_from_prd|refresh_from_prd", "abort"):
                  assert action in decision_block

              assert "unknown decision" in decision_block
              assert "latest PRD nightly backup" in decision_block
      >       assert 'restore_test_db.py' in decision_block
      E       assert 'restore_test_db.py' in 'case "$DB_ACTION" in\n    abort)\n        fail "Database inspector returned abort. See the inspection report above."\...abase refresh was applied."\nfi\necho "No migrations applied in this integration step."\necho "No services changed."\n'

      tests/test_bootstrap_database_inspection.py:92: AssertionError
      _______ test_compatible_database_prompts_for_optional_freshness_refresh ________

          def test_compatible_database_prompts_for_optional_freshness_refresh():
              text = read_script()
      >       assert '[[ "$DB_OFFER_REFRESH" == "true" ]]' in text
      E       assert '[[ "$DB_OFFER_REFRESH" == "true" ]]' in '#!/usr/bin/env bash\nset -Eeuo pipefail\nSCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\ncd /\n\nusage() ...abase refresh was applied."\nfi\necho "No migrations applied in this integration step."\necho "No services changed."\n'

      tests/test_bootstrap_database_inspection.py:99: AssertionError
      __ test_old_prd_dump_requirement_and_automatic_migration_are_not_reintroduced __

          def test_old_prd_dump_requirement_and_automatic_migration_are_not_reintroduced():
              text = read_script()
      >       assert "pg_dump" not in text
      E       assert 'pg_dump' not in '#!/usr/bin/... changed."\n'
      E
      E         'pg_dump' is contained here:
      E           live PRD (pg_dump then restore)"
      E         ?           +++++++
      E                       [[ -t 0 ]] || fail "User input required for TEST database choice, but no interactive terminal is available."
      E                       read -r -p "Enter choice [1-3]: " db_choice
      E                       case "$db_choice" in...
      E
      E         ...Full output truncated (253 lines hidden), use '-vv' to show

      tests/test_bootstrap_database_inspection.py:117: AssertionError
      __________ test_current_database_gate_does_not_start_or_stop_services __________

          def test_current_database_gate_does_not_start_or_stop_services():
              text = read_script()
      >       assert 'systemctl is-active --quiet pifitness-fastapi-test.service' in text
      E       assert 'systemctl is-active --quiet pifitness-fastapi-test.service' in '#!/usr/bin/env bash\nset -Eeuo pipefail\nSCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\ncd /\n\nusage() ...abase refresh was applied."\nfi\necho "No migrations applied in this integration step."\necho "No services changed."\n'

      tests/test_bootstrap_database_inspection.py:127: AssertionError
      _____________ test_test_database_on_different_server_fails_closed ______________

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf6b1b70>

          def test_test_database_on_different_server_fails_closed(monkeypatch):
              _install_connections(monkeypatch, test_address="127.0.0.2")
              compare_calls = []

              report = inspect_test_db.inspect_test_database(
                  *_configs(), compare_fn=lambda *a, **k: compare_calls.append((a, k))
              )

              assert report["ok"] is False
              assert report["decision"]["action"] == "abort"
      >       assert "different PostgreSQL server endpoint" in report["error"]
      E       assert 'different PostgreSQL server endpoint' in "PostgreSQL server address '127.0.0.2' is not loopback; expected localhost, 127.0.0.1, or ::1."

      tests/test_inspect_test_db.py:287: AssertionError
      ________________ test_get_settings_raises_when_pg_host_missing _________________

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf6880c0>

          @pytest.mark.regression
          def test_get_settings_raises_when_pg_host_missing(monkeypatch):
              """Fail-loud: get_settings() must raise when PG_HOST is unresolvable."""
              # Block .env loading so backend/.env cannot mask the missing PG_HOST.
              # Patch config.load_dotenv (config.py does `from dotenv import load_dotenv`).
              import pifitness.core.config as cfg
              monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
              # Remove PG_HOST from environment if present
              monkeypatch.delenv("PG_HOST", raising=False)

      >       with pytest.raises(RuntimeError, match="PG_HOST not configured"):
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      E       Failed: DID NOT RAISE RuntimeError

      tests/test_no_prd_default.py:32: Failed
      _ test_pg_restore_uses_admin_and_ownership_transfer_avoids_cluster_wide_reassign _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf6b44b0>
      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_pg_restore_uses_admin_and0')

          def test_pg_restore_uses_admin_and_ownership_transfer_avoids_cluster_wide_reassign(monkeypatch, tmp_path):
              captured = {}
              class Cursor:
                  def execute(self, query, params=None):
                      captured.setdefault("sql", []).append(query)
                  def close(self):
                      pass
              class Connection:
                  autocommit = False
                  def cursor(self):
                      return Cursor()
                  def close(self):
                      pass
              monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")
              monkeypatch.setattr(recovery, "_make_pgpass", lambda directory, config: directory / "pgpass")
              monkeypatch.setattr(recovery, "_connect", lambda *a, **k: Connection())
              monkeypatch.setattr(recovery, "_safe_run", lambda args, **kwargs: captured.update(args=args, env=kwargs["env"]))
      >       recovery._restore_archive(
                  Path("/tmp/prd.dump"), "personal_fitness_test",
                  _config("prod", "personal_fitness", "devuser"), temp_dir=tmp_path,
                  timescaledb_version=None,
              )
      E       TypeError: _restore_archive() got an unexpected keyword argument 'timescaledb_version'

      tests/test_restore_test_db.py:248: TypeError
      ______ test_old_incomplete_journal_never_drops_a_newer_completed_baseline ______

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_old_incomplete_journal_ne0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf262d60>

          def test_old_incomplete_journal_never_drops_a_newer_completed_baseline(tmp_path, monkeypatch):
      >       dbs, conn = _install_fake_recovery(monkeypatch, original_exists=False)
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:289:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf262d60>
      original_exists = False, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      _____ test_recovery_replaces_bad_test_and_keeps_only_verified_prd_baseline _____

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_recovery_replaces_bad_tes0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263d20>

          def test_recovery_replaces_bad_test_and_keeps_only_verified_prd_baseline(tmp_path, monkeypatch):
      >       dbs, conn = _install_fake_recovery(monkeypatch)
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:316:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263d20>
      original_exists = True, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      _______ test_missing_test_database_is_created_directly_without_snapshot ________

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_missing_test_database_is_0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263150>

          def test_missing_test_database_is_created_directly_without_snapshot(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch, original_exists=False)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:340:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263150>
      original_exists = False, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      ____ test_restore_failure_removes_partial_test_database_and_records_failure ____

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_restore_failure_removes_p0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf260a60>

          def test_restore_failure_removes_partial_test_database_and_records_failure(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch, restore_fails=True)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:352:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf260a60>
      original_exists = True, restore_fails = True, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      _ test_live_prd_schema_change_after_nightly_backup_does_not_discard_restored_baseline _

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_live_prd_schema_change_af0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf261080>

          def test_live_prd_schema_change_after_nightly_backup_does_not_discard_restored_baseline(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch, schema_status="different")
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:367:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf261080>
      original_exists = True, restore_fails = False, schema_status = 'different'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      ___________ test_recovery_reconciler_keeps_a_completed_prd_baseline ____________

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_recovery_reconciler_keeps0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf261fd0>

          def test_recovery_reconciler_keeps_a_completed_prd_baseline(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch, original_exists=False)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:382:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf261fd0>
      original_exists = False, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      ___________ test_admin_preflight_failure_does_not_drop_existing_test ___________

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_admin_preflight_failure_d0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf777620>

          def test_admin_preflight_failure_does_not_drop_existing_test(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:400:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf777620>
      original_exists = True, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      ______ test_test_role_must_be_login_enabled_and_non_superuser_before_drop ______

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_test_role_must_be_login_e0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf777540>

          def test_test_role_must_be_login_enabled_and_non_superuser_before_drop(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:414:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf777540>
      original_exists = True, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      _______________ test_test_role_must_be_login_enabled_before_drop _______________

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_test_role_must_be_login_e1')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf262040>

          def test_test_role_must_be_login_enabled_before_drop(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:430:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf262040>
      original_exists = True, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      _____ test_bad_test_password_is_detected_before_dropping_existing_database _____

      tmp_path = PosixPath('/tmp/pytest-of-god/pytest-0/test_bad_test_password_is_dete0')
      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263d20>

          def test_bad_test_password_is_detected_before_dropping_existing_database(tmp_path, monkeypatch):
      >       dbs, _ = _install_fake_recovery(monkeypatch)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

      tests/test_restore_test_db.py:446:
      _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

      monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7ffecf263d20>
      original_exists = True, restore_fails = False, schema_status = 'identical'

          def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
              test_db = "personal_fitness_test"
              prd_db = "personal_fitness"
              dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
              if original_exists:
                  dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
              conn = FakeConnection()
              monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
              monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
              monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
              monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
                  "can_login": True, "is_superuser": False,
              } if role == "pifitness_test_app" else None)
              monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
              monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
              monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
              extension_specs = [
                  {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                  {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
              ]
              monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
      >       monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)
      E       AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitness-test/tools/db/restore_test_db.py'> has no attribute '_install_timescaledb'

      tests/test_restore_test_db.py:154: AttributeError
      ________________ test_validate_pair_rejects_non_loopback_hosts _________________

          @pytest.mark.unit
          def test_validate_pair_rejects_non_loopback_hosts():
              test = _config("test", "fitness_test", host="db.example.com")
              prd = _config("prod", "fitness_prd", host="db.example.com")
      >       with pytest.raises(RuntimeError, match="remote host"):
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      E       AssertionError: Regex pattern did not match.
      E         Expected regex: 'remote host'
      E         Actual message: "Refusing to connect to non-loopback host 'db.example.com'."

      tests/test_schema_compare.py:201: AssertionError
      _____________ test_test_down_critical_failures_are_not_suppressed ______________

          def test_test_down_critical_failures_are_not_suppressed():
              text = read_script("test-down.sh")
      >       assert "if ! sudo systemctl start pifitness-fastapi.service; then" in text
      E       assert 'if ! sudo systemctl start pifitness-fastapi.service; then' in '#!/usr/bin/env bash\nset -euo pipefail\n\nPRD_DIR="${PRD_DIR:-$HOME/PiFitness}"\nTEST_DIR="${TEST_DIR:-$HOME/PiFitnes... "$TEST_DIR"\n\necho "Step 6: remove state file (cleanup complete)"\nrm -f "$STATE_FILE"\n\necho "test-down complete."'

      tests/test_test_down.py:413: AssertionError
      ______________________ test_test_up_stop_order_in_script _______________________

          def test_test_up_stop_order_in_script():
              text = script_text("test-up.sh")
      >       fastapi = text.index("systemctl stop pifitness-fastapi.service")
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      E       ValueError: substring not found

      tests/test_test_processes.py:715: ValueError
      ____________ test_test_up_verifies_writers_quiesced_before_db_clone ____________

          def test_test_up_verifies_writers_quiesced_before_db_clone():
              text = script_text("test-up.sh")
              last_stop = max(
      >           text.index("systemctl stop pifitness-fastapi.service"),
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                  text.index("systemctl stop pifitness_agent.timer"),
                  text.index("systemctl stop pifitness-hardware-health.timer"),
                  text.index("pm2 stop pifitness-next"),
              )
      E       ValueError: substring not found

      tests/test_test_processes.py:727: ValueError
      _____________ test_test_down_restore_gated_on_recorded_prior_state _____________

          def test_test_down_restore_gated_on_recorded_prior_state():
              text = script_text("test-down.sh")
              load = text.index('. "$STATE_FILE"')
      >       assert load < text.index("systemctl start pifitness-fastapi.service")
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      E       ValueError: substring not found

      tests/test_test_processes.py:745: ValueError
      _______ test_integrations_disabled_and_timers_stopped_during_test_window _______

          def test_integrations_disabled_and_timers_stopped_during_test_window():
              text = script_text("test-up.sh")
              for integration in ("SPOTIFY_ENABLED", "GARMIN_ENABLED", "PIRATE_ENABLED"):
                  assert integration in text
      >       assert "systemctl stop pifitness_agent.timer" in text
      E       assert 'systemctl stop pifitness_agent.timer' in '\ufeff#!/usr/bin/env bash\nSCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\nset -euo pipefail\n\nPRD_DIR="... (marker=\'${marker:-<none>}\')"\nfi\n\ncurrent_phase="complete"\nwrite_state\ntrap - EXIT\necho "test-up complete."\n'

      tests/test_test_processes.py:758: AssertionError
      __________________ test_state_written_before_any_prd_shutdown __________________

          @pytest.mark.unit
          def test_state_written_before_any_prd_shutdown():
              lines = read_script("test-up.sh").splitlines()
      >       capture_index = next(
                  i for i, line in enumerate(lines)
                  if "Capture the initial active state" in line
              )
      E       StopIteration

      tests/test_test_state.py:182: StopIteration
      _____________________ test_test_up_starts_test_units_fatal _____________________

          @pytest.mark.unit
          def test_test_up_starts_test_units_fatal():
              text = read_script("test-up.sh")
              start = text.index('echo "Step 8: start test units"')
              verify = text.index('echo "Step 9: verify the complete test stack"')
              complete = text.index('echo "test-up complete."')
              block = text[start:verify]
              assert start < verify < complete
      >       assert 'sudo systemctl start pifitness-fastapi-test.service' in block
      E       assert 'sudo systemctl start pifitness-fastapi-test.service' in 'echo "Step 8: start test units"\n# Start test units from the systemd directory (filename contains "-test").\n# No uni...d_fail "failed to start PM2 test process pifitness-next-test"\nfi\ncurrent_phase="test-units-started"\nwrite_state\n\n'

      tests/test_test_up.py:241: AssertionError
      _________ test_test_up_verifies_complete_test_stack_before_completion __________

          @pytest.mark.unit
          def test_test_up_verifies_complete_test_stack_before_completion():
              text = read_script("test-up.sh")
              verify = text.index('echo "Step 9: verify the complete test stack"')
              complete = text.index('echo "test-up complete."')
              tail = text[verify:complete]
              for probe in (
                  "systemctl is-active pifitness-fastapi-test.service",
                  "pm2 describe pifitness-next-test",
                  "health_body",
                  '"status"',
                  "curl -fsS http://127.0.0.1:8000/api/health",
                  "curl -fsS http://127.0.0.1:3000",
                  "$HTTPS_HOSTNAME",
                  "current_setting('pifitness.env', true)",
                  "recover_and_fail",
              ):
      >           assert probe in tail, "missing verification probe: %s" % probe
      E           AssertionError: missing verification probe: systemctl is-active pifitness-fastapi-test.service
      E           assert 'systemctl is-active pifitness-fastapi-test.service' in 'echo "Step 9: verify the complete test stack"\n# systemd must report all test units active.\n# No unit name is hardco...is not marked for test use (marker=\'${marker:-<none>}\')"\nfi\n\ncurrent_phase="complete"\nwrite_state\ntrap - EXIT\n'

      tests/test_test_up.py:263: AssertionError
      =============================== warnings summary ===============================
      ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1
        /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
          from starlette.testclient import TestClient as TestClient  # noqa

      ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53
        /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53: DeprecationWarning: The anyio.abc.BlockingPortal alias is deprecated, use anyio.from_thread.BlockingPortal instead.
          _PortalFactoryType = Callable[[], AbstractContextManager[anyio.abc.BlockingPortal]]

      backend/pifitness/volleyball/schemas.py:62
        /home/god/PiFitness-test/backend/pifitness/volleyball/schemas.py:62: PydanticDeprecatedSince20: Using extra keyword arguments on `Field` is deprecated and will be removed. Use `json_schema_extra` instead. (Extra keys: 'strip_whitespace'). Deprecated in Pydantic V2.0 to be removed in V3.0. See Pydantic V2 Migration Guide at https://errors.pydantic.dev/2.13/migration/
          message: str = Field(

      -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
      - generated xml file: /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/pytest.xml -
      =========================== short test summary info ============================
      SKIPPED [1] tests/test_activities.py:223: quarantined: asserts SQL text and cursor call counts on a MagicMock connection; the strict-threshold semantics need a real database
      SKIPPED [1] tests/test_activities.py:668: quarantined: asserts the exact TRUNCATE string and the execute_values template
      SKIPPED [1] tests/test_create_segment_details_write.py:30: quarantined: asserts the SQL text of three qec calls by index
      SKIPPED [1] tests/test_food_api_contract.py:501: quarantined: reads SQL parameters by position and counts placeholder groups
      SKIPPED [1] tests/test_food_api_contract.py:566: quarantined: reads SQL parameters by position
      SKIPPED [1] tests/test_food_api_contract.py:705: quarantined: asserts SQL fragments
      SKIPPED [1] tests/test_food_api_contract.py:737: quarantined: asserts SQL fragments
      SKIPPED [1] tests/test_food_api_contract.py:810: quarantined: round-trips through a fake database the test itself writes and asserts SQL text
      SKIPPED [1] tests/test_food_search_backend.py:462: quarantined: a mocked database cannot exercise the CHECK constraint and the test hard-codes the allowed set
      SKIPPED [1] tests/test_food_search_backend.py:716: quarantined: depends on a fixture under .features/ outside the test tree
      SKIPPED [1] tests/test_food_search_backend.py:733: quarantined: depends on a fixture under .features/ outside the test tree
      SKIPPED [1] tests/test_food_search_backend.py:767: quarantined: depends on a fixture under .features/ outside the test tree
      SKIPPED [1] tests/test_garmin_login_flow.py:236: Set GARMIN_LIVE_TEST=1 to run real login verification
      SKIPPED [1] tests/test_music_actions.py:392: quarantined: asserts SQL fragments and positional parameter order
      SKIPPED [1] tests/test_process_generator_events.py:62: quarantined: never collected because a rename dropped the underscore; patches private names that may no longer exist
      SKIPPED [1] tests/test_process_generator_events.py:115: quarantined: never collected because a rename dropped the underscore; patches private names that may no longer exist
      SKIPPED [1] tests/test_test_db.py:328: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:341: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:365: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:379: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:393: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:406: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:416: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:426: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:437: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:451: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:466: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:497: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:521: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:547: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:574: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:601: Git bash and git are required
      SKIPPED [1] tests/test_test_db.py:625: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:102: Git bash is required
      SKIPPED [1] tests/test_test_deploy_scripts.py:165: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:184: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:205: Git bash and git are required
      SKIPPED [2] tests/test_test_deploy_scripts.py:226: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:251: Git bash and git are required
      SKIPPED [2] tests/test_test_deploy_scripts.py:280: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:301: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:318: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:335: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:362: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:382: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:419: Git bash and git are required
      SKIPPED [1] tests/test_test_deploy_scripts.py:440: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:204: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:233: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:267: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:295: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:324: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:353: Git bash and git are required
      SKIPPED [1] tests/test_test_down.py:388: Git bash and git are required
      SKIPPED [1] tests/test_test_processes.py:596: Git bash and git are required
      SKIPPED [1] tests/test_test_processes.py:642: Git bash and git are required
      SKIPPED [1] tests/test_test_processes.py:656: Git bash and git are required
      SKIPPED [1] tests/test_test_processes.py:686: Git bash and git are required
      SKIPPED [1] tests/test_test_processes.py:764: Git bash is required
      SKIPPED [1] tests/test_test_up.py:131: Git bash and git are required
      SKIPPED [1] tests/test_test_up.py:144: Git bash and git are required
      SKIPPED [1] tests/test_test_up.py:267: Git bash and git are required
      SKIPPED [1] tests/test_test_up.py:284: Git bash and git are required
      SKIPPED [1] tests/test_verify_spotify_isrc_refresh.py:59: quarantined: mutates real database rows for a hard-coded playlist id
      SKIPPED [1] tests/test_verify_spotify_isrc_refresh.py:87: quarantined: compares live Spotify state with the database, so it can fail without any code change
      FAILED tests/test_bootstrap_database_inspection.py::test_inspector_is_invoked_read_only_with_prd_authorization_and_evidence - ValueError: substring not found
      FAILED tests/test_bootstrap_database_inspection.py::test_policy_actions_are_all_handled_and_route_refreshes_to_recovery - assert 'restore_test_db.py' in 'case "$DB_ACTION" in\n    abort)\n        f...
      FAILED tests/test_bootstrap_database_inspection.py::test_compatible_database_prompts_for_optional_freshness_refresh - assert '[[ "$DB_OFFER_REFRESH" == "true" ]]' in '#!/usr/bin/env bash\nset -...
      FAILED tests/test_bootstrap_database_inspection.py::test_old_prd_dump_requirement_and_automatic_migration_are_not_reintroduced - assert 'pg_dump' not in '#!/usr/bin/... changed."\n'
      FAILED tests/test_bootstrap_database_inspection.py::test_current_database_gate_does_not_start_or_stop_services - assert 'systemctl is-active --quiet pifitness-fastapi-test.service' in '#!/...
      FAILED tests/test_inspect_test_db.py::test_test_database_on_different_server_fails_closed - assert 'different PostgreSQL server endpoint' in "PostgreSQL server address...
      FAILED tests/test_no_prd_default.py::test_get_settings_raises_when_pg_host_missing - Failed: DID NOT RAISE RuntimeError
      FAILED tests/test_restore_test_db.py::test_pg_restore_uses_admin_and_ownership_transfer_avoids_cluster_wide_reassign - TypeError: _restore_archive() got an unexpected keyword argument 'timescale...
      FAILED tests/test_restore_test_db.py::test_old_incomplete_journal_never_drops_a_newer_completed_baseline - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_recovery_replaces_bad_test_and_keeps_only_verified_prd_baseline - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_missing_test_database_is_created_directly_without_snapshot - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_restore_failure_removes_partial_test_database_and_records_failure - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_live_prd_schema_change_after_nightly_backup_does_not_discard_restored_baseline - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_recovery_reconciler_keeps_a_completed_prd_baseline - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_admin_preflight_failure_does_not_drop_existing_test - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_test_role_must_be_login_enabled_and_non_superuser_before_drop - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_test_role_must_be_login_enabled_before_drop - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_restore_test_db.py::test_bad_test_password_is_detected_before_dropping_existing_database - AttributeError: <module 'tools.db.restore_test_db' from '/home/god/PiFitnes...
      FAILED tests/test_schema_compare.py::test_validate_pair_rejects_non_loopback_hosts - AssertionError: Regex pattern did not match.
      FAILED tests/test_test_down.py::test_test_down_critical_failures_are_not_suppressed - assert 'if ! sudo systemctl start pifitness-fastapi.service; then' in '#!/u...
      FAILED tests/test_test_processes.py::test_test_up_stop_order_in_script - ValueError: substring not found
      FAILED tests/test_test_processes.py::test_test_up_verifies_writers_quiesced_before_db_clone - ValueError: substring not found
      FAILED tests/test_test_processes.py::test_test_down_restore_gated_on_recorded_prior_state - ValueError: substring not found
      FAILED tests/test_test_processes.py::test_integrations_disabled_and_timers_stopped_during_test_window - assert 'systemctl stop pifitness_agent.timer' in '\ufeff#!/usr/bin/env bash...
      FAILED tests/test_test_state.py::test_state_written_before_any_prd_shutdown - StopIteration
      FAILED tests/test_test_up.py::test_test_up_starts_test_units_fatal - assert 'sudo systemctl start pifitness-fastapi-test.service' in 'echo "Step...
      FAILED tests/test_test_up.py::test_test_up_verifies_complete_test_stack_before_completion - AssertionError: missing verification probe: systemctl is-active pifitness-f...
      =========== 27 failed, 374 passed, 67 skipped, 3 warnings in 12.59s ============
  EVIDENCE: suite_pytest_command = cd $APP_DIR && $VENV_DIR/bin/python -m pytest --junitxml=$REPORT
  EVIDENCE: suite_pytest_exit_code = 1
  EVIDENCE: suite_pytest_tests = passed 374, failed 27, skipped 67 (total 468)
  EVIDENCE: suite_pytest_duration = 14.3s
  EVIDENCE: suite_pytest_report = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/pytest.xml
  EVIDENCE: suite_pytest_result = FAIL (exit status 1)
        failed: tests.test_bootstrap_database_inspection > test_inspector_is_invoked_read_only_with_prd_authorization_and_evidence
        failed: tests.test_bootstrap_database_inspection > test_policy_actions_are_all_handled_and_route_refreshes_to_recovery
        failed: tests.test_bootstrap_database_inspection > test_compatible_database_prompts_for_optional_freshness_refresh
        failed: tests.test_bootstrap_database_inspection > test_old_prd_dump_requirement_and_automatic_migration_are_not_reintroduced
        failed: tests.test_bootstrap_database_inspection > test_current_database_gate_does_not_start_or_stop_services
        failed: tests.test_inspect_test_db > test_test_database_on_different_server_fails_closed
        failed: tests.test_no_prd_default > test_get_settings_raises_when_pg_host_missing
        failed: tests.test_restore_test_db > test_pg_restore_uses_admin_and_ownership_transfer_avoids_cluster_wide_reassign
        failed: tests.test_restore_test_db > test_old_incomplete_journal_never_drops_a_newer_completed_baseline
        failed: tests.test_restore_test_db > test_recovery_replaces_bad_test_and_keeps_only_verified_prd_baseline
        failed: tests.test_restore_test_db > test_missing_test_database_is_created_directly_without_snapshot
        failed: tests.test_restore_test_db > test_restore_failure_removes_partial_test_database_and_records_failure
        failed: tests.test_restore_test_db > test_live_prd_schema_change_after_nightly_backup_does_not_discard_restored_baseline
        failed: tests.test_restore_test_db > test_recovery_reconciler_keeps_a_completed_prd_baseline
        failed: tests.test_restore_test_db > test_admin_preflight_failure_does_not_drop_existing_test
        failed: tests.test_restore_test_db > test_test_role_must_be_login_enabled_and_non_superuser_before_drop
        failed: tests.test_restore_test_db > test_test_role_must_be_login_enabled_before_drop
        failed: tests.test_restore_test_db > test_bad_test_password_is_detected_before_dropping_existing_database
        failed: tests.test_schema_compare > test_validate_pair_rejects_non_loopback_hosts
        failed: tests.test_test_down > test_test_down_critical_failures_are_not_suppressed
        ... and 7 more
        last output lines:
          FAILED tests/test_restore_test_db.py::test_recovery_reconciler_keeps_a_completed_prd_baseline - AttributeError: <module 'tools.db.restore_test_
          FAILED tests/test_restore_test_db.py::test_admin_preflight_failure_does_not_drop_existing_test - AttributeError: <module 'tools.db.restore_test
          FAILED tests/test_restore_test_db.py::test_test_role_must_be_login_enabled_and_non_superuser_before_drop - AttributeError: <module 'tools.db.re
          FAILED tests/test_restore_test_db.py::test_test_role_must_be_login_enabled_before_drop - AttributeError: <module 'tools.db.restore_test_db' fro
          FAILED tests/test_restore_test_db.py::test_bad_test_password_is_detected_before_dropping_existing_database - AttributeError: <module 'tools.db.
          FAILED tests/test_schema_compare.py::test_validate_pair_rejects_non_loopback_hosts - AssertionError: Regex pattern did not match.
          FAILED tests/test_test_down.py::test_test_down_critical_failures_are_not_suppressed - assert 'if ! sudo systemctl start pifitness-fastapi.servi
          FAILED tests/test_test_processes.py::test_test_up_stop_order_in_script - ValueError: substring not found
          FAILED tests/test_test_processes.py::test_test_up_verifies_writers_quiesced_before_db_clone - ValueError: substring not found
          FAILED tests/test_test_processes.py::test_test_down_restore_gated_on_recorded_prior_state - ValueError: substring not found
          FAILED tests/test_test_processes.py::test_integrations_disabled_and_timers_stopped_during_test_window - assert 'systemctl stop pifitness_agent.
          FAILED tests/test_test_state.py::test_state_written_before_any_prd_shutdown - StopIteration
          FAILED tests/test_test_up.py::test_test_up_starts_test_units_fatal - assert 'sudo systemctl start pifitness-fastapi-test.service' in 'echo "Ste
          FAILED tests/test_test_up.py::test_test_up_verifies_complete_test_stack_before_completion - AssertionError: missing verification probe: systemc
          =========== 27 failed, 374 passed, 67 skipped, 3 warnings in 12.59s ============
    suite npm: cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT

      > pifitness-frontend@0.1.0 test
      > vitest run --reporter=junit --outputFile=/home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/npm.xml

      (!) Your Vite config uses features that are unsupported by `configLoader: 'native'`, which is planned to become the default in a future major version of Vite:
        - ESM syntax in a file loaded as CommonJS (vitest.config.ts:11:1). Use a `.mjs` extension or set `"type": "module"` in the closest package.json
      Set `VITE_CONFIG_NATIVE_IGNORE_WARNING=true` to suppress this warning.
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLMediaElement's play() method
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
      JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/npm.xml
  EVIDENCE: suite_npm_command = cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT
  EVIDENCE: suite_npm_exit_code = 1
  EVIDENCE: suite_npm_tests = passed 352, failed 3, skipped 0 (total 355)
  EVIDENCE: suite_npm_duration = 30.9s
  EVIDENCE: suite_npm_report = /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/npm.xml
  EVIDENCE: suite_npm_result = FAIL (exit status 1)
        failed: src/app/food/components/__tests__/RecipeCreate.test.tsx > RecipeCreate > save carries built steps; skip path saves with steps: []
        failed: src/app/food/components/__tests__/RecipeSteps.test.tsx > RecipeSteps > adds one step at a time with default 60s timer
        failed: src/app/food/components/__tests__/RecipeSteps.test.tsx > RecipeSteps > edits and removes a step, renumbering survivors
        last output lines:
          Not implemented: HTMLMediaElement's play() method
          Not implemented: HTMLMediaElement's play() method
          Not implemented: HTMLMediaElement's play() method
          Not implemented: HTMLMediaElement's play() method
          Not implemented: HTMLMediaElement's play() method
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
          JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/npm.xml
  WARNING: the TEST state marker stays 'dirty': the exit handler restores the clean baseline
[STEP 6/10] regression ..................................... FAIL (45.5s)
  REASON: 2 of 2 suite(s) failed: pytest, npm
  WHERE: /home/god/PiFitness-test/deploy/tests.conf
  last output:
    |         Not implemented: HTMLMediaElement's play() method
    |         Not implemented: HTMLMediaElement's play() method
    |         Not implemented: HTMLMediaElement's play() method
    |         Not implemented: HTMLMediaElement's play() method
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         Not implemented: HTMLCanvasElement's getContext() method: without installing the canvas npm package
    |         JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d/reports/npm.xml
    | WARNING: the TEST state marker stays 'dirty': the exit handler restores the clean baseline
[autodeploy] ERROR: helper failed: run_step.py exited with status 1
[autodeploy] ERROR:   called as: run_step.py --run-dir /home/god/pifitness-logs/autodeploy-test-20261115T174329Z-5bf97dc.d --step regression --number 6 --total 10
[autodeploy] ERROR: step 6/10 (regression) failed with exit status 1
[autodeploy] run ended with status 1; no further steps will run; entering cleanup...

[cleanup] running cleanup; Ctrl-C is ignored until it finishes
(1/3) Assess the run ...
    EVIDENCE: exit_status = 1
    EVIDENCE: steps_recorded = 6 of 10
    EVIDENCE: step_statuses = FAIL=1, PASS=4, SKIPPED=1
    EVIDENCE: outcome = failure
(1/3) Assess the run ... OK
(2/3) Undo registered actions (last registered first) ...
    EVIDENCE: undo_actions_registered = 2 (test_db, checkout)
[UNDO 1/2] test_db ......................................... START
  EVIDENCE: outcome = failure
  EVIDENCE: test_state_marker = dirty
  EVIDENCE: baseline_dump = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 18h 5m old)
    rolling the TEST database back to test_baseline_20261114T233555Z.dump
    dropping database personal_fitness_test (other sessions are terminated)
    creating database personal_fitness_test
    pg_restore test_baseline_20261114T233555Z.dump  (1.2 GiB)
  ... still running (31s)
      ... still running (30s)
  ... still running (61s)
      ... still running (60s)
      ... still running (90s)
  ... still running (121s)
      ... still running (120s)
  ... still running (151s)
      ... still running (150s)
  ... still running (181s)
      ... still running (180s)
  ... still running (211s)
      ... still running (210s)
  ... still running (241s)
      ... still running (240s)
  ... still running (271s)
      ... still running (270s)
  ... still running (301s)
      ... still running (300s)
  ... still running (331s)
      ... still running (330s)
  ... still running (361s)
      ... still running (360s)
  ... still running (391s)
      ... still running (390s)

