I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

# Your Task
- Regression testing seems complete with the bootstrapper. Before moving on to step-smoke.py, I'd like to change the output of the bootstrapper to only show the progress and suppress from display all evidence, instead routing all the evidence and summary to an evergreen bootstrap_log file located at the `LOG_DIR` directory.
- I would then want a companion powershell or python script to pull that file over ssh from the pi into the local `deploy/` directory so it can be referenced. If it's possible to PUSH this file to the dev machine at the conclusion of the bootstrapping (whether a pass or fail), that would be preferred to having to execute another command.
- Before beginning to build, make a plan and advise how difficult and extensive this request is, and how much risk it introduces into the existing scripts.
- A current sample of the last bootstrap console output is at the bottom of this file.

# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
4. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
5. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
6. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
7. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.

# Latest pi-shell/bootstrap output:
[STEP 3/10] dependencies ................................... START
    (1/5) Configuration and paths ...
  EVIDENCE: app_dir = /home/god/PiFitness-test
  EVIDENCE: venv_dir = /home/god/.local/share/pifitness/test-venv (outside the app dir: yes)
  EVIDENCE: python_project_dir = /home/god/PiFitness-test/backend
  EVIDENCE: frontend_dir = /home/god/PiFitness-test/frontend/pifitness
  EVIDENCE: tools_file = /home/god/pifitness-bootstrap/bootstrap.pgRREA/deploy/scripts/tools.conf
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
      Prepared 1 package in 1.27s
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
[STEP 3/10] dependencies ................................... PASS (20.2s)
[STEP 4/10] db_prepare ..................................... START
    (1/8) Decision ...  [elapsed 0s]
  EVIDENCE: db_action = keep
  EVIDENCE: test_state_marker = clean
  EVIDENCE: test_clean_baseline = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 1d 20h old)
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
      Applied: 20261108000000_int19b_move_tokens_to_api_services.sql in 269.448582ms
      Applying: 20261108000001_int25_repoint_get_spotify_client.sql
      Applied: 20261108000001_int25_repoint_get_spotify_client.sql in 1.720286ms
      Applying: 20261108000002_int31d_repoint_garmin_dispatchers.sql
      Applied: 20261108000002_int31d_repoint_garmin_dispatchers.sql in 1.113697ms
      Applying: 20261108000003_int35_repoint_pirate_dispatchers.sql
      Applied: 20261108000003_int35_repoint_pirate_dispatchers.sql in 1.386473ms
      Applying: 20261108000004_int38_repoint_worker_tasks.sql
      Applied: 20261108000004_int38_repoint_worker_tasks.sql in 1.576731ms
      Applying: 20261108000005_comment_backfill_exercises.sql
      Applied: 20261108000005_comment_backfill_exercises.sql in 2.090543ms
      Applying: 20261108000006_comment_backfill_activities.sql
      Applied: 20261108000006_comment_backfill_activities.sql in 31.247435ms
      Applying: 20261108000007_comment_backfill_api_services.sql
      Applied: 20261108000007_comment_backfill_api_services.sql in 2.268653ms
      Applying: 20261108000008_comment_backfill_food.sql
      Applied: 20261108000008_comment_backfill_food.sql in 4.740804ms
      Applying: 20261108000009_comment_backfill_health.sql
      Applied: 20261108000009_comment_backfill_health.sql in 6.702976ms
      Applying: 20261108000010_comment_backfill_logging.sql
      Applied: 20261108000010_comment_backfill_logging.sql in 4.476546ms
      Applying: 20261108000011_comment_backfill_music.sql
      Applied: 20261108000011_comment_backfill_music.sql in 13.422879ms
      Applying: 20261108000012_comment_backfill_staging.sql
      Applied: 20261108000012_comment_backfill_staging.sql in 15.181961ms
      Applying: 20261108000013_comment_backfill_tasks.sql
      Applied: 20261108000013_comment_backfill_tasks.sql in 7.429528ms
      Applying: 20261108000014_comment_backfill_volleyball.sql
      Applied: 20261108000014_comment_backfill_volleyball.sql in 1.728379ms
      Applying: 20261108000015_dev_reset_replay_sentinel.sql
      Applied: 20261108000015_dev_reset_replay_sentinel.sql in 2.662168ms
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
[STEP 5/10] migrate ........................................ PASS (0.9s)
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
      collected 243 items

      tests/test_activities.py ............                                    [  4%]
      tests/test_admin.py .....                                                [  6%]
      tests/test_admin_failure_message.py ..                                   [  7%]
      tests/test_api_endpoint_contract.py .                                    [  8%]
      tests/test_auth_status.py .....                                          [ 10%]
      tests/test_auth_status_contract.py ....                                  [ 11%]
      tests/test_auto_shuffle_pandas_free.py .                                 [ 12%]
      tests/test_config.py ...                                                 [ 13%]
      tests/test_create_segment_details_write.py ..                            [ 14%]
      tests/test_empty_queue_noop.py ...                                       [ 15%]
      tests/test_failure_propagation.py ..............                         [ 21%]
      tests/test_food_api_contract.py ............................             [ 32%]
      tests/test_food_search_backend.py ....................                   [ 41%]
      tests/test_garmin_auth.py .......                                        [ 44%]
      tests/test_garmin_login_flow.py ......                                   [ 46%]
      tests/test_health.py .                                                   [ 46%]
      tests/test_integration_protocol.py ..                                    [ 47%]
      tests/test_isrc_dupe_endpoints.py ..........                             [ 51%]
      tests/test_isrc_dupes.py ........                                        [ 55%]
      tests/test_json_extractors_garmin_data.py ...                            [ 56%]
      tests/test_music_actions.py .....................................        [ 71%]
      tests/test_no_prd_default.py ..........                                  [ 75%]
      tests/test_playlist_details_href.py .                                    [ 76%]
      tests/test_playlist_relationships.py ..                                  [ 76%]
      tests/test_process_generator_events.py .                                 [ 77%]
      tests/test_ratchet.py .......                                            [ 80%]
      tests/test_route_smoke.py ..................                             [ 87%]
      tests/test_schema_compare.py ..................                          [ 95%]
      tests/test_spotify_oauth_flow.py ...                                     [ 96%]
      tests/test_test_state.py ....                                            [ 97%]
      tests/test_token_db_persistence.py ....                                  [ 99%]
      tests/test_verify_spotify_isrc_refresh.py .                              [100%]

      =============================== warnings summary ===============================
      ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1
        /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
          from starlette.testclient import TestClient as TestClient  # noqa

      ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53
        /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53: DeprecationWarning: The anyio.abc.BlockingPortal alias is deprecated, use anyio.from_thread.BlockingPortal instead.
          _PortalFactoryType = Callable[[], AbstractContextManager[anyio.abc.BlockingPortal]]

      -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
      - generated xml file: /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/reports/pytest.xml -
      ======================= 243 passed, 2 warnings in 11.67s =======================
  EVIDENCE: suite_pytest_command = cd $APP_DIR && $VENV_DIR/bin/python -m pytest --junitxml=$REPORT
  EVIDENCE: suite_pytest_exit_code = 0
  EVIDENCE: suite_pytest_tests = passed 243, failed 0, skipped 0 (total 243)
  EVIDENCE: suite_pytest_duration = 13.3s
  EVIDENCE: suite_pytest_report = /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/reports/pytest.xml
  EVIDENCE: suite_pytest_result = PASS
    suite npm: cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT

      > pifitness-frontend@0.1.0 test
      > vitest run --reporter=junit --outputFile=/home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/reports/npm.xml

      JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/reports/npm.xml
  EVIDENCE: suite_npm_command = cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT
  EVIDENCE: suite_npm_exit_code = 0
  EVIDENCE: suite_npm_tests = passed 317, failed 0, skipped 0 (total 317)
  EVIDENCE: suite_npm_duration = 27.7s
  EVIDENCE: suite_npm_report = /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/reports/npm.xml
  EVIDENCE: suite_npm_result = PASS
  EVIDENCE: regression = all 2 suite(s) passed
  EVIDENCE: prd_database = PRD database was not modified
[STEP 6/10] regression ..................................... PASS (41.3s)
[STEP 7/10] stop_services .................................. START
    polling the Pi for pifitness* units ...
  EVIDENCE: active_services = pifitness-fastapi.service
  EVIDENCE: active_timers = pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: restore_units = pifitness-fastapi.service pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: unit_files_to_move_aside = pifitness-fastapi-test.service pifitness-fastapi.service pifitness-hw-health.service pifitness-hw-health.timer pifitness_agent.service pifitness_agent.timer
  EVIDENCE: enabled_before = pifitness-fastapi.service pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: unit_record = /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units.json
  EVIDENCE: stopped_pifitness-hw-health.timer = active -> inactive
  EVIDENCE: stopped_pifitness_agent.timer = active -> inactive
  EVIDENCE: stopped_pifitness-fastapi.service = active -> inactive
  EVIDENCE: disabled_pifitness-fastapi.service = yes
  EVIDENCE: disabled_pifitness-hw-health.timer = yes
  EVIDENCE: disabled_pifitness_agent.timer = yes
  EVIDENCE: moved_pifitness-fastapi-test.service = /etc/systemd/system/pifitness-fastapi-test.service -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness-fastapi-test.service
  EVIDENCE: moved_pifitness-fastapi.service = /etc/systemd/system/pifitness-fastapi.service -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness-fastapi.service (+ drop-ins)
  EVIDENCE: moved_pifitness-hw-health.service = /etc/systemd/system/pifitness-hw-health.service -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness-hw-health.service
  EVIDENCE: moved_pifitness-hw-health.timer = /etc/systemd/system/pifitness-hw-health.timer -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness-hw-health.timer
  EVIDENCE: moved_pifitness_agent.service = /etc/systemd/system/pifitness_agent.service -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness_agent.service
  EVIDENCE: moved_pifitness_agent.timer = /etc/systemd/system/pifitness_agent.timer -> /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup/pifitness_agent.timer
  EVIDENCE: active_pifitness_units_after = none
  EVIDENCE: backup_dir = /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units_backup
[STEP 7/10] stop_services .................................. PASS (2.6s)
[STEP 8/10] start_services ................................. START
  EVIDENCE: units_rendered = pifitness-hardware-health.timer pifitness_agent.timer pifitness-fastapi.service pifitness-hardware-health.service pifitness_agent.service
  EVIDENCE: placeholders = @APP_DIR@=/home/god/PiFitness-test @VENV_DIR@=/home/god/.local/share/pifitness/test-venv
  EVIDENCE: installed_pifitness-hardware-health.timer = /etc/systemd/system/pifitness-hardware-health.timer
  EVIDENCE: installed_pifitness_agent.timer = /etc/systemd/system/pifitness_agent.timer
  EVIDENCE: installed_pifitness-fastapi.service = /etc/systemd/system/pifitness-fastapi.service
  EVIDENCE: installed_pifitness-hardware-health.service = /etc/systemd/system/pifitness-hardware-health.service
  EVIDENCE: installed_pifitness_agent.service = /etc/systemd/system/pifitness_agent.service
  EVIDENCE: enabled_pifitness-fastapi.service = enabled
  EVIDENCE: enabled_pifitness-hardware-health.timer = enabled
  EVIDENCE: enabled_pifitness_agent.timer = enabled
    starting: pifitness-fastapi.service pifitness-hardware-health.timer pifitness_agent.timer
  EVIDENCE: started_pifitness-fastapi.service = active/running
  EVIDENCE: started_pifitness-hardware-health.timer = active/waiting
  EVIDENCE: started_pifitness_agent.timer = active/waiting
  EVIDENCE: paired_services_not_started_directly = pifitness-hardware-health.service pifitness_agent.service
  EVIDENCE: old_units_active = none
[STEP 8/10] start_services ................................. PASS (2.5s)
[STEP 9/10] smoke .......................................... NOT IMPLEMENTED
  expected helper: /home/god/pifitness-bootstrap/bootstrap.pgRREA/deploy/scripts/lib/step_smoke.py
  the run stops cleanly here; later steps are not run
[autodeploy] step sequence stopped cleanly at step 9/10 (smoke)
[autodeploy] run ended with status 0; no further steps will run; entering cleanup...

[cleanup] running cleanup; Ctrl-C is ignored until it finishes
(1/3) Assess the run ...
    EVIDENCE: exit_status = 0
    EVIDENCE: steps_recorded = 9 of 10
    EVIDENCE: step_statuses = NOT_IMPLEMENTED=1, PASS=7, SKIPPED=1
    EVIDENCE: outcome = incomplete
(1/3) Assess the run ... OK
(2/3) Undo registered actions (last registered first) ...
    EVIDENCE: undo_actions_registered = 4 (halt_units, units, test_db, checkout)
[UNDO 1/4] halt_units ...................................... START
  EVIDENCE: environment = test
  EVIDENCE: outcome = incomplete
  EVIDENCE: units_to_halt = pifitness-hardware-health.timer pifitness_agent.timer pifitness-fastapi.service pifitness-hardware-health.service pifitness_agent.service
    halting the units this run installed before anything else is rolled back
  EVIDENCE: halted_pifitness-hardware-health.timer = inactive
  EVIDENCE: halted_pifitness_agent.timer = inactive
  EVIDENCE: halted_pifitness-fastapi.service = inactive
  EVIDENCE: halted_pifitness-hardware-health.service = inactive
  EVIDENCE: halted_pifitness_agent.service = inactive
[UNDO 1/4] halt_units ...................................... PASS (0.3s)
[UNDO 2/4] units ........................................... START
  EVIDENCE: environment = test
  EVIDENCE: outcome = incomplete
  EVIDENCE: action = revert: remove new units, restore the originals
    unit record: /home/god/pifitness-logs/autodeploy-test-20261116T194605Z-bc6cd70.d/units.json
  EVIDENCE: stopped_pifitness-hardware-health.timer = inactive
  EVIDENCE: stopped_pifitness_agent.timer = inactive
  EVIDENCE: stopped_pifitness-fastapi.service = inactive
  EVIDENCE: installed_units_removed = /etc/systemd/system/pifitness-hardware-health.timer /etc/systemd/system/pifitness_agent.timer /etc/systemd/system/pifitness-fastapi.service /etc/systemd/system/pifitness-hardware-health.service /etc/systemd/system/pifitness_agent.service
  EVIDENCE: unit_files_restored = pifitness-fastapi-test.service pifitness-fastapi.service pifitness-fastapi.service.d pifitness-hw-health.service pifitness-hw-health.timer pifitness_agent.service pifitness_agent.timer
  EVIDENCE: re_enabled = pifitness-fastapi.service pifitness-hw-health.timer pifitness_agent.timer
  EVIDENCE: restored_pifitness-fastapi.service = active/running
  EVIDENCE: restored_pifitness-hw-health.timer = active/waiting
  EVIDENCE: restored_pifitness_agent.timer = active/waiting
[UNDO 2/4] units ........................................... PASS (4.2s)
[UNDO 3/4] test_db ......................................... START
  EVIDENCE: outcome = incomplete
  EVIDENCE: test_state_marker = dirty
  EVIDENCE: baseline_dump = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 1d 20h old)
    rolling the TEST database back to test_baseline_20261114T233555Z.dump
    dropping database personal_fitness_test (other sessions are terminated)
    creating database personal_fitness_test
    pg_restore test_baseline_20261114T233555Z.dump  (1.2 GiB)