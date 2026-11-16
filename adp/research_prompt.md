I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

# Your Task
- Review the latest shell output (at the bottom of this prompt) -- determine the next step required to advance this project.

# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
4. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
5. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
6. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
7. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.

# Latest Shell output (trailing, all omitted lines above pass):
[STEP 1/10] preflight ...................................... PASS (4.3s)
[STEP 2/10] checkout ....................................... START
    (1/8) Configuration ...
  EVIDENCE: environment = test
  EVIDENCE: requested_sha = ce027ce3ce337ce09072b35cf5b3a1cba23c7b22
  EVIDENCE: app_dir = /home/god/PiFitness-test (previous checkout present)
  EVIDENCE: source_repo = /home/god/PiFitness (contains the commit)
  EVIDENCE: origin_remote = https://github.com/patrick-britton/PiFitness.git (copied onto the new checkout)
  EVIDENCE: staging_dir = /home/god/PiFitness-test.stage-autodeploy-test-20261116T170100Z-ce027ce
  EVIDENCE: env_file_wiring = symlink backend file backend/.env -> /home/god/.config/pifitness/test.env
  EVIDENCE: previous_checkout_policy = deleted right after the swap
  EVIDENCE: leftover_stage_dirs = 0
    (2/8) Fetch the exact commit ...
      git: From /home/god/PiFitness
      git:  * branch            ce027ce3ce337ce09072b35cf5b3a1cba23c7b22 -> FETCH_HEAD
  EVIDENCE: fetch_method = shallow fetch of the exact SHA from the local repo
  EVIDENCE: origin_remote_set = https://github.com/patrick-britton/PiFitness.git
    (3/8) Verify the checkout ...
  EVIDENCE: actual_sha = ce027ce3ce337ce09072b35cf5b3a1cba23c7b22
  EVIDENCE: clean_tree = yes (git status --porcelain is empty)
  EVIDENCE: expected_paths = backend deploy/scripts (all present)
  EVIDENCE: file_count = 520 on disk, 520 tracked
  EVIDENCE: running_scripts_match_checkout = 27 identical, 0 differ, 0 missing
    (4/8) Persistent resources ...
  EVIDENCE: persistent_resources_preserved = none
    (5/8) Environment file ...
  EVIDENCE: env_file = symlink backend/.env -> /home/god/.config/pifitness/test.env
    (6/8) Final checks ...
  EVIDENCE: untracked_files = 0 (apart from persistent resources and backend/.env)
  EVIDENCE: file_count_final = 521
    (7/8) Swap in ...
  EVIDENCE: swap_method = atomic exchange (renameat2)
  EVIDENCE: live_checkout = /home/god/PiFitness-test at ce027ce3ce337ce09072b35cf5b3a1cba23c7b22
    (8/8) Previous checkout ...
  WARNING: previous TEST checkout held top-level entries that are NOT in the new one and are being discarded: .pytest_cache
  EVIDENCE: old_checkout_removed = /home/god/PiFitness-test.stage-autodeploy-test-20261116T170100Z-ce027ce
  EVIDENCE: checkout = ready: /home/god/PiFitness-test is commit ce027ce3ce33
    checkout complete (2.1s)
[STEP 2/10] checkout ....................................... PASS (2.2s)
[STEP 3/10] dependencies ................................... START
    (1/5) Configuration and paths ...
  EVIDENCE: app_dir = /home/god/PiFitness-test
  EVIDENCE: venv_dir = /home/god/.local/share/pifitness/test-venv (outside the app dir: yes)
  EVIDENCE: python_project_dir = /home/god/PiFitness-test/backend
  EVIDENCE: frontend_dir = /home/god/PiFitness-test/frontend/pifitness
  EVIDENCE: tools_file = /home/god/pifitness-bootstrap/bootstrap.gIMjep/deploy/scripts/tools.conf
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
      Resolved 52 packages in 0.79ms
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

      added 270 packages in 16s
  EVIDENCE: npm_ci = ok, /home/god/PiFitness-test/frontend/pifitness/node_modules
    (5/5) Playwright browsers ...
  EVIDENCE: playwright_version = Version 1.60.0
    playwright install chromium
  EVIDENCE: browser_chromium = launches, version 148.0.7778.0
  EVIDENCE: dependencies = reconciled
[STEP 3/10] dependencies ................................... PASS (19.8s)
[STEP 4/10] db_prepare ..................................... START
    (1/8) Decision ...  [elapsed 0s]
  EVIDENCE: db_action = keep
  EVIDENCE: test_state_marker = clean
  EVIDENCE: test_clean_baseline = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 1d 17h old)
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
      Applied: 20261108000000_int19b_move_tokens_to_api_services.sql in 276.93328ms
      Applying: 20261108000001_int25_repoint_get_spotify_client.sql
      Applied: 20261108000001_int25_repoint_get_spotify_client.sql in 1.948294ms
      Applying: 20261108000002_int31d_repoint_garmin_dispatchers.sql
      Applied: 20261108000002_int31d_repoint_garmin_dispatchers.sql in 1.063877ms
      Applying: 20261108000003_int35_repoint_pirate_dispatchers.sql
      Applied: 20261108000003_int35_repoint_pirate_dispatchers.sql in 1.910071ms
      Applying: 20261108000004_int38_repoint_worker_tasks.sql
      Applied: 20261108000004_int38_repoint_worker_tasks.sql in 2.587379ms
      Applying: 20261108000005_comment_backfill_exercises.sql
      Applied: 20261108000005_comment_backfill_exercises.sql in 2.421992ms
      Applying: 20261108000006_comment_backfill_activities.sql
      Applied: 20261108000006_comment_backfill_activities.sql in 30.269035ms
      Applying: 20261108000007_comment_backfill_api_services.sql
      Applied: 20261108000007_comment_backfill_api_services.sql in 2.207253ms
      Applying: 20261108000008_comment_backfill_food.sql
      Applied: 20261108000008_comment_backfill_food.sql in 6.937423ms
      Applying: 20261108000009_comment_backfill_health.sql
      Applied: 20261108000009_comment_backfill_health.sql in 4.733725ms
      Applying: 20261108000010_comment_backfill_logging.sql
      Applied: 20261108000010_comment_backfill_logging.sql in 4.277508ms
      Applying: 20261108000011_comment_backfill_music.sql
      Applied: 20261108000011_comment_backfill_music.sql in 13.440129ms
      Applying: 20261108000012_comment_backfill_staging.sql
      Applied: 20261108000012_comment_backfill_staging.sql in 14.95726ms
      Applying: 20261108000013_comment_backfill_tasks.sql
      Applied: 20261108000013_comment_backfill_tasks.sql in 7.386215ms
      Applying: 20261108000014_comment_backfill_volleyball.sql
      Applied: 20261108000014_comment_backfill_volleyball.sql in 1.794702ms
      Applying: 20261108000015_dev_reset_replay_sentinel.sql
      Applied: 20261108000015_dev_reset_replay_sentinel.sql in 4.614431ms
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
^A^A      collected 333 items

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
      - generated xml file: /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/pytest.xml -
      ============================= 3 warnings in 4.72s ==============================
      ERROR: test classification problems:
        tests/test_garmin_login_flow.py::test_get_status_authenticated_when_db_token_present: needs exactly one purpose marker, has none
        tests/test_garmin_login_flow.py::test_is_configured_false_when_creds_missing: needs exactly one purpose marker, has none

  EVIDENCE: suite_pytest_command = cd $APP_DIR && $VENV_DIR/bin/python -m pytest --junitxml=$REPORT
  EVIDENCE: suite_pytest_exit_code = 4
  EVIDENCE: suite_pytest_tests = passed 0, failed 0, skipped 0 (total 0)
  EVIDENCE: suite_pytest_duration = 6.2s
  EVIDENCE: suite_pytest_report = /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/pytest.xml
  EVIDENCE: suite_pytest_result = FAIL (exit status 4)
        last output lines:
          ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1
            /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/fastapi/testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.te
              from starlette.testclient import TestClient as TestClient  # noqa
          ../.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53
            /home/god/.local/share/pifitness/test-venv/lib/python3.13/site-packages/starlette/testclient.py:53: DeprecationWarning: The anyio.abc.BlockingPortal alias is
              _PortalFactoryType = Callable[[], AbstractContextManager[anyio.abc.BlockingPortal]]
          backend/pifitness/volleyball/schemas.py:62
            /home/god/PiFitness-test/backend/pifitness/volleyball/schemas.py:62: PydanticDeprecatedSince20: Using extra keyword arguments on `Field` is deprecated and wil
              message: str = Field(
          -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
          - generated xml file: /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/pytest.xml -
          ============================= 3 warnings in 4.72s ==============================
          ERROR: test classification problems:
            tests/test_garmin_login_flow.py::test_get_status_authenticated_when_db_token_present: needs exactly one purpose marker, has none
            tests/test_garmin_login_flow.py::test_is_configured_false_when_creds_missing: needs exactly one purpose marker, has none
    suite npm: cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT

      > pifitness-frontend@0.1.0 test
      > vitest run --reporter=junit --outputFile=/home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/npm.xml

      JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/npm.xml
  EVIDENCE: suite_npm_command = cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT
  EVIDENCE: suite_npm_exit_code = 0
  EVIDENCE: suite_npm_tests = passed 317, failed 0, skipped 0 (total 317)
  EVIDENCE: suite_npm_duration = 28.6s
  EVIDENCE: suite_npm_report = /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/npm.xml
  EVIDENCE: suite_npm_result = PASS
  WARNING: the TEST state marker stays 'dirty': the exit handler restores the clean baseline
[STEP 6/10] regression ..................................... FAIL (35.1s)
  REASON: 1 of 2 suite(s) failed: pytest
  WHERE: /home/god/PiFitness-test/deploy/tests.conf
  last output:
    |           /home/god/PiFitness-test/backend/pifitness/volleyball/schemas.py:62: PydanticDeprecatedSince20: Using extra keyword arguments on `Field` is deprecated and wil
    |             message: str = Field(
    |         -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    |         - generated xml file: /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/pytest.xml -
    |         ============================= 3 warnings in 4.72s ==============================
    |         ERROR: test classification problems:
    |           tests/test_garmin_login_flow.py::test_get_status_authenticated_when_db_token_present: needs exactly one purpose marker, has none
    |           tests/test_garmin_login_flow.py::test_is_configured_false_when_creds_missing: needs exactly one purpose marker, has none
    |   suite npm: cd $APP_DIR/frontend/pifitness && CI=true npm test -- --reporter=junit --outputFile=$REPORT
    |
    |     > pifitness-frontend@0.1.0 test
    |     > vitest run --reporter=junit --outputFile=/home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/npm.xml
    |
    |     JUNIT report written to /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d/reports/npm.xml
    | WARNING: the TEST state marker stays 'dirty': the exit handler restores the clean baseline
[autodeploy] ERROR: helper failed: run_step.py exited with status 1
[autodeploy] ERROR:   called as: run_step.py --run-dir /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d --step regression --number 6 --total 10
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
  EVIDENCE: baseline_dump = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump (1.2 GiB, 1d 17h old)
    rolling the TEST database back to test_baseline_20261114T233555Z.dump
    dropping database personal_fitness_test (other sessions are terminated)
    creating database personal_fitness_test
    pg_restore test_baseline_20261114T233555Z.dump  (1.2 GiB)
      ... still running (30s)
  ... still running (60s)
      ... still running (60s)
      ... still running (90s)
  ... still running (120s)
      ... still running (120s)
  ... still running (150s)
      ... still running (150s)
      ... still running (180s)
  ... still running (210s)
      ... still running (210s)
  ... still running (240s)
      ... still running (240s)
      ... still running (270s)
  ... still running (300s)
      ... still running (300s)
  ... still running (330s)
      ... still running (330s)
  ... still running (360s)
      ... still running (360s)
  ... still running (390s)
      ... still running (390s)
  ... still running (420s)
      ... still running (420s)
  ... still running (450s)
      ... still running (450s)
  ... still running (480s)
      ... still running (480s)
  ... still running (510s)
      ... still running (510s)
  ... still running (540s)
      ... still running (540s)
  EVIDENCE: restore_seconds = 555 (9m15s)
  EVIDENCE: rolled_back_to = /home/god/Documents/DB_Backups/test_baseline_20261114T233555Z.dump
  EVIDENCE: rollback_seconds = 556
  EVIDENCE: test_state_marker = clean
  EVIDENCE: prd_database = PRD database was not modified
[UNDO 1/2] test_db ......................................... PASS (556.1s)
[UNDO 2/2] checkout ........................................ START
  EVIDENCE: outcome = failure
  EVIDENCE: app_dir = /home/god/PiFitness-test
  SKIPPED: the new checkout is live and no previous checkout remains
[UNDO 2/2] checkout ........................................ SKIPPED (the new checkout is live and no previous checkout remains)
(2/3) Undo registered actions ... DONE
(3/3) Summary
========================================================================
RUN SUMMARY   environment=test   commit=ce027ce3ce33
========================================================================
  #  Step               Status               Time  Detail
  1  preflight          PASS                 4.3s
  2  checkout           PASS                 2.2s
  3  dependencies       PASS                19.8s
  4  db_prepare         SKIPPED              0.1s  not required
  5  migrate            PASS                 0.8s
  6  regression         FAIL                35.1s  1 of 2 suite(s) failed: pytest
  7  stop_services      not run                 -
  8  start_services     not run                 -
  9  smoke              not run                 -
 10  manual_gate        not run                 -

Undo actions (in the order they ran):
     test_db                  PASS
     checkout                 SKIPPED  the new checkout is live and no previous checkout remains

Outcome : failure
Result  : FAIL (exit status 1)
Log     : /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.log
========================================================================
[autodeploy] ERROR: helper failed: cleanup.py exited with status 1
[autodeploy] ERROR:   called as: cleanup.py --run-dir /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.d --exit-status 1
[autodeploy] run finished: exit status 1 (log: /home/god/pifitness-logs/autodeploy-test-20261116T170100Z-ce027ce.log)