I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

Your task is to build the next helper in a half-finished deployment bootstrapper: deploy/scripts/lib/step_dependencies.py, plus the supporting manifest deploy/scripts/tools.conf. The step reconciles the Python venv, the Node dependencies and the non-pip tools (dbmate, Playwright browsers) on a Raspberry Pi.

Deliver, as complete files (not diffs):

step_dependencies.py
tools.conf
Any edits needed elsewhere, again as complete files and with a plain list of what changed: common.py (only if a shared helper is genuinely needed), autodeploy.conf (remove INSTALLABLE_TOOLS once the manifest replaces it), and step_preflight.py (it must read the installable tool names from tools.conf instead).
Exact instructions for the user: which lines to add to which config file, and how to run and read the first Pi test.


# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no 4. TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
5. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
6. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
7. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
8. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.


# Task: build deploy/scripts/lib/step_db_prepare.py (TEST step 4)

I'm building a Raspberry Pi deployment bootstrapper one helper at a time. Steps
preflight, checkout and dependencies are built and working. Build the next one:
step_db_prepare.py, the TEST-only "database clone / restore" step. It is TEST step 4 in
autodeploy_objectives.md. The PRD equivalent (step_db_baseline.py) is a separate later task.

I'll attach: autodeploy_objectives.md (the spec), common.py, autodeploy.conf, tools.conf,
step_preflight.py, step_dependencies.py (as a style reference) and legacy_bootstrap_parameters.md.
Read them first. If you need any other file or fact, ask. Don't assume.

## What the step does (spec: TEST Step 4, plus 4.4 and the file-level contracts in autodeploy.sh)
- Reads the decision preflight stored in run.state (DB_ACTION = keep | restore_snapshot |
  fresh_clone, DB_SNAPSHOT_FILE).
- keep: SKIPPED (not required); verify the clean baseline dump recorded in STATE_FILE still
  exists and print its age.
- Otherwise: set the TEST state marker to dirty BEFORE overwriting anything, verify the write
  target is NOT PRD, restore the nightly dump (or pg_dump -Fc from PRD then pg_restore) into TEST,
  mark the database as TEST (database COMMENT = TEST_DB_MARKER from autodeploy.conf; keep that,
  not the legacy pifitness.env setting), make sure the TEST-only role exists, then write the clean
  baseline dump (test_baseline_<UTC>.dump in PG_BACKUP_LOCATION), record baseline_dump=<path> in
  STATE_FILE, and set state to clean.
- On failure leave the state dirty and report the cause. Do not retry automatically.
- Print elapsed time at intervals (restores are slow), and the evidence lines the spec lists,
  including "PRD database was not modified".
- Register whatever undo action the exit handler needs BEFORE changing anything. Check what
  cleanup.py and common.register_undo expect, and whether an undo helper for the TEST database
  already exists. If not, define its contract, but don't build it unless I ask.

## Known facts
- PostgreSQL 18.6, client tools 18.6, no TimescaleDB. PostGIS is in use.
- The role in each env file should have full privileges. Verify this at run time rather than
  assuming; fail with a clear reason if it can't drop/create or restore.
- public.schema_migrations does not exist anywhere. No migration has ever run. The snapshot
  doesn't have it either. Preflight already treats that as 0 applied.
- Nightly dumps are in PG_BACKUP_LOCATION, matched by BACKUP_GLOB. A TEST run reads PRD only
  through common.load_prd_source().

## Ground rules (same as before)
1. autodeploy_objectives.md is the spec. Say so explicitly if you must depart from it.
2. TEST and PRD run the same kind of code. No "if environment ==" branches. Differences come
   from the Pi env file.
3. Minimal config. Constants for values that never vary; work things out at run time; add a
   config key only when a value truly varies.
4. Use canonical tools (pg_dump, pg_restore, psql). Don't over-engineer; this is a personal,
   single-developer app.
5. I only test by running the Pi launcher and reading its output, so make failures print a clear
   REASON and WHERE.
6. Python 3 stdlib only. Use common.py helpers.
7. Deliver complete files, not diffs, plus a plain list of what changed. Say what you tested and
   what you couldn't (you can't reach the Pi or its database).

## Deliverables
step_db_prepare.py, any edits to other files (complete files plus a change list), and exact
instructions for the first Pi test.

## Open questions to raise with me before or during the work
- Where `next build` goes (a small step before start_services, or inside it). Not part of this task.
- With no migrations ever run, the first `dbmate up` in the next step will apply every migration
  to a database that already has its tables. Ask me what the migrations directory contains
  (is there a baseline migration safe on an existing schema?) before the migrate step.

I’d attach autodeploy_objectives.md, common.py, autodeploy.conf, tools.conf, step_preflight.py and step_dependencies.py, plus legacy_bootstrap_parameters.md. If you have tools/db/restore_test_db.py, add it too. It’s the legacy restore logic, and it’s probably the quickest way to get the TEST-role and ownership details right.