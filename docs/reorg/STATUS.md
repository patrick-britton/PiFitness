# PiFitness Reorg — STATUS

Current state of the reorg (current-state only, no reorg history).

## HEAD + branch
Current HEAD: run `git rev-parse HEAD` on branch reorg/02-moves
Branch: reorg/02-moves

## Test baseline
Fast-lane pytest: run `python -m pytest -q -m "not slow"`; expected `~223 passed, 17 skipped, 1 deselected`

## Load-bearing architecture facts
- `/health/auth` satisfied via `GET /api/auth/status` at `backend/pifitness/auth/router.py:59-87`
- INT-T4 retired (never executed)
- `browser_login.py` cannot be deleted (Playwright fresh-login path)
- Garmin file cache (`{LOCAL_STORAGE_PATH}/garmin/native-oauth2.json`) is load-bearing (AuthManager interface)

## Pending compat shims
- `extract_pirate_universal = extract_garmin_universal` in `backend/backend_functions/json_extractors.py` — remove after `deploy/migrations/INT35__repoint_pirate_dispatchers.sql` runs at PRD
- `backend/backend_functions/backend_tasks.py` shim re-exporting from `pifitness.worker.backend_tasks` — **load-bearing**: 2 DB rows in `tasks.task_config.python_function` still dispatch to `backend_functions.backend_tasks.*` (per INT-38b DB probe). Remove only after `deploy/migrations/INT38__repoint_worker_tasks.sql` runs at PRD.
- Note: `task_execution` and `ultimate_task_executioner_v2` were also moved but have **zero** DB dispatch rows (verified via `SELECT COUNT(*) FROM tasks.task_config WHERE python_function LIKE '%backend_functions.{module}%'`). No shims or migration updates needed for those modules.

## Phase D/E status
Deferred, not spec'd. Historical context archived at `docs/reorg/archive/PLAN_v2.md`.

## Pending migration scripts
Unapplied migration scripts: run `Get-ChildItem deploy/migrations`; cross-check against applied set in `deploy/README.md` runbook.