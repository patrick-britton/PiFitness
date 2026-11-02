# PiFitness Reorg — STATUS

Current state of the reorg (current-state only, no reorg history).

## HEAD + branch
HEAD: cb6a4abd49ed1c0ccb6bbd6080931efbec207427
Branch: reorg/02-moves

## Test baseline
Fast-lane pytest: 223 passed / 17 skipped / 1 deselected (post-INT-36)

## Load-bearing architecture facts
- `/health/auth` satisfied via `GET /api/auth/status` at `backend/pifitness/auth/router.py:59-87`
- INT-T4 retired (never executed)
- `browser_login.py` cannot be deleted (Playwright fresh-login path)
- Garmin file cache (`{LOCAL_STORAGE_PATH}/garmin/native-oauth2.json`) is load-bearing (AuthManager interface)

## Pending compat shims
- `extract_pirate_universal = extract_garmin_universal` in `backend/backend_functions/json_extractors.py` — remove after `deploy/migrations/INT35__repoint_pirate_dispatchers.sql` runs at PRD

## Phase D/E status
Deferred, not spec'd. Historical context archived at `docs/reorg/archive/PLAN_v2.md`.

## Pending migration scripts
6 files in `deploy/migrations/` are unexecuted. Runbook: `deploy/README.md`.