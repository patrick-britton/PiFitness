# PiFitness Reorg - PLAN.md (v3)

**HEAD:** `25bd50e` (INT-14 complete, 2026-11-01)
**Rollback point:** `6549d0a` (post-INT-13, pre-INT-14)
**Baseline:** `183 passed, 15 skipped, 28 deselected` (fast-lane, HEAD 25bd50e)
**State:** INT-N Phase A complete (INT-0..INT-14) - Phase B/C/D/E planned (INT-16..INT-29) - INT-15 in progress

This file is the single source of truth for the active INT-N reorg. Section 1.5 holds the current roadmap; section 1.6 holds Phase A close-out. Sections 2-6 are the historical phases-0-6 plan, preserved as record, not as active work. Per-block execution reports live in `docs/reorg/blocks/INT-N-report.md` (gitignored).

## Section 1 - Historical Task Status Ledger (phases 0-6, superseded)

Preserved from PLAN v2 at HEAD `287c9dff`. Kept for history only. Active work is in section 1.5.

| Ph | Task | Status | Evidence |
|---|---|---|---|
| 0 | 0.1 audit tool | [DONE] | `scripts/repo_audit` present |
| 0 | 0.2 secrets handled | [DONE] | `.gitignore` covers tokens |
| 0 | 0.3 gitignore logs/caches | [DONE] | `.gitignore` covers `.next`, `*.dump` |
| 0 | 0.5 streamlit residue | [DONE] | `.streamlit` absent |
| 0 | 0.6 regenerate-audit | [DONE] | wrapper present |
| 0 | T0.5 move tool | [DONE] | libcst engine; move+rename; multi-name/alias safe; comment/string preserved; fixture covers all cases |
| 0 | T0.5.1 rename support | [DONE] | move via rope, rename via fs+regex; fixture covers both |
| 1 | 1.1 dangling config | [DONE] | docstring only, no real import (Task 0.3.5) |
| 1 | 1.2 fastapi removed from exec | [DONE] | no `import fastapi` in exec |
| 1 | 1.3 router logic extracted | [UNVERIFIED] | not probed - see Task 1.3.verify |
| 1 | 1.4 DYNAMIC_TARGETS | [DONE] | `docs/reorg/DYNAMIC_TARGETS.md` |
| 1 | 1.5 Garmin vendored deleted | [DONE] | both absent |
| 1 | 1.6 6 orphans deleted | [DONE] | all 6 absent |
| 1 | 1.7 forbid rules | [DONE] | `audit.toml` has `[[forbid]]` |
| 2 | 2.0 bootstrap manifests | [TODO] | `requirements-react.txt` MISSING -> Task 2.5.dep |
| 2 | 2.2 scripts triaged | [DONE] | `tools/db/` populated |
| 2 | 2.4 deploy merged | [DONE] | `deploy/systemd` present, `deployment/` absent |
| 2 | 2.5 root pkg.json removed | [DONE] | root absent, frontend present |
| 3 | 3.0 pyproject.toml | [UNVERIFIED] | see Task 3.0.verify |
| 3 | 3.health | [DONE] | `backend/pifitness/health/` |
| 3 | 3.exercises | [DONE] | dir present |
| 3 | 3.volleyball | [DONE] | dir present |
| 3 | 3.admin | [DONE] | dir present |
| 3 | 3.food | [DONE] | dir present |
| 3 | 3.music | [DONE] | dir present |
| 3 | 3.segments | [DONE] | dir present |
| 3 | 3.activities | [DONE] | dir present |
| 3 | 3.auth | [DONE] | `backend/pifitness/auth/` (was `backend/api/auth.py`) |
| 3 | 3.config | [DONE] | `backend/pifitness/config/` router only; settings still in `backend_functions/config.py` |
| 3 | 3.tri_tip | [DONE] | `backend/pifitness/tri_tip/` - discovered 2026-10-29, not in original scope |
| 3 | 3.integrations | [SUPERSEDED] | split into 3.auth + 3.config; Garmin/Spotify logic in `service_logins.py` |
| 3 | 3.worker | [TODO] | `backend/pifitness/worker/` missing; source `backend_functions/backend_tasks.py` |
| 3 | 3.core.1 | [DONE] | `backend/pifitness/core/config.py` (was `backend_functions/config.py`); libcst move, 2 importers rewritten |
| 3 | 3.core.2 | [TODO] | rename `domain_exceptions.py` -> `exceptions.py`, `logging_functions.py` -> `logging.py` into `pifitness.core` |
| 3 | 3.config-env-fix | [TODO] | `config.py:93` `.env` path: currently `__file__`-relative (silent no-op); should use `backend/.env` |
| 3 | 3.api | [TODO] | `backend/pifitness/api/main.py` missing; source `backend/main.py`; note: `pifitness` importable as top-level, not `backend.pifitness` |
| 3 | 3.2 systemd entrypoints | [UNVERIFIED] | see Task 3.2.verify |
| 3 | 3.3 TARGET forbid rules | [UNVERIFIED] | see Task 3.3.verify |
| 3 | 3.4 clean-room | [UNVERIFIED] | blocked on 3.api |
| 4 | 4.1 frontend flatten | [TODO] | `frontend/pifitness/` still exists |
| 4 | 4.2 features/ | [TODO] | not started |
| 3 | 3.prep-root | [DONE] | `backend_functions/` moved to `backend/backend_functions/`; root unified |
| 3 | 3.credential-fix | [DONE] | `credential_management.py`: lazy `load_key()`, correct `.env` path |
| 5 | 5.* guardrails | [TODO] | not started |

## Section 1.5 - INT-N Reorg Roadmap (active)

The INT-N reorg supersedes the phases-0-6 plan for the current codebase state. Blocks execute one at a time via `/execute-block INT-N`. Per-block specs and reports are in `docs/reorg/blocks/` (gitignored).

### Done

| Block | Deliverable | Commit |
|---|---|---|
| INT-0..INT-10b | extraction / unvendoring | `a7cdcd7` (pre-INT-12) |
| INT-T1, INT-T2b, INT-T2c | test tiering + triage | `a7cdcd7` (pre-INT-12) |
| INT-11 | attempted `browser_login.py` deletion - correctly aborted | none |
| INT-12 | delete `pirate-garmin_clone/`, 15 files, 2941 deletions | `a7cdcd7` |
| INT-13 | `GARMIN_LOGIN_FLOW.md` + `test_garmin_login_flow.py` + 3 provider fixes | `6549d0a` |
| INT-13-live | manual Garmin live login test - passed in 17.56s | (manual) |
| INT-14 | inline `import spotipy` cleanup, stale-note cleanup, canonical `get_auth_status` tests | `25bd50e` |
| INT-15 | this block - plan close-out | (this commit) |
| INT-17 | Rename `create_native_session` -> `create_browser_session` | `[INT-17]` |
| INT-18 | Rename `pirate_garmin_login` -> `garmin_login` + compatibility alias + migration script | `[INT-18]` |
| INT-18-final | Remove compatibility alias + re-export; local dev DB has 1 api_services row still pointing at old name, reconciled at production merge | `[INT-18-final]` |
| INT-19 | Delete dead `default_app_dir()` fallback; rename cache subdir `pirate-garmin` -> `garmin`; add intent comment | `[INT-19]` |
| INT-19b | Move token tables `_migration` -> `api_services`; fix `save_token_to_db`; test-DB guard; persistence tests | `[INT-19b]` |
| INT-19c | PRD migration artifact + doc refs aligned + local clinerules rewritten (operator) | `[INT-19c]` |

### Remaining

| Block | Deliverable |
|---|---|
| INT-16 | Garmin refresh-path tests (mock HTTP; prove `refresh_di_token_slot` / `refresh_it_token_slot` work without Playwright) |
| INT-20 | Scrub remaining `pirate-garmin` mentions in comments/docstrings (including `provider.py:10`) |
| INT-21 | Untrack `.audit/`, add to `.gitignore`, delete from disk |
| INT-22 | Remove redundant `pifitness-0.1.0` editable install; update deploy script; verify `_backend_root.pth` covers both packages |
| INT-23 | Move `check_rate_limit_cached` from `service_logins` to `_common.py` |
| INT-24 | Retire `sql_rate_limited` cluster (4 importers) |
| INT-25 | Retire `get_spotify_client` cluster (3 importers) |
| INT-26 | Retire `load_api_credentials` cluster (3 importers) |
| INT-27 | Retire `mapbox_token` cluster (2 importers) |
| INT-28 | Delete `service_logins.py` entirely |
| INT-29 | `pytest.ini` `slow` description; `test_triage.yaml` header; stale comments in `router.py`; INT-T4 retirement note (already retired here, see section 1.6) |

### Notes carried into Phase B-E

- **`/health/auth` is already satisfied.** The original plan line "`/health/auth` endpoint consuming `pifitness.auth.status.get_auth_status`" is satisfied by `GET /api/auth/status` at `backend/pifitness/auth/router.py:59-87`, which has called `get_auth_status()` since before INT-14. INT-14 did not add a duplicate endpoint. This closes the plan line without redundant HTTP surface.
- **INT-T4 is retired.** INT-T4 was never executed. Phase A ended at INT-14. INT-29 does not need to re-retire it.
- **`browser_login.py` cannot be deleted.** See section 1.6. It is the fresh-login path.
- **The Garmin file cache (`{LOCAL_STORAGE_PATH}/garmin/native-oauth2.json`) is load-bearing.** It exists because `AuthManager` reads/writes tokens to disk, not DB. `garmin_login()` bridges: DB -> file cache -> AuthManager -> file cache -> DB. Removing either tier breaks cross-machine session sharing or the `AuthManager` interface respectively.

## Section 1.6 - Phase A Close-out (complete)

Phase A (INT-0..INT-14) is closed at HEAD `25bd50e`. Each block's evidence is the raw report at `docs/reorg/blocks/INT-N-report.md`. Three durable facts are recorded here because they prevent a future agent from re-deleting working code:

### 1. The actual Garmin login flow

Every fresh Garmin login goes through Playwright. `create_browser_session()` (renamed from `create_native_session` in INT-17) calls `login_via_browser()`. There is no pure-HTTP native path. The session returned is an Android-native OAuth2 session (hence `NativeOAuth2Session`), but the creator acquires it via a browser (hence `create_browser_session`). The old "native auth" naming on the creator was a misnomer; the dataclass and loader names remain accurate.

### 2. Three-layer anti-over-login protection

This is why login is reliable and does not trip Garmin 429 rate limits.

| Layer | Mechanism | Location | Prevents login when |
|---|---|---|---|
| 1. DB session cache | `api_services.api_tokens` via `load_token_from_db` / `save_token_to_db` | `provider.py:166, 194` | Any machine that ever logged in shares session |
| 2. HTTP refresh | `AuthManager.ensure_authenticated()` | `auth.py` | DI or IT refresh tokens still valid |
| 3. In-memory reuse | `get_garmin_client(token_age)` with `<= 300s` | `provider.py:90+` | Recent client in-process |

Fresh Playwright fires only when all three are exhausted - typically when both DI and IT refresh tokens have expired (24h refresh-token lifetime). Removing layer 1 would cause every machine to login independently, exhausting Garmin's rate limit within hours.

### 3. Consequences

- **`browser_login.py` cannot be deleted.** It is the fresh-login path, not vendored cruft. INT-11 aborted correctly on this basis.
- **Playwright stays in `pi5_deploy.sh`.** Removing it breaks fresh login on any machine without a cached session.
- **`api_services.api_tokens` cannot be removed** without an equivalent cross-machine session store.
- **`pirate-garmin_clone/` is gone.** Deleted at INT-12. Garmin code is first-class under `backend/pifitness/integrations/garmin/`.

### 4. Verification

INT-13 added `tests/test_garmin_login_flow.py`, including a live test `test_real_garmin_login_returns_usable_api_response`. It passed manually on 2026-11-01 in 17.56s (fresh Playwright path, not a cache hit). If that test fails after any future change, the change broke fresh login and must be reverted.

### 5. Phase B naming sequence

Phase B renames aspirational "pirate" and "native" identifiers to reflect what
the code actually does. Because live DB dispatch references some old names,
each rename runs in a three-phase sequence: code rename + compatibility alias
(block), DB migration (live, no commit), alias removal (block). INT-18 is the
first such sequence. INT-19 (env var + cache dir renames) is likely a single
block because no DB rows reference `PIRATE_GARMIN_APP_DIR`.


### 6. INT-18 DB reconciliation deferral

INT-18-live was **skipped**. The pirate-garmin-report claimed 13 `tasks.vw_task_info`
rows + 2 `api_services.api_service_list` rows referenced `pirate_garmin_login`.
Actual query on the live database (2026-11-01) shows:

- `tasks.task_configuration.python_execution_function` — **0 rows** reference the old name (15 rows total, all N/A or unrelated)
- `api_services.api_service_list.api_service_function` — **1 row** references `backend_functions.service_logins.pirate_garmin_login`

Rick's decision: acceptable to leave this 1 dev-DB row broken until merge to production, at which time it is rewritten. The alias was removed in INT-18-final because a 1-row dev-only deferral does not justify permanent code debt.

Artifact retained: `scripts/migrations/int-18-rename-db-functions.sql` for use at merge time. It includes placeholder table names and must be adapted to the actual schema before running.

## Section 2 - Policy (applies to every task in section 4)

1. **Export-preservation on moves.** A move changes the module path; it does not rename the symbols inside. If `health.py` exported `health_router`, `pifitness/health/router.py` still exports `health_router`. Renames are separate tasks.
2. **Commits-per-task.** One task = one commit. Message format: `reorg(<scope>): <task-slug> [<task-id>]`. `wip(...)` commits are for checkpoints only.
3. **Verification-per-task.** Every task's `Success criteria` table must pass before `[DONE]`. A failed check -> task is `[BLOCKED]`, not `[DONE]`.
4. **Rollback-by-reset.** Every task has an explicit rollback command. For non-commit tasks, rollback is "delete these files." For commit tasks, rollback is `git reset --hard <sha>` where `<sha>` is HEAD before the task.
5. **Decisions become actions.** No section 3 to hold decisions. When a decision is needed, it becomes a sub-task's `Actions`. The ledger records the resulting commit as `DONE`. There is no `DECISIONS.md`.

## Section 3 - Decisions Needed

*(Empty. All prior decisions have been folded into task bodies in section 4. Any new decision that surfaces will be inserted as a task Action, not written here.)*

## Section 4 - Tasks (historical phases 0-6)

### Next batch - run in order

#### Task T0.5 - Install `rope`, build `tools/refactor/move_python.py`, fixture-test  `[AGENT]` `[TODO]`

**Scope:** `.venv` (adds `rope`); creates `tools/refactor/move_python.py`, `tools/refactor/tests/fixture/`, `tools/refactor/tests/test_move_python.ps1`. No source moves yet.
**Depends on:** none. **Environment:** plain PowerShell; `$py = ".venv\Scripts\python.exe"`.

**Actions:**

1. Install rope:
   ```
   & ".venv\Scripts\python.exe" -m pip install rope
   ```
2. Create `tools\refactor\move_python.py` with this contract:
   - CLI: `python move_python.py <manifest.json> (--dry-run | --apply)`
   - Manifest: `{"old.module.path": "new.module.path", ...}`
   - Uses `rope.refactor.move.MoveModule` (or `rope.refactor.rename.Rename` if installed rope version moved the API - check `python -c "import rope.refactor; print(dir(rope.refactor))"` first).
   - Prints per-file rewrites on `--dry-run`, applies on `--apply`.
   - Exits non-zero if any module cannot be resolved.
3. Create fixture at `tools\refactor\tests\fixture\`:
   - `pkg_a\__init__.py`, `pkg_a\foo.py` (`def hello(): return "hi"`)
   - `pkg_b\__init__.py`
   - `user.py` (`from pkg_a.foo import hello`; `hello()`)
   - `manifest.json` -> `{"pkg_a.foo": "pkg_b.foo"}`
4. Create `tools\refactor\tests\test_move_python.ps1`:
   - Copies fixture to temp dir, `cd` temp, runs `& $py <path>\move_python.py manifest.json --apply`
   - Asserts: `pkg_b\foo.py` exists; `pkg_a\foo.py` absent; `user.py` contains `from pkg_b.foo import hello`; `& $py -c "import user; print(user.hello())"` prints `hi`.
   - Removes temp dir.
5. Commit: `reorg(tool): rope wrapper + fixture [0.5]`

**Success criteria:**

| # | Check | Expected |
|---|---|---|
| 1 | `& $py -m pip show rope` | exit 0 |
| 2 | `powershell -File tools\refactor\tests\test_move_python.ps1` | exit 0 |
| 3 | `tools\refactor\move_python.py` exists, has `--apply` and `--dry-run` args | `Select-String -Path tools\refactor\move_python.py -Pattern "--apply\|--dry-run"` -> >=1 match |
| 4 | `git status --short` after commit | empty |

**If verification fails:** the fixture test *is* the debug surface. Do not test on real source. Fix `move_python.py` until the fixture passes, then re-commit.

**Rollback:** `git reset --hard HEAD~1; & ".venv\Scripts\python.exe" -m pip uninstall -y rope; Remove-Item -Recurse tools\refactor`

---

#### Task 2.5.dep - Fix `deploy_react.sh` manifest reference  `[AGENT]` `[TODO]`

**Scope:** `deploy/scripts/deploy_react.sh`, `deploy/scripts/library_requirements.txt` (only if merge needed). No source code.
**Depends on:** none - parallel-safe with 0.5. **Environment:** plain PowerShell for inspection; `[git-bash]` for `bash -n`.

**Actions:**

1. Inspect what `deploy_react.sh` expects:
   ```
   Select-String -Path deploy\scripts\deploy_react.sh -Pattern "REQUIREMENTS_FILE|requirements-react|library_requirements"
   ```
2. Inspect what `library_requirements.txt` contains:
   ```
   Get-Content deploy\scripts\library_requirements.txt
   ```
3. Inspect what `deploy_react.sh` was trying to install. Reference commit `bb90e2c^` where `requirements-react.txt` existed:
   ```
   git show bb90e2c^:deploy/scripts/requirements-react.txt
   ```
4. **If `library_requirements.txt` already contains every package from the historical `requirements-react.txt`** -> repoint only:
   - In `deploy\scripts\deploy_react.sh`, change `REQUIREMENTS_FILE="requirements-react.txt"` -> `REQUIREMENTS_FILE="library_requirements.txt"`
5. **If `library_requirements.txt` is missing packages from the historical file** -> merge first, then repoint:
   - Append missing lines to `deploy\scripts\library_requirements.txt`
   - Then do the repoint from step 4.
6. Validate shell syntax:
   ```
   & "C:\Program Files\Git\bin\bash.exe" -n deploy/scripts/deploy_react.sh
   ```
7. Commit: `reorg(2.5): repoint deploy_react.sh to library_requirements.txt [2.5.dep]`

**Success criteria:**

| # | Check | Expected |
|---|---|---|
| 1 | `Select-String deploy\scripts\deploy_react.sh -Pattern "requirements-react"` | zero matches |
| 2 | `Select-String deploy\scripts\deploy_react.sh -Pattern "library_requirements.txt"` | >=1 match |
| 3 | `bash -n deploy/scripts/deploy_react.sh` | exit 0, no output |
| 4 | Every package from `git show bb90e2c^:deploy/scripts/requirements-react.txt` present in `library_requirements.txt` | manual check - list both, diff visually |
| 5 | `git status --short` after commit | empty |

**If verification fails:** do not merge additional packages without seeing the diff first. Paste `git show bb90e2c^:deploy/scripts/requirements-react.txt` and `Get-Content library_requirements.txt` - we'll decide the merge together.

**Rollback:** `git reset --hard HEAD~1`

---

### Deferred - expand after 0.5 is green

- ~~**3.integrations**~~ - SUPERSEDED 2026-10-29. Reality: `pifitness/auth/` and `pifitness/config/` landed as separate slices. Garmin/Spotify login logic remains in `backend_functions/service_logins.py`.
- **3.worker** - move + rename `backend_functions/backend_tasks.py` -> `backend/pifitness/worker/worker.py`. Spec written after T0.5.1 (rename support) is green.
- **3.core** - move + rename `backend_functions/{config,domain_exceptions,logging_functions}.py` -> `backend/pifitness/core/{config,exceptions,logging}.py`. Spec written after T0.5.1 green. Must precede 3.api.
- **3.api** - move `backend/main.py` -> `backend/pifitness/api/main.py`; then `from pifitness.api.main import app` must pass. Note: `pifitness` is importable as top-level, not `backend.pifitness` - investigate sys.path before this task. Spec written after 3.core green.
- **3.0.verify, 3.2.verify, 3.3.verify** - probe tasks for the unverified rows in section 1.
- **3.4** - clean-room check, blocked on 3.api.
- **4.1-4.4** - frontend flatten + features/ restructure.
- **5.1-5.4** - guardrails.

Each will be written in full task format once its predecessor is green. No partial-spec tasks.

## Section 5 - Test baseline

Full suite runs once per phase, not per task. Task-level verification uses targeted paths.

```
Full suite  : & ".venv\Scripts\python.exe" -m pytest -q
Fast-lane   : & ".venv\Scripts\python.exe" -m pytest -m 'not slow and not live' -q  (183 passed, ~15s)
Compileall  : & ".venv\Scripts\python.exe" -m compileall -q backend backend_functions tests
Targeted    : & ".venv\Scripts\python.exe" -m pytest -q <specific/paths>
```

After every task-commit:
```
& ".venv\Scripts\python.exe" scripts\repo_audit all
& ".venv\Scripts\python.exe" scripts\repo_audit check
```

## Section 6 - Python 3.15 Upgrade (queued, spec deferred)

Runs after INT-29 (or after `3.finalize` if that phase activates first). Placeholder only - full task spec at that time.

Scope: upgrade `.venv` to 3.15; verify FastAPI/libcst/cryptography; audit `open()` for `encoding=`; update `requires-python`; verify Pi 5 install path; regression test suite.