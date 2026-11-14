# PiFitness `autodeploy.sh` Requirements

Replaces `deploy/scripts/bootstrap.sh` with a greenfield `deploy/scripts/autodeploy.sh`.

---

## 1. Purpose and Workflow

Deploy changes developed on a Windows PC to a Raspberry Pi, first to **TEST**, then to **PRD**. PRD is a first-class environment with its own step sequence; it is not a variant of TEST.

**Operating constraint:** there are no manual workflows on the Pi. The launcher is the only thing that runs. It pulls the requested commit into a temp folder and calls `autodeploy.sh`, which performs every action. `/home/god/PiFitness` is the PRD application directory — a disposable, atomically-swapped checkout — and nothing else. Git-to-Pi is one-directional and only ever invoked by the shell script; nothing else on the Pi reads it as a git repo.

**Before launching, the developer has:**
1. Finished local development on the `feature-test` branch (for a TEST run) or merged into `main` (for a PRD run)
2. Tested locally
3. Committed and pushed to `origin`

**TEST deployment sequence:**
1. Developer runs the Pi launcher and picks TEST.
2. `autodeploy.sh` checks readiness and collects every decision (Step 1). It then deploys the exact commit to TEST and runs the regression tests **while PRD is still serving** (Steps 2–6).
3. `autodeploy.sh` cuts over: it stops PRD, starts TEST, and runs the smoke tests against the running TEST stack (Steps 7–9).
4. Developer runs the manual test script against TEST and answers GO or NO-GO (Step 10).
5. On GO, developer merges `feature-test` into `main` locally and re-runs the launcher, choosing PRD.

**PRD deployment sequence:**
1. Developer runs the launcher and picks PRD.
2. `autodeploy.sh` runs a read-only pre-flight (Step 1). No prompt.
3. It stops PRD services (Step 2), swaps in a fresh `main` checkout (Step 3), reconciles the PRD venv and dependencies (Step 4).
4. If `dbmate status` (now that the migrations directory is checked out) shows pending migrations, it captures a PRD rollback baseline (Step 5) and applies the migrations (Step 6).
5. It starts PRD services (Step 7), runs regression tests (Step 8) and smoke tests (Step 9) against the live PRD deployment.
6. **Success = all tests passed.** Any failure triggers automatic rollback (code, units, database) via the exit handler.

---

## 1.1 Step order at a glance

### TEST

| Phase | Step | Name | PRD state | Developer input |
|---|---|---|---|---|
| **A. Decide** (read-only) | 1 | Pre-flight and decisions | Running | Database choice (only if TEST compatible) |
| **B. Prepare TEST** (PRD still up) | 2 | Clean TEST application checkout | Running | None |
| | 3 | Reconcile dependencies | Running | None |
| | 4 | Database clone / restore (only if required) | Running | None |
| | 5 | Apply pending migrations | Running | None |
| | 6 | Regression tests (pytest, npm) | Running | None |
| **C. Cut over** | 7 | Record and stop PRD services | Stopped | None |
| | 8 | Start TEST services | Stopped | None |
| | 9 | Smoke tests (against running TEST) | Stopped | None |
| **D. Human gate** | 10 | Manual test hand-off (GO / NO-GO) | Stopped | GO or NO-GO |

**Why this order (TEST):**
- **Read-only checks first.** Anything that can fail cheaply fails before anything is changed.
- **Cheap before expensive.** Checkout and dependencies run before the database restore, so a bad lockfile never wastes a restore.
- **Prepare while PRD is up.** PRD downtime is limited to Steps 7–10, not the whole run.
- **Smoke tests need a running stack,** so they run after TEST services are up.

### PRD

| Phase | Step | Name | PRD state | Developer input |
|---|---|---|---|---|
| **A. Decide** (read-only) | 1 | Pre-flight | Running | None |
| **B. Cut over** | 2 | Record and stop PRD services | Stopped | None |
| | 3 | Clean PRD application checkout | Stopped | None |
| | 4 | Reconcile dependencies | Stopped | None |
| | 5 | Take PRD rollback baseline (only if migrations pending) | Stopped | None |
| | 6 | Apply pending migrations | Stopped | None |
| | 7 | Start PRD services | Stopped | None |
| **C. Verify** | 8 | Regression tests (pytest, npm) against PRD | Running | None |
| | 9 | Smoke tests against live PRD | Running | None |
| *(no human gate)* | | Success = run complete | | |

**Why this order (PRD):**
- **Read-only pre-flight first** so a missing tool or unreachable PRD DB never takes PRD down.
- **Stop services before touching the app dir** because PRD services execute from `/home/god/PiFitness`; the swap requires exclusive access.
- **Baseline only if migrations are pending.** A no-op migration run never pays the ~10-minute dump.
- **Tests run after services start** because smoke tests hit the live URL.
- **No human gate.** Any failure triggers the exit handler's rollback (code + units + database).

---

## 2. Golden Rules

1. **Launcher is dumb.** `/home/god/pifitness-bootstrap/bootstrap.sh` only pulls the right branch into a temp folder and calls the deploy script. It rarely changes.
2. **`autodeploy.sh` is a dumb orchestrator.** It performs *functions* and contains no logic specific to any one deployment. The repo changes underneath it.
   - Required packages, versions and tools come from files in the repo (lockfiles, config), never from the script.
   - Example: migrations are discovered at run time by reading the migrations directory, never hardcoded.
3. **Modular.** Each step lives in its own helper under `deploy/scripts/lib/`. `autodeploy.sh` only sequences them.
4. **Use free, canonical tools** (dbmate, uv, npm ci, pytest, systemctl, git, pg_dump/pg_restore) instead of custom code wherever one exists.
5. **Everything the script needs lives in `deploy/scripts/` and `deploy/systemd/`** (or is read from the checked-out repo *after* Step 2 / Step 3). The launcher extracts only those two directories from the commit into a temp folder, and that folder is deleted on exit.
6. **Running copy is immune to the checkout swap.** Both the TEST swap (TEST Step 2) and the PRD swap (PRD Step 3) replace an app directory, but the running script executes from the launcher's temp folder, so it is unaffected.
7. **Fail fast, cheapest first.** Read-only validation precedes any change. No slow or destructive operation starts until every question has been asked and every foreseeable failure has been checked.
8. **Ask once, up front.** All input is gathered in TEST Step 1. After Step 1 the run needs no one until the GO / NO-GO in Step 10. PRD runs prompt **nobody**.
9. **Keep PRD up as long as possible.** PRD is stopped only when TEST actually needs the shared URL and ports (TEST Step 7). A PRD run takes it down as a necessity of the deploy.
10. **PRD rollback is complete or it is a failure.** The exit handler restores code, units and database to their pre-run state; a partial rollback is itself a failure and is reported as one.
11. **Never trust "most recent" at restore time.** Rollback restores the **specific** dump recorded in the state file, not whatever the newest file happens to be.
12. **Restore state automatically; never ask the user to run restore commands.** If a prior TEST run left units active, `test-down.sh` is invoked automatically.
13. **The Pi is unattended.** No manual workflows run on the Pi. The launcher is the only entry point, and the only file system it writes outside the app dirs, log dir, state files and backup dir is what `autodeploy.sh` explicitly manages.

---

## 3. Output Contract

**Per-step format:**

```
[STEP n/N] <Step name> ......................... START
  <what it is doing>
  EVIDENCE: <key> = <value>
  EVIDENCE: <key> = <value>
[STEP n/N] <Step name> ......................... PASS (4.2s)
```

- `N` = **10** for TEST, **9** for PRD.
- Failure ends with `FAIL (4.2s)` followed by `REASON: <one-line cause>`, `WHERE: <command / file / line>`, and the last relevant lines of the failing tool's output.
- A PRD run that reaches a step with nothing to do prints `SKIPPED (<reason>)`.

**Rules:**
- **Evidence over assertion.** A step never just says "done". It prints the facts that prove it.
- **No silent success.** Every command whose result matters is checked, and its outcome is printed.
- **Final summary table** at the end of every run, whatever the outcome.
- **Full log** written to `/home/god/pifitness-logs/autodeploy-<env>-<UTC timestamp>-<short SHA>.log`, and printed at start and end. Screen output and log are identical.
- **Unbuilt steps** print `[STEP n/N] <name> NOT IMPLEMENTED` and the run stops cleanly there (running cleanup).
- **Exit codes:** `0` = all built steps passed (and GO if reached for TEST), non-zero = failure, NO-GO or timeout.
- **Re-runnable.** Safe to start again after any failure or interruption, with no manual cleanup.

---

## 4. Cross-Cutting Behavior

### 4.1 Arguments
`--environment test|prd` and `--commit <40-char SHA>` (both required, SHA validated as 40 hex characters). Both are supplied by the launcher. `--through <n>` stops cleanly after step *n* (per-environment numbering).

### 4.2 Exit handler (installed first, active from the first line)

On **any** exit (success, failure, Ctrl-C, timeout), for either environment:

1. **Stop TEST services.** No-op if already down.
2. **TEST run** — restore exactly the PRD units that were active when TEST Step 7 recorded them. If Step 7 has not run, PRD was never touched, and this is reported as "PRD was never stopped."
3. **TEST run** — roll back the TEST database to the clean baseline only when the state marker is `dirty` and the outcome is not GO. Leave it as-is on GO.
4. **PRD run** — restore PRD services to the state recorded in PRD Step 2 (no-op if Step 2 did not complete).
5. **PRD run** — if the outcome is not success **and** the PRD state marker is `dirty`, restore the PRD database from the specific baseline dump recorded in the state file (`baseline_dump=<path>`). If the restore cannot be performed, the exit handler itself reports FAIL and the overall exit code is non-zero.
6. **PRD run** — if the outcome is success, leave the PRD database as-is (migrated).
7. Print the cleanup result as part of the final summary.

The handler is built **before** the steps that need it. Each new step registers what it needs the handler to undo.

### 4.3 Development convenience
`--through <n>` stops cleanly after step *n* (running cleanup). For PRD, `--through 5` is a useful way to verify baseline capture without applying migrations.

### 4.4 TEST database state marker

A small state file (`TEST_STATE_FILE`, default `/home/god/.config/pifitness/test-db-state`, kept outside the app directory) records the TEST database's condition:
- `clean`: equals the clean baseline. Set only **after** a clone/restore has completed and the baseline has been saved, or after a successful rollback.
- `dirty`: may differ from the baseline. Set **immediately before** the first thing that can write to the TEST database: the restore in TEST Step 4 (when it runs), and the migration in TEST Step 5 (which also covers the tests and manual testing that follow).
- `migrated`: set on GO.

The exit handler rolls back to the clean baseline **only** when the state is `dirty`, TEST Step 4 is not the step that failed, and the outcome is not GO. A failure in TEST Steps 1–3 costs no restore time. The state file is only a hint: TEST Step 1 always inspects the actual database and never trusts the file alone. A `dirty` marker found at the start of a run (an interrupted restore or crash) is treated by TEST Step 1 as an unusable database.

**Clean baseline.** A **pinned dump file** produced by `pg_dump -Fc`, with a distinct prefix so it never pollutes the nightly backup glob:

- Filename: `test_baseline_<UTC timestamp>.dump` (`CLEAN_BASELINE_PREFIX`, default `test_baseline_`).
- Location: `$PIFITNESS_PRD_BACKUP_DIR` (same directory as the nightlies).
- **Created only when TEST Step 4 performs a clone/restore.** Survives across deployments.
- The **state file records its exact path** (`baseline_dump=<path>`). Rollback restores *that specific file*, never "the most recent one."

### 4.5 PRD database state marker

Symmetric to TEST, a small state file (`PRD_STATE_FILE`, default `/home/god/.config/pifitness/prd-db-state`) records the PRD database's condition during a PRD run:

- `clean`: equals the recorded baseline. No PRD writes have occurred in this run.
- `dirty`: PRD Step 5 has captured a baseline and PRD Step 6 (or a later step) is in progress. The DB may differ from the baseline.
- `migrated`: set after PRD Step 9 passes. The DB now contains this run's migrations — it is the new known-good state.

The exit handler rolls back PRD to the **specific** baseline dump recorded in the state file (`baseline_dump=<path>`) when the state is `dirty` and the outcome is not success. It does not consult "the most recent dump" at rollback time.

**PRD rollback baseline.** A **pinned dump file**, distinct prefix:

- Filename: `prd_baseline_<UTC timestamp>.dump` (`PRD_BASELINE_PREFIX`, default `prd_baseline_`).
- Location: `$PIFITNESS_PRD_BACKUP_DIR` (same directory).
- **Created only when PRD Step 6 has pending migrations to apply.** A no-op migration run never pays the capture.
- The state file records the exact path.

**Crash recovery.** A `dirty` marker found at the start of a PRD run (interrupted mid-run) means the PRD DB is in an unknown state. PRD Step 1 reports this; PRD Step 5 then **restores** from the recorded baseline instead of capturing a new one, then clears `dirty` back to `clean` so Step 6 can proceed from a known-good state.

### 4.6 Interaction policy

- **TEST run:** exactly one prompt is guaranteed — GO / NO-GO in TEST Step 10. One additional prompt may appear in TEST Step 1 (database choice, only when TEST is compatible with PRD). Both require a terminal; TEST Step 1 checks for one.
- **PRD run:** **no** prompts. Success = all tests pass; failure = automatic rollback. PRD runs do not require an interactive terminal (they just log to the log dir).
- **Restore is never a prompt.** If a prior run left TEST units active, `test-down.sh` is invoked automatically by Step 1 — the user is never asked to know which command restores state.

---

## 5. Steps

### 5.1 TEST deployment (10 steps)

---

### TEST Step 1: Pre-flight and decisions
**Objective:** Prove, without changing anything, that the run can succeed, and collect every answer the developer must give.

**Required behavior:**
1. **Read-only**, with one exception: **if any TEST unit is active, run `test-down.sh` automatically** to return to a known state. This is a remediation, not a decision — see 4.6.
2. **Uses only what exists before checkout:** `autodeploy.conf` (from the launcher's temp folder) and the Pi env file.
3. **Readiness checks**, each failing early with a clear message:
   - An interactive terminal is present.
   - Required system tools are present (`git`, `psql`, `pg_dump`, `pg_restore`, `systemctl`, `dbmate`, `uv`, `node`, `npm`). Tools that Step 3 installs are reported present/missing, not failed.
   - The TEST and PRD databases are reachable, and the TEST connection target is verified as **not** PRD.
   - Every unit the commit defines (`SERVICES_LIST` / `TIMERS_LIST`, read from `deploy/systemd`) is installed on the Pi. The units running on the Pi (`ACTIVE_SERVICES` / `ACTIVE_TIMERS`) are discovered, not checked.
   - The log directory and persistent-resource paths exist and are writable.
   - The backup directory (`$PIFITNESS_PRD_BACKUP_DIR`) exists and is writable.
4. **Database comparison** using dbmate's applied-migrations table and a schema-only dump comparison. Migrations table read directly with SQL (the migrations directory is not checked out yet).
5. **Classify TEST** as:
   - **Unusable**: missing, unreachable, lacking the TEST marker, or `TEST_STATE_FILE` shows `dirty` from an interrupted run. Treated as Incompatible.
   - **Incompatible**: TEST contains structural changes PRD does not have. Automatically use an existing PRD backup in Step 4. No prompt.
   - **Compatible**: TEST is identical to PRD or merely *behind* PRD. Prompt `[K] Keep  [R] Restore most recent nightly  [C] Take a fresh clone`; the prompt waits indefinitely.
6. If a restore is required (Incompatible, Unusable, or the developer chose R or C) and no usable PRD backup exists, **fail now**.
7. Store the decision for Step 4 and print a **plan**: the database action, the PRD units that will be stopped, and the statement that no further input is needed until Step 10.

**Required evidence:** terminal check, tool presence list, units defined by the commit and units running (`SERVICES_LIST`, `TIMERS_LIST`, `ACTIVE_SERVICES`, `ACTIVE_TIMERS`), PRD migration versions (latest + count), TEST migration versions (latest + count), versions in TEST but not PRD, versions in PRD but not TEST, schema diff line count, classification, newest nightly snapshot filename and age, WARNING if the snapshot's migration versions differ from live PRD, decision taken, plan.

**Required evidence (auto test-down):** "TEST units found active: <list>; ran test-down.sh: PASS/FAIL", and the resulting state file path.

---

### TEST Step 2: Clean TEST application checkout
**Objective:** The TEST application directory contains exactly the requested commit's files, plus explicitly configured persistent resources, and nothing else.

**Required behavior:**
1. Create a temporary checkout of the exact requested commit on the same filesystem as the TEST app dir.
2. Verify: `HEAD` equals the requested SHA, working tree clean, expected top-level paths exist.
3. Swap it in atomically.
4. Delete the previous checkout only after the swap succeeds.
5. Persistent resources listed in `autodeploy.conf` carried across or kept outside the app dir. **The TEST venv is outside the app dir at `TEST_VENV_DIR`** and is never touched by the swap.
6. The TEST env file is **symlinked** into the checkout at `backend/.env` (matches legacy `bootstrap.sh:279`).
7. Does not stop/start services, touch the venv contents, or touch the database.

**Required evidence:** requested SHA, actual SHA of new checkout, clean-tree check result, count of files in checkout, list of persistent resources preserved, path of old checkout removed, number of untracked files (must be 0 apart from persistent resources).

**On failure:** previous checkout is untouched and still active; temp checkout is removed.

---

### TEST Step 3: Reconcile the TEST virtual environment (and other dependencies)
**Objective:** The TEST venv contains exactly the packages the repo requires, with the exact pinned versions, and nothing more.

**Required behavior:**
1. Source of truth: `uv.lock` + `pyproject.toml`.
2. `uv sync --locked` with `UV_PROJECT_ENVIRONMENT=$TEST_VENV_DIR`. Dependency groups come from config.
3. Required system tools/binaries from config; use `ensure_binary` where a tool must be installed.
4. Node deps installed with `npm ci` from the lockfile.
5. Never touches the system Python.

**Required evidence:** uv version, `uv.lock` hash, venv Python version, packages added/removed/changed, second sync "no changes", `uv pip check` result, version of each declared tool, "extraneous packages after sync: 0".

**On failure:** report which package/tool failed and the pip/npm error. No partial claims of success.

---

### TEST Step 4: Database clone / restore (only if TEST Step 1 requires it)
**Objective:** A running TEST database that is a faithful, correctly marked and permissioned copy of PRD, with a clean baseline dump saved.

**Required behavior:**
1. Skip with `SKIPPED (not required)` when Step 1 chose to keep the current TEST database. Verify the clean baseline from an earlier clone still exists and print its age.
2. Otherwise restore from the nightly PRD snapshot or take a fresh clone. **Fresh clone = `pg_dump -Fc` from PRD then `pg_restore` into TEST.** Print elapsed time at intervals.
3. Set the TEST state marker to `dirty` **before** the TEST database is overwritten.
4. Ensure TEST is marked as TEST and has TEST-only roles.
5. **Verify the write target is not PRD** before any write.
6. On clone/restore completion: create the clean baseline dump with the `test_baseline_` prefix, record its path in the state file (`baseline_dump=<path>`), then set the state to `clean`.
7. **On failure:** leave the state `dirty`, do not attempt a second automatic restore, report the cause. Next run's Step 1 treats the DB as Unusable.

**Required evidence:** source of data (backup filename or "fresh clone"), TEST DB host/name/port, marker value, app user & privileges, row counts for a few key tables vs PRD, schema diff vs PRD (must be zero), baseline dump path & size, and the explicit line `PRD database was not modified`.

---

### TEST Step 5: Apply pending migrations (dbmate)
**Objective:** TEST database is migrated to the schema in the checked-out commit.

**Required behavior:**
1. Discover migrations by letting dbmate read the configured `MIGRATIONS_DIR`. Nothing hardcoded.
2. Print pending migrations, set the TEST state marker to `dirty`, then run `dbmate up` against TEST only.
3. **On failure:** roll back to the clean baseline dump (`pg_restore` from the path recorded in the state file), report which migration failed, what the SQL error was, and confirm the rollback.
4. If nothing is pending, report that and pass.

**Required evidence:** pending migrations before, applied migrations, `dbmate status` afterward, on failure the failing filename + error text + "rolled back to clean baseline: yes/no".

**How you verify:** Include a deliberately broken migration on a throwaway branch and confirm the failure message, the rollback, and that a rerun starts from a clean DB.

---

### TEST Step 6: Regression tests (pytest and npm)
**Objective:** Automated tests pass against the TEST database and new code, before PRD is touched.

**Required behavior:**
1. Run `pytest` and `npm test` from `deploy/tests.conf`.
2. Run them against the TEST database only.
3. These suites must **not** depend on the TEST services running and must not bind shared ports. Verified: all 257 backend tests use `TestClient` in-process; frontend is vitest+jsdom. Tests that need the running stack belong in TEST Step 9.
4. Run both suites even if one fails, then fail the step if any failed.

**Pass criteria:** Both suites exit 0.

**Required evidence:** per suite: command, exit code, passed/failed/skipped counts, duration; test reports saved to the log directory; on failure the failing test names and last lines of output.

---

### TEST Step 7: Record and stop PRD services
**Objective:** Free the shared URL and ports safely, remembering exactly how to put PRD back. PRD downtime begins here.

**Required behavior:**
1. Read the PRD units to stop from `ACTIVE_SERVICES` / `ACTIVE_TIMERS` (the units running on the Pi when the run began, recorded by Step 1).
2. **Record** which units are active **before** stopping, and persist the record (on disk, outside the app dir) so the exit handler can restore it even after a crash.
3. Stop them gracefully (timers first, then services), wait with a timeout.
4. Register restoration with the exit handler **before** stopping.
5. Units that were not active are not started on restore.
6. Confirm the ports TEST needs are free.

**Required evidence:** table of unit name / state before / state after; path of the saved record; TEST ports and that they are free.

**How you verify:** Manually stop one PRD timer beforehand, run through the end, and confirm that timer is not started during restore.

---

### TEST Step 8: Start TEST services
**Objective:** TEST runs and is served at `https://pifitness.duckdns.org`, intentionally replacing PRD there.

**Required behavior:**
1. Start the TEST units (`SERVICES_LIST` / `TIMERS_LIST`, from the commit's `deploy/systemd`); wait until healthy (URL and/or `systemctl is-active` + HTTP).
2. Verify the public URL is served by TEST, not PRD (version/commit/environment endpoint or header).
3. Fail if TEST is not healthy within the timeout.

**Required evidence:** unit states, health check result and response time, URL and what it reported (environment = TEST, SHA), confirmation PRD units are inactive.

---

### TEST Step 9: Smoke tests (against the running TEST stack)
**Objective:** The deployed TEST stack, as the developer will use it, passes the automated end-to-end checks.

**Required behavior:**
1. Run smoke commands from `deploy/tests.conf` against the running TEST services and TEST URL.
2. Run every smoke command even if one fails, then fail the step if any failed.
3. A smoke failure ends the run as a failed deployment. Manual hand-off is not offered.

**Required evidence:** per command: command, target URL, exit code, passed/failed/skipped, duration; reports saved to the log directory.

---

### TEST Step 10: Manual test hand-off (GO / NO-GO)
**Objective:** Developer runs the manual test script, then explicitly decides.

**Required behavior:**
1. Print TEST URL, deployed SHA, instruction to run the manual test script.
2. Prompt `Enter GO or NO-GO`. Accept only those (case-insensitive); reprompt otherwise.
3. **30 minutes with no answer = failure** (treated like NO-GO). Countdown/reminder at intervals.
4. **GO:** print the exact git commands to merge `feature-test` into `main`, and the instruction to re-run the launcher and select PRD.
5. **NO-GO / timeout:** report as a failed deployment.
6. Fall through to the exit handler, which stops TEST and restores PRD.

**Pass criteria:** An explicit GO.

**Required evidence:** prompt, answer (or "timeout after 30m"), elapsed time, merge command on GO.

---

### 5.2 PRD deployment (9 steps)

PRD runs share most mechanics with TEST but differ in checkout source, env wiring, sequencing and rollback scope. Steps below **fully specify** the PRD-specific behavior and cross-reference the TEST step where behavior is identical.

---

### PRD Step 1: Pre-flight
**Objective:** Prove, without changing anything, that the PRD run can succeed. **No prompt.**

**Required behavior:**
1. **Read-only**, with one exception: **if any TEST unit is active, run `test-down.sh` automatically** (same as TEST Step 1; see 4.6).
2. **Readiness checks**, each failing early:
   - Required system tools (`git`, `psql`, `pg_dump`, `pg_restore`, `systemctl`, `dbmate`, `uv`, `node`, `npm`).
   - PRD database reachable and writable.
   - The units running on the Pi are discovered and recorded (`ACTIVE_SERVICES` / `ACTIVE_TIMERS`); there is nothing to check because nothing is listed.
   - The log directory, app dir parent, and backup directory exist and are writable.
   - Any stale `dirty` marker in `PRD_STATE_FILE` is noted (recovery will restore from the recorded baseline in Step 5).
3. **No interactive terminal is required** (PRD runs prompt nobody).
4. **No DB schema comparison** is performed in Step 1 — the migrations directory is not checked out yet. That decision is made in Step 5 after checkout.
5. Print a **plan**: units that will be stopped, whether a baseline will be taken (decided in Step 5), and the statement that no input is needed.

**Required evidence:** tool presence list, PRD DB reachability, units defined by the commit and units running, writable paths, plan; if `test-down` ran: "TEST units found active: <list>; ran test-down.sh: PASS/FAIL", resulting state file path.

---

### PRD Step 2: Record and stop PRD services
**Objective:** Free the ports and stop serving gracefully. PRD downtime begins here.

**Required behavior:** Same mechanics as TEST Step 7, but:
- Stops the **PRD** active set (`ACTIVE_SERVICES` / `ACTIVE_TIMERS`, recorded by Step 1); no unit list is configured.
- Writes the state file so the exit handler can restore even after a crash.

**Required evidence:** table of unit / state before / state after; path of the saved record.

---

### PRD Step 3: Clean PRD application checkout
**Objective:** The PRD application directory `/home/god/PiFitness` contains exactly the requested `main` commit's files plus configured persistent resources.

**Required behavior:** Same mechanics as TEST Step 2, but:
- Branch = `main` (from the `--commit` SHA supplied by the launcher).
- Target directory = `PRD_APP_DIR` (`/home/god/PiFitness`).
- **The PRD env file is copied** (not symlinked) from `PRD_ENV_FILE` to `backend/.env` inside the new checkout. A copy survives the swap + rollback without a dangling symlink.
- **The previous checkout is kept until the exit handler decides whether to keep it.** On success, it is deleted by the exit handler; on failure, it is restored.
- **The PRD venv is outside the app dir** at `PRD_VENV_DIR` and is never touched by the swap.

**Required evidence:** requested SHA, actual SHA, clean-tree check, file count, persistent resources preserved, PRD env file copied (source → dest), previous checkout path (retained until success), number of untracked files (must be 0 apart from persistent resources).

**On failure:** previous checkout is still active (the swap is atomic and reversible by the exit handler).

---

### PRD Step 4: Reconcile dependencies
**Objective:** The PRD venv contains exactly the packages the repo requires.

**Required behavior:** Same mechanics as TEST Step 3, but:
- `UV_PROJECT_ENVIRONMENT=$PRD_VENV_DIR`.
- Verifies the new PRD venv is **outside** `PRD_APP_DIR` (must be, or the swap breaks it).

**Required evidence:** uv version, `uv.lock` hash, venv Python version, packages added/removed/changed, second sync "no changes", `uv pip check` result, version of each declared tool, "extraneous packages after sync: 0".

---

### PRD Step 5: Take PRD rollback baseline (only if migrations pending)
**Objective:** A pinned PRD dump file that the exit handler can restore if the deploy fails.

**Required behavior:**
1. With the checkout now available, run `dbmate status` against PRD to determine whether migrations are pending.
2. If **no migrations pending:** `SKIPPED (no migrations pending)`, do not take a dump, leave the state `clean`.
3. If **migrations pending and state is `clean`:** take a `pg_dump -Fc` from PRD to a `prd_baseline_<UTC>.dump`, record the path in the state file (`baseline_dump=<path>`), then set state to `dirty`.
4. If **state is already `dirty`** (crash recovery): **restore** the PRD database from the recorded baseline dump instead of capturing a new one. On success, set state to `clean` so Step 6 can proceed; the new run will capture a fresh baseline after Step 6's `dbmate status`.
5. Print elapsed time at intervals (the dump is ~10 minutes).

**Required evidence:** `dbmate status` (pending count, latest applied, latest pending), decision taken (skip / capture / restore), baseline dump path & size (when captured), state marker value before and after, elapsed time, and the explicit line `PRD database was not written by this step` (dump is read-only) — or `PRD database restored to <baseline_dump>` in recovery.

**On failure:** leave the state `dirty`. Exit handler will not attempt a second restore (see 4.2 item 5); the next run's Step 1 will detect the stale `dirty` marker and Step 5 will restore.

---

### PRD Step 6: Apply pending migrations (dbmate)
**Objective:** PRD database is migrated to the schema in the checked-out `main` commit.

**Required behavior:** Same mechanics as TEST Step 5, but:
- Migration runs against **PRD** only.
- Set `dirty` immediately before `dbmate up`.
- **On failure:** the exit handler restores PRD from the pinned baseline dump (not the TEST clean baseline).

**Required evidence:** pending migrations before, applied migrations, `dbmate status` after, on failure the failing filename + error text + "rolled back to PRD baseline: yes/no".

---

### PRD Step 7: Start PRD services
**Objective:** PRD runs and is served at `https://pifitness.duckdns.org`.

**Required behavior:** Same mechanics as TEST Step 8, but:
- Re-applies the recorded **PRD** active set (`ACTIVE_SERVICES` / `ACTIVE_TIMERS`): the units that were running when the run began.
- Verifies the public URL is served by **PRD** (environment = PRD, deployed SHA = the `main` SHA).
- Fails if PRD is not healthy within the timeout. On failure, the exit handler rolls back code, units and DB.

**Required evidence:** unit states, health check result and response time, URL response (environment = PRD, SHA), confirmation TEST units are inactive.

---

### PRD Step 8: Regression tests (pytest and npm) against PRD
**Objective:** The PRD code passes the same automated tests that passed on TEST, now against the PRD database.

**Required behavior:** Same mechanics as TEST Step 6, but:
- Runs against the PRD deployment (PRD env).
- Same two suites from `deploy/tests.conf`.
- Run both suites even if one fails.

**Caveat — test writes to the PRD database.** Backend tests instantiate the FastAPI app via `TestClient`, which uses the app's configured database — i.e. the PRD database. Any test that writes therefore writes to production. Two acceptable mitigations exist:
- **(a)** accept the writes on success (single-user personal app); on failure, the exit handler's rollback to the pinned baseline removes them;
- **(b)** audit the tests and make writes transactional before the first PRD run.

This design assumes **(a)** until (b) is done. See Decision D22.

**Required evidence:** per suite: command, exit code, passed/failed/skipped, duration; reports saved to the log directory.

---

### PRD Step 9: Smoke tests against live PRD
**Objective:** The deployed PRD stack passes the automated end-to-end checks.

**Required behavior:** Same mechanics as TEST Step 9, but:
- Runs against the live PRD URL and PRD services.
- A smoke failure ends the run as a failed deployment. The exit handler rolls back code, units and DB.

**Required evidence:** per command: command, target URL, exit code, passed/failed/skipped, duration; reports saved to the log directory.

**On success:** the exit handler sets `PRD_STATE_FILE` to `migrated`, deletes the previous code checkout, and leaves the database as-is. Exit code 0.

---

## 6. Final State Guarantees

### TEST run

| Outcome | PRD services | TEST services | TEST database |
|---|---|---|---|
| Failure in TEST Steps 1–3 | Untouched (never stopped) | Stopped | Untouched |
| Failure in TEST Step 4 | Untouched (never stopped) | Stopped | Left `dirty`; next run's Step 1 restores it |
| Failure in TEST Steps 5–6 | Untouched (never stopped) | Stopped | Rolled back to clean baseline |
| Failure in TEST Steps 7–9 | Restored to prior state | Stopped | Rolled back to clean baseline |
| NO-GO / timeout | Restored to prior state | Stopped | Rolled back to clean baseline |
| GO | Restored to prior state | Stopped | Left as is (migrated, plus any test data) |

### PRD run

| Outcome | PRD services | PRD code | PRD database |
|---|---|---|---|
| Failure in PRD Step 1 | Untouched (never stopped) | Untouched | Untouched |
| Failure in PRD Step 2 | Restored to recorded state | Untouched | Untouched |
| Failure in PRD Steps 3–4 | Restored to recorded state | Rolled back to previous checkout | Untouched |
| Failure in PRD Step 5 (capture) | Restored to recorded state | Rolled back to previous checkout | Untouched |
| Failure in PRD Step 5 (recovery restore) | Restored to recorded state | Rolled back to previous checkout | Left `dirty`; next run's PRD Step 1 detects stale marker and Step 5 re-restores |
| Failure in PRD Steps 6–9 | Restored to recorded state | Rolled back to previous checkout | Rolled back to the pinned `prd_baseline_<ts>.dump` |
| Success | Recorded set re-applied | New `main` checkout live | Migrated; state `migrated` |

---

## 7. Launcher Requirements

Unchanged, except that it also extracts `deploy/systemd`. The launcher:
- Extracts `deploy/scripts` (and `deploy/systemd`, when the commit has one) from the commit and runs `bash -n` on `autodeploy.sh`.
- Calls `autodeploy.sh --environment <env> --commit <40-char SHA>` and nothing else.
- Does not hardcode helper libraries. `autodeploy.sh` verifies its own libraries at startup.

---

## 8. Configuration

**One file: `deploy/scripts/autodeploy.conf`** — plain `KEY=value` lines read with `source`. Lives in `deploy/scripts/` because the launcher extracts that folder from the exact commit. The config is available from the very first step, and always matches the code it describes.

Per-environment keys that remain use a prefix, e.g. `TEST_STEPS`, `PRD_STEPS`.

### `autodeploy.conf` (in git) — proposed keys

| Key | Example | Purpose |
|---|---|---|
| `UNIT_NAME_GLOB` | `pifitness*` | Which of the Pi's systemd units belong to PiFitness. A name pattern, not a list; must start with literal text |
| `MIGRATIONS_DIR` | `deploy/migrations` | dbmate migrations directory |
| `HEALTH_URL` | `https://pifitness.duckdns.org/api/health` | TEST and PRD health check |
| `TEST_PORTS` | `8000 3000` | Ports to free before starting TEST |
| `REQUIRED_TOOLS` | `dbmate uv node npm psql pg_dump pg_restore` | Binaries checked in Step 1 |
| `DEPENDENCY_GROUPS` | `--no-dev --group deployment` | uv sync groups |
| `PERSISTENT_RESOURCES` | `(none; venv lives outside app dir)` | Files/dirs carried across the swap |
| `CLEAN_BASELINE_PREFIX` | `test_baseline_` | TEST baseline dump prefix |
| `PRD_BASELINE_PREFIX` | `prd_baseline_` | PRD baseline dump prefix |
| `SMOKE_SUITE` | `smoke` | Suite label in `deploy/tests.conf` |

### Unit sets (worked out at run time, never configured)

No systemd unit is named in configuration. Step 1 derives two sets and records them in `run.state`; later steps read them from there.

| Set | Source | Meaning |
|---|---|---|
| `SERVICES_LIST`, `TIMERS_LIST` | The `*.service` / `*.timer` files in the commit's `deploy/systemd` | The units the commit defines (the TEST units). Any mix, including none, is valid. |
| `ACTIVE_SERVICES`, `ACTIVE_TIMERS` | `systemctl` at the start of the run: loaded units matching `UNIT_NAME_GLOB` that are active, minus the units in the lists above | What is running on the Pi when the deployment begins; what is stopped and later restored. |

The launcher extracts `deploy/systemd` beside `deploy/scripts` (optional: a commit without it defines no units) because the lists are needed in Step 1, before any checkout exists.

### `deploy/tests.conf` (in git, created) — new

Plain `KEY=value`. Each key is a shell command. `autodeploy.sh` reads this **after checkout** (safe — Steps 6/8/9 are post-checkout).

```ini
TEST_PYTEST="cd $APP_DIR && uv run pytest --junitxml=$LOG_DIR/pytest.xml"
TEST_NPM="cd $APP_DIR/frontend/pifitness && npm test -- --reporter=junit --outputFile=$LOG_DIR/npm.xml"
TEST_SMOKE="python -m pytest tests/smoke --junitxml=$LOG_DIR/smoke.xml"
```

The `$APP_DIR` and `$LOG_DIR` values are injected by `autodeploy.sh` before evaluation, so the file does not hardcode Pi paths.

### Pi env file `/home/god/.config/pifitness/<env>.env` (not in git)

| Key | Purpose |
|---|---|
| `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DB` | DB connection (per env: `pifitness` vs `pifitness_test`) |
| `APP_ENV`, `SPOTIFY_ENABLED`, `GARMIN_ENABLED`, `PIRATE_ENABLED` | App env vars |
| `NEXT_PUBLIC_APP_ENV` | `production` or `test` |
| `TEST_APP_DIR` | `/home/god/PiFitness-test` |
| `PRD_APP_DIR` | `/home/god/PiFitness` |
| `TEST_VENV_DIR` | `/home/god/.local/share/pifitness/test-venv` |
| `PRD_VENV_DIR` | `/home/god/.local/share/pifitness/prd-venv` |
| `TEST_STATE_FILE` | `/home/god/.config/pifitness/test-db-state` |
| `PRD_STATE_FILE` | `/home/god/.config/pifitness/prd-db-state` |
| `TEST_ENV_FILE` | `/home/god/.config/pifitness/test.env` |
| `PRD_ENV_FILE` | `/home/god/.config/pifitness/prd.env` |
| `LOG_DIR` | `/home/god/pifitness-logs` |
| `PIFITNESS_PRD_BACKUP_DIR` | `/home/god/Documents/DB_Backups` (already default) |
| `PIFITNESS_PRD_BACKUP_GLOB` | `personal_fitness_[0-9]*.dump` (already default) |

**No disk-space keys.** Removed per user direction.

**Startup evidence (printed before Step 1):** config path and hash, env file path, number of keys loaded, list of missing required keys. A missing key fails the run immediately, before anything is touched.

---

## 9. Decisions

| # | Decision | Resolution |
|---|---|---|
| D1 | Python sync | `uv sync --locked` from the committed `uv.lock` into the persistent venv |
| D2 | TEST DB after a run | Rolled back to the clean baseline only if dirty and not GO; left as-is on GO |
| D3 | Restore source | The PRD nightly snapshots (for TEST restore) and pinned baselines (for rollback) |
| D4 | Configuration | Single `deploy/scripts/autodeploy.conf` plus the Pi env file (section 8) |
| D5 | TEST Step 1 prompt | Waits indefinitely; no TTY means fail |
| D6 | TEST step order | Read-only pre-flight first, prepare TEST while PRD is up, cut over (section 1.1) |
| D7 | PRD downtime (TEST run) | PRD stopped as late as possible (TEST Step 7); restored on every exit |
| D8 | Smoke tests | Run after services are up (TEST Step 9, PRD Step 9) |
| D9 | Developer input | Captured in TEST Step 1; only later prompt is GO/NO-GO in TEST Step 10 |
| D10 | Pre-flight migration read | Applied migrations read directly from dbmate's migrations table (migrations dir not checked out) |
| D11 | PRD is first-class | Same orchestrator, env-aware step list; N = 10 (TEST) / 9 (PRD) |
| D12 | PRD order | Pre-flight → stop → checkout → deps → baseline → migrate → start → tests → rollback on failure; **no human gate** |
| D13 | PRD venv location | Moves to `/home/god/.local/share/pifitness/prd-venv` (outside app dir) |
| D14 | TEST clean baseline | Pinned `pg_dump` file, `test_baseline_<ts>.dump`; rollback uses the recorded path, not "most recent" |
| D15 | PRD rollback baseline | Pinned `pg_dump` file, `prd_baseline_<ts>.dump`; taken **only when migrations pending**; rollback uses the recorded path |
| D16 | Test commands | `deploy/tests.conf` (created) for regression + smoke; `autodeploy.conf` for units/ports/paths/tools/prefixes |
| D17 | Env file wiring | TEST env **symlinked** into checkout; PRD env **copied** into checkout |
| D18 | Disk-space checks | None (removed per user) |
| D19 | Auto `test-down` | Both Step 1s auto-invoke `test-down.sh` if any TEST unit is unexpectedly active |
| D20 | PRD success definition | All tests pass; no GO/NO-GO prompt; any failure = automatic rollback |
| D21 | `/home/god/PiFitness` role | PRD application directory only. No manual workflows run on the Pi; the launcher is the sole entry point and calls only the specified files from the repo. `/home/god/PiFitness` is atomically swapped on every PRD run. |
| D22 | PRD test writes | Accepted for now (single-user personal app). If any backend test writes meaningfully to the PRD database, revisit and apply mitigation (b) from PRD Step 8 before the first PRD run. |

---

## 10. Legacy paths, parameters and settings

Refer to `legacy_bootstrap_parameters.md` for the legacy environment and path conventions. The following **new** paths and names supersede the legacy ones:

| Legacy | New |
|---|---|
| `deploy/scripts/bootstrap.sh` | `deploy/scripts/autodeploy.sh` |
| `PRD` = not implemented | `PRD` = first-class (9 steps) |
| `deploy.sh` target `react-ui` | `main` (retired) |
| `/home/god/PiFitness/venv` (inside app dir) | `/home/god/.local/share/pifitness/prd-venv` (outside) |
| `$HOME/.pifitness-test-state` | `TEST_STATE_FILE` (`/home/god/.config/pifitness/test-db-state`) |
| (none) | `PRD_STATE_FILE` (`/home/god/.config/pifitness/prd-db-state`) |
| `$TEST_DIR/.env.test` (copied inside checkout) | `backend/.env` (symlinked for TEST, copied for PRD) |
| legacy `personal_fitness_<ts>.dump` (nightly) | same, plus `test_baseline_<ts>.dump` and `prd_baseline_<ts>.dump` |

Assume legacy paths still work until execution proves otherwise.

