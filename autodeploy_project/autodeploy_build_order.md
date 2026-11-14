# PiFitness deployment — build order and verification sequence

**Goal:** Build the deployment implementation one file at a time, keeping every test at the supported entry boundary: run the launcher and `autodeploy.sh`; do not invoke individual helper functions/scripts as a separate test harness.

**Current specification:** Use `bootstrap_objectives.md` as canonical: 10 TEST steps and 9 PRD steps. The older `autodeploy_objectives.md` is superseded where its 9-step flow differs.

## 1. Non-negotiable implementation rules

1. **Build `deploy/scripts/autodeploy.sh` first.** It must already know the complete expected file/step map, even while the downstream helper files do not exist. It must not try to source a missing file and crash with an opaque shell error.
2. **Missing implementation is not success.** The first missing step helper prints `[STEP n/N] <name> NOT IMPLEMENTED`, then the run performs cleanup and prints the final table. It must never continue past a missing step and accidentally look like a complete deploy.
3. **Differentiate missing code from runtime failure.** A missing step helper is `NOT IMPLEMENTED`; a present helper that errors is `FAIL`; a required config/env/tool/DB/service that is missing is a clear startup or pre-flight `FAIL`.
4. **Install the exit handler before doing work.** The first version may have no-op cleanup functions, but the handler must exist from the beginning and grow as each action becomes reversible. It must be idempotent and safe when the process receives INT/TERM or exits early.
5. **All helper tests go through the orchestrator.** Build each helper, then call the launcher (which calls `autodeploy.sh`) using `--through <n>` to verify the new boundary. Do not test a helper by running it directly.
6. **TEST first, PRD second.** TEST is the safe integration sequence. Do not exercise real PRD stop/migration/rollback steps until the PRD branch is complete and a controlled deployment window and known-good backup are available.
7. **Use disposable test conditions for failure cases.** Deliberately broken migrations, incompatible TEST schema, active/inactive timer cases and rollback cases must use a controlled TEST fixture/database. Never manufacture a failure in the live PRD database to test rollback.

## 2. Required first-file behavior: `autodeploy.sh`

Before any downstream file exists, `autodeploy.sh` should be executable and support a bootstrap/incomplete-build run.

### It must contain the complete step registry

| Environment | Step | Name | Expected helper |
|---|---:|---|---|
| TEST | 1 | Pre-flight and decisions | `lib/test-step-01-preflight.sh` |
| TEST | 2 | Clean TEST application checkout | `lib/test-step-02-checkout.sh` |
| TEST | 3 | Reconcile TEST dependencies | `lib/test-step-03-dependencies.sh` |
| TEST | 4 | Database clone / restore | `lib/test-step-04-database-restore.sh` |
| TEST | 5 | Apply pending migrations | `lib/test-step-05-migrations.sh` |
| TEST | 6 | Regression tests | `lib/test-step-06-regression.sh` |
| TEST | 7 | Record and stop PRD services | `lib/test-step-07-stop-prd.sh` |
| TEST | 8 | Start TEST services | `lib/test-step-08-start-test.sh` |
| TEST | 9 | Smoke tests against running TEST | `lib/test-step-09-smoke.sh` |
| TEST | 10 | Manual GO / NO-GO gate | `lib/test-step-10-manual-gate.sh` |
| PRD | 1 | Pre-flight | `lib/prd-step-01-preflight.sh` |
| PRD | 2 | Record and stop PRD services | `lib/prd-step-02-stop-prd.sh` |
| PRD | 3 | Clean PRD application checkout | `lib/prd-step-03-checkout.sh` |
| PRD | 4 | Reconcile PRD dependencies | `lib/prd-step-04-dependencies.sh` |
| PRD | 5 | Take / recover PRD rollback baseline | `lib/prd-step-05-baseline.sh` |
| PRD | 6 | Apply pending migrations | `lib/prd-step-06-migrations.sh` |
| PRD | 7 | Start PRD services | `lib/prd-step-07-start-prd.sh` |
| PRD | 8 | Regression tests against PRD | `lib/prd-step-08-regression.sh` |
| PRD | 9 | Smoke tests against live PRD | `lib/prd-step-09-smoke.sh` |

### Initial `autodeploy.sh` acceptance checks

- Reject missing/unknown arguments, invalid environment, and SHA values not exactly 40 hexadecimal characters.
- Accept `--through <n>` and validate the range for the selected environment (TEST 1–10, PRD 1–9).
- Establish a log path outside app directories. Use the configured log directory once available; while configuration is missing, use a clearly identified bootstrap fallback path so the script can still report the config failure.
- Install the EXIT/INT/TERM/HUP handler before loading helpers or running steps.
- Print startup information and the exact selected environment/SHA.
- Know all expected shared-library and step-helper paths/function names. Check each path before sourcing; no `source missing-file.sh` shell error. Report missing shared libraries explicitly, and do not mark any step PASS when a required shared library is absent.
- If a step helper is missing, print its numbered `NOT IMPLEMENTED` status and stop there. Also show later steps as `NOT IMPLEMENTED`/not reached in the final summary, not PASS.
- Maintain distinct step states (PASS, FAIL, SKIPPED, NOT IMPLEMENTED, NOT RUN) and always print the final table plus cleanup outcome.
- Ensure `--through n` stops after step n and still runs cleanup.
- Do not run destructive commands from the orchestrator itself; step helpers own the operations.

**Boundary test:** run `autodeploy.sh` directly from the exact scripts tree for TEST with a known 40-character commit SHA and `--through 1`. With no helper files present, output must be clean and explicit: startup/config result, missing shared-library paths, Step 1 `NOT IMPLEMENTED` (or blocked by missing shared libraries), cleanup result, final table, and non-zero exit because the run is incomplete. Repeat for PRD and verify 9-step numbering. Then run the launcher independently and confirm it extracts the scripts, syntax-checks `autodeploy.sh`, and invokes it with only the specified environment and commit arguments. Also test invalid args and interrupted early exit.

## 3. Ordered file build plan

The order below is intentional. Complete each row and its orchestrator-boundary test before moving on. If a named repository asset already exists, audit it against the requirement instead of rewriting it.

| Order | File / artifact to build or verify | Why it comes here; implementation target | Required verification through launcher / `autodeploy.sh` |
|---:|---|---|---|
| 1 | `deploy/scripts/autodeploy.sh` | Establish the argument contract, complete TEST/PRD step registry, explicit expected-file registry for every shared library and every step helper, first-installed exit handler, logging, `--through`, missing-file detection, step-status table and final summary. Reference all downstream files now, even though they do not exist yet. | Incomplete-build run; invalid arguments; invalid SHA; `--through` bounds; early INT/TERM/EXIT; prove missing code is NOT IMPLEMENTED and no deployment mutation occurs. |
| 2 | `deploy/scripts/autodeploy.conf` | Add the repository-side configuration contract and required-key validation list. Include all unit/timer names, migration path, URL, ports, required tools, groups, resource list and baseline prefixes described in the objectives. | Run orchestrator and verify config path/hash, key count, missing-key diagnostics, and stop-before-mutation for a deliberately omitted required key. |
| 3 | Pi-side `/home/god/.config/pifitness/test.env` and `prd.env` | Populate the host-specific paths and per-environment DB/app settings. Keep credentials out of git and avoid printing secret values. Confirm distinct TEST/PRD DB targets. | Run via orchestrator and verify env-file presence, safe key-count reporting, path validation and the DB-identity guard. Use dummy credentials/fixtures where necessary. |
| 4 | Audit project inputs and external prerequisites: `pyproject.toml`, `uv.lock`, frontend `package.json`/lockfile, `deploy/migrations/`, Python/frontend/smoke test trees, manual-test script, systemd units/timers, nightly backup job and directories | Establish which assets already exist and which are missing before implementing helpers that depend on them. Do not rewrite existing assets unless they fail the stated contract. Record exact paths for the manual test script and unit files; confirm all configured names/paths are real. | Confirm readiness through the orchestrator's startup/pre-flight diagnostics; do not run helper files directly. Use disposable databases and units for any destructive validation. |
| 5 | `deploy/tests.conf` | Finalize command strings for Python regression, npm regression and smoke suites. PRD reuses the same suites as TEST, so normalize the example's `TEST_*` key naming or explicitly adapt it to the selected environment. Inject `APP_DIR` and `LOG_DIR` at runtime. | Verify command loading and variable injection through TEST/PRD test steps once the test runner is present; malformed/missing command must fail with a specific diagnostic. |
| 6 | Application health/version response file(s) | Ensure the configured health endpoint exposes environment and deployed SHA; exact code path depends on the application. This identity response is required to prove that the public URL serves the intended environment/commit. | Verify through TEST Step 8 and PRD Step 7, not by direct endpoint-only testing. |
| 7 | Verify Pi system configuration and storage paths | Confirm TEST/PRD service and timer definitions exist, app/venv/env/backup/log directories are writable as required, PostgreSQL databases are reachable, and nightly backup files match the configured glob. These are pre-existing prerequisites unless a separate scope adds their installation. | Verify through TEST Step 1 and PRD Step 1; use disposable TEST data for backup/restore tests. |
| 8 | `deploy/scripts/lib/common.sh` | Provide shared evidence/log/command/timing/failure utilities used by all later helpers. | Invoke through a temporary earliest step stub and verify evidence formatting, failing-command capture, output tail and duration in the regular step log. |
| 9 | `deploy/scripts/lib/config.sh` | Centralize sourcing/validation of repo config and the selected Pi env file; validate required keys, paths and environment selection. | Verify TEST and PRD config resolution, missing key/path failure, no secret disclosure, and no mutation when validation fails. |
| 10 | `deploy/scripts/lib/state.sh` | Implement atomic state-file writes/reads and per-run persistent decision/transaction state. Define how `TEST_STATE_FILE`, `PRD_STATE_FILE`, baseline paths and the service snapshot path are represented. | Through orchestrator, write/read test state in a disposable state directory; interrupt a run after state update and confirm next invocation reads the exact stored state. |
| 11 | `deploy/scripts/lib/cleanup.sh` | Implement the orchestrator-called cleanup framework, first as safe no-op, then register actions. Make repeated cleanup safe. | Exercise early exits and simulated registered/unregistered cleanup actions through `autodeploy.sh`; verify no-op actions are reported and cleanup runs once. |
| 12 | `deploy/scripts/lib/checkout.sh` | Implement common staging, SHA verification, atomic swap and rollback primitives; allow TEST symlink and PRD copy wiring. | Through the appropriate step harness, swap a disposable app tree; verify old/new paths, SHA, tree cleanliness, stray-file removal, and safe recovery when staging/swap fails. |
| 13 | `deploy/scripts/lib/dependencies.sh` | Implement `uv sync --locked`, configured dependency groups, npm ci, required binary checks/`ensure_binary`, tool versions and second-sync verification. | Run through TEST Step 3 against a disposable venv/app checkout; verify an injected extra package is removed, second sync reports no changes, and tool failures are reported accurately. |
| 14 | `deploy/scripts/lib/database.sh` | Build reusable DB read/guard/schema/migration/backup/restore primitives. Hard guard: resolve target identity and refuse PRD writes from any TEST operation. | Through TEST/PRD step runners using disposable databases, verify identity check, migrations-table reads, schema diff, backup selection, dump/restore and pinned-path rollback. No live PRD write test. |
| 15 | `deploy/scripts/lib/services.sh` | Build unit/timer discovery, active-state snapshots, timers-first stop, start, wait/timeout, restore-only-previously-active, free-port and HTTP identity/health checks. | Through orchestrator with controlled systemd test units, verify snapshot is persisted before stop, initially inactive units remain inactive, timeout/failure evidence, and restore after interruption. |
| 16 | `deploy/scripts/lib/test-runner.sh` | Build the common regression/smoke command runner, report capture and “run every command even if an earlier one fails” behavior. | Through a step helper/orchestrator call, use harmless commands with known pass/fail outputs; verify each executes, counts/durations/tails/report paths are recorded, and aggregate status fails when any command fails. |
| 17 | `deploy/scripts/lib/test-down.sh` | Implement the recovery used when pre-flight finds TEST units unexpectedly active. Confirm its exact path and whether it is a sourced function or executable; this plan uses a sourced function in `lib/`. | Simulate active and inactive TEST units and invoke only through TEST/PRD pre-flight; prove it stops only TEST units and reports final state. |
| 18 | `deploy/scripts/lib/test-step-01-preflight.sh` | Implement TEST pre-flight, DB compatibility classification, K/R/C prompt, backup age/version evidence, test-down recovery and saved decision. | Run through launcher with `--through 1`; test Unusable, Incompatible and Compatible states, each of K/R/C, absent TTY, no backup, stale dirty state, and unexpectedly active TEST units. Confirm PRD remains running/untouched. |
| 19 | `deploy/scripts/lib/test-step-02-checkout.sh` | Atomically stage/activate exact TEST commit; TEST env symlink; preserve venv/resources. | Run through `--through 2`; plant stray file and verify removal, commit SHA, clean tree, preserved resource report, no service/DB/venv mutation. |
| 20 | `deploy/scripts/lib/test-step-03-dependencies.sh` | Reconcile TEST Python and Node dependencies. | Run through `--through 3`; verify exact versions, extra removal, clean second sync, tool versions and `uv pip check`. |
| 21 | `deploy/scripts/lib/test-step-04-database-restore.sh` | Apply saved K/R/C decision; dirty-before-restore; clone/restore; marker/roles/schema checks; save pinned TEST baseline and mark clean only at end. | Run through `--through 4` with disposable TEST DB; verify Keep skip, nightly restore, fresh clone, target guard, long-operation progress and dirty marker left on injected interrupted restore. |
| 22 | `deploy/scripts/lib/test-step-05-migrations.sh` | Apply pending TEST migrations; mark dirty immediately before dbmate up; immediate pinned-baseline rollback on migration failure. | Run through `--through 5`; test no-pending and pending migrations, then a deliberately broken migration on a throwaway commit; confirm failure evidence, restore from exact pinned dump, and clean rerun. |
| 23 | `deploy/scripts/lib/test-step-06-regression.sh` | Run Python and npm suites against TEST before PRD stop; no running services/port binding; run both suites even after a failure. | Run through `--through 6`; test all-pass and one-suite-fails cases; confirm PRD has remained available and reports land in log dir. |
| 24 | `deploy/scripts/lib/test-step-07-stop-prd.sh` | Record PRD active set then stop timers/services; free required ports. | Run through `--through 7` only in a controlled window; pre-stop one timer manually and verify it is not re-started on cleanup; interrupt after stop and verify restore. |
| 25 | `deploy/scripts/lib/test-step-08-start-test.sh` | Start TEST and verify health URL identifies TEST plus exact SHA. | Run through `--through 8`; verify healthy TEST identity, URL response, PRD inactive state, and failure/timeout cleanup. |
| 26 | `deploy/scripts/lib/test-step-09-smoke.sh` | Run live TEST smoke suite; do not offer manual gate if smoke fails. | Run through `--through 9`; prove all smoke commands execute; force one failure and verify step FAIL and automatic cleanup, with no Step 10 prompt. |
| 27 | `deploy/scripts/lib/test-step-10-manual-gate.sh` | Add GO/NO-GO, 30-minute timeout/reminders, local merge command and final outcome flag. | Run complete TEST sequence in controlled conditions; verify invalid answers reprompt, GO exits 0 and leaves TEST DB migrated, NO-GO/timeout fails and restores PRD/stops TEST/rolls back TEST DB. |
| 28 | `deploy/scripts/lib/prd-step-01-preflight.sh` | PRD read-only pre-flight; auto test-down; no prompt; detect stale PRD dirty marker but defer recovery to Step 5. | Run PRD with `--through 1`; test missing tools/DB/unit/path and stale dirty state; prove no PRD stop or DB write occurs. |
| 29 | `deploy/scripts/lib/prd-step-02-stop-prd.sh` | Reuse service snapshot/stop primitives for PRD. | Run PRD with `--through 2` in controlled window; verify persisted active set, timers-first order and cleanup restores exact prior state. |
| 30 | `deploy/scripts/lib/prd-step-03-checkout.sh` | Atomically deploy exact `main` SHA to `/home/god/PiFitness`; retain previous checkout; copy PRD env file into `backend/.env`. | Run PRD with `--through 3` using a disposable PRD-like app directory; inject a failure and confirm previous checkout restoration. Confirm env copy and venv outside app dir. |
| 31 | `deploy/scripts/lib/prd-step-04-dependencies.sh` | Reconcile PRD venv and npm dependencies; enforce PRD venv outside app directory. | Run PRD with `--through 4` against disposable venv; verify same dependency invariants as TEST plus directory separation. |
| 32 | `deploy/scripts/lib/prd-step-05-baseline.sh` | Determine pending migrations after checkout; capture pinned PRD baseline only when pending and state clean; restore recorded baseline when state is already dirty (crash recovery); skip clean/no-pending case. | Run PRD with `--through 5` using disposable PRD DB and backup directory; test no pending, pending capture, stale dirty recovery, missing/corrupt pinned dump, and interrupted restore. Do not use live PRD for fault injection. |
| 33 | `deploy/scripts/lib/prd-step-06-migrations.sh` | Apply PRD migrations only after baseline has been pinned and state marked dirty; failure is rolled back by exit handler. | Run PRD with `--through 6` on disposable DB; test no-pending/pending and a broken migration; verify rollback from exact state-file dump and that DB/units/code return to prior state. |
| 34 | `deploy/scripts/lib/prd-step-07-start-prd.sh` | Start PRD units and verify URL reports PRD plus exact SHA; TEST remains down. | Run PRD with `--through 7` in controlled window; test healthy identity and failed health cleanup. |
| 35 | `deploy/scripts/lib/prd-step-08-regression.sh` | Run Python/npm regression suites against PRD after PRD services are running; preserve current Decision D22 behavior (success may retain test writes; any failure rolls back). | Run PRD with `--through 8` against a disposable PRD-like database first; verify both suites execute and any failure triggers full rollback. |
| 36 | `deploy/scripts/lib/prd-step-09-smoke.sh` | Run live PRD smoke suite. On success mark state `migrated` and permit prior checkout deletion; failure triggers full rollback. | Run full PRD sequence in a controlled window; verify all-pass success, and injected smoke failure restoring previous checkout, DB baseline and prior unit state. |
## 4. How to run the one-file-at-a-time build loop

For each implementation file:

1. Add the file and only the minimum contract needed for the next step.
2. For incremental development, execute `autodeploy.sh` itself with `--environment <env> --commit <SHA> --through <n>` from the exact extracted/checked-out `deploy/scripts` tree. This is still an orchestrator-boundary test; do not invoke individual helpers. Separately run the actual launcher to verify extraction, syntax checking and the required two-argument hand-off. Keep the launcher contract unchanged: it passes `--environment` and `--commit`, not a hardcoded helper list or deployment logic.
3. Use `--through <last completed step>` on the direct orchestrator run to prevent later incomplete steps from running. For a new step N, verify the sequence through N. Do not mark it complete until its evidence and cleanup path are visible in the ordinary log.
4. Exercise at least one success path and one controlled failure path for the new file. Confirm the final summary shows earlier steps, the new step and cleanup correctly.
5. Do not call a helper file directly as a substitute for the orchestrator boundary test.
6. Keep TEST data isolated and repeatable. For PRD helpers, use a disposable PRD-like DB/app/unit fixture until the full PRD sequence is ready; then use a controlled production window for final smoke/acceptance only.

### Important test interpretation

The objective says every step is verified by running the Pi script and reading its output. That means the helper files may have internal functions, but the acceptance record for each one is the actual `autodeploy.sh` run, its log, evidence lines, final summary, and exit code. Syntax checks on helper files may be performed automatically at orchestrator startup, but they do not replace an execution-through-orchestrator test.

## 5. Acceptance gates before calling the build complete

- [ ] Launcher still extracts only `deploy/scripts`, runs syntax validation on `autodeploy.sh`, and passes only the specified environment/SHA arguments (plus the development `--through` option if explicitly forwarded).
- [ ] TEST complete sequence has 10 numbered steps; PRD complete sequence has 9 numbered steps.
- [ ] Missing downstream file produces `NOT IMPLEMENTED` and a final summary, never a shell sourcing traceback or false PASS.
- [ ] Startup failure happens before mutation when config/env/required pre-flight inputs are missing.
- [ ] Cleanup runs on PASS, FAIL, NO-GO, timeout and signal interruption, and is safe to run more than once.
- [ ] TEST never writes to PRD; all TEST DB write paths verify target identity.
- [ ] PRD rollback restores the exact baseline dump path stored in `PRD_STATE_FILE`, never a latest-by-glob substitute.
- [ ] PRD prior code checkout and previously active unit set are recoverable after any failure after cutover.
- [ ] Regression suites execute before or after service start as specified; smoke tests execute only against the live stack.
- [ ] Every step emits evidence, final table, cleanup result, log path and correct exit code.
- [ ] `deploy/tests.conf`, health/version response, manual-test script and service-state persistence contracts are finalized; see the open items in `deployment_file_index.md`.

## 6. Specific ambiguity resolutions used by this plan

- **Build ordering:** `autodeploy.sh` is first even though it references files that do not yet exist. It uses an explicit expected-helper registry and treats absence as `NOT IMPLEMENTED`.
- **Helper placement:** proposed per-step files live directly in `deploy/scripts/lib/`, following the modular requirement. Common reusable code is in shared `lib/*.sh` files.
- **`test-down.sh`:** proposed at `deploy/scripts/lib/test-down.sh`; the source docs name it but do not locate it.
- **PRD crash recovery:** a stale `PRD_STATE_FILE=dirty` must cause Step 5 to restore the pinned `baseline_dump` before proceeding, even if a new capture would otherwise be skipped. This follows the stated crash-recovery behavior.
- **TEST restore failure:** a failure during TEST Step 4 leaves the state dirty for next-run classification/recovery; do not run a second restore in the same failing step.
- **Tests config:** PRD reuses TEST's regression/smoke suites. The example's `TEST_*` key naming must be normalized or adapted so commands run with the selected environment, rather than maintaining two inconsistent copies.
