# PiFitness `autodeploy.sh` Design Investigation Report

**Repository:** `C:\users\rickb\vsprojects\pifitness`  
**Branch:** `feature-test` (commit `88c88b9` "Bootstrap edit")  
**Mode:** Read-only investigation only. No files modified.

---

## 1. Quick Answer Table

| Question | Short answer | Confidence | Key evidence |
|---|---|---|---|
| Q1.6 Step structure | Legacy `bootstrap.sh` (720 lines) is a single TEST-only orchestrator with preflight → deps → checkout → sync → db-inspect → promote; PRD prints "not yet implemented" and exits 2. Separate `test-up.sh`/`test-down.sh` exist. `deploy.sh`/`pi5_deploy.sh` are legacy PRD deploy scripts. No single orchestrator covers both. | High | `deploy/scripts/bootstrap.sh:1-720`, `:49-51` |
| Q1.7 PRD pre-flight | No PRD pre-flight exists (PRD unimplemented). TEST pre-flight checks: dependency binaries (`uv`, `python3`, `node`, `npm`, `dbmate`), TEST units not active, venv not symlink, commit is 40 hex and ancestor of origin/branch, APP_DIR is a registered worktree. No `df`, `systemctl cat`, `pg_isready`, `psql -c`, `curl` in bootstrap pre-flight. | High | `deploy/scripts/bootstrap.sh:96-176,183-218` |
| Q1.8 Manual gate | Launcher `bootstrap.sh` on the Pi prompts `read -r -p "Select environment: "` (1/PRD, 2/TEST, Q/Quit). `bootstrap.sh` has no human prompt. `deploy.sh` nuclear mode has `read -rp "Continue? [y/N]: "`. `autodeploy.sh` objectives specify a GO/NO-GO prompt with 30-min timeout — not yet implemented. | High | `.audit/.../pi_bootstrap_shell.sh:20-21`, `deploy/scripts/deploy.sh:71`, `.audit/.../autodeploy_objectives.md:242-244` |
| Q1.9 Branch | TEST → `feature-test`; PRD → `main` (unimplemented). Legacy `deploy.sh` targets `react-ui` (frozen failsafe). CI: none found (`.github/` absent). | High | `deploy/scripts/bootstrap.sh:44,49`, `legacy_bootstrap_parameters.md:11-13`, `deploy/scripts/deploy.sh:40` |
| Q1.10 App dir & venv | PRD app dir `/home/god/PiFitness`, venv `/home/god/PiFitness/venv` (INSIDE app dir). TEST app dir `/home/god/PiFitness-test`, persistent venv `/home/god/.local/share/pifitness/test-venv` (OUTSIDE app dir). `UV_PROJECT_ENVIRONMENT` set to `$VENV_DIR` (outside app dir) for `uv sync`. | High | `deploy/scripts/bootstrap.sh:44-47,296`, `legacy_bootstrap_parameters.md:37-42` |

| Q1.11 PRD order | No legacy PRD order exists (PRD = "not yet implemented", exit 2). TEST order: preflight → deps → DB recover/reconcile → DB inspect → checkout promote → (ends; no services/tests/migrate in this increment). `test-up.sh` order: record commit → dump PRD → write state → stop PRD → create TEST DB → fetch/checkout → provision → start units → verify. Tests run AFTER services start (Step 9 verification curls `:8000`, `:3000`, HTTPS). | High | `deploy/scripts/bootstrap.sh:49-51`, `:666-719`, `deploy/scripts/test-up.sh:190-516` |
| Q1.12 Rollback scope | Legacy `bootstrap.sh` trap cleanup removes staging worktree on failure; preserves previous TEST checkout via worktree move, then removes it on success. Does NOT roll back code (no staging/previous dirs restored), does NOT roll back DB (TEST baseline retained), does NOT touch PRD. `test-down.sh` restores PRD units/PM2 from state file, drops TEST DB, removes TEST checkout. No DB backup/restore for PRD rollback. | High | `deploy/scripts/bootstrap.sh:226-262,666-691`, `deploy/scripts/test-down.sh:160-242` |
| Q2.13 Cost | No documented DB size, backup size, or timing in repo. No backup files present in repo. NOT VERIFIABLE FROM REPO. Pi command: `ls -lh $HOME/Documents/DB_Backups | tail`. | High | Searched README, docs, comments, `tools/db/*`; no size/timing found |
| Q2.14 Clean baseline | No `.pifitness-test-state` mechanism in `bootstrap.sh` (legacy state is `$HOME/.pifitness-test-state` used by `test-up.sh`/`test-down.sh`). `bootstrap.sh` uses `$JOURNAL_DIR` recovery state files (`baseline_ready` status). No clean-baseline enforcement mechanism beyond journal. | High | `legacy_bootstrap_parameters.md:50`, `deploy/scripts/bootstrap.sh:234-252,326-351` |
| Q2.15 Dump naming | Two patterns: `personal_fitness_YYYYmmdd_HHMMSS.dump` (nightly PRD, `bootstrap.sh:511`) and `pifitness_prd_YYYYmmdd_HHMMSS.dump` (test-up, `test-up.sh:118`). `pg_dump -F c` produces; `pg_restore` consumes. `PRD_BACKUP_GLOB` defaults to `personal_fitness_[0-9]*.dump`. Max backup age 24h. | High | `deploy/scripts/bootstrap.sh:71-72,505-516`, `deploy/scripts/test-up.sh:117-118`, `legacy_bootstrap_parameters.md:123-125` |

| Q3.A Test classification | See detailed table below. Backend: 257 tests, all TestClient/in-process (jsdom for frontend). Frontend: vitest + jsdom, in-process. No test needs a live server. | High | `tests/test_*.py` (TestClient), `frontend/pifitness/vitest.config.ts` (jsdom) |
| Q3.4 Test tree & npm | Backend test dir: root `tests/` (257 tests collected, pytest config). No `backend/tests/`. Frontend `package.json` scripts: `dev`, `build`, `start`, `test` = `vitest run`, `postinstall`. Root `package.json` does not exist. | High | `tests/` directory listing, `frontend/pifitness/package.json:5-11` |
| Q3.5 Next test runner | Vitest (jsdom environment) for frontend; pytest for backend. Neither needs a live URL (TestClient / jsdom / mocked HTTP). | High | `frontend/pifitness/package.json:34-47`, `frontend/pifitness/vitest.config.ts:21-26`, `backend/pyproject.toml:31-34` |
| Q4.16 PRD backup timing | No legacy PRD deploy script implements backup (PRD unimplemented in `bootstrap.sh`). `test-up.sh` takes PRD dump BEFORE stopping PRD (Step 2, writers still running), preserves as DR artifact; `test-down` never restores PRD from it. `dbmate status` is NOT consulted before backup (dbmate not used in legacy test-up). | High | `deploy/scripts/test-up.sh:194-208,200-204`, `deploy/scripts/test-down.sh:206-207` |
| Q4.17 Backup location | `$HOME/Documents/DB_Backups` (legacy PRD backup dir). Override env vars: `PIFITNESS_PRD_BACKUP_DIR`, `PIFITNESS_PRD_BACKUP_GLOB` (referenced in `bootstrap.sh:71-72` as `${VAR:-default}`). `BACKUP_DIR` also used in `test-up.sh`/`test-down.sh` (defaults `$HOME/Documents/DB_Backups`). | High | `deploy/scripts/bootstrap.sh:71-72`, `deploy/scripts/test-up.sh:14`, `deploy/scripts/test-down.sh:14`, `legacy_bootstrap_parameters.md:49,193-194` |
| Q4.18 Code/units rollback | Legacy `bootstrap.sh` does NOT record pre-existing active PRD set or restore previous code checkout (previous dir removed after success). `test-down.sh` DOES restore PRD units/timers from `UNIT_STATES` state file and restarts PM2 `pifitness-next` if it was online. `deploy.sh` stops all PRD units before wipe and restarts after. | High | `deploy/scripts/bootstrap.sh:666-691`, `deploy/scripts/test-down.sh:164-186`, `deploy/scripts/deploy.sh:87-95,782-789` |
| Q5.1 Config keys | See inventory table below. 20+ keys across `bootstrap.sh`, `test-up.sh`, `test-down.sh`, `deploy.sh`, `pi5_deploy.sh`, `ecosystem.config.js`, `test.env.template`, `.env.test.example` (blocked). Key statuses vary. | Med | Multiple files (see table) |
| Q5.2 `deploy/tests.conf` | NOT FOUND. Does not exist. Referenced in `autodeploy_file_index.md:34` and `legacy_bootstrap_parameters.md:57` as "Test command config". No other test-command config found in repo. | High | Searched `deploy/`, `deploy/scripts/`; file absent |
| Q5.3 Env file wiring | Legacy `test-up.sh`: copies `$HOME/.config/pifitness/test.env` → `$TEST_DIR/.env.test` (`test-up.sh:407`). Legacy `bootstrap.sh`: symlinks `$ENV_FILE` → `$STAGE_DIR/backend/.env` (`bootstrap.sh:279`) and `$VENV_DIR` → `$STAGE_DIR/venv` (`bootstrap.sh:280`). PRD env file `backend/.env` is the PRD source (`bootstrap.sh:61`); `prd.env` at `/home/god/.config/pifitness/prd.env` is implied but missing (conflict). | High | `deploy/scripts/bootstrap.sh:61,279-280`, `deploy/scripts/test-up.sh:407`, `legacy_bootstrap_parameters.md:40` |
| Q19 Final-state guarantees | Stated in `autodeploy_objectives.md:256-260` (target spec, not implemented): Failure → PRD restored, TEST stopped, TEST DB rolled back if dirty; NO-GO/timeout → same; GO → PRD restored, TEST stopped, DB left as-is. `test-up.sh`/`test-down.sh` implement restore on failure via `on_exit`/`recover_and_fail` traps. No README statement of final state. | High | `.audit/.../autodeploy_objectives.md:256-260`, `deploy/scripts/test-up.sh:260-273`, `deploy/scripts/test-down.sh:143-242` |
| Q20 Disk thresholds | NOT FOUND. No `df`, `disk`, `free`, `threshold`, `MB`, `GB` checks in any deploy script. | High | Searched all `deploy/scripts/*.sh` |
| Q21 TEST services during PRD | `test-down.sh` stops TEST units and restores PRD (Step 1 restores PRD units from state; TEST units stopped first). No explicit "verify TEST units are inactive" step in PRD deploy (PRD unimplemented). `bootstrap.sh` pre-flight fails if TEST units are active (`bootstrap.sh:186-194`). | High | `deploy/scripts/bootstrap.sh:186-194`, `deploy/scripts/test-down.sh:146-153` |
| Q22 Config split | `deploy/tests.conf` does NOT exist, so it cannot be read by anything today. `autodeploy.conf` does NOT exist either. Target design: `autodeploy.conf` (in git, read at startup before checkout) holds non-secret values; Pi env file `/home/god/.config/pifitness/<env>.env` (not in git) holds secrets/paths. Legacy reads `backend/.env` (PRD, inside checkout) and `$HOME/.config/pifitness/test.env` (TEST, outside checkout). | High | `deploy/scripts/bootstrap.sh:61,80-81`, `.audit/.../autodeploy_objectives.md:274-282` |

---

## 2. Phase 0 — Orientation

**Git state:**
- Branch: `feature-test`
- 20 local branches, 7 remote tracking branches
- Latest commit: `88c88b9` "Bootstrap edit"

**Files under `deploy/` with line counts:**

| File | Lines |
|---|---|
| `deploy/.env.test.example` | 18 (blocked — secret file) |
| `deploy/README.md` | 119 |
| `deploy/POST_DEPLOY_CHECKLIST.md` | 47 |
| `deploy/list-sql.ps1` | 101 |
| `deploy/migrations/*.sql` | 16 files, 12→629 lines each |
| `deploy/nginx/nginx-react.conf` | 89 |
| `deploy/nginx/nginx-template.conf` | 89 |
| `deploy/scripts/bootstrap.sh` | 720 |
| `deploy/scripts/test-up.sh` | 517 |
| `deploy/scripts/test-down.sh` | 244 |
| `deploy/scripts/deploy.sh` | 821 |
| `deploy/scripts/pi5_deploy.sh` | 328 |
| `deploy/scripts/lib/ensure_binary.sh` | 62 |
| `deploy/scripts/test.env.template` | 16 |
| `deploy/scripts/library_requirements.txt` | 22 |
| `deploy/scripts/npm_requirements.txt` | 33 |
| `deploy/scripts/ecosystem.config.js` | 20 |
| `deploy/scripts/pathfinder.py` | 50 |
| `deploy/scripts/pifitness-hardware-health.py` | 47 |
| `deploy/scripts/CHANGES_SUMMARY.md` | 200 |
| `deploy/scripts/list-sql.ps1` | 87 |
| `deploy/systemd/pifitness-fastapi.service` | 25 |
| `deploy/systemd/pifitness-fastapi-test.service` | 25 |
| `deploy/systemd/pifitness-hardware-health.service` | 6 |
| `deploy/systemd/pifitness-hardware-health.timer` | 9 |
| `deploy/systemd/pifitness_agent.service` | 28 |
| `deploy/systemd/pifitness_agent.timer` | 10 |

**File existence check (Phase 0 #4):**

| File | Exists? | Path |
|---|---|---|
| `deploy/scripts/bootstrap.sh` | Yes | `deploy/scripts/bootstrap.sh` (720 lines) |
| `deploy/scripts/autodeploy.sh` | No | Does not exist (target of new design) |
| `deploy/scripts/autodeploy.conf` | No | Does not exist (target config) |
| `deploy/scripts/lib/` | Yes | Contains `ensure_binary.sh` only |
| `deploy/scripts/lib/ensure_binary.sh` | Yes | 62 lines |
| `deploy/tests.conf` | No | Does not exist |
| `deploy/systemd/` | Yes | 6 unit/timer files |
| `deploy/nginx/` | Yes | `nginx-react.conf`, `nginx-template.conf` |
| Any `deploy.sh` at root or elsewhere | Yes | `deploy/scripts/deploy.sh` (legacy, 821 lines) |
| `test-up.sh` / `test-down.sh` | Yes | `deploy/scripts/test-up.sh`, `deploy/scripts/test-down.sh` |
| CI workflow under `.github/` | No | `.github/` directory does not exist |

**Repo-wide string search hits (Phase 0 #5):**

- `autodeploy`: 58 hits, concentrated in `.audit/autodeploy_project/` design docs (build order, objectives, file index, legacy params, research prompt). Zero hits in actual `deploy/scripts/`.
- `PIFITNESS_`: ~100 hits, mostly in `.audit/autodeploy_project/legacy_bootstrap_parameters.md` (parameter index table) plus `PIFITNESS_FAILPOINT_*` in tests and `cline_bootstrap_handoff.md`.
- `GO or NO-GO`: 5 hits — `autodeploy_objectives.md:19,243`, `legacy_bootstrap_parameters.md:185`, `research_prompt.md`, `cline_bootstrap_handoff.md:82`.
- `failpoint`: ~100 hits — `legacy_bootstrap_parameters.md:180-182` (legacy: `during_restore`, `pre_start`), `test-up.sh:47-60,451`, `test-down.sh:45-58,158`, tests in `tests/test_test_deploy_scripts.py`.
- `react-ui`: ~100 hits — `deploy.sh:40` (TARGET=react-ui), `pi5_deploy.sh:252`, `legacy_bootstrap_parameters.md:13` (conflict noted), `evergreen_plan.md` (frozen failsafe).
- `feature-test`: 18 hits — `bootstrap.sh:44`, `legacy_bootstrap_parameters.md:11`, `pi_bootstrap_shell.sh:32`, `pi_bootstrap_output.md`, `autodeploy_objectives.md:12-14,20,245`.
- `pifitness_test`: 79 hits — DB name in legacy params, test scripts, test files.
- `UV_PROJECT_ENVIRONMENT`: 7 hits — `autodeploy_file_index.md:19`, `autodeploy_objectives.md:122`, `legacy_bootstrap_parameters.md:150,192`, `bootstrap.sh:296`.

---

## Q1.6 Step structure

**Existing deploy/test scripts and their structure:**

1. **`deploy/scripts/bootstrap.sh`** (720 lines, `set -Eeuo pipefail`, no top-of-file comment block, has `trap cleanup EXIT` at line 263):
   - Argument parsing: `--environment prd\|test`, `--commit SHA` (40 hex validated), `--help`
   - Environment case: `test` → sets BRANCH/APP_DIR/ENV_FILE/VENV_DIR; `prd` → prints "PRD deployment is not yet implemented." exit 2
   - Dependency preflight: audits uv, python3, node, npm, dbmate; selectively installs missing; writes JSON audit report
   - TEST unit check: fails if any `*-test` systemd unit is active
   - Venv validation: not a symlink, must be directory
   - Commit validation: `git cat-file -e`, `merge-base --is-ancestor origin/$BRANCH`
   - Worktree checks: APP_DIR must be registered worktree; STAGING_DIR must not exist
   - `cleanup()` trap: removes staging worktree on failure; handles DB recovery state
   - Git worktree add staging, link env + venv into stage, uv sync --locked, MigraDiff version check, dbmate ensure
   - DB recovery reconciliation (`restore_test_db.py`), DB inspection (`inspect_test_db.py`), decision parsing
   - Interactive DB choice prompt (`read -r -p "Enter choice [1-3]"`): keep / refresh from nightly / fresh dump
   - DB recovery: fresh `pg_dump` or nightly restore
   - Worktree promotion: move previous → `$APP_DIR.previous.$$`, move stage → APP_DIR, remove previous
   - Ends after checkout; no services, no migrations, no tests in this increment

2. **`deploy/scripts/test-up.sh`** (517 lines, `set -euo pipefail`, BOM at line 1):
   - Failpoint function (`failpoint()`) with `PIFITNESS_FAILPOINT_*` / `PIFITNESS_FAILPOINT_ACK`
   - Steps array (dry-run prints plan): record PRD commit → dump PRD DB → write state → stop PRD → create TEST DB → fetch/checkout → provision → start units → write state
   - Step 1: record PRD commit; Step 2: `pg_dump -Fc` PRD → `$BACKUP_DIR/pifitness_prd_<ts>.dump`; Step 3: stop PRD units/timers + PM2, verify quiesced; Step 4: create TEST DB (template clone or pg_dump/pg_restore fallback), stamp `pifitness.env='test'`; Step 5: fetch ref from origin; Step 6: `git clone` PRD→TEST_DIR, checkout SHA, `cp test.env → .env.test`; Step 7: provision venv (`python3 -m venv` INSIDE checkout), pip install, npm ci, npm build; Step 8: failpoint `pre_start`, start test units (systemd `*-test`) + PM2 `pifitness-next-test`; Step 9: verify (unit active, PM2 online, curl `:8000/api/health`, curl `:3000`, curl HTTPS, psql marker check)

3. **`deploy/scripts/test-down.sh`** (244 lines, `set -euo pipefail`):
   - `--keep-db`, `--dry-run` flags
   - Loads state file (`~/.pifitness-test-state`)
   - Step 1: stop TEST units/timers, `pm2 delete pifitness-next-test`; Step 2: failpoint `during_restore`, restore PRD units from `UNIT_STATES`, restart PM2 `pifitness-next` if was online; Step 3: health check (curl HEALTH_URL + HTTPS, requires curl); Step 4: drop TEST DB (marker check: must be `test`); Step 5: `rm -rf TEST_DIR`; Step 6: `rm -f STATE_FILE`

4. **`deploy/scripts/deploy.sh`** (821 lines, legacy PRD deploy, `set -e`):
   - Target `react-ui`; modes: nuclear/fast/full
   - Nuclear: `read -rp "Continue? [y/N]:"`, backups `.env` to `.env.pre-nuclear`, stops services, `rm -rf PROJECT_DIR`, re-clone
   - Installs Python requirements (`library_requirements.txt`), npm, dbmate
   - Creates systemd units from `deploy/systemd/` (non-test units), daemon-reload
   - Starts PM2 `pifitness-next` on port 3000, `pm2 save`
   - Updates nginx from `nginx-react.conf`, reloads nginx
   - Cleans old backups (keeps last 2)

5. **`deploy/scripts/pi5_deploy.sh`** (328 lines, `set -e`):
   - Branch-based (react-ui / streamlit-prd)
   - Stops/starts PRD units from systemd dir, PM2 Next.js, nginx template swap, port detection (8000/8501)

**Top-of-file comments:**
- `bootstrap.sh`: No comment block; starts with shebang + `set -Eeuo pipefail`
- `test-up.sh`: BOM, then shebang; failpoint comments explain opt-in drill hooks
- `test-down.sh`: No comment block; starts with shebang
- `deploy.sh`: Comment block "PiFitness React Deployment Script / Called by bootstrap.sh -- target: react-ui"
- `pi5_deploy.sh`: Comment block "PiFitness Deployment Script"

**`set -euo pipefail` / trap / exit-handler code:**
- `bootstrap.sh`: `set -Eeuo pipefail` (line 2), `trap cleanup EXIT` (line 263)
- `test-up.sh`: `set -euo pipefail` (line 3), `trap 'write_state' INT TERM` (line 272, duplicated at 277), `trap on_exit EXIT` (line 273)
- `test-down.sh`: `set -euo pipefail` (line 2), no trap
- `deploy.sh`: `set -e` (line 14), no trap
- `pi5_deploy.sh`: `set -e` (line 5), no trap

**Is there any existing single orchestrator?** No. `bootstrap.sh` is TEST-only (PRD = exit 2). `deploy.sh`/`pi5_deploy.sh` are legacy PRD deploy scripts with different structure. No single orchestrator covers both environments.

---

## Q1.7 PRD Step 1 pre-flight

No PRD pre-flight exists — `bootstrap.sh` line 49-51: `echo "PRD deployment is not yet implemented."; exit 2`.

**TEST pre-flight checks in `bootstrap.sh`:**
- Dependency binaries: `-x` path checks for uv, python3, node, npm, dbmate (`bootstrap.sh:96-176`)
- TEST units not active: `systemctl is-active --quiet` for each `*-test` unit (`bootstrap.sh:186-194`)
- Venv not a symlink, must be directory (`bootstrap.sh:196-202`)
- Commit 40-hex and ancestor of `origin/$BRANCH` (`bootstrap.sh:76-78,206-211`)
- APP_DIR registered worktree (`bootstrap.sh:213-218`)
- Staging path absent (`bootstrap.sh:220-224`)

**No hits for:** `df `, `systemctl cat`, `pg_isready`, `psql -c`, `curl` in bootstrap pre-flight. `curl` appears only in `test-up.sh` step 9 verification and `test-down.sh` step 3 health check.

---

## Q1.8 Manual gate

- **Launcher** (Pi-resident, `.audit/autodeploy_project/pi_bootstrap_shell.sh:20-21`): `read -r -p "Select environment: "` — 1) PRD, 2) TEST, Q) Quit
- **`bootstrap.sh`**: No human prompt at all
- **`deploy.sh:71`**: `read -rp "Continue? [y/N]:"` in nuclear mode
- **`bootstrap.sh:466`**: `read -r -p "Enter choice [1-3]:"` for TEST DB refresh choice (interactive, requires TTY)
- **Target design** (`autodeploy_objectives.md:242-244`): `Enter GO or NO-GO`, case-insensitive, reprompt otherwise, 30-minute timeout = failure — NOT IMPLEMENTED
- `timeout`: Not used as shell `timeout` command anywhere; "30 minutes" is a design requirement only

---

## Q1.9 Branch

- `bootstrap.sh:44`: TEST → `BRANCH="feature-test"`; `bootstrap.sh:49`: PRD → unimplemented (target `main` per design docs)
- `legacy_bootstrap_parameters.md:11-13`: TEST branch `feature-test`, PRD branch `main`, legacy deploy target `react-ui` (conflict)
- `deploy.sh:40`: `TARGET="react-ui"` (legacy)
- `pi_bootstrap_shell.sh:32`: `BRANCH="feature-test"` for TEST
- `.github/`: does not exist — no CI
- **Each script's branch:** `bootstrap.sh` TEST=`feature-test`, PRD=unimplemented; `deploy.sh`=`react-ui`; `pi5_deploy.sh`=`react-ui` or `streamlit-prd`; launcher=`feature-test` for TEST

---

## Q1.10 PRD app dir & venv

| Path | Inside/Outside app dir | Used by |
|---|---|---|
| `/home/god/PiFitness` | N/A (is the app dir) | PRD source repo (`bootstrap.sh:60`) |
| `/home/god/PiFitness/venv` | INSIDE app dir | Legacy PRD venv (`legacy_bootstrap_parameters.md:42`, `deploy.sh:53`) |
| `/home/god/PiFitness-test` | N/A (is TEST app dir) | TEST app dir (`bootstrap.sh:45`) |
| `/home/god/.local/share/pifitness/test-venv` | OUTSIDE app dir | Persistent TEST venv (`bootstrap.sh:47`, `legacy_bootstrap_parameters.md:41`) |
| `$TEST_DIR/venv` | INSIDE TEST checkout | Legacy TEST venv in `test-up.sh` (`legacy_bootstrap_parameters.md:43`, `test-up.sh:417`) |
| `$VENV_DIR` (=`/home/god/.local/share/pifitness/test-venv`) | OUTSIDE app dir | `UV_PROJECT_ENVIRONMENT` for `uv sync` (`bootstrap.sh:296`) |
| `/home/god/.config/pifitness/test.env` | OUTSIDE app dir | TEST env file (`bootstrap.sh:46`) |
| `/home/god/PiFitness/backend/.env` | INSIDE app dir | PRD env file (`bootstrap.sh:61`) |

`--python`: `bootstrap.sh:302` passes `--python "$PYTHON_BIN"` (`/usr/bin/python3`) to uv sync.

---

## Q1.11 PRD order

**No legacy PRD order exists** — `bootstrap.sh:49-51` exits 2 with "PRD deployment is not yet implemented."

**TEST order in `bootstrap.sh` (this increment ends at checkout):**
1. Dependency preflight & install
2. TEST unit quiesce check
3. Venv validation
4. Commit & worktree validation
5. Staging worktree add + env/venv symlinks
6. uv sync --locked + MigraDiff check
7. dbmate ensure
8. DB recovery reconciliation
9. DB inspection (read-only)
10. Interactive DB choice (keep / nightly refresh / fresh dump)
11. DB recovery (fresh pg_dump or nightly restore)
12. Worktree promotion (previous → stage → APP_DIR)
13. Cleanup (remove previous worktree)
**Ends here** — no migrations, no service start, no tests (`bootstrap.sh:719: "No migrations applied in this integration step."`; `:719: "No services changed."`)

**`test-up.sh` order (full TEST stack):**
1. Record PRD commit → 2. Dump PRD DB → 3. Write state → 4. Stop PRD → 5. Create TEST DB → 6. Fetch/checkout → 7. Provision → 8. Start test units → 9. Verify stack

**Tests run AFTER services start:** `test-up.sh:453-512` starts test units (Step 8) then verifies with curl to `:8000`, `:3000`, HTTPS (Step 9). `bootstrap.sh` does not run tests at all in this increment.

---

## Q1.12 PRD rollback scope

**No PRD rollback exists** (PRD unimplemented).

**TEST rollback in `bootstrap.sh`:**
- `cleanup()` trap removes staging worktree on failure (`bootstrap.sh:256-260`)
- Previous TEST checkout preserved via worktree move, then removed on success (`bootstrap.sh:666-691`)
- Does NOT roll back code (no staging/previous dirs restored after success — previous is deleted)
- Does NOT roll back DB (TEST baseline retained via `baseline_ready` journal)
- Does NOT touch PRD

**`test-down.sh` rollback (restores PRD after TEST):**
- Restores PRD units/timers from `UNIT_STATES` state file (`test-down.sh:164-177`)
- Restarts PM2 `pifitness-next` if it was online before (`test-down.sh:178-186`)
- Drops TEST DB (`test-down.sh:212-223`)
- Removes TEST checkout (`test-down.sh:239`)
- Removes state file (`test-down.sh:242`)
- Does NOT restore PRD from the dump (dump is DR artifact only, `test-down.sh:206-207`)

**What legacy does NOT roll back:** PRD code checkout, PRD database (never modified), previous TEST code (deleted after successful promotion).

---

## Q2.13 Cost

No documented DB size, backup size, or timing anywhere in the repo. Searched README, docs, comments, `tools/db/*`. No backup files present in the repo.

**NOT VERIFIABLE FROM REPO.** Pi command to check: `ls -lh $HOME/Documents/DB_Backups | tail`

Legacy references to backup dir: `/home/god/Documents/DB_Backups` (`legacy_bootstrap_parameters.md:49`), glob `personal_fitness_[0-9]*.dump` (`bootstrap.sh:72`), max age 24 hours (`legacy_bootstrap_parameters.md:125`, `bootstrap.sh:520`).

---

## Q2.14 Clean baseline

No `.pifitness-test-state` mechanism in `bootstrap.sh` (that legacy state file is `$HOME/.pifitness-test-state`, used by `test-up.sh`/`test-down.sh` only — `legacy_bootstrap_parameters.md:50`).

`bootstrap.sh` uses `$JOURNAL_DIR` (`/home/god/Documents/PiFitness_Test_Evidence/journals`) recovery state files with `baseline_ready` status (`bootstrap.sh:234-252,349-350`). On failure after baseline_ready, the baseline is retained (`bootstrap.sh:246-248`).

**No clean-baseline enforcement mechanism exists today** beyond the journal state files. No "clean" / "dirty" / "migrated" state machine in `bootstrap.sh` (that is in the target `state.sh` design per `autodeploy_build_order.md:28`).

---

## Q2.15 Dump naming

| Pattern | Produced by | Consumed by | File:line |
|---|---|---|---|
| `personal_fitness_YYYYmmdd_HHMMSS.dump` | `pg_dump -F c` (fresh PRD dump) | `restore_test_db.py` recovery | `bootstrap.sh:511`, `legacy_bootstrap_parameters.md:176` |
| `pifitness_prd_YYYYmmdd_HHMMSS.dump` | `pg_dump -Fc` (test-up) | test-down (DR artifact, never restores PRD) | `test-up.sh:118`, `legacy_bootstrap_parameters.md:124,177` |
| `personal_fitness_[0-9]*.dump` (glob) | Nightly job (separate, not in repo) | `bootstrap.sh` nightly restore | `bootstrap.sh:72`, `legacy_bootstrap_parameters.md:122` |

`pg_dump` / `pg_restore` used in: `bootstrap.sh:512` (pg_dump fresh), `test-up.sh:196,372` (pg_dump/pg_restore), `test-down.sh` (dropdb, not pg_restore).

---

## Q3.A Static inspection — all hits with path:line

**Backend tests** (root `tests/` directory, 257 tests collected):

| Pattern | Hits (representative) |
|---|---|
| `localhost` | Not found in backend tests (uses TestClient, in-process) |
| `127.0.0.1` | Not found in backend tests |
| `:8000` | Not found in backend tests |
| `:3000` | Not found in backend tests |
| `BASE_URL` | Not found in backend tests |
| `API_URL` | Not found in backend tests |
| `baseURL` | Not found in backend tests |
| `TestClient` | `tests/test_activities.py:8,11`, `test_admin.py:4,7`, `test_api_endpoint_contract.py:8,86`, `test_config.py:6,16`, `test_food_api_contract.py:9,109,127`, `test_health.py:9,18`, `test_isrc_dupe_endpoints.py:11,14,219`, `test_music_actions.py:14,19`, `test_route_smoke.py:6,10`, plus many more (64 total hits) |
| `httpx` | `backend/pyproject.toml:19` (dependency only, not in tests) |
| `requests.` | `backend/pyproject.toml:18` (dependency only) |
| `playwright` | `tests/test_garmin_login_flow.py` (mocked, `patch`), `backend/pyproject.toml:27` (dependency) |
| `selenium` | Not found |
| `cypress` | Not found |
| `page.goto` | Not found in backend tests (only in `browser_login.py:119` and `tools/probe/probe5.py:8`) |
| `uvicorn` | `backend/main.py` not a test; `deploy/systemd/pifitness-fastapi.service:12` |
| `subprocess` | Not in backend tests (in `backend_tasks.py`, `tools/db/*`) |
| `npm run dev` / `next start` | Not in backend tests |
| `TestClient(` / `AsyncClient` | `TestClient(app)` in many test files; no `AsyncClient` |

**Frontend tests** (`frontend/pifitness/src/**/__tests__/` + `src/test/setup.ts`):

| Pattern | Hits |
|---|---|
| `localhost`, `127.0.0.1`, `:8000`, `:3000` | Not found in frontend test files |
| `BASE_URL`, `API_URL`, `baseURL` | Not found in frontend test files |
| `TestClient`, `httpx`, `requests.` | N/A (Python) |
| `playwright`, `selenium`, `cypress` | Not found |
| `page.goto` | Not found |
| `npm run dev`, `next start` | Not found |
| `jsdom` | `vitest.config.ts:22` (`environment: 'jsdom'`), `package.json:43` (devDependency) |

**Classification table:**

| File | Evidence line | Class | Rationale |
|---|---|---|---|
| `tests/test_activities.py` | `:8,11` `TestClient(app)` | (a) in-process | FastAPI TestClient, no server |
| `tests/test_admin.py` | `:4,7` `TestClient(app)` | (a) in-process | TestClient |
| `tests/test_api_endpoint_contract.py` | `:8,86` TestClient | (a) in-process | Route table check via TestClient |
| `tests/test_config.py` | `:6,16` `TestClient(app)` | (a) in-process | TestClient; imports `.env` but in-process |
| `tests/test_food_api_contract.py` | `:9,109,127` `TestClient(_make_app())` | (a) in-process | Patched reads, TestClient |
| `tests/test_health.py` | `:9,18` `TestClient(app)` | (a) in-process | TestClient |
| `tests/test_isrc_dupe_endpoints.py` | `:11,14` `TestClient(app)` | (a) in-process | TestClient |
| `tests/test_music_actions.py` | `:14,19` `TestClient(app)` | (a) in-process | TestClient + mocked functions |
| `tests/test_route_smoke.py` | `:6,10` `TestClient(app)` | (a) in-process | TestClient |
| `tests/test_garmin_login_flow.py` | `:131-160` `patch("...login_via_browser")` | (a) in-process | Playwright mocked; no real browser |
| `tests/test_schema_compare.py` | (migadiff JSON comparison) | (a) in-process | File-based comparison, no server |
| `frontend/pifitness/src/lib/__tests__/*.test.ts` | `vitest.config.ts:22` jsdom | (a) in-process | Vitest + jsdom, no server |
| `frontend/pifitness/src/lib/barcode/__tests__/*.test.ts` | `vitest.config.ts:22` jsdom | (a) in-process | Vitest + jsdom |

**No test file is class (b) needs a live server.** No test file is class (c) unclear. All 257 backend tests + frontend vitest tests are in-process.

---

## Q3.4 Test tree & npm script

**Backend test tree:** Root `tests/` directory (NOT `backend/tests/`). 257 tests collected (`pytest --collect-only` exit=0, 6.10s). Config lives in `backend/pyproject.toml` (pytest as dev dependency; `[pytest]` config block present). The `pythonpath = backend backend/pifitness` confirms tests import from backend. No `backend/tests/` directory exists.

**package.json scripts:**
- Root `package.json`: **NOT FOUND** (does not exist)
- `frontend/pifitness/package.json`:
  ```json
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "test": "vitest run",
    "postinstall": "node scripts/copy-zxing-wasm.mjs"
  }
  ```
- **Test script:** `"test": "vitest run"` in `frontend/pifitness/package.json:9`

**Backend test runner:** pytest (dev dependency in `backend/pyproject.toml:31-34`: `pytest==9.1.1`, `pytest-asyncio==1.4.0` under `[project.optional-dependencies] dev`).

---

## Q3.5 Next test runner

| Runner | Config file | Needs live URL? |
|---|---|---|
| Vitest | `frontend/pifitness/vitest.config.ts` (jsdom env) | No |
| pytest | `backend/pyproject.toml` (dev deps), root `tests/` | No (TestClient/in-process) |
| Jest | Not found | N/A |
| Playwright (test runner) | `playwright.config.*`: not found | N/A (playwright is a Python dependency for Garmin browser login, mocked in tests) |
| Cypress | Not found | N/A |
| Karma | Not found | N/A |
| Mocha | Not found | N/A |

---

## Q4.16 PRD backup timing

No legacy PRD deploy script implements backup (PRD = exit 2 in `bootstrap.sh`).

**`test-up.sh` (TEST, closest analog):** Dump taken at Step 2 (`test-up.sh:194-208`) **before** PRD is stopped (Step 3, `test-up.sh:308-313`). Writers still running during dump. Comment at `test-up.sh:200-204`: "Dump-before-stop order (D3, not amended): the dump is taken while PRD writers are still running... Preserve the dump as a disaster-recovery artifact; test-down never restores PRD from it."

**`dbmate status` is NOT consulted** before deciding to back up. `dbmate` is only ensured as a binary (`test-up.sh:180-182`, `bootstrap.sh:315-317`); no dbmate status call in legacy scripts.

---

## Q4.17 Backup location

- **Backup directory:** `$HOME/Documents/DB_Backups` (legacy PRD backup dir)
- **Override env vars:** `PIFITNESS_PRD_BACKUP_DIR` (default `$HOME/Documents/DB_Backups`), `PIFITNESS_PRD_BACKUP_GLOB` (default `personal_fitness_[0-9]*.dump`) — referenced in `bootstrap.sh:71-72` as `${VAR:-default}`
- `BACKUP_DIR` also used in `test-up.sh:14` and `test-down.sh:14` (defaults `$HOME/Documents/DB_Backups`)
- `legacy_bootstrap_parameters.md:49`: `/home/god/Documents/DB_Backups` | Legacy PRD backup dir | L
- `legacy_bootstrap_parameters.md:193-194`: `PIFITNESS_PRD_BACKUP_DIR` / `PIFITNESS_PRD_BACKUP_GLOB` | Override PRD backup dir/glob

---

## Q4.18 Code/units rollback

**`bootstrap.sh` (TEST, this increment):**
- Does NOT record pre-existing active PRD set (PRD is not touched in TEST mode)
- Does NOT restore previous code checkout — previous worktree is removed after successful promotion (`bootstrap.sh:686-688`)
- Rollback on failure: removes staging worktree only (`bootstrap.sh:256-260`)

**`test-down.sh` (restores PRD after TEST):**
- Records `UNIT_STATES` (unit=state pairs) in state file during test-up (`test-up.sh:293,238`)
- Restores each PRD unit to its pre-test state (`test-down.sh:164-177`)
- Restarts PM2 `pifitness-next` if it was online before (`test-down.sh:178-186`)
- Does NOT restore previous code checkout (PRD dir untouched throughout)

**`deploy.sh`:** Stops all PRD units before wipe (`deploy.sh:87-94`), restarts after (`deploy.sh:782-789`). No record/restore of pre-existing active set — just stops all and starts all non-test units.

---

## Q5.1 Config key inventory

| Key | File:line | Example value | Status |
|---|---|---|---|
| `ENVIRONMENT` | `bootstrap.sh:15,22` | `prd\|test` (CLI) | PRESENT |
| `COMMIT` | `bootstrap.sh:16,27` | 40-hex SHA | PRESENT |
| `BRANCH` | `bootstrap.sh:44` | `feature-test` | PRESENT |
| `APP_DIR` | `bootstrap.sh:45` | `/home/god/PiFitness-test` | PRESENT |
| `ENV_FILE` | `bootstrap.sh:46` | `/home/god/.config/pifitness/test.env` | PRESENT |
| `VENV_DIR` | `bootstrap.sh:47` | `/home/god/.local/share/pifitness/test-venv` | PRESENT |
| `SOURCE_REPO` | `bootstrap.sh:60` | `/home/god/PiFitness` | PRESENT |
| `PRD_ENV_FILE` | `bootstrap.sh:61` | `/home/god/PiFitness/backend/.env` | PRESENT |
| `UV_BIN` | `bootstrap.sh:62` | `/home/god/.local/bin/uv` | PRESENT |
| `PYTHON_BIN` | `bootstrap.sh:63` | `/usr/bin/python3` | PRESENT |
| `NODE_BIN`/`NPM_BIN` | `bootstrap.sh:64-65` | `/usr/bin/node`, `/usr/bin/npm` | PRESENT |
| `DBMATE_BIN` | `bootstrap.sh:66` | `/usr/local/bin/dbmate` | PRESENT |
| `EVIDENCE_DIR` | `bootstrap.sh:67` | `/home/god/Documents/PiFitness_Test_Evidence` | PRESENT |
| `JOURNAL_DIR` | `bootstrap.sh:68` | `$EVIDENCE_DIR/journals` | PRESENT |
| `PRD_BACKUP_DIR` | `bootstrap.sh:71` | `${PIFITNESS_PRD_BACKUP_DIR:-$HOME/Documents/DB_Backups}` | PRESENT (with override) |
| `PRD_BACKUP_GLOB` | `bootstrap.sh:72` | `${PIFITNESS_PRD_BACKUP_GLOB:-personal_fitness_[0-9]*.dump}` | PRESENT (with override) |
| `DB_RECOVERY_STATE_FILE` | `bootstrap.sh:73` | `""` | NAMED-ONLY (empty init) |
| `DB_RECOVERY_SCRIPT` | `bootstrap.sh:74` | `""` | NAMED-ONLY (empty init) |
| `AUDIT_REPORT` | `bootstrap.sh:87` | `$EVIDENCE_DIR/dependency-audit-<ts>.json` | PRESENT (generated) |
| `PRD_DIR`/`TEST_DIR` | `test-up.sh:5-6` | `$HOME/PiFitness`, `$HOME/PiFitness-test` | PRESENT (defaults) |
| `STATE_FILE` | `test-up.sh:7` | `$HOME/.pifitness-test-state` | PRESENT (default) |
| `PRD_DB`/`TEST_DB` | `test-up.sh:8-9` | `pifitness`, `pifitness_test` | PRESENT (defaults) |
| `DB_USER`/`DB_HOST`/`DB_PORT` | `test-up.sh:10-12` | `dev`, `localhost`, `15432` | PRESENT (defaults) |
| `HTTPS_HOSTNAME` | `test-up.sh:13` | `https://pifitness.duckdns.org` | PRESENT (default) |
| `BACKUP_DIR` | `test-up.sh:14` | `$HOME/Documents/DB_Backups` | PRESENT (default) |
| `APP_ENV` | `test-up.sh:159` | must be `test` | PRESENT (validated) |
| `SPOTIFY_ENABLED`/`GARMIN_ENABLED`/`PIRATE_ENABLED` | `test-up.sh:171` | must be `false` | PRESENT (validated) |
| `PIFITNESS_FAILPOINT_*` | `test-up.sh:50` | `PIFITNESS_FAILPOINT_PRE_START=1` | NAMED-ONLY (env var pattern) |
| `PIFITNESS_FAILPOINT_ACK` | `test-up.sh:54` | `test-only` | NAMED-ONLY |
| `HEALTH_URL` | `test-down.sh:12` | `http://127.0.0.1/api/health` | PRESENT (default) |
| `PM2_NEXTPROD_ACTIVE` | `test-down.sh` state | `online\|unknown` | PRESENT (state) |
| `UNIT_STATES` | `test-down.sh` state | `unit=active ...` | PRESENT (state) |
| `TARGET` | `deploy.sh:40` | `react-ui` | PRESENT |
| `PROJECT_DIR` | `deploy.sh:52` | `/home/god/PiFitness` | PRESENT |
| `VENV_DIR` (deploy.sh) | `deploy.sh:53` | `$PROJECT_DIR/venv` | PRESENT |
| `REQUIREMENTS_FILE` | `deploy.sh:55` | `library_requirements.txt` | PRESENT |
| `NPM_REQUIREMENTS_FILE` | `deploy.sh:56` | `npm_requirements.txt` | PRESENT |
| `NEXT_PUBLIC_APP_ENV` | `deploy.sh:763` | `production` | PRESENT |
| `NEXT_PUBLIC_API_URL` | `deploy.sh:762` | unset | NAMED-ONLY (unset) |
| `APP_ENV` (test.env.template) | `test.env.template:2` | `test` | PRESENT (template) |
| `PG_HOST`/`PG_PORT`/`PG_DB`/`PG_USER`/`PG_PASSWORD` | `test.env.template:4-8` | `localhost`, `15432`, `pifitness_test`, `dev`, (empty) | PRESENT (template) |
| `PGSSLMODE` | `test.env.template:9` | `disable` | PRESENT (template) |
| `BOX` | `test.env.template:11` | `local` | PRESENT (template) |
| `UV_PROJECT_ENVIRONMENT` | `legacy_bootstrap_parameters.md:192` | (no value in repo) | NAMED-ONLY (design doc) |
| `PIFITNESS_PRD_BACKUP_DIR` | `legacy_bootstrap_parameters.md:193` | (no value) | NAMED-ONLY (design doc) |
| `PIFITNESS_PRD_BACKUP_GLOB` | `legacy_bootstrap_parameters.md:194` | (no value) | NAMED-ONLY (design doc) |
| `TEST_SERVICES`/`PRD_SERVICES` | `legacy_bootstrap_parameters.md:203` | (no value) | NAMED-ONLY (design doc, target autodeploy.conf keys) |
| `NEXT_PUBLIC_API_URL` | `legacy_bootstrap_parameters.md:199` | Unset in legacy | NAMED-ONLY |

**Keys marked MISSING (needed by design docs but nowhere):**
- Exact `autodeploy.conf` key names for service lists, migrations dir, health URL, persistent resources, dependency groups, required tools — `autodeploy_file_index.md:59` confirms "exact key names not yet specified"
- `prd.env` at `/home/god/.config/pifitness/prd.env` — implied by design but not in repo (`legacy_bootstrap_parameters.md:40` notes "implied by T; legacy uses backend/.env" — T/L conflict)
- `deploy/tests.conf` — referenced but does not exist

---

## Q5.2 `deploy/tests.conf`

**NOT FOUND.** `deploy/tests.conf` does not exist. Referenced as "Test command config" in `autodeploy_file_index.md:34` and `legacy_bootstrap_parameters.md:57`. No other test-command config found in `deploy/` or `deploy/scripts/`.

---

## Q5.3 Env file wiring

**Legacy `test-up.sh`:** Copies (not symlinks) `$HOME/.config/pifitness/test.env` → `$TEST_DIR/.env.test` at `test-up.sh:407-408`. Source: outside checkout; destination: inside test checkout.

**Legacy `bootstrap.sh`:** Symlinks (not copies) `$ENV_FILE` → `$STAGE_DIR/backend/.env` (`bootstrap.sh:279: ln -sT -- "$ENV_FILE" "$STAGE_ENV_LINK"`) and `$VENV_DIR` → `$STAGE_DIR/venv` (`bootstrap.sh:280`). Source: outside checkout; destination: inside staging checkout.

**PRD env file:** `bootstrap.sh:61` reads `$PRD_ENV_FILE = "$SOURCE_REPO/backend/.env"` — inside the PRD checkout, not symlinked/copied. `prd.env` at `/home/god/.config/pifitness/prd.env` is implied by the `<env>.env` convention but does not exist in the repo (`legacy_bootstrap_parameters.md:40`).

---

## Q19 Final-state guarantees

**Stated in target design** (`autodeploy_objectives.md:256-260`), not implemented:

| Outcome | PRD services | TEST services | TEST database |
|---|---|---|---|
| Failure at any step | Restored to prior state | Stopped | Rolled back to clean baseline if touched; otherwise untouched |
| NO-GO / timeout | Restored to prior state | Stopped | Rolled back to clean baseline |
| GO | Restored to prior state | Stopped | Left as is (migrated, plus test data) |

**Implemented in `test-up.sh`/`test-down.sh`:** `on_exit` trap (`test-up.sh:260-268`) and `recover_and_fail` (`test-up.sh:249-255`) run `test-down.sh` on abnormal exit, restoring PRD and cleaning TEST. State file (`~/.pifitness-test-state`) survives for retry. `trap - EXIT` at `test-up.sh:516` after successful completion.

**No README or docs statement of final state** outside `autodeploy_objectives.md`. `deploy/README.md` is about SQL migrations, not deploy final state.

**NOT VERIFIABLE FROM REPO:** Whether the Pi actually achieves these states — requires running on the Pi.

---

## Q20 Disk thresholds

**NOT FOUND.** No `df`, `disk`, `free`, `threshold`, `MB`, `GB` checks in any deploy script. Searched all `deploy/scripts/*.sh`. No disk space pre-flight exists.

---

## Q21 TEST services during PRD

**`test-down.sh`:** Stops TEST units first (Step 1, `test-down.sh:146-153`), then restores PRD services/timers (Step 2, `test-down.sh:164-186`). No explicit "verify TEST units are inactive" assertion step — it just stops them and proceeds.

**`bootstrap.sh` pre-flight:** Fails if any TEST unit is active (`bootstrap.sh:186-194`): "TEST unit $install_name is active. Stop it before reconciling its shared Python environment."

**PRD deploy:** Unimplemented (`bootstrap.sh:49-51`), so no PRD deploy step verifies TEST units inactive.

**Systemd `Conflicts=`:** `pifitness-fastapi-test.service` has `Conflicts=pifitness-fastapi.service` (`deploy/systemd/pifitness-fastapi-test.service:5`), so systemd cannot run both simultaneously.

---

## Q22 Config split

**`deploy/tests.conf`:** Does NOT exist, so it cannot be read by anything today, at any point in the flow.

**`deploy/scripts/autodeploy.conf`:** Does NOT exist either.

**Target design** (`autodeploy_objectives.md:274-282`):
- `autodeploy.conf` (in git): Service/timer names, test commands, migrations directory, health-check URL, persistent resources, dependency groups, required tools. Read at startup (before Step 1), always matches the code.
- Pi env file `/home/god/.config/pifitness/<env>.env` (not in git): DB host/port/credentials, secrets, absolute Pi-specific paths (venv, backup dir, log dir, checkout dir).

**Legacy wiring today:**
- `bootstrap.sh:61`: `PRD_ENV_FILE="$SOURCE_REPO/backend/.env"` — read from INSIDE the PRD checkout (validates existence at line 81)
- `bootstrap.sh:46`: `ENV_FILE="/home/god/.config/pifitness/test.env"` — read from OUTSIDE the checkout (validates existence at line 80)
- `test-up.sh:148-156`: sources `$HOME/.config/pifitness/test.env` (outside checkout)
- `test-up.sh:407`: copies test.env → `$TEST_DIR/.env.test` (inside checkout, after checkout)

**Can `deploy/tests.conf` only be read after checkout?** It doesn't exist. In the target design, `autodeploy.conf` is extracted by the launcher from the commit (so available from Step 1, before checkout), while the Pi env file is read from outside the checkout.

---

## 5. CONTRADICTIONS / GAPS

| # | Contradiction / Gap | Fact A (path:line) | Fact B (path:line) |
|---|---|---|---|
| 1 | `autodeploy.sh` / `autodeploy.conf` referenced but missing | `autodeploy_file_index.md:11-12` lists them as required files | `deploy/scripts/autodeploy.sh` does not exist (searched `deploy/scripts/`, 33 files listed, none named autodeploy) |
| 2 | `deploy/tests.conf` referenced but missing | `autodeploy_file_index.md:34`, `legacy_bootstrap_parameters.md:57` list it as test command config | `deploy/tests.conf` does not exist |
| 3 | Target venv outside app dir vs legacy venv inside app dir | `bootstrap.sh:47`: `VENV_DIR="/home/god/.local/share/pifitness/test-venv"` (OUTSIDE `/home/god/PiFitness-test`) | `legacy_bootstrap_parameters.md:42`: `/home/god/PiFitness/venv` is "Legacy PRD venv" (INSIDE `/home/god/PiFitness`); `deploy.sh:53`: `VENV_DIR="$PROJECT_DIR/venv"` |
| 4 | PRD env file at `~/.config` vs `backend/.env` | `autodeploy_objectives.md:278-280`: Pi env file `/home/god/.config/pifitness/<env>.env` (not in git) | `legacy_bootstrap_parameters.md:40`: legacy uses `backend/.env`; `bootstrap.sh:61`: `PRD_ENV_FILE="$SOURCE_REPO/backend/.env"` (inside checkout) |
| 5 | Legacy `react-ui` vs target `main` | `legacy_bootstrap_parameters.md:13`: "Legacy deploy target branch `react-ui` — conflict with target"; `deploy.sh:40`: `TARGET="react-ui"` | `legacy_bootstrap_parameters.md:12`: "PRD branch `main`"; `bootstrap.sh:44`: TEST=`feature-test`, PRD target=`main` |
| 6 | PG port 15432 vs 5432 | `test-up.sh:12`: `DB_PORT="15432"`; `test.env.template:5`: `PG_PORT=15432`; `legacy_bootstrap_parameters.md:112`: DB host default `localhost` | `autodeploy_file_index.md:60`: "DB port conflict. The legacy index shows 15432 for TEST scripts and 5432 for the fresh-dump path"; `bootstrap.sh:512`: `${PG_PORT:-5432}` default 5432 for fresh pg_dump |
| 7 | DB name: `pifitness` vs `personal_fitness` | `legacy_bootstrap_parameters.md:109`: "PRD DB name `pifitness`"; `test-up.sh:8`: `PRD_DB="pifitness"` | `deploy/README.md:86`: `psql -U god -d personal_fitness`; `evergreen_plan.md`/handoff: "PRD `personal_fitness`" |
| 8 | No PRD implementation in bootstrap | `bootstrap.sh:49-50`: `prd)` → `echo "PRD deployment is not yet implemented."; exit 2` | `autodeploy_objectives.md:20`: Step 4 "re-run the launcher, choosing PRD. PRD runs the same stages." |
| 9 | `deploy.sh` targets `react-ui` but trunk is `main` | `deploy.sh:3-4`: "PiFitness React Deployment Script / Called by bootstrap.sh -- target: react-ui"; `:40`: `TARGET="react-ui"` | `evergreen_plan.md:24`: "`main` is the deployable trunk"; `legacy_bootstrap_parameters.md:12`: PRD branch = `main` |
| 10 | `.github/` CI referenced but absent | `legacy_bootstrap_parameters.md` and design docs reference CI-like automation | `.github/` directory does not exist (verified by `Get-ChildItem .github` — no output) |

---

## 6. Files inspected

Full list with line counts:

**`deploy/` (33 files):**
1. `deploy/.env.test.example` — 18 lines (BLOCKED — secret file, not read)
2. `deploy/README.md` — 119 lines
3. `deploy/POST_DEPLOY_CHECKLIST.md` — 47 lines
4. `deploy/list-sql.ps1` — 101 lines
5. `deploy/migrations/*.sql` — 16 files, 12→629 lines each
6. `deploy/nginx/nginx-react.conf` — 89 lines
7. `deploy/nginx/nginx-template.conf` — 89 lines
8. `deploy/scripts/bootstrap.sh` — 720 lines (FULLY READ)
9. `deploy/scripts/test-up.sh` — 517 lines (FULLY READ)
10. `deploy/scripts/test-down.sh` — 244 lines (FULLY READ)
11. `deploy/scripts/deploy.sh` — 821 lines (FULLY READ)
12. `deploy/scripts/pi5_deploy.sh` — 328 lines (FULLY READ)
13. `deploy/scripts/ensure_binary.sh` — 62 lines (FULLY READ)
14. `deploy/scripts/test.env.template` — 16 lines (FULLY READ)
15. `deploy/scripts/library_requirements.txt` — 22 lines
16. `deploy/scripts/npm_requirements.txt` — 33 lines
17. `deploy/scripts/ecosystem.config.js` — 20 lines
18. `deploy/scripts/pathfinder.py` — 50 lines
19. `deploy/scripts/pifitness-hardware-health.py` — 47 lines
20. `deploy/scripts/CHANGES_SUMMARY.md` — 200 lines
21. `deploy/scripts/list-sql.ps1` — 87 lines
22. `deploy/systemd/pifitness-fastapi.service` — 25 lines (FULLY READ)
23. `deploy/systemd/pifitness-fastapi-test.service` — 25 lines (FULLY READ)
24. `deploy/systemd/pifitness-hardware-health.service` — 6 lines (FULLY READ)
25. `deploy/systemd/pifitness-hardware-health.timer` — 9 lines (FULLY READ)
26. `deploy/systemd/pifitness_agent.service` — 28 lines (FULLY READ)
27. `deploy/systemd/pifitness_agent.timer` — 10 lines (FULLY READ)

**`.audit/autodeploy_project/` (design docs, fully read):**
- `autodeploy_objectives.md`
- `autodeploy_file_index.md`
- `autodeploy_build_order.md`
- `legacy_bootstrap_parameters.md`
- `pi_bootstrap_shell.sh`
- `pi_bootstrap_output.md`
- `research_prompt.md`

**Backend:**
- `backend/pyproject.toml` (FULLY READ)
- `backend/main.py` (FULLY READ)
- `backend/.env` (BLOCKED — secret file, keys not read)
- Root `tests/` — 257 tests, no `backend/tests/` directory
- No `pytest.ini` found; config in pyproject or root config

**Frontend:**
- `frontend/pifitness/package.json` (FULLY READ) — test script = `vitest run`
- `frontend/pifitness/vitest.config.ts` (FULLY READ) — jsdom environment
- `frontend/pifitness/src/test/setup.ts` (FULLY READ)
- `frontend/pifitness/src/lib/api-client.ts:129` — `API_BASE = NEXT_PUBLIC_API_URL || ""`
- Frontend `__tests__` under `src/lib/__tests__/` and `src/lib/barcode/__tests__/`
- No `frontend/pifitness/tests/` directory
- No `jest.config.*`, `playwright.config.*`, `cypress.config.*`, `karma.conf.*`, `.mocharc.*`

**Missing (referenced but absent):**
- `deploy/scripts/autodeploy.sh`
- `deploy/scripts/autodeploy.conf`
- `deploy/tests.conf`
- `deploy.sh` at repo root
- `.github/` CI workflows
- `vite.config.ts` (not found)
- Root `package.json` (not found)

---

**End of report.** All findings are read-only; no repository files were modified. Confidence is High for all questions backed by direct file reads; Medium where design docs (not executable code) are the sole source. Items marked NOT VERIFIABLE FROM REPO require Pi-side execution (commands given where applicable).
