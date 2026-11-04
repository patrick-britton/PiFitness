# PiFitness Agentic Workflow: Plan

**Status.** Phase 1.5 is done. Phase 2 has not started.

---

## 1. How an agent uses this plan

1. You get one task ID. Read only that task's row, the files it lists, and `.clinerules/`. Do not read the rest of this plan.
2. A task touches at most 3 files and has one verify command. Exception: a script-applied mechanical batch of up to 10 files with a deterministic check.
3. State lives in files: append one line per task event to `docs/workflow/PROGRESS.md` (format in Appendix B). Never rely on chat memory.
4. Done = you ran the verify command and pasted its command line and last 20 output lines into `PROGRESS.md`.
5. Same failure twice with no new information: stop, write a `BLOCKED` line, end the session.
6. Anything outside the task goes to `docs/workflow/BACKLOG.md` as the next ID. Never fix it.
7. Tasks tagged **[human]** are not for agents. Tasks tagged **[Qn]** wait for that open question (section 3).
8. Model use: all models run at medium reasoning effort. Long context is available but is no substitute for small tasks.

## 2. Decisions (resolved)

Decisions marked *(planner)* are proposals made while rewriting this plan; they are reversible by the human.

| # | Decision |
|---|---|
| D1 | **Trunk.** `main` is the deployable trunk. PRD is whatever commit the Pi runs, recorded by a `prd-YYYYMMDD` tag; it can lag `main` (the Pi checkout still has a `deployment/` folder, and 9 migrations are undeployed). `react-ui` is **frozen** at the last deployed commit as a production failsafe (D19) — not retired, and not mirrored from `main` until a `main`-based deploy succeeds or Phase 5.1 replaces bootstrap's branch fetch. |
Agents work on a long-lived agent/phase-<N> branch cut from main at phase start. Each task is one commit made directly on it (message <task-id>: <summary>, PROGRESS.md update in the same commit). Agents never push, tag, create or switch branches, merge, rebase, reset, or touch main. The human reviews by exception: tasks reporting STATUS: REVIEW or BLOCKED via git show <hash>; CLEAN tasks are covered at phase end by the reviewer pack against git diff pre-trunk-<N>...agent/phase-<N>. To undo a task the human runs git revert <hash>. The human optionally runs the phase branch on the Pi with test-up (D3). At phase end the human merges agent/phase-<N> to main with git merge --no-ff, pushes, tags prd-YYYYMMDD if it is a deploy, and runs bootstrap. One task at a time; the human clears any REVIEW or BLOCKED before the next starts.
| D3 | **Test on the Pi is a swap, not a parallel stack.** Human-run `test-up <ref>`: refuses if the TEST and PRD DB names match; records the running PRD commit; dumps PRD to `DB_Backups`; stops the PRD services and both timers; creates `pifitness_test` from the stopped PRD DB (`CREATE DATABASE ... TEMPLATE`, fallback `pg_dump`/`pg_restore` with the TimescaleDB pre/post-restore calls); applies pending migrations to it; checks the ref out into a separate directory (`~/PiFitness-test`, own build and venv); starts test units on the same ports (nginx and the HTTPS name are unchanged, so PWA install is testable). `test-down` stops the test units, drops the test DB, and restarts PRD from its untouched directory, so restore needs no checkout or rebuild; it is idempotent and safe after a half-finished `test-up`. PRD data is never written while testing, and test units `Conflicts=` the PRD units so systemd cannot run both. Merge to `main` and deploy to PRD only after `test-down`. *(planner, from your Q3 answer)* |
| D4 | **Migrations.** `deploy/migrations/` is the single home. Tool: dbmate, subject to the 3.1 spike (Windows dev plus Pi arm64, `transaction:false` for TimescaleDB continuous aggregates, baseline marking, its own schema dump disabled so `schema.generated.md` stays the only schema map). Fallback: Alembic with raw SQL. The 9 existing files in `deploy/migrations/` are not legacy: none has reached PRD, so after renaming to dbmate versions (order confirmed by the human, `-- migrate:up/down` headers) they are the first pending migrations and the payload of the first test deployment (5.6). The baseline is the current PRD schema, before them. Migrations are additive and backward compatible with the last released code (expand/contract), so rolling back code needs no DB restore. DEV applies via tool; PRD applied by the human after a `pg_dump`. *(planner)* |
| D5 | **One home per kind.** Executables live in `tools/`; `.features/` holds data only. `fd.py` moves to `tools/workflow/fd.py` at task 4.0. |
| D6 | **Backlog home** is `docs/workflow/BACKLOG.md` (tracked, human+agent, append-only, IDs continue from B35). Not under `.audit/` (generated evidence, gitignored) or `.features/` (gitignored, reserved for the future feature workflow). |
| D7 | **Ratchet, not rewrite.** V1, V3 and V4 pass against recorded baselines in `tools/baselines/`; only new violations fail; baselines only shrink (Appendix A). The comment-ID strip is the last Phase 2 step. |
| D8 | **DEV safety (revised E1).** A host check cannot tell DEV from PRD (both are local). Use two layers: (a) `.env` is the only config source and the DEV `.env` only ever names the DEV database; (b) a database-level marker enforces it: `ALTER DATABASE <dev> SET pifitness.env = 'dev'`, stamped by dev reset and by the PRD-to-DEV refresh (a template clone and a restore do not carry it); the Pi test DB gets `'test'`, stamped by `test-up`, and `test-down` refuses to drop any database whose marker is not `test`. Tools (verify smoke, integration tests, reset, migrate, schema gen) refuse to run unless the marker is `dev`. The backend refuses to start when `APP_ENV=dev` and the marker is not `dev`, or when `APP_ENV` is `prod` or unset and the marker is `dev` or `test` (`APP_ENV=test` needs marker `test`). The Pi needs no change: unset means prod and PRD has no marker. Network reality: Postgres on the Pi listens on all interfaces and `pg_hba.conf` admits the whole LAN (B27), so the marker protects the tools only; closing that is B27. *(planner)* |
| D9 | **Adopt, don't build.** import-linter (G1-G6), vulture (dead Python), knip (dead TypeScript), pytest markers (test classes), dbmate (migrations), shellcheck (deploy scripts), tbls or equivalent (schema map and comment coverage; 3.2 spike, fallback a short script). **Custom, small:** `tools/check_docs.py` (cites, duplicate headings, INDEX rows), folder-map check in `repo_health`, `tools/lint_comments.py`, SQL smell greps, and the workflow tool `fd.py`. |
| D10 | **Legacy deletion.** A folder is deleted immediately once `git grep` (and for deploy-related folders `deploy/systemd/`, cron and the Pi, Q2) shows no references. History stays in git. No `archive/` or `legacy/` folders. |
| D11 | **Reviewer step per phase.** The last task of each phase produces a reviewer pack (section 8) for an independent model. A phase is closed only when every failure is resolved under section 8, "Failure handling". |
| D12 | **Forced closes.** `fd.py` cannot truly make force human-only (the agent runs as the same OS user). Forced closes are logged as `FORCED`, printed by `verify`, and listed in every reviewer pack. |
| D13 | **No tests on the Pi at deploy.** The gate runs on the dev machine before merge; the Pi runs the post-deploy checklist (read-only smoke). Phase 5 removes the test step from `deploy_react.sh` full mode. *(planner)* |
| D14 | **Citation form.** `path` or `path:line-line`. `check_docs.py cites` verifies the path exists and any line range is inside the file. Line drift is accepted; prefer `path` plus a symbol name in new text. |
| D16 | **Script source.** One source: the ref being deployed. `origin` has `deployment_script`, `main`, `react-ui`, `reorg` and `streamlit-prd`; it has no `deploy_merge`, although `bootstrap.sh` at HEAD names it (B32). The Pi keeps a stub of about 30 lines, rarely changed, that fetches the ref and runs `deploy/scripts/deploy.sh` from it. `test-up` therefore runs the scripts of the branch under test, and rollback to a `prd-` tag uses the scripts that tag shipped with. `deployment_script` is deleted only after the stub is live (5.1), because the Pi's current bootstrap most likely fetches from it. *(planner, replaces Q1)* |
| D17 | **Test mode and integrations.** `INT19b__move_tokens_to_api_services` appears to move API tokens into the database, so assume the PRD copy holds them. A refresh-token rotation in the test instance could invalidate PRD's tokens. Test mode therefore disables Garmin and Spotify calls and keeps the agent and hw-health timers off. Enabling integrations in a test run is a human decision per run. *(planner)* |
| D18 | **Package hygiene.** Deploy syncs the Pi venv to the runtime requirements (pip-tools `pip-sync` or `uv pip sync`; spike in 5.2; keep the editable `pifitness` install), so anything not listed is uninstalled and the removals are printed. Requirements are split into runtime and dev. First removals: streamlit, pytest, pytest-asyncio, pathlib, and httpx2 unless code imports it. *(from your answer)* |
| D15 | Kept from v1: evolve `fd.py`; Chrome PWA first, APK later; human acceptance via generated UAT page; module-by-module stabilization sweep instead of ad hoc bug fixes; workflow targets the weakest likely model. |
| D19 | **`react-ui` frozen as production failsafe.** Until a `main`-based deploy succeeds on the Pi and `test-up` (Phase 5.5) exists, `origin/react-ui` stays at the last deployed commit (`d19c5898a4389783f8a3bfcddef8cbe9be81e680`, tagged `failsafe-react-ui`) and is not fast-forwarded to `main`. Bootstrap and `deploy_react.sh` still fetch it, so an accidental bootstrap is a no-op redeploy of the current PRD, not a jump to untested code. `main` remains the trunk (D1). To deploy a `main`-based release before 5.1, the human first runs `git push origin main:react-ui`; from 5.1 onward bootstrap fetches the ref being deployed (D16) and the freeze ends naturally. Current PRD is recorded as `prd-YYYYMMDD`. *(human, 2026-11-03)* |

## 3. Open questions

No open questions. The human steps in 1.5.1 are tasks, not questions.

## 4. Ratchet and baseline rules (summary; full spec Appendix A)

Checks emit stable keys (`check|path|rule|hash`, no line numbers). Baselines: `tools/baselines/<check>.txt`. Gate fails on keys not in the baseline; warns on baseline keys that no longer occur. `--update-baseline` may remove keys only; growing a baseline needs `--allow-grow`, human only. Quarantined tests go in `tools/baselines/tests_quarantine.txt` and are deselected and counted. `verify` prints each baseline's size so the trend is visible.

## 5. Backlog dispositions (input to task 1.5.4; delete this section afterwards)

| ID | Disposition |
|---|---|
| B1 | Phase 8 roadmap pilot. Needs the gate and characterization tests first. |
| B2 | Closed by D4. The 9 files stay (Q4) and are renamed in 3.1. |
| B3, B6, B8 | Delete in 1.5.5 after `git grep` shows no references (scripts/, `frontend/pifitness/*.out` plus `.gitignore`, `backend/api/`). |
| B4 | Phase 5. Grep systemd, cron and `deploy/scripts/` first; consider renaming. |
| B5, B14, B15 | Closed by 1.5.5 (delete `.features/descriptions/`) and the REPO_MAP fix. One defect, not three findings. |
| B7 | Closed by 1.5.5 (delete empty folders; recreate with content if a tool needs them). |
| B9 | Phase 2.6 (vulture and the dead-file check find it). |
| B10 | Closed by D5. |
| B11 | Superseded by import-linter (2.5). |
| B12 | Closed; W9 exists in TARGET_STATE. |
| B13 | Human check at HEAD: `git grep -n "health\|agent" deploy/scripts/deploy_react.sh` against the folders it expects; fix Phase 5. |
| B16, B17, B18 | Phase 5 (B16 via D16). B17: collapse five blocks with Pi rollback steps. B18: delete with the branch-choice rewrite. |
| B19 | The PRD venv has `httpx2 2.5.0` (identified by the human as the Pydantic Services fork of httpx; its SSE-parser advisory is noted, and the human accepts it for a personal app), `pathlib 1.0.1`, `streamlit 1.58.0`, `pytest` and `pytest-asyncio`; `setup.py` lists pytest as a runtime requirement. Decision: `git grep -n httpx2` decides it; keep and pin a patched version only if code imports it, otherwise remove. Package hygiene is a deploy feature (D18). `pathlib` on PyPI is dead weight on Python 3.13. Clean up in 2.6 and 5.2. |
| B20 | Measure in 3.7; fix in Phase 8. |
| B21, B22 | Phase 5 (B22 with B4). |
| B23 | Phase 8, probably one global `:active` style. |
| B24 + B25 | Phase 8 together: shared BottomSheet owning back-gesture handling. Medium or Large, not Small. |
| B26 | Phase 8 PWA pilot (E5). |
| B27 | Postgres on the Pi: `listen_addresses = '*'`, `pg_hba.conf` admits `192.168.86.0/24` with password auth, no host firewall (`ufw` not installed). Port 5432 is closed from outside (canyouseeme.org), so exposure is LAN-only. The human connects with DataGrip over the LAN; move DataGrip to an SSH tunnel, then bind to `localhost` and drop the LAN line (Appendix D walkthrough). Human, now. |
| B28 | uvicorn binds `0.0.0.0:8000` though nginx proxies to `127.0.0.1`; bind loopback unless LAN access is wanted. Phase 5. |
| B29 | Installed `pifitness-fastapi.service` has duplicate `ReadWritePaths`, a drop-in `override.conf`, and `EnvironmentFile=/home/god/PiFitness/.env`; reconcile with the repo unit so one file is the truth. Phase 5. |
| B30 | Mostly closed: the unit and `deployment/deploy_react.sh` and `bootstrap.sh` on the Pi all use `/home/god/PiFitness/venv`. `~/venv` and `~/venvs/pifitness-react-backend` look unreferenced; the human deletes them once `grep -rn venvs /etc/systemd ~/.pm2 /etc/cron*` is empty. At HEAD, confirm with `git grep -n VENV_DIR deploy/`. |
| B31 | Closed: the human confirmed no tokens are committed to git. |
| B32 | `bootstrap.sh` at HEAD names `deploy_merge`, which does not exist on `origin`; it would fail on first use. Fixed by 5.1. |
| B33 | `frontend/pifitness/next-env.d.ts` shows as modified on the Pi (generated by the build); ignore or commit it so `git pull` stays clean. Phase 5. |
| B34 | Stale remote branches `streamlit-prd` and `reorg`: tag as `archive/<name>`, then delete (1.5.1b). `deployment_script` waits for 5.1. |

## 6. Phases

Order: 1.5, then 2; 3 and 4 in parallel after 2.4; 5.1-5.4 can run alongside 2 to 4, 5.5-5.7 need 3.1; then 6, 7, 8, 9. Each phase ends with a reviewer-pack task.

### Phase 1.5: Close Phase 1 and install interim rules

| ID | Task | Files | Done when |
|---|---|---|---|
| 1.5.0a | **[human]** Commit DEV working tree to `reorg` before tagging. `git switch reorg`; confirm `git stash list` shows `stash@{0}: On reorg: wip pre-1.5.0`; `git stash pop`; `git status --short` (expect the Phase 1 files from the stash); `git add -A`; `git commit -m "Phase 1 drafts; retire docs/reorg and stale audit script"`; `git status --short` (must be empty). Resolve conflicts manually only if the pop reports them; otherwise stop and ask. | git | `git status --short` empty; `git rev-parse reorg` is a new tip (was `03086c62a3b16b381487bea42add3de4b3f27d93`); `docs/ARCHITECTURE.md`, `docs/REPO_MAP.md`, `docs/INDEX.md`, `docs/UI_PRINCIPLES.md` and `docs/workflow/` are in the commit |
| 1.5.0 | **[human]** Tag and set trunk (D19: `react-ui` frozen). On the Pi: `cd ~/PiFitness && git rev-parse HEAD` — currently `d19c5898a4389783f8a3bfcddef8cbe9be81e680`. On DEV: `git tag prd-YYYYMMDD d19c5898a4389783f8a3bfcddef8cbe9be81e680` (current PRD per D1; substitute today's date, e.g. `prd-20261103`); `git tag pre-trunk-main main`, `git tag pre-trunk-origin-react-ui origin/react-ui`, `git tag pre-trunk-reorg 03086c62a3b16b381487bea42add3de4b3f27d93`, `git tag failsafe-react-ui origin/react-ui`; `git switch main`; `git merge --ff-only reorg`; `git push origin --tags`; `git push origin main`. **Do not push `reorg:react-ui`** — the freeze is deliberate (D19). | git | `git rev-parse main origin/main` prints the same hash (= post-`1.5.0a` reorg tip); `git rev-parse origin/react-ui` unchanged at `d19c5898a4389783f8a3bfcddef8cbe9be81e680`; `prd-YYYYMMDD` exists on origin and equals the Pi's HEAD |
| 1.5.1 | **[human]** Security steps from Appendix D, in order: (a) Postgres: DataGrip over an SSH tunnel first, then bind to `localhost` and drop the LAN `pg_hba` line (B27); (b) run `git grep -nE "httpx2" -- '*.py' '*.txt'` and record whether anything imports it (B19). | none | `sudo ss -ltnp \| grep 5432` shows 127.0.0.1 only; app and DataGrip still work |
| 1.5.1b | **[human, after 1.5.0]** Prune branches (B34): `git branch -r --merged origin/main`, then for `reorg` and `streamlit-prd`: `git tag archive/<name> origin/<name>`, `git push origin archive/<name> :<name>`. Leave `deployment_script` until 5.1. | git | `git ls-remote --heads origin` lists only main, react-ui, deployment_script |
| 1.5.2 | Create `.clinerules/interim-rules.md` from Appendix C. Replace the "no git commands" line in `.clinerules/default-rules.md` with the D2 allowlist and denylist. | 2 | `(Get-Item .clinerules/interim-rules.md).Length` is 1024 or less; `Select-String -Path .clinerules/default-rules.md -Pattern "denylist"` prints a line; `Select-String -Path .clinerules/default-rules.md -Pattern "no git commands"` prints nothing |
| 1.5.3 | Write `docs/workflow/GIT_MODEL.md` (one page: D1 and D2 flow, tag naming (`prd-YYYYMMDD`, `pre-trunk-*`, `failsafe-react-ui`), `react-ui` frozen as production failsafe per D19, rollback by redeploying a `prd-` tag, the D3 swap from `test-up` through `test-down` to merge and deploy). Add INDEX rows for it, `PLAN.md` and `PROGRESS.md`. | 2 | `python tools/check_docs.py index` once it exists (1.5.7); until then human review |
| 1.5.4 | **[human]** Move this plan to `docs/workflow/PLAN.md`; create `docs/workflow/PROGRESS.md` with the Appendix B header. | 2 | files exist |
| 1.5.5 | Delete legacy per D10, discovery-driven. For each target below, classify current state (tracked / untracked / absent / referenced) and act; record classification and `git grep` evidence per target in PROGRESS. Targets the row intends removed (if still present and unreferenced externally): `.features/description_draft/`, `.features/descriptions/`, `.features/designs_active/`, `.features/designs_complete/`, `.clinerules/hooks/`, `backend/api/`. **Carve-outs (do not delete here):** `scripts/repo_audit/` (referenced in `docs/REPO_MAP.md` and `docs/architecture/dynamic-targets.md`; superseded by `tools/repo_health.py` at 2.5 — append backlog row for its removal at 2.5). **Generated data (git rm --cached + gitignore if not already present):** `scripts/schema_cache.json` (8467 lines, generated). **Untracked artifact (gitignore only):** `scripts/sync-prod-to-dev.log` → add `scripts/*.log` to `.gitignore` if not present. Absent or already-handled targets: skip with "already absent" classification. For any listed target with external `git grep` references not covered by a carve-out: do not delete; append backlog row. Remove stale references to deleted items from REPO_MAP, INDEX and ARCHITECTURE (these docs are named in the row, in scope). | batches of 3 | per-target classification and outcome recorded in PROGRESS with `git grep` evidence; each target with zero external refs absent from disk and git; `.gitignore` covers `scripts/*.log` and `scripts/schema_cache.json` (or notes "already present"); REPO_MAP/INDEX/ARCHITECTURE contain no references to deleted paths; backlog row for `scripts/repo_audit/` removal at 2.5 |
| 1.5.6 | Move `.audit/p1/backlog_phase1.md` to `docs/workflow/BACKLOG.md` (tracked; stage it with `git add`); add `Status` and `Phase` columns; apply section 5 and append B27-B31; fix references (see Done-when). Update REPO_MAP and INDEX rows. | 3 | `git grep -n "backlog_phase1" -- . ':!docs/workflow/PLAN.md' ':!docs/workflow/PROGRESS.md'` empty, and `git ls-files docs/workflow/BACKLOG.md` prints the path |
| 1.5.7 | Build `tools/check_docs.py` with subcommands `cites` (R2: backticked paths exist, line ranges valid, factual bullets carry a path), `headings` (duplicate-heading scan, R3) and `index` (one INDEX row per file in `docs/` and `.clinerules/`). Baseline with the ratchet rules (use the Appendix A format by hand until `tools/ratchet.py` exists in 2.1). Fix every `TODO(cite)` in REPO_MAP. | 3 | `python tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md` exits 0 |
| 1.5.8 | Reviewer pack for Phase 1.5 (section 8). Human runs it on an independent model. | pack | review recorded in PROGRESS |
| 1.5.9 | (R: 1.5.3) Make `python tools/check_docs.py index` exit 0. In `tools/check_docs.py`: skip `docs/INDEX.md` itself, and treat an INDEX row whose path ends in `/` as covering every file beneath it. In `docs/INDEX.md`: add one row, in the existing format, for each of `docs/architecture/dynamic-targets.md`, `docs/architecture/garmin-login.md`, `docs/architecture/test-strategy.md`, `.clinerules/interim-rules.md`, `.clinerules/workflows/plan-task.md`, plus one directory row for `docs/sample_json_food_payloads/`. Touch no other file. | 2 | `python tools/check_docs.py index` exits 0 and `python tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md` still exits 0 |

### Phase 2: Verify gate, repo health, DEV reset

| ID | Task | Files | Done when |
|---|---|---|---|
| 2.1 | `tools/ratchet.py` (compare, prune, shrink-only update) per Appendix A; add a `tools/baselines/` row to REPO_MAP. | 2 | its own tests pass; a planted new key fails, a removed key warns |
| 2.2 | `tools/lint_comments.py`: flags feature, bug and task IDs and change-history comments (V3). Baseline it. | 2 | `python tools/lint_comments.py` exits 0 against baseline |
| 2.3 | `tools/verify.py` skeleton. Scopes: `backend`, `frontend`, `docs`, `deploy`, `db`, `all`. Steps in cost order, stop at first failure: compile, tsc, lint, shellcheck, comment lint, pytest, vitest, API smoke against DEV. Use the working commands in `.clinerules/environment.md`. Each step uses ratchet baselines. Ends with `VERIFY <scope> exit=<n>` and prints `FORCED` count and baseline sizes. Fix the `.env` path problem and root-page issues here only if they block a step; otherwise quarantine and backlog. | 3 | `python tools/verify.py --scope all` exits 0 on the current red baseline |
| 2.4 | **DEV safety and reset (moved from v1 Phase 5).** (a) Marker and guard per D8 in `backend/pifitness/core/` plus a shared tool guard; extend `tests/test_no_prd_default.py`. (b) Template database and `tools/db/dev_reset.py` (drop DEV, `CREATE DATABASE ... TEMPLATE`, stamp marker; later also replay migrations, 3.6). Spike first: confirm TimescaleDB survives a template clone and measure reset time. (c) Human patches `tools/db/sync-prod-to-dev.ps1` to re-snapshot the template and re-stamp the marker. | 3 per sub-step | guard tests pass; `dev_reset.py` then `verify --scope db` exits 0; reset time recorded |
| 2.5 | `tools/repo_health.py`. (a) Folder-vs-REPO_MAP check (R1) plus import-linter contracts for G1-G6, with K1 imports listed as `ignore_imports` (this replaces the broken generator, B11). (b) vulture, knip and deptry (unused and missing Python dependencies), baselined. (c) SQL smell greps: `SELECT *`, unbounded queries, OFFSET, pandas in `backend/pifitness/`, baselined. N+1 is not statically detectable; it is a review item, not a script check. | 3 per sub-step | `python tools/repo_health.py` exits 0 against baselines |
| 2.6 | Red-baseline triage. First task: record counts (tsc errors, failing tests, vulture hits) in PROGRESS. Then one small task per cluster, 3 files max, each shrinking a baseline: B9, B19 cleanup (split runtime and dev requirements; declare runtime dependencies in one place, not both `setup.py` and the requirements file), pre-existing tsc errors, fail
ing tests. | 3 each | the named baseline shrank; verify exits 0 |
| 2.7 | Test classification. Register markers `invariant`, `contract`, `smoke`, `journey` in `pytest.ini` with `--strict-markers`. Classify in batches of 15 tests into `tests/manifest.tsv` (test, class, Keep/Quarantine/Delete, one-line reason); script applies it. Rules for new tests: purpose docstring, no mocking the unit under test, no IDs. | 3 per batch | `pytest --collect-only -q` shows every test with a class marker |
| 2.8 | Comment sweep, last. Tier A: `lint_comments.py --fix-tier-a` deletes comments that consist only of an ID or history note. Tier B: agent rewrites mixed comments in batches of at most 10 files to say what and how. Baseline shrinks each batch. | 10 per batch | `lint_comments` baseline reaches 0 |
| 2.9 | Exit check and reviewer pack. | pack | `python tools/dev_reset.py` then `python tools/verify.py --scope all` exits 0; review recorded |
| 2.10 | Remediation from Phase 2 reviewer: make `verify.py` and `dev_reset.py` set/use `APP_ENV=dev` against the stamped DEV DB; baseline `tools/workflow/` in REPO_MAP and the new SQL smells in `repo_health`; remove throwaway outputs (`_dev_*`, `_verify_*`, `_violations.txt`, `tools/_tmp_*`); refresh the 2.9 review record after verify exits 0. | 3 | `python tools/db/dev_reset.py` then `python tools/verify.py --scope all` exits 0; `python tools/repo_health.py` exits 0; `git status --short` clean of throwaway files |
| 2.11 | (R: 2.5, 2.10; class T) Fix `tools/repo_health.py` so `violation_key` never includes line numbers (Appendix A: "Never include line numbers"): the hashed detail embeds `line N` / `@line_no` in every G3/G4/G5/G6 import and sql-smells select-star/offset/pandas call, so any line shift (task 2.8 comment sweep, future edits) invalidates every baseline key for that file and the gate falsely reports drift as new violations. Remove line numbers from the detail text passed to `violation_key` (path-only identification per Appendix A), then regenerate `tools/baselines/import-contracts.txt` and `tools/baselines/sql-smells.txt` with `--update-baseline --allow-grow`. | `tools/repo_health.py`, `tools/baselines/import-contracts.txt`, `tools/baselines/sql-smells.txt` | `python tools/repo_health.py` exits 0; `python tools/verify.py --scope all` exits 0; `git status --short` shows only the three files modified |

### Phase 3: Data layer

| ID | Task | Files | Done when |
|---|---|---|---|
| 3.0 | **Backlog grooming, Phase 3.** Read only: Phase 3 and Phase 4 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`, and section 5 if still present. For every open item with `Phase` <= 3: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task. Do not modify any existing task row. Log `GROOM-ESCALATE` for: pull-forward past Phase 4; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 3 is named in a Phase 3 task row, or has `Phase` > 3 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 3.1 | Migration spike and baseline (D4). (a) Inventory the 9 files in `deploy/migrations/`: apply order from names and `git log`, dependencies between them, which are already applied to DEV (schema diff); human confirms the order. (b) Rename to dbmate versions with up/down headers; SQL unchanged. (c) `tools/db/migrate.*`; baseline version = current PRD schema, recorded on DEV; human records it on PRD. DEV is rebuilt from a fresh PRD copy and the 9 replayed, never patched in place. | 3 per step | status shows baseline applied and 9 pending; replay on a fresh DEV copy succeeds |
| 3.2 | Schema map: generate `docs/data/schema.generated.md` (tables, columns, comments, FKs, indexes, views; exclude TimescaleDB internal schemas). Spike tbls; fall back to a short script. Add the regenerate-and-diff check to `verify --scope db`. Add `docs/data/` to REPO_MAP and INDEX. | 3 | regeneration produces no diff |
| 3.3.0 | COMMENT backfill, one migration per domain batch (comments are DDL, so they go through 3.1). Agent creates the dbmate templates of all columns/tables needing comments.| 10 | coverage baseline shrank; PRD application routes through dbmate protocal. |
| 3.4 | `docs/data/dictionary.md` per domain: grain of key tables, canonical source per concept, rules marked enforced or convention with a one-line why. | 3 per domain | domain section present; human reviewed |
| 3.5 | `tools/db/dd.py find <term>`: thin search over dictionary, names and comments. D4 check: designs adding a table or column paste its output. | 2 | `dd.py find <known term>` returns the right table |
| 3.6 | Make `dev_reset.py` replay pending migrations after the template clone and after a PRD restore. | 2 | a deliberate test migration survives `dev_reset.py` |
| 3.7 | Measure B20 with `EXPLAIN ANALYZE` on DEV for row counts and plan shape; human runs one read-only `EXPLAIN` on PRD for Pi timing (dev timings do not transfer). Overlap audit findings go to the backlog. | 2 | numbers recorded in backlog item B20 |
| 3.8 | Exit check and reviewer pack. | pack | review recorded |
| 3.9 | (B2) Record `deploy/migrations/` as the single migration home in Phase 3 text; check INT19b__-style naming vs D3 "numbered files". | docs/workflow/PLAN.md | `git grep -n "single home" docs/workflow/PLAN.md` exits 0; migration naming reviewed vs D3 |
| 3.10 | (B3) Decide fate of `scripts/` and `scripts/repo_audit/`: retool useful parts into `tools/audit/` or delete; check overlap with `tools/audit/p1_facts.py`. | scripts/repo_audit, tools/audit | `git ls-files scripts/repo_audit` exits 0; decision recorded in PROGRESS |
| 3.11 | (B5) Hash-compare `.features/descriptions/json_food_responses/` to `docs/sample_json_food_payloads/`; decide what `.features/descriptions/` still holds. | .features/descriptions, docs/sample_json_food_payloads | `test -d .features/descriptions` exits 0; comparison recorded in PROGRESS |
| 3.12 | (B9) Confirm whether `backend/pifitness/config/queries.py` is dead or wired indirectly. | backend/pifitness/config/queries.py | `git grep -n "queries" backend/main.py` runs; wiring status recorded in PROGRESS |
| 3.13 | (B12) Add missing statement W9 (module walkthrough checklist) to `docs/workflow/TARGET_STATE.md`. | docs/workflow/TARGET_STATE.md | `git grep -n "W9" docs/workflow/TARGET_STATE.md` exits 0 |
| 3.14 | (B14) Fix REPO_MAP rows citing absent `.features/descriptions/json_food_responses/almond_responses.json`. | docs/REPO_MAP.md | `git grep -n "json_food_responses" docs/REPO_MAP.md` runs; citations reviewed |
| 3.15 | (B15) Resolve `.features/descriptions/` citation to absent `almond_responses.json` (folder is empty). | docs/REPO_MAP.md | `git grep -n "almond_responses" docs/REPO_MAP.md` runs; citation resolved |
| 3.16 | (B35) Confirm `.features/descriptions/` keep decision; external refs in backend/pifitness/tri_tip/queries.py:8 and frontend files. | docs/REPO_MAP.md, backend/pifitness/tri_tip/queries.py | `git grep -n "json_food_responses" -- '*.py' '*.ts' '*.tsx'` runs; keep recorded |
| 3.17 | (B36) Confirm `.features/designs_active/` keep decision; Phase 4 workflow may use it. | docs/REPO_MAP.md | `git grep -n "designs_active" -- '*.py' '*.ts' '*.tsx'` runs; keep recorded |
| 3.18 | (B37) Confirm `backend/api/` keep decision; marker `__init__.py` referenced in comments, nothing imports it. | backend/api | `git grep -n "backend/api" -- '*.py' '*.ts' '*.tsx'` runs; keep recorded |
| 3.19 | (B38) Schedule `scripts/repo_audit/` removal (superseded by tools/repo_health.py at 2.5); references in docs/REPO_MAP.md and docs/architecture/dynamic-targets.md. | scripts/repo_audit, docs/REPO_MAP.md | `git ls-files scripts/repo_audit` exits 0; removal scheduled in PROGRESS |

### Phase 4: Workflow engine (strong model for 4.1 and 4.3)

| ID | Task | Files | Done when |
|---|---|---|---|
| 4.0.0 | **Backlog grooming, Phase 4.** Read only: Phase 4 and Phase 5 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`. For every open item with `Phase` <= 4: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task. Do not modify any existing task row, including 4.0.1. Log `GROOM-ESCALATE` for: pull-forward past Phase 5; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 4 is named in a Phase 4 task row, or has `Phase` > 4 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 4.0.1 | Move `fd.py` to `tools/workflow/fd.py`; fix references in `.clinerules/workflows/*.md` (separate task per 3 files) and REPO_MAP. | 3 per step | `git grep -n ".features/tools"` empty; existing commands still run |
| 4.1 | Evidence gate: `done T##` runs the task's verify command, records exit code and output tail, refuses without a matching `start`; `--force` writes `FORCED` (D12). Structured log lines tied to task and AC IDs. | 2 | synthetic fixture: gate fires |
| 4.2 | `backlog add` (next ID, append-only). Import B1-B26 as-is. | 2 | `fd.py backlog add` appends B27 |
| 4.3 | `brief T##` and `resume` (brief, recent log, `git status --porcelain`, current branch, commits since task start, a verify run). Depends on D2. | 2 | an interrupted fixture task resumes with no human input |
| 4.4 | Slim bug schema (symptom, root cause, fix, check added) linked to task or AC, with duplicate check. Defer if 4.1-4.3 overran. | 2 | duplicate fixture rejected |
| 4.5 | Synthetic fixture feature with deliberate flaws (task closed without start, unticked AC, duplicate bug, anchor to a missing file). | 3 | each gate fires |
| 4.6 | Exit check and reviewer pack. UAT HTML (`uat build`, `uat import`) is not built here; it is task 8.0. | pack | scripted fake feature runs start, done, interruption, resume |

### Phase 5: Environments and deployment

| ID | Task | Files | Done when |
|---|---|---|---|
| 5.0 | **Backlog grooming, Phase 5.** Read only: Phase 5 and Phase 6 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`. For every open item with `Phase` <= 5: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task; section 5's Phase 5 dispositions (B4, B13, B16-B18, B21, B22, B28-B30, B32, B33) must each map to a Phase 5 task row or be escalated. Do not modify any existing task row. Log `GROOM-ESCALATE` for: pull-forward past Phase 6; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 5 is named in a Phase 5 task row, or has `Phase` > 5 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 5.1 | **[Q2]** Per D16: reduce bootstrap to a stub of about 30 lines that fetches a ref (default `main`) and runs `deploy/scripts/deploy.sh` from it; print a change summary before applying; remove branch-choice code (B16, B18); delete the extra scripts branch. Add `--dry-run`. Tag the previous scripts for rollback. | 3 | `shellcheck` clean against baseline; `--dry-run` on the Pi prints the plan |
| 5.2 | Collapse the five duplicated systemd-install blocks (B17); resolve B13; add package sync (D18); remove the test step from full mode (D13); rename scripts and nginx conf that still say `react` when safe. | 3 per step | human runs full deploy on the Pi; service health OK; rollback steps written |
| 5.3 | Pi post-deploy checklist (service health, `/api/health`, timers, build-time env vars, two heavy queries, one feature spot check). | 1 | human runs it once |
| 5.4 | Fix B21 (`frontend/DEPLOYMENT_GUIDE.md`), B4 and B22 (with rename). Confirm the HTTPS certificate for `pifitness.duckdns.org` is valid. | 3 | cert expiry date recorded |
| 5.5a | Test-mode units and config (D3, D17): `pifitness-fastapi-test.service` and a pm2 entry `pifitness-next-test` running from `~/PiFitness-test` with its own venv, `Conflicts=pifitness-fastapi.service` and `ReadWritePaths` that include `~/PiFitness-test` (the PRD unit runs with `ProtectHome=read-only`); test env template with `APP_ENV=test`, test DB name, integrations off. | 3 | `systemd-analyze verify` passes on the Pi |
| 5.5b | `test-up <ref>` per D3. Refuses on: equal DB names, failed dump, already in test mode, dirty state. Writes `~/.pifitness-test-state` (PRD commit, dump path). Prints each step before running; `--dry-run`. Spike the TimescaleDB template clone first; fall back to dump and restore. **Needs 3.1.** | 2 | `--dry-run` prints the plan; shellcheck clean |
| 5.5c | `test-down`: idempotent; stops test units, drops the test DB (`--keep-db` optional, marker check from D8), restarts PRD units and timers, calls `/api/health`. | 2 | shellcheck clean |
| 5.5d | **[human]** Run test-up then test-down. Then inject a failure mid-`test-up`, run `test-down`, confirm PRD is healthy and its row counts match before and after. | none | counts recorded in PROGRESS |
| 5.6 | **[human]** First test deployment: `test-up` the branch carrying the 9 migrations; apply and time them; walk the changed features on the phone over HTTPS; `test-down`. Failures go to the backlog and are the first Fix-tier work after the merge. The fresh test venv also exposes undeclared dependencies. Expect a large jump, not just 9 migrations: PRD runs the 16 Oct commit with the old layout (`deployment/`, root `backend_functions/`), so also confirm the editable install (`pip install -e`) resolves `pifitness` at the new layout. | none | migrations applied cleanly to the PRD copy |
| 5.7 | Exit check and reviewer pack. | pack | full deploy from `main` after the test run; rollback steps written |

### Phase 6: Templates (strong model plus human)

Each fact appears once. Feature brief (human, small): intent, numbered problems P1.., out of scope, constraints by link, what done looks like. Design (agent): AC list is the only place ACs live (ID, intent, `auto` line, `human` line, mobile flag); one-line decisions; risks separate. Tasks: ID, dependencies, AC IDs only, tool-verified anchors, change, one verify command, at most 3 files. Log, bugs, backlog: tool-written. Intake tiers: Fix (one-page brief), Feature (full flow), Roadmap (architect splits). Traceability: every P# maps to an AC or an out-of-scope line, checked at review.

| ID | Task | Done when |
|---|---|---|
| 6.0 | **Backlog grooming, Phase 6.** Read only: Phase 6 and Phase 7 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`. For every open item with `Phase` <= 6: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task. Do not modify any existing task row. Log `GROOM-ESCALATE` for: pull-forward past Phase 7; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 6 is named in a Phase 6 task row, or has `Phase` > 6 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 6.1 | Write the three templates; replace the old `_DESCRIPTION_TEMPLATE.md` and `_DESIGN_TEMPLATE.md`; rewrite the five legacy workflows as thin wrappers over `fd.py`, or delete them. | new feature shorter than an old one; every P# maps to an AC |
| 6.2 | Reviewer pack. | review recorded |

### Phase 7: Rules, skills, model notes

| ID | Task | Done when |
|---|---|---|
| 7.0 | **Backlog grooming, Phase 7.** Read only: Phase 7 and Phase 8 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`. For every open item with `Phase` <= 7: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task. Do not modify any existing task row. Log `GROOM-ESCALATE` for: pull-forward past Phase 8; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 7 is named in a Phase 7 task row, or has `Phase` > 7 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 7.1 | Merge `interim-rules.md` into `default-rules.md`; the file is about 2 KB or less. | size check |
| 7.2 | Collapse the seven skills in `.agents/skills/` to four or fewer: requirements (from business-analyst), design (from code-architect), implement (from backend-engineer, frontend-dev, sql-expert and deployment-engineer, sections by task tag), review. Decide block-executor's fate (superseded by `fd.py` tasks). | at most 4 `SKILL.md` files |
| 7.3 | Ablation: run a benchmark task with and without each skill; delete any skill that does not help. | results recorded |
| 7.4 | Write `docs/workflow/model-notes.md`: each model (Nemotron 3.5 Super VL, Sonnet 5.5, DeepSeek), usable-for, quirks, reasoning effort (medium for all), date verified. Run the three-task benchmark (frontend tweak, backend endpoint with DB write, SQL change). | benchmark completes on the best current model |
| 7.5 | Reviewer pack. | review recorded |

### Phase 8: Stabilization sweep and pilots

| ID | Task | Done when |
|---|---|---|
| 8.0.0 | **Backlog grooming, Phase 8.** Read only: Phase 8 and Phase 9 task rows in `docs/workflow/PLAN.md`, `docs/workflow/BACKLOG.md`, the last 40 rows of `PROGRESS.md`. For every open item with `Phase` <= 8: place it by editing only its `Status` and `Phase` cells, and/or append a task row to a phase table (next free number, Task cell starting `(B<n>)`, satisfying ≤3 files and one verify command). Close items the plan already handled, naming the closing task; the Phase 8 pilots (B1, B20, B24+B25, B26) must each map to a Phase 8 task row or be escalated, and any Large item must be split per Phase 6 templates or escalated, not placed whole. Do not modify any existing task row, including 8.0.1. Log `GROOM-ESCALATE` for: pull-forward past Phase 9; second deferral; conflict with a D-decision, TARGET_STATE or section 9; two items needing the same file in-phase with no order; Size Large or cannot fit ≤3 files; Block severity. | 2 | every open item with `Phase` <= 8 is named in a Phase 8 task row, or has `Phase` > 8 with a one-line reason in `Status`, or has a `GROOM-ESCALATE` line in `PROGRESS.md`; every task row appended here satisfies ≤3 files and one verify command |
| 8.0.1 | Build `uat build` and `uat import` (self-contained phone HTML from the design file, pass or fail per item, note field, exported results). Built only now, because the sweep is its first user. | fixture round trip |
| 8.1 | **Sweep.** Per module (food, activities, weight, music, ...) a strong model drafts acceptance criteria from code and dictionary; human edits; human walks it on the phone; failures go to the backlog as Fix-tier items grouped by module and are worked through the gates. Auto-checkable steps become journey tests. Satisfies W9. | every finding in the backlog |
| 8.2 | **Pilot: PWA install** (B26, E5): manifest, service worker, icons, safe areas. Exercises UAT and mobile checks (V5). | installs from Chrome over HTTPS |
| 8.3 | **Pilot: BottomSheet** (B24 + B25, R6, R7). Shared component owning back-gesture handling; global pressed style (B23, R3). | R3, R6, R7 pass |
| 8.4 | **Pilot: DB feature with a migration.** Exercises `dd find`, the migration flow, D4 and `verify --scope db`. | migration applied DEV then PRD by human |
| 8.5 | **Roadmap pilot: B1** (fold `backend_functions/` into `core/` and domain modules). First break the three reverse imports (K1), and write characterization tests; architect splits it into features. Then fix B20. | `repo_health` K1 baseline empty |
| 8.6 | Record metrics (section 7). Reviewer pack. | review recorded |

### Phase 9: Freeze and retro

Convert recurring lessons into checks and prune what did not earn its place. Finalize `TARGET_STATE.md`, store the evergreen prompt in `docs/workflow/`, tag the repo.

## 7. Metrics and review triggers

`docs/workflow/metrics.md`, one line per feature: bugs, tasks, first-pass UAT result, sessions, whether the model changed mid-feature. Starting targets, adjusted after the sweep: at most 0.3 bugs per task; at least 80 percent of ACs pass human UAT first time. No baseline exists; the first month of sweep results becomes it. Re-run the evergreen prompt when the architecture changes significantly, the default model changes, two consecutive features miss the bug target, a production problem traces to an agent change, or a quarter passes.

## 8. Reviewer pack (D11)

The last task of each phase writes `.audit/reviews/phase-<N>.md` from the template below, filling the bracketed parts. The human pastes it into an independent model (a different one from the builder where possible) together with the listed files, then records the result in PROGRESS and handles any failure under "Failure handling" below.

```
You are an independent reviewer. You did not build this. 
You are forbidden from using the editor/create/delete tools. You may read any file needed to run or interpret a command. If completing a step appears to require a file edit, stop and report it as a tooling defect (T) instead.
Repo: PiFitness. Phase [N] of docs/workflow/PLAN.md. Read only the files attached.
1. For each exit criterion below, run the command yourself and paste the last 20 lines of output.
   Do not trust PROGRESS.md claims.
2. Check: any baseline growth (git diff main -- tools/baselines, added lines only), any FORCED lines
   in PROGRESS.md, any file touched outside its task's listed files, any new fact stated in two places.
3. Report PASS or FAIL per criterion with the evidence. List defects as: file, problem, severity. For each FAIL, say whether it looks like a work defect (W), a criterion defect (C) or a tool defect (T), and why.
4. Stop. Do not propose redesigns beyond the criteria.

verify.py --scope all may take 60–120 s; if your runner times out at 30 s, use the provided long-command launcher with its existing presets, or report a tooling defect.

Exit criteria and commands: [copied from the phase]
Files to attach: [PLAN.md task rows for this phase, PROGRESS.md, files changed (git diff main --stat), TARGET_STATE.md statements it touches]
```

### Failure handling (D11)

For every FAIL the reviewer reports, the human assigns exactly one class before anything changes:

| Class | Meaning | Remedy | Who |
|---|---|---|---|
| W (work) | The deliverable is missing or wrong and the criterion is valid. | Add a task with the next free number in the phase, its Task cell starting `(R: <criterion id>)`. One concern, at most 3 files, Done-when is the failed command verbatim. Run it with `/plan-task.md`. | agent |
| C (criterion) | The criterion cannot tell pass from fail, or checks the wrong thing, and the evidence shows the work meets the row's stated intent. | Amend the row's Done-when in this file and log a PROGRESS `REVIEW` line naming the criterion and why. | human |
| T (tool) | A checking tool reports a false failure. | Add a remediation task as for W that fixes the tool. | agent |
| B (backlog) | Valid failure, severity below Block, human chooses to defer. | Backlog row with an ID, the criterion id and a target phase. | human |

Rules:
1. A Block-severity failure cannot be class B.
2. Never reword a criterion after seeing a result unless the failure is class C, with the reason logged.
3. Remediation tasks follow the normal task rules: START first, one commit, no scope expansion. Out-of-scope findings go to the backlog.
4. After remediation, run the reviewer once more in delta form: the same prompt, with exit criteria limited to the failed ones plus the step 2 cross-checks, and `git diff <review-commit>..HEAD --stat` attached.
5. Log each round in PROGRESS: `REVIEW | phase N round K: FAIL <ids> | PASS | PASS-with-backlog <B-IDs>`.
6. At most two remediation rounds. A third failure of the same criterion stops the phase: the human decides whether to change the design, the criterion or the scope, and logs it.
7. A phase is closed only when its last round is PASS or PASS-with-backlog. Nothing in the next phase starts before that.

## 9. TARGET_STATE amendments (human applies; the file is human-owned)

- **E1:** "DEV runs locally and the agent environment holds no PRD credentials. DEV and TEST databases carry a database-level marker (`pifitness.env=dev` or `test`); tools refuse to run without it, and the backend refuses to start when its `APP_ENV` and the marker disagree."
- **E3:** "Bootstrap is a minimal stub that fetches the ref being deployed (default `main`) and runs that ref's deployment scripts, showing what will change before applying."
- **E6 (new):** "`main` is the deployable trunk. Agents work on `agent/*` branches and never push or merge; the human merges, tags `prd-YYYYMMDD` and deploys (`docs/workflow/GIT_MODEL.md`)."
- **E7 (new):** "`test-up <ref>` and `test-down` run a branch on the Pi against a copy of the PRD database while PRD is stopped and untouched, and have been run end to end."
- **E8 (new):** "Until Phase 5.1 replaces bootstrap's branch fetch, `origin/react-ui` is frozen at the last deployed commit (`failsafe-react-ui`). `main` is the trunk; an accidental bootstrap from the frozen `react-ui` is a no-op redeploy of current PRD, not a jump to untested code. To deploy a `main`-based release before 5.1, the human first runs `git push origin main:react-ui`. From 5.1 onward, bootstrap fetches the ref being deployed (D16) and the freeze ends (D19)."
- **W10 (new):** "Every phase closes with an independent reviewer pack run; its failures are fixed or backlogged."
- **V1, V3, V4:** "passes against the recorded baselines in `tools/baselines/`; any new violation fails; baselines only shrink." V3's baseline must reach zero by task 2.8. V4: remove N+1 from the scripted checks (review item).
- **W1:** replace "Force is human-only" with "Forced closes are logged as FORCED and listed in every reviewer pack."
- **W2:** add "except script-applied mechanical batches of up to 10 files with a deterministic check."
- **D3:** "DDL changes only through numbered migration files in `deploy/migrations/`, additive and backward compatible with the last released code. The tool applies them to DEV; the human applies them to PRD after a backup and a clean run on the TEST copy; the DEV reset replays pending migrations."
- **R2/R3 checks:** name `tools/check_docs.py`.

---

## Appendix A: Ratchet spec

Key format: `check|path|rule-or-symbol|8-char hash of the normalized message`. Never include line numbers. One baseline file per check, sorted, one key per line. Exit codes: 0 no new violations; 1 new violations; stale baseline keys print `PRUNE` but do not fail. `--update-baseline` removes stale keys only; `--allow-grow` is for the human. Test failures: node id lines in `tests_quarantine.txt` with a backlog ID in a trailing field.

## Appendix B: PROGRESS.md

Header: `date | task | model | event | evidence`. Events: `START`, `DONE`, `DONE (deferred verification)`, `DONE (human review)`, `BLOCKED`, `FORCED`, `REVIEW`. `DONE (deferred verification)` is for a Done-when whose tool does not exist yet: record the attempted command and its not-found output. `DONE (human review)` means artifacts are produced and await the human. `REVIEW` is the human's acceptance. `DONE` evidence is the command and the last 20 output lines (indented under the row). Append only.

## Appendix C: `.clinerules/interim-rules.md` (1 KB or less)

```
# Interim rules (merged into default-rules.md in Phase 7)
1. One task per session. Read only your task row in docs/workflow/PLAN.md. Touch only its listed files.
2. Loop-breaker: same failure twice with no new information -> append BLOCKED to docs/workflow/PROGRESS.md, commit that file only, and stop.
3. Flag, do not expand: out-of-scope findings go to docs/workflow/BACKLOG.md as the next ID. Never fix them.
4. Done = run the verify command; paste command and last 20 lines into PROGRESS.md.
5. Git: see default-rules.md. Commit once per task on the current agent/phase-* branch; never create or switch branches. Start from a clean tree or stop and ask.
6. Never ssh, scp, rsync or connect to any database host other than localhost.
7. Comments say what and how. No task, bug or feature IDs.
8. Plan tasks do not use skills or /feature-* workflows.
```

## Appendix D: Pi security walkthrough (human, do in order)

Why: Postgres listens on all interfaces and admits the whole LAN. It is closed from the internet, so this is defense in depth. Do it at a quiet time (the app drops for a few seconds). Keep a second SSH session open throughout.

# Status : Done
- Steps 1 through 4 completed and verified.

**Step 1: add the SSH tunnel to DataGrip first, so you keep access.** In DataGrip open the PRD data source, go to the SSH/SSL tab, tick "Use SSH tunnel", add an SSH configuration (host `192.168.86.104`, port 22, user `god`, authentication by key pair, your passphrase-protected key). On the General tab set Host to `localhost`, Port `5432` (this is the Pi's localhost, through the tunnel). Test Connection. Do not continue until it succeeds.

**Step 2: back up the config (on the Pi).**
```
PGV=$(ls /etc/postgresql); echo $PGV
sudo cp /etc/postgresql/$PGV/main/postgresql.conf ~/postgresql.conf.bak
sudo cp /etc/postgresql/$PGV/main/pg_hba.conf ~/pg_hba.conf.bak
```
`echo` must print a single version number. If it prints more than one, stop.

**Step 3: bind to localhost and drop the LAN rule.**
```
sudo sed -i "s/^listen_addresses = .*/listen_addresses = 'localhost'/" /etc/postgresql/$PGV/main/postgresql.conf
sudo sed -i '/192.168.86.0\/24/d' /etc/postgresql/$PGV/main/pg_hba.conf
grep listen_addresses /etc/postgresql/$PGV/main/postgresql.conf
sudo grep 192.168 /etc/postgresql/$PGV/main/pg_hba.conf
```
The first grep must show `'localhost'`; the second must print nothing.

**Step 4: restart and verify.**
```
sudo systemctl restart postgresql
sudo systemctl restart pifitness-fastapi
sudo ss -ltnp | grep 5432
```
`ss` must show only `127.0.0.1` and `::1`. Then load the app on your phone and connect with DataGrip through the tunnel.

**Rollback:** `sudo cp ~/postgresql.conf.bak /etc/postgresql/$PGV/main/postgresql.conf`, the same for `pg_hba.conf`, then restart both services.

**Not now:** uvicorn (8000) and Next.js (3000) also listen on all interfaces (B28). They are fixed in the repo's deploy files in Phase 5, not by hand, because the installed unit has already drifted from the repo (B29). A firewall is optional once everything but nginx binds to loopback.

**Dev machine:** Cline has no auto-approve for terminal commands, so each command needs your approval; deny any `ssh`, `scp` or `psql` to a non-local host. Do not leave the SSH key loaded in an unlocked agent while Cline runs, or load it with `ssh-add -c`.
