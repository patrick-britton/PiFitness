# PiFitness Reorg — PLAN.md

Generated from `repo_audit baseline` + `all` (2026-10-25, commit `fcdcb14`).
Baseline frozen at `.audit/baseline.json`. Full report at `.audit/REPORT.md`.

Scope: reorganize from technical-layer layout to domain-sliced layout, remove dead weight,
make structure enforceable by tooling. No runtime behavior changes. No new features.
No dependency upgrades. No DB object renames.

## Baseline metrics (from audit scoreboard)

| metric | value | target |
|---|---:|---:|
| hygiene.secret_hits | 1 | 0 (Phase 0) |
| hygiene.tracked_junk | 8 | 0 (Phase 0) |
| hygiene.pyc_husks | 24 | 0 (Phase 0) |
| hygiene.sensitive_exposed | 0 | 0 |
| inventory.clutter_files | 15 | 0 (Phase 0) |
| inventory.npm_manifests | 2 | 1 (Phase 4) |
| py.dangling_imports | 0 | 0 |
| py.layer_violations | 1 | 0 (Phase 1) |
| py.module_cycles | 0 | 0 |
| py.private_cross_imports | 16 | 0 (Phase 1/3) |
| py.router_direct_db_handlers | 11 | 0 (Phase 3) |
| py.nonliteral_dynamic_imports | 6 | pinned in audit.toml (Phase 1) |
| py.orphans_likely_dead | 0 | 0 |
| py.orphans_total | 6 | keep/pin decision (Phase 1) |
| py.near_duplicate_pairs | 1 | 0 after Garmin decision (Phase 1) |
| domains.unassigned_files | 38 | 0 after DECISIONS.md (Phase 2) |
| domains.cross_edges | 26 | reduced (Phase 3) |
| scripts.oneoff | 0 | 0 |
| scripts.misplaced_tests | 5 | 0 (Phase 2) |
| scripts.keep | 6 | moved to tools/ (Phase 2) |
| ts.unreachable_files | 4 | 0 after approvals (Phase 4) |
| ts.unresolved_imports | 1 | 0 (Phase 4) |
| ts.npm_unused | 5 | 0 after approvals (Phase 4) |
| contracts.frontend_ghost_paths | 1 | investigate (Phase 4) |
| git.avg_code_dirs_per_commit | 2.52 | expected to fall |
| git.avg_domains_per_commit | 1.69 | expected to fall |

Repo size: 372 files, 96,066 lines (76,590 code). Python 126 files, TS 180 files.

## Phase 0 — Safety net and hygiene (branch `reorg/00-safety`; current branch)

No source-file moves. Only cleanup + audit tooling.

Tasks:
- 0.1 [safe] Commit audit tool + `audit.toml`; gitignore `.audit/*` with `!.audit/baseline.json`; commit baseline.
- 0.2 [safe] Handle every HYG-001 item: untrack + gitignore tracked secrets. Report HYG-003/004 to human; do not open files.
  - HYG-004 evidence: `backend_functions/database_functions.py:50` [url-with-password] — do NOT open; report path only.
- 0.3 [safe] `.gitignore` additions for HYG-005/006 and INV-003 items (logs, `*.out`, `*.tsbuildinfo`, `.next`, caches, `local_storage/`, dumps, tokens). `git rm --cached` anything already tracked (8 tracked junk files listed in hygiene.json).
- 0.4 [needs-approval] Delete confirmed junk: root `_task07*.txt`, `vitest-t11.log`, other INV-003 items (15 clutter files listed in inventory.json), pyc husks (HYG-007).
  - Diff before deleting duplicates: `.codes/…/010-006_design.md` vs `.features/designs_active/010-006_design.md`; `"004-006 copy.md"` vs `004-006.md`; `.features/zz_workfow_archive/` vs `.clinerules/workflows/` (byte-compare via INV-002).
- 0.5 [needs-approval] Streamlit residue (HYG-008): remove `.streamlit/`; `scripts/map_streamlit_to_backend.py` only if `impact` = 0; move `memory-bank/streamlit_*` docs to `docs/archive/`; propose (do not run) venv uninstall + requirements edit.
- 0.6 [safe] Replace `scripts/regenerate-audit.ps1` with a 3-line wrapper around `repo_audit all --with-external`; delete superseded `.audit/` outputs (tree.txt, tree-filtered.txt, js-imports.txt, knip-report.txt, culler-report.txt).

Exit: `hygiene.sensitive_exposed = 0`, `hygiene.tracked_junk = 0`, `hygiene.pyc_husks = 0`,
`inventory.clutter_files = 0`, `inventory.stray_path_files = 0`, tests green.

Estimated commits: 3–4.

## Phase 1 — Fix boundaries in place (branch `reorg/01-boundaries`; behavior-preserving code edits)

Tasks:
- 1.1 [blocked-on-decision] Resolve dangling `backend.config` import (PY-001). Audit shows `py.dangling_imports = 0` but hypothesis H1 says `backend_functions/config.py` imports `backend.config` with no `backend/config.py` (only stale `.pyc`). Use `git log --diff-filter=D -- backend/config.py` to learn what happened. If ambiguous → DECISIONS.md, stop.
- 1.2 [safe] Remove `fastapi` from executioner/library layer (PY-004): `backend_functions/ultimate_task_executioner_v2.py:8` imports `<fastapi>`. Replace `HTTPException` etc. with plain domain exceptions; translate in routers.
- 1.3 [safe] Extract router-resident logic (PY-005/PY-006/PY-007): for each private helper, create a public function in `backend_functions/<domain>_service.py`; routers and callers (scripts/tests) import it. Move threading/orchestration out of routers.
  - PY-005: 16 private cross-imports (food_search `_quality_label` etc., admin `_failed_run_message`, activities `_run_step`/`_task_failure_message`).
  - PY-006: 11 route handlers calling DB directly (admin.py, music.py, segments.py, main.py).
- 1.4 [safe] Write `docs/reorg/DYNAMIC_TARGETS.md` (real `importlib` targets from PY-010): 6 non-literal dynamic imports in `service_logins.py`, `task_execution.py`, `ultimate_task_executioner_v2.py`. Add them as pinned in `audit.toml`.
- 1.5 [complete] Garmin: vendored copies deleted; imports repointed to `pirate-garmin_clone` library; `json_extractors.py` and `service_logins.py` updated; `pytest.ini` pythonpath added.
  - PY-013: `backend_functions/pirate_garmin_endpoints.py` ~ `pirate-garmin_clone/src/pirate_garmin/endpoints.py` (0.93 similarity).
- 1.6 [complete] Deleted 6 legacy orphan modules: `admin_functions.py`, `running_functions.py`, `admin_schemas.py`, `data_helpers.py`, `db_wrapper.py`, `image_creation.py`. All had zero code importers.
- 1.7 [safe] Enable all current `[[forbid]]` rules in `audit.toml`.

Exit: `py.dangling_imports = 0`, `py.pkg_cycles = 0`, `py.layer_violations = 0`,
`py.private_cross_imports = 0`, `py.router_direct_db_handlers` reduced, OpenAPI snapshot identical, tests green.

Estimated commits: 5–7.

## Phase 2 — Mechanical moves that do not change Python import paths (branch `reorg/02-moves`)

Tasks:
- 2.0 [safe] Pre-flight: prove/restore bootstrap manifests required by `deploy_react.sh`. `requirements-react.txt` and `npm_requirements.txt` were restored to `deploy/scripts/` from commit `bb90e2c^` after being inadvertently removed during reorganization. Verify both files exist at the paths `deploy_react.sh` references (lines 54–55, 112, 117–122) and that `bash -n deploy_react.sh` passes. Do not proceed to 2.4/2.5 until this gate passes.
- 2.1 [needs-approval] `db/`: No migration files on disk (`sql.migrations = 0`). Any future SQL scripts land in `.features/migration_ddl/` (outside git) and are applied one-by-one by humans on PRD/DEV. No `sql.proposed_order`; no global ordering; no `db/migrations/` move.
  - Audit shows `sql.files = 0`, `sql.migrations = 0` — no migration files on disk.
- 2.2 [safe] `scripts/` triage per approved list: durable tools → `tools/{db,diagnostics,audit,features,codemod}/`; approved one-offs → delete; test-like scripts → `backend/tests/integration/` only if trivial else delete; misplaced `test_*.py` → `tests/`. Update `pytest.ini`, `.clinerules`, docs.
  - keep-tool (6): `schema_cache.py`, `sync-prod-to-dev.ps1`, `validate_pydantic_models.py`, `validate_queries.py`, `validate_sql.py`, `regenerate-audit.ps1`
  - misplaced-test (5): `test_db_connectivity.py`, `test_dotenv.py`, `test_garmin_login.py`, `test_logins.py`, `test_queries.py`
  - review (6, human decision): `audit_db_investigation.py`, `audit_view_columns.py`, `document_stored_procedures.py`, `generate_data_flow_diagrams.py`, `generate_schema_documentation.py`, `smoke_test.py`
- 2.3 [needs-approval] Docs: `.features/designs_complete` → `docs/features/done/`; `designs_active` + `descriptions` → `docs/features/active/`; `memory-bank/` → `docs/` (hand-written) and `docs/generated/` (reports; gitignore or regenerate). **Update `.clinerules/workflows/*`, `.agents/skills/*`, `local_clinerules.txt`, and `tools/features/fd.py` path constants in the same commit.** Skip if human chose to keep `.features/`.
- 2.4 [safe] Merge `deploy/` + `deployment/` → `deploy/{systemd,nginx,scripts}`; update units/bootstrap in same commit; append to `CUTOVER.md`.
- 2.5 [needs-approval] Consolidate dependency manifests per DECISIONS.md §13 (two categories, not one file):
  - Canonical Python source: `backend/pyproject.toml`; bootstrap copy: `deploy/scripts/library_requirements.txt` (read by `pi5_deploy.sh:152`).
  - Canonical npm source: `frontend/pifitness/package.json`; bootstrap copy: `deploy/scripts/npm_requirements.txt` (read by `deploy_react.sh` lines 117–122).
  - Merge `deploy/scripts/requirements-react.txt` into `library_requirements.txt`, then delete; repoint `deploy_react.sh:54` (`REQUIREMENTS_FILE`) to `library_requirements.txt`.
  - Delete root `npm_requirements.txt` only after proof; repoint `pi5_deploy.sh:183` (`../../npm_requirements.txt`) to `deploy/scripts/npm_requirements.txt`.
  - `setup.py` replaced by `backend/pyproject.toml` in Phase 3.0.
  - Proof gate before any deletion: `pip install -e backend` + `pytest -q` green; `npm ci` + `next build` in `frontend/pifitness/` green; `bash -n` on both shell scripts after repointing.

Exit: `scripts.oneoff = 0`, `scripts.misplaced_tests = 0`,
`inventory.duplicate_groups = 0`; `impact` on every old path = 0 references.

Estimated commits: 6–9.

## Phase 3 — Python package by domain (branch per slice: `reorg/03-<domain>`)

Tasks:
- 3.0 [safe] Create `backend/pyproject.toml` (package `pifitness` at `backend/pifitness/`), move dependency declarations into it, update `pytest.ini` (`pythonpath`), reinstall editable. **No compatibility shims at end of phase.**
- 3.1 [safe per slice, needs-approval for deletions] Slice order: `health` → `exercises` → `volleyball` → `admin` → `food` → `music` → `integrations` → `activities` → `worker` → `core` → `api` wiring. For each slice build `moves.json` from `domains.assignment` (router → `router.py`, queries → `queries.py`, schemas → `schemas.py`, functions → `service.py` or named modules, tests → `backend/tests/<domain>/`), run `impact`, run codemod (`tools/codemod/move_python.py`), run gates, commit.
- 3.2 [safe] Systemd units and entrypoints (`uvicorn pifitness.api.main:app`, worker entrypoint) change in the slice that moves them; update `deploy/` and `CUTOVER.md` in that commit.
- 3.3 [safe] Final slice: enable TARGET `[[forbid]]` rules, disable old ones, update `[domains]` keywords.
- 3.4 [safe] Clean-room check: new temp venv outside repo → `pip install -e backend` → `pytest -q` → import the app.

Exit: OpenAPI snapshot identical to `openapi.before.json`; `py.layer_violations = 0` under TARGET rules;
`py.module_cycles = 0`; `py.router_direct_db_handlers = 0`; no `backend_functions`, `frontend_functions`, `agents` dirs.

Estimated commits: 10–14 (one per slice + wiring).

## Phase 4 — Frontend (branch `reorg/04-frontend`, slices `reorg/04-<domain>`)

Tasks:
- 4.1 [needs-approval] Flatten `frontend/pifitness` → `frontend`; update every reference (`deploy/`, nginx-react.conf, scripts, `.clineignore`, docs, `package.json` scripts). Delete root `package.json`/lockfile/`node_modules` only after `ts` audit (TS-009) and clean `npm ci` + build prove unused + human approved.
  - TS-009: root `package.json` duplicates 17 deps of frontend manifest.
- 4.2 [safe] Introduce `features/<domain>/` per `domains.assignment`: move route-local components, `hooks/use<Domain>.ts`, `lib/types/<domain>.ts`, single-consumer `lib/*` (TS-007) into their feature; keep `app/<route>/page.tsx` thin; move `app/components` → `components/`. Eliminate route → route imports (TS-006: `app/beachchanger -> app/activities`).
- 4.3 [needs-approval] Only if approved: generate API types from FastAPI OpenAPI schema (`openapi-typescript`) into `src/lib/api/generated.ts`; replace hand-written `lib/types/*` incrementally.
- 4.4 [needs-approval] Remove modules import graph proves unreachable (TS-001: 4 unreachable files — `DBSessions.tsx`, `index.ts`, `MapView.tsx`, `useViewport.ts`) with approval; remove unused npm packages (TS-004: 5 unused) only if approved (lockfile changes need sign-off).

Exit: `ts.unresolved_imports = 0`, `ts.cross_route_imports = 0`, `ts.npm_undeclared = 0`,
`ts.unreachable_files` ≤ baseline (target 0 after approvals), `inventory.npm_manifests = 1`,
`tsc --noEmit`, `vitest run`, `next build` green.

Estimated commits: 8–12.

## Phase 5 — Guardrails and wrap-up (branch `reorg/05-guardrails`)

Tasks:
- 5.1 [safe] Add ESLint boundaries rule (or `no-restricted-imports`) so `features/*` import only from `components/`, `lib/`, and their own folder — only if no new dependency; otherwise list as recommendation.
- 5.2 [needs-approval] Add `import-linter` contracts mirroring TARGET rules only if approved (new dev dependency); otherwise keep `repo_audit check` as gate and document in `README.md`.
- 5.3 [propose] Wire `repo_audit check` into PR/pre-commit flow (`.clinerules/hooks` or git hook; propose, don't force).
- 5.4 [safe] Write `docs/ARCHITECTURE.md` (layout, dependency rules, how to add a domain, how to run the audit), update `README.md`, generate `docs/reorg/FINAL_REPORT.md` from `repo_audit diff` plus git co-change baseline.

Exit: all gated metrics at targets; baseline refreshed (`repo_audit baseline`) and committed.

Estimated commits: 2–3.

## Risks and rollback

- **Rollback for every phase:** discard the branch. `git mv` preserves history so reverting a branch is clean.
- **Stale editable installs:** `__editable__.pifitness-*` finders can keep resolving old module names after a rename; clean-room check (3.4) is mandatory.
- **Windows case-insensitivity:** case-only renames need an intermediate name. Long paths under `.features/` may hit limits.
- **Dynamic loading:** `importlib` targets invisible to static analysis; pinned modules must not move without `DYNAMIC_TARGETS.md` entry.
- **uvicorn/systemd:** reference module paths as strings; `impact` finds them only if in repo. Pi-external config goes in `CUTOVER.md`.
- **Generated artifacts:** `memory-bank/*report*.json`, `data_flow_diagrams.*`, `.audit/*` need regeneration command documented before deletion/gitignore.

Gate commands (after every commit, per §8): `compileall`, `pytest -q`, OpenAPI snapshot diff vs `openapi.before.json`, `repo_audit all` + `check` (exit 0), `tsc --noEmit`, `vitest run`, `next build`, `git status --short`.