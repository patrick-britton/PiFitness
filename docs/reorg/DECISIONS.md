# PiFitness Reorg — DECISIONS.md

Open questions with recommendations and defaults. Default = what I will do if the human says "use defaults".

---

## 1. Tracked secrets (HYG-001/HYG-002/HYG-003/HYG-004)

Sensitive files present on disk (all git-ignored, `sensitive_exposed = 0`, `sensitive_in_history = 0`):
- `.spotify_cache` (878 bytes, ignored)
- `backend/.env` (286 bytes, ignored)
- `garmin_tokens.json` (241 bytes, ignored)
- `oauth1_token.json` (205 bytes, ignored)
- `prod-latest.dump` (~1.26 GB, ignored)
- `prod-sanitized.dump` (~1.26 GB, ignored)
- `prod-schema-reference.dump` (~1.15 GB, ignored)

**Recommendation:** Keep gitignored; add to `.gitignore` if missing. Rotation is the human's job — I never open these files.
**Secret hit in source (HYG-004, do not open):** `backend_functions/database_functions.py:50` [url-with-password]. Report path only; human must rotate credentials.

**Default:** Untrack + gitignore any tracked secrets; report HYG-003/004 paths in this file; do not open.

---

## 2. Garmin duplicate (PY-013) — RESOLVED

Near-duplicate pair (0.93 similarity):
- `backend_functions/pirate_garmin_endpoints.py` → DELETED
- `pirate-garmin_clone/src/pirate_garmin/endpoints.py` → KEPT (canonical)

**Actions taken:**
- `backend_functions/json_extractors.py` line 485: repointed import from `backend_functions.pirate_garmin_endpoints` to `pirate_garmin.endpoints`
- `backend_functions/service_logins.py` line 739: repointed imports from `backend_functions.pirate_garmin_auth` to `pirate_garmin.auth` (GarminAuthError, MissingCredentialsError) and `pirate_garmin.client` (GarminClient)
- Added `pythonpath = pirate-garmin_clone/src` to `pytest.ini`
- `git rm` deleted `backend_functions/pirate_garmin_endpoints.py` and `backend_functions/pirate_garmin_auth.py`
- All 210 tests pass; gate OK

Diff size: not computed (INV-002 byte-compare not run).

## 3. Orphan modules (PY-009) — RESOLVED: all 6 deleted

`py.orphans_total = 6` (0 likely-dead, 3 dynamic-possible, 3 string-referenced).
All 6 had zero code importers — references existed only in documentation,
diagram labels (`.features/designs_complete/*.md`), and streamlit mapping
docs (`docs/archive/streamlit_backend_mapping.json`). Streamlit is legacy
(HYG-008) and no longer part of the app.

**Actions taken (all `git rm`):**
- `backend_functions/admin_functions.py` (81 loc) — `get_runtime_status()` systemctl checker; no code importers
- `backend_functions/running_functions.py` (282 loc) — `leaderboard_update()` SQL generator; no code importers
- `backend_functions/schemas/admin_schemas.py` (215 loc) — Pydantic admin models; models are redefined inline in `backend/api/admin.py`
- `backend_functions/data_helpers.py` (756 loc) — pandas-replacement helpers; no code importers
- `backend_functions/db_wrapper.py` (103 loc) — async DB wrappers; no code importers (self-docstring reference only)
- `backend_functions/image_creation.py` (390 loc) — SVG chart rendering with pandas; no code importers

All 210 tests pass; gate OK. `py.orphans_total` reduced to 0.

**Default:** All 6 deleted with human approval (this decision recorded).

---

## 4. Real targets of every `importlib` loader (PY-010, 6 non-literal dynamic imports)

Locations (prefix='' in all cases):
- `backend_functions/service_logins.py:720`
- `backend_functions/task_execution.py:108`
- `backend_functions/task_execution.py:146`
- `backend_functions/ultimate_task_executioner_v2.py:137`
- `backend_functions/ultimate_task_executioner_v2.py:275`
- `backend_functions/ultimate_task_executioner_v2.py:453`

**Recommendation:** Read these call sites + any task seed SQL/JSON (no DB access). List real module targets in `DYNAMIC_TARGETS.md` and pin them in `audit.toml`. Do not move pinned modules without updating the registry wherever the name is stored.
**Default:** Create `DYNAMIC_TARGETS.md` listing the 6 call sites; pin discovered targets; verify `py.nonliteral_dynamic_imports` is accounted for (not loosened).

## 5. Scripts triage (17 total scripts)

Keep-tool → `tools/{db,diagnostics,audit,features,codemod}/` (6):
- `scripts/schema_cache.py` (72 loc)
- `scripts/sync-prod-to-dev.ps1` (286 loc)
- `scripts/validate_pydantic_models.py` (249 loc)
- `scripts/validate_queries.py` (174 loc)
- `scripts/validate_sql.py` (86 loc)
- `scripts/regenerate-audit.ps1` (375 loc) → replace with 3-line wrapper around `repo_audit all --with-external`

Misplaced-test → `backend/tests/integration/` only if trivial, else delete (5):
- `scripts/test_db_connectivity.py` (177 loc)
- `scripts/test_dotenv.py` (33 loc)
- `scripts/test_garmin_login.py` (70 loc)
- `scripts/test_logins.py` (639 loc)
- `scripts/test_queries.py` (410 loc)

Review → human decision (6):
- `scripts/audit_db_investigation.py` (182 loc)
- `scripts/audit_view_columns.py` (13 loc)
- `scripts/document_stored_procedures.py` (304 loc)
- `scripts/generate_data_flow_diagrams.py` (344 loc)
- `scripts/generate_schema_documentation.py` (270 loc)
- `scripts/smoke_test.py` (39 loc)

**Default:** Move keep-tool to `tools/`; move trivial misplaced-tests to `tests/` else delete; leave review scripts in place pending human decision; record all in PLAN.md.

Phase 3 Debt:
- `tests/integration/test_dotenv.py` deleted (Phase 3). Phase 2.2 moved it from `scripts/test_dotenv.py` without updating its from backend_functions.database_functions import .... Phase 2.2 policy was "move only if trivial, else delete." Content tests only that backend/.env exists, not application behavior. No test imports it. Replaced by nothing — startup .env validation, if desired, belongs in pifitness.core.config (a future slice), not a test.

---

## 6. `domains.unassigned` files (38 files, DOM-001) — proposed domain or `core`

Python:
- `backend/api/auth.py` → **core** (auth is cross-cutting)
- `backend/main.py` → **core** (app wiring)
- `deployment/pathfinder.py` → **tools/diagnostics/**

Frontend (unassigned = shared/layout, not a domain):
- `frontend/pifitness/next-env.d.ts` → **core** (Next.js generated, leave in place)
- `frontend/pifitness/src/app/components/{DebugPanel,Header,Layout,MapView,NavBar,Sidebar,ThemeToggle}.tsx` → **components/** (shared UI)
- `frontend/pifitness/src/app/layout.tsx` → **app/** (root layout)
- `frontend/pifitness/src/app/page.tsx` → **app/** (root page)
- `frontend/pifitness/src/app/providers.tsx` → **app/** (providers)
- `frontend/pifitness/src/hooks/useAuth.ts` → **core** (auth hook)
- `frontend/pifitness/src/hooks/useViewport.ts` → **lib/** (shared)
- `frontend/pifitness/src/lib/__tests__{date-format,map-styles,step-estimates}.test.ts` → **lib/__tests__/**
- `frontend/pifitness/src/lib/stores/{uiStore,viewportStore}.ts` → **lib/** (shared stores)
- `frontend/pifitness/src/test/setup.ts` → **test setup**

Tests:
- `tests/test_empty_queue_noop.py` → **core** (worker/core)
- `tests/test_failure_propagation.py` → **activities**
- `tests/test_process_generator_events.py` → **activities**

Other:
- `setup.py` → **core** (replaced by `backend/pyproject.toml` in Phase 3)
- `validate_endpoints.py` → **tools/diagnostics/**

**Default:** Map as above; any unassigned file without a clear home goes to `core` or `components/` (frontend shared).

## 7. `.features/`, `memory-bank/`, `.clinerules/workflows` path references

`.clinerules/workflows/*` and `tools/features/fd.py` encode `.features/...` paths. Moving docs without updating them silently breaks Cline workflows.

**Recommendation:** In Phase 2.3, update all `.features/` path constants in `.clinerules/workflows/*`, `.agents/skills/*`, `local_clinerules.txt`, and `tools/features/fd.py` in the same commit as the docs move. **OR** keep `.features/` in place and only move docs that aren't referenced.
**Default:** Keep `.features/` paths intact; move only docs not referenced by workflows; flag any workflow breakage as a blocker.

---

## 8. Meaning of the three `npm_requirements.txt` files, `compose.yaml`, `validate_endpoints.py`, `setup.py`

- `npm_requirements.txt` (root), `deploy/scripts/npm_requirements.txt`, `frontend/npm_requirements.txt` — three manifests; TS-009 says root duplicates 17 deps of frontend manifest. Hypothesis H9: root manifest is a stale install.
- `compose.yaml` (root, 15 loose files) — Docker compose; deployment-related.
- `validate_endpoints.py` (root) — unassigned; proposed `tools/diagnostics/`.
- `setup.py` (root) — Python packaging; to be replaced by `backend/pyproject.toml` in Phase 3.0.

**Default:** Root `npm_requirements.txt` + `package.json`/lockfile/`node_modules` are candidates for deletion in Phase 4.1 after proof (clean `npm ci` + build in `frontend/`) + human approval. `setup.py` replaced by `backend/pyproject.toml`. `compose.yaml` stays at root (deployment). `validate_endpoints.py` → `tools/diagnostics/`.

---

## 9. Add `openapi-typescript` for generated API types? (new devDependency)

**Recommendation:** Only if approved — it's a new dependency. Generate into `src/lib/api/generated.ts`, replace hand-written `lib/types/*` incrementally domain by domain. Otherwise keep hand-written types.
**Default:** Do NOT add; list as a recommendation in Phase 4.3.

---

## 10. Root `package.json` + `node_modules`: delete after proof, or keep?

**Recommendation:** Delete after TS-009 proof (root manifest duplicates frontend manifest; clean `npm ci` + build in `frontend/` proves unused) + human approval.
**Default:** Propose deletion in Phase 4.1; do not execute without explicit approval.

---

## 11. Dangling `backend.config` import (PY-001, hypothesis H1) — RESOLVED: no dangling import

`git log --diff-filter=D -- backend/config.py` shows commit `37df43e` moved `backend/config.py` → `backend_functions/config.py` ("Fix inversion 3B: move backend.config to backend_functions.config to cut one cycle edge"). Reading `backend_functions/config.py` confirms it does NOT import `backend.config` — the docstring line 9 (`from backend.config import get_settings`) is just an outdated usage example, not an actual import. Actual imports are `os`, `platform`, `dataclasses`, `typing`, `dotenv`.

`py.dangling_imports = 0` confirms no dangling import exists.

**Decision:** No code change needed for the import itself. Optionally update the docstring usage example to `from backend_functions.config import get_settings` as a documentation fix (does not affect behavior or metrics).

---


## 12. Deployment scripts and requirements manifests — KEEP, not delete
**Decision (final, per human review):** The deployment scripts under `deploy/scripts/` are required for installing Python and npm packages on the Pi during deployment. They are **NOT** candidates for deletion.

| File | Action | Rationale / usage |
|---|---|---|
| `deploy/scripts/pi5_deploy.sh` | **KEEP** | Unified deployment script for `streamlit-prd` and `react-ui` branches. References `deploy/scripts/library_requirements.txt`, `deploy/nginx/nginx-template.conf`, `deploy/systemd/pifitness-fastapi.service`, `deploy/systemd/pifitness-hw-health.service`, `deploy/systemd/pifitness_agent.service`, `frontend/pifitness`, `npm_requirements.txt`. 20 refs in repo (audit); documented in `CUTOVER.md`; invoked from `frontend/DEPLOYMENT_GUIDE.md`. |
| `deploy/scripts/bootstrap.sh` | **KEEP** | Bootstrap launcher for `react-ui`. Validates environment, fetches deployment scripts from origin, then invokes `deploy_react.sh`. Documented in `CUTOVER.md`. |
| `deploy/scripts/deploy_react.sh` | **KEEP** | React UI branch deployment script. Called by `bootstrap.sh`. Requires `deploy/scripts/requirements-react.txt` and `deploy/scripts/npm_requirements.txt` for package install. |
| `deploy/scripts/npm_requirements.txt` | **KEEP** | Global npm package manifest used by both `pi5_deploy.sh` and `deploy_react.sh`. Restored from commit `bb90e2c^` after being inadvertently removed during reorganization. |
| `deploy/scripts/library_requirements.txt` | **KEEP** | Python dependency manifest used by `pi5_deploy.sh`. |
| `deploy/scripts/requirements-react.txt` | **KEEP** | Python requirements manifest (React UI frontend) used by `deploy_react.sh`. Restored from commit `bb90e2c^` after being inadvertently removed during reorganization. |

**Relationship:** `bootstrap.sh` (launcher) → calls → `deploy_react.sh` (React worker). `pi5_deploy.sh` is a standalone unified script supporting both `streamlit-prd` and `react-ui` branches; it does **not** call `deploy_react.sh`. They are parallel entry points with overlapping react-ui logic.

**Evidence from `deploy_react.sh` content:** contains `REQUIREMENTS_FILE="requirements-react.txt"`, `NPM_REQUIREMENTS_FILE="npm_requirements.txt"`, `pip install -r "$PROJECT_DIR/deploy/scripts/$REQUIREMENTS_FILE"`, `done < "$PROJECT_DIR/deploy/scripts/$NPM_REQUIREMENTS_FILE"` (npm packages). Requires both requirements manifests.

**Note on `deployment/pi5_deploy.sh`:** that path does not exist. Per `CUTOVER.md`, deployment orchestration was consolidated from `deployment/` → `deploy/`; `deployment/` is now a historical snapshot containing only `agent/` and `health/` subdirs. The active file is `deploy/scripts/pi5_deploy.sh`.

**Bottom line:** these files are NOT "junk" — they are the bootstrap that ensures new packages are installed on the Pi correctly. "delete after proof" applies only to files with no dependencies or usage, not to active deployment scripts.

---

## Defaults summary (if human says "use defaults")

| Area | Default action |
|---|---|
| Secrets | Untrack + gitignore; report paths; rotate externally |
| Garmin | `pirate-garmin_clone` canonical; vendored copies deleted; imports repointed |
| Orphans | All 6 deleted (admin_functions, running_functions, admin_schemas, data_helpers, db_wrapper, image_creation) |
| importlib targets | List in DYNAMIC_TARGETS.md; pin in audit.toml |
| Scripts | keep-tool → tools/; misplaced-tests → tests/ or delete; review → pending |
| unassigned | Map per §6; unclear → core / components/ |
| `.features/` paths | Keep intact; don't break workflows |
| Manifest consolidation | Python: canonical `backend/pyproject.toml`, bootstrap copy `deploy/scripts/library_requirements.txt`. npm: canonical `frontend/pifitness/package.json`, bootstrap copy `deploy/scripts/npm_requirements.txt`. Two categories, not one file. |
| npm manifests | Delete root `npm_requirements.txt` after proof + approval; keep `deploy/scripts/npm_requirements.txt` |
| openapi-typescript | Do not add (recommendation only) |
| Root package.json/node_modules | Delete after proof + approval |
| PY-001 dangling import | Check git log; stop if ambiguous |
| Deployment scripts & requirements manifests | KEEP (`deploy/scripts/pi5_deploy.sh`, `deploy/scripts/bootstrap.sh`, `deploy/scripts/deploy_react.sh`, `deploy/scripts/library_requirements.txt`, `deploy/scripts/npm_requirements.txt`) — required by bootstrap |

## 13. Dependency manifest consolidation — two categories, not one file
**Decision (approved for Phases 2–4):** Python (pip) and npm packages cannot share one file — they are different package managers with different formats and installers. Maintain **two manifest categories**, each with a single canonical source plus a bootstrap copy read by the deployment scripts.

| Category | Canonical source | Bootstrap copy (read by deploy scripts) |
|---|---|---|
| Python (pip) | `backend/pyproject.toml` | `deploy/scripts/library_requirements.txt` |
| npm | `frontend/pifitness/package.json` | `deploy/scripts/npm_requirements.txt` |

**Files to consolidate / eliminate:**
- `deploy/scripts/requirements-react.txt` → merge into `deploy/scripts/library_requirements.txt`, then delete. Repoint `deploy_react.sh:54` (`REQUIREMENTS_FILE`) to `library_requirements.txt`.
- Root `npm_requirements.txt` → delete after proof + approval. Repoint `pi5_deploy.sh:183` (`../../npm_requirements.txt`) to `deploy/scripts/npm_requirements.txt`.
- `setup.py` → replaced by `backend/pyproject.toml` in Phase 3.0 (already planned).

**Version integration:**
- `backend/pyproject.toml` is the single source for Python dependency versions.
- `deploy/scripts/library_requirements.txt` is a frozen `pip install -r`-format copy of `pyproject.toml` dependencies; keep in sync (preferably generated, or manually identical).
- `frontend/pifitness/package.json` is the single source for npm dependency versions.
- `deploy/scripts/npm_requirements.txt` mirrors `package.json` dependencies (one `pkg@version` per line, `npm install --no-save` format).
- Prefer pinned versions in bootstrap manifests for reproducible Pi deployments.

**Deployment script integration:**
- `pi5_deploy.sh:152` (`pip install -r deploy/scripts/library_requirements.txt`) — keep reading the bootstrap copy; ensure it stays in sync with `pyproject.toml`.
- `deploy_react.sh:112` + lines 117–122 — repoint Python requirements from `requirements-react.txt` to `library_requirements.txt`.
- `pi5_deploy.sh:183` (`../../npm_requirements.txt`) — repoint to `deploy/scripts/npm_requirements.txt`.

**Proof gate (before any deletion):** `pip install -e backend` + `pytest -q` green; `npm ci` + `next build` in `frontend/pifitness/` green; `bash -n` on both shell scripts after repointing.

**Related:** Decision #5 confirmed separately — `.features/` paths kept as-is (no docs move in Phase 2.3).

## Defaults summary (if human says "use defaults")

| Area | Default action |
|---|---|
| Secrets | Untrack + gitignore; report paths; rotate externally |
| Garmin | `pirate-garmin_clone` canonical; vendored copies deleted; imports repointed |
| Orphans | All 6 deleted (admin_functions, running_functions, admin_schemas, data_helpers, db_wrapper, image_creation) |
| importlib targets | List in DYNAMIC_TARGETS.md; pin in audit.toml |
| Scripts | keep-tool → tools/; misplaced-tests → tests/ or delete; review → pending |
| unassigned | Map per §6; unclear → core / components/ |
| `.features/` paths | Keep intact; don't break workflows |
| npm manifests | Delete root after proof + approval |
| openapi-typescript | Do not add (recommendation only) |
| Root package.json/node_modules | Delete after proof + approval |
| PY-001 dangling import | Check git log; stop if ambiguous |