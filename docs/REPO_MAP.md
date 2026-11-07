# REPO_MAP

Covers statement R1 in `docs/workflow/TARGET_STATE.md`. Every folder in the repo has exactly one row. `repo_health` (Phase 2) fails on any folder missing from this table.

Folder purposes live here. How the folders work together lives in `docs/ARCHITECTURE.md`. Observed imports are generated into `.audit/p1/deps.md`, not copied here.

## How to read the table

- **folder**: repo-relative path with a trailing slash. One row per folder, sorted by path.
- **purpose**: one line, with one file that proves it in parentheses. Empty folders have no file to cite and are marked `(empty)`.
- **written by**: who is allowed to write there. `human`, `agent`, `generated` or a `+` combination. Agents write only inside tasks that name the folder, and the human applies anything to PRD.
- **must not contain**: what repo_health or a reviewer should reject.
- **allowed depends on**: folders this folder may import from. `-` means none beyond rules G1 and G2. `X/**` means X and all its subfolders.
- A cell reading `TODO(cite)` means the citation still has to be filled from the code.

## Dependency rules (apply to every row)

- **G1.** A folder may always import from its own subfolders.
- **G2.** Every folder under `backend/pifitness/` except `core/` may import `backend/pifitness/core/`.
- **G3.** `backend/pifitness/core/` imports nothing else in the repo.
- **G4.** Nothing under `backend/` imports from `tests/`, `tools/`, `deploy/`, `scripts/` or `agents/`.
- **G5.** Domain modules (`activities`, `admin`, `auth`, `config`, `exercises`, `food`, `health`, `music`, `segments`, `tri_tip`, `volleyball`) never import `worker/` unless their row lists it.
- **G6.** `backend/backend_functions/` is a legacy layer: `pifitness/` may import it, and it may import only `core/`. New shared code goes in `backend/pifitness/core/`.

## Table

| folder | purpose (cite a file) | written by | must not contain | allowed depends on |
|---|---|---|---|---|
| .agents/ | Agent skill definitions (.agents/skills/backend-engineer/SKILL.md) | human | application source code or runtime logic | - |
| .agents/skills/ | One folder per agent skill, each with a SKILL.md (.agents/skills/sql-expert/SKILL.md) | human | feature implementations or production code | - |
| .agents/skills/backend-engineer/ | Backend agent skill for FastAPI/Python work (.agents/skills/backend-engineer/SKILL.md) | human | frontend code or SQL schema definitions | - |
| .agents/skills/block-executor/ | Agent skill for executing work blocks (.agents/skills/block-executor/SKILL.md) | human | feature designs or business logic | - |
| .agents/skills/business-analyst/ | Agent skill for requirements and feature descriptions (.agents/skills/business-analyst/SKILL.md) | human | implementation code or test files | - |
| .agents/skills/code-architect/ | Agent skill for task planning and delegation (.agents/skills/code-architect/SKILL.md) | human | runtime code or database schemas | - |
| .agents/skills/deployment-engineer/ | Agent skill for deployment and library registration (.agents/skills/deployment-engineer/SKILL.md) | human | application source code | - |
| .agents/skills/frontend-dev/ | Agent skill for React frontend work (.agents/skills/frontend-dev/SKILL.md) | human | backend API code or SQL migrations | - |
| .agents/skills/sql-expert/ | Agent skill for PostgreSQL/TimescaleDB schema and queries (.agents/skills/sql-expert/SKILL.md) | human | frontend components or FastAPI routes | - |
| .audit/ | Evidence and plan documents produced by the repo-restructuring plan, kept so the plan can be re-run (.audit/p1/README.md) | generated+agent | application source code or anything the app imports | - |
| .audit/p1/ | Phase 1 evidence: inventory, deps, backend, frontend, refs, comments and backlog files (.audit/p1/deps.md) | generated+agent | application source code or anything the app imports | - |
| .clinerules/ | Always-loaded agent rules and verified environment facts (.clinerules/environment.md) | human | skill definitions or feature-specific designs | - |
| .clinerules/workflows/ | Step-by-step agent workflow procedures (.clinerules/workflows/feature-design.md) | human | global rules or skill definitions | - |
| .features/ | Feature lifecycle files: designs, descriptions, workflow tooling (.features/tools/fd.py) | human+agent | production application code | - |

| .features/descriptions/ | Legacy home of feature descriptions and food JSON copies; food payloads now live in docs/sample_json_food_payloads/ (docs/sample_json_food_payloads/almond_responses.json) | human | production source code or approved designs | - |
| .features/descriptions/json_food_responses/ | Legacy copy of food API JSON samples, superseded by docs/sample_json_food_payloads/ (docs/sample_json_food_payloads/almond_responses.json) | human | implementation code or schema definitions | - |
| .features/designs_active/ | Designs for features in progress (empty today; kept only if the Phase 4 workflow uses it) (.features/tools/fd.py) | human+agent | implementation code or approved designs | - |

| .features/tools/ | Feature workflow tooling (.features/tools/fd.py) | human+agent | feature designs or description files | - |
| .features/zz_workfow_archive/ | Archived documents from earlier workflows, kept for reference (.features/zz_workfow_archive/bug-analysis.md) | human | active feature designs or current workflow rules | - |
| .vscode/ | VS Code workspace settings and extension recommendations (.vscode/extensions.json) | human | application source code or build output | - |
| agents/ | Script the Pi runs on a schedule that calls the background task executor; retained until deployment services are rewritten (agents/background_task_execution.py) | human | skill definitions, agent rules or application logic | backend/pifitness/worker/ |
| backend/ | Thin FastAPI app entry that imports routers from pifitness (backend/main.py) | human+agent | route definitions or business logic | backend/pifitness/**, backend/backend_functions/ |
| backend/api/ | Package marker only, possibly vestigial (backend/api/__init__.py) | human | route definitions or business logic | - |
| backend/backend_functions/ | Legacy shared database and utility layer, to be folded into pifitness/core/ and the domain modules (backend/backend_functions/database_functions.py) | human+agent | FastAPI routes, domain models or new shared code | backend/pifitness/core/ |
| backend/backend_functions/queries/ | SQL query module (backend/backend_functions/queries/__init__.py) | human+agent | application logic | - |
| backend/backend_functions/schemas/ | Pydantic schema definitions (backend/backend_functions/schemas/leaderboard_schemas.py) | human+agent | database connection logic | - |
| backend/pifitness/ | Package root for the FastAPI app: domain modules plus a shared core (backend/pifitness/core/config.py) | human+agent | standalone scripts or deployment assets | backend/backend_functions/ |
| backend/pifitness/activities/ | Activities API routes and services (backend/pifitness/activities/router.py) | human+agent | shared utility functions | backend/backend_functions/**, backend/pifitness/worker/ |
| backend/pifitness/admin/ | Admin task management routes (backend/pifitness/admin/router.py) | human+agent | shared utility functions | backend/backend_functions/, backend/pifitness/worker/ |
| backend/pifitness/auth/ | Authentication routes and status (backend/pifitness/auth/router.py) | human+agent | shared utility functions | backend/backend_functions/, backend/pifitness/integrations/** |
| backend/pifitness/config/ | Configuration routes (backend/pifitness/config/router.py) | human+agent | shared utility functions | backend/pifitness/integrations/ |
| backend/pifitness/core/ | Shared configuration and utilities; destination for code folded out of backend_functions/ (backend/pifitness/core/config.py) | human+agent | route handlers or domain models | - |
| backend/pifitness/exercises/ | Exercises API routes (backend/pifitness/exercises/router.py) | human+agent | shared utility functions | backend/backend_functions/ |
| backend/pifitness/food/ | Food API routes and lookup (backend/pifitness/food/router.py) | human+agent | shared utility functions | backend/backend_functions/, backend/pifitness/integrations/ |
| backend/pifitness/health/ | Health data routes (backend/pifitness/health/router.py) | human+agent | shared utility functions | backend/backend_functions/ |
| backend/pifitness/integrations/ | Base package for external-service integrations (backend/pifitness/integrations/__init__.py) | human+agent | route handlers | backend/backend_functions/ |
| backend/pifitness/integrations/garmin/ | Garmin API integration (backend/pifitness/integrations/garmin/client.py) | human+agent | route handlers or shared utilities | backend/backend_functions/, backend/pifitness/integrations/ |
| backend/pifitness/integrations/spotify/ | Spotify API integration (backend/pifitness/integrations/spotify/provider.py) | human+agent | route handlers or shared utilities | backend/backend_functions/, backend/pifitness/integrations/ |
| backend/pifitness/music/ | Music API routes (backend/pifitness/music/router.py) | human+agent | shared utility functions | backend/backend_functions/**, backend/pifitness/integrations/spotify/ |
| backend/pifitness/segments/ | Segments API routes (backend/pifitness/segments/router.py) | human+agent | shared utility functions | backend/backend_functions/**, backend/pifitness/activities/, backend/pifitness/config/ |
| backend/pifitness/tri_tip/ | Tri-tip prediction routes (backend/pifitness/tri_tip/router.py) | human+agent | shared utility functions | backend/backend_functions/ |
| backend/pifitness/volleyball/ | Volleyball API routes (backend/pifitness/volleyball/router.py) | human+agent | shared utility functions | backend/backend_functions/ |
| backend/pifitness/worker/ | Background task execution engine (backend/pifitness/worker/task_execution.py) | human+agent | route handlers or shared utilities | backend/backend_functions/, backend/pifitness/integrations/garmin/, backend/pifitness/integrations/spotify/ |
| deploy/ | Deployment configuration and scripts for the Pi (deploy/scripts/bootstrap.sh) | human+agent | application source code | - |
| deploy/migrations/ | Single home of numbered DDL migration files; applied to DEV by the tool and to PRD by the human only (deploy/migrations/INT19b__move_tokens_to_api_services.sql) | human+agent | application code, configuration, or DDL applied outside a numbered file | - |
| deploy/nginx/ | Nginx configuration (deploy/nginx/nginx-react.conf) | human+agent | application code or database files | - |
| deploy/scripts/ | Deployment shell and Python scripts (deploy/scripts/bootstrap.sh) | human+agent | application source code | - |
| deploy/systemd/ | Systemd service unit files (deploy/systemd/pifitness-fastapi.service) | human+agent | application code or scripts | - |
| docs/ | Project documentation (docs/architecture/dynamic-targets.md) | human+agent | application code or test files | - |
| docs/architecture/ | Architecture decision documents (docs/architecture/dynamic-targets.md) | human+agent | application code or workflow rules | - |
| docs/archive/ | Archived project documents (empty today; kept only if the doc index uses it) | human | current active documentation | - |
| docs/reorg/ | Reorganization plan status and scratch notes (docs/reorg/STATUS.md) | human | application source code or anything the app imports | - |
| docs/reorg/archive/ | Archived reorganization plans, blockers and handoff notes (docs/reorg/archive/README.md) | human | active project documentation or application code | - |
| docs/sample_json_food_payloads/ | Reference JSON payloads for the food diary module; reference material only, not test fixtures (docs/sample_json_food_payloads/almond_responses.json) | human | application code or test fixtures | - |
| docs/data/ | Generated schema map (docs/data/schema.generated.md) | generated | application source code | - |
| docs/workflow/ | Workflow contract and target state (docs/workflow/TARGET_STATE.md) | human | application code or architecture docs | - |
| frontend/ | Next.js frontend project root (frontend/pifitness/package.json) | human+agent | backend code or database files | - |
| frontend/pifitness/ | Next.js application source and configuration (frontend/pifitness/next.config.ts) | human+agent | backend code, database dumps or build output | - |
| frontend/pifitness/public/ | Static public assets (frontend/pifitness/public/wasm/zxing_reader.wasm) | human+agent | source code or build output | - |
| frontend/pifitness/public/wasm/ | WebAssembly modules (frontend/pifitness/public/wasm/zxing_reader.wasm) | human+agent | source code or build output | - |
| frontend/pifitness/scripts/ | Frontend build and utility scripts (frontend/pifitness/scripts/copy-zxing-wasm.mjs) | human+agent | application source code | frontend/pifitness/src/lib/ |
| frontend/pifitness/src/ | React application source (frontend/pifitness/src/app/page.tsx) | human+agent | build output or configuration files | - |
| frontend/pifitness/src/app/ | Next.js App Router pages and layouts (frontend/pifitness/src/app/layout.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/ | Activity feature route pages (frontend/pifitness/src/app/activities/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/activity-processing/ | Activity-processing route page (frontend/pifitness/src/app/activities/activity-processing/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/activity-processing/__tests__/ | Tests for the activity-processing page (frontend/pifitness/src/app/activities/activity-processing/__tests__/failure-finalization.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/activities/beach/ | Beach volleyball route page (frontend/pifitness/src/app/activities/beach/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/beach/components/ | Beach activity components (frontend/pifitness/src/app/activities/beach/components/BeachVolleyball.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/beach/components/__tests__/ | Tests for beach components (frontend/pifitness/src/app/activities/beach/components/__tests__/volleyball-axis.test.ts) | human+agent | application source code | - |
| frontend/pifitness/src/app/activities/leaderboards/ | Leaderboards route page (frontend/pifitness/src/app/activities/leaderboards/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/leaderboards/components/ | Leaderboard components (frontend/pifitness/src/app/activities/leaderboards/components/LeaderboardTable.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/leaderboards/components/__tests__/ | Tests for leaderboard components (frontend/pifitness/src/app/activities/leaderboards/components/__tests__/LeaderboardTable.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/activities/recent-activity/ | Recent-activity route page (frontend/pifitness/src/app/activities/recent-activity/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/recent-activity/__tests__/ | Tests for the recent-activity page (frontend/pifitness/src/app/activities/recent-activity/__tests__/page.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/activities/recent-activity/components/ | Recent-activity components (frontend/pifitness/src/app/activities/recent-activity/components/EfficiencyBullets.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/recent-activity/components/__tests__/ | Tests for recent-activity components (frontend/pifitness/src/app/activities/recent-activity/components/__tests__/EfficiencyBullets.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/activities/segment-management/ | Segment-management route page (frontend/pifitness/src/app/activities/segment-management/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/segment-management/components/ | Segment-management match components (frontend/pifitness/src/app/activities/segment-management/components/ActivitySourceList.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/activities/segment-management/components/__tests__/ | Tests for segment-management components (frontend/pifitness/src/app/activities/segment-management/components/__tests__/ActivitySourceList.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/admin/ | Admin route page (frontend/pifitness/src/app/admin/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/admin/components/ | Admin components (frontend/pifitness/src/app/admin/components/TaskList.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/admin/components/__tests__/ | Tests for admin components (frontend/pifitness/src/app/admin/components/__tests__/failure-message.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/beachchanger/ | Beachchanger route page (frontend/pifitness/src/app/beachchanger/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/beachchanger/components/ | Beachchanger components (frontend/pifitness/src/app/beachchanger/components/BeachViewer.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/components/ | Shared page components (frontend/pifitness/src/app/components/Layout.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/components/__tests__/ | Tests for shared components (frontend/pifitness/src/app/components/__tests__/IsrcDupeHomeBanner.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/exercises/ | Exercises route page (frontend/pifitness/src/app/exercises/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/exercises/components/ | Exercise components (frontend/pifitness/src/app/exercises/components/ExerciseSelect.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/food/ | Food diary route page (frontend/pifitness/src/app/food/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/food/components/ | Food diary components (frontend/pifitness/src/app/food/components/Diary.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/food/components/__tests__/ | Tests for food components (frontend/pifitness/src/app/food/components/__tests__/Diary.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/health/ | Health route page (frontend/pifitness/src/app/health/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/music/ | Music route page (frontend/pifitness/src/app/music/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/music/components/ | Music components (frontend/pifitness/src/app/music/components/NowPlayingView.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/app/music/components/__tests__/ | Tests for music components (frontend/pifitness/src/app/music/components/__tests__/IsrcReviewView.test.tsx) | human+agent | application source code | - |
| frontend/pifitness/src/app/running/ | Running route page (frontend/pifitness/src/app/running/page.tsx) | human+agent | library code or hook definitions | frontend/pifitness/src/hooks/, frontend/pifitness/src/lib/, frontend/pifitness/src/stores/ |
| frontend/pifitness/src/hooks/ | React custom hooks (frontend/pifitness/src/hooks/useAuth.ts) | human+agent | page components or library code | frontend/pifitness/src/lib/ |
| frontend/pifitness/src/lib/ | Shared frontend library utilities (frontend/pifitness/src/lib/api-client.ts) | human+agent | page components or hook definitions | - |
| frontend/pifitness/src/lib/__tests__/ | Frontend library tests (frontend/pifitness/src/lib/__tests__/date-format.test.ts) | human+agent | application source code | - |
| frontend/pifitness/src/lib/barcode/ | Barcode decoding library (frontend/pifitness/src/lib/barcode/decoder.ts) | human+agent | page components or hook definitions | - |
| frontend/pifitness/src/lib/barcode/__tests__/ | Tests for the barcode library (frontend/pifitness/src/lib/barcode/__tests__/decoder.test.ts) | human+agent | application source code | - |
| frontend/pifitness/src/lib/types/ | Shared TypeScript type contracts (frontend/pifitness/src/lib/types/food-contract.ts) | human+agent | page components or hook definitions | - |
| frontend/pifitness/src/stores/ | Client state stores (frontend/pifitness/src/stores/uiStore.ts) | human+agent | page components or hook definitions | - |
| frontend/pifitness/src/test/ | Frontend test setup (frontend/pifitness/src/test/setup.ts) | human+agent | application source code | - |
| scripts/ | Legacy repo-audit and reorganization tooling, kept because it may be retooled; it has no bearing on app behavior (scripts/repo_audit/cli.py) | human | application source code or anything the app imports | - |
| scripts/repo_audit/ | Legacy repo audit CLI and analyzers (scripts/repo_audit/cli.py) | human | application source code or anything the app imports | - |
| tests/ | Backend test suite (tests/test_no_prd_default.py) | human+agent | application source code | backend/** |
| tests/integration/ | Integration tests against DEV (tests/integration/conftest.py) | human+agent | unit tests or application code | backend/** |
| tools/ | Developer and maintenance tooling (tools/db/sync-prod-to-dev.ps1) | human+agent | application source code or stored credentials | backend/** |
| tools/audit/ | Repo audit and fact-extraction tooling used by the restructuring plan (tools/audit/p1_facts.py) | human+agent | application source code | - |
| tools/baselines/ | Ratchet baseline files, one per check (tools/baselines/cites.txt) | generated+agent | non-baseline files | - |
| tools/db/ | Database maintenance scripts; the PRD-to-DEV refresh is run by the human only (tools/db/sync-prod-to-dev.ps1) | human | application source code or stored PRD credentials | backend/** |
| tools/diagnostics/ | Diagnostic and analysis scripts (tools/diagnostics/schema_cache.py) | human+agent | application source code | backend/** |
| tools/probe/ | Capability probes for DEV identity, write round trip and headless browser (tools/probe/probe_db.py) | human+agent | application source code | backend/** |
| tools/workflow/ | Workflow helper scripts (tools/workflow/append_progress.ps1) | human+agent | application source code | - |
| tools/refactor/ | Code refactoring utilities (tools/refactor/tests/fixture/manifest.json) | human+agent | application source code | - |
| tools/refactor/tests/ | Refactor tool tests (tools/refactor/tests/fixture/manifest.json) | human+agent | application source code | - |
| tools/refactor/tests/fixture/ | Refactor tool fixtures (tools/refactor/tests/fixture/manifest.json) | human+agent | application source code | - |
| tools/refactor/tests/fixture/pkg_a/ | Fixture package A for refactor tool tests (tools/refactor/tests/fixture/pkg_a/foo.py) | human+agent | application source code | - |
| tools/refactor/tests/fixture/pkg_b/ | Fixture package B for refactor tool tests (tools/refactor/tests/fixture/pkg_b/__init__.py) | human+agent | application source code | - |

## Known deviations

Each deviation names the backlog item in `docs/workflow/BACKLOG.md` that fixes it. They are recorded here so the map describes the target honestly; they are not fixed in Phase 1.

- **K1 (B1).** `backend/backend_functions/` imports `backend/pifitness/integrations/`, `backend/pifitness/integrations/spotify/` and `backend/pifitness/worker/`, while those folders import it back. This breaks G6 and creates three mutual dependencies.
- **K2 (B1).** `pifitness/` importing `backend_functions/` is allowed only as a transition. It ends when the fold-in is done.
- **K3 (B3, B4, B8).** Four folders look redundant or vestigial and need a keep-or-delete decision: `scripts/` (legacy), `agents/` (duplicates what `worker/` does), `backend/api/` (marker only), and `.features/descriptions/` (old home).
- **K4 (B7).** `.features/designs_active/` and `docs/archive/` are empty and will not exist in a fresh clone unless they hold a placeholder file.
- **K5 (B11).** The generated observed-dependency list shows no imports for `backend/`, although `backend/main.py` imports `pifitness.*`. The generator likely misses package-root imports, so repo_health must not trust it until that is checked.