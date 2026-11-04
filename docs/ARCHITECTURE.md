# ARCHITECTURE.md

Statement R2 in `docs/workflow/TARGET_STATE.md`. Explains how the folders in `docs/REPO_MAP.md` work together: layers, request flow, deployment topology, and Pi performance constraints. Every factual claim cites a path.

Folder purposes live in `docs/REPO_MAP.md` and are not repeated here. Endpoint and table lists are generated into `.audit/p1/backend.md` and `.audit/p1/deps.md` and are not copied here; this document points to them instead. One home per fact.

## 1. Overview

PiFitness is a personal fitness web application: a Next.js frontend (App Router) and a FastAPI backend on PostgreSQL, deployed on a Raspberry Pi 5 behind nginx (`docs/PI_SPECS.md:1`; `deploy/nginx/nginx-react.conf:3-4`).
Requests enter through nginx on `pifitness.duckdns.org:443`, which proxies `/api` to FastAPI on port 8000 and all other traffic to Next.js on port 3000 (`deploy/nginx/nginx-react.conf:21-22,48-49`).
The backend layers are app entry `backend/main.py`, domain routers under `backend/pifitness/<domain>/` with SQL in `queries.py`, shared config in `backend/pifitness/core/`, integrations (Garmin, Spotify), a worker engine, and the legacy `backend/backend_functions/` layer (`docs/REPO_MAP.md:52-73`).
The frontend layers are `src/app` (pages), `src/hooks` (data fetching), `src/lib` (utilities including the API client), `src/stores` (Zustand state) (`docs/REPO_MAP.md:92,126-127,132`).
Folder purposes live in `docs/REPO_MAP.md`; endpoint and table lists live in `.audit/p1/backend.md` and `.audit/p1/deps.md` — this document points to them and does not repeat them.

## 2. Layers

Backend layers:
- `backend/main.py` — FastAPI app entry: creates the app, adds CORS and Private-Network-Access middleware, mounts every domain router, and runs `/api/health` which opens a database connection via `backend_functions.database_functions.get_conn` (`backend/main.py:45-51,57-63,127-143,145-161`).
- `backend/pifitness/<domain>/router.py` — HTTP layer per domain (activities, admin, auth, food, health, music, segments, tri_tip, volleyball, config); each router calls its domain's `queries.py` functions, e.g. `GET /api/food/diary` at `backend/pifitness/food/router.py:121-131` and `POST /api/food/diary` at `backend/pifitness/food/router.py:500-511`.
- `backend/pifitness/<domain>/queries.py` — SQL layer: parameterized queries, keyset pagination, no OFFSET (`backend/pifitness/food/queries.py:5,243-252`).
- `backend/pifitness/core/config.py` — shared configuration: `get_settings()` reads `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD` from `backend/.env` with platform-aware defaults (`backend/pifitness/core/config.py:87-115`); `core/` imports nothing else in the repo (`docs/REPO_MAP.md:62`, G3 at `docs/REPO_MAP.md:20`).
- `backend/pifitness/integrations/` (garmin, spotify) — external-service API clients (`docs/REPO_MAP.md:66-68`).
- `backend/pifitness/worker/` — background task execution engine (`backend/pifitness/worker/task_execution.py:15-17`; `docs/REPO_MAP.md:73`).
- `backend/backend_functions/` — legacy shared database/utility layer, e.g. `get_conn` (`backend/backend_functions/database_functions.py:45-62`; `docs/REPO_MAP.md:54`).

Allowed import direction (G1-G6, `docs/REPO_MAP.md:18-23`): a folder may import its own subfolders (G1); every `backend/pifitness/` folder except `core/` may import `core/` (G2); `core/` imports nothing else in the repo (G3); nothing under `backend/` imports `tests/`, `tools/`, `deploy/`, `scripts/` or `agents/` (G4); domain modules never import `worker/` unless their row lists it (G5); `backend/backend_functions/` is legacy — `pifitness/` may import it and it may import only `core/` (G6). Deviations from these rules are recorded in `docs/REPO_MAP.md` "Known deviations" (`docs/REPO_MAP.md:149-156`) and tracked in `docs/workflow/BACKLOG.md`; they are not restated here.

Frontend layers:
- `src/app` — Next.js App Router pages and layouts (`frontend/pifitness/src/app/layout.tsx`, `frontend/pifitness/src/app/food/page.tsx`) (`docs/REPO_MAP.md:92,118`).
- `src/hooks` — React Query data-fetching hooks (`frontend/pifitness/src/hooks/useAuth.ts`) (`docs/REPO_MAP.md:126`).
- `src/lib` — shared utilities, including the typed API client `frontend/pifitness/src/lib/api-client.ts` (`docs/REPO_MAP.md:127`).
- `src/stores` — Zustand client state (`frontend/pifitness/src/stores/uiStore.ts`) (`docs/REPO_MAP.md:132`).

Each example above was opened to confirm it exists on disk.

## 3. Request flow

Read — `GET /api/food/diary?day=YYYY-MM-DD`:
1. Browser: `Diary.tsx` calls `loadEntries(day)` → `API.food.diary(d)` → `fetch("/api/food/diary?day=...")` (`frontend/pifitness/src/app/food/components/Diary.tsx:220-223`; `frontend/pifitness/src/lib/api-client.ts:830-831`).
2. To the API: in dev, Next.js rewrites `/api/:path*` → `http://localhost:8000/api/:path*` (`frontend/pifitness/next.config.ts:31-37`). On the Pi, nginx terminates HTTPS on `pifitness.duckdns.org:443` and proxies `location /api` to `http://127.0.0.1:8000` (`deploy/nginx/nginx-react.conf:2-4,21-22`).
3. Where the API URL is decided: `api-client.ts` builds `API_BASE` from `NEXT_PUBLIC_API_URL` or `""` (`frontend/pifitness/src/lib/api-client.ts:129`). In production `deploy_react.sh` unsets `NEXT_PUBLIC_API_URL` and exports `NEXT_PUBLIC_APP_ENV=production` (`deploy/scripts/deploy_react.sh:168-169,590-591`), so calls are same-origin and nginx routes `/api` to FastAPI.
4. Router: `backend/main.py` mounts the food router (`backend/main.py:143`); `router.py` validates the day and calls `queries.get_diary_entries` (`backend/pifitness/food/router.py:121-131`).
5. Database: queries run through `backend_functions.database_functions` (`sql_to_dict`, `backend/pifitness/food/queries.py:20-24`) → `get_conn()` which resolves host/port/db/user/password via `pifitness.core.config.get_settings` (`backend/backend_functions/database_functions.py:32-42,45-62`; `backend/pifitness/core/config.py:87-115`; `.env` loaded from `backend/`, `backend/backend_functions/database_functions.py:15-18`). The query reads `food.diary_entries` with a `LIMIT` (`backend/pifitness/food/queries.py:247-252`), rows are shaped into `DayEntry[]` (`backend/pifitness/food/queries.py:128-140`), and the JSON travels back the same path to the browser.

Write — `POST /api/food/diary`:
1. Browser: `AddEntry.tsx` calls `API.food.logEntry({food_id, qty, unit, logged_at})` → `fetch("/api/food/diary", {method: "POST", ...})` (`frontend/pifitness/src/app/food/components/AddEntry.tsx:215`; `frontend/pifitness/src/lib/api-client.ts:862`).
2. To the API: same hops as the read — nginx `/api` → `127.0.0.1:8000` on the Pi, Next.js rewrite to `localhost:8000` in dev (`deploy/nginx/nginx-react.conf:21-22`; `frontend/pifitness/next.config.ts:31-37`).
3. Router/query/DB: `log_entry` handler (`backend/pifitness/food/router.py:500-511`) calls `queries.log_diary_entry` (`backend/pifitness/food/queries.py:406-459`), which validates qty, snapshots name + nutrients from `food.foods`, and inserts into `food.diary_entries` via `sql_insert_returning` (`backend/pifitness/food/queries.py:443-456`) through `get_conn` (`backend/backend_functions/database_functions.py:45-62`). The `DayEntry` result returns as JSON to the browser, and `Diary.tsx` refreshes (`frontend/pifitness/src/app/food/components/Diary.tsx:229-233`).

Second example — trigger a background task (`POST /api/admin/tasks/v2/{task_name}/execute`):
1. Browser → nginx → FastAPI: `execute_task_v2_async` at `backend/pifitness/admin/router.py:170-171`.
2. Router → worker: the handler spawns a daemon `threading.Thread` (`backend/pifitness/admin/router.py:238-272`) that runs `pifitness.worker.ultimate_task_executioner_v2.ultimate_task_executioner` (`backend/pifitness/admin/router.py:242-243`).
3. Worker → database: the executioner queries `tasks.vw_task_execution` (`backend/pifitness/worker/task_execution.py:16-17`) and the router records the run in `tasks.task_executions` (`backend/pifitness/admin/router.py:222-228`) through `backend_functions.database_functions` (`sql_insert_returning`, `backend/pifitness/admin/router.py:221-222`). Status is polled at `GET /api/admin/tasks/executions/{execution_id}` (`backend/pifitness/admin/router.py:288`).

## 4. Background work

What runs in the background, based on `.audit/p1/backend.md` "Background work / scheduling mentions" (`.audit/p1/backend.md:175-181`) plus `deploy/systemd/` and `deploy/scripts/`:

- Task executor / agents: `pifitness_agent.timer` fires every 30 minutes (`OnCalendar=*:0/30`, `deploy/systemd/pifitness_agent.timer:5`) → `pifitness_agent.service` (oneshot) → `python3 -m agents.background_task_execution` (`deploy/systemd/pifitness_agent.service:13`) → `agents/background_task_execution.py:17-19` runs `pifitness.worker.ultimate_task_executioner_v2.ultimate_task_executioner()`. The timer has `RuntimeMaxSec=28min` (`deploy/systemd/pifitness_agent.service:18`). Trigger: systemd timer; also on demand via `POST /api/admin/tasks/v2/{task_name}/execute`, which spawns a daemon thread running the same executioner (`backend/pifitness/admin/router.py:170-171,238-272`).
- Hardware health: `pifitness-hw-health.timer` fires hourly (`OnCalendar=hourly`, `deploy/systemd/pifitness-hw-health.timer:5`) → `pifitness-hw-health.service` → `/usr/local/bin/pifitness-hw-health.py` (`deploy/systemd/pifitness-hw-health.service:5`), which captures `vcgencmd get_throttled`, `nvme smart-log`, and disk usage into `/home/god/Documents/PiFitness_Local/hardware_health.json` (`deploy/scripts/pifitness-hw-health.py:30-41`). Trigger: systemd timer, hourly.
- Inline thread: `POST /api/admin/tasks/v2/{task_name}/execute` starts a daemon thread (`backend/pifitness/admin/router.py:238-272`) that updates `tasks.task_executions` (`:222-228`) as the task runs. Trigger: user request through the API.
- Note: `deploy_react.sh` installs the auxiliary systemd units from `$SELF_DIR/health` and `$SELF_DIR/agent`, but `deploy/scripts/health/` and `deploy/scripts/agent/` do not exist under `deploy/` (`deploy/scripts/deploy_react.sh:132-152`; directory listing of `deploy/scripts/` shows neither folder) — tracked as backlog item B13 (`docs/workflow/BACKLOG.md:21`).

## 5. Deployment topology

What exists today (not the target state), all from the deploy files:

Processes on the Pi:
- FastAPI under systemd `pifitness-fastapi.service`: `uvicorn backend.main:app --host 0.0.0.0 --port 8000` (`deploy/systemd/pifitness-fastapi.service:12`).
- Next.js under pm2 as `pifitness-next`: `npm run start -- --port 3000` (`deploy/scripts/deploy_react.sh:170,600`).
- nginx on HTTPS 443 with HTTP 80 → 301 redirect (`deploy/nginx/nginx-react.conf:3,85-88`).

Nginx routing (`deploy/nginx/nginx-react.conf`):
- `location /api` → `http://127.0.0.1:8000` (lines 21-22); dedicated `/api/health` → 8000, no-cache (lines 70-71).
- `location /_next/static/` → `http://127.0.0.1:3000`, cached 1 year (lines 39-45).
- `location /` → `http://127.0.0.1:3000` (lines 48-49).
- Server name `pifitness.duckdns.org`, Certbot-managed TLS (lines 2-4,7-10).

Git branches and deploy modes (`deploy/scripts/bootstrap.sh`, `deploy/scripts/deploy_react.sh`):
- `bootstrap.sh` targets only `react-ui` (`TARGET="react-ui"`, line 23) and fetches deployment scripts from `origin/deploy_merge` (`DEPLOY_BRANCH="deploy_merge"`, line 25; fetch at lines 237-251).
- `deploy_react.sh` checks out/pulls `origin/react-ui` (lines 98, 504).
- `nuclear` — wipe the project directory, re-clone, rebuild from scratch (lines 60-185).
- `fast` — pull, restart only what changed, skip tests and package installs (lines 191-462).
- `full` (default) — pull, install packages, run tests, rebuild, restart everything (lines 465-642).

Config and secrets: the master `.env` lives outside the repo (`/home/god/Documents/.env`) and is copied into `backend/.env` and `.env` by `deploy_react.sh` (lines 401-402 in fast mode, 507-508 in full mode, 100-101 in nuclear). Credentials are never printed here.

Production database: the backend finds it via `get_settings()` / `get_settings_for_db()`, which read `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD` from `backend/.env` (`backend/pifitness/core/config.py:87-115`; `backend/backend_functions/database_functions.py:32-42`); on Linux the default host is `localhost` (`backend/pifitness/core/config.py:70-84`). The production host/database name/credentials live only in the master `.env` outside the repo and are not read or printed by this task. The systemd unit declares `Wants=postgresql.service` (`deploy/systemd/pifitness-fastapi.service:4`), and `tools/db/sync-prod-to-dev.ps1:63` references the Pi host `pfpi`.

Frontend build: Next.js is built on the Pi at deploy time — `npm run build` in `frontend/pifitness` (`deploy/scripts/deploy_react.sh:126` nuclear, `deploy/scripts/deploy_react.sh:415,428` fast, `deploy/scripts/deploy_react.sh:598` full), not as a pre-built artifact.

E3 gap (one line): `TARGET_STATE` E3 says bootstrap should offer a choice of `main` (PRD) or `react-ui`, but `bootstrap.sh` hard-codes `TARGET="react-ui"` (`deploy/scripts/bootstrap.sh:23`) — added to the backlog as B16.

## 6. Pi performance constraints

Two kinds of content only: hardware facts (a) and constraints visible in the repo (b). No memory budgets, latency numbers, or timings are stated because none are measured in the repo.

(a) Hardware facts, all from `docs/PI_SPECS.md`:
- Raspberry Pi 5, 8 GB RAM / 500 GB NVMe SSD (`docs/PI_SPECS.md:1`).
- SoC Broadcom BCM2712 (`docs/PI_SPECS.md:4`).
- CPU: quad-core 64-bit Arm Cortex-A76 @ 2.4 GHz (`docs/PI_SPECS.md:6`).
- Memory: 8 GB LPDDR4X-4267 (`docs/PI_SPECS.md:16`).
- Storage: 500 GB NVMe SSD over PCIe 2.0 x1, peak ~500 MB/s (PCIe Gen 3 software override ~800-900 MB/s, non-standard) (`docs/PI_SPECS.md:23-29`).
- Networking: Gigabit Ethernet, Wi-Fi 802.11ac (`docs/PI_SPECS.md:35-37`).

(b) Constraints visible in the repo:
- The Next.js app is built on the Pi at deploy time (`npm run build` in `frontend/pifitness`, `deploy/scripts/deploy_react.sh:126,598`) — there is no pre-built artifact deployed from elsewhere.
- FastAPI runs as a single uvicorn process with no `--workers` flag (`deploy/systemd/pifitness-fastapi.service:12`).
- Next.js runs as a single pm2 process (`npm run start -- --port 3000`, no cluster mode) (`deploy/scripts/deploy_react.sh:170,600`).
- Bounded reads: `GET /api/food/usuals` clamps `limit` to 1-100 (default 20) (`backend/pifitness/food/router.py:103`); `GET /api/food/search` clamps `limit` to 1-30 (`backend/pifitness/food/router.py:191`); `GET /api/food/foods` uses cursor + `limit` 1-100 (`backend/pifitness/food/router.py:483-487`); `get_diary_entries` applies `LIMIT 100` (`backend/pifitness/food/queries.py:250-252`); the food queries module docstring states "keyset pagination, no OFFSET" (`backend/pifitness/food/queries.py:5`).
- Unbounded reads (risk): `get_usual_foods` runs `SELECT food_id, logged_at FROM food.diary_entries` with no `LIMIT` and scores every row in Python (`backend/pifitness/food/queries.py:188-192`); `get_log_data` selects `*` from a logging table ordered by time with no `LIMIT` (`backend/backend_functions/database_functions.py:251`).
- Database size observability: admin endpoints `get_db_size_breakdown` / `get_db_size_history` (`.audit/p1/backend.md:23-24`) and view `logging.vw_db_size_chart` (`.audit/p1/backend.md:288`).
- Background-task scheduling intervals: `pifitness_agent.timer` every 30 minutes (`deploy/systemd/pifitness_agent.timer:5`, `RuntimeMaxSec=28min` in `deploy/systemd/pifitness_agent.service:18`); `pifitness-hw-health.timer` hourly (`deploy/systemd/pifitness-hw-health.timer:5`).
- Heavy worker path: `task_executioner` reads all tasks from `tasks.vw_task_execution` (`backend/pifitness/worker/task_execution.py:16-17`); the admin router runs `ultimate_task_executioner` on demand in a daemon thread (`backend/pifitness/admin/router.py:242-243,271-272`).

No change recommendations are made here; the unbounded-read risk is logged as B20.

## 7. Known deviations

Recorded in `docs/REPO_MAP.md` "Known deviations" (`docs/REPO_MAP.md:149-156`); they are not restated here, and each names the backlog item in `docs/workflow/BACKLOG.md` that fixes it:
- K1, K2 — `backend/backend_functions/` reverse imports break G6 (backlog B1, `docs/workflow/BACKLOG.md:9`).
- K3 — redundant/vestigial folders `scripts/`, `agents/`, `backend/api/`, `.features/descriptions/` (backlog B3, B4, B8, `docs/workflow/BACKLOG.md:11,13,15`).
- K4 — empty folders `.features/designs_active/`, `docs/archive/` (backlog B7, `docs/workflow/BACKLOG.md:15`).
- K5 — dependency generator shows no imports for `backend/` although `backend/main.py` imports `pifitness.*` (backlog B11, `docs/workflow/BACKLOG.md:19`).

## 8. Unverified, human to confirm

Raised during authoring; answered by human confirmation (not re-derived from repo paths):

1. Production PostgreSQL runs locally on the Pi; the `.env` guides the app to the local instance there (systemd unit declares `Wants=postgresql.service`, `deploy/systemd/pifitness-fastapi.service:4`; master `.env` at `/home/god/Documents/.env` is outside the repo).
2. Production DB name/user/size are not needed — Cline never accesses PRD; credentials live only in the master `.env`; `tools/db/sync-prod-to-dev.ps1` copies PRD to dev (sanitized).
3. `frontend/DEPLOYMENT_GUIDE.md:66-68` (build-time `NEXT_PUBLIC_API_URL=http://localhost:8000`) is likely stale vs production behavior (`deploy_react.sh` unsets `NEXT_PUBLIC_API_URL`, `deploy/scripts/deploy_react.sh:168-169,590-591`; app URL `https://pifitness.duckdns.org`). Verification is a backlog candidate.
4. Unbounded reads (`get_usual_foods`, `get_log_data`) likely not a problem at personal scale (under 10k rows per human); not measured from repo. Risk logged as B20.
5. `/usr/local/bin/pifitness-hw-health.py` vs `deploy/scripts/pifitness-hw-health.py` likely match; cautious of "health" naming collision with the app module. Exact match not verified.
