# System Architecture (`ARCHITECTURE.md`)

This document defines the high-level architecture, layer hierarchy, request flows, and operational invariants of PiFitness. It serves as an evergreen structural blueprint for developers and AI agents.

---

## 1. System Overview & Topology

PiFitness is a personal health, fitness, and music analytics web application deployed on a dedicated Raspberry Pi 5 behind Nginx with local PostgreSQL storage.

```mermaid
flowchart TD
    Client["Client Browser / Phone (HTTPS)"]
    Nginx["Nginx Reverse Proxy (:443)"]
    Next["Next.js App (:3000)"]
    FastAPI["FastAPI Backend (:8000)"]
    DB[(PostgreSQL)]
    Worker["Background Worker Engine"]
    Garmin["Garmin Connect API"]
    Spotify["Spotify Web API"]

    Client -->|pifitness.duckdns.org| Nginx
    Nginx -->|/ (Static & SSR)| Next
    Nginx -->|/api/*| FastAPI
    FastAPI --> DB
    Worker --> DB
    Worker --> Garmin
    Worker --> Spotify
```

* **Production Entry Point:** Nginx terminates HTTPS on `pifitness.duckdns.org:443`.
  * Traffic matching `/api/*` is proxied to the FastAPI application listening on `127.0.0.1:8000`.
  * All other traffic is proxied to the Next.js frontend listening on `127.0.0.1:3000`.
* **Development Topology:**
  * Next.js dev server runs on `localhost:3000` and rewrites `/api/*` requests to `localhost:8000`.
  * FastAPI runs locally via Uvicorn on `localhost:8000`.
  * Database runs in Docker container `pifitness-dev-db` on `localhost:15432` (`pifitness_dev`).

---

## 2. Layered Architecture & Import Boundaries

### Backend Layers (`backend/`)
1. **Application Entry (`backend/main.py`):** Instantiates FastAPI, configures CORS and Private-Network-Access middleware, registers domain routers, and provides basic `/api/health`.
2. **Domain Routers (`backend/pifitness/<domain>/router.py`):** HTTP layer per business domain (`activities`, `admin`, `auth`, `food`, `health`, `music`, `segments`, `tri_tip`, `volleyball`, `config`). Routers handle HTTP request parsing, payload validation, and invoke domain queries.
3. **Domain Queries (`backend/pifitness/<domain>/queries.py`):** Pure SQL / database access layer. Encapsulates parameterized SQL statements, keyset pagination, and result shaping.
4. **Core Configuration (`backend/pifitness/core/`):** Shared settings (`config.py`) loading environment variables (`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`). `core/` is a leaf module and must not import domain logic.
5. **Integrations (`backend/pifitness/integrations/`):** Dedicated API clients for external providers (Garmin, Spotify).
6. **Worker Engine (`backend/pifitness/worker/`):** Background task scheduling, queue processing, and data synchronization workers.
7. **Legacy Utilities (`backend/backend_functions/`):** Shared database connection helper (`get_conn`) and legacy utility routines.

#### Strict Import Rules
* Domain modules (`backend/pifitness/<domain>/`) may import `core/` and their own submodules.
* Domain modules do not import other domain modules directly; cross-domain communication happens via shared DB tables or dedicated service interfaces.
* `core/` imports nothing else in the repository.
* Nothing under `backend/` imports from `tools/`, `deploy/`, or test suites.

### Frontend Layers (`frontend/pifitness/src/`)
1. **App Router Pages (`src/app/`):** Next.js routes, layouts, and page-level views.
2. **Custom Hooks (`src/hooks/`):** React Query data-fetching, caching, and mutation hooks.
3. **API Client (`src/lib/api-client.ts`):** Centralized, typed HTTP client managing all `/api/` fetch operations.
4. **UI Components (`src/components/` & domain `components/`):** Modular React components built with Tailwind CSS.
5. **Client State (`src/stores/`):** Zustand stores for lightweight, client-only UI state (such as drawer or filter states).

---

## 3. Request & Execution Flows

### Synchronous Read Flow (e.g. `GET /api/food/diary?day=YYYY-MM-DD`)
1. **Browser:** Component calls a React Query hook $\rightarrow$ invokes `api-client.ts` $\rightarrow$ sends `fetch('/api/food/diary?day=...')`.
2. **Proxy:** In DEV, Next.js rewrites to `localhost:8000`; on the Pi, Nginx routes directly to FastAPI.
3. **Router:** `backend/pifitness/food/router.py` validates query parameters and calls `queries.get_diary_entries(day)`.
4. **Database:** `queries.py` executes parameterized SQL against PostgreSQL using connection pool `get_conn()`, applies `LIMIT`, shapes rows into response dicts, and returns JSON.

### Synchronous Write Flow (e.g. `POST /api/food/diary`)
1. **Browser:** Component submits payload via `api-client.ts` $\rightarrow$ `fetch('/api/food/diary', {method: 'POST', body: ...})`.
2. **Router:** Fast-fails on schema validation, calls `queries.log_diary_entry()`.
3. **Database:** Executes parameterized `INSERT ... RETURNING`, returning the committed entry to the UI for immediate cache update.

### Asynchronous Background Task Flow (Worker Engine)
1. **Task Dispatch:** Tasks are queued in PostgreSQL table `tasks.task_execution` either on a timer or via administrative trigger.
2. **Worker Execution:** Systemd timers (`pifitness_agent.timer`) or backend worker dispatchers poll pending tasks, execute the mapped Python routine or integration sync, and log execution timing and status to PostgreSQL.
3. **Token Caching:** External sync tasks utilize multi-tier session caching to avoid exceeding third-party API rate limits.

---

## 4. Hardware & Deployment Constraints

* **Compute & Concurrency:** The backend runs under a single Uvicorn process; the frontend runs under a single Node.js instance managed by systemd/pm2 on the Raspberry Pi 5. No multi-worker clusters are run on the host.
* **Bounded Reads:** Because memory is shared with database and OS on a single SBC, all list queries must be bounded. Endpoints enforce maximum page limits and employ keyset/cursor pagination over high-cardinality tables (avoiding unbounded `OFFSET`).
* **Static Assets:** Built directly on the Pi at deploy time (`npm run build`).

---

## 5. Architectural Invariants

1. **DEV/PRD Isolation:** Development strictly targets the local Docker container on port 15432 (`pifitness_dev`). PRD is only accessed via read-only backup sync scripts run manually by the human.
2. **Parameterized SQL:** All dynamic values in SQL must use database driver parameter placeholders (`%s`). String interpolation or formatting into SQL statements is prohibited.
3. **Single Active Feature:** Feature development occurs serially on the `feature-test` branch. No concurrent feature branches are maintained.
