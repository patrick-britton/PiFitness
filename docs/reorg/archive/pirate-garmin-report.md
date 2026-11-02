Now I have all the evidence. Here is the complete audit report.

---

# Pirate-garmin liveness audit
Date: 2026-10-30
Repo: c:\Users\rickb\vsProjects\PiFitness
HEAD: d218c01c08251cab333f4d27e305428cf010abae (2026-10-29 reorg(3.core.1): move config.py to pifitness.core.config)
Method: static analysis + dynamic-dispatch DB probe (DEV only)
Read-only: yes

## Executive summary

The `pirate_garmin` package is **not dead code**. It is **live in production** through two independent paths:

1. **LIVE-PROD (static):** `backend/main.py` (FastAPI) registers `pifitness.auth.router`, which exposes `POST /api/auth/garmin/login`. That handler calls `garmin_login()` (a deprecated wrapper in `service_logins.py`) which delegates to `pirate_garmin_login()`. `pirate_garmin_login()` instantiates `GarminClient.from_credentials()` and catches `GarminAuthError` / `MissingCredentialsError`. This path is reachable from a production entrypoint and actually invokes `pirate_garmin`-owned symbols.

2. **LIVE-DYNAMIC (DB-backed task registry):** `agents/background_task_execution.py` calls `ultimate_task_executioner()`, which dynamically imports function names from the DEV database. The DB contains 13 `tasks.vw_task_info` rows with `python_login_function = 'backend_functions.service_logins.pirate_garmin_login'` and `python_extraction_function = 'extract_pirate_universal'`, plus 12 `tasks.vw_task_execution` rows with `api_service_function = 'backend_functions.service_logins.get_garmin_client'`, plus 2 `api_services.api_service_list` rows. These resolve at runtime to `pirate_garmin`-owned code.

One symbol, `render_endpoint`, is **IMPORT-ONLY** (imported at module/function scope but never invoked in production code).

The package is installed as an editable package (`.venv/Lib/site-packages/_editable_impl_pirate_garmin.pth` → `pirate-garmin_clone/src`), and `deploy/scripts/pi5_deploy.sh` runs `pip install -e pirate-garmin_clone`.

## Classification table

| Integration point | Classification | Evidence (file:line) |
|-------------------|----------------|----------------------|
| `GarminClient` (pirate_garmin.client) | LIVE-PROD | service_logins.py:782 (called by pirate_garmin_login); chain: backend.main → pifitness.auth.router → service_logins.garmin_login → pirate_garmin_login → GarminClient.from_credentials |
| `GarminAuthError` (pirate_garmin.auth) | LIVE-PROD | service_logins.py:738 (import), 811 (caught); chain: backend.main → pifitness.auth.router → service_logins.garmin_login → pirate_garmin_login → except GarminAuthError |
| `MissingCredentialsError` (pirate_garmin.auth) | LIVE-PROD | service_logins.py:738 (import), 811 (caught); same chain as GarminAuthError |
| `pirate_garmin_login()` (service_logins.py:742) | LIVE-PROD + LIVE-DYNAMIC | service_logins.py:498,515,521,527,541 (calls); chain: backend.main → pifitness.auth.router → service_logins.garmin_login → pirate_garmin_login; also DB: tasks.vw_task_info python_login_function (13 rows) |
| `garmin_login()` (service_logins.py:490) | LIVE-PROD | service_logins.py:498 (delegates to pirate_garmin_login); auth/router.py:292 (called by trigger_garmin_login); chain: backend.main → pifitness.auth.router → trigger_garmin_login → garmin_login |
| `get_garmin_client()` (service_logins.py:501) | LIVE-DYNAMIC | service_logins.py:501 (definition, no static caller); DB: tasks.vw_task_execution api_service_function (12 rows), api_services.api_service_list (2 rows); dynamic call via task_execution.py:146-150 |
| `resolve_endpoint()` (pirate_garmin.endpoints) | LIVE-DYNAMIC | json_extractors.py:485 (import), 488 (call); chain: agents.background_task_execution → ultimate_task_executioner → [DB python_extraction_function=extract_pirate_universal] → extract_pirate_universal → get_pirate_data → resolve_endpoint |
| `render_endpoint()` (pirate_garmin.endpoints) | IMPORT-ONLY | json_extractors.py:485 (import only — NOT called anywhere in production); no DB rows; no test coverage in production tests |
| `extract_pirate_universal()` (json_extractors.py:525) | LIVE-DYNAMIC | json_extractors.py:525 (definition), 588 (calls get_pirate_data); ultimate_task_executioner_v2.py:291 (dynamic call); DB: tasks.vw_task_info python_extraction_function (13 rows) |
| `get_pirate_data()` (json_extractors.py:469) | LIVE-DYNAMIC | json_extractors.py:469 (definition), 588 (called by extract_pirate_universal); DB: indirectly via extract_pirate_universal rows |

## Integration inventory

### Symbol: GarminClient
- Home: pirate-garmin_clone/src/pirate_garmin/client.py (class definition)
- Local references:
  - `backend/backend_functions/service_logins.py:782` — `client = GarminClient.from_credentials(username=email, password=password, app_dir=str(cache_dir))` (call site inside `pirate_garmin_login()`)
- Shortest entrypoint chain: `backend.main → pifitness.auth.router → backend_functions.service_logins.garmin_login → backend_functions.service_logins.pirate_garmin_login → GarminClient.from_credentials`
- Also reachable via DYNAMIC: `agents.background_task_execution → backend_functions.ultimate_task_executioner_v2.ultimate_task_executioner → [DB python_login_function='backend_functions.service_logins.pirate_garmin_login'] → pirate_garmin_login → GarminClient.from_credentials`
- DB-loaded: yes — `api_services.api_service_list.api_service_function = 'backend_functions.service_logins.pirate_garmin_login'`; `tasks.vw_task_info.python_login_function = 'backend_functions.service_logins.pirate_garmin_login'` (13 rows)
- Test coverage: `tests/test_empty_queue_noop.py:90,117` — `monkeypatch.setattr(sl, 'pirate_garmin_login', ...)` (stubs the function that calls GarminClient)
- Classification: **LIVE-PROD**
- Confidence: **high** — GarminClient.from_credentials() is actually invoked inside pirate_garmin_login(), which is called by the production /garmin/login endpoint and by DB-driven tasks

### Symbol: GarminAuthError
- Home: pirate-garmin_clone/src/pirate_garmin/auth.py (exception class)
- Local references:
  - `backend/backend_functions/service_logins.py:738` — `from pirate_garmin.auth import GarminAuthError, MissingCredentialsError` (import)
  - `backend/backend_functions/service_logins.py:811` — `except (GarminAuthError, MissingCredentialsError) as e:` (caught in pirate_garmin_login)
- Shortest entrypoint chain: `backend.main → pifitness.auth.router → backend_functions.service_logins.garmin_login → backend_functions.service_logins.pirate_garmin_login → except (GarminAuthError, ...)`
- DB-loaded: yes (via pirate_garmin_login DB rows)
- Test coverage: `tests/test_empty_queue_noop.py:90,117` (monkeypatches pirate_garmin_login which raises/catches these)
- Classification: **LIVE-PROD**
- Confidence: **high** — imported and actually caught in the except clause of pirate_garmin_login(), which is production-reachable

### Symbol: MissingCredentialsError
- Home: pirate-garmin_clone/src/pirate_garmin/auth.py (exception class)
- Local references:
  - `backend/backend_functions/service_logins.py:738` — import (same line as GarminAuthError)
  - `backend/backend_functions/service_logins.py:811` — caught in except clause
- Shortest entrypoint chain: same as GarminAuthError
- DB-loaded: yes (via pirate_garmin_login DB rows)
- Test coverage: `tests/test_empty_queue_noop.py:90,117`
- Classification: **LIVE-PROD**
- Confidence: **high**

### Symbol: pirate_garmin_login()
- Home: `backend/backend_functions/service_logins.py:742` — `def pirate_garmin_login(headless=False):`
- Local references (calls to it):
  - `service_logins.py:498` — `return pirate_garmin_login()` (inside deprecated `garmin_login()`)
  - `service_logins.py:515` — `new_token = pirate_garmin_login()` (inside `get_garmin_client()`)
  - `service_logins.py:521` — `new_token = pirate_garmin_login()` (inside `get_garmin_client()`)
  - `service_logins.py:527` — `new_token = pirate_garmin_login()` (inside `get_garmin_client()`)
  - `service_logins.py:541` — `new_token = pirate_garmin_login()` (inside `get_garmin_client()`)
- Shortest entrypoint chain (static): `backend.main → pifitness.auth.router → backend_functions.service_logins.garmin_login → backend_functions.service_logins.pirate_garmin_login`
- Shortest entrypoint chain (dynamic): `agents.background_task_execution → backend_functions.ultimate_task_executioner_v2.ultimate_task_executioner → [DB tasks.vw_task_info.python_login_function] → backend_functions.service_logins.pirate_garmin_login`
- DB-loaded: yes — `api_services.api_service_list` (1 row), `tasks.vw_task_info` (13 rows, all `Pirate Garmin` service tasks)
- Test coverage: `tests/test_empty_queue_noop.py:87,114` (python_login_function string), `:90,117` (monkeypatch)
- Classification: **LIVE-PROD** and **LIVE-DYNAMIC**
- Confidence: **high** — called by production FastAPI endpoint and by 13 DB-registered tasks

### Symbol: garmin_login() (deprecated wrapper)
- Home: `backend/backend_functions/service_logins.py:490` — `def garmin_login():`
- Local references:
  - `service_logins.py:498` — `return pirate_garmin_login()` (delegates)
  - `backend/pifitness/auth/router.py:24` — `garmin_login,` (imported from service_logins)
  - `backend/pifitness/auth/router.py:292` — `client = garmin_login()` (called by `trigger_garmin_login`)
- Shortest entrypoint chain: `backend.main → pifitness.auth.router → trigger_garmin_login → garmin_login → pirate_garmin_login`
- DB-loaded: no direct DB rows (but delegates to pirate_garmin_login which has DB rows)
- Test coverage: none directly (tests monkeypatch pirate_garmin_login, not garmin_login)
- Classification: **LIVE-PROD** (deprecated wrapper, but actively called in production)
- Confidence: **high** — `trigger_garmin_login()` at auth/router.py:292 is a production HTTP handler

### Symbol: get_garmin_client()
- Home: `backend/backend_functions/service_logins.py:501` — `def get_garmin_client(incoming_token=None):`
- Local references: definition only at :501; no static callers found in any Python source file
- Shortest entrypoint chain: DYNAMIC only — `agents.background_task_execution → backend_functions.task_execution.task_executioner → [DB tasks.vw_task_execution.api_service_function='backend_functions.service_logins.get_garmin_client'] → task_execution.py:146-150 → get_garmin_client(client_dict)`
  - Note: `task_execution.py` is not directly imported by `backend/main.py` or `agents/background_task_execution.py` in the current codebase, but it is a documented worker entry and is referenced by `deploy/systemd/` patterns. The `ultimate_task_executioner_v2` path (used by `agents/background_task_execution.py`) does NOT call `get_garmin_client` directly — it calls `pirate_garmin_login` via `python_login_function`.
- DB-loaded: yes — `api_services.api_service_list` (2 rows including get_garmin_client), `tasks.vw_task_execution` (12 rows with `api_service_function = 'backend_functions.service_logins.get_garmin_client'`)
- Test coverage: none directly
- Classification: **LIVE-DYNAMIC**
- Confidence: **medium** — DB rows exist and task_execution.py has the dynamic import logic, but `task_execution.py` itself is not currently imported by any active entrypoint (only `ultimate_task_executioner_v2.py` is, via `agents/background_task_execution.py`). The 12 DB rows with `get_garmin_client` as `api_service_function` would be activated if `task_executioner()` is run.

### Symbol: resolve_endpoint()
- Home: pirate-garmin_clone/src/pirate_garmin/endpoints.py:355 — `def resolve_endpoint(key: str) -> Endpoint:`
- Local references:
  - `backend/backend_functions/json_extractors.py:485` — `from pirate_garmin.endpoints import resolve_endpoint, render_endpoint` (import inside `get_pirate_data()`)
  - `backend/backend_functions/json_extractors.py:488` — `endpoint_def = resolve_endpoint(endpoint)` (call site)
- Shortest entrypoint chain (DYNAMIC-REACHABLE): `agents.background_task_execution → backend_functions.ultimate_task_executioner_v2.ultimate_task_executioner → [DB tasks.vw_task_info.python_extraction_function='extract_pirate_universal'] → backend_functions.json_extractors.extract_pirate_universal → json_extractors.get_pirate_data → resolve_endpoint`
- DB-loaded: yes — `tasks.vw_task_info.python_extraction_function = 'extract_pirate_universal'` (13 rows)
- Test coverage: `tests/test_empty_queue_noop.py:41` (calls extract_pirate_universal which calls get_pirate_data which calls resolve_endpoint)
- Classification: **LIVE-DYNAMIC**
- Confidence: **high** — resolve_endpoint() is called inside get_pirate_data(), which is called by extract_pirate_universal(), which is DB-registered for 13 tasks

### Symbol: render_endpoint()
- Home: pirate-garmin_clone/src/pirate_garmin/endpoints.py:362 — `def render_endpoint(endpoint, path_values):`
- Local references:
  - `backend/backend_functions/json_extractors.py:485` — `from pirate_garmin.endpoints import resolve_endpoint, render_endpoint` (import only)
- Call sites in production: **none** — `render_endpoint` is imported at `json_extractors.py:485` but never invoked. Only `resolve_endpoint` is called (line 488).
- Shortest entrypoint chain: none (no callers)
- DB-loaded: no
- Test coverage: none in production tests (clone's `tests/test_endpoints.py` tests it, but that's within the clone)
- Classification: **IMPORT-ONLY**
- Confidence: **high** — imported but never called in the production codebase

### Symbol: extract_pirate_universal()
- Home: `backend/backend_functions/json_extractors.py:525` — `def extract_pirate_universal(client=None, td=None, aid=None):`
- Local references:
  - `json_extractors.py:525` — definition
  - `json_extractors.py:588` — `raw_json, error = get_pirate_data(...)` (calls get_pirate_data internally)
  - `backend/backend_functions/ultimate_task_executioner_v2.py:271-291` — dynamic import of `backend_functions.json_extractors` and call to `local_function(client=cd.get('client'), td=td)` where `extract_function_name = td.get('python_extraction_function')`
- Shortest entrypoint chain (DYNAMIC-REACHABLE): `agents.background_task_execution → backend_functions.ultimate_task_executioner_v2.ultimate_task_executioner → [DB tasks.vw_task_info.python_extraction_function='extract_pirate_universal'] → json_extractors.extract_pirate_universal`
- DB-loaded: yes — `tasks.vw_task_info.python_extraction_function = 'extract_pirate_universal'` (13 rows)
- Test coverage: `tests/test_empty_queue_noop.py:29` (python_extraction_function string), `:41` (direct invocation `je.extract_pirate_universal(...)`), `:69` (monkeypatch of extract_pirate_universal)
- Classification: **LIVE-DYNAMIC**
- Confidence: **high** — DB-registered for 13 tasks, dynamically invoked by ultimate_task_executioner

### Symbol: get_pirate_data()
- Home: `backend/backend_functions/json_extractors.py:469` — `def get_pirate_data(client, endpoint, path_params=None, query_params=None):`
- Local references:
  - `json_extractors.py:469` — definition
  - `json_extractors.py:485` — `from pirate_garmin.endpoints import resolve_endpoint, render_endpoint` (import inside this function)
  - `json_extractors.py:488` — `resolve_endpoint(endpoint)` (call inside this function)
  - `json_extractors.py:588` — `raw_json, error = get_pirate_data(client=client, endpoint=endpoint, ...)` (called by extract_pirate_universal)
- Shortest entrypoint chain (DYNAMIC-REACHABLE): `agents.background_task_execution → ultimate_task_executioner → [DB python_extraction_function=extract_pirate_universal] → extract_pirate_universal → get_pirate_data → [pirate_garmin.endpoints] resolve_endpoint`
- DB-loaded: yes (indirectly via extract_pirate_universal DB rows)
- Test coverage: `tests/test_empty_queue_noop.py:105` (monkeypatch of get_pirate_data)
- Classification: **LIVE-DYNAMIC**
- Confidence: **high**

## Test impact

Tests that will fail if `pirate_garmin` is removed:

| Test | File:line | Reason |
|------|-----------|--------|
| `test_empty_iterator_raises_no_work` | tests/test_empty_queue_noop.py:36-47 | Calls `je.extract_pirate_universal(client=object(), td=td)` directly — does not import pirate_garmin but exercises the code path that calls `get_pirate_data` → `pirate_garmin.endpoints.resolve_endpoint` |
| `test_extract_load_flatten_treats_no_work_as_success` | tests/test_empty_queue_noop.py:50-74 | Monkeypatches `extract_pirate_universal` (which is the function that calls `get_pirate_data` → `pirate_garmin.endpoints`) |
| `test_executioner_reconciles_no_work_as_success` | tests/test_empty_queue_noop.py:77-99 | Sets `python_login_function='backend_functions.service_logins.pirate_garmin_login'` (line 87) and `monkeypatch.setattr(sl, 'pirate_garmin_login', ...)` (line 90) |
| `test_nonempty_all_failed_still_fails` | tests/test_empty_queue_noop.py:102-126 | Sets `python_login_function='backend_functions.service_logins.pirate_garmin_login'` (line 114) and `monkeypatch.setattr(sl, 'pirate_garmin_login', ...)` (line 117); monkeypatches `get_pirate_data` (line 105) |

Clone-internal tests (would fail if the clone itself is deleted):
- `pirate-garmin_clone/tests/test_auth.py` — imports `pirate_garmin.auth`, `pirate_garmin.browser_login`, `pirate_garmin.client`
- `pirate-garmin_clone/tests/test_browser_login.py` — imports `pirate_garmin.browser_login`
- `pirate-garmin_clone/tests/test_endpoints.py` — imports `pirate_garmin.auth`, `pirate_garmin.endpoints`

## DB impact

Every DB row referencing a pirate_garmin-related name (DEV database, `pifitness_dev`):

### `api_services.api_service_list` (table, column: `api_service_function`)
| Value |
|-------|
| `backend_functions.service_logins.get_garmin_client` |
| `backend_functions.service_logins.pirate_garmin_login` |

### `tasks.vw_task_info` (view, column: `python_login_function`)
| task_name | api_service_name | python_login_function |
|-----------|-----------------|----------------------|
| Steps | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Heartrate Variation (HRV) | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Activities | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Sleep | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Respiration | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Body Composition - 6 AM | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Heartrate | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Stress | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Body Battery | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Race Predictions | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Training Status | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Fitness Age | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |
| Activity Details | Pirate Garmin | backend_functions.service_logins.pirate_garmin_login |

### `tasks.vw_task_info` (view, column: `python_extraction_function`)
Same 13 tasks, all with `python_extraction_function = 'extract_pirate_universal'`.

### `tasks.vw_task_execution` (view, column: `api_service_function`)
| task_name | api_service_name | api_service_function |
|-----------|-----------------|----------------------|
| Sync Sleep | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Training Status | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Race Predictions | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Body Composition | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Fitness Age | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync HRV | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Body Battery | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Stress | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Garmin Activities | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Heartrate | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Respiration | Garmin | backend_functions.service_logins.get_garmin_client |
| Sync Steps | Garmin | backend_functions.service_logins.get_garmin_client |

This is the complete list a human would need to update (set to NULL or remove) to safely remove the code.

## What would need to change to remove pirate_garmin entirely

Ordered list of changes:

1. **`backend/pifitness/auth/router.py:24`** — Remove `garmin_login` from the import from `backend_functions.service_logins`. (Diff: ~1 line removed, risk: low, test coverage: tests/test_empty_queue_noop.py doesn't test this import directly)

2. **`backend/pifitness/auth/router.py:284-307`** — Delete the `trigger_garmin_login()` function and its `@router.post("/garmin/login")` decorator (the `/api/auth/garmin/login` endpoint). (Diff: ~25 lines removed, risk: medium — this is a production API endpoint used by the React frontend; test coverage: none directly)

3. **`backend/backend_functions/service_logins.py:738-739`** — Remove `from pirate_garmin.auth import GarminAuthError, MissingCredentialsError` and `from pirate_garmin.client import GarminClient`. (Diff: 2 lines removed, risk: low)

4. **`backend/backend_functions/service_logins.py:742-820`** — Delete the `pirate_garmin_login()` function (~80 lines). (Diff: ~80 lines removed, risk: medium — DB rows still reference it; must be handled first)

5. **`backend/backend_functions/service_logins.py:490-498`** — Delete the deprecated `garmin_login()` wrapper (~10 lines). (Diff: ~10 lines removed, risk: low — step 2 removes its only caller)

6. **`backend/backend_functions/service_logins.py:501-547`** — Delete `get_garmin_client()` (~47 lines). (Diff: ~47 lines removed, risk: low — no static callers; only DB-referenced)

7. **`backend/backend_functions/json_extractors.py:485`** — Remove `from pirate_garmin.endpoints import resolve_endpoint, render_endpoint` from inside `get_pirate_data()`. (Diff: 1 line removed, risk: low)

8. **`backend/backend_functions/json_extractors.py:469-516`** — Delete `get_pirate_data()` (~48 lines). (Diff: ~48 lines removed, risk: medium — called by extract_pirate_universal which is DB-referenced)

9. **`backend/backend_functions/json_extractors.py:525-650+)`** — Delete `extract_pirate_universal()` (~130+ lines). (Diff: ~130+ lines removed, risk: medium — DB-referenced for 13 tasks)

10. **Database rows** — Update or delete 13 `tasks.vw_task_info` rows (set `python_login_function` and `python_extraction_function` to NULL), 12 `tasks.vw_task_execution` rows (set `api_service_function` to NULL), and 2 `api_services.api_service_list` rows. (Risk: high — these are the runtime activation mechanism; if not cleared, dynamic imports will fail at runtime)

11. **`deploy/scripts/pi5_deploy.sh:154-156`** — Remove the `pip install -e pirate-garmin_clone` lines. (Diff: ~3 lines removed, risk: low)

12. **`pirate-garmin_clone/` directory** — Delete the entire cloned package. (Diff: ~15 files, risk: low — after all above steps)

13. **`.venv/Lib/site-packages/_editable_impl_pirate_garmin.pth`** and `pirate_garmin-0.1.0.dist-info` — Remove from venv. (Risk: low — venv artifact)

**Total estimated diff:** ~350+ lines removed across ~10 files, plus DB row updates.

**Risk assessment:** Medium-high overall. The static path (steps 1-6) is low risk because it's a single endpoint. The dynamic path (steps 7-10) is higher risk because 25 DB rows across 3 tables/views actively reference these functions at runtime; missing a row would cause runtime import failures in the task executioner.

## Confidence assessment

- **Import graph: high** — All static imports of `pirate_garmin` were found via grep and verified by reading the files. The two module-scope imports in `service_logins.py:738-739` and the function-scope import in `json_extractors.py:485` are the only production-code references to the library itself.
- **Dynamic dispatch: high** — The DEV database was queried successfully. 25 rows across `api_services.api_service_list`, `tasks.vw_task_info`, and `tasks.vw_task_execution` reference `pirate_garmin_login`, `get_garmin_client`, or `extract_pirate_universal`. The dynamic import sites in `task_execution.py:108,146` and `ultimate_task_executioner_v2.py:137,275,453` were verified by reading the code.
- **Test coverage: high** — `tests/test_empty_queue_noop.py` directly exercises the pirate-garmin code path (4 tests, all monkeypatching `pirate_garmin_login` and/or `get_pirate_data` and/or `extract_pirate_universal`).
- **Overall: confidently live** — `pirate_garmin` is live in production via both a static path (FastAPI `/garmin/login` endpoint) and a dynamic path (DB-backed task registry with 25 rows). Only `render_endpoint` is import-only/dead.

## Raw evidence appendix

### Phase 1 — Static grep results (production code only, excluding .audit, .pytest_cache, clone-internal)

```
backend/backend_functions/service_logins.py:738: from pirate_garmin.auth import GarminAuthError, MissingCredentialsError
backend/backend_functions/service_logins.py:739: from pirate_garmin.client import GarminClient
backend/backend_functions/json_extractors.py:485: from pirate_garmin.endpoints import resolve_endpoint, render_endpoint
backend/backend_functions/service_logins.py:498: return pirate_garmin_login()
backend/backend_functions/service_logins.py:515: new_token = pirate_garmin_login()
backend/backend_functions/service_logins.py:521: new_token = pirate_garmin_login()
backend/backend_functions/service_logins.py:527: new_token = pirate_garmin_login()
backend/backend_functions/service_logins.py:541: new_token = pirate_garmin_login()
backend/backend_functions/service_logins.py:742: def pirate_garmin_login(headless=False):
backend/backend_functions/service_logins.py:501: def get_garmin_client(incoming_token=None):
backend/backend_functions/service_logins.py:490: def garmin_login():
backend/pifitness/auth/router.py:24:     garmin_login,
backend/pifitness/auth/router.py:292: client = garmin_login()
tests/test_empty_queue_noop.py:87: python_login_function='backend_functions.service_logins.pirate_garmin_login'
tests/test_empty_queue_noop.py:90: monkeypatch.setattr(sl, 'pirate_garmin_login',
tests/test_empty_queue_noop.py:114: python_login_function='backend_functions.service_logins.pirate_garmin_login'
tests/test_empty_queue_noop.py:117: monkeypatch.setattr(sl, 'pirate_garmin_login',
deploy/scripts/pi5_deploy.sh:154-156: pip install -e pirate-garmin_clone
.venv/Lib/site-packages/_editable_impl_pirate_garmin.pth: C:\Users\rickb\vsProjects\PiFitness\pirate-garmin_clone\src
```

### Phase 2 — Import chains

**Static (production):**
```
backend.main
  -> pifitness.auth.router (line 37 import, line 134 include_router)
    -> backend_functions.service_logins (router.py:21-32 import garmin_login)
      -> trigger_garmin_login() (router.py:284-307)
        -> garmin_login() (router.py:292)
          -> service_logins.garmin_login() (service_logins.py:490-498)
            -> return pirate_garmin_login() (service_logins.py:498)
              -> service_logins.pirate_garmin_login() (service_logins.py:742-820)
                -> GarminClient.from_credentials() (service_logins.py:782)
                -> except (GarminAuthError, MissingCredentialsError) (service_logins.py:811)
```

**Dynamic (DB-backed):**
```
agents.background_task_execution
  -> backend_functions.ultimate_task_executioner_v2.ultimate_task_executioner() (background_task_execution.py:9,19)
    -> [DB tasks.vw_task_info.python_login_function = 'backend_functions.service_logins.pirate_garmin_login'] (ultimate_task_executioner_v2.py:119-139)
      -> importlib.import_module('backend_functions.service_logins') (line 136)
      -> getattr(module, 'pirate_garmin_login') (line 137)
      -> pirate_garmin_login(client_dict) (line 139)
    -> [DB tasks.vw_task_info.python_extraction_function = 'extract_pirate_universal'] (ultimate_task_executioner_v2.py:270-291)
      -> importlib.import_module('backend_functions.json_extractors') (line 274)
      -> getattr(module, 'extract_pirate_universal') (line 275)
      -> extract_pirate_universal(client=..., td=...) (line 291)
        -> json_extractors.get_pirate_data() (json_extractors.py:588)
          -> from pirate_garmin.endpoints import resolve_endpoint, render_endpoint (json_extractors.py:485)
          -> resolve_endpoint(endpoint) (json_extractors.py:488)
```

**Dynamic (task_execution.py — not currently wired to an active entrypoint, but DB rows exist):**
```
backend_functions.task_execution.task_executioner() (task_execution.py:15)
  -> [DB tasks.vw_task_execution.api_service_function = 'backend_functions.service_logins.get_garmin_client'] (task_execution.py:137-150)
    -> importlib.import_module('backend_functions.service_logins') (line 146)
    -> getattr(module, 'get_garmin_client') (line 149)
    -> get_garmin_client(client_dict) (line 150)
```

### Phase 4 — DB query results (DEV: pifitness_dev, PG_HOST=localhost, PG_PORT=15432)

```
Query: SELECT DISTINCT api_service_function FROM api_services.api_service_list WHERE api_service_function LIKE '%pirate%' OR api_service_function LIKE '%garmin%';
Result: backend_functions.service_logins.get_garmin_client, backend_functions.service_logins.pirate_garmin_login (2 rows)

Query: SELECT DISTINCT python_login_function FROM tasks.vw_task_info WHERE python_login_function LIKE '%pirate%' OR python_login_function LIKE '%garmin%';
Result: backend_functions.service_logins.pirate_garmin_login (1 row, 13 task instances)

Query: SELECT DISTINCT python_extraction_function FROM tasks.vw_task_info WHERE python_extraction_function LIKE '%pirate%' OR python_extraction_function LIKE '%garmin%';
Result: extract_pirate_universal (1 row, 13 task instances)

Query: SELECT DISTINCT api_service_function FROM tasks.vw_task_execution WHERE api_service_function LIKE '%pirate%' OR api_service_function LIKE '%garmin%';
Result: backend_functions.service_logins.get_garmin_client (1 row, 12 task instances)
```

### Phase 6 — Git history

```
pirate-garmin_clone/ (3 commits, all 2026-04-01):
  490dbcf Incorporation of cloned pirate_garmin with headless login
  c325352 Incorporation of cloned pirate_garmin with headless login
  4545b31 Incorporation of cloned pirate_garmin with headless login

backend/backend_functions/service_logins.py (1 commit):
  a086c60 2026-10-29 reorg(3.prep-root): unify Python root + fix credential_management

backend/backend_functions/json_extractors.py (1 commit):
  a086c60 2026-10-29 reorg(3.prep-root): unify Python root + fix credential_management

HEAD: d218c01c 2026-10-29 reorg(3.core.1): move config.py to pifitness.core.config
```

---

**Bottom line:** `pirate_garmin` is **live**, not dead. It is actively used in production via the FastAPI `/api/auth/garmin/login` endpoint (static path) and via 25 DB-registered task rows that dynamically import `pirate_garmin_login`, `get_garmin_client`, and `extract_pirate_universal` at runtime (dynamic path). Only `render_endpoint` is import-only. Removing the package would require updating 25 DB rows, deleting a production API endpoint, and removing ~350 lines of code across 10 files.