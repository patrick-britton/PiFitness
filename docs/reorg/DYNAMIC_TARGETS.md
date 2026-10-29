# DYNAMIC_TARGETS.md

Real targets of every `importlib` loader (PY-010). These modules are
pinned in `audit.toml` and must not be moved without updating this file
and every location where the module name is stored (e.g. DB task registry,
seed SQL/JSON).

## Non-literal dynamic imports (PY-010, 6 sites)

All use `prefix=''` — the module name is computed at runtime, invisible to
static analysis.

| # | Location | Line | Loader | Module name source | Notes |
|---|----------|------|--------|-------------------|-------|
| 1 | `backend_functions/service_logins.py` | 720 | `importlib.import_module(module_name)` | `test_str.rsplit('.', 1)` — `test_str` from `one_sql_result(test_sql)` querying `api_services.api_service_list.api_service_function` | Runtime-resolved login test function |
| 2 | `backend_functions/task_execution.py` | 108 | `importlib.import_module(module_name)` | `task.get("python_function").rsplit('.', 1)` — from task table `python_function` column | Runtime-resolved local task function |
| 3 | `backend_functions/task_execution.py` | 146 | `importlib.import_module(module_name)` | `raw_api_function.rsplit('.', 1)` — `raw_api_function` from task table `api_service_function` column | Runtime-resolved API service client function |
| 4 | `backend_functions/ultimate_task_executioner_v2.py` | 137 | `importlib.import_module(module_name)` | `module_function.rsplit('.', 1)` — `module_function` from task execution context | Runtime-resolved login function |
| 5 | `backend_functions/ultimate_task_executioner_v2.py` | 275 | `importlib.import_module(module_name)` | `module_name = 'backend_functions.json_extractors'` (literal) | JSON extractor function; literal module name |
| 6 | `backend_functions/ultimate_task_executioner_v2.py` | 453 | `importlib.import_module(module_name)` | `module_function.rsplit('.', 1)` — `module_function` from task execution context | Runtime-resolved Python execution function |

## Pinned modules

The following modules are targets of dynamic loading and must not be
moved or renamed without updating this file:

| Module | Reason | Decision |
|--------|--------|----------|
| (none — all previously pinned orphan modules were deleted in Phase 1.6) | — | — |

## audit.toml pins

The following `[[dynamic_import]]` entries are pinned in
`scripts/repo_audit/audit.toml`:

| # | File | Line |
|---|------|------|
| 1 | `backend_functions/service_logins.py` | 720 |
| 2 | `backend_functions/task_execution.py` | 108 |
| 3 | `backend_functions/task_execution.py` | 146 |
| 4 | `backend_functions/ultimate_task_executioner_v2.py` | 137 |
| 5 | `backend_functions/ultimate_task_executioner_v2.py` | 275 |
| 6 | `backend_functions/ultimate_task_executioner_v2.py` | 453 |

These modules are targets of dynamic loading and must not be moved or renamed
without updating this file, `audit.toml`, and every location where the module
name is stored (e.g. DB task registry, seed SQL/JSON).