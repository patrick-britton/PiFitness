# DYNAMIC_TARGETS.md

Real targets of every `importlib` loader (PY-010). These modules are
pinned in `audit.toml` and must not be moved without updating this file
and every location where the module name is stored (e.g. DB task registry,
seed SQL/JSON).

## Non-literal dynamic imports (PY-010, 6 sites)

All use `prefix=''` — the module name is computed at runtime, invisible to
static analysis.

| # | Location | Line | Loader |
|---|----------|------|--------|
| 1 | `backend_functions/service_logins.py` | 720 | `importlib.import_module(...)` |
| 2 | `backend_functions/task_execution.py` | 108 | `importlib.import_module(...)` |
| 3 | `backend_functions/task_execution.py` | 146 | `importlib.import_module(...)` |
| 4 | `backend_functions/ultimate_task_executioner_v2.py` | 137 | `importlib.import_module(...)` |
| 5 | `backend_functions/ultimate_task_executioner_v2.py` | 275 | `importlib.import_module(...)` |
| 6 | `backend_functions/ultimate_task_executioner_v2.py` | 453 | `importlib.import_module(...)` |

## Pinned modules

The following modules are targets of dynamic loading and must not be
moved or renamed without updating this file:

| Module | Reason | Decision |
|--------|--------|----------|
| (none — all previously pinned orphan modules were deleted in Phase 1.6) | — | — |

## audit.toml pins

No pinned modules remain. All previously pinned orphan modules
(`admin_functions`, `running_functions`, `admin_schemas`, `data_helpers`,
`db_wrapper`, `image_creation`) were deleted in Phase 1.6 after verifying
zero code importers. The two Garmin vendored modules
(`pirate_garmin_endpoints`, `pirate_garmin_auth`) were deleted in Phase 1.5
after repointing `json_extractors.py` and `service_logins.py` to the
`pirate-garmin_clone` library.