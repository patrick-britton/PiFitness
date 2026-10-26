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
| `backend_functions/admin_functions.py` | Dynamic-possible orphan (PY-009) | PIN |
| `backend_functions/running_functions.py` | Dynamic-possible orphan (PY-009) | PIN |
| `backend_functions/schemas/admin_schemas.py` | Dynamic-possible orphan (PY-009) | PIN |
| `backend_functions/data_helpers.py` | String-referenced orphan (PY-009) | KEEP |
| `backend_functions/db_wrapper.py` | String-referenced orphan (PY-009) | KEEP |
| `backend_functions/image_creation.py` | String-referenced orphan (PY-009) | KEEP |

## audit.toml pins

Add these to `[pinned_modules]` in `audit.toml` (or equivalent
`[[forbid]]` exemption list) so `repo_audit` does not flag them as dead
when they have no static importers.

**Default:** Pin all 6 modules above; no movement without human approval.