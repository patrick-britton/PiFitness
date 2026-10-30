# PiFitness Reorg — HANDOFF

**Repo:** `C:\users\rickb\vsprojects\pifitness`
**Branch:** `reorg/02-moves`
**Last commit:** `d218c01 reorg(3.core.1): move config.py to pifitness.core.config [3.core.1]`
**Working tree:** clean (verified at handoff)
**Python:** 3.14.0 in `.venv` (`C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\python.exe`)
**Test baseline:** 210 tests collected; full suite passes in ~2:40.

This file is the entry point. Read it fully before doing anything. It describes the process discipline, the state, the tools, the pending work, and the anti-patterns that have already cost cycles. `docs/reorg/PLAN.md` is the source of truth for task status; this file condenses it and adds operational context.

---

## 1. Mission

Reorganize the repo from technical-layer layout to domain-sliced layout with no runtime behavior change, no new features, no dependency upgrades. Definition of done is in `PLAN.md` §6 (Phase 6 is queued Python 3.15 upgrade — not part of this reorg's done criteria).

Non-goals: adding features, reformatting-only diffs, DB object renames, rewriting git history, touching the Pi.

---

## 2. Process discipline (non-negotiable; this is how the last 3 tasks succeeded)

Every task follows **one-step-at-a-time, implement-then-verify**:

1. **Block A — Preconditions (read-only).** State check (HEAD, dirty count), file existence, importers list, baseline imports, test collection count. Write nothing to source. End with STOP; human pastes output.
2. **Block B — Dry-run or write-only.** For moves, tool's `--dry-run`; for tool writes, write tool + fixture + test to disk, do not run the test. End with STOP.
3. **Block C — Apply + verify + commit.** Move/rename, verify imports, compileall, full pytest, update `PLAN.md` ledger, commit. `git commit` is gated by pytest exit 0. End with STOP.

Not every task needs three blocks; the invariant is: **destructive action is gated by a verified checkpoint in a previous block.** Never put a destructive command in the same block as its preflight check.

### Rules for building blocks

- **No `exit` in interactive PowerShell blocks.** `exit` kills the shell, closing the window and losing output. Use a `$failed = $false` sentinel; set `$failed = $true` on failure; gate downstream sections on `if (-not $failed)`. End with `*** Block X complete ***` (green) or `*** Block X stopped ***` (yellow).
- **All `.ps1` files and all fixtures are ASCII-only.** Windows PowerShell 5.1 reads `.ps1` via `-File` using the system codepage when no BOM is present; any non-ASCII character (`—`, `→`, smart quotes) corrupts parsing. Use `-` for dashes, `->` for arrows, `"` for quotes.
- **All writes to disk use `[System.IO.File]::WriteAllText($path, $text, $utf8NoBom)`.** `Out-File -Encoding utf8` writes a BOM. BOMs break JSON parsers and stray into `.py` files.
- **Every block ends by echoing what was done and what state is now.** Human pastes the full output; do not summarize for them.
- **State check at the top of every block.** Confirm HEAD matches the expected sha and dirty count matches expectation. If not, STOP without running the rest.
- **Full pytest is the gate, not the check.** Individual import checks are preflight; only `pytest -q` returning exit 0 authorizes a commit. `210 passed` is the current baseline.
- **Rollback is `git reset --hard HEAD`** for uncommitted work, `git reset --hard HEAD~1` for a bad commit. Tree must be clean when starting each block.

---

## 3. Repo layout (post-`3.prep-root`)

```
pifitness/
├── .venv/                                    # python 3.14 venv; runs `$py` commands
├── .audit/
│   ├── baseline.json                         # frozen audit baseline
│   ├── REPORT.md                             # current audit scoreboard
│   ├── report.json                           # full audit output (JSON)
│   ├── sections/*.json                       # per-section details
│   └── discovery/                            # gitignored evidence per task
│       ├── *.txt / *.json                    # block outputs, manifests
│       └── build_ledger.py / probe-rope.py   # diagnostic scripts (untracked, .audit-scoped)
├── .clinerules/workflows/*.md                # Cline workflow definitions
├── .agents/skills/{name}/SKILL.md            # Cline skill definitions
├── .features/                                # AGENTIC DEV WORKSPACE — NOT IN GIT, skip in all walks
├── agents/                                   # Python background-task entry
├── backend/                                  # ← unified Python root (post-3.prep-root)
│   ├── __init__.py
│   ├── main.py                               # current entrypoint: `import backend.main`
│   ├── pifitness/                            # the ONE importable package
│   │   ├── __init__.py
│   │   ├── admin/  activities/  auth/        # domain slices (each has router.py, __init__.py, ...)
│   │   ├── config/  exercises/  food/        # ← `config/` here is the router (imports from pifitness.config)
│   │   ├── health/  music/  segments/        #    NOT the settings module (that's backend_functions.config, moved)
│   │   ├── tri_tip/  volleyball/
│   │   └── core/                             # ← NEW from 3.core.1
│   │       ├── __init__.py
│   │       └── config.py                     # ← was backend_functions/config.py
│   ├── backend_functions/                    # ← legacy, being drained (26 files)
│   │   ├── __init__.py
│   │   ├── config.py? — GONE (moved to pifitness/core/)
│   │   ├── credential_management.py          # FIXED (lazy load_key, correct .env path)
│   │   ├── database_functions.py             # imports pifitness.core.config
│   │   ├── domain_exceptions.py              # ← 3.core.2 target: rename to exceptions.py
│   │   ├── logging_functions.py              # ← 3.core.2 target: rename to logging.py
│   │   ├── music_functions.py  service_logins.py  task_execution.py
│   │   ├── ultimate_task_executioner_v2.py   # ← 3.worker target
│   │   ├── json_extractors.py                # ← 3.worker target
│   │   └── {others}
│   ├── pifitness.egg-info                    # editable install marker
│   └── pyproject.toml                        # (may be stale post-3.prep-root; verify in 3.0.verify)
├── deploy/                                   # systemd/, nginx/, scripts/
│   ├── scripts/
│   │   ├── bootstrap.sh  pi5_deploy.sh  deploy_react.sh
│   │   ├── library_requirements.txt  npm_requirements.txt
│   │   └── requirements-react.txt            # ← MISSING (T2.5.dep)
├── docs/
│   ├── reorg/
│   │   ├── BRIEF.md                          # original spec
│   │   ├── PLAN.md                           # SOURCE OF TRUTH for status
│   │   ├── HANDOFF.md                        # this file
│   │   ├── DYNAMIC_TARGETS.md                # 4 modules DB-loaded at runtime
│   │   ├── archive/PLAN_v1.md
│   │   ├── CUTOVER.md  LOG.md  BLOCKERS.md   # (may exist, verify)
│   │   └── moves/*.json                       # (may exist, unused)
│   └── architecture/                         # (empty — will hold patterns.md, folder-map.md post-reorg)
├── frontend/pifitness/                       # Next.js app (Phase 4 target: flatten to frontend/)
├── pirate-garmin_clone/                      # sibling project — slated for deletion, skip in walks
├── memory-bank/                              # LEGACY — slated for deletion, skip in walks
├── local_storage/                            # data files, skip in walks
├── scripts/                                  # mostly migrated to tools/ (verify state)
├── tests/                                    # 210 tests
├── tools/
│   ├── refactor/
│   │   ├── move_python.py                    # ← THE move tool (see §4)
│   │   └── tests/
│   │       ├── test_move_python.ps1          # 4-case fixture test
│   │       └── fixture/                      # pkg_a/, pkg_b/, user*.py, manifest.json
│   ├── audit/  codemod/  db/  diagnostics/  features/
└── pytest.ini                                # pythonpath = backend backend/pifitness
```

**`import` roots:** `backend/` is on sys.path via `.venv/Lib/site-packages/_backend_root.pth` (written during 3.prep-root; not committed). `pifitness` and `backend_functions` are both importable as top-level packages. `backend.main` is importable.

---

## 4. Key tools

### `tools/refactor/move_python.py` — move/rename Python modules with import rewriting

**Engine:** libcst AST transformer (proven against 4 fixture cases including comment/string preservation).

**Invocation:**
```powershell
$py = ".venv\Scripts\python.exe"
$pkgMap = "backend_functions=backend/backend_functions,pifitness=backend/pifitness"

# Dry-run (safe, no writes):
& $py tools\refactor\move_python.py <manifest.json> --pkg-map $pkgMap --dry-run

# Apply (moves file, rewrites imports):
& $py tools\refactor\move_python.py <manifest.json> --pkg-map $pkgMap --apply
```

**Manifest:** `{"old.module.path": "new.module.path", ...}` — JSON, UTF-8 no BOM.

**Behavior:**
- Move-only (`pkg_a.foo` → `pkg_b.foo`): `os.rename`, imports rewritten
- Rename-only (`pkg_a.foo` → `pkg_a.bar`): `os.rename`, imports rewritten
- Move+rename (`pkg_a.foo` → `pkg_b.baz`): both
- Auto-creates `__init__.py` in dest folder
- Only rewrites `Import`/`ImportFrom` AST nodes — comments and strings pass through byte-identical
- Handles multi-name imports (`from X import a, b`), aliases (`as h`), submodule moves

**Known issues (T0.5.3 pending fix):**
- **Walk scope too broad:** currently walks `.audit/`, `.features/`, `memory-bank/`, `pirate-garmin_clone/`, `local_storage/`, `docs/`. These should be in `SKIP_DIRS`. `.features` should be blanket-ignored per user decision (2026-10-30).
- **Counter over-reports:** "rewrote imports in N files" counts `new_text != text` (libcst round-trip artifact) rather than actual AST changes. Fix: track `any_changed` flag in transformer, write/count only if changed.

**Does NOT handle (fails loudly):**
- `from parent_pkg import child_module` (importing a module from its parent)
- Attribute access like `module.x.y()` (fully-qualified names in call sites)
- Dynamic imports (`importlib.import_module("...")`) — see `DYNAMIC_TARGETS.md`

### Fixture test

```powershell
powershell -NoProfile -File tools\refactor\tests\test_move_python.ps1
```
Covers: multi-name import, move+rename, subfolder auto-create, alias + comment/string preservation. Prints `PASS: all assertions` on success. Run this whenever `move_python.py` changes.

### `scripts/repo_audit/` — audit scoreboard

```powershell
& $py scripts\repo_audit all          # → .audit/REPORT.md, .audit/report.json
& $py scripts\repo_audit check        # exit 1 if any gated metric worse than baseline
& $py scripts\repo_audit impact --paths <path>   # who references this path
```

`.audit/baseline.json` is frozen; `.audit/REPORT.md` is the scoreboard. Metric regressions block commits. The tool is stdlib-only.

### `tools/db/sync-prod-to-dev.ps1` — copy PRD → DEV (do not invoke this unless you know why)

`.env` holds connection info. Per `PLAN.md` policy this is never touched by reorg tasks. If `.env`'s `KEY_PATH` file is missing, `credential_management.load_key()` raises a clear `RuntimeError` at first call (not at import).

---

## 5. Task status (condensed from `PLAN.md` §1 — check there for authoritative state)

### DONE
- **Phase 0:** audit tool, secrets untracked + gitignored, tracked junk cleared, streamlit residue removed, audit-regenerate wrapper
- **Phase 1:** dangling config resolved (docstring only), fastapi removed from exec layer, `DYNAMIC_TARGETS.md` created, Garmin vendor deleted (canonical = `pirate-garmin_clone`), 6 orphans deleted, forbid rules enabled
- **Phase 2 partial:** scripts triaged to `tools/`, deploy merged, root package.json removed
- **Phase 3 slices:** health, exercises, volleyball, admin, food, music, segments, activities, tri_tip, auth, config (router) — all landed pre-`3.prep-root`
- **`3.prep-root`** (`a086c60`): `backend_functions/` moved under `backend/`; `pytest.ini` updated; `credential_management.py` fixed (lazy `load_key`, correct `.env` path)
- **`T0.5.2`** (`0adf856`): libcst move engine replaces rope+regex; 4-case fixture
- **`3.core.1`** (`d218c01`): `backend_functions/config.py` → `backend/pifitness/core/config.py`; 2 importers rewritten; commit lands clean

### PENDING — Phase 3
| Task | Scope | Notes |
|---|---|---|
| **`T0.5.3`** | Fix `move_python.py` walk scope + counter | See §4. Blocking clarity for remaining moves. |
| **`3.core.2`** | `backend_functions/domain_exceptions.py` → `pifitness/core/exceptions.py`; `logging_functions.py` → `logging.py` | 2 renames, 1 manifest, 1 commit |
| **`3.worker`** | `backend_functions/{backend_tasks, task_execution, ultimate_task_executioner_v2, json_extractors}.py` → `pifitness/worker/` | 4 files; per user decision, dynamic-target modules (service_logins, task_execution, ultimate_task_executioner_v2, json_extractors) WILL FAIL at runtime after move — expected; fix on demand via DB row updates |
| **`3.api`** | `backend/main.py` → `pifitness/api/main.py`; verify `import pifitness.api.main` | **Investigate `pifitness` sys.path importability BEFORE moving** — `pifitness` is top-level importable, `backend.pifitness` is not. Entrypoint path changes to `pifitness.api.main:app` |
| **`3.finalize`** | Delete `backend/backend_functions/__init__.py`, empty `backend/api/`, `backend/pifitness.egg-info` at root | After 3.api |
| **`3.config-env-fix`** | `pifitness/core/config.py:93` — `env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")` — currently silent no-op pre and post move. Should point at `backend/.env` | Separate commit; do not bundle with moves |

### PENDING — other phases
- **`T2.5.dep`** — `deploy/scripts/deploy_react.sh:54` references `requirements-react.txt` which is missing. Fix by repointing to `library_requirements.txt` and merging any packages unique to the old file. Pi deployment is broken until this lands.
- **`1.3.verify`**, **`3.0.verify`**, **`3.2.verify`**, **`3.3.verify`** — unverified from prior phases (router logic extraction, pyproject.toml, systemd entrypoints, forbid rules) — probes not yet run
- **`3.4`** — clean-room check (fresh temp venv, `pip install -e backend`, `pytest -q`), blocked on 3.api
- **Phase 4** — frontend flatten `frontend/pifitness/` → `frontend/`, introduce `features/<domain>/`, remove root `package.json`/`node_modules` after proof
- **Phase 5** — ESLint boundaries, `import-linter` contracts, pre-commit wiring, `docs/ARCHITECTURE.md`
- **Phase 6** — Python 3.15 upgrade (queued, spec deferred until `3.finalize` complete)

---

## 6. Open decisions & deferred work

- **`DYNAMIC_TARGETS.md` (4 modules)** — DB rows reference these by string (`api_service_list.api_service_function`, task table `python_function`). After moves, runtime import fails. User decision (2026-10-30): **let it fail, fix on-demand with a targeted UPDATE**. Don't write speculative DDL now.
- **`pirate-garmin_clone/`** — slated for deletion in Phase 4. **Skip in all tool walks** so its imports aren't rewritten before deletion.
- **`.features/`** — user workbench, not in git, blanket ignored (2026-10-30). All tool walks skip it.
- **`memory-bank/`** — legacy, slated for deletion. Skip in walks.
- **Shim vs DB-update for `service_logins` move** — user chose Option X (DDL) then deferred the DDL itself. So no shims, no DDL now; runtime failures handled on demand.

---

## 7. Anti-patterns to avoid (learned the hard way)

| Anti-pattern | What happened | Fix |
|---|---|---|
| `exit 1` in interactive PS | Closed the window; lost all output | Use `$failed` sentinel; only `exit` in `.ps1` bodies |
| Putting "apply" in same block as "dry-run" | Both commands ran despite the comment; moved source unexpectedly | Dry-run and apply are separate blocks, always |
| Non-ASCII in `.ps1` fixtures | `â?"` parse errors, cascading failures | ASCII-only all `.ps1` + all fixtures |
| `Out-File -Encoding utf8` for JSON | BOM broke `json.loads` | `[System.IO.File]::WriteAllText(..., UTF8NoBom)` |
| `git mv` on untracked files | `fatal: not under version control` | `Move-Item` then `git add -A` for untracked |
| Assuming `^R ` matches `RM` | 25 vs 26 rename count mismatch | Use `^R` (no space) for any-rename-in-progress |
| Trusting rope's API guesses | Wasted cycles on `is_folder`, `Rename` semantics | libcst proven; only that |
| Counting `new_text != text` as "rewritten" | False positives from libcst round-trip | Track AST change flag (T0.5.3) |
| Bundling two logical changes in one commit | Can't isolate which broke | `3.prep-root` split into move + credential-fix; keep the discipline |
| `git clean -fd` without `-n` dry-run | Silently removed empty untracked dirs (`docs/architecture/`, `tools/codemod/`, `tools/features/`) | Always `git clean -nd` first, verify list, then `-fd` |
| Assuming precondition state stayed static | Between blocks, a Cline agent (`run=30`–`run=33`) made git changes | Verify HEAD + dirty count at the top of **every** block |

---

## 8. What to do first in a fresh session

1. **Read this file, then `docs/reorg/PLAN.md` §1 (ledger) and §2 (policy).**
2. **Verify state:**
   ```powershell
   git log -1 --format='%h %s'      # expect: d218c01 reorg(3.core.1)...
   git status --short               # expect: empty
   git branch --show-current        # expect: reorg/02-moves
   ```
3. **Confirm no Cline agent is active** on `reorg/02-moves`. If `git log` shows fresh `cline checkpoint` commits since `d218c01`, stop — an agent is live and may revert or interleave work. Wait for it to finish, then re-verify.
4. **Report back in this format, and stop:**
   ```
   state:            HEAD=<sha> branch=<branch> dirty=<n>
   current task:     <task-id from PLAN.md §1>
   next action:      <one-sentence description>
   open questions:   <list, or "none">
   ```
   Do not execute any task until the human says "proceed" or names a task.

## 9. When told to proceed

Pick the **first task in §5 PENDING Phase 3** that isn't blocked. Structure the work as 1–3 blocks per §2. End each block with STOP and paste-back instructions. Do not run two destructive blocks in a row without human confirmation between them.

**Recommended next: `T0.5.3`** (move tool walk scope + counter accuracy), then **`3.core.2`** (two renames).

Block structure for `T0.5.3` (example — adapt to actual findings):
- Block A — read-only: confirm HEAD, dirty count, current `SKIP_DIRS`, current walk scope
- Block B — write: modified `move_python.py` + extended fixture + extended test; do not run test
- Block C — run test, verify count accuracy on a sample move, commit

Block structure for `3.core.2`:
- Block A — read-only: HEAD, dirty, existence checks on the 2 source files, importers list, dry-run manifest written
- Block B — dry-run, inspect output
- Block C — apply, verify imports, compileall, full pytest, PLAN.md ledger update, commit

---

## 10. Quick-reference commands

```powershell
$py = ".venv\Scripts\python.exe"

# State
git log -1 --format='%h %s'
git status --short
git branch --show-current

# Imports (post-3.prep-root)
& $py -c "import backend.main; print('OK')"
& $py -c "from pifitness.core.config import get_settings; print('OK')"
& $py -c "import backend_functions.database_functions; print('OK')"

# Tests
& $py -m pytest -q                                        # full suite, ~2:40
& $py -m pytest --co -q 2>&1 | Select-Object -Last 1      # collection count
& $py -m compileall -q backend                            # syntax only

# Tool
& $py tools\refactor\move_python.py --help
powershell -NoProfile -File tools\refactor\tests\test_move_python.ps1

# Audit
& $py scripts\repo_audit all
& $py scripts\repo_audit check                            # exit 1 on regression
```