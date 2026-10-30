# PiFitness Reorg — PLAN.md (v2)

**HEAD:** `287c9dff` (wip checkpoint, 2026-10-29)
**Rollback point:** `c1315503` (pre-checkpoint, exact dirty state before reconciliation)
**Baseline:** `.audit/discovery/report-now.json` (371 files, `pytest 210 passed`)
**State:** Phase 0 ✅ · Phase 1 ✅ (except 1.3 unverified) · Phase 2 partial · Phase 3 partial · Phase 4 not started

This file is the single source of truth. Every task is here — no external decision docs, no moves JSON read by humans, no "see DECISIONS.md." When a decision is made, it becomes the *actions of a task* in §4. When a task is done, it moves to `[DONE <sha>]` in the ledger (§1).

## §1 Task Status Ledger

DONE = verified by probe at HEAD `287c9dff` (see `.audit/discovery/ledger.tsv`). Commit-level attribution deferred; rollback target is `c1315503` for anything unexpected.

| Ph | Task | Status | Evidence |
|---|---|---|---|
| 0 | 0.1 audit tool | ✅ DONE | `scripts/repo_audit` present |
| 0 | 0.2 secrets handled | ✅ DONE | `.gitignore` covers tokens |
| 0 | 0.3 gitignore logs/caches | ✅ DONE | `.gitignore` covers `.next`, `*.dump` |
| 0 | 0.5 streamlit residue | ✅ DONE | `.streamlit` absent |
| 0 | 0.6 regenerate-audit | ✅ DONE | wrapper present |
| 0 | T0.5 rope tool | ✅ DONE | move_python.py + fixture test passes |
| 1 | 1.1 dangling config | ✅ DONE | docstring only, no real import (Task 0.3.5) |
| 1 | 1.2 fastapi removed from exec | ✅ DONE | no `import fastapi` in exec |
| 1 | 1.3 router logic extracted | ⚠️ UNVERIFIED | not probed — see Task 1.3.verify |
| 1 | 1.4 DYNAMIC_TARGETS | ✅ DONE | `docs/reorg/DYNAMIC_TARGETS.md` |
| 1 | 1.5 Garmin vendored deleted | ✅ DONE | both absent |
| 1 | 1.6 6 orphans deleted | ✅ DONE | all 6 absent |
| 1 | 1.7 forbid rules | ✅ DONE | `audit.toml` has `[[forbid]]` |
| 2 | 2.0 bootstrap manifests | ❌ **TODO** | `requirements-react.txt` MISSING → Task 2.5.dep |
| 2 | 2.2 scripts triaged | ✅ DONE | `tools/db/` populated |
| 2 | 2.4 deploy merged | ✅ DONE | `deploy/systemd` present, `deployment/` absent |
| 2 | 2.5 root pkg.json removed | ✅ DONE | root absent, frontend present |
| 3 | 3.0 pyproject.toml | ⚠️ UNVERIFIED | see Task 3.0.verify |
| 3 | 3.health | ✅ DONE | `backend/pifitness/health/` |
| 3 | 3.exercises | ✅ DONE | dir present |
| 3 | 3.volleyball | ✅ DONE | dir present |
| 3 | 3.admin | ✅ DONE | dir present |
| 3 | 3.food | ✅ DONE | dir present |
| 3 | 3.music | ✅ DONE | dir present |
| 3 | 3.segments | ✅ DONE | dir present |
| 3 | 3.activities | ✅ DONE | dir present |
| 3 | 3.integrations | ❌ TODO | `backend/pifitness/integrations/` missing |
| 3 | 3.worker | ❌ TODO | `backend/pifitness/worker/` missing |
| 3 | 3.core | ❌ TODO | `backend/pifitness/core/` missing |
| 3 | 3.api | ❌ TODO | `backend/pifitness/api/main.py` missing → import fails |
| 3 | 3.2 systemd entrypoints | ⚠️ UNVERIFIED | see Task 3.2.verify |
| 3 | 3.3 TARGET forbid rules | ⚠️ UNVERIFIED | see Task 3.3.verify |
| 3 | 3.4 clean-room | ⚠️ UNVERIFIED | blocked on 3.api |
| 4 | 4.1 frontend flatten | ❌ TODO | `frontend/pifitness/` still exists |
| 4 | 4.2 features/ | ❌ TODO | not started |
| 5 | 5.* guardrails | ❌ TODO | not started |

## §2 Policy (applies to every task in §4)

1. **Export-preservation on moves.** A move changes the module path; it does not rename the symbols inside. If `health.py` exported `health_router`, `pifitness/health/router.py` still exports `health_router`. Renames are separate tasks.
2. **Commits-per-task.** One task = one commit. Message format: `reorg(<scope>): <task-slug> [<task-id>]`. `wip(...)` commits are for checkpoints only.
3. **Verification-per-task.** Every task's `Success criteria` table must pass before `[DONE]`. A failed check → task is `[BLOCKED]`, not `[DONE]`.
4. **Rollback-by-reset.** Every task has an explicit rollback command. For non-commit tasks, rollback is "delete these files." For commit tasks, rollback is `git reset --hard <sha>` where `<sha>` is HEAD before the task.
5. **Decisions become actions.** No §3 to hold decisions. When a decision is needed, it becomes a sub-task's `Actions`. The ledger records the resulting commit as `DONE`. There is no `DECISIONS.md`.

## §3 Decisions Needed

*(Empty. All prior decisions have been folded into task bodies in §4. Any new decision that surfaces will be inserted as a task Action, not written here.)*

## §4 Tasks

### Next batch — run in order

#### Task T0.5 — Install `rope`, build `tools/refactor/move_python.py`, fixture-test  `[AGENT]` `[TODO]`

**Scope:** `.venv` (adds `rope`); creates `tools/refactor/move_python.py`, `tools/refactor/tests/fixture/`, `tools/refactor/tests/test_move_python.ps1`. No source moves yet.
**Depends on:** none. **Environment:** plain PowerShell; `$py = ".venv\Scripts\python.exe"`.

**Actions:**

1. Install rope:
   ```
   & ".venv\Scripts\python.exe" -m pip install rope
   ```
2. Create `tools\refactor\move_python.py` with this contract:
   - CLI: `python move_python.py <manifest.json> (--dry-run | --apply)`
   - Manifest: `{"old.module.path": "new.module.path", ...}`
   - Uses `rope.refactor.move.MoveModule` (or `rope.refactor.rename.Rename` if installed rope version moved the API — check `python -c "import rope.refactor; print(dir(rope.refactor))"` first).
   - Prints per-file rewrites on `--dry-run`, applies on `--apply`.
   - Exits non-zero if any module cannot be resolved.
3. Create fixture at `tools\refactor\tests\fixture\`:
   - `pkg_a\__init__.py`, `pkg_a\foo.py` (`def hello(): return "hi"`)
   - `pkg_b\__init__.py`
   - `user.py` (`from pkg_a.foo import hello`; `hello()`)
   - `manifest.json` → `{"pkg_a.foo": "pkg_b.foo"}`
4. Create `tools\refactor\tests\test_move_python.ps1`:
   - Copies fixture to temp dir, `cd` temp, runs `& $py <path>\move_python.py manifest.json --apply`
   - Asserts: `pkg_b\foo.py` exists; `pkg_a\foo.py` absent; `user.py` contains `from pkg_b.foo import hello`; `& $py -c "import user; print(user.hello())"` prints `hi`.
   - Removes temp dir.
5. Commit: `reorg(tool): rope wrapper + fixture [0.5]`

**Success criteria:**

| # | Check | Expected |
|---|---|---|
| 1 | `& $py -m pip show rope` | exit 0 |
| 2 | `powershell -File tools\refactor\tests\test_move_python.ps1` | exit 0 |
| 3 | `tools\refactor\move_python.py` exists, has `--apply` and `--dry-run` args | `Select-String -Path tools\refactor\move_python.py -Pattern "--apply\|--dry-run"` → ≥1 match |
| 4 | `git status --short` after commit | empty |

**If verification fails:** the fixture test *is* the debug surface. Do not test on real source. Fix `move_python.py` until the fixture passes, then re-commit.

**Rollback:** `git reset --hard HEAD~1; & ".venv\Scripts\python.exe" -m pip uninstall -y rope; Remove-Item -Recurse tools\refactor`

---

#### Task 2.5.dep — Fix `deploy_react.sh` manifest reference  `[AGENT]` `[TODO]`

**Scope:** `deploy/scripts/deploy_react.sh`, `deploy/scripts/library_requirements.txt` (only if merge needed). No source code.
**Depends on:** none — parallel-safe with 0.5. **Environment:** plain PowerShell for inspection; `[git-bash]` for `bash -n`.

**Actions:**

1. Inspect what `deploy_react.sh` expects:
   ```
   Select-String -Path deploy\scripts\deploy_react.sh -Pattern "REQUIREMENTS_FILE|requirements-react|library_requirements"
   ```
2. Inspect what `library_requirements.txt` contains:
   ```
   Get-Content deploy\scripts\library_requirements.txt
   ```
3. Inspect what `deploy_react.sh` was trying to install. Reference commit `bb90e2c^` where `requirements-react.txt` existed:
   ```
   git show bb90e2c^:deploy/scripts/requirements-react.txt
   ```
4. **If `library_requirements.txt` already contains every package from the historical `requirements-react.txt`** → repoint only:
   - In `deploy\scripts\deploy_react.sh`, change `REQUIREMENTS_FILE="requirements-react.txt"` → `REQUIREMENTS_FILE="library_requirements.txt"`
5. **If `library_requirements.txt` is missing packages from the historical file** → merge first, then repoint:
   - Append missing lines to `deploy\scripts\library_requirements.txt`
   - Then do the repoint from step 4.
6. Validate shell syntax:
   ```
   & "C:\Program Files\Git\bin\bash.exe" -n deploy/scripts/deploy_react.sh
   ```
7. Commit: `reorg(2.5): repoint deploy_react.sh to library_requirements.txt [2.5.dep]`

**Success criteria:**

| # | Check | Expected |
|---|---|---|
| 1 | `Select-String deploy\scripts\deploy_react.sh -Pattern "requirements-react"` | zero matches |
| 2 | `Select-String deploy\scripts\deploy_react.sh -Pattern "library_requirements.txt"` | ≥1 match |
| 3 | `bash -n deploy/scripts/deploy_react.sh` | exit 0, no output |
| 4 | Every package from `git show bb90e2c^:deploy/scripts/requirements-react.txt` present in `library_requirements.txt` | manual check — list both, diff visually |
| 5 | `git status --short` after commit | empty |

**If verification fails:** do not merge additional packages without seeing the diff first. Paste `git show bb90e2c^:deploy/scripts/requirements-react.txt` and `Get-Content library_requirements.txt` — we'll decide the merge together.

**Rollback:** `git reset --hard HEAD~1`

---

### Deferred — expand after 0.5 is green

- **3.integrations** — move `backend/api/auth.py`, `backend/api/config.py` → `backend/pifitness/integrations/`. Uses `rope` (Task 0.5). Spec written after 0.5 verified.
- **3.worker** — move `backend_functions/backend_tasks.py` → `backend/pifitness/worker/worker.py`. Spec written after 3.integrations passes.
- **3.core** — move `backend_functions/{config,domain_exceptions,logging_functions}.py` → `backend/pifitness/core/`. Must precede 3.api.
- **3.api** — move `backend/main.py` → `backend/pifitness/api/main.py`; then `from pifitness.api.main import app` must pass. This is the final Phase-3 gate.
- **3.0.verify, 3.2.verify, 3.3.verify** — probe tasks for the unverified rows in §1.
- **3.4** — clean-room check, blocked on 3.api.
- **4.1–4.4** — frontend flatten + features/ restructure.
- **5.1–5.4** — guardrails.

Each will be written in full task format once its predecessor is green. No partial-spec tasks.

## §5 Test baseline

Full suite runs once per phase, not per task. Task-level verification uses targeted paths.

```
Full suite  : & ".venv\Scripts\python.exe" -m pytest -q      (210 passed, 161s)
Compileall  : & ".venv\Scripts\python.exe" -m compileall -q backend backend_functions tests
Targeted    : & ".venv\Scripts\python.exe" -m pytest -q <specific/paths>
```

After every task-commit:
```
& ".venv\Scripts\python.exe" scripts\repo_audit all
& ".venv\Scripts\python.exe" scripts\repo_audit check
```

Both must exit 0.