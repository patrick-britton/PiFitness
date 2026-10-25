# PiFitness reorganization — Cline agent brief

> **Human: how to use this file**
> 1. Copy `scripts/repo_audit/` (the audit tool) and put this file at `docs/reorg/BRIEF.md`.
> 2. Create branch `reorg/00-safety`, then start Cline in **Plan mode** and paste the kickoff message below.
> 3. Review `docs/reorg/PLAN.md` + `DECISIONS.md`, answer the open questions, then reply `go phase 0`.
> 4. After each phase the agent stops. Review the diff and the audit scoreboard, then say `go phase N`.

### Kickoff message (paste into Cline, Plan mode)

```
Read docs/reorg/BRIEF.md completely and follow it exactly. You are in PLAN mode: do not modify any
file outside docs/reorg/. First run the baseline audit (section 3), then write docs/reorg/PLAN.md and
docs/reorg/DECISIONS.md as specified in section 6, summarize them in under 40 lines, and wait for my
"go phase N" before touching anything else.
```

---

## 1. Mission

Reorganize this polyglot repo (Python/FastAPI backend, Next.js/React frontend, PostgreSQL, systemd+nginx
deployment on a Raspberry Pi) from a **technical-layer layout** to a **domain-sliced layout**, remove dead
weight, and make the structure **enforceable by tooling** — without changing runtime behavior.

**Non-goals (do not do these):** new features; behavior changes; dependency upgrades; reformatting-only
diffs; touching production data or the Pi; rewriting git history; renaming DB objects.

Success = every phase ends with tests green, the OpenAPI snapshot unchanged, and the audit scoreboard
(`repo_audit check`) not regressed, with the metric targets in section 7 reached.

## 2. Hard rules (violating any of these means STOP and ask)

**Secrets and data**
- Never open, print, `cat`, or paste the contents of: `*token*.json`, `.spotify_cache`, `.env*`, `*.dump`,
  `*.pem`, `*.key`, or anything the audit lists under `HYG-001`/`HYG-004`. Handle them by path only.
- If a sensitive file is tracked: `git rm --cached` it, gitignore it, and report it in `DECISIONS.md` so the
  human can rotate credentials. **Do not rewrite history** (`filter-repo`, `rebase -i` on shared commits, etc.).
- Never connect to or modify a database. Never execute `.sql` files. SQL work is renaming/moving files only.

**Git discipline**
- Work only on branches named `reorg/<phase>-<slug>`. Never `git push`, `git reset --hard`, `git clean -fdx`,
  `git checkout .`, or force anything. Never commit to `main`/`master`.
- Move with `git mv` (never copy+delete) so history follows. On Windows, case-only renames need two steps.
- One logical change per commit; every commit must leave `pytest` and the frontend gates green.
- Commit message format: `reorg(P3.music): move music slice into domains/music` + a body listing audit metrics
  that changed.

**Change safety**
- Never delete a file on a single signal. Deletion requires **all** of: audit evidence, `repo_audit impact`
  showing 0 references, not listed as `dynamic-possible`/`string-referenced`/pinned, and — for anything
  that is not obviously a build artifact — approval recorded in `DECISIONS.md`.
- No behavior changes. If a boundary fix needs a code change (section 7, Phase 1), keep it minimal and covered by
  an existing or new test.
- Do not upgrade dependencies, change lockfiles, or run `npm audit fix` / `pip install -U`.
- Respect `.clinerules/default-rules.md` and `.clineignore`. If they conflict with this brief, stop and ask.

**Windows / PowerShell**
- Use `;` not `&&`. Python is `.venv\Scripts\python.exe`. Quote paths with spaces (e.g. `"004-006 copy.md"`).
- Never wrap native commands in `2>&1 | Out-File` (it turns stderr into `NativeCommandError` noise and writes
  UTF-16). Use the Python tool, or `cmd /c "... > file 2>&1"`.
- Prefer `git rm` over `Remove-Item`. When `Remove-Item -Recurse` is unavoidable it must target a path listed in
  `PLAN.md`, never a variable, and never anything under `.venv`, `.git`, or `node_modules` roots.
- Mark any destructive or broad command `requires_approval: true`.

**Deployment**
- Production is systemd + nginx on a Pi. Any rename of a module path, working directory, or script must update
  `deploy*/` unit files, nginx configs, and bootstrap scripts **in the same commit**, and be appended to
  `docs/reorg/CUTOVER.md` (exact old → new paths and the restart steps). Never touch the Pi itself.

**Escalation**
- If a gate fails twice for the same cause, stop; write `docs/reorg/BLOCKERS.md` (command, output excerpt,
  hypotheses, what you tried) and ask.
- If the audit and reality disagree (e.g. a file the audit calls dead is clearly used), trust reality, record it,
  and ask whether to adjust `audit.toml`.

## 3. Tooling you have: `repo_audit`

Stdlib-only Python, installed at `scripts/repo_audit/` (moves to `tools/audit/` in Phase 2). Config:
`scripts/repo_audit/audit.toml` (domains, layer rules). Outputs go to `.audit/` (gitignored except
`baseline.json`).

```powershell
$py = ".venv\Scripts\python.exe"
& $py scripts\repo_audit baseline          # full audit, freezes .audit\baseline.json  (do once, first)
& $py scripts\repo_audit all               # full audit -> .audit\REPORT.md, report.json, sections\*.json, TREE.txt
& $py scripts\repo_audit python frontend   # only some sections
& $py scripts\repo_audit diff              # baseline vs latest metrics (✔ improved, ✘ regressed)
& $py scripts\repo_audit check             # exit 1 if any gated metric is worse than baseline  <- run after EVERY commit
& $py scripts\repo_audit impact --map docs\reorg\moves\phase2.json   # who references each old path/module
& $py scripts\repo_audit impact --paths backend_functions\music_functions.py
```

How to read it (do **not** load whole JSON files into context):
- Start with `.audit/REPORT.md` (scoreboard + findings). Drill into `.audit/sections/<name>.json` with targeted
  `python -c` queries.
- Sections: `inventory`, `python`, `frontend`, `contracts`, `domains`, `hygiene`, `sql`, `triage`, `git`.
- Evidence grades: `likely-dead` = no import/string/config/doc reference anywhere; `dynamic-possible` = a
  non-literal `importlib` call could load it; `string-referenced` = named in SQL/JSON/systemd/py strings (a task
  registry?). Only `likely-dead` + human approval may be deleted.
- Heuristic sections (`contracts`, `dependencies`, `domains`) give candidates, never verdicts.
- `domains.assignment` (path → domain) and `domains.unassigned` are the input for every `moves.json` you build.
  Unassigned files need a human decision; list them in `DECISIONS.md` with a proposed home.

**If the tool itself misbehaves** (crash, obviously wrong result): note it in `BLOCKERS.md`, fix the tool in a
separate commit (`reorg(tool): ...`) with a test fixture, and continue. The tool must never be "fixed" by loosening
a metric.

## 4. Hypotheses from the first audit (verify with the new audit; do not assume)

1. `backend_functions/config.py` imports `backend.config`, but there is no `backend/config.py` — only a stale
   `.pyc`. Dangling or guarded? (`py.dangling_imports`)
2. `ultimate_task_executioner_v2.py` (worker) imports `fastapi` (`PY-004`). Executioner + `agents/` should be
   framework-free.
3. Router modules (`backend/api/activities.py` especially) call the DB layer directly, contain orchestration
   (threading), and export private helpers (`_run_step`, `_resolve_activity_id`, `_verify_spotify`,
   `_task_failure_message`, `_failed_run_message`) that scripts and tests import. (`PY-005/006/007`)
4. `pirate_garmin_auth.py` / `pirate_garmin_endpoints.py` duplicate `pirate-garmin_clone/src/pirate_garmin/*`.
   (`PY-013`)
5. `admin_functions`, `data_helpers`, `image_creation`, `running_functions` had no static importers; task loaders
   (`service_logins`, `task_execution`, executioner) use `importlib`, so some may be dynamic targets. (`PY-009/010`)
6. Streamlit is effectively already gone: only `.pyc` husks (`frontend_functions/`, `backend_functions/viz_factory/`),
   `.streamlit/config.toml`, `scripts/map_streamlit_to_backend.py`, docs, and the package in `.venv`. (`HYG-007/008`)
7. ~31 of 49 `scripts/*.py` are ticket-specific one-offs; 7 behave like integration tests; several `test_*.py` live in
   `scripts/`. (`TRI-*`)
8. DB migration history lives in `.features/migrations/` with no global order. (`SQL-001`)
9. Two `package.json`/lockfile/`node_modules` pairs (root + `frontend/pifitness`); the earlier knip report (all 187
   files "unused") was a false positive caused by running from the root. (`INV-004`, `TS-009`)
10. Secrets-adjacent files at the repo root (`garmin_tokens.json`, `oauth1_token.json`, `.spotify_cache`,
    `prod-*.dump`, `backend/.env`). Tracked or only ignored? (`HYG-001/002/003`)
11. `.codes/Users/rickb/...`, `.features/zz_workfow_archive/`, `"004-006 copy.md"` look like tool leftovers/duplicates.
    (`INV-002/006`)

## 5. Target architecture

```
pifitness/
├── backend/
│   ├── pyproject.toml                 # single Python manifest (replaces setup.py, egg-info, 3 requirements files)
│   ├── pifitness/                     # ONE importable package (was backend + backend_functions + agents)
│   │   ├── api/                       #   main.py, deps.py — app wiring only; includes routers from domains
│   │   ├── core/                      #   config, db (database_functions, db_wrapper, db_schema), logging, credentials, shared helpers
│   │   ├── domains/
│   │   │   ├── activities/            #   router.py, service.py, queries.py, schemas.py (+ segments, leaderboards, smoothing, running)
│   │   │   ├── food/                  #   (+ food_lookup, normalize, search, tri_tip)
│   │   │   ├── music/   exercises/   volleyball/   health/   admin/
│   │   ├── integrations/              #   garmin/ (ONE client), spotify/
│   │   └── worker/                    #   task_runner (was ultimate_task_executioner_v2), task_execution, entrypoint (was agents/)
│   └── tests/<domain>/                #   + tests/integration/
├── frontend/                          # flattened from frontend/pifitness; ONE package.json
│   └── src/
│       ├── app/                       #   route files only (page/layout/route); thin
│       ├── features/<domain>/         #   components/, hooks.ts, types.ts, domain-specific lib; tests colocated
│       ├── components/                #   shared UI (Header, NavBar, Sidebar, Layout, ThemeToggle, MapView)
│       └── lib/                       #   truly shared helpers (date-format, token-color, geo, api client)
├── db/  migrations/ (NNNN_*.sql + NNNN_*.down.sql, one global order)  schema/  dumps/ (gitignored)
├── deploy/                            # merged deploy + deployment: systemd/, nginx/, bootstrap, compose.yaml
├── tools/                             # durable scripts only: db/, diagnostics/, audit/, features/, codemod/
├── docs/                              # architecture, runbooks, features/{active,done}, archive/, reorg/
└── .agents/  .clinerules/  .vscode/   # tool config only (tool-mandated paths stay)
```

**Dependency direction (enforced by `[[forbid]]` rules in `audit.toml`):**
`api → domains → core`, `worker → domains → core`, `integrations → core`. `core` imports nothing above it;
`domains` never import `api`; `worker` never imports `api` or any web framework. Cross-domain imports go through
a domain's public `service.py` only; never private (`_x`) names.

**Naming:** snake_case modules; domain folders singular nouns matching `audit.toml [domains]`; migrations
`NNNN_<feature-id>_<slug>.sql`; no version suffixes (`_v2`), no `copy`, no ticket ids in code paths.

## 6. Operating protocol

### 6.1 PLAN mode (first, no source changes)

1. Create `docs/reorg/` (the only place you may write). Verify `.clinerules` allows it.
2. Run `baseline` (section 3). If git is dirty, stop and ask.
3. Read `.audit/REPORT.md`; drill into sections only as needed.
4. Write **`docs/reorg/PLAN.md`**: for each phase — ordered tasks, exact file lists (counts + paths from the audit),
   gate commands, expected metric deltas, risks, rollback (always "discard the branch"), estimated commits.
   Mark each task `[safe]`, `[needs-approval]` or `[blocked-on-decision]`.
5. Write **`docs/reorg/DECISIONS.md`**: every open question with your recommendation and the default you will take
   if the human says "use defaults". Mandatory items:
   - Tracked secrets: list by path only; confirm rotation is the human's job.
   - Garmin duplicate: which copy is canonical (include the diff size from `PY-013`).
   - Every `likely-dead`, `dynamic-possible`, `string-referenced` module: keep / delete / pin.
   - Real targets of every `importlib` loader (read the code and any task seed SQL/JSON; **no DB access**).
   - Scripts triage: the full keep / archive-or-delete / convert / review list.
   - `domains.unassigned` files with proposed domain or `core`.
   - Whether `.features/`, `memory-bank/`, `.clinerules/workflows` path references can change (Cline workflows and
     `tools/features/fd.py` depend on `.features/...` paths).
   - Meaning of the three `npm_requirements.txt` files, `compose.yaml`, `validate_endpoints.py`, `setup.py` entry.
   - Whether to add `openapi-typescript` for generated API types (new devDependency).
   - Root `package.json` + `node_modules`: delete after proof, or keep?
6. Draft `docs/reorg/moves/phase2.json … phase4.json` as `{ "old/path": "new/path" }` generated from
   `domains.assignment` and the proposed numbering in `sql.proposed_order`. Drafts only; run `repo_audit impact`
   on them and record reference counts in `PLAN.md`.
7. Capture behavior baselines (read-only commands):
   ```powershell
   & $py -c "import json; from backend.main import app; print(json.dumps(app.openapi(), sort_keys=True, indent=1))" > docs\reorg\openapi.before.json
   & $py -m pytest -q > docs\reorg\pytest.before.txt
   ```
   (Use `cmd /c "... > file 2>&1"` form if stderr matters.) If importing `backend.main` needs a DB or network,
   record that in `BLOCKERS.md` and ask how to stub it. Record the frontend baseline too (`npx tsc --noEmit`,
   `npx vitest run`, `npm run build` — use the script names actually in `package.json`).
8. Reply with a summary of **at most 40 lines** (phases, counts, top risks, decisions needed) via `plan_mode_respond`
   and **wait**.

### 6.2 ACT mode (only after "go phase N")

Per task: state intent → `impact` on what you will touch → change → run gates (section 8) → `repo_audit all` then
`check` → commit → append a line to `docs/reorg/LOG.md` (`task, commit, metrics delta, notes`).

- Do bulk rewrites with a **script**, not hundreds of manual edits: write `tools/codemod/move_python.py`
  (Phase 2) with `--map moves.json --dry-run`. It must rewrite `from a.b import c`, `import a.b as x`,
  `from pkg import moved_module`, relative imports inside moved files, dotted/colon strings
  (`"backend.main:app"`, `mock.patch("a.b.c")`, `importlib.import_module("a.b")`), and print every file it changed.
  Verify with `compileall`, `pytest --collect-only`, and `py.dangling_imports == 0` — that metric is your
  rewrite-correctness gate. For frontend moves use `tsc --noEmit` and `ts.unresolved_imports == 0`.
- After every phase: `all`, `diff`, `check`; write `docs/reorg/HANDOFF.md` (done / remaining / gotchas / next
  commands); stop and ask for approval. Prefer starting a fresh task (`new_task`) per phase to keep context small.
- Clear stale caches after moves: all `__pycache__` outside `.venv`, `.pytest_cache`, and `frontend\.next`.

## 7. Phases

### Phase 0 — Safety net and hygiene (branch `reorg/00-safety`; no source-file moves)
- 0.1 Commit the audit tool + `audit.toml`; gitignore `.audit/*` with `!.audit/baseline.json`; commit the baseline.
- 0.2 Secrets: handle every `HYG-001` item (untrack + gitignore). Report `HYG-003/004` to the human; do not open the files.
- 0.3 `.gitignore` additions for everything `HYG-005/006` and `INV-003` reports (logs, `*.out`, `*.tsbuildinfo`, `.next`,
  caches, `local_storage/`, dumps, tokens). `git rm --cached` anything already tracked.
- 0.4 Delete confirmed junk: root `_task07*.txt`, `vitest-t11.log`, other `INV-003` items, pyc husks (`HYG-007`).
  Diff before deleting duplicates: `.codes/…/010-006_design.md` vs `.features/designs_active/010-006_design.md`;
  `"004-006 copy.md"` vs `004-006.md`; `.features/zz_workfow_archive/` vs `.clinerules/workflows/` (byte-compare via `INV-002`).
- 0.5 Streamlit residue (`HYG-008`): remove `.streamlit/`; `scripts/map_streamlit_to_backend.py` only if `impact` = 0;
  move `memory-bank/streamlit_*` docs to `docs/archive/`; propose (do not run) the venv uninstall and any requirements edit.
- 0.6 Replace `scripts/regenerate-audit.ps1` with a 3-line wrapper around `repo_audit all --with-external`; delete the
  superseded outputs in `.audit/` (tree.txt, tree-filtered.txt, js-imports.txt, knip-report.txt, culler-report.txt, …).
- **Exit:** `hygiene.sensitive_exposed = 0`, `hygiene.tracked_junk = 0`, `hygiene.pyc_husks = 0`,
  `inventory.clutter_files = 0`, `inventory.stray_path_files = 0`, tests green.

### Phase 1 — Fix boundaries in place (branch `reorg/01-boundaries`; behavior-preserving code edits)
- 1.1 Resolve the dangling `backend.config` import (`PY-001`). Use `git log --diff-filter=D -- backend/config.py` to learn what happened.
  If the intent is ambiguous → `DECISIONS.md`, stop.
- 1.2 Remove `fastapi` from the executioner/library layer: replace `HTTPException` etc. with plain domain exceptions;
  translate in routers.
- 1.3 Extract router-resident logic: for each private helper in `PY-005`, create a public function in
  `backend_functions/<domain>_service.py`; routers and callers (scripts/tests) import it; update with `impact`. Move
  threading/orchestration out of routers (`PY-007`) into the service, behavior identical.
- 1.4 Write `docs/reorg/DYNAMIC_TARGETS.md` (real `importlib` targets) and add them as pinned in `audit.toml`.
- 1.5 Garmin: apply the human's decision from `DECISIONS.md` (default: keep `pirate-garmin_clone` as the library,
  make `backend_functions` a thin adapter, delete the vendored copy only after tests pass).
- 1.6 Delete approved dead modules. Remove `pandas`/`numpy` imports only where the audit shows no real use.
- 1.7 Enable all current `[[forbid]]` rules.
- **Exit:** `py.dangling_imports = 0`, `py.pkg_cycles = 0`, `py.layer_violations = 0`,
  `py.private_cross_imports = 0`, `py.router_direct_db_handlers` reduced (target 0 for migrated routers),
  OpenAPI snapshot identical, tests green.

### Phase 2 — Mechanical moves that do not change Python import paths (branch `reorg/02-moves`)
- 2.1 `db/`: move `.features/migrations/*` to `db/migrations/` using `sql.proposed_order` **after the human confirms the
  numbering against what prod applied**; rollbacks become `NNNN_*.down.sql`; `schema_reference.sql` → `db/schema/`; dumps
  → `db/dumps/` (gitignored). Update scripts via `impact`.
- 2.2 `scripts/` triage per the approved list: durable tools → `tools/{db,diagnostics,audit,features,codemod}/`;
  approved one-offs → delete; test-like scripts → `backend/tests/integration/` only if trivial, else delete; misplaced
  `test_*.py` → `tests/`. Update `pytest.ini`, `.clinerules`, and docs that mention moved scripts.
- 2.3 Docs: `.features/designs_complete` → `docs/features/done/`, `designs_active` + `descriptions` →
  `docs/features/active/`, `memory-bank/` → `docs/` (hand-written) and `docs/generated/` (reports; gitignore or
  regenerate). **Update `.clinerules/workflows/*`, `.agents/skills/*`, `local_clinerules.txt`, and `tools/features/fd.py`
  path constants in the same commit.** Skip this task if the human chose to keep `.features/`.
- 2.4 Merge `deploy/` + `deployment/` → `deploy/{systemd,nginx,scripts}`; update units/bootstrap in the same commit;
  append to `CUTOVER.md`.
- 2.5 Consolidate dependency manifests only as far as approved (npm/requirements duplicates).
- **Exit:** `sql.migrations_outside_db_dir = 0`, `scripts.oneoff = 0`, `scripts.misplaced_tests = 0`,
  `inventory.duplicate_groups = 0`; `impact` on every old path = 0 references.

### Phase 3 — Python package by domain (branch per slice: `reorg/03-<domain>`)
- 3.0 Create `backend/pyproject.toml` (package `pifitness` at `backend/pifitness/`), move dependency declarations into it,
  update `pytest.ini` (`pythonpath`), reinstall editable. **No compatibility shims at the end of the phase.**
- 3.1 Slice order (smallest/least coupled first): `health` → `exercises` → `volleyball` → `admin` → `food` → `music` →
  `integrations` → `activities` → `worker` → `core` → `api` wiring. For each slice build `moves.json` from
  `domains.assignment` (router → `router.py`, queries → `queries.py`, schemas → `schemas.py`, functions → `service.py` or
  named modules, tests → `backend/tests/<domain>/`), run `impact`, run the codemod, run gates, commit.
- 3.2 Systemd units and entrypoints (`uvicorn pifitness.api.main:app`, worker entrypoint) change in the slice that moves them;
  update `deploy/` and `CUTOVER.md` in that commit.
- 3.3 Final slice: enable the TARGET `[[forbid]]` rules, disable the old ones, update `[domains]` keywords.
- 3.4 Clean-room check: new temp venv outside the repo → `pip install -e backend` → `pytest -q` → import the app. This catches
  imports that only worked through the old editable finder.
- **Exit:** OpenAPI snapshot identical to `openapi.before.json`; `py.layer_violations = 0` under TARGET rules;
  `py.module_cycles = 0`; `py.router_direct_db_handlers = 0`; no `backend_functions`, `frontend_functions`, `agents` dirs.

### Phase 4 — Frontend (branch `reorg/04-frontend`, slices `reorg/04-<domain>`)
- 4.1 Flatten `frontend/pifitness` → `frontend`; update every reference (`deploy/`, nginx-react.conf, scripts, `.clineignore`,
  docs, `package.json` scripts). Delete the root `package.json`/lockfile/`node_modules` only after `ts` audit (`TS-009`) and
  a clean `npm ci` + build in `frontend/` prove they are unused **and** the human approved.
- 4.2 Introduce `features/<domain>/` per `domains.assignment`: move route-local components, `hooks/use<Domain>.ts`,
  `lib/types/<domain>.ts`, and single-consumer `lib/*` (`TS-007`) into their feature; keep `app/<route>/page.tsx` thin;
  move `app/components` → `components/`. Eliminate route → route imports (`TS-006`).
- 4.3 Only if approved: generate API types from the FastAPI OpenAPI schema (`openapi-typescript`) into
  `src/lib/api/generated.ts`; replace hand-written `lib/types/*` incrementally, domain by domain.
- 4.4 Remove modules the import graph proves unreachable (`TS-001`) with approval; remove unused npm packages (`TS-004`)
  only if approved (lockfile changes need explicit sign-off).
- **Exit:** `ts.unresolved_imports = 0`, `ts.cross_route_imports = 0`, `ts.npm_undeclared = 0`, `ts.unreachable_files` ≤ baseline
  (target 0 after approvals), `inventory.npm_manifests = 1`, `tsc --noEmit`, `vitest run`, `next build` green.

### Phase 5 — Guardrails and wrap-up (branch `reorg/05-guardrails`)
- 5.1 Add an ESLint boundaries rule (or `no-restricted-imports`) so `features/*` import only from `components/`, `lib/`, and
  their own folder — only if it requires no new dependency; otherwise list it as a recommendation.
- 5.2 Add `import-linter` contracts mirroring the TARGET rules **only if approved** (new dev dependency); otherwise keep
  `repo_audit check` as the gate and document it in `README.md`.
- 5.3 Wire `repo_audit check` into the PR/pre-commit flow (`.clinerules/hooks` or a git hook; propose, don't force).
- 5.4 Write `docs/ARCHITECTURE.md` (layout, dependency rules, how to add a domain, how to run the audit),
  update `README.md`, and generate `docs/reorg/FINAL_REPORT.md` from `repo_audit diff` (before → after for every metric) plus
  the git co-change baseline (`git.avg_code_dirs_per_commit`, `git.avg_domains_per_commit` — expected to fall over time).
- **Exit:** all gated metrics at their targets; baseline refreshed (`repo_audit baseline`) and committed.

## 8. Verification gates (run after every commit that touches code; record outputs in `LOG.md`)

```powershell
$py = ".venv\Scripts\python.exe"
& $py -m compileall -q backend backend_functions agents scripts 2>$null      # adjust roots to the current layout
& $py -m pytest -q
& $py -c "import backend.main"                                              # becomes: import pifitness.api.main
& $py -c "import json; from backend.main import app; print(json.dumps(app.openapi(), sort_keys=True, indent=1))" > docs\reorg\openapi.after.json
fc.exe docs\reorg\openapi.before.json docs\reorg\openapi.after.json         # must show no differences
& $py scripts\repo_audit all ; & $py scripts\repo_audit check               # must exit 0
Push-Location frontend\pifitness ; npx tsc --noEmit ; npx vitest run ; Pop-Location   # + npm run build at phase end
git status --short                                                          # nothing unexpected (no secrets, no junk)
```

A gate that cannot run (missing dependency, DB needed at import) is **not** a pass: record it and ask.

## 9. Known hazards

- Stale editable installs: `__editable__.pifitness-*` finders can keep resolving old module names after a rename; the clean-room
  check in 3.4 is mandatory.
- Windows is case-insensitive: case-only renames need an intermediate name. Very long paths under `.features/…` may hit limits.
- `.clinerules/workflows` and `tools/features/fd.py` encode `.features/` paths — moving docs without updating them silently breaks
  the Cline workflows.
- Dynamic loading by task name (importlib/DB task registry) is invisible to static analysis; pinned modules must not move
  without an explicit entry in `DYNAMIC_TARGETS.md` and a matching update wherever the name is stored.
- `uvicorn`/systemd units reference module paths and working directories as strings; `impact` finds them only if they are in the repo.
  Anything configured on the Pi outside the repo goes in `CUTOVER.md` for the human.
- Generated artifacts (`memory-bank/*report*.json`, `data_flow_diagrams.*`, `.audit/*`) must have a regeneration command documented
  before they are deleted or gitignored.

## 10. Deliverables checklist

`docs/reorg/` should end up containing: `BRIEF.md`, `PLAN.md`, `DECISIONS.md`, `LOG.md`, `HANDOFF.md`, `CUTOVER.md`,
`DYNAMIC_TARGETS.md`, `BLOCKERS.md` (if any), `moves/phase*.json`, `openapi.before.json`, `FINAL_REPORT.md`.
Every phase ends with a stop. Never start the next phase without an explicit `go phase N`.
