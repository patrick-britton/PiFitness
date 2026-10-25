# repo_audit

Stdlib-only structural audit for a Python + Next.js + SQL repo. Drop this folder in `scripts/repo_audit/`.
Needs Python 3.11+ (uses `tomllib`) and, optionally, `git` on PATH (history, tracked-file and ignore checks).

```powershell
.venv\Scripts\python.exe scripts\repo_audit baseline     # first run: audit + freeze .audit\baseline.json
.venv\Scripts\python.exe scripts\repo_audit all          # full audit -> .audit\REPORT.md (+ report.json, sections\*.json, TREE.txt)
.venv\Scripts\python.exe scripts\repo_audit python frontend
.venv\Scripts\python.exe scripts\repo_audit diff         # metric deltas vs baseline
.venv\Scripts\python.exe scripts\repo_audit check        # exit 1 if a gated metric regressed
.venv\Scripts\python.exe scripts\repo_audit impact --map moves.json     # references to paths that will move
.venv\Scripts\python.exe scripts\repo_audit impact --paths backend_functions\music_functions.py
.venv\Scripts\python.exe scripts\repo_audit all --with-external         # also cloc / knip (run from the app dir)
```

## Sections

| section | what it answers |
|---|---|
| inventory | real code vs data/docs/generated; duplicates; clutter; manifests; stray path mirrors |
| python | AST import graph, dangling imports, package cycles, layer rules, private cross-imports, orphans (with string/dynamic-reference evidence), routes, router→DB calls, complexity, tests, deps |
| frontend | TS import graph with tsconfig aliases, Next entry-point reachability (replaces knip when run from the wrong dir), unused/undeclared npm packages, cross-route imports, single-consumer modules |
| contracts | FastAPI routes vs frontend path literals (heuristic) |
| domains | domain × layer matrix, cross-domain edges, unassigned files |
| hygiene | tracked/untracked secrets (values never written), history, tracked junk, `.pyc` husks, Streamlit residue |
| sql | migration classification, rollback pairing, DDL objects, proposed global numbering |
| triage | scripts scored as durable tool vs ticket one-off |
| git | churn hotspots, co-change coupling, folders/domains touched per commit |

Configure domains and layer rules in `audit.toml` (looked up in the repo root, then next to the package).
Every analyzer failure is captured in the report instead of aborting the run.
Findings are heuristics: `likely-dead`, `contracts` and `dependencies` are candidates, not verdicts.
