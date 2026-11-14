# TASK: Read-only repository investigation → structured report

You are investigating the PiFitness repository to answer 22 open design questions
about a new `deploy/scripts/autodeploy.sh`. You must NOT modify anything.
You are producing a REPORT, not code.

## HARD RULES
- READ-ONLY. Do not edit, create, delete, commit, stash, or checkout anything.
  Do not run `git pull`, `git fetch`, `git reset`, or `git clean`.
- Do not install packages, do not start/stop services, do not run migrations,
  do not connect to any database in write mode.
- Safe commands only: `ls`, `find`, `grep`/`rg`, `cat`, `sed -n`, `head`, `tail`,
  `wc`, `git log`, `git branch -a`, `git show`, `git status`,
  `pytest --collect-only`, `npm run` (to LIST scripts, not execute them),
  `node -e "..."` only to read package.json.
- NEVER print secret VALUES. For any `.env` / credentials file: list the KEY
  NAMES only, and print the value as `<redacted>`. Same for PGPASSWORD,
  tokens, API keys.
- For every claim, cite `path:line`. If you cannot find something, write
  "NOT FOUND" and say where you looked. Do not invent paths or names.
- If a file referenced by the design docs does not exist yet, say so explicitly;
  "file does not exist" is a valid and important finding.
- Where a question needs state that lives on the Raspberry Pi (not in this repo),
  say "NOT VERIFIABLE FROM REPO" and give the exact shell command the user
  should run on the Pi to answer it.
- Keep the report in Markdown. Aim for completeness over brevity, but do not
  pad; every line should carry a fact or an evidence pointer.

## PHASE 0 — Orientation (do this first)
1. `git rev-parse --abbrev-ref HEAD`, `git log --oneline -20`, `git branch -a`.
2. Produce a tree (2–3 levels) of: `deploy/`, `backend/` (esp. `backend/tests`,
   `backend/app`), `frontend/` (esp. tests/config), and any top-level
   `pyproject.toml`, `package.json`, `uv.lock`, `package-lock.json`.
3. List every file under `deploy/` with `wc -l`.
4. Locate and state whether these exist (yes/no + path):
   `deploy/scripts/bootstrap.sh`, `deploy/scripts/autodeploy.sh`,
   `deploy/scripts/autodeploy.conf`, `deploy/scripts/lib/`,
   `deploy/scripts/lib/ensure_binary.sh`, `deploy/tests.conf`,
   `deploy/systemd/`, `deploy/nginx/`, any `deploy.sh` at repo root or elsewhere,
   any `test-up.sh` / `test-down.sh`, any CI workflow under `.github/`.
5. Search the whole repo for the strings `autodeploy`, `PIFITNESS_`,
   `GO or NO-GO`, `failpoint`, `react-ui`, `feature-test`, `pifitness_test`,
   `UV_PROJECT_ENVIRONMENT`. Report each hit as `path:line: snippet`.

## PHASE 1 — Q1: What does a PRD run do?
Q1.6 Step structure: inspect existing deploy/test scripts. Report their internal
   step/function structure (function names, numbered sections, order). Is there
   any existing single orchestrator, or separate test/prd scripts? Quote the
   top-of-file comments and any `set -euo pipefail` / trap/exit-handler code.
Q1.7 PRD Step 1 pre-flight: grep for existing preflight checks: `df `, `command -v`,
   `systemctl cat`, `pg_isready`, `psql -c`, `curl`. Report what is checked today.
Q1.8 Manual gate: grep for `read -r`, `read -p`, `GO`, `NO-GO`, `timeout`.
   Report whether any existing script prompts a human, and how.
Q1.9 Branch: grep `react-ui`, `main`, `feature-test` across deploy scripts and
   CI. Report which branch each script pulls/targets today.
Q1.10 PRD app dir & venv: grep `/home/god/PiFitness`, `venv`, `test-venv`,
   `UV_PROJECT_ENVIRONMENT`, `--python`. Report every venv path used and whether
   it is inside or outside the app dir.
Q1.11 PRD order: reconstruct the ORDER of operations in the legacy PRD deploy
   script (stop → env → checkout → deps → migrate → start → tests → rollback?).
   Quote the relevant lines. State whether tests run before or after services
   start.
Q1.12 PRD rollback scope: grep `rollback`, `restore`, `backup`, `trap`,
   `previous`, `staging`. Report exactly what the legacy script rolls back
   (code? units? DB?) and what it does NOT.

## PHASE 2 — Q2: Fresh-clone method (dump/restore)
Q2.13 Cost: find any documented DB size, backup size, or timing. Look in README,
   docs, comments, `tools/db/*`. Report actual `ls -lh` of any backup files
   present in the repo (unlikely) or say NOT VERIFIABLE FROM REPO and give the
   Pi command: `ls -lh $HOME/Documents/DB_Backups | tail`.
Q2.14 Clean baseline: grep `baseline`, `clean`, `state`, `dirty`, `migrated`,
   `.pifitness-test-state`. Report whether any baseline mechanism exists today.
Q2.15 Dump naming: grep `personal_fitness_`, `.dump`, `pifitness_prd_`,
   `pg_dump`, `pg_restore`. Report every dump filename pattern and where each is
   produced/consumed.

## PHASE 3 — Q3: Do pytest / npm tests need the running stack?
Q3.A Static inspection — for BOTH the backend test tree and the frontend test
   tree, grep (case-insensitive) and report hits with `path:line`:
   `localhost`, `127.0.0.1`, `:8000`, `:3000`, `BASE_URL`, `API_URL`,
   `baseURL`, `TestClient`, `httpx`, `requests.`, `playwright`, `selenium`,
   `cypress`, `page.goto`, `uvicorn`, `subprocess`, `npm run dev`, `next start`,
   `TestClient(`, `AsyncClient`.
   Then classify each test file as: (a) in-process (TestClient / jsdom /
   mocked HTTP), (b) needs a live server, (c) unclear. Give the classification
   as a table: file | evidence line | class | rationale.
Q3.4 Test tree & npm script: print the full tree of the backend test dir, and the
   exact `scripts` block from every `package.json` (root, frontend, any nested).
   Identify which script is the “test” script.
Q3.5 Next test runner: from `package.json` devDependencies and any
   `jest.config.*`, `vitest.config.*`, `playwright.config.*`, `cypress.config.*`,
   `karma.conf.*`, `.mocharc.*`, state which runner(s) are used and whether they
   need a live URL.

## PHASE 4 — Q4: Rollback for PRD
Q4.16 PRD baseline timing: inspect the legacy deploy script for when a DB backup
   is taken (before migration? every run? only if pending?). Quote lines. Report
   whether `dbmate status` is consulted before deciding to back up.
Q4.17 Location: report the exact backup directory and any override env vars
   (`PIFITNESS_PRD_BACKUP_DIR`, `PIFITNESS_PRD_BACKUP_GLOB`, `BACKUP_DIR`).
Q4.18 Code/units rollback: grep `systemctl`, `enable`, `disable`, `start`,
   `stop`, `is-active`, `daemon-reload`. Report whether the legacy script
   records the pre-existing active set and restores it, and whether it restores
   the previous code checkout (staging/previous dirs).

## PHASE 5 — Q5: Config keys
Q5.1 Inventory every `KEY=value` assignment in `deploy/scripts/*.sh`,
   `deploy/*.conf`, `deploy/tests.conf`, and the Pi env files if present in the
   repo. Present as a table: key | file:line | example value (redact secrets).
   Then mark each key as: PRESENT (has a value) / NAMED-ONLY (mentioned in docs
   or comments, no value) / MISSING (needed by the design docs but nowhere).
Q5.2 `deploy/tests.conf`: does it exist? If yes, print it in full. If no, say
   NOT FOUND and search for any other test-command config.
Q5.3 Env file wiring: grep `backend/.env`, `ln -s`, `cp `, `.env.test`,
   `.env`. Report whether the legacy flow symlinks or copies the env file into
   the checkout, and the exact source/destination paths.

## PHASE 6 — Q19–Q22
Q19 Final-state guarantees: search docs/README for any statement of what state
   is left after failure/success. Report quotes or NOT FOUND.
Q20 Disk thresholds: grep `df `, `disk`, `free`, `threshold`, `MB`, `GB`.
   Report any existing check and its threshold, or NOT FOUND.
Q21 TEST services during PRD: grep for a “test-down” / `systemctl stop
   *-test*` step and whether PRD deploy verifies TEST units are inactive.
Q22 Config split: for every config file, report which script reads it and at
   what point in the flow (before vs after checkout). Specifically: can
   `deploy/tests.conf` only be read after checkout, and is it read by anything
   today?

## PHASE 7 — Contradictions & gaps
Produce a short list titled “CONTRADICTIONS / GAPS”:
- e.g. target venv outside app dir vs legacy venv inside app dir;
  PRD env file at ~/.config vs backend/.env; legacy `react-ui` vs `main`;
  PG port 15432 vs 5432; `deploy/tests.conf` referenced but missing;
  `autodeploy.sh` / `autodeploy.conf` referenced but missing.
For each, state the two conflicting facts with `path:line` on both sides.

## OUTPUT FORMAT (strict)
1. **Quick answer table** — one row per question Q1.6 … Q22:
   `Question | Short answer | Confidence (High/Med/Low) | Key evidence (path:line)`.
2. **Detailed sections** — for each question: Findings, Evidence (bulleted
   `path:line` + snippet ≤3 lines), Answer/Recommendation, Unknowns /
   NOT VERIFIABLE FROM REPO (with the Pi command to verify).
3. **Test classification table** (from Q3.A).
4. **Config key inventory table** (from Q5.1).
5. **CONTRADICTIONS / GAPS** list.
6. **Files inspected** — full list with line counts, so the user can see coverage.

Do not stop early. If a grep returns hundreds of hits, summarize counts and
show the 10 most relevant with `path:line`. Finish with the Quick answer table
first so it is readable without scrolling.