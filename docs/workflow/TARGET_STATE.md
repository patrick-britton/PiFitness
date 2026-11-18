# PiFitness Target State

This document defines the architectural target state and operational invariants of PiFitness. Tools enforce these principles; prose advises developers and agents.

---

## 1. Core Principles

1. **Tools enforce; prose advises.** If a rule matters, a script checks it.
2. **Evidence before done.** No task or feature closes without recorded command output. Humans confirm only what requires human judgment.
3. **Design for the weakest model.** Tasks are small and bounded; state lives in files and sidecars, never in conversation history. Open-question answers, decisions, and approvals are written to the design file; an answer given only in chat does not exist.
4. **One home per fact.** Every concept, requirement, and data definition lives in exactly one place; everything else links to it.
5. **Agents flag; human scopes.** Discovered out-of-scope defects go to the backlog; agents never expand scope unilaterally.
6. **Generate where possible.** Task briefs, schema maps, UAT checklists, and request templates are produced by deterministic tools, not written by hand.
7. **Single-feature serial flow.** Exactly one feature is in development at a time across the entire codebase by a single human developer, on the single `feature-test` branch. Agents never commit; the human commits.
8. **Lineage-first discovery.** Before inspecting source or schema, agents consult the data dictionary and lineage graph to bound context.
9. **Unattended Pi Deployment:** No manual workflows run on the Raspberry Pi. The launcher is the sole entry point, orchestrating deployments through `deploy/scripts/autodeploy.sh` with modular step helpers in `deploy/scripts/lib/`.

---

## 2. Verifiable Statements

### Repo & Documentation

- **R1. Mapped Repository.** Every folder is documented in `docs/REPO_MAP.md` with its purpose, allowed dependencies, and prohibited contents. *Check: `tools/check_docs.py` / folder health check flags unmapped folders.*
- **R2. Evidenced Architecture.** `docs/ARCHITECTURE.md` defines system layers, request flow, and deployment topology; all architectural claims cite file paths. *Check: `python tools/check_docs.py cites` passes with zero unresolved citations.*
- **R3. Canonical Index.** `docs/INDEX.md` indexes all documentation and rules, establishing explicit ownership to prevent duplication. *Check: duplicate-heading and index scan passes.*
- **R4. Documented UI Principles.** `docs/UI_PRINCIPLES.md` defines mobile-first interaction rules, tap targets (≥44px), back-gesture behavior, and bottom-sheet conventions, each with an automated or human check method.

### Data & Lineage Layer

- **D1. Generated Schema Map.** `docs/data/schema.generated.md` matches the live DEV database catalog. *Check: regeneration against DEV produces zero git diff.*
- **D2. Catalog & Comments.** Every table and column carries a PostgreSQL `COMMENT`. `docs/data/dictionary.md` defines domain grains, canonical sources, and business rules.
- **D3. Numbered Additive Migrations.** DDL changes occur solely through numbered migration files in `deploy/migrations/`. Migrations are additive and backward-compatible with deployed PRD code. DEV applies via tool; PRD is applied automatically during `autodeploy.sh` via `dbmate up` after taking a rollback baseline.
- **D4. Lineage Precedence.** For any database, ETL, API, or UI data flow investigation, the lineage CLI is invoked first (`python tools/data_dictionary/lineage.py describe <target>`). No broad repository searches occur before the lineage graph is queried.

### Verification & Quality Gates

- **V1. Single Verification Gate.** `python tools/verify.py --scope all` runs the full verification pipeline (compilation, typecheck, linting, tests, smoke) and exits 0 against recorded baselines.
- **V2. Classified Tests.** Every test carries a docstring and an explicit marker registered in `pytest.ini`:
  - `@pytest.mark.invariant`: Mathematical and business logic invariants.
  - `@pytest.mark.contract`: API and schema shape agreements.
  - `@pytest.mark.smoke`: Fast critical-path sanity checks.
  - `@pytest.mark.journey`: End-to-end user workflows.
  Tests never mock the unit under test.
- **V3. Clean Comment Hygiene.** `python tools/lint_comments.py` exits 0. Source code contains no task IDs, bug IDs, or change-history comments.
- **V4. Structural Health.** Import linter contracts prevent reverse dependencies between layers (e.g. backend functions calling domain routers). Unbounded queries (`SELECT *` without limit) and performance smells are prohibited.
- **V5. Mobile Viewport Standards.** All UI screens are verified at 360×740 and 448×875: no horizontal scroll, tap targets ≥44px, appropriate virtual keyboard types, and clean console logs.

### Workflow & Lifecycle

- **W1. Interactive Request Form.** Feature requests originate from an interactive local HTML form (`tools/workflow/request_form.html`, served by `tools/workflow/request_form.py`) that saves structured Markdown into `.features/descriptions/<id>.md`.
- **W2. Automated UAT Generation.** Every feature design contains testable Acceptance Criteria (ACs). Each AC line is the human's UAT step (do, then see, checked at 360×740 and 448×875 for UI) and carries one `auto:` sub-line. `python tools/workflow/fd.py uat build <id>` compiles the AC lines into an interactive, mobile-responsive `<id>_uat.html` checklist.
- **W3. Strict Micro-Task Sizing.** A task represents one concern, touches at most as many files as it has points (never more than 3), has exactly one verify command, and carries `Points: 1`, `2`, or `3`:
  - `1 pt`: Single-file edit.
  - `2 pt`: Standard paired edit (route + query, or component + helper).
  - `3 pt`: Maximum complexity (route + model + migration, or component + hook + helper).
  Tasks estimated >3 points are split during planning. *Check: `python tools/workflow/fd.py check <id>` errors on any other Points value or on too many files; `fd.py set <id> resume` writes the feature's total `points:`.*
- **W4. Anchored Planning.** Every task in the feature plan defines verified file/symbol anchors (`modify`, `create`, `delete`, `pattern`, `read`). No speculative or unverified file paths exist in the plan.
- **W5. Evidence-Gated Execution.** `fd.py done <id> T##` refuses to run without a prior `fd.py start <id> T##` entry, and records the verify command's exit code and output. A task is set `complete` (`fd.py set <id> T## complete`) only after a `done` entry with exit 0. Agents never use `--force`. *Check: `fd.py log <id>` shows start and a passing done for every complete task; reviewed by the human, not enforced by the tool.*
- **W6. Budgeted Execution Sessions.** Agents execute against a story point budget, completing tasks in order and cleanly checkpointing state (`fd.py resume <id>`) when the budget is reached. Agents leave changes uncommitted; the human reviews and commits.
- **W7. UAT Feedback Loop.** Human mobile testing results are written to `.features/designs_active/<id>_feedback.md` in the format `fd.py uat import <id>` parses, by a local UAT launcher (to be built), driving a dedicated polish cycle before test hardening and code review.
- **W8. Pre-Deploy Code & Test Review.** Prior to deployment, automated tests are verified, comments linted, and repository structure audited to ensure zero technical debt or stray files.
- **W9. Out-of-Scope Backlog.** Findings outside the active feature are appended to `docs/workflow/BACKLOG.md` via `tools/workflow/fd.py backlog add`. Agents never expand active feature scope.
- **W10. Bounded Test Growth.** The `auto:` lines of the design's Acceptance Criteria are the complete list of new tests a feature may add. Every new test traces to one of them, carries a docstring and a V2 marker, is named for the behavior it guards (never the feature or task), and does not duplicate existing coverage. Scratch and one-off checks are never left in the repository. *Check: the review workflow lists new test files and functions from `git status` and `git diff main` and compares them with the AC `auto:` lines; a tool for this is to be built.*
- **W11. File-Recorded Decisions.** Open questions live in the design file. Only the human's written `Answer:` resolves one; an agent sets `Decision` and `Status` from it. *Check: `python tools/workflow/fd.py oq <id> --open` and `--unanswered`.*

### Environments & Deployment

- **E1. Isolated Local DEV.** DEV runs locally. The local agent environment contains no PRD credentials. The DEV database runs on `localhost:15432` (`pifitness_dev`) and carries a database-level marker (`pifitness.env = 'dev'`). Tools and backend refuse to run without this marker.
- **E2. Fast DEV Reset.** A clean template database enables DEV resets in seconds via `python tools/db/dev_reset.py`, automatically replaying pending migrations and re-stamping the dev marker.
- **E3. Git Trunk Discipline.** `main` is the deployable trunk. All agent work happens on the single `feature-test` branch, which the human keeps up to date with `main` between features. Agents never commit, push, tag, switch branches, or merge, and every code-touching workflow stops unless `git branch --show-current` prints `feature-test`. The human commits, pushes `feature-test` for TEST deployment, merges to `main`, and pushes.
- **E4. Pi Autodeploy Engine (`autodeploy.sh`).** All deployment to the Raspberry Pi is orchestrated by `deploy/scripts/autodeploy.sh` driven by modular Python step handlers in `deploy/scripts/lib/`:
  - **TEST Deployment (10 steps):**
    1. Read-only preflight and DB comparison (`step_preflight.py`).
    2. Clean TEST app checkout into `/home/god/PiFitness-test` (`step_checkout.py`).
    3. Reconcile dependencies via `uv sync` and `npm ci` (`step_dependencies.py`).
    4. Database clone / restore with clean baseline capture (`step_db_prepare.py`).
    5. Apply pending migrations via `dbmate up` (`step_migrate.py`).
    6. In-process regression tests (`step_regression.py` running pytest and vitest while PRD is still up).
    7. Stop PRD services and record pre-existing state (`step_stop_services.py`).
    8. Start TEST services on normal ports (`step_start_services.py`).
    9. Automated smoke tests against running TEST stack (`step_smoke.py`).
    10. Human manual test gate (GO / NO-GO) with 30-minute timeout (`step_manual_gate.py`). On failure or NO-GO, the exit handler automatically halts TEST and restores PRD.
  - **PRD Deployment (9 steps, zero prompt):**
    1. Read-only preflight (`step_preflight.py`).
    2. Stop PRD services and record active set (`step_stop_services.py`).
    3. Atomic swap of clean PRD application checkout in `/home/god/PiFitness` (`step_checkout.py`).
    4. Reconcile PRD dependencies outside app dir (`step_dependencies.py`).
    5. Capture pinned PRD rollback baseline dump if migrations pending (`step_db_baseline.py`).
    6. Apply pending migrations via `dbmate up` (`step_migrate.py`).
    7. Start PRD services and verify public URL (`step_start_services.py`).
    8. Run regression tests against PRD (`step_regression.py`).
    9. Run live smoke tests against public HTTPS URL (`step_smoke.py`).
- **E5. Complete Automatic Rollback.** The `autodeploy.sh` exit handler (`deploy/scripts/lib/cleanup.py`) restores code, units, and database automatically on any error. Rollback restores from the exact pinned baseline dump recorded in the run state, never an ambiguous latest dump.
- **E6. Immutable PRD Releases.** Every production deployment is stamped with a git tag `prd-YYYYMMDD`. Rollback to an earlier release deploys that tag's specific commit.

### Agent Ergonomics

- **A1. Lean Always-On Rules.** Core global rules in `.clinerules/` are modular, concise, and enforce tool constraints without consuming excessive context tokens.
- **A2. Specialized Workflow Prompts.** Distinct workflows (`.clinerules/workflows/`) guide specific lifecycle phases (design, plan, execute, polish, test, review), preventing model confusion.
- **A3. Native Tool Priority.** Agents prioritize dedicated project tools (`fd.py`, `lineage.py`, `verify.py`) over manual discovery or broad shell commands. The tools are canonical: instructions and templates follow the tools, and a change to a tool's behavior is made in the tool first.
