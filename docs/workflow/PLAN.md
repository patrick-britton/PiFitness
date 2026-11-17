# PiFitness Development Plan

This document defines the roadmap from our current Phase 6 workflow hardening through full application stabilization.

---

## 1. Operating Rules for Agents

1. **One Task at a Time:** Read only the assigned task row in this file, the specific files it lists, and `.clinerules/`. Do not read the entire plan.
2. **Strict Scope:** Touch only the files explicitly listed in the task row's **Files** column (maximum 3 files).
3. **Evidence Gate:** Every task specifies exactly one verify command. A task is complete only when the verify command is executed and its command line and last 20 output lines are appended to `docs/workflow/PROGRESS.md`.
4. **No Git Branching by Agents:** Work on the assigned `agent/*` branch. Never switch branches, create branches, push, merge, or touch `main`.
5. **Flag Out of Scope:** Any discovered issues outside the assigned task must be appended to `docs/workflow/BACKLOG.md` via `python tools/workflow/fd.py backlog add`. Never fix out-of-scope defects within the current task.

---

## 2. Completed Milestones (Phases 1–5 Summary)

| Phase | Core Achievements | Status |
|---|---|---|
| **Phase 1: Architecture & Maps** | Documented repository map (`docs/REPO_MAP.md`), architecture (`docs/ARCHITECTURE.md`), index, and UI principles. Established trunk-based git model (`docs/workflow/GIT_MODEL.md`). | **Completed** |
| **Phase 2: Gate & Dev Safety** | Built `tools/verify.py` gate, ratchet baseline engine (`tools/ratchet.py`), comment linter (`tools/lint_comments.py`), doc checker (`tools/check_docs.py`), and DEV DB marker enforcement (`pifitness.env = 'dev'`). | **Completed** |
| **Phase 3: Data Layer & Lineage** | Catalogued database schema and comments (`docs/data/`), unified DDL migrations in `deploy/migrations/`, built `tools/data_dictionary/lineage.py` for instant data-flow queries. | **Completed** |
| **Phase 4: Workflow Engine** | Built `tools/workflow/fd.py` with evidence gating (`start`/`done`), task briefing, state resumption, structured bug tracker, and backlog management. Tested with synthetic fixtures `fx5` and `fx6`. | **Completed** |
| **Phase 5: Environments & Deployment** | Designed and built greenfield deployment engine: modular `deploy/scripts/autodeploy.sh` and `deploy/scripts/lib/` (10-step TEST sequence, 9-step PRD sequence, atomic rollback, and pinned baseline dumps). Automated deployment script test harness passing. | **Completed** |

---

## 3. Phase 6: Workflow Hardening, Templates & Tooling

Phase 6 reconciles the workflow engine and agent prompts with the 12-step single-developer lifecycle defined in `docs/workflow/FEATURE_DEVELOPMENT_WORKFLOW.md`.

| ID | Task | Files | Done When |
|---|---|---|---|
| **6.1** | **Cleanup legacy docs & tools.** Relocate `tools/pifitness-hardware-health.py` to `backend/pifitness/worker/` and update `deploy/systemd/pifitness-hardware-health.service` `ExecStart`; retire superseded legacy deployment scripts (`deploy/scripts/test-up.sh`, `deploy/scripts/test-down.sh`, `deploy/scripts/pi5_deploy.sh`) now that `autodeploy.sh` and `lib/` are established; delete obsolete probe and diagnostic scripts (`tools/probe/`, `tools/diagnostics/`, `tools/refactor/`, `tools/audit/p1_facts.py`); prune temporary discovery dumps in `docs/workflow/pi-test-env-discovery-*`; update `docs/REPO_MAP.md` and `docs/INDEX.md` to reflect canonical tools. | `tools/`, `backend/pifitness/worker/`, `deploy/systemd/`, `deploy/scripts/`, `docs/` | `python tools/check_docs.py cites docs/REPO_MAP.md` exits 0; `python tools/verify.py --scope all` passes; no orphaned scripts in `tools/probe` or `tools/diagnostics`. |
| **6.2** | **Interactive Feature Request HTML Form.** Create `tools/workflow/request_form.html`: a standalone, dependency-free local web form allowing the human developer to fill out non-technical problem statements (P1..), desired behaviors, surfaces, and non-goals, with an "Export Request Markdown" button targeting `.features/descriptions/<id>.md`. | `tools/workflow/request_form.html` | File opens cleanly in Chrome/Edge, form validation works, clicking export downloads/generates valid markdown adhering to `_FEATURE_REQUEST_TEMPLATE.md`. |
| **6.3** | **Interactive UAT compilation in `fd.py`.** Add `uat build <id>` and `uat import <id>` commands to `tools/workflow/fd.py`. `uat build` parses Acceptance Criteria from `.features/designs_active/<id>_design.md` and renders a self-contained mobile-responsive `uat.html` checklist with [Pass]/[Fail] buttons, note fields, and an "Export Feedback Markdown" button. | `tools/workflow/fd.py`, `tests/test_fd.py` | `python tools/workflow/fd.py uat build fx6` generates valid `fx6_uat.html`; pytest passes in `tests/test_fd.py`. |
| **6.4** | **Modernize `.features/templates/` suite.** Create clean canonical templates: `_FEATURE_REQUEST_TEMPLATE.md`, `_FEATURE_DESIGN_TEMPLATE.md` (with 1–3 pt task sizing, file/symbol anchors, ACs), and `_FEATURE_FEEDBACK_TEMPLATE.md`. Remove legacy templates `.features/_DESCRIPTION_TEMPLATE.md`, `.features/_DESIGN_TEMPLATE.md`, and unused `.features/tools/`. | `.features/templates/`, `.features/` | Canonical templates exist in `.features/templates/`; legacy templates removed; `fd.py check` passes against new design template format. |
| **6.5** | **Author specialized workflow prompts in `.clinerules/workflows/`.** Create compact, non-verbose workflow instructions: `feature-design.md`, `feature-plan.md`, `feature-execute.md`, `feature-polish.md`, `feature-test.md`, and `feature-review.md`. Embed clear guidance on how agents utilize `lineage.py` and `fd.py`. Retire stale workflows (`ddrb.md`, `test_cleanup.md`). | `.clinerules/workflows/` | All 6 workflow markdown files exist and comply with the ≤2 KB per rule guideline; `check_docs.py` index scan passes. |
| **6.6** | **Phase 6 exit check & review pack.** Run full verification gate, validate template fixtures, and generate reviewer pack in `.audit/reviews/phase-6.md`. | `.audit/reviews/phase-6.md`, `docs/workflow/PROGRESS.md` | `python tools/verify.py --scope all` exits 0; reviewer pack recorded. |

---

## 4. Phase 7: Pilot Feature Validation

Execute a real, end-to-end pilot feature through the complete 12-step lifecycle to validate the interaction between human forms, agent workflows, mobile UAT, and the Pi test swap.

| ID | Task | Files | Done When |
|---|---|---|---|
| **7.1** | **Pilot Feature Intake & Design.** Select a small pending capability or fix (e.g. food search polish or timer display). Fill request via `request_form.html`, run `/feature-design`, generate `uat.html`. | `.features/descriptions/`, `.features/designs_active/` | Design file and `uat.html` exist with valid ACs and lineage references. |
| **7.2** | **Pilot Feature Planning & Execution.** Run `/feature-plan` to split into 1–3 point micro-tasks. Run `/feature-execute` with a point budget to implement, verify, and commit each task. | As defined by pilot tasks | All tasks complete; verify command passes for each task. |
| **7.3** | **Pilot UAT & Polish Loop.** Human tests locally via `uat.html`, exports feedback, agent runs `/feature-polish` to fix defects, runs `/feature-test` to add classified tests, and `/feature-review` for final code hygiene. | `.features/designs_active/`, test files | All ACs marked Pass in `uat.html`; full verify gate exits 0. |
| **7.4** | **Pilot Staging Swap & Acceptance.** Human runs launcher on Pi selecting TEST (`autodeploy.sh --environment test`), verifies feature on phone over HTTPS, answers GO in the terminal gate, merges to `main`, tags `prd-YYYYMMDD`, and deploys via PRD run (`autodeploy.sh --environment prd`). | Pi deployment | Real phone acceptance complete; PRD service active; feature archived to `.features/designs_complete/`. |

---

## 5. Phase 8: Module Stabilization Sweep & Backlog Burndown

Systematic walkthrough and stabilization of core functional modules using the battle-tested 12-step workflow:
- **8.1: Food & Nutrition Module** (diary entries, search, barcode scanner, recipe builder).
- **8.2: Activities & GPS Module** (activity details, elevation, segment matching).
- **8.3: Weight & Metrics Module** (weigh-ins, trend calculations, hardware health).
- **8.4: Music & Spotify Integration** (listening history, playlist syncing, token rotation).
- **8.5: Performance Optimization (B20)** (indexing, query bounds, elimination of N+1 and unbounded `SELECT *`).

---

## 6. Phase 9: Final Freeze & Retrospective

1. Convert recurring developer lessons into permanent static checks.
2. Store evergreen prompts in `docs/workflow/prompts/`.
3. Freeze core architecture and create release tag `v1.0.0`.
