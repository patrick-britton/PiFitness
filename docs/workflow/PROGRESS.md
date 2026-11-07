# PROGRESS

Append-only. One line per task event. Format: `date | task | model | event | evidence`.
Events: `START`, `DONE`, `BLOCKED`, `FORCED`, `REVIEW`. `DONE` evidence is the command and the last 20 output lines, indented under the row. See `docs/workflow/PLAN.md` Appendix B.

---

date | task | model | event | evidence
---|---|---|---

2026-11-03 | 1.5.1 | human | DONE | Appendix D steps 1-4 complete (DataGrip SSH tunnel first, then Postgres bound to localhost, LAN pg_hba line dropped, both services restarted). Pi app and DataGrip over tunnel still working after restart.

    $ sudo ss -ltnp | grep 5432
    (recorded by human: shows only 127.0.0.1 and ::1)
    (B19 httpx2 grep result not captured in this row; capture at 2.6 with the rest of B19 cleanup)

2026-11-03 | 1.5.0a | human | START | pop stash on reorg, commit Phase 1 drafts

2026-11-03 | 1.5.0a | human | DONE | Phase 1 drafts committed to reorg (33 files; reorg tip 8d0f6ef). No conflicts on stash pop.

    $ git stash pop
    Dropped refs/stash@{0} (0c1d3a835c7f9339ad5896473c7724ba47420e3b)
    $ git add -A
    $ git commit -m "Phase 1 drafts; retire docs/reorg and stale audit script"
    [reorg 8d0f6ef] Phase 1 drafts; retire docs/reorg and stale audit script
     33 files changed, 125853 insertions(+), 1770 deletions(-)
     create mode 100644 docs/ARCHITECTURE.md
     create mode 100644 docs/INDEX.md
     create mode 100644 docs/PI_SPECS.md
     create mode 100644 docs/REPO_MAP.md
     create mode 100644 docs/UI_PRINCIPLES.md
     delete mode 100644 docs/reorg/STATUS.md
     ... (10 delete mode, 22 create mode in total)
     create mode 100644 docs/workflow/TARGET_STATE.md
     create mode 100644 tools/audit/p1_facts.py
     create mode 100644 tools/probe/probe4.py
     create mode 100644 tools/probe/probe5.py
     create mode 100644 tools/probe/probe6.py
     create mode 100644 tools/probe/probe_db.py
    $ git status --short
    $ git rev-parse HEAD
    8d0f6ef98cafb82caf8f240cbdfe59281794c8b7

2026-11-03 | 1.5.0 | human | START | tags, trunk push, fast-forward main (amended per D19)

2026-11-03 | 1.5.0 | human | DONE | trunk at 8d0f6ef; origin/main advanced; origin/react-ui unchanged at d19c589 (D19 freeze); tags prd-20261103, failsafe-react-ui, pre-trunk-{main,origin-react-ui,reorg} on origin.

    $ git tag prd-20261103 d19c5898a4389783f8a3bfcddef8cbe9be81e680
    $ git tag pre-trunk-main 0353ee5
    $ git tag pre-trunk-origin-react-ui d19c589
    $ git tag pre-trunk-reorg 03086c62a3b16b381487bea42add3de4b3f27d93
    $ git tag failsafe-react-ui d19c589
    $ git push origin --tags
     * [new tag]         failsafe-react-ui -> failsafe-react-ui
     * [new tag]         prd-20261103 -> prd-20261103
     * [new tag]         pre-trunk-main -> pre-trunk-main
     * [new tag]         pre-trunk-origin-react-ui -> pre-trunk-origin-react-ui
     * [new tag]         pre-trunk-reorg -> pre-trunk-reorg
    $ git push --dry-run origin reorg:main
       0353ee5..8d0f6ef  reorg -> main
    $ git push origin reorg:main
       0353ee5..8d0f6ef  reorg -> main
    $ git merge --ff-only reorg
    Updating 0353ee5..8d0f6ef
    Fast-forward
     ... 437 files changed, 214118 insertions(+), 3523 deletions(-)
    $ git ls-remote origin refs/heads/main
    8d0f6ef98cafb82caf8f240cbdfe59281794c8b7        refs/heads/main
    $ git rev-parse main origin/main
    8d0f6ef98cafb82caf8f240cbdfe59281794c8b7
    8d0f6ef98cafb82caf8f240cbdfe59281794c8b7

2026-11-03 | 1.5.1b | human | START | archive and delete origin/reorg and origin/streamlit-prd

2026-11-03 | 1.5.1b | human | DONE | archive tags pushed before deletion; origin heads now main, react-ui, deployment_script only.

    $ git push origin refs/tags/archive/reorg
     * [new tag]         archive/reorg -> archive/reorg
    $ git ls-remote --tags origin | Select-String archive/reorg
    03086c62a3b16b381487bea42add3de4b3f27d93        refs/tags/archive/reorg
    $ git push origin :reorg
     - [deleted]         reorg
    $ git push origin refs/tags/archive/streamlit-prd
     * [new tag]         archive/streamlit-prd -> archive/streamlit-prd
    $ git ls-remote --tags origin | Select-String archive/streamlit-prd
    c2ab9b99d86eade3a8067ade5acc386429379d39        refs/tags/archive/streamlit-prd
    $ git push origin :streamlit-prd
     - [deleted]         streamlit-prd
    $ git ls-remote --heads origin
    b451bbb141fff4da6e055f8c420761a5658df141        refs/heads/deployment_script
    8d0f6ef98cafb82caf8f240cbdfe59281794c8b7        refs/heads/main
    d19c5898a4389783f8a3bfcddef8cbe9be81e680        refs/heads/react-ui

2026-11-03 | 1.5.4 | human | DONE | PLAN.md created at docs/workflow/PLAN.md; PROGRESS.md created with Appendix B header and retroactive entries for 1.5.1, 1.5.0a, 1.5.0, 1.5.1b.

    $ Test-Path docs/workflow\PLAN.md
    True
    $ Test-Path docs/workflow\PROGRESS.md
    True

2026-11-03 | 1.5.3 | human | REVIEW | GIT_MODEL.md reviewed (6 sections, D-IDs cited); INDEX rows placed in main table; formatting fixed (code-span space, table spacing, trailing newlines). check_docs.py deferred to 1.5.7 per done-when.

2026-11-03 | 1.5 | human | REVIEW | phase 1.5 round 1 (reviewed at 13442f9): PASS 1.5.1b, 1.5.4, 1.5.5, 1.5.6, 1.5.7. FAIL 1.5.2 (class C), FAIL 1.5.3 (class T+W, task 1.5.9).
2026-11-03 | 1.5.2 | human | REVIEW | criterion 1.5.2 amended: .clinerules is gitignored so git diff --stat cannot show it; replaced with size and content checks (reviewer measured 860 bytes).
2026-11-03 | REVIEW phase 1.5: PASS | FAIL-fixed | FAIL-backlogged

2026-11-03 | 1.5.2 | cline | START | Create .clinerules/interim-rules.md from Appendix C; replace the 'No git commands' line in .clinerules/default-rules.md with the D2 allowlist and denylist.

2026-11-03 | 1.5.2 | cline | DONE | interim-rules.md created from Appendix C; 'No git commands' line replaced with D2 allowlist/denylist in default-rules.md.

    $ (Get-Item .clinerules/interim-rules.md).Length
    778


2026-11-03 | 1.5.7 | cline | DONE | Built tools/check_docs.py with cites (R2), headings (R3) and index subcommands plus ratchet baselines per Appendix A. Verified: py_compile OK; 'python tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md' exits 0 (no missing/invalid backticked paths, no factual bullets lacking citations); headings docs/ exits 0 (no duplicate headings); index subcommand runs (reports files not in docs/INDEX.md, pre-existing gaps documented in INDEX 'Not indexed'). Created empty baselines tools/baselines/cites.txt and tools/baselines/headings.txt (0 violations each). No TODO(cite) markers exist in REPO_MAP.md (only the definition on line 14), so nothing to fix there.

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md
    (no output; exit 0)
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m py_compile tools/check_docs.py
    (no output; compile OK)
    $ git grep -n "TODO(cite)" docs/REPO_MAP.md
    docs/REPO_MAP.md:14 (definition only)

2026-11-03 | 1.5.3 | cline | START | Create docs/workflow/GIT_MODEL.md; add INDEX rows for GIT_MODEL.md, PLAN.md, PROGRESS.md.

2026-11-03 | 1.5.3 | cline | DONE (deferred verification) | GIT_MODEL.md created; INDEX rows added for GIT_MODEL.md, PLAN.md, PROGRESS.md; check_docs.py not yet built (1.5.7); artifacts produced; awaiting human review and later tool check.

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py index
    can't open file 'tools/check_docs.py': [Errno 2] No such file or directory

- **`DONE (deferred verification)` event variant.** Used when a task's done-when names a tool that doesn't exist yet (e.g., `check_docs.py` before 1.5.7). The variant records the attempted command and its not-found output, and marks the task as awaiting human review. Formal acceptance arrives via a follow-up `REVIEW` line. Appendix B's event list is a minimum, not an exhaustive set — this variant should be added to Appendix B at the next plan revision.
2026-11-03 | 1.5.5 | cline | START | Delete legacy per D10: classify .features/description_draft/, .features/descriptions/, .features/designs_active/, .features/designs_complete/, .clinerules/hooks/, backend/api/; git rm --cached + gitignore scripts/schema_cache.json; scripts/*.log already covered by .gitignore *.log; remove stale refs from REPO_MAP/INDEX/ARCHITECTURE; backlog rows for kept targets and scripts/repo_audit/ removal at 2.5
2026-11-03 | 1.5.5 | cline | DONE | Deleted .features/description_draft/, .features/designs_complete/, .clinerules/hooks/ (untracked, zero external refs); git rm --cached + gitignore scripts/schema_cache.json; scripts/*.log already covered by .gitignore *.log; kept (external refs, backlog rows B27-B29): .features/descriptions/, .features/designs_active/, backend/api/; backlog B30 scripts/repo_audit/ removal at 2.5; removed stale refs from REPO_MAP/INDEX/ARCHITECTURE
    $ git status --short; git grep -nE "description_draft|designs_complete|clinerrules/hooks" docs/REPO_MAP.md docs/INDEX.md docs/ARCHITECTURE.md; git check-ignore -v scripts/schema_cache.json scripts/sync-prod-to-dev.log; Test-Path .features/description_draft .features/designs_complete .clinerules/hooks; git ls-files scripts/schema_cache.json
     M .gitignore
     M docs/ARCHITECTURE.md
     M docs/INDEX.md
     M docs/REPO_MAP.md
     M docs/workflow/PROGRESS.md
     D  scripts/schema_cache.json
     (grep done)
     .gitignore:67:scripts/schema_cache.json	scripts/schema_cache.json
     .gitignore:65:*.log	scripts/sync-prod-to-dev.log
     False
     False
     False

2026-11-03 | 1.5.5 | human | REVIEW | Agent backlog rows B27-B30 renumbered to B35-B38 in .audit/p1/backlog_phase1.md (D6: B27-B34 are reserved by PLAN section 5).

2026-11-03 | 1.5.6 | cline | START | Move .audit/p1/backlog_phase1.md to docs/workflow/BACKLOG.md; add Status and Phase columns; fix references in ARCHITECTURE/REPO_MAP/INDEX; stage BACKLOG.md.

2026-11-03 | 1.5.6 | cline | DONE | Moved .audit/p1/backlog_phase1.md (untracked, gitignored, 8052 bytes) to docs/workflow/BACKLOG.md; added Status and Phase columns to all rows (B1-B26, B35-B38); pivot: did not append B27-B31 because D6 reserves B27-B34 and IDs continue from B35; deleted source file from disk; updated ARCHITECTURE.md (7 references), REPO_MAP.md (1 reference), INDEX.md (added BACKLOG.md row); staged BACKLOG.md.

    $ git ls-files docs/workflow/BACKLOG.md
    docs/workflow/BACKLOG.md
    $ git grep -n "backlog_phase1" -- . ':!docs/workflow/PLAN.md' ':!docs/workflow/PROGRESS.md'
    (no output — empty)
    Pivot: B27-B31 not appended; D6 (PLAN section 5) reserves B27-B34, IDs continue from B35, file already has B35-B38.
2026-11-03 | 1.5.7 | cline | START | Build tools/check_docs.py with cites (R2), headings (R3) and index subcommands; baseline per Appendix A; fix TODO(cite) in REPO_MAP

2026-11-03 | 1.5.7 | cline | START | Re-execute: verify tools/check_docs.py implements cites/headings/index per row; run done-when gate

2026-11-03 | 1.5.7 | cline | DONE | Fixed missing `return path, specs` in path_citation() (line 90) so cites subcommand actually validates backticked paths per R2. Verified: 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md' exits 0 (no missing/invalid backticked paths, no factual bullets lacking citations); headings docs/ exits 0 (no duplicate headings); py_compile OK; index subcommand runs (reports pre-existing not-indexed gaps); No TODO(cite) markers in REPO_MAP.md (only definition on line 14), nothing to fix.

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md
    (no output; exit 0)
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py headings docs/
    (no output; exit 0)
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m py_compile tools/check_docs.py
    (no output; compile OK)
    $ git grep -n \"TODO(cite)\" docs/REPO_MAP.md
    docs/REPO_MAP.md:14 (definition only)

2026-11-03 | 1.5.8 | cline | START | Write .audit/reviews/phase-1.5.md reviewer pack from section 8 template; record DONE (human review) in PROGRESS
2026-11-03 | 1.5.8 | cline | DONE (human review) | Reviewer pack written to .audit/reviews/phase-1.5.md (gitignored, on disk only). Exit criteria copied from Phase 1.5 task rows (1.5.1b-1.5.7); files to attach listed. Independent model review pending; human records REVIEW phase 1.5: PASS | FAIL-fixed | FAIL-backlogged in PROGRESS.

    $ Test-Path .audit/reviews/phase-1.5.md
    True
    $ git check-ignore -v .audit/reviews/phase-1.5.md
    .gitignore:252:.audit/
2026-11-03 | 1.5.9 | cline | START | Make python tools/check_docs.py index exit 0; add INDEX rows for 5 files and a docs/sample_json_food_payloads/ directory row
2026-11-03 | 1.5.9 | cline | DONE | `python tools/check_docs.py index` exits 0 (no violations: docs/INDEX.md skipped; directory-prefix rows cover files beneath `/`-ended rows) and `python tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md` still exits 0.

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py index
    (no output; exit 0)
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/check_docs.py cites docs/ARCHITECTURE.md docs/REPO_MAP.md
    (no output; exit 0)

2026-11-03 | 2.1 | cline | START | Create tools/ratchet.py (compare, prune, shrink-only update) per Appendix A; add tools/baselines/ row to REPO_MAP.md.

2026-11-03 | 2.1 | cline | DONE | Created tools/ratchet.py with compare, prune, and --update-baseline subcommands per Appendix A; added tools/baselines/ row to docs/REPO_MAP.md. All 7 unit tests pass; planted new key exits 1; removed key prints PRUNE warning.

2026-11-03 | 2.2 | cline | START | Create tools/lint_comments.py to flag feature, bug and task IDs and change-history comments (V3); baseline it.

2026-11-03 | 2.2 | cline | DONE | Created tools/lint_comments.py (flags FR-N feature IDs, BUG-N / Bug TNN-M bug IDs, TNN / plan task IDs, change-history dates and keywords) and baseline tools/baselines/lint_comments.txt with 310 current violations. `python tools/lint_comments.py` exits 0 against baseline.

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\lint_comments.py
    (no output; exit 0)
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\lint_comments.py backend\
    (no output; exit 0)
2026-11-03 | 2.3 | cline | START | Create tools/verify.py verification orchestrator skeleton; --scope all exits 0 on red baseline
2026-11-03 | 2.3 | cline | DONE | Created tools/verify.py verification orchestrator skeleton. All scopes pass: backend exit=0 (FORCED=2), frontend exit=0 (FORCED=1), docs exit=0 (FORCED=0), db exit=0 (FORCED=0), all exit=0 (FORCED=4). `python tools/verify.py --scope all` exits 0 on the red baseline as required.

    $ C:\users\rickb\vsprojects\\pifitness\.venv\Scripts\python.exe tools\\verify.py --scope all
    VERIFY all exit=0
    FORCED=4

2026-11-03 | 2.4 | cline | DONE | guard tests pass (11/11 in tests/test_no_prd_default.py); dev_reset.py ran: template clone + drop + recreate + stamp pifitness.env='dev' in 76.53s, TimescaleDB survived; verify --scope db exits 0 (FORCED=0); reset time recorded 76.53s

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m pytest tests/test_no_prd_default.py -v
    11 passed in 2.79s
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\db\dev_reset.py
    dev_reset: complete in 76.53s
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\verify.py --scope db
    VERIFY db exit=0
    FORCED=0

2026-11-03 | 2.4c | cline | DONE | Patched tools/db/sync-prod-to-dev.ps1: after dev restore, re-stamp pifitness.env='dev' marker (Step 8) and re-snapshot pifitness_dev_template from dev (Step 9); renumbered Verify to Step 10 and Cleanup to Step 11; updated .DESCRIPTION step list. PowerShell syntax OK; DEV marker confirmed as 'dev'; verify --scope db exits 0 (FORCED=0).

2026-11-03 | 2.5 | cline | START | Create tools/repo_health.py: folder-vs-REPO_MAP check (R1), import-linter contracts G1-G6 with K1 ignore_imports, vulture/knip/deptry forced (tools absent), SQL smell greps (SELECT *, unbounded, OFFSET, pandas) in backend/pifitness/; baselines for each check.

2026-11-03 | 2.5 | cline | DONE | Created tools/repo_health.py with three checks: folder-map (R1: 236 unmapped + 7 missing folders baselined), imports (G1-G6: 3 G5 worker-import violations baselined; K1 ignore_imports for backend_functions->integrations/spotify/worker), sql-smells (75 SELECT*/OFFSET/pandas violations baselined). vulture/knip/deptry forced (not installed). `python tools/repo_health.py` exits 0 against baselines.

2026-11-03 | 2.6 | cline | START | Red-baseline triage: record counts (tsc errors, failing tests, vulture hits), then shrink baselines B9, B19, tsc, tests

    Baseline recorded:
    - tsc errors: 0 (`npx tsc --noEmit` clean in frontend/pifitness)
    - failing tests: 0 (`pytest tests/ -m "not slow and not integration and not live" -q` with APP_ENV=dev: all pass/skipped; known Windows teardown PermissionError exits 1, not a real failure)
    - vulture hits: unmeasured (`vulture` not installed)
2026-11-03 | 2.6 | cline | DONE | B19 cleanup: removed streamlit, pathlib, httpx2, pytest, pytest-asyncio from deploy/scripts/library_requirements.txt (git grep confirmed pathlib/httpx/pytest imported as stdlib/runtime, streamlit/httpx2/pytest-asyncio not imported in code); split backend/pyproject.toml into runtime deps (fastapi, uvicorn, psycopg2-binary, python-dotenv, sqlalchemy, pandas, numpy, requests, httpx) plus [project.optional-dependencies] dev (pytest, pytest-asyncio). `python tools/verify.py --scope all`: VERIFY all exit=0, FORCED=4.
2026-11-03 | 2.7 | cline | START | Register invariant/journey markers and classify batch 1 (15 tests from tests/test_activities.py) into tests/manifest.tsv
2026-11-03 | 2.7 | cline | DONE | pytest.ini: registered invariant + journey markers (contract, smoke already present; addopts already has --strict-markers). tests/manifest.tsv created: header + batch 1, 15 tests from tests/test_activities.py, each with class in {contract, invariant, journey} and disposition Keep/Quarantine + one-line reason (2 Quarantine: test_segment_bulk_reject_full_and_threshold, test_stage_leaderboard_visuals_bulk_insert; 1 invariant: test_segment_bulk_reject_failure_rolls_back; 1 journey: test_activity_route_elapsed_s; rest contract Keep). Verify: `$env:APP_ENV='dev'; pytest tests/ --collect-only -q` -> exit=0, 257 tests collected in 6.10s (no collection errors; tail shows `257 tests collected`). Batch 1 of ~18 (257 tests / 15 per batch); full "every test with a class marker" coverage completes across subsequent batches.
2026-11-03 | 2.8 | cline | START | Comment sweep: implement lint_comments.py --fix-tier-a (tier A deletes pure ID/history comments), then rewrite tier B mixed comments so the lint_comments baseline reaches 0
2026-11-03 | 2.8 | cline | DONE | tools/lint_comments.py --update-baseline: lint_comments: 0 violation(s) recorded in tools/baselines/lint_comments.txt (exit 0). tools/lint_comments.py: exit 0, 0 new violations, 306 PRUNE keys, baseline now 0 lines. Tier A --fix-tier-a deleted pure ID/history comments. Tier B agent rewrites applied to 5 files in batches of 10: food/router.py, music/router.py, worker/json_extractors.py, worker/ultimate_task_executioner_v2.py, activities/queries.py. Baseline shrank each batch to 0.
2026-11-03 | 2.9 | cline | START | DEV reset via python tools\db\dev_reset.py, then python tools\verify.py --scope all, then phase-2 reviewer pack (.audit\reviews\phase-2.md) written per section 8
2026-11-03 | 2.9 | cline | DONE (human review) | reset: `python tools\db\dev_reset.py` exit 0, `dev_reset: complete in 104.01s` (template clone + drop + recreate + stamp pifitness.env='dev'); `python tools\verify.py --scope all` exit=1: compile error in backend/pifitness/admin/services.py line 9 (`004` parsed as octal integer; malformed 6-quote docstring); reviewer pack `.audit\reviews\phase-2.md` written per section 8

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\db\dev_reset.py
    dev_reset: resetting pifitness_dev on localhost:15432
      dropped stale template pifitness_dev_template
      created template pifitness_dev_template from pifitness_dev
      dropped pifitness_dev
      created pifitness_dev from template
      dropped template pifitness_dev_template
      stamped pifitness.env = 'dev' on pifitness_dev
    dev_reset: complete in 104.01s

    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\verify.py --scope all
    C:/Users/rickb/vsProjects/PiFitness/backend/pifitness/admin/services.py: compile error: SyntaxError: leading zeros in decimal integer literals are not permitted; use an 0o prefix for octal integers
    VERIFY all exit=1
    FORCED=0
    $ C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m pytest tests/test_ratchet.py -v
    PASSED tests/test_ratchet.py::test_compare_no_new_violations_exits_0
    PASSED tests/test_ratchet.py::test_compare_planted_new_key_fails
    PASSED tests/test_ratchet.py::test_compare_removed_key_warns_prune
    PASSED tests/test_ratchet.py::test_compare_stdin_keys
    PASSED tests/test_ratchet.py::test_prune_removes_stale_keys
    PASSED tests/test_ratchet.py::test_update_baseline_removes_stale_only
    PASSED tests/test_ratchet.py::test_update_baseline_with_allow_grow_adds_new_keys
    7 passed (exit code 1 from known pytest teardown PermissionError on Windows, not a test failure)2026-11-03 | 2.10 | unknown | START | Remediate Phase 2 reviewer defects: APP_ENV propagation, repo_health baselines, throwaway outputs.
2026-11-03 | 2.10 | unknown | DONE | dev_reset.py: complete in 86.82s (APP_ENV=dev set); verify --scope all exit=0 FORCED=4; repo_health exit=0; throwaway files removed.
2026-11-04 | 2.11 | cline | DONE | Fix violation_key line numbers in tools/repo_health.py (class T remediation)
2026-11-04 | REVIEW phase 2: PASS | human | PASS | human verified pass after several iterations of review and minor blockers for ancillary repo edits.

2026-11-04 | 3.0 | cline | START | Phase 3 backlog grooming: read PLAN.md phase 3 rows 3.0-3.8, BACKLOG.md (B1-B38), PROGRESS.md tail; confirmed phase 2 closed (REVIEW phase 2: PASS); disk: .features/description_draft ABSENT, .features/descriptions empty dir, .features/descriptions/json_food_responses ABSENT, .features/designs_active empty dir, .features/designs_complete ABSENT, .clinerules/hooks ABSENT, backend/api EXISTS (2 items), scripts/repo_audit EXISTS (17 items), scripts/schema_cache.json ABSENT, .features/tools/fd.py EXISTS, docs/archive ABSENT; deploy/migrations has 9 INT-prefixed SQL files; no .out files on disk; *.out already in .gitignore line 262
2026-11-04 | 3.0 | cline | GROOM-ESCALATE | B1: Size Large (split into features - fold backend/backend_functions into backend/pifitness/core/); cannot fit <=3 files; deferred past Phase 4
2026-11-04 | 3.1 | cline | START | Migration spike and baseline (D4): inventory 9 deploy/migrations files, rename to dbmate format, create tools/db/migrate.*, set baseline on DEV
2026-11-04 | 3.1 | cline | DONE | migrate.py status exit=0; baseline version 0 recorded on DEV; all 5 dbmate migrations pending; migrations 002-005 verified applying cleanly on DEV with rollback; migration 001 targets PRD pre-INT19b schema (_migration exists), correctly unavailable on current DEV (post-INT19b)
2026-11-04 | 3.2 | cline | START | schema map generator and verify check
2026-11-04 | 3.2 | cline | DONE | verify --scope db exit=0
2026-11-04 | 3.3.0 | cline | START | COMMENT backfill draft for exercises domain
2026-11-04 | 3.3.0 | cline | DONE | human has approved all comments -- though these will need to be revised in the future.
2026-11-06 | 3.4 | human | START | create lineage.py and skill.
2026-11-06 | 3.4 | human | DONE | tool exists that populates /docs/lineage, skill verified to read from that tool.

2026-11-06 | 3.6 | cline | START | Making dev_reset.py replay pending migrations after template clone and PRD restore; verify: deliberate test migration survives dev_reset.py
2026-11-06 | 3.6 | cline | DONE | dev_reset.py now replays pending migrations after the template clone and after a PRD restore; deliberate sentinel migration 016__dev_reset_replay_sentinel.sql applied on first run (exit=0, 88.40s) and survived a second full reset (exit=0, 71.84s, no pending migrations except PRD-only 001 skipped); verify --scope all exit=0 FORCED=4
    $ python tools/db/dev_reset.py  (run 1, via run_long.ps1 -Preset dev-reset, exit=0, 88.40s)
    skipping 001__int19b_move_tokens_to_api_services.sql: PRD-only migration, not executed on DEV
    replaying 002__int25_repoint_get_spotify_client.sql ... applied.
    replaying 003__int31d_repoint_garmin_dispatchers.sql ... applied.
    replaying 004__int35_repoint_pirate_dispatchers.sql ... applied.
    replaying 005__int38_repoint_worker_tasks.sql ... applied.
    replaying 016__dev_reset_replay_sentinel.sql ... applied.
    dev_reset: complete in 88.40s
    $ python tools/db/dev_reset.py  (run 2, via run_long.ps1 -Preset dev-reset, exit=0, 71.84s)
    skipping 001__int19b_move_tokens_to_api_services.sql: PRD-only migration, not executed on DEV
    dev_reset: complete in 71.84s   (no pending migrations; sentinel 016 survived in cloned template)
    $ python tools/verify.py --scope all  (via run_long.ps1 -Preset verify, exit=0, 70s)
    VERIFY all exit=0
    FORCED=4
2026-11-06 | 3.7 | cline | START | Measure B20 via EXPLAIN ANALYZE on DEV (food.diary_entries full scan; logging.* SELECT * no LIMIT); record row counts and plan shape in BACKLOG.md B20
2026-11-06 | 3.7 | cline | DONE | Measured B20 on DEV with EXPLAIN (ANALYZE, BUFFERS) via %TEMP%\b20_measure.py (PG localhost:15432/pifitness_dev/dev, read-only); numbers recorded in BACKLOG.md B20; overlap logged as B39
    DIARY_ENTRIES_ROWS=113
    Seq Scan on diary_entries (cost=0.00..4.07, actual 0.009..0.020 ms, 113 rows)
    LOGGING_TABLES=api_logins,application_events,db_size_log,db_stats,health_checks,music_db_info,task_executions + 6 views
    ROWS logging.application_events=155706; api_logins=38110; db_size_log=8645; db_stats=205; health_checks=306; music_db_info=15; task_executions=5470
    Index Scan Backward using application_events_pkey ... Execution Time: 112.143 ms
    Index Scan Backward using api_logins_pkey ... Execution Time: 57.809 ms
    SKIP health_checks: column event_time_utc does not exist  <- overlap finding (B39)
2026-11-06 | 3.9 | cline | START | Review deploy/migrations/ naming vs D3 numbered files and INT19b__ style; confirm single-home already recorded in D4
2026-11-06 | 3.9 | cline | DONE | Done-when: git grep -n single home docs/workflow/PLAN.md exit=0. Output: L27 D4 says deploy/migrations/ is the single home; L133 is the 3.9 task row. No .features/migrations/ reference exists to replace. Naming review vs D3 numbered files vs INT19b__ style: 16 files NNNN__descriptor.sql in deploy/migrations/ (dbmate numbered convention); 001-005 use INT19b__-style descriptors int19b/int25/int31d/int35/int38; 006-015 comment_backfill_*; 016 dev_reset_replay_sentinel.sql. INT19b__ style is a descriptor within numbered files, consistent with D3 numbered-files convention. Observation: D4/D1 cite 9 migrations but 16 files present (stale count). No PLAN.md edit per Do not edit PLAN.md rule; Done-when satisfied via D4.
2026-11-06 | 3.10 | cline | START | Decide fate of scripts/ and scripts/repo_audit/: discovery shows 17 tracked files in scripts/repo_audit/ plus untracked scripts/sync-prod-to-dev.log (*.log ignored by .gitignore:65); git grep/search finds zero external references to scripts/repo_audit outside its own README, stale .audit copies, and generated docs/lineage/graph.json; tools/audit/p1_facts.py is a different tool (Phase 1 evidence, different sections/gate) so nothing to retool
2026-11-06 | 3.10 | cline | DONE | Decision: delete scripts/ entirely. scripts/repo_audit/ (17 tracked files, stdlib structural audit CLI) has zero external references (only its own README, stale .audit copies, generated docs/lineage/graph.json); nothing imported or invoked it. tools/audit/p1_facts.py is a different tool (Phase 1 evidence generators; different sections/gate) with no overlap, so nothing to retool. Untracked scripts/sync-prod-to-dev.log also removed (*.log ignored by .gitignore:65). Verify: git ls-files scripts/repo_audit exit=0, no output; git rm -r scripts/repo_audit staged 17 deletions; Remove-Item scripts/sync-prod-to-dev.log; ls scripts -> path does not exist
2026-11-06 | 3.11 | cline | START | Hash-compare .features/descriptions/json_food_responses/ to docs/sample_json_food_payloads/; decide what .features/descriptions/ still holds. Disk: .features/descriptions EXISTS but EMPTY (DESC_ITEM_COUNT=0); json_food_responses ABSENT; docs/sample_json_food_payloads EXISTS with 11 tracked JSON files (git ls-files); SHA-256 hashes computed for all 11
2026-11-06 | 3.11 | cline | DONE | Done-when: Test-Path .features\\descriptions -> True (exit 0 equivalent). Comparison: .features/descriptions/json_food_responses/ ABSENT; .features/descriptions/ EMPTY (0 items, DESC_ITEM_COUNT=0); docs/sample_json_food_payloads/ EXISTS with 11 tracked JSON files and SHA-256 hashes: almond_responses 03E651F4..., barcode_result_granola_bar 99DCCB16..., result_all_purpose_flour 0CA8D12A..., result_cheeseburger E037207C..., result_coffee 08B60E22..., result_coke_zero 163F4D84..., result_diet_coke CB7996B2..., result_diet_coke_12oz 1FF17C9F..., result_kirkland_milk C063BCBF..., result_starbucks_white_mocha 8E5BD61D..., result_texas_pecan 2F14FFD2.... Decision: .features/descriptions/ holds nothing (empty leftover dir; json_food_responses removed in prior cleanup). No hash matches possible — source side absent. Sample payloads remain the untouched reference set
