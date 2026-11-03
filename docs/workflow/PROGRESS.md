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

---

## Deviations and observations (for the 1.5.8 reviewer pack)

- **2026-11-03 Pi redeploy before test-up existed.** PRD moved from `2c52e7c` (pre-rewrite tree = `66d4fb1`) to `d19c589` mid-session, per the human's decision. Rationale: `d19c589` = `66d4fb1` + deployment tooling only; no runtime backend/frontend impact. Next redeploy is gated on Phase 5.5 `test-up`. Pi is currently running `d19c589` on branch `react-ui`.

- **Origin history rewrite observed at fetch.** `origin/main`, `origin/react-ui`, `origin/streamlit-prd` had forced updates. `2c52e7c` (PRD at session start) is orphaned from origin's branches; content is preserved in `66d4fb1` (which is an ancestor of `d19c589`). Pi-local tag `prd-baseline` (created but never pushed; Pi push auth is broken) records `2c52e7c` as a historical marker only.

- **Pi push auth broken (HTTPS, no PAT).** Pi cannot push to origin. Per D2 the Pi never pushes; no action taken. Backlog candidate only if Pi-initiated pushes are ever needed.

- **`prd-YYYYMMDD` tag convention applied** as `prd-20261103` at `d19c589` (current PRD per D1), not at `2c52e7c`. The historical `2c52e7c` is recorded by Pi-local `prd-baseline` only.

- **1.5.6 backlog source file (`.audit/p1/backlog_phase1.md`):** may not exist on disk (removed by INT-21 as tracked); verify before running 1.5.6, do not create it from an agent task.

- **`deployment_script` branch intentionally not pruned.** Waits for 5.1 per D16.