---
trigger: manual
---

# Feature Workflow Common Rules

Apply to every `/feature-*` run. `<id>` is the prefix of the single `*_design.md` in `.features/designs_active/`.

## Always
- Use the `python` command forms from 00-environment.
- Send anything outside the current request, task or bug to `python tools/workflow/fd.py backlog add <item>`, then carry on.
- Never pass `--force` to `fd.py`. Never edit `tools/workflow/fd.py`, `.features/descriptions/` or `<id>_feedback.md`.
- Never tick or untick an Acceptance Criteria box by hand; `fd.py uat import --apply` does it.
- Never write log text beginning with a task id followed by `start`, `done` or `forced`; `fd.py` reads that as evidence.
- Source comments describe WHAT and HOW only: no task ids, bug ids or change history.
- Write no tests, snapshots or scratch scripts unless the workflow authorizes a test. Never run `tools/verify.py --scope all`.
- Do not create plan, task-list or walkthrough artifacts; the design file and `fd.py` are the record.
- Never commit, and never start UAT or another workflow's work.

## Chat is not a record
A decision counts only when written on the `Answer:` line of an OQ in the design file. If the human answers in chat, do not act; reply "Write it in the Answer line of OQ-n in the design file."

## Standard gate (every workflow except /feature-design)
1. Branch is `feature-test` and `designs_active/` holds exactly one `*_design.md`. Otherwise stop and tell the human.
2. `fd.py oq <id> --open` lists nothing. Otherwise stop and tell the human to answer in the design file and run /feature-design.
3. `fd.py hot <id> --brief` shows the status the workflow requires.

## Blocking question
Add `OQ-n` at the end of `## Open Questions` in template format: a closed Question ending `Default: <recommendation>`, a Rationale (what would change), `Status: open`, `Answer: (to be provided)`, `Decision: (to be recorded)`. Never reopen or resolve an OQ yourself.

## Report
At most 8 lines: what you did, what is left or blocked, the exact next step for the human.
