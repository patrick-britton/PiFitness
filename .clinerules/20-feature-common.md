# Feature Workflow Common Rules

These apply to every `/feature-design`, `/feature-plan`, `/feature-execute`, `/feature-polish`, and `/feature-review` run. Each workflow file adds only what is specific to its phase. `<id>` is the feature id: the prefix of the single `*_design.md` in `.features/designs_active/`.

## Always

- Run commands that start with `python` using the interpreter in `.clinerules/00-environment.md`.
- Git rules: `.clinerules/10-default-rules.md` section 1 (feature-test branch, read-only).
- Read at most 200 lines of any file at a time. Change files with targeted replacements, never by rewriting a whole file to change part of it.
- Use only command forms from `.clinerules/00-environment.md`.
- Send anything outside the current request, task, or bug to `python tools/workflow/fd.py backlog add <item>`, then carry on.
- Never pass `--force` to any `fd.py` command.
- Never edit `tools/workflow/fd.py`, `.features/descriptions/`, or `<id>_feedback.md`.
- Never tick or untick an Acceptance Criteria box by hand. `fd.py uat import --apply` does that.
- Never write log text that begins with a task id followed by `start`, `done`, or `forced`. `fd.py` reads those as evidence.
- Source comments describe WHAT and HOW only. They never contain task ids, bug ids, or change history.
- Write no tests, snapshots, or scratch scripts except where the workflow says a test is authorized. Never run `tools/verify.py --scope all`; the autodeploy bootstrap runs the full suite.
- Never commit. Never start UAT or work that belongs to another workflow.

## Chat is not a record

A decision counts only when the human writes it on the `Answer:` line of an OQ in the design file. If the human gives an answer in chat, do not act on it. Reply: "Write it in the Answer line of OQ-n in the design file."

## Standard gate (every workflow except `/feature-design`)

1. `git branch --show-current` must print `feature-test`. Otherwise stop and tell the human to switch.
2. `.features/designs_active/` must hold exactly one `*_design.md`. Otherwise stop and tell the human.
3. `python tools/workflow/fd.py oq <id> --open` must list nothing. If it lists any open OQ, stop and tell the human to write answers in the design file and run /feature-design.
4. `python tools/workflow/fd.py hot <id> --brief` must show the status the workflow requires. The workflow says what to tell the human otherwise.

## Raising a blocking question

When work needs a decision the design does not settle, add a new `OQ-n` block at the end of `## Open Questions` in the template format:

- Question: closed (A or B, yes or no, or a concrete value), ending with `Default: <your recommendation>`.
- Rationale: what in the design, tasks, or bugs would change with the answer.
- `Status: open`, `Answer: (to be provided)`, `Decision: (to be recorded)`.

Never reopen a resolved OQ. Never resolve one yourself.

## Report

Reply with at most 8 lines: what you did, what is left or blocked, and the exact next step for the human.
