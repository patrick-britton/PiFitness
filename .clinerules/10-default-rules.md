# Global Rules

These apply to all repository work. A workflow may narrow them but never relax them.

## 1. Git

- Work only on the `feature-test` branch. Run `git branch --show-current` first in every workflow. If it does not print `feature-test`, stop and tell the human.
- Git is read-only for you. The only allowed commands are:
  - `git branch --show-current`
  - `git status`
  - `git diff`
  - `git log`
  - `git show`
- Never stage or commit. Every other git command is off limits, including `add`, `commit`, `restore`, `reset`, `clean`, `stash`, `checkout`, `switch`, `branch` (other than `--show-current`), `merge`, `rebase`, `tag`, `push`, `pull`, `rm`, `mv`, `grep`, and `ls-files`.
- Never touch `main`. The human reviews the working tree and commits.

## 2. Deployment, database, secrets

- Follow `.clinerules/00-environment.md`: DEV database only, never PRD or `pfpi`, never run `tools/db/sync-prod-to-dev.ps1`, never SSH to the Pi.
- Never run deployment or migration operations against production.
- Edit a deployment script only when a task anchors it. That never authorizes running it against a live system.
- Never expose secrets from env files, credentials, or tokens in output, logs, or fixtures. Do not print a whole env file to confirm one value.

## 3. Tools and environment

- Tool use, searching, reading, error recovery, and file writes: `.clinerules/00-tool-constraints.md`.
- PowerShell and slow commands: `.clinerules/01-terminal-use.md`.
- Command forms and the Python interpreter: `.clinerules/00-environment.md`. Commands that start with `python` run with the interpreter path given there.
- Do not guess tool names, paths, shell dialect, exit codes, or test results. Use evidence, or ask one direct question when an essential decision cannot be settled safely.
- Do not ask the human for file paths or code locations the repository can tell you.

## 4. Lineage first

For a `/lineage` request, or any how-built, where-from, what-writes, what-uses, or feature-impact question about database, ETL, API, or UI data flow:

1. The first tool action is `skills(lineage)`.
2. Run its prescribed compact `lineage.py describe` command next. Call no other tool first.
3. Use the exact locations and `evidence` command it reports. Search only for a specific unresolved node or a coverage gap the report names.
4. Do not run repository-wide searches to rediscover relationships the graph already gave you.
5. Run lineage commands one per call, with no pipes, redirects, or `2>&1`. Bound output with the CLI's own options.

Inside a `/feature-*` workflow, run the `lineage.py` command the workflow names directly; the skill step is not needed.

## 5. Communication

- Be concise and factual. No flattery, no repeated restating of the task, no closing essay.
- Give a brief plan when the task has several steps. Report findings when they become actionable, then move on.
- Do not reopen a settled decision unless new evidence changes it.
- Separate observed facts, assumptions, unverified requirements, and manual gates. Report a test, deployment, or criterion as passed only when its evidence exists.
