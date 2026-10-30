# docs/reorg/blocks/

Block specs and reports for the integrations refactor.

## Convention

- `INT-N.md` - the spec for block N. Written by the reviewer, saved before execution.
- `INT-N-report.md` - the report produced by Cline when block N executes.

Both tracked in git. Do not delete. They are the audit trail.

## Execution

The human runs each block by invoking `/execute-block INT-N` (or "execute block INT-N"). The workflow loads the `block-executor` skill, reads `INT-N.md`, executes, writes `INT-N-report.md`, and stops. The reviewer verifies the report before the next block is issued.

Do not execute two blocks in one session. Do not skip the report. Do not chain.

## Discipline

Execution discipline (steps, hard rules, report format) lives in the `block-executor` skill at `.agents/skills/block-executor/SKILL.md`. That file is gitignored (local dev tooling); if the repo is wiped or cloned fresh, re-paste the skill from the session history where it was authored.

The skill is authoritative for how blocks execute. This README is authoritative for where block files live and what to name them.