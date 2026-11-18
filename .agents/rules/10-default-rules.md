---
trigger: always_on
---

# Global Rules

## Git
- Work only on branch `feature-test` (`git branch --show-current`; if different, stop and tell the human). Never touch `main`.
- Git is read-only: `status`, `diff`, `log`, `show`. Never stage or commit; the human commits. On an index-lock error, do not delete the lock; tell the human.

## Production and secrets
- DEV database only: any connection must target `localhost`, and before a write confirm the effective target is local DEV (not just `.env`). Never connect to PRD or `pfpi`.
- Never run `tools/db/sync-prod-to-dev.ps1`, SSH/SCP to the Pi or a production host, or run deployment, migration, restore or destructive commands against production. A task naming a deployment script authorizes editing it only. Pi-side checks are human gates.
- Never expose secrets from env files, credentials or tokens.

## Working
- Do not create or change repo source via shell (`Set-Content`, redirection, Python one-liners); use the editing tools.
- Never weaken code, skip a required check, or change a test expectation to make a failure go away. A timeout is not a pass. Report something as passed only with evidence.
- Do not retry an identical failed command. If the same failure happens twice with no new information, stop and report it as blocked.
- For how-built / where-from / what-uses questions about data flow, run `lineage.py describe <target>` first and use its reported locations before searching.
