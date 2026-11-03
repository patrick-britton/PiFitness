# Git Model

## Trunk flow (D1, D2)

`main` is the deployable trunk. Agents work on `agent/<task-id>-<slug>` branches cut from `main`, commit locally, and never push, merge, tag, or touch `main` (D2). The human reviews `git diff main...HEAD`, optionally runs the branch on the Pi with `test-up` (D3), squash-merges, pushes, tags `prd-YYYYMMDD`, and runs bootstrap. One task branch at a time; the human merges before the next starts.

PRD is whatever commit the Pi runs, recorded by a `prd-YYYYMMDD` tag; it can lag `main` (D1).

## Tag naming

- `prd-YYYYMMDD`: current PRD at deploy time.
- `pre-trunk-*`: historical pre-restructure markers (`pre-trunk-main`, `pre-trunk-origin-react-ui`, `pre-trunk-reorg`).
- `failsafe-react-ui`: frozen react-ui baseline, tagged at the last deployed commit.
- `archive/*`: retired branch tips (`archive/reorg`, `archive/streamlit-prd`).

## react-ui frozen as production failsafe (D19)

Until a `main`-based deploy succeeds on the Pi and `test-up` (Phase 5.5) exists, `origin/react-ui` stays at the last deployed commit and is not fast-forwarded to `main`. Bootstrap and `deploy_react.sh` still fetch it, so an accidental bootstrap is a no-op redeploy of the current PRD, not a jump to untested code. To deploy a `main`-based release before 5.1, the human first runs `git push origin main:react-ui`; from 5.1 onward bootstrap fetches the ref being deployed (D16) and the freeze ends naturally.

## Rollback

Rollback redeploys a `prd-` tag: `git checkout prd-YYYYMMDD` (or `git reset --hard <tag>`) then redeploy; the deploy uses the scripts that tag shipped with (D16).

## Test flow (D3)

`test-up` swaps the Pi into test mode against a copy of the PRD database while PRD is stopped and untouched. `test-down` stops the test units, drops the test DB, and restarts PRD from its untouched directory; it is idempotent and safe after a half-finished `test-up`. Merge to `main` and deploy to PRD only after `test-down` succeeds.

## Script source (D16)

From Phase 5.1 onward, bootstrap fetches the ref being deployed and runs that ref's `deploy/scripts/deploy.sh`. Before 5.1, bootstrap fetches `react-ui`. The Pi keeps a stub of about 30 lines that fetches the ref and runs `deploy/scripts/deploy.sh` from it; `deployment_script` is deleted only after the stub is live (5.1).