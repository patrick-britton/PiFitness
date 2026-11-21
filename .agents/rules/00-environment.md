---
trigger: always_on
---

# PiFitness Environment

- Windows PC, Windows PowerShell 5.1. Repo: `C:\users\rickb\vsprojects\pifitness`. Backend: FastAPI in `backend/`. Frontend: Next.js in `frontend/pifitness`.
- DEV database: Docker `pifitness-dev-db` at `localhost:15432`, db `pifitness_dev`, user `dev`. Settings are in `backend/.env`: read only the one non-secret value you need, never print secrets.
- Python is always `C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe` (PY). Workflow commands that start with `python` (`fd.py`, `lineage.py`, `lint_comments.py`, `check_docs.py`, `tools/verify.py`) run with it: `& 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe' tools/workflow/fd.py hot <id> --brief`. Below, `& PY` means that call.
- PowerShell 5.1: chain with `;` (no `&&`/`||`), read `$LASTEXITCODE` right after a native command, run `.ps1` files with `powershell -NoProfile -ExecutionPolicy Bypass -File`.
- Backend tests: `& PY -m pytest tests/ -m "not slow and not integration and not live" -q`. Frontend type check (cwd `frontend\pifitness`): `npx tsc --noEmit`. DEV DB reset: `& PY tools/db/dev_reset.py`. Lineage: `& PY tools/data_dictionary/lineage.py describe|search|trace <target>`.
- Known issues: `npm run test` (Vitest) fails to start (Vite configLoader). pytest can exit 1 after passing because of a Windows temp-teardown `PermissionError`; a non-zero exit is still a failure, report the teardown separately.
- Principle docs in `docs/`; follow the one for the surface you touch: `ARCHITECTURE.md` (backend), `PI_SPECS.md` (deployment), `UI_PRINCIPLES.md` (frontend), `DATABASE_PRINCIPLES.md` (database), `INTEGRATION_PRINCIPLES.md` (integrations, workers).
