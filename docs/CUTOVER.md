# CUTOVER — `deployment/` → `deploy/` Migration

## Overview

Moved all deployment orchestration, systemd units, nginx configs, and helper scripts from `deployment/` (legacy) to `deploy/` (consolidated). The `deployment/` directory is now a historical snapshot only.

## What Was Moved

### `deploy/scripts/` (formerly `deployment/*`)
- `bootstrap.sh` → bootstrap + unit orchestration
- `deploy_react.sh` → React/Nginx deployment
- `pi5_deploy.sh` → Pi5 unified script
- `library_requirements.txt`, `npm_requirements.txt`, `requirements-react.txt` — dependency manifests
- `pathfinder.py` — deployment helper
- `pifitness-hw-health.py` — hardware health probe
- `list-sql.ps1` (previously `deploy/list-sql.ps1`)

### `deploy/systemd/` (formerly `deployment/{agent,health}/*`)
- `pifitness-fastapi.service` — main FastAPI service
- `pifitness-hw-health.service` / `.timer` — hardware health helper
- `pifitness_agent.service` / `.timer` — background task runner

### `deploy/nginx/` (formerly `deployment/nginx-*.conf`)
- `nginx-react.conf` — React routing template
- `nginx-template.conf` — base nginx template

## Reference Updates

- Shell scripts: `deployment/` → `deploy/` (scripts, systemd, nginx paths)
- Docs: `deployment/` → `deploy/` in `frontend/DEPLOYMENT_GUIDE.md`, `docs/reorg/DECISIONS.md`, `deploy/scripts/CHANGES_SUMMARY.md`
- Audit config: `deployment/**/*.py` → `deploy/**/*.py` (audit.toml)
- Branch name: `deployment_script` → `deploy_merge`

## Verification

- 210 tests pass: ✅ `python -m pytest tests/ -x -q`
- Audit gate clean: ✅ `python scripts/repo_audit/core.py` (0 path references to `deployment/` remain)
- Shell script syntax: bash `-n` (where available) — see commit verification
