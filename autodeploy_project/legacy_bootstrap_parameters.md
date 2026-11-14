# PiFitness Bootstrap Parameter Index 

Legend: **T** = target objective, **L** = legacy script, **C** = candidate config/default.

## 1. Environments, branches, commits, launcher

| Item | Value / convention | Source |
|---|---|---|
| Environment names | `test`, `prd` | T, L |
| CLI args | `--environment test|prd`, `--commit <40-char SHA>`, `--through <n>` | T |
| TEST branch | `feature-test` | T, L |
| PRD branch | `main` | T |
| Legacy deploy target branch | `react-ui` | L `deploy.sh` — conflict with target |
| Commit validation | 40 hex chars | T, L |
| Launcher dir | `/home/god/pifitness-bootstrap` | T, L |
| Launcher script | `/home/god/pifitness-bootstrap/bootstrap.sh` | T, L |
| Source repo | `/home/god/PiFitness` | L |
| Git remote | `https://github.com/patrick-britton/PiFitness.git` | L `deploy.sh` |
| Launcher fetch ref | `refs/heads/$BRANCH:refs/remotes/origin/$BRANCH` | L |
| Launcher commit resolution | `refs/remotes/origin/$BRANCH^{commit}` | L |
| Launcher temp dir | `$BOOTSTRAP_DIR/bootstrap.XXXXXX` | L |
| Launcher extract path | `git archive "$COMMIT" deploy/scripts` → temp dir | T, L |
| Target deploy script | `deploy/scripts/autodeploy.sh` | T |
| Legacy deploy script | `deploy/scripts/bootstrap.sh` | L |
| Helper dir | `deploy/scripts/lib/` | T |
| Legacy helper | `deploy/scripts/lib/ensure_binary.sh` | L |
| New log dir | `/home/god/pifitness-logs` | T |
| New log filename | `autodeploy-<env>-<UTC timestamp>-<short SHA>.log` | T |
| Exit codes | `0` success; non-zero failure / NO-GO / timeout | T |
| Legacy exit codes | `64` usage error, `2` PRD not implemented | L |
| Public URL | `https://pifitness.duckdns.org` | T, L |

## 2. Filesystem paths

| Path / file | Purpose | Source |
|---|---|---|
| `/home/god/PiFitness` | PRD app dir / source repo | L |
| `/home/god/PiFitness-test` | TEST app dir | L |
| `/home/god/.config/pifitness/test.env` | TEST Pi env file | T, L |
| `/home/god/.config/pifitness/prd.env` | PRD Pi env file — **implied** by T `<env>.env`; legacy uses `backend/.env` | T / L conflict |
| `/home/god/.local/share/pifitness/test-venv` | Persistent TEST venv | L |
| `/home/god/PiFitness/venv` | Legacy PRD venv | L `deploy.sh` |
| `$TEST_DIR/venv` | Legacy TEST checkout venv in `test-up.sh` | L |
| `/home/god/Documents/.env` | Legacy master env file | L |
| `/home/god/Documents/.env.pre-nuclear` | Legacy nuclear env backup | L |
| `/home/god/Documents/.env.pre-nuclear.master` | Legacy nuclear master env backup | L |
| `/home/god/Documents/PiFitness_Test_Evidence` | Legacy evidence dir | L |
| `/home/god/Documents/PiFitness_Test_Evidence/journals` | Legacy journal dir | L |
| `/home/god/Documents/DB_Backups` | Legacy PRD backup dir | L |
| `$HOME/.pifitness-test-state` | Legacy durable test state file | L |
| `/etc/systemd/system/` | Installed systemd units | L |
| `/etc/nginx/sites-available/pifitness` | Nginx site | L |
| `/etc/nginx/sites-enabled/pifitness` | Nginx enabled symlink | L |
| `deploy/nginx/nginx-react.conf` | Nginx template | L |
| `deploy/systemd/` | Repo systemd unit source dir | T, L |
| `deploy/scripts/autodeploy.conf` | Target config file in git | T |
| `deploy/tests.conf` | Test command config | T |
| `deploy/scripts/library_requirements.txt` | Legacy Python requirements | L `deploy.sh` |
| `deploy/scripts/npm_requirements.txt` | Legacy npm requirements | L `deploy.sh` |
| `deploy/scripts/ecosystem.config.js` | Legacy PM2 ecosystem for TEST | L `test-up.sh` |
| `backend/pyproject.toml` | Python project config | T, L |
| `backend/uv.lock` | Python lockfile | T, L |
| `backend/requirements.txt` | Legacy Python requirements | L `test-up.sh` |
| `frontend/pifitness/` | Frontend dir | L |
| `frontend/pifitness/package.json` | Frontend package manifest | L |
| `frontend/pifitness/package-lock.json` | Frontend lockfile | L |
| `tools/db/restore_test_db.py` | Legacy DB recovery helper | L |
| `tools/db/inspect_test_db.py` | Legacy DB inspection helper | L |
| `$APP_DIR.staging.$$` | Legacy staging dir | L |
| `$APP_DIR.previous.$$` | Legacy previous checkout dir | L |
| `$STAGE_DIR/backend/.env` | Legacy staged env symlink | L |
| `$STAGE_DIR/venv` | Legacy staged venv symlink | L |
| `$TEST_DIR/.env.test` | Legacy test env copy inside checkout | L |
| `$TEST_DIR/frontend/pifitness/node_modules` | Legacy TEST node modules | L |
| `$TEST_DIR/frontend/pifitness/.next` | Legacy TEST Next build | L |
| `/tmp/${token_file}.backup` | Legacy token backups | L `deploy.sh` |
| `/tmp/.env.backup`, `/tmp/.env.root.backup` | Legacy env backups | L `deploy.sh` |
| `/home/god/PiFitness/backups` | Legacy backup cleanup dir | L `deploy.sh` |

## 3. Systemd, PM2, services, ports, URLs

| Item | Value / convention | Source |
|---|---|---|
| Systemd unit source pattern | `deploy/systemd/pifitness-*.service`, `deploy/systemd/pifitness-*.timer` | L |
| TEST unit detection | filename contains `-test`, e.g. `pifitness-*-test.service`, `pifitness-*-test.timer` | T, L |
| PRD unit detection | `pifitness-*` **without** `-test` | L |
| Unit install name | `basename "$unit_file"` copied to `/etc/systemd/system/` | L |
| Target config keys | `TEST_SERVICES`, `PRD_SERVICES` and timer equivalents | T |
| PRD PM2 process | `pifitness-next` | L |
| TEST PM2 process | `pifitness-next-test` | L |
| PM2 startup unit | `pm2-god.service` | L |
| PM2 dump file | `~/.pm2/dump.pm2` | L |
| FastAPI port | `8000` | L |
| Next.js port | `3000` | L |
| Next.js bind host | `127.0.0.1` | L |
| Nginx `server_name` | `pifitness.duckdns.org` | L |
| Nginx proxy target port | `8000` | L `deploy.sh` |
| Legacy health URL | `http://127.0.0.1/api/health` | L `test-down.sh` |
| Legacy API health URL | `http://127.0.0.1:8000/api/health` | L `test-up.sh` |
| HTTPS health URL | `https://pifitness.duckdns.org` | L |
| Next public app env PRD | `NEXT_PUBLIC_APP_ENV=production` | L |
| Next public app env TEST | `NEXT_PUBLIC_APP_ENV=test` | L |
| Next public API URL | unset | L |

## 4. Database, migrations, backups, markers

| Item | Value / convention | Source |
|---|---|---|
| PRD DB name | `pifitness` | L |
| TEST DB name | `pifitness_test` | L |
| DB user default | `dev` | L `test-up.sh`, `test-down.sh` |
| DB host default | `localhost` | L |
| DB port default | `15432` | L `test-up.sh`, `test-down.sh` |
| Legacy fresh-dump PG defaults | `PG_HOST=localhost`, `PG_PORT=5432`, `PG_USER=postgres`, `PG_DB=pifitness` | L `bootstrap.sh` — port conflict |
| TEST DB marker setting | `pifitness.env = 'test'` | L |
| Marker query | `SELECT current_setting('pifitness.env', true);` | L |
| Target TEST DB state values | `clean`, `dirty`, `migrated` | T |
| Target state file location | outside app dir; exact filename unspecified | T |
| Target clean baseline | persistent local copy of TEST DB cloned from PRD; created only on clone/restore | T |
| PRD nightly backup dir default | `$HOME/Documents/DB_Backups` | L |
| PRD nightly backup glob | `personal_fitness_[0-9]*.dump` | L |
| Backup timestamp regex | `_([0-9]{8}_[0-9]{6})\.dump$` | L |
| Legacy fresh PRD dump name | `personal_fitness_YYYYmmdd_HHMMSS.dump` | L |
| Legacy test-up PRD dump name | `pifitness_prd_YYYYmmdd_HHMMSS.dump` | L `test-up.sh` |
| Max backup age for restore | `24` hours | L |
| Migration tool | `dbmate` | T, L |
| Migration commands | `dbmate up`, `dbmate status` | T |
| Migrations directory | from config; **exact path unspecified in target** | T |
| DB comparison tooling | dbmate applied-migrations table + schema-only dump comparison; legacy MigraDiff | T, L |
| TEST DB creation legacy | `CREATE DATABASE "$TEST_DB" TEMPLATE "$PRD_DB";` fallback `pg_dump`/`pg_restore` | L `test-up.sh` |
| Legacy DB recovery helper args | `--test-env-file`, `--prd-env-file`, `--state-dir`, `--backup-file`, `--backup-dir`, `--backup-glob`, `--max-backup-age-hours`, `--state-file`, `--diff-file` | L |
| Legacy DB inspection args | `--test-env-file`, `--prd-env-file`, `--allow-prd`, `--diff-file`, `--baseline-state-dir` | L |

## 5. Tools, binaries, versions

| Tool / item | Value | Source |
|---|---|---|
| `uv` | `/home/god/.local/bin/uv` | L |
| Python | `/usr/bin/python3` | L |
| Node | `/usr/bin/node` | L |
| npm | `/usr/bin/npm` | L |
| dbmate | `/usr/local/bin/dbmate` | L |
| dbmate version | `2.36.0` | L |
| dbmate install URL | `https://github.com/amacneil/dbmate/releases/download/v2.36.0/dbmate-linux-arm64` | L |
| uv installer URL | `https://astral.sh/uv/install.sh` | L |
| Node version legacy | `v20.10.0` | L |
| Node install URL legacy | `https://nodejs.org/dist/v20.10.0/node-v20.10.0-linux-arm64.tar.xz` | L |
| MigraDiff version | `1.7.2` | L |
| Playwright | `python -m playwright install chromium` | L |
| Python sync target | `uv sync --locked` with `UV_PROJECT_ENVIRONMENT` | T |
| Legacy uv sync flags | `--locked --no-dev --group deployment --no-editable --python /usr/bin/python3` | L |
| Node install target | `npm ci` | T, L |
| Node build | `npm run build` | L |
| Python tests | `pytest` | T |
| Frontend tests | npm test script | T |
| Smoke tests | from `deploy/tests.conf` | T |

## 6. Naming conventions and generated filenames

| Item | Convention | Source |
|---|---|---|
| Config env prefix | `TEST_`, `PRD_` | T |
| New log | `autodeploy-<env>-<UTC timestamp>-<short SHA>.log` | T |
| Legacy timestamp | `date +%Y%m%d_%H%M%S` | L |
| Legacy RUN_ID | `YYYYmmdd_HHMMSS_${COMMIT:0:12}_$$` | L |
| Legacy evidence audit | `dependency-audit-YYYYmmdd_HHMMSS.json` | L |
| Legacy recovery reconcile | `test-db-recovery-reconcile-YYYYmmdd_HHMMSS_$$.json` | L |
| Legacy inspection | `test-db-inspection-${RUN_ID}.json` | L |
| Legacy schema diff | `test-prd-schema-diff-${RUN_ID}.json` | L |
| Legacy recovery report | `test-db-recovery-report-${RUN_ID}.json` | L |
| Legacy post-recovery inspection | `test-db-inspection-after-recovery-${RUN_ID}.json` | L |
| Legacy error sidecar | same basename with `.stderr` | L |
| Temp launcher dir | `bootstrap.XXXXXX` | L |
| Temp staging dir | `$APP_DIR.staging.$$` | L |
| Temp previous dir | `$APP_DIR.previous.$$` | L |
| Backup filename | `personal_fitness_YYYYmmdd_HHMMSS.dump` | L |
| Legacy test-up dump | `pifitness_prd_YYYYmmdd_HHMMSS.dump` | L |
| Systemd TEST marker | substring `-test` in unit filename | L |
| Systemd PRD marker | absence of `-test` | L |
| Failpoint env var | `PIFITNESS_FAILPOINT_${name^^}=1` | L |
| Failpoint ack | `PIFITNESS_FAILPOINT_ACK=test-only` | L |
| Legacy failpoints | `during_restore`, `pre_start` | L |
| Legacy DB prompt | `[K] Keep current TEST database   [R] Restore most recent nightly snapshot   [C] Take a fresh clone` | T |
| Legacy numeric DB prompt | `Enter choice [1-3]` | L |
| GO prompt | `Enter GO or NO-GO` | T |
| Legacy launcher menu | `1) PRD`, `2) TEST`, `Q) Quit` | L |

## 7. Environment variables and config keys seen

| Name | Meaning |
|---|---|
| `UV_PROJECT_ENVIRONMENT` | Persistent venv path for `uv sync` |
| `PIFITNESS_PRD_BACKUP_DIR` | Override PRD backup dir |
| `PIFITNESS_PRD_BACKUP_GLOB` | Override PRD backup glob |
| `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_DB`, `PGPASSWORD` | DB connection vars |
| `APP_ENV` | Must be `test` in TEST env |
| `SPOTIFY_ENABLED`, `GARMIN_ENABLED`, `PIRATE_ENABLED` | Must be `false` in TEST env |
| `NEXT_PUBLIC_APP_ENV` | `production` or `test` |
| `NEXT_PUBLIC_API_URL` | Unset in legacy |
| `PRD_DIR`, `TEST_DIR`, `STATE_FILE` | Legacy test-up/down paths |
| `PRD_DB`, `TEST_DB`, `DB_USER`, `DB_HOST`, `DB_PORT` | Legacy DB defaults |
| `HEALTH_URL`, `HTTPS_HOSTNAME`, `BACKUP_DIR` | Legacy health/backup vars |
| `TEST_SERVICES`, `PRD_SERVICES` | Target service lists |
| Test commands, migrations dir, health-check URL, persistent resources, dependency groups, required tools | Target `autodeploy.conf` keys — exact key names not yet specified |