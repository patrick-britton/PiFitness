# Pi Post-Deploy Checklist

Run once after a full deploy on the Pi. Check each item; stop and investigate
on any failure before declaring the deploy complete.

## 1. Service health

- [ ] `sudo systemctl is-active pifitness-fastapi.service` -> `active`
- [ ] `sudo systemctl is-active pifitness-hardware-health.timer` -> `active`
- [ ] `sudo systemctl is-active pifitness_agent.timer` -> `active`
- [ ] `pm2 show pifitness-next` reports `status: online`
- [ ] `sudo systemctl is-active nginx` -> `active`

## 2. API health

- [ ] `curl -s http://localhost:8000/api/health` returns `{"status":"ok"}` (or equivalent)
- [ ] `curl -sI https://pifitness.duckdns.org/api/health` returns `200 OK`
- [ ] HTTPS certificate valid (Let's Encrypt, issuer YE2); expires 2026-12-31

## 3. Build-time env vars

- [ ] `grep -q "NEXT_PUBLIC_APP_ENV=production" /home/god/PiFitness/frontend/pifitness/.env.production 2>/dev/null || true`
- [ ] Frontend bundle contains production API base (no localhost references in `.next/static`)

## 4. Timers firing

- [ ] `sudo systemctl list-timers --no-pager | grep pifitness` shows next trigger within expected window
- [ ] `journalctl -u pifitness_agent.service -n 5 --no-pager` shows a recent successful run
- [ ] `journalctl -u pifitness-hardware-health.service -n 5 --no-pager` shows a recent successful run

## 5. Two heavy queries (performance sanity)

- [ ] Dashboard load (`/api/health` + main dashboard endpoint) returns within 2s
- [ ] Activity list endpoint returns within 2s for a full month range

## 6. One feature spot check

- [ ] Log in via the deployed UI, open one feature page (e.g. Activities), confirm data renders and no console errors.

## Rollback

If any item fails:

1. Stop services: `sudo systemctl stop pifitness-fastapi.service; pm2 stop pifitness-next`
2. Restore previous build from the last known-good git ref (recorded in `deploy/scripts/CHANGES_SUMMARY.md`)
3. Re-run `bootstrap.sh --dry-run` first, then without `--dry-run` against the previous ref
4. Re-verify this checklist before announcing completion
