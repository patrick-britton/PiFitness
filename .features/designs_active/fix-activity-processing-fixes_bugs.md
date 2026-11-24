# Known Issues / Bugs - fix-activity-processing-fixes


### BUG-1 — Syncing activities fails with Client error
- **AC:** AC-1
- **Symptom:** 'Syncing Activities' still fails -- but now with a 'Activities: Client error [...' -- but I can't read the remainder on the phone. I would note that this passed in the dev environment. In UAT re-test, user reported 'Activities: Garmin login failed -- check credentials or Playwright browser installation.'
- **Root cause:** In headless/test environments (such as the Pi running against test database `personal_fitness_test`), `personal_fitness_test` DB has no pre-existing active token in `api_services.api_tokens` and no decrypted credentials in `api_services.credentials`. `garmin_login` aborted on `elif not email or not password` without inspecting whether a valid local session file (`native-oauth2.json`) existed in the host filesystem cache. Furthermore, `garmin_login` did not accept an incoming client dictionary passed from multi-step executioner tasks, triggering redundant re-logins, and `garmin_creds` did not inspect the `Pirate Garmin` service name or `username` key.
- **Fix:** In `garmin_login`, inspect local disk cache paths before aborting on missing DB tokens/credentials, accept incoming client dictionaries for inter-task client reuse, fall back to `username` and `Pirate Garmin` in `garmin_creds`, and save refreshed session back to the DB; files: `backend/pifitness/integrations/garmin/provider.py`, `tests/test_garmin_login_flow.py`
- **Status:** in-review
- **Since:** 2026-11-23T15:10:58

### BUG-2 — Progress bars do not update on mobile viewports while step executes
- **AC:** AC-3
- **Symptom:** Progress bars were NOT updating on mobile -- so that remains bugged as well.
- **Root cause:** FastAPI StreamingResponse omitted X-Accel-Buffering: no causing reverse proxy (Nginx on Pi) to buffer NDJSON chunks until step completion, withholding start/running events; additionally, StepChecklist did not clamp elapsedMs to zero against potential clock drift.
- **Fix:** Added X-Accel-Buffering: no and Cache-Control headers to /process StreamingResponse in router.py, clamped elapsedMs and barFraction to >= 0, added w-full to step-bar container, and anchored started_at to client time; files: backend/pifitness/activities/router.py, frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx, frontend/pifitness/src/app/activities/activity-processing/page.tsx
- **Status:** in-review
- **Since:** 2026-11-23T15:25:46
