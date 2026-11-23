# Known Issues / Bugs - fix-activity-processing-fixes


### BUG-1 — Syncing activities fails with Client error
- **AC:** AC-1
- **Symptom:** 'Syncing Activities' still fails -- but now with a 'Activities: Client error [...' -- but I can't read the remainder on the phone. I would note that this passed in the dev environment.
- **Root cause:** garmin_login checked credentials before checking DB token cache, aborting on headless/test hosts lacking raw credentials despite having a valid DB session in api_services.api_tokens.
- **Fix:** Reordered garmin_login to load and restore DB session cache first before requiring raw credentials; files: backend/pifitness/integrations/garmin/provider.py, backend/pifitness/integrations/spotify/provider.py, frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx
- **Status:** in-review
- **Since:** 2026-11-23T15:10:58

### BUG-2 — Progress bars do not update on mobile viewports while step executes
- **AC:** AC-3
- **Symptom:** Progress bars were NOT updating on mobile -- so that remains bugged as well.
- **Root cause:** FastAPI StreamingResponse omitted X-Accel-Buffering: no causing reverse proxy (Nginx on Pi) to buffer NDJSON chunks until step completion, withholding start/running events; additionally, StepChecklist did not clamp elapsedMs to zero against potential clock drift.
- **Fix:** Added X-Accel-Buffering: no and Cache-Control headers to /process StreamingResponse in router.py, clamped elapsedMs and barFraction to >= 0, added w-full to step-bar container, and anchored started_at to client time; files: backend/pifitness/activities/router.py, frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx, frontend/pifitness/src/app/activities/activity-processing/page.tsx
- **Status:** in-review
- **Since:** 2026-11-23T15:25:46
