# Known Issues / Bugs - fix-activity-processing-fixes


### BUG-1 — Syncing activities fails with Client error
- **AC:** AC-1
- **Symptom:** 'Syncing Activities' still fails -- but now with a 'Activities: Client error [...' -- but I can't read the remainder on the phone. I would note that this passed in the dev environment.
- **Root cause:** In provider auth, garmin_creds and spotify_creds did not handle missing credentials (NoneType), and garmin_login lacked a fallback for missing LOCAL_STORAGE_PATH, raising unhandled TypeError which surfaced as Client error; additionally, StepChecklist truncated error text with ellipsis inside a single-line paragraph on mobile viewports.
- **Fix:** Safely guard None credentials in garmin_creds and spotify_creds, fall back to default user directory when LOCAL_STORAGE_PATH is unset, wrap login setup in try-except, and render step errors as dedicated break-words block to prevent mobile truncation; files: backend/pifitness/integrations/garmin/provider.py, backend/pifitness/integrations/spotify/provider.py, frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx
- **Status:** in-review
- **Since:** 2026-11-23T15:10:58
