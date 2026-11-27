# Known Issues / Bugs - fix-ensure-all-activities-process


### BUG-1 — Unprocessed activities missing from Recent Activity Report navigation
- **AC:** AC-1
- **Symptom:** I'm failing this in UAT. Expecation is that, on the recent page -- every activity from activities table (that is > 1000m) shows, whether is is processed or not, whether it is most recent or not. This way I can see that the activity has been recorded, and process it right there if it's missing. Currently only processed activities seem to show up -- and possibly not even then.
- **Root cause:** activities.f_report_header filtered out unprocessed activities via AND a.activity_path IS NOT NULL and restricted activity_type_name to run/walk/hike, preventing un-postprocessed activities > 1000m from appearing in Recent Activity Report; also _run_postprocess_task needed to sync details if is_downloaded is false.
- **Fix:** Updated activities.f_report_header to include all activities with distance_m > 1000 regardless of activity_path or type, updated postprocess endpoint to sync details first if not yet downloaded, and added an unprocessed-activity banner with Process Now button on RecentActivityPage; files: deploy/migrations/20261127000000_f_report_header_all_activities_over_1000m.sql, docs/lineage/defs/function.activities.f_report_header.sql, backend/pifitness/activities/router.py, frontend/pifitness/src/app/activities/recent-activity/page.tsx
- **Status:** in-review
- **Since:** 2026-11-27T11:30:30
