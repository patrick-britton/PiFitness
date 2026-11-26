---
feature-id: fix-ensure-all-activities-process
title: Ensure All Activities Process
category: Bug Fix
status: draft
created: 2026-11-26
last-updated: 2026-11-26
surfaces: page:/activities/recent-activity, endpoint:POST /api/activities/{activity_id}/postprocess, view:activities.vw_activity_ids_to_sync, table:activities.activity_processing_queue, py:backend/pifitness/activities/smoothing.py::activity_post_processing
points:
request: .features/descriptions/fix-ensure-all-activities-process.md
---

# Design: Ensure All Activities Process

## Resume Here
**progress:** 0/0 tasks complete
**next:** review
**blocked-by:** none

## Architecture

### Surfaces
- `page:/activities/recent-activity`: frontend/pifitness/src/app/activities/recent-activity/page.tsx (`RecentActivityPage`); pattern: `frontend/pifitness/src/app/activities/recent-activity/page.tsx`; touches header action bar to add a postprocessing button; gotcha: button must handle loading/disabled state and notify user.
- `endpoint:POST /api/activities/{activity_id}/postprocess` (new): backend/pifitness/activities/router.py (`postprocess_activity_endpoint(activity_id: int, background_tasks: BackgroundTasks)`); pattern: `POST /api/segments/{segment_id}/matches/find`; gotcha: enqueue background postprocessing and return HTTP 202 accepted without blocking the client.
- `view:activities.vw_activity_ids_to_sync`: PostgreSQL database view queried by `backend/pifitness/worker/json_extractors.py::gen_activity_list`; pattern: `lineage/defs/view.activities.vw_activity_ids_to_sync.sql`; bug: previously ordered by run and included newest run without excluding it from auto-download; must exclude the newest run/trail run so only prior activities and walks/hikes are auto-downloaded.
- `table:activities.activity_processing_queue`: PostgreSQL database table queried by `backend/pifitness/activities/smoothing.py::activity_post_processing`; bug: activity 25165738610 had downloaded details but remained queued and never post-processed because task 21 had not executed on it and had no single-activity trigger.
- `py:backend/pifitness/activities/smoothing.py::activity_post_processing`: backend/pifitness/activities/smoothing.py (`activity_post_processing(manual_list=None)`); pattern: `backend/pifitness/activities/smoothing.py`; gotcha: runs `activity_post_processing_steps` which updates `activity_path`, sets `is_downloaded = TRUE`, matches courses, and clears the queue row.

### Principles Alignment
- Backend: `docs/ARCHITECTURE.md` — background tasks are decoupled from sync HTTP responses; errors logged with context.
- Frontend: `docs/UI_PRINCIPLES.md` — responsive layout checked at 360x740 and 448x875; clear visual feedback for action states.
- Database: `docs/DATABASE_PRINCIPLES.md` — additive view adjustments preserving existing table schemas.
- Integrations: `docs/INTEGRATION_PRINCIPLES.md` — idempotent task executions and safe queue draining.

### Approach
- Update `activities.vw_activity_ids_to_sync` to exclude the single newest run/trail-run activity (> 1000m) while selecting all prior runs/trail runs and walks/hikes (> 1000m).
- Add endpoint `POST /api/activities/{activity_id}/postprocess` to enqueue or trigger `activity_post_processing([activity_id])` in background and return immediate status.
- Add a postprocessing icon button to `RecentActivityPage` next to the navigation bar/title to trigger background postprocessing for the active activity with user feedback.
- Ensure task 21 ("Activity Detail Post Processing") processes queued activities without hanging on the newest run.

### Data and Migrations
- Additive view definition update: alter `activities.vw_activity_ids_to_sync` to filter out `activity_id = (SELECT max(activity_id) FROM activities.activities WHERE (activity_type_name LIKE '%running%' OR activity_type_name LIKE '%trail%') AND distance_m > 1000)`.

### API Changes
- New endpoint `POST /api/activities/{activity_id}/postprocess` returning `{"status": "queued", "activity_id": <int>}`.

## Acceptance Criteria
- [ ] AC-1: Open Recent Activity Report and inspect activity 25165738610 -> activity 25165738610 was missing from report and unlinked to 'Beeler Summit 4.9' (covers B1)
  - auto: `none (Bug reproduction step for human UAT)`
- [ ] AC-2: Navigate to any activity on the Recent Activity Report page and click the postprocess button -> button initiates background postprocessing with confirmation feedback (covers B3)
  - auto: `npx tsc --noEmit`
- [ ] AC-3: Run postprocessing for activity 25165738610 -> activity 25165738610 has course matching completed to 'Beeler Summit 4.9' and appears in Recent Activity Report (covers B1)
  - auto: `python -m py_compile backend/pifitness/activities/router.py backend/pifitness/activities/smoothing.py backend/pifitness/worker/json_extractors.py`
- [ ] AC-4: Verify background sync views for a newly recorded run when older runs exist -> older runs and walks/hikes > 1000m are eligible for auto-download and auto-postprocessing while the newest run is withheld (covers B1, B2)
  - auto: `python -m py_compile backend/pifitness/activities/router.py backend/pifitness/activities/smoothing.py backend/pifitness/worker/json_extractors.py`

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** In my words: All qualifying prior activities (walk, hike, run, trail run > 1000m) are automatically processed by background task execution (details downloaded and course matching attempted), except for the single most recent run/trail run which is reserved for manual processing (to capture playlist/music). If a newer run appears, prior runs are auto-processed. Furthermore, the Recent Activity Report page displays a button to initiate background postprocessing for the currently viewed activity. I am assuming: (1) "Most recent run / trail run" is defined as the single newest activity with activity type containing 'run' or 'trail_running' and distance > 1000m; (2) Walking and hiking activities (> 1000m) are never excluded from auto-processing by Task 19/21 since they do not capture Spotify playlists; (3) The postprocess button on Recent Activity Report initiates background postprocessing for that specific activity via a new endpoint (`POST /api/activities/{activity_id}/postprocess`). Is this right? Default: yes, this is right.
  - **Answer:** yes, this is right.
  - **Decision:** Scope confirmed: auto-process prior qualifying activities (>1000m) while withholding newest run/trail run for manual processing, and add background postprocessing button on Recent Activity Report.
  - **Rationale:** If the scope or definition of most recent run differs, the view filter criteria and postprocessing triggers change.
  - **Status:** resolved

- **OQ-2** Button design and interaction on Recent Activity Report
  - **Question:** Where should the background postprocessing button be placed on the Recent Activity Report page, and what feedback should be shown? Should it be an icon button placed in the top header next to the navigation controls that displays a loading spinner and success toast when clicked, or a labeled button in the footer? Default: An icon button in the header bar next to navigation controls with tooltip 'Reprocess activity' and a temporary confirmation badge/toast.
  - **Answer:** yes, default is correct.
  - **Decision:** Add icon button in header bar next to navigation controls on Recent Activity Report with tooltip 'Reprocess activity' and temporary confirmation badge/toast.
  - **Rationale:** Affects UI placement and component styling in `RecentActivityPage`.
  - **Status:** resolved

- **OQ-3** Auto-processing exclusion mechanism for newest run
  - **Question:** Should the exclusion of the newest run/trail run be implemented directly in the database view `activities.vw_activity_ids_to_sync` (so Task 19 automatically leaves it untouched until a newer run is recorded), and similarly checked in Task 21 postprocessing? Default: yes, implement the exclusion in `activities.vw_activity_ids_to_sync` and Task 21 queue processing.
  - **Answer:** yes, implement the exclusion in the view
  - **Decision:** Implement exclusion of the newest run/trail run directly in `activities.vw_activity_ids_to_sync` so Task 19 and Task 21 auto-process only prior runs/trail runs and walks/hikes.
  - **Rationale:** Decides whether the filtering logic lives in SQL views or application Python logic.
  - **Status:** resolved

- **OQ-4** Add invariant test for activity auto-processing eligibility
  - **Question:** Add test `tests/test_activity_processing_eligibility.py::test_newest_run_excluded_prior_runs_included` [invariant]? It guards the rule that Task 19/21 auto-process all prior qualifying activities while reserving the newest run for manual processing. Default: no.
  - **Answer:** No.
  - **Decision:** Do not add new test; compile and type-check checks remain.
  - **Rationale:** If yes, an automated invariant test will be created during implementation; if no, compile/type-checks remain.
  - **Status:** resolved

## Task Plan
Filled by /feature-plan.
