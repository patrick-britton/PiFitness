---
feature-id: fix-activity-processing-fixes
title: Activity Processing fixes
category: Bug Fix
status: in-progress
created: 2026-11-23
last-updated: 2026-11-23
surfaces: /activities/activity-processing, POST /api/activities/process, activities.activities, activities.activity_details, activities.activity_details_distance
points: 4
request: .features/descriptions/fix-activity-processing-fixes.md
---

# Design: Activity Processing fixes

## Resume Here
**progress:** 3/3 tasks complete
**next:** none
**blocked-by:** none

## Architecture

### Surfaces
- page:/activities/activity-processing: renders ActivityProcessingPage at frontend/pifitness/src/app/activities/activity-processing/page.tsx:59-339; manages form submission, NDJSON stream events, and failure finalization; pattern: self; gotcha: status bar animation must remain performant on mobile viewports.
- component:frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx::StepChecklist: renders step checklist and progress bars at frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx:44-188; touched: StepChecklist, estimateForStep, barFraction; pattern: self; gotcha: status bar progress fill omitted transition classes, leading to abrupt or un-animated bar progression on mobile screens.
- endpoint:POST /api/activities/process: handled by process_activity and process_generator at backend/pifitness/activities/router.py:448-464; touched: process_generator; pattern: backend/pifitness/activities/services.py; bug: run_step is invoked at lines 203, 213, 300, 334, 343 without being imported in router.py, causing NameError: name 'run_step' is not defined on step 1 (sync_activities).
- py:backend/pifitness/activities/services.py::run_step: execution wrapper at backend/pifitness/activities/services.py:42-80; touched: run_step(step_id: str, fn, *args, **kwargs) -> ProcessStepResult; pattern: self; constraint: executes synchronous task runner functions, measures elapsed time, and returns ProcessStepResult.
- procedure:activities.resample_activity_to_distance: resamples activity GPS and telemetry to 1-meter intervals; defined at docs/lineage/defs/procedure.activities.resample_activity_to_distance.sql; invoked via backend/pifitness/activities/smoothing.py:168-169 (call_resample_activity_to_distance); pattern: backend/pifitness/activities/smoothing.py; gotcha: 1-meter non-equi join across large activity tracks is high latency, requiring profiling before any structural changes.

### Principles Alignment
- ARCHITECTURE.md: backend route process_generator adheres to stream handling and modular service imports.
- UI_PRINCIPLES.md: responsive checklist rendering across 360x740 and 448x875 viewports with smooth visual feedback.
- DATABASE_PRINCIPLES.md: non-breaking analysis and additive optimizations without altering existing table schemas or degrading matching accuracy.

### Approach
1. Import `run_step` from `pifitness.activities.services` into `backend/pifitness/activities/router.py` to fix the `NameError` crash when executing `sync_activities` and subsequent pipeline steps, and resolve any further runtime errors encountered along the pipeline to achieve clean end-to-end processing.
2. Update `frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx` to ensure progress bars animate smoothly on mobile viewports using CSS width transitions and accurate live elapsed time against step estimates.
3. Profile and evaluate backend pipeline steps (especially `resample_activity_to_distance` and segment matching procedures) to document execution times and identify targeted database performance opportunities without modifying match tolerances or race replay fidelity.

### Data and Migrations
- None

### API Changes
- None

## Acceptance Criteria
- [ ] AC-1: Navigate to /activities/activity-processing, select Last Walk or Last Run, and click Start Processing -> step 1 Syncing activities starts and runs without a "name 'run_step' is not defined" error, and processing runs to completion (covers B1)
  - auto: new test tests/test_process_generator_events.py::test_process_generator_yields_step_without_name_error [smoke]
- [ ] AC-2: On /activities/activity-processing with Last Run and a playlist selected, click Start Processing -> all sync, playlist shuffle, elevation/smoothing, and segment matching steps execute in order to completion (covers B1)
  - auto: python -m py_compile backend/pifitness/activities/router.py
- [ ] AC-3: At mobile viewports (360x740 and 448x875) on /activities/activity-processing while a step is executing -> the step status bar displays active progress that fills smoothly over time based on elapsed time and estimated duration (covers B2)
  - auto: node frontend/pifitness/node_modules/typescript/bin/tsc -p frontend/pifitness/tsconfig.json --noEmit
- [ ] AC-4: Profile backend pipeline execution times and database stored procedures (including resample_activity_to_distance and segment matching) -> evaluation summary identifies latency hotspots and safe optimization opportunities without breaking match criteria or replay smoothness (covers B3)
  - auto: python -m py_compile backend/pifitness/activities/smoothing.py

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** You said: "I can't promise that B1 is the full scope of the error since no further steps can currently be run. Expectation is that the scope extends to any new errors that arrive as part of the activity processing". Restated understanding: The activity processing pipeline will be fixed so that triggering processing runs end-to-end (sync, details, playlist shuffle, smoothing, segment matching) without crashing, fixing `run_step` and any subsequent unhandled errors encountered along the pipeline. Step progress bars will animate smoothly on mobile viewports. Backend and database processing steps will be profiled and evaluated for performance improvements without sacrificing replay smoothness or match accuracy. Assumption: Scope covers fixing any errors within the activity processing pipeline steps to achieve successful end-to-end processing. Is this right? Default: yes, this is right.
  - **Answer:** yes, this is right.
  - **Decision:** The activity processing pipeline will be fixed so that triggering processing runs end-to-end without crashing, fixing run_step and any subsequent errors in the activity processing steps.
  - **Rationale:** If this understanding is wrong, the scope and architecture of the fix must change.
  - **Status:** resolved
- **OQ-2** Performance scope for B3
  - **Question:** For B3, should this feature focus on profiling and evaluating the pipeline bottlenecks (identifying high-latency stored procedures such as resample_activity_to_distance and providing optimization proposals or safe index/query fixes), or should we alter activity_details_distance table granularity or stored procedure logic in this feature? Default: evaluate bottlenecks, implement safe non-breaking query/index fixes if identified, and record structural redesign proposals for future features.
  - **Answer:** I'm open to all potential performance improvement paths, but would want to understand what, if anything we would sacrifice to enable them, and what the estimated improvement would be.
  - **Decision:** Profile and evaluate pipeline bottlenecks (identifying stored procedure and query latency), document estimated speedups and trade-offs, and implement safe non-breaking optimizations.
  - **Rationale:** Changing meter granularity or matching stored procedures risks degrading match accuracy and replay smoothness, which the user flagged as out of scope.
  - **Status:** resolved
- **OQ-3** Mobile status bar animation behavior
  - **Question:** Should mobile status bar animation be enhanced by adding smooth CSS transition animations (transition-[width] duration-300) to the progress fill in StepChecklist.tsx, ensuring smooth visual progression between timer ticks at 360x740 and 448x875? Default: yes.
  - **Answer:** It should be smooth and it should work/display. I don't know technically how else to answer.
  - **Decision:** Add smooth CSS width transitions (transition-[width] duration-300) and ensure step progress bars visually animate and display correctly on mobile viewports in StepChecklist.tsx.
  - **Rationale:** Determines the frontend styling and animation behavior for B2.
  - **Status:** resolved
- **OQ-4** Test addition
  - **Question:** You said: "I'm open to a test, but I would not want it to execute the actual pipeline as a test -- because that could take minutes and would involve actually hitting APIs to do so. I also don't want to write a test against test criteria that is guaranteed to pass. If there's somewhere in the middle, I'm open to that.". Still open: Add a fast unit test `tests/test_process_generator_events.py::test_process_generator_yields_step_without_name_error` that mocks `sync_activities` and steps (no API calls, no DB hits, finishes in <1s) to verify generator initialization and symbol resolution without NameError? Default: yes.
  - **Answer:** yes
  - **Decision:** Add fast unit test `tests/test_process_generator_events.py::test_process_generator_yields_step_without_name_error` [smoke] with mocked steps.
  - **Rationale:** AC-1 is marked pending OQ-4; if approved, a new test will be added under tests/test_process_generator_events.py to verify process_generator invocation.
  - **Status:** resolved

## Task Plan

### T01 Profile and evaluate activity smoothing pipeline bottlenecks
- **Surface:** database
- **Skill:** postgres
- **Points:** 1
- **Depends-on:** none
- **Traces-to:** AC-4
- **Anchors:**
  - `modify`: `backend/pifitness/activities/smoothing.py` `call_resample_activity_to_distance`
  - `read`: `docs/lineage/defs/procedure.activities.resample_activity_to_distance.sql` `resample_activity_to_distance`
- **Detail:** Profile the activity smoothing pipeline and stored procedures (specifically `activities.resample_activity_to_distance` and segment matching) to identify high-latency execution points. Evaluate non-breaking optimization opportunities and calculate estimated speedups and functional trade-offs for future structural changes. Record the evaluation summary directly in code documentation.
  Finding: `activities.resample_activity_to_distance` resamples activity GPS data to 1-meter resolution using a non-equi join across `generate_series`.
  Finding: 1-meter granularity in `activity_details_distance` is essential for race replay smoothness and segment matching accuracy; any reduction in granularity would sacrifice interpolation fidelity.
  Finding: Stored procedures are called synchronously via `call_resample_activity_to_distance` in `backend/pifitness/activities/smoothing.py:168`.
- **Verify:** python -m py_compile backend/pifitness/activities/smoothing.py
- **Status:** complete

### T02 Fix run_step import and add process generator smoke test
- **Surface:** backend
- **Skill:** fastapi
- **Points:** 2
- **Depends-on:** none
- **Traces-to:** AC-1, AC-2
- **Anchors:**
  - `modify`: `backend/pifitness/activities/router.py` `process_generator`
  - `modify`: `tests/test_process_generator_events.py` `test_post_processing_steps_yields_marker_before_execution`
  - `read`: `backend/pifitness/activities/services.py` `run_step`
- **Detail:** Import `run_step` from `pifitness.activities.services` in `backend/pifitness/activities/router.py` to fix the `NameError` crash during activity processing. Ensure all pipeline steps execute in order without unhandled runtime errors. Add fast unit test `test_process_generator_yields_step_without_name_error` to `tests/test_process_generator_events.py` with mocked step runners to verify generator initialization and symbol resolution without hitting external APIs or databases.
  Finding: `run_step` is invoked in `router.py:203` (`sync_activities`), line 213 (`sync_details`), line 300 (`shuffle`), line 334 (`elevation`), and line 343 (`segments`).
  Finding: `run_step` signature in `services.py:42` is `run_step(step_id: str, fn, *args, **kwargs) -> ProcessStepResult`.
  Finding: Unit test in `tests/test_process_generator_events.py` mocks runner functions using `monkeypatch` to complete quickly in <1s.
- **Verify:** .venv\Scripts\python.exe -m pytest tests/test_process_generator_events.py::test_process_generator_yields_step_without_name_error -q
- **Status:** complete

### T03 Enable smooth status bar animations on mobile
- **Surface:** frontend
- **Skill:** react
- **Points:** 1
- **Depends-on:** none
- **Traces-to:** AC-3
- **Anchors:**
  - `modify`: `frontend/pifitness/src/app/activities/activity-processing/StepChecklist.tsx` `StepChecklist`
- **Detail:** Update `StepChecklist.tsx` so that step status progress bars animate smoothly on mobile viewports (360x740 and 448x875). Add smooth CSS transitions (`transition-[width] duration-300 ease-out`) to the inner progress bar fill element. Ensure width progression accurately tracks elapsed time against step estimates.
  Finding: `StepChecklist.tsx:151-159` renders the inner bar fill with `style={{ width: `${Math.round(frac * 100)}%` }}` without transition classes.
  Finding: Mobile layout uses `w-20 sm:w-28` for the bar track container at `StepChecklist.tsx:137`.
- **Verify:** node frontend/pifitness/node_modules/typescript/bin/tsc -p frontend/pifitness/tsconfig.json --noEmit
- **Status:** complete
