---
feature-id: fix-the-target-running-playlist
title: Fix the target running playlist
category: Bug Fix
status: in-progress
created: 2026-11-18
last-updated: 2026-11-20
surfaces: endpoint:POST /api/activities/process, py:backend/pifitness/activities/router.py::_lookup_playlist, py:backend/pifitness/activities/router.py::_query_isrc_stats, py:backend/backend_functions/music_functions.py::auto_shuffle_playlists, view:music.vw_playlist_isrc_stats
points: 4
request: .features/descriptions/fix-the-target-running-playlist.md
---

# Design: Fix the target running playlist

## Resume Here
**progress:** 3/3 tasks complete
**next:** none
**blocked-by:** none

## Architecture

### Surfaces
- `endpoint:POST /api/activities/process`: handles activity processing pipeline including playlist shuffle sequence
- `py:backend/pifitness/activities/router.py::_lookup_playlist`: resolves target child playlist for activity music
- `py:backend/pifitness/activities/router.py::_query_isrc_stats`: queries tracks from `music.vw_playlist_isrc_stats` with duration cap
- `py:backend/backend_functions/music_functions.py::auto_shuffle_playlists`: background automated playlist shuffle and upload
- `view:music.vw_playlist_isrc_stats`: view computing track scores, new order, and cumulative minutes

### Principles alignment
- Backend: `docs/ARCHITECTURE.md` (domain isolation in `pifitness/activities/`, service extraction)
- Integrations: `docs/INTEGRATION_PRINCIPLES.md` (Spotify client rate limiting and atomic uploads)
- Database: `docs/DATABASE_PRINCIPLES.md` (view stability, additive only)

### Approach
1. In `_lookup_playlist`, ensure target child selection queries `music.vw_playlist_shuffle_targets` safely with deterministic target ordering and accurate error reporting.
2. In `_query_isrc_stats`, enforce that when `minutes_to_sync` is > 0 and < 9999, tracks are capped by `cumulative_playlist_minutes <= minutes_to_sync`; when `minutes_to_sync` is NULL, 0, or >= 9999, return all tracks.
3. In `auto_shuffle_playlists`, ensure track selection respects `minutes_to_sync` when configured (> 0 and < 9999) using cumulative duration rather than sending all tracks from the parent playlist.
4. Verify Spotify upload and auto-shuffle query behavior with existing test suites.

### Data and Migrations
- None

### API Changes
- None

## Acceptance Criteria
- [x] AC-1: Process an activity with Run mode and Running music -> the shuffle step sends only the duration-capped track list (~20 to 35 tracks, <= 150 minutes) to the child playlist ('Running (a)') on Spotify rather than all 400+ tracks from the parent playlist (covers B1)
  - auto: `& 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe' -m pytest tests/test_activities.py -q`
- [x] AC-2: Run `auto_shuffle_playlists` for a playlist with a configured `minutes_to_sync` duration cap -> tracks sent to the target child playlist are limited to the duration cap rather than all tracks from the parent playlist (covers B2)
  - auto: `& 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe' -m pytest tests/test_auto_shuffle_pandas_free.py -q`

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** In my words: When processing an activity with a run playlist, the application will resolve the manual child playlist and send only an autoshuffled duration-limited mix (~20-30 tracks, based on `minutes_to_sync`) to Spotify instead of all 400+ tracks from the parent playlist. Normal activity metrics, GPS smoothing, and segment matching remain unchanged. I am assuming scope is bounded to B1 and B2. Is this right? Default: yes, this is right.
  - **Answer:** Scope is bound to B1 -- the manually shuffled playlists UNLESS this same root cause also applies to the autoshuffled playlists, in which case B2 is in scope as well.
  - **Decision:** human: Scope is bounded to B1 (manually shuffled playlists) and B2 if autoshuffle shares the root cause.
  - **Rationale:** Scope definition; establishes the core boundary for the bug fix.
  - **Status:** resolved

- **OQ-2** Duration limit and tuning weights source
  - **Question:** In `music.vw_playlist_isrc_stats`, `minutes_to_sync` and weights are joined from the parent playlist configuration in `music.playlist_config`. Should the duration limit and weights always come from the parent playlist config, or should child playlist config override them if present? Default: use the parent playlist configuration.
  - **Answer:** The parameters should come from the parent playlist configuration.
  - **Decision:** human: Duration limit and tuning weights come from the parent playlist configuration.
  - **Rationale:** Determines whether config lookup queries the parent or child playlist record.
  - **Status:** resolved

- **OQ-3** Fallback duration ceiling when minutes_to_sync is 9999
  - **Question:** If the resolved playlist has `minutes_to_sync = 9999` (the DB default meaning no duration limit), should the manual run shuffle during activity processing enforce a fallback default cap (e.g. 150 minutes, ~25-35 tracks), or send all tracks? Default: enforce a 150-minute ceiling when `minutes_to_sync >= 9999` or unset.
  - **Answer:** Send all tracks when >= 9999 or minutes_to_sync = 0 or minutes_to_sync is null.
  - **Decision:** human: Send all tracks when minutes_to_sync >= 9999, = 0, or NULL; cap by cumulative minutes otherwise.
  - **Rationale:** Prevents inadvertently sending 400+ tracks if a parent playlist config has default 9999 minutes.
  - **Status:** resolved

- **OQ-4** Duration cap in auto_shuffle_playlists
  - **Question:** In `backend_functions/music_functions.py::auto_shuffle_playlists`, `limit_minutes` currently defaults to `False`. Should `auto_shuffle_playlists` respect `minutes_to_sync` whenever `minutes_to_sync < 9999` (or default `limit_minutes=True`) so auto-shuffled playlists with a duration cap are also protected against sending all parent tracks? Default: yes, respect `minutes_to_sync` when < 9999.
  - **Answer:** yes, respect
  - **Decision:** human: auto_shuffle_playlists respects minutes_to_sync whenever minutes_to_sync < 9999 and > 0.
  - **Rationale:** Addresses B2 to ensure auto-shuffled playlists also obey duration limits.
  - **Status:** resolved

- **OQ-5** Target child playlist filtering
  - **Question:** In `_lookup_playlist`, `SELECT DISTINCT target_playlist_id FROM music.vw_playlist_shuffle_targets WHERE parent_playlist_id = %s` does not specify `target_playlist_type`. Should it explicitly filter by `target_playlist_type = 'manual'` to guarantee the manual child playlist is chosen? Default: yes, filter by `target_playlist_type = 'manual'`.
  - **Answer:** No -- because I beleive this same used is also used by the autoshuffling background task, and hard-coding manual would break the autoshuffle task.
  - **Decision:** human: Do not hard-code target_playlist_type = 'manual' in _lookup_playlist.
  - **Rationale:** Guarantees that if a parent has both auto and manual targets, the manual running playlist target is selected.
  - **Status:** resolved

- **OQ-6** Add regression test for activity processing shuffle cap
  - **Question:** Add test `tests/test_activities.py::test_process_activity_shuffle_duration_cap` [invariant]? It guards that activity processing shuffle sends only duration-capped tracks to the child playlist. Default: yes.
  - **Answer:** No.
  - **Decision:** human: Do not add a new regression test; verify via existing test suite.
  - **Rationale:** Adding a test requires human approval per workflow rules; guards against regression.
  - **Status:** resolved

## Task Plan

### T01 Guard playlist target resolution in _lookup_playlist
- **Surface:** backend
- **Skill:** fastapi
- **Points:** 1
- **Depends-on:** none
- **Traces-to:** AC-1
- **Anchors:**
  - `modify`: `backend/pifitness/activities/router.py` `_lookup_playlist`
- **Detail:** Ensure `_lookup_playlist` queries `music.vw_playlist_shuffle_targets` safely with deterministic target ordering and correct error reporting referencing the shuffle targets view. Preserves compatibility without hard-coding manual playlist type.
- **Verify:** C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m py_compile backend/pifitness/activities/router.py
- **Status:** complete

### T02 Enforce duration capping logic in _query_isrc_stats
- **Surface:** backend
- **Skill:** fastapi
- **Points:** 1
- **Depends-on:** T01
- **Traces-to:** AC-1
- **Anchors:**
  - `modify`: `backend/pifitness/activities/router.py` `_query_isrc_stats`
- **Detail:** Update `_query_isrc_stats` to apply duration capping when `minutes_to_sync` is greater than 0 and less than 9999. If `minutes_to_sync` is NULL, 0, or >= 9999, return all tracks without duration filtering.
- **Verify:** C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m py_compile backend/pifitness/activities/router.py
- **Status:** complete

### T03 Enforce duration cap in auto_shuffle_playlists
- **Surface:** integration
- **Skill:** postgres
- **Points:** 2
- **Depends-on:** none
- **Traces-to:** AC-2
- **Anchors:**
  - `modify`: `backend/backend_functions/music_functions.py` `auto_shuffle_playlists`
- **Detail:** Update `auto_shuffle_playlists` query logic to respect `minutes_to_sync` duration caps when configured (> 0 and < 9999) using cumulative playlist minutes. Preserves full playlist return when unlimited or unconfigured.
- **Verify:** C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe -m py_compile backend/backend_functions/music_functions.py
- **Status:** complete

