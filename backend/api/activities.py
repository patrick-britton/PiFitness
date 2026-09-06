"""
Activities API Endpoints
========================

FastAPI endpoints for activity data (GPS tracks, segments, metrics).
Includes the Activity Processing & Playlist Shuffle pipeline.
"""

import json
import time
import threading
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from starlette.responses import JSONResponse
from typing import Optional
from datetime import date

from backend_functions.queries import (
    get_activities_list,
    get_activity_by_id,
    get_activity_telemetry,
    get_segment_matches,
    resolve_latest_activity_id,
    get_activity_report_header,
    get_activity_percentile_hr,
    get_activity_report_efforts,
)
from backend_functions.database_functions import sql_to_dict, get_conn, qec, one_sql_result, sql_to_list
from backend_functions.music_functions import (
    get_spotify_client,
    playlist_upload,
    playlist_to_db,
)
from backend_functions.ultimate_task_executioner_v2 import ultimate_task_executioner

from backend.schemas.activity_schemas import (
    ProcessActivityRequest,
    ProcessActivityResponse,
    ProcessStepResult,
    ProcessStepResultData,
    ProcessStepStartEvent,
    ProcessSummaryData,
    ActivityReport,
    ActivityReportHeader,
    ActivityReportSegment,
)

router = APIRouter(prefix="/api/activities", tags=["activities"])


# ---------------------------------------------------------------------------
# Helper: run a step with timing, return a ProcessStepResult
# ---------------------------------------------------------------------------

def _run_step(step_id: str, fn, *args, **kwargs) -> ProcessStepResult:
    """Execute a step function, measure elapsed time, do not return internal result data."""
    t0 = time.perf_counter()
    try:
        fn(*args, **kwargs)
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id=step_id,
            status="complete",
            elapsed_ms=elapsed,
            error=None,
            result=None,
        )
    except Exception as e:
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id=step_id,
            status="error",
            elapsed_ms=elapsed,
            error=str(e),
            result=None,
        )


def _skip_step(step_id: str) -> ProcessStepResult:
    """Return a skipped step result."""
    return ProcessStepResult(
        step_id=step_id,
        status="skipped",
        elapsed_ms=0,
        error=None,
        result=None,
    )


def _start_event(step_id: str) -> dict:
    """Return a JSON-safe dict for a step start (running) event."""
    return ProcessStepStartEvent(step_id=step_id).model_dump(mode="json")


# ---------------------------------------------------------------------------
# In-process concurrency guard (FR-1, OQ-4): one processing run at a time.
# A second concurrent run is rejected immediately.
# ---------------------------------------------------------------------------
_process_lock = threading.Lock()


def _run_step_data(step_id: str, fn, *args, **kwargs) -> ProcessStepResult:
    """Execute a step function that returns ProcessStepResultData; measure elapsed time."""
    t0 = time.perf_counter()
    try:
        data = fn(*args, **kwargs)
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id=step_id,
            status="complete",
            elapsed_ms=elapsed,
            error=None,
            result=data,
        )
    except Exception as e:
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id=step_id,
            status="error",
            elapsed_ms=elapsed,
            error=str(e),
            result=None,
        )


def _resolve_activity_id(activity_type: str) -> Optional[int]:
    """Resolve the target activity via activities.vw_last_activity_id_by_type (T03/T04).

    activity_type is 'Walk' (last_walk mode) or 'Run' (last_run mode). The view
    bakes in most-recent selection (max activity_id) and distance constraints.
    """
    sql = (
        "SELECT activity_id FROM activities.vw_last_activity_id_by_type "
        f"WHERE activity_type = '{activity_type}'"
    )
    row = one_sql_result(sql)
    if not row:
        raise ValueError(
            f"No activity found by activities.vw_last_activity_id_by_type "
            f"for activity_type='{activity_type}'"
        )
    return int(row)


def _step_to_dict(step: ProcessStepResult) -> dict:
    """Convert a ProcessStepResult to a JSON-safe dict for NDJSON streaming."""
    d = {
        "step_id": step.step_id,
        "status": step.status,
        "elapsed_ms": step.elapsed_ms,
    }
    if step.error:
        d["error"] = step.error
    if step.result:
        try:
            # exclude_none: ProcessStepResultData carries several optional
            # fields; emitting them as null made the UI render stubs like
            # "songs heard." on steps that never produced them (Bug T10-8).
            d["result"] = step.result.model_dump(mode="json", exclude_none=True)
        except Exception:
            try:
                d["result"] = dict(step.result)
            except Exception:
                d["result"] = {}
    return d


# ---------------------------------------------------------------------------
# Activity Processing Endpoint (NDJSON stream)
# ---------------------------------------------------------------------------


def _process_generator(req: ProcessActivityRequest):
    """
    Generator that yields one NDJSON line per step (start + terminal) and a
    final terminal event with `complete: true`.

    Fixed order (AC-5): sync_activities → sync_details → resolve_activity →
    [FR-8.1–8.7 shuffle sequence, only last_run + not no_music] → activity
    post-processing (on the resolved activity_id) → summary.
    Halts on first error (FR-9); every executed step emits a start (running)
    event and exactly one terminal complete/error event (Bug #2 / FR-12).
    """
    # FR-1 / OQ-4: reject a concurrent second run.
    if not _process_lock.acquire(blocking=False):
        yield json.dumps({
            "complete": True,
            "success": False,
            "error": "A processing run is already in progress.",
        }) + "\n"
        return
    try:
        run_t0 = time.perf_counter()
        is_last_run = req.mode == "last_run"
        do_shuffle = is_last_run and req.music != "no_music"
        playlist_name = "Running" if req.music == "running" else "Jogging"
        has_error = False
        post_row: Optional[int] = None
        shuffle_completed = False

        # -------------------------------------------------------------------
        # Step 1: Sync Activities (always first — Bug #1 invariant, OQ-3)
        # -------------------------------------------------------------------
        yield json.dumps(_start_event("sync_activities")) + "\n"
        result = _run_step("sync_activities", ultimate_task_executioner, force_task_id=4)
        yield json.dumps(_step_to_dict(result)) + "\n"
        if result.status == "error":
            has_error = True

        # -------------------------------------------------------------------
        # Step 2: Sync Activity Details
        # -------------------------------------------------------------------
        if not has_error:
            yield json.dumps(_start_event("sync_details")) + "\n"
            result = _run_step("sync_details", ultimate_task_executioner, force_task_id=19)
            yield json.dumps(_step_to_dict(result)) + "\n"
            if result.status == "error":
                has_error = True

        # -------------------------------------------------------------------
        # Step 3: Resolve Activity (FR-5/FR-6 — view-based, OQ-1/OQ-2)
        # -------------------------------------------------------------------
        if not has_error:
            yield json.dumps(_start_event("resolve_activity")) + "\n"
            t0 = time.perf_counter()
            try:
                post_row = _resolve_activity_id("Run" if is_last_run else "Walk")
                result = ProcessStepResult(
                    step_id="resolve_activity",
                    status="complete",
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                    error=None,
                    result=None,
                )
            except Exception as e:
                post_row = None
                result = ProcessStepResult(
                    step_id="resolve_activity",
                    status="error",
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                    error=str(e),
                    result=None,
                )
            yield json.dumps(_step_to_dict(result)) + "\n"
            if result.status == "error":
                has_error = True

        # -----------------------------------------------------------------------
        # Step 4: FR-8 playlist shuffle sequence (only last_run + not no_music)
        # Sub-steps in FR-8 order; each emits start + terminal events (Bug #2).
        # Runs BEFORE post-processing per AC-5.
        # -----------------------------------------------------------------------
        playlist_id: Optional[str] = None
        track_ids: list = []
        if not has_error:
            if not do_shuffle:
                for sid in (
                    "lookup_playlist",
                    "insert_history",
                    "query_isrc_stats",
                    "send_to_spotify",
                    "verify_spotify",
                    "report_shuffle",
                ):
                    yield json.dumps(_step_to_dict(_skip_step(sid))) + "\n"
            else:
                # FR-8.1: query heard songs for the target playlist (playlist_name
                # scoped; the view derives its own time window — OQ-3).
                yield json.dumps(_start_event("lookup_playlist")) + "\n"
                t0 = time.perf_counter()
                try:
                    lookup_result = _lookup_playlist(playlist_name)
                except Exception as e:  # defensive: guarantee a terminal event (Bug #2)
                    lookup_result = ProcessStepResult(
                        step_id="lookup_playlist",
                        status="error",
                        elapsed_ms=int((time.perf_counter() - t0) * 1000),
                        error=str(e),
                        result=None,
                    )
                yield json.dumps(_step_to_dict(lookup_result)) + "\n"
                if lookup_result.status == "error":
                    has_error = True
                elif lookup_result.result:
                    playlist_id = lookup_result.result.playlist_id

                # FR-8.2: insert listening history from the heard songs.
                if not has_error:
                    yield json.dumps(_start_event("insert_history")) + "\n"
                    result = _run_step("insert_history", _insert_listening_history, playlist_name)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

                # FR-8.4: read the target playlist order (default_new_order asc).
                if not has_error:
                    yield json.dumps(_start_event("query_isrc_stats")) + "\n"
                    t0 = time.perf_counter()
                    try:
                        track_ids = _query_isrc_stats(playlist_id)
                        result = ProcessStepResult(
                            step_id="query_isrc_stats",
                            status="complete",
                            elapsed_ms=int((time.perf_counter() - t0) * 1000),
                            error=None,
                            result=None,
                        )
                    except Exception as e:
                        result = ProcessStepResult(
                            step_id="query_isrc_stats",
                            status="error",
                            elapsed_ms=int((time.perf_counter() - t0) * 1000),
                            error=str(e),
                            result=None,
                        )
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

                # FR-8.5: send the compiled order to Spotify (halt on failure —
                # never report success unverified, FR-10).
                if not has_error:
                    yield json.dumps(_start_event("send_to_spotify")) + "\n"
                    result = _run_step("send_to_spotify", _send_to_spotify, playlist_id, track_ids)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

                # FR-8.6 (Bug #1 fix): re-pull from Spotify and refresh the
                # stored order so future vw_watch_music_heard runs are correct.
                if not has_error:
                    yield json.dumps(_start_event("verify_spotify")) + "\n"
                    result = _run_step("verify_spotify", _verify_spotify, playlist_id)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

                # FR-8.7: report the shuffled song count.
                if not has_error:
                    yield json.dumps(_start_event("report_shuffle")) + "\n"
                    result = _run_step_data("report_shuffle", _report_shuffle, playlist_id)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True
                    else:
                        shuffle_completed = True

        # -----------------------------------------------------------------------
        # Step 5: Activity Post-processing substeps (elevation/smoothing +
        # segment matching) on the resolved activity_id. Runs after the shuffle
        # sequence per AC-5; each executed substep emits start + terminal.
        # -----------------------------------------------------------------------
        if not has_error:
            from backend_functions.activity_smoothing import activity_post_processing_steps

            if not post_row:
                # No resolvable activity: skip all known substeps
                skipped_substeps = [
                    "insert_heartrate",
                    "assign_elevation_reference_time",
                    "smooth_elevation_spikes_by_time",
                    "smooth_elevation_python_time",
                    "update_elevation_reference_by_time",
                    "resample_activity_to_distance",
                    "smooth_elevation_spikes_by_distance",
                    "smooth_elevation_python_distance",
                    "smooth_elevation_python_reference",
                    "update_elevation_reference_by_distance",
                    "build_activity_path",
                    "segment_match_segments",
                    "segment_pair_generation",
                    "segment_polygon_match",
                    "segment_mass_confirm_1",
                    "segment_hausdorff_match",
                    "segment_mass_confirm_2",
                    "segment_frechet_match",
                    "segment_mass_confirm_3",
                    "segment_update_details",
                ]
                for sid in skipped_substeps:
                    yield json.dumps(_step_to_dict(_skip_step(sid))) + "\n"
            else:
                try:
                    substeps = activity_post_processing_steps(post_row)
                except Exception as e:  # defensive: guarantee a terminal event (Bug #2)
                    substeps = [("post_processing", 0, str(e))]
                for step_id, elapsed_ms, error in substeps:
                    yield json.dumps(_start_event(step_id)) + "\n"
                    if error:
                        has_error = True
                        step = ProcessStepResult(
                            step_id=step_id,
                            status="error",
                            elapsed_ms=elapsed_ms,
                            error=error,
                            result=None,
                        )
                    else:
                        step = ProcessStepResult(
                            step_id=step_id,
                            status="complete",
                            elapsed_ms=elapsed_ms,
                            error=None,
                            result=None,
                        )
                    yield json.dumps(_step_to_dict(step)) + "\n"
                    if has_error:
                        break

        # -----------------------------------------------------------------------
        # End-of-run summary (AC-11/FR-14)
        # -----------------------------------------------------------------------
        summary_payload = None
        if not has_error:
            total_elapsed_ms = int((time.perf_counter() - run_t0) * 1000)
            summary_payload = _build_process_summary(shuffle_completed, post_row, total_elapsed_ms)

        # Terminal event
        terminal = {"complete": True, "success": not has_error}
        if summary_payload is not None:
            terminal["summary"] = summary_payload.model_dump(mode="json")
        yield json.dumps(terminal) + "\n"

    except Exception as e:
        yield json.dumps({"complete": True, "success": False, "error": f"Internal server error: {str(e)}"}) + "\n"
    finally:
        # FR-1/OQ-4: always release the guard so future runs are not locked out.
        _process_lock.release()


@router.post("/process")
async def process_activity(req: ProcessActivityRequest):
    """
    Run the activity processing pipeline (NDJSON stream).

    Fixed order (AC-5), halting on first error (FR-9):
      sync_activities → sync_details → resolve_activity →
      [FR-8.1–8.7 shuffle sequence, only last_run + not no_music] →
      activity post-processing substeps (on the resolved activity_id) →
      summary + terminal event.

    Every executed step emits a start (running) event and exactly one terminal
    complete/error event (Bug #2 / FR-12). One run at a time (FR-1).
    """
    return StreamingResponse(
        _process_generator(req),
        media_type="application/x-ndjson",
    )


# ---------------------------------------------------------------------------
# Internal step implementations
# ---------------------------------------------------------------------------


def _lookup_playlist(playlist_name: str) -> ProcessStepResult:
    """Query vw_watch_music_heard for the given playlist name."""
    t0 = time.perf_counter()
    try:
        conn = get_conn(alchemy=True)
        import pandas as pd
        sql = f"SELECT * FROM activities.vw_watch_music_heard WHERE playlist_name = '{playlist_name}'"
        df = pd.read_sql(sql, con=conn)
        conn.dispose()
        song_count = len(df)
        if song_count == 0:
            raise ValueError(f"No songs found for playlist '{playlist_name}'")
        first_song = str(df["track_name_clean"].iloc[0])
        last_song = str(df["track_name_clean"].iloc[song_count - 1])
        parent_playlist_id = str(df["playlist_id"].iloc[0])
        target_ids = sql_to_list(
            "SELECT DISTINCT target_playlist_id FROM music.vw_playlist_shuffle_targets "
            "WHERE parent_playlist_id = %s ",
            (parent_playlist_id,),
        )
        if not target_ids:
            raise ValueError(
                f"No target_playlist_id found in music.vw_playlist_isrc_stats "
                f"for playlist '{parent_playlist_id}'"
            )
        playlist_id = str(target_ids[0])
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id="lookup_playlist",
            status="complete",
            elapsed_ms=elapsed,
            error=None,
            result=ProcessStepResultData(
                song_count=song_count,
                first_song=first_song,
                last_song=last_song,
                playlist_shuffled=None,
                playlist_id=playlist_id,
            ),
        )
    except Exception as e:
        elapsed = int((time.perf_counter() - t0) * 1000)
        return ProcessStepResult(
            step_id="lookup_playlist",
            status="error",
            elapsed_ms=elapsed,
            error=str(e),
            result=None,
        )


def _insert_listening_history(playlist_name: str) -> Optional[ProcessStepResultData]:
    """Truncate temp_listening_history, insert from vw_watch_music_heard, reconcile."""
    conn = get_conn(alchemy=True)
    insert_sql = f"SELECT played_at_utc, isrc, playlist_id FROM activities.vw_watch_music_heard WHERE playlist_name = '{playlist_name}'"
  
    # Reconcile into listening_history
    reconcile_sql = f"""INSERT INTO music.listening_history (
        played_at_utc, isrc, playlist_id)
        {insert_sql}
        ON CONFLICT(played_at_utc, isrc) DO NOTHING;"""
    err = qec(reconcile_sql)
    if err:
        raise ValueError(f"Error reconciling listening history: {err}")

    return None


def _query_isrc_stats(playlist_id: str) -> list:
    """FR-8.4: track send order from music.vw_playlist_isrc_stats (default_new_order asc).

    Filter on the view's `playlist_id` column using the resolved target_playlist_id
    (Bug T10-5 fix). `playlist_id` here is the target (child) playlist passed in from
    _lookup_playlist.
    """
    sql = (
        "SELECT track_id FROM music.vw_playlist_isrc_stats "
        "WHERE target_playlist_id = %s AND cumulative_playlist_minutes <= minutes_to_sync"
        "ORDER BY default_new_order ASC"
    )
    track_ids = sql_to_list(sql, (playlist_id,))
    if not track_ids:
        raise ValueError(f"No tracks returned by music.vw_playlist_isrc_stats for playlist '{playlist_id}'")
    return track_ids


def _send_to_spotify(playlist_id: str, track_ids: list) -> None:
    """FR-8.5: atomically replace the target playlist with the compiled track list."""
    client = get_spotify_client()
    playlist_upload(client, list_id=playlist_id, track_list=track_ids)


def _verify_spotify(playlist_id: str) -> None:
    """FR-8.6 (Bug #1 fix): after a successful send, re-pull the target playlist
    from Spotify and refresh the stored track order in music.playlist_isrcs.

    FR-10: this step actively verifies its effect. playlist_to_db can silently
    no-op (early return) when no usable Spotify client is available, so we
    (1) require a working client up front and (2) confirm music.playlist_isrcs
    was actually refreshed before reporting success.
    """
    client = get_spotify_client()
    if client is None or client.get("client") is None:
        raise RuntimeError(
            "verify_spotify: could not obtain a usable Spotify client "
            "(rate-limited or token unavailable) — cannot re-pull playlist details."
        )

    playlist_to_db(client, list_id=playlist_id)

    # FR-10: confirm the re-pull actually refreshed the stored order in the DB.
    fresh = one_sql_result(
        "SELECT max(updated_at_utc) >= (CURRENT_TIMESTAMP - interval '5 minutes') "
        "FROM music.playlist_isrcs WHERE playlist_id = %s",
        (playlist_id,),
    )
    if not fresh:
        raise RuntimeError(
            f"verify_spotify: music.playlist_isrcs not refreshed for playlist "
            f"'{playlist_id}' (updated_at_utc not recent) — the stored order was "
            f"not updated to reflect the Spotify send."
        )


def _report_shuffle(playlist_id: str) -> ProcessStepResultData:
    """FR-8.7: report the number of tracks sent to Spotify (target playlist)."""
    count = one_sql_result(
        "SELECT count(*) FROM music.playlist_isrcs WHERE playlist_id = %s",
        (playlist_id,),
    )
    if count is None:
        raise ValueError(f"Could not count tracks for playlist '{playlist_id}'")
    return ProcessStepResultData(
        songs_sent=int(count),
        playlist_id=str(playlist_id),
    )


def _build_process_summary(
    shuffle_completed: bool,
    activity_id: Optional[int],
    total_elapsed_ms: Optional[int] = None,
) -> ProcessSummaryData:
    """Build the end-of-run summary (AC-11, FR-14).

    Read-only DB reads; any failure degrades the affected fields to null so the
    summary can never convert a successful run into an error.
    """
    segments_matched: Optional[int] = None
    courses_matched: Optional[int] = None
    course_found: Optional[bool] = None
    course_name: Optional[str] = None
    if activity_id is not None:
        try:
            seg_count = one_sql_result(
                "SELECT count(*) FROM activities.segments_details WHERE activity_id = %s",
                (activity_id,),
            )
            segments_matched = int(seg_count) if seg_count is not None else 0
            course_rows = sql_to_dict(
                """SELECT s.segment_name
                   FROM activities.segments_details sd
                   JOIN activities.segments s ON s.segment_id = sd.segment_id
                   WHERE sd.activity_id = %s AND s.is_course
                   ORDER BY sd.start_time_utc""",
                (activity_id,),
            )
            courses_matched = len(course_rows)
            course_found = bool(course_rows)
            course_name = course_rows[0]["segment_name"] if course_rows else None
        except Exception as e:
            print(f"[activities] End-of-run summary DB read failed: {e}")
            segments_matched = None
            courses_matched = None
            course_found = None
            course_name = None
    return ProcessSummaryData(
        total_elapsed_ms=total_elapsed_ms,
        playlist_shuffled=True if shuffle_completed else None,
        segments_matched=segments_matched,
        courses_matched=courses_matched,
        course_found=course_found,
        course_name=course_name,
        activity_id=activity_id,
    )


# ---------------------------------------------------------------------------
# Activity Report endpoint (009-001)
# ---------------------------------------------------------------------------

VALID_REPORT_TYPES = ('Run', 'Walk')


def _format_total_time(total_time_s: float) -> str:
    """Format seconds (e.g. 3723.456) as h:mm:ss.ms (e.g. 1:02:03.456)."""
    if total_time_s is None:
        return "--:--:--.---"
    total_ms = int(round(float(total_time_s) * 1000))
    hours, rem = divmod(total_ms, 3600_000)
    minutes, rem = divmod(rem, 60_000)
    seconds, ms = divmod(rem, 1000)
    return f"{hours}:{minutes:02d}:{seconds:02d}.{ms:03d}"


def _format_pace(distance_mi: float, total_time_s: float) -> str:
    """Format pace as m:ss.ms/mi from miles and seconds (e.g. 8:30.25/mi)."""
    if not distance_mi or not total_time_s or float(distance_mi) <= 0:
        return "--:--.--/mi"
    secs_per_mi = float(total_time_s) / float(distance_mi)
    minutes = int(secs_per_mi // 60)
    remainder = secs_per_mi - (minutes * 60)
    return f"{minutes}:{remainder:05.2f}/mi"


def _is_run_type(activity_type: str) -> bool:
    """True for the Run report type (running/trail); False for Walk (walk/hike)."""
    return (activity_type or '').lower() == 'run'


@router.get("/report")
async def get_activity_report(activity_type: str = 'Run'):
    """
    Get the Recent Activity Report for the most recent Run or Walk activity (009-001).

    Resolves the target activity via activities.vw_last_activity_id_by_type (the
    same view Activity Processing uses), then composes the summary header, the
    course (if any), and crossed segments with comparisons. Read-only; no writes.

    Args:
        activity_type: 'Run' or 'Walk'. Defaults to 'Run'.

    Returns:
        ActivityReport matching the cross-surface contract.
    """
    if activity_type not in VALID_REPORT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"activity_type must be one of {VALID_REPORT_TYPES}",
        )

    try:
        activity_id = resolve_latest_activity_id(activity_type)
        if activity_id is None:
            raise HTTPException(
                status_code=404,
                detail=f"No activity found for activity_type='{activity_type}'",
            )

        header_row = get_activity_report_header(activity_id)
        if header_row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No report data found for activity_id={activity_id}",
            )

        hr_median = get_activity_percentile_hr(activity_id, 0.5)
        hr_p75 = get_activity_percentile_hr(activity_id, 0.75)
        hr_max = get_activity_percentile_hr(activity_id, 1.0)

        effort_rows = get_activity_report_efforts(activity_id)
        course_row = next((r for r in effort_rows if r.get('is_course')), None)

        # show_efficiency_placeholder: true for Run (running/trail); false for Walk.
        # Resolved via the activity_type selected (the report only serves Run/Walk).
        show_eff = _is_run_type(activity_type)

        header = ActivityReportHeader(
            start_utc=str(header_row['start_utc']),
            distance_mi=float(header_row['distance_mi'] or 0),
            total_time_s=float(header_row['total_time_s'] or 0),
            total_time_text=_format_total_time(header_row['total_time_s']),
            pace_text=_format_pace(header_row['distance_mi'], header_row['total_time_s']),
            hr_median=float(hr_median) if hr_median is not None else None,
            hr_p75=float(hr_p75) if hr_p75 is not None else None,
            hr_max=float(hr_max) if hr_max is not None else None,
            show_efficiency_placeholder=show_eff,
        )

        course = None
        segments = []
        if course_row:
            course = ActivityReportSegment(
                segment_id=int(course_row['segment_id']),
                name=str(course_row['name']),
                is_course=True,
                all_time_rank=int(course_row['all_time_rank']) if course_row.get('all_time_rank') is not None else None,
                total_attempts=int(course_row['total_attempts'] or 0),
                prior_delta_s=float(course_row['prior_delta_s']) if course_row.get('prior_delta_s') is not None else None,
                best_delta_s=float(course_row['best_delta_s']) if course_row.get('best_delta_s') is not None else None,
            )

        for r in effort_rows:
            if r.get('is_course'):
                continue
            segments.append(ActivityReportSegment(
                segment_id=int(r['segment_id']),
                name=str(r['name']),
                is_course=False,
                all_time_rank=int(r['all_time_rank']) if r.get('all_time_rank') is not None else None,
                total_attempts=int(r['total_attempts'] or 0),
                prior_delta_s=float(r['prior_delta_s']) if r.get('prior_delta_s') is not None else None,
                best_delta_s=float(r['best_delta_s']) if r.get('best_delta_s') is not None else None,
            ))

        report = ActivityReport(
            activity_id=activity_id,
            activity_type=activity_type,
            header=header,
            course=course,
            segments=segments,
            has_segments=bool(course_row or segments),
        )
        return report.model_dump(mode="json")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activity report: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Existing endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def list_activities(
    limit: Optional[int] = 50,
    offset: Optional[int] = 0,
    activity_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    """
    List activities with optional filtering.

    Args:
        limit: Maximum number of activities to return
        offset: Pagination offset
        activity_type: Filter by activity type (e.g., 'run', 'ride')
        start_date: Filter by start date (inclusive)
        end_date: Filter by end date (inclusive)

    Returns:
        List of activity summaries
    """
    try:
        activities = get_activities_list(
            limit=limit or 50,
            offset=offset or 0,
            activity_type=activity_type,
            start_date=start_date,
            end_date=end_date,
        )
        return {"data": activities, "count": len(activities)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activities: {str(e)}",
        )


@router.get("/{activity_id}")
async def get_activity(activity_id: int):
    """
    Get a single activity by ID with full details.

    Args:
        activity_id: The activity ID

    Returns:
        Activity record with metadata
    """
    try:
        activity = get_activity_by_id(activity_id)
        if not activity:
            raise HTTPException(status_code=404, detail="Activity not found")
        return {"data": activity}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activity: {str(e)}",
        )


@router.get("/{activity_id}/telemetry")
async def get_activity_telemetry_data(activity_id: int):
    """
    Get telemetry data (GPS, heart rate, cadence) for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        Telemetry data points
    """
    try:
        telemetry = get_activity_telemetry(activity_id)
        if not telemetry:
            raise HTTPException(status_code=404, detail="Telemetry not found")
        return {"data": telemetry}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telemetry: {str(e)}",
        )


@router.get("/{activity_id}/segments")
async def get_activity_segment_matches(activity_id: int):
    """
    Get segment matches for an activity.

    Args:
        activity_id: The activity ID

    Returns:
        List of segment match records
    """
    try:
        segments = get_segment_matches(activity_id)
        return {"data": segments, "count": len(segments)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch segment matches: {str(e)}",
        )