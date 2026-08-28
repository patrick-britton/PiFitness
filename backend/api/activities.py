"""
Activities API Endpoints
========================

FastAPI endpoints for activity data (GPS tracks, segments, metrics).
Includes the Activity Processing & Playlist Shuffle pipeline.
"""

import json
import time
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
)
from backend_functions.database_functions import sql_to_dict, get_conn, qec, one_sql_result, sql_to_list
from backend_functions.music_functions import auto_shuffle_playlists
from backend_functions.ultimate_task_executioner_v2 import ultimate_task_executioner

from backend.schemas.activity_schemas import (
    ProcessActivityRequest,
    ProcessActivityResponse,
    ProcessStepResult,
    ProcessStepResultData,
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
            d["result"] = dict(step.result)
        except Exception:
            try:
                d["result"] = step.result.model_dump(mode="json")
            except Exception:
                d["result"] = {}
    return d


# ---------------------------------------------------------------------------
# Activity Processing Endpoint (NDJSON stream)
# ---------------------------------------------------------------------------


def _process_generator(req: ProcessActivityRequest):
    """
    Generator that yields one NDJSON line per step completion.
    Final line is a terminal event with `complete: true`.
    """
    try:
        playlist_name: str | None = req.playlist_name
        is_manual = playlist_name == "Manual Processing"
        is_no_playlist = playlist_name == "No Playlist"
        has_error = False

        # -----------------------------------------------------------------------
        # Step 1: Sync Activities
        # -----------------------------------------------------------------------
        result = _run_step("sync_activities", ultimate_task_executioner, force_task_id=4)
        yield json.dumps(_step_to_dict(result)) + "\n"
        if result.status == "error":
            has_error = True

        # -----------------------------------------------------------------------
        # Step 2: Sync Activity Details
        # -----------------------------------------------------------------------
        if not has_error:
            result = _run_step("sync_details", ultimate_task_executioner, force_task_id=19)
            yield json.dumps(_step_to_dict(result)) + "\n"
            if result.status == "error":
                has_error = True

            # -----------------------------------------------------------------------
            # Step 3: Activity Post-processing substeps (elevation/smoothing + segment matching)
            # -----------------------------------------------------------------------
            if not has_error:
                from backend_functions.activity_smoothing import activity_post_processing_steps

                post_sql = """SELECT activity_id FROM activities.activities
                              WHERE activity_type_name in ('running', 'trail_running')
                              ORDER BY activity_id DESC LIMIT 1"""
                post_row = one_sql_result(post_sql)

                if not post_row:
                    # No running/trail activity: skip all known substeps
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
                    for step_id, elapsed_ms, error in activity_post_processing_steps(post_row):
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
        # Step 4: Look Up Playlist (skip if No Playlist)
        # -----------------------------------------------------------------------
        if not has_error:
            if is_no_playlist:
                yield json.dumps(_step_to_dict(_skip_step("lookup_playlist"))) + "\n"
                yield json.dumps(_step_to_dict(_skip_step("insert_history"))) + "\n"
                yield json.dumps(_step_to_dict(_skip_step("auto_shuffle"))) + "\n"
            else:
                pn = "Running" if is_manual else (playlist_name or "Running")
                lookup_result = _lookup_playlist(pn)
                yield json.dumps(_step_to_dict(lookup_result)) + "\n"
                if lookup_result.status == "error":
                    has_error = True

                # ---------------------------------------------------------------
                # Step 5: Insert Listening History
                # ---------------------------------------------------------------
                if not has_error:
                    result = _run_step("insert_history", _insert_listening_history, pn)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

                # ---------------------------------------------------------------
                # Step 6: Auto Shuffle
                # ---------------------------------------------------------------
                if not has_error:
                    result = _run_step("auto_shuffle", _do_auto_shuffle, pn)
                    yield json.dumps(_step_to_dict(result)) + "\n"
                    if result.status == "error":
                        has_error = True

        # -----------------------------------------------------------------------
        # Step 7: Cleanup (Manual Processing only)
        # -----------------------------------------------------------------------
        if not has_error:
            if is_manual:
                result = _run_step("cleanup", _cleanup_fake_activity)
                yield json.dumps(_step_to_dict(result)) + "\n"
            else:
                yield json.dumps(_step_to_dict(_skip_step("cleanup"))) + "\n"

        # Terminal event
        yield json.dumps({"complete": True, "success": not has_error}) + "\n"

    except Exception as e:
        yield json.dumps({"complete": True, "success": False, "error": f"Internal server error: {str(e)}"}) + "\n"


@router.post("/process")
async def process_activity(req: ProcessActivityRequest):
    """
    Run the activity processing pipeline.

    Steps (sequential, halts on first error):
      1. sync_activities  — ultimate_task_executioner(task_id=4)
      2. sync_details     — ultimate_task_executioner(task_id=19)
      3. match_segments   — ultimate_task_executioner(task_id=21)
      4. lookup_playlist  — query vw_watch_music_heard
      5. insert_history   — truncate temp_listening_history, insert, reconcile
      6. auto_shuffle     — auto_shuffle_playlists()
      7. cleanup          — delete fake activity (Manual Processing only)

    Returns NDJSON stream with one event per step, plus a terminal event.
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
        playlist_id = str(df["playlist_id"].iloc[0])
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
    import pandas as pd
    conn = get_conn(alchemy=True)
    sql = f"SELECT played_at_utc, isrc, playlist_id FROM activities.vw_watch_music_heard WHERE playlist_name = '{playlist_name}'"
    df = pd.read_sql(sql, con=conn)
    if df.empty:
        conn.dispose()
        raise ValueError(f"No listening history data for playlist '{playlist_name}'")

    # Truncate temp table
    err = qec("TRUNCATE music.temp_listening_history")
    if err:
        conn.dispose()
        raise ValueError(f"Error truncating temp_listening_history: {err}")

    # Insert into temp table
    df.to_sql(
        schema="music",
        name="temp_listening_history",
        con=conn,
        if_exists="replace",
        index=False,
    )
    conn.dispose()

    # Reconcile into listening_history
    reconcile_sql = """INSERT INTO music.listening_history (
        played_at_utc, isrc, playlist_id)
        SELECT played_at_utc::TIMESTAMPTZ, isrc, playlist_id
        FROM music.temp_listening_history
        ON CONFLICT(played_at_utc, isrc) DO NOTHING;"""
    err = qec(reconcile_sql)
    if err:
        raise ValueError(f"Error reconciling listening history: {err}")

    return None


def _do_auto_shuffle(playlist_name: str) -> Optional[ProcessStepResultData]:
    """Get the playlist ID and call auto_shuffle_playlists."""
    import pandas as pd
    conn = get_conn(alchemy=True)
    sql = f"SELECT DISTINCT playlist_id FROM activities.vw_watch_music_heard WHERE playlist_name = '{playlist_name}'"
    df = pd.read_sql(sql, con=conn)
    conn.dispose()
    if df.empty:
        raise ValueError(f"No playlist_id found for '{playlist_name}'")
    target_id = str(df["playlist_id"].iloc[0])
    auto_shuffle_playlists(target_id, limit_minutes=True)
    return ProcessStepResultData(
        song_count=None,
        first_song=None,
        last_song=None,
        playlist_shuffled=True,
        playlist_id=str(target_id),
    )


def _cleanup_fake_activity() -> Optional[ProcessStepResultData]:
    """Delete the fake activity used for Manual Processing."""
    del_sql = "DELETE FROM activities.activities WHERE activity_id = 9223372036854775800"
    err = qec(del_sql)
    if err:
        raise ValueError(f"Error deleting fake activity: {err}")
    return None


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