"""Public service functions for the activities domain.

Extracted from router modules so that tests and other callers import from a
service layer rather than reaching into router private names (PY-005).
"""

from typing import Optional

from backend_functions.database_functions import one_sql_result
from backend_functions.music_functions import get_spotify_client, playlist_to_db
from pifitness.activities.schemas import ProcessStepResult


def task_failure_message(ret) -> Optional[str]:
    """Aggregate failed entries from an ultimate_task_executioner results dict.

    OQ-2 option (a): the executioner already returns
    `{tasks_processed, results: [{task_id, task_name, success, error}], status}`;
    any entry with success=False is aggregated into one message so a forced
    task failure can mark the step `error` and halt the pipeline instead of
    passing silently (Bug 004-005-2 / Chain A2).

    Returns None unless `ret` is that summary dict — unrelated step functions
    returning other shapes (or nothing) are unaffected. Entries without an
    explicit `success` key are not treated as failures (conservative).
    """
    if not isinstance(ret, dict):
        return None
    results = ret.get('results')
    if not isinstance(results, list):
        return None
    failed = [r for r in results if isinstance(r, dict) and r.get('success') is False]
    if not failed:
        return None
    parts = []
    for r in failed:
        name = r.get('task_name') or f"task {r.get('task_id')}"
        parts.append(f"{name}: {r.get('error') or 'failed'}")
    return "; ".join(parts)


def run_step(step_id: str, fn, *args, **kwargs) -> ProcessStepResult:
    """Execute a step function, measure elapsed time, do not return internal result data.

    Task-runner results dicts are inspected via `task_failure_message`
    (OQ-2a): a forced task that reports failure makes the step `error` with
    the aggregated messages, driving the normal halt + terminal
    `{complete:true, success:false, error}` flow (AC-2).
    """
    import time

    t0 = time.perf_counter()
    try:
        ret = fn(*args, **kwargs)
        elapsed = int((time.perf_counter() - t0) * 1000)
        failure = task_failure_message(ret)
        if failure:
            return ProcessStepResult(
                step_id=step_id,
                status="error",
                elapsed_ms=elapsed,
                error=failure,
                result=None,
            )
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


def verify_spotify(playlist_id: str) -> None:
    """After a successful send, re-pull the target playlist from Spotify
    and refresh the stored track order in music.playlist_isrcs.

    This step actively verifies its effect. playlist_to_db can silently
    no-op (early return) when no usable Spotify client is available, so we
    (1) require a working client up front and (2) confirm
    music.playlist_isrcs was actually refreshed before reporting success.
    """
    client = get_spotify_client()
    if client is None or client.get("client") is None:
        raise RuntimeError(
            "verify_spotify: could not obtain a usable Spotify client "
            "(rate-limited or token unavailable) — cannot re-pull playlist details."
        )

    playlist_to_db(client, list_id=playlist_id)

    # Confirm the re-pull actually refreshed the stored order in the DB.
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