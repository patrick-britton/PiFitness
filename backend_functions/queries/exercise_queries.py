"""
Exercise Timer Query Functions
===============================

Backend CRUD + per-timer summary reads for the Exercise Timer feature
(design .features/designs_active/007-001_design.md).

Writes/reads the pre-built tables `exercises.exercise_timers` (master data)
and `exercises.exercise_attempts` (one row per saved Start/Stop cycle).
No schema changes are required; the tables already exist empty as specced.

Master data lifecycle: `exercise_timers` rows are independently editable
(create/update/delete). The unique index on `LOWER(name)` makes case-insensitive
duplicate names a DB-level constraint; we pre-check it to surface a friendly
conflict before the INSERT.

Attempts:
- `create_attempt` writes exactly one `exercise_attempts` row per Start/Stop
  cycle, only after the user confirms the save prompt. It snapshots
  `interval_seconds_used` from the timer's interval in effect during the run
  (editing a timer later must not re-pace historical attempts).
- Deleting a timer (OQ-1, human-approved) wipes its history too. Because the FK
  is `ON DELETE RESTRICT`, we delete that exercise's `exercise_attempts` rows
  first, then the `exercise_timers` row, in a single transaction — scoped
  strictly by `exercise_id`, so no other timer or history is ever affected.

Exceptions:
    ExerciseError             base
    ExerciseNotFoundError      referenced timer does not exist
    ExerciseNameConflictError  case-insensitive duplicate name
    ExerciseValidationError    invalid attempt counts (total < paced)
"""

from typing import Any, Dict, Optional, Sequence

from backend_functions.database_functions import (
    get_conn,
    one_sql_result,
    qec,
    sql_insert_returning,
    sql_to_dict,
)


class ExerciseError(Exception):
    """Base error for exercise timer operations."""


class ExerciseNotFoundError(ExerciseError):
    """Raised when a referenced exercise timer does not exist."""


class ExerciseNameConflictError(ExerciseError):
    """Raised when a create/update would violate the LOWER(name) unique index."""


class ExerciseValidationError(ExerciseError):
    """Raised when an attempt payload violates a CHECK constraint (total >= paced)."""


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

_TIMER_COLUMNS = """
            exercise_id,
            name,
            interval_seconds,
            notes,
            created_at,
            updated_at
"""

_ATTEMPT_COLUMNS = """
            attempt_id,
            exercise_id,
            interval_seconds_used,
            started_at,
            ended_at,
            paced_count,
            total_count,
            notes,
            created_at
"""


def get_exercise_timer(exercise_id: int) -> Optional[Dict[str, Any]]:
    """Return a single timer row, or None if it does not exist."""
    sql = f"""
        SELECT
            {_TIMER_COLUMNS}
        FROM exercises.exercise_timers
        WHERE exercise_id = %s
    """
    rows = sql_to_dict(sql, (exercise_id,))
    return rows[0] if rows else None


def get_last_attempt(exercise_id: int) -> Optional[Dict[str, Any]]:
    """
    Return the most recent attempt for a timer (the one the live progress bar
    calibrates against), or None if the timer has no attempts yet.
    Uses the `(exercise_id, started_at DESC)` index for a fast direct lookup.
    """
    sql = f"""
        SELECT
            {_ATTEMPT_COLUMNS}
        FROM exercises.exercise_attempts
        WHERE exercise_id = %s
        ORDER BY started_at DESC, attempt_id DESC
        LIMIT 1
    """
    rows = sql_to_dict(sql, (exercise_id,))
    return rows[0] if rows else None
def list_exercise_summaries() -> Sequence[Dict[str, Any]]:
    """
    List every timer enriched with per-timer attempt aggregates, ordered by name.

    Derived fields (not columns):
      last_attempt_total_count / last_attempt_paced_count  -> most recent attempt
      highest_score                                       -> MAX(total_count)  (OQ-2)
      highest_paced_count                                 -> MAX(paced_count)  (OQ-2)

    Aggregation is pushed down to SQL (no in-memory rollups); the lateral
    last-attempt lookup is served by the `(exercise_id, started_at DESC)` index.
    """
    sql = """
        SELECT
            t.exercise_id,
            t.name,
            t.interval_seconds,
            t.notes,
            t.created_at,
            t.updated_at,
            la.total_count  AS last_attempt_total_count,
            la.paced_count  AS last_attempt_paced_count,
            ha.highest_score,
            ha.highest_paced_count
        FROM exercises.exercise_timers t
        LEFT JOIN LATERAL (
            SELECT a.total_count, a.paced_count
            FROM exercises.exercise_attempts a
            WHERE a.exercise_id = t.exercise_id
            ORDER BY a.started_at DESC, a.attempt_id DESC
            LIMIT 1
        ) la ON true
        LEFT JOIN LATERAL (
            SELECT
                MAX(a.total_count) AS highest_score,
                MAX(a.paced_count) AS highest_paced_count
            FROM exercises.exercise_attempts a
            WHERE a.exercise_id = t.exercise_id
        ) ha ON true
        ORDER BY t.name
    """
    return sql_to_dict(sql)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def create_exercise(
    name: str,
    interval_seconds: float,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new exercise timer.

    Raises:
        ExerciseNameConflictError: case-insensitive duplicate name.
        ExerciseError: the INSERT failed.
    """
    _assert_name_available(name)

    rows = sql_insert_returning(
        f"""
            INSERT INTO exercises.exercise_timers (name, interval_seconds, notes)
            VALUES (%s, %s, %s)
            RETURNING {_TIMER_COLUMNS}
        """,
        (name, interval_seconds, notes),
    )
    if not rows:
        raise ExerciseError("Failed to create exercise timer.")
    return rows[0]


def update_exercise(
    exercise_id: int,
    name: Optional[str] = None,
    interval_seconds: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update an existing timer's editable master-data fields. `updated_at` is
    bumped to server now() by the UPDATE itself.

    Only the provided fields change; omitted fields keep their current values
    (partial update).

    Raises:
        ExerciseNotFoundError: timer does not exist.
        ExerciseNameConflictError: new name collides with another timer.
        ExerciseError: the UPDATE failed.
    """
    current = get_exercise_timer(exercise_id)
    if current is None:
        raise ExerciseNotFoundError(f"Exercise timer {exercise_id} not found.")

    new_name = name if name is not None else current["name"]
    new_interval = (
        interval_seconds
        if interval_seconds is not None
        else current["interval_seconds"]
    )
    new_notes = (
        notes
        if notes is not None
        else current["notes"] if current["notes"] is not None else None
    )

    # Case-insensitive conflict against ANY OTHER timer (never self).
    conflict = one_sql_result(
        """
            SELECT 1
            FROM exercises.exercise_timers
            WHERE LOWER(name) = LOWER(%s)
              AND exercise_id <> %s
            LIMIT 1
        """,
        (new_name, exercise_id),
    )
    if conflict:
        raise ExerciseNameConflictError(f"An exercise named '{new_name}' already exists.")

    err = qec(
        """
            UPDATE exercises.exercise_timers
            SET name = %s,
                interval_seconds = %s,
                notes = %s,
                updated_at = now()
            WHERE exercise_id = %s
        """,
        (new_name, new_interval, new_notes, exercise_id),
    )
    if err:
        raise ExerciseError(f"Failed to update exercise timer: {err[0]}")

    updated = get_exercise_timer(exercise_id)
    if updated is None:
        raise ExerciseError("Timer disappeared after update.")
    return updated
def delete_exercise(exercise_id: int) -> bool:
    """
    Permanently delete a timer and ALL of its attempt history in one atomic
    transaction (OQ-1).

    The attempts are deleted first because `exercise_attempts.exercise_id` has
    `ON DELETE RESTRICT` — deleting the timer row while attempts still reference
    it would be rejected. If either DELETE fails, the whole operation rolls back.
    The operation is scoped strictly by `exercise_id`; no other timer or history
    is touched.

    Raises:
        ExerciseNotFoundError: timer does not exist.
        ExerciseError: the transaction failed.
    """
    current = get_exercise_timer(exercise_id)
    if current is None:
        raise ExerciseNotFoundError(f"Exercise timer {exercise_id} not found.")

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM exercises.exercise_attempts WHERE exercise_id = %s",
            (exercise_id,),
        )
        cur.execute(
            "DELETE FROM exercises.exercise_timers WHERE exercise_id = %s",
            (exercise_id,),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        if conn is not None:
            conn.rollback()
        raise ExerciseError(f"Failed to delete exercise timer: {e}")
    finally:
        if conn is not None:
            conn.close()


def create_attempt(
    exercise_id: int,
    started_at: str,
    ended_at: str,
    interval_seconds_used: float,
    paced_count: int,
    total_count: int,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record one confirmed attempt. Called only after the user confirms the save
    prompt — the in-progress run lives entirely in the client, never here.

    Persists the `interval_seconds_used` snapshot so later edits to the timer's
    interval cannot re-pace historical attempts (FR-10).

    Raises:
        ExerciseNotFoundError: timer does not exist.
        ExerciseValidationError: paced_count > total_count (CHECK constraint).
        ExerciseError: the INSERT failed.
    """
    current = get_exercise_timer(exercise_id)
    if current is None:
        raise ExerciseNotFoundError(f"Exercise timer {exercise_id} not found.")

    if paced_count < 0 or total_count < paced_count:
        raise ExerciseValidationError(
            "paced_count must be >= 0 and total_count must be >= paced_count."
        )

    rows = sql_insert_returning(
        f"""
            INSERT INTO exercises.exercise_attempts (
                exercise_id,
                interval_seconds_used,
                started_at,
                ended_at,
                paced_count,
                total_count,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING {_ATTEMPT_COLUMNS}
        """,
        (
            exercise_id,
            interval_seconds_used,
            started_at,
            ended_at,
            paced_count,
            total_count,
            notes,
        ),
    )
    if not rows:
        raise ExerciseError("Failed to create exercise attempt.")
    return rows[0]


def _assert_name_available(name: str) -> None:
    """Raise ExerciseNameConflictError if LOWER(name) is already taken."""
    conflict = one_sql_result(
        "SELECT 1 FROM exercises.exercise_timers WHERE LOWER(name) = LOWER(%s) LIMIT 1",
        (name,),
    )
    if conflict:
        raise ExerciseNameConflictError(f"An exercise named '{name}' already exists.")