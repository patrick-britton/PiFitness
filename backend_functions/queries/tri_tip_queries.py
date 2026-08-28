"""
Tri-tip Timer Query Functions
=============================

Backend CRUD + single-active guard for the Tri-tip Timer feature.

Writes to the pre-built tables `food.tri_tip_events` and
`food.tri_tip_readings` (described in .features/descriptions/005-001.md).
No schema changes are required.

Lifecycle: initiated -> active -> complete, or initiated/active -> (deleted).
- `initiate` inserts a row with status='initiated'.
- `place` inserts the first reading (internal fixed at 38F) then flips status
  to 'active' and sets `started_at` = MIN(recorded_at).
- subsequent readings are added while 'active'.
- `complete` sets status='complete' and `completed_at` = MAX(recorded_at).
- `abandon` deletes the event (readings cascade).

Single-active invariant (OQ-3: block-and-guide): initiating while another
event is 'initiated' or 'active' raises TriTipActiveEventExistsError carrying
the blocking event, so the API can surface it to the UI.
"""

from typing import Any, Dict, Optional, Sequence

from backend_functions.database_functions import (
    qec,
    sql_to_dict,
    sql_insert_returning,
    one_sql_result,
)


class TriTipError(Exception):
    """Base error for tri-tip timer operations."""


class TriTipActiveEventExistsError(TriTipError):
    """Raised when an initiate is attempted while another event is in progress."""

    def __init__(self, blocking_event: Optional[Dict[str, Any]]) -> None:
        self.blocking_event = blocking_event
        super().__init__("A tri-tip event is already in progress.")


class TriTipStateError(TriTipError):
    """Raised when an action is invalid for the event's current status."""


class TriTipNotFoundError(TriTipError):
    """Raised when a referenced event does not exist."""


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def get_tri_tip_event(event_id: int) -> Optional[Dict[str, Any]]:
    """Return a single event row, or None if it does not exist."""
    sql = """
        SELECT
            tri_tip_id,
            weight_lbs,
            shape,
            target_internal_temp_f,
            status,
            started_at,
            completed_at,
            label,
            notes,
            created_at
        FROM food.tri_tip_events
        WHERE tri_tip_id = %s
    """
    rows = sql_to_dict(sql, (event_id,))
    return rows[0] if rows else None


def list_tri_tip_events(limit: int = 100) -> Sequence[Dict[str, Any]]:
    """List events, most recently created first."""
    sql = """
        SELECT
            tri_tip_id,
            weight_lbs,
            shape,
            target_internal_temp_f,
            status,
            started_at,
            completed_at,
            label,
            notes,
            created_at
        FROM food.tri_tip_events
        ORDER BY created_at DESC, tri_tip_id DESC
        LIMIT %s
    """
    return sql_to_dict(sql, (limit,))


def get_event_readings(event_id: int) -> Sequence[Dict[str, Any]]:
    """Return all readings for an event, ordered oldest first."""
    sql = """
        SELECT
            reading_id,
            tri_tip_id,
            recorded_at,
            grill_temp_f,
            internal_temp_f,
            note,
            created_at
        FROM food.tri_tip_readings
        WHERE tri_tip_id = %s
        ORDER BY recorded_at ASC, reading_id ASC
    """
    return sql_to_dict(sql, (event_id,))


def get_active_event() -> Optional[Dict[str, Any]]:
    """Return the current in-progress event (initiated or active), if any."""
    sql = """
        SELECT
            tri_tip_id,
            weight_lbs,
            shape,
            target_internal_temp_f,
            status,
            started_at,
            completed_at,
            label,
            notes,
            created_at
        FROM food.tri_tip_events
        WHERE status IN ('initiated', 'active')
        ORDER BY tri_tip_id ASC
        LIMIT 1
    """
    rows = sql_to_dict(sql)
    return rows[0] if rows else None


def _blocking_event() -> Optional[Dict[str, Any]]:
    """The in-progress event that would block a new initiate, if any."""
    return get_active_event()


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def initiate_tri_tip(weight_lbs: float, shape: str) -> Dict[str, Any]:
    """
    Create an initiated event.

    Raises:
        TriTipActiveEventExistsError: if an event is already 'initiated'/'active'.
    """
    blocking = _blocking_event()
    if blocking is not None:
        raise TriTipActiveEventExistsError(blocking)

    sql = """
        INSERT INTO food.tri_tip_events (weight_lbs, shape, status)
        VALUES (%s, %s, 'initiated')
        RETURNING
            tri_tip_id,
            weight_lbs,
            shape,
            target_internal_temp_f,
            status,
            started_at,
            completed_at,
            label,
            notes,
            created_at
    """
    rows = sql_insert_returning(sql, (weight_lbs, shape))
    if not rows:
        raise TriTipError("Failed to create tri-tip event.")
    return rows[0]


def place_tri_tip(event_id: int, grill_temp_f: float) -> Dict[str, Any]:
    """
    Place the meat: record the first reading (internal fixed at 38F), flip the
    event to 'active', and set started_at = MIN(recorded_at).

    Raises:
        TriTipNotFoundError: event does not exist.
        TriTipStateError: event is not 'initiated'.
    """
    event = get_tri_tip_event(event_id)
    if event is None:
        raise TriTipNotFoundError(f"Tri-tip event {event_id} not found.")
    if event["status"] != "initiated":
        raise TriTipStateError(
            f"Cannot place meat on event {event_id} in status "
            f"'{event['status']}' (expected 'initiated')."
        )

    rows = sql_insert_returning(
        """
            INSERT INTO food.tri_tip_readings (tri_tip_id, grill_temp_f, internal_temp_f)
            VALUES (%s, %s, %s)
            RETURNING
                reading_id,
                tri_tip_id,
                recorded_at,
                grill_temp_f,
                internal_temp_f,
                note,
                created_at
        """,
        (event_id, grill_temp_f, 38.0),
    )
    if not rows:
        raise TriTipError("Failed to record placement reading.")

    # Flip status to active; started_at = MIN(recorded_at) of this event.
    update_sql = """
        UPDATE food.tri_tip_events
        SET status = 'active',
            started_at = (
                SELECT MIN(recorded_at)
                FROM food.tri_tip_readings
                WHERE tri_tip_id = %s
            )
        WHERE tri_tip_id = %s
    """
    err = qec(update_sql, (event_id, event_id))
    if err:
        raise TriTipError(f"Failed to activate event: {err}")

    updated = get_tri_tip_event(event_id)
    if updated is None:
        raise TriTipError("Event disappeared after placement.")
    return updated


def add_tri_tip_reading(
    event_id: int,
    grill_temp_f: float,
    internal_temp_f: float,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record a reading for an event. Only valid while the event is 'active'.

    Raises:
        TriTipNotFoundError: event does not exist.
        TriTipStateError: event is not 'active'.
    """
    event = get_tri_tip_event(event_id)
    if event is None:
        raise TriTipNotFoundError(f"Tri-tip event {event_id} not found.")
    if event["status"] != "active":
        raise TriTipStateError(
            f"Cannot record a reading on event {event_id} in status "
            f"'{event['status']}' (expected 'active')."
        )

    rows = sql_insert_returning(
        """
            INSERT INTO food.tri_tip_readings
                (tri_tip_id, grill_temp_f, internal_temp_f, note)
            VALUES (%s, %s, %s, %s)
            RETURNING
                reading_id,
                tri_tip_id,
                recorded_at,
                grill_temp_f,
                internal_temp_f,
                note,
                created_at
        """,
        (event_id, grill_temp_f, internal_temp_f, note),
    )
    if not rows:
        raise TriTipError("Failed to record reading.")
    return rows[0]


def complete_tri_tip(event_id: int) -> Dict[str, Any]:
    """
    Pull the meat: set status='complete' and completed_at = MAX(recorded_at).

    Raises:
        TriTipNotFoundError: event does not exist.
        TriTipStateError: event has no readings / is not 'active'.
    """
    event = get_tri_tip_event(event_id)
    if event is None:
        raise TriTipNotFoundError(f"Tri-tip event {event_id} not found.")

    max_recorded = one_sql_result(
        "SELECT MAX(recorded_at) FROM food.tri_tip_readings WHERE tri_tip_id = %s",
        (event_id,),
    )
    if max_recorded is None:
        raise TriTipStateError(
            f"Cannot complete event {event_id}: no readings recorded."
        )

    update_sql = """
        UPDATE food.tri_tip_events
        SET status = 'complete',
            completed_at = (
                SELECT MAX(recorded_at)
                FROM food.tri_tip_readings
                WHERE tri_tip_id = %s
            )
        WHERE tri_tip_id = %s
    """
    err = qec(update_sql, (event_id, event_id))
    if err:
        raise TriTipError(f"Failed to complete event: {err}")

    updated = get_tri_tip_event(event_id)
    if updated is None:
        raise TriTipError("Event disappeared after completion.")
    return updated


def abandon_tri_tip(event_id: int) -> bool:
    """
    Abandon this tri-tip and all recorded readings (FK cascade).

    Raises:
        TriTipNotFoundError: event does not exist.
    """
    event = get_tri_tip_event(event_id)
    if event is None:
        raise TriTipNotFoundError(f"Tri-tip event {event_id} not found.")

    err = qec(
        "DELETE FROM food.tri_tip_events WHERE tri_tip_id = %s",
        (event_id,),
    )
    if err:
        raise TriTipError(f"Failed to abandon event: {err}")
    return True