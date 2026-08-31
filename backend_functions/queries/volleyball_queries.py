"""
Volleyball Scorekeeping Query Functions
=======================================

Backend CRUD + single-active guard for the Beach Volleyball scorekeeping
feature (design .features/designs_active/006-001_design.md).

Writes to the pre-built tables `volleyball.games` and `volleyball.points`.
No schema changes are required. Team A is always "SR" and is never stored;
only the opponent (team_b_name) is prompted at creation.

Event sourcing: the score is never stored. It is derived by counting
`volleyball.points` rows per team. `remove point` deletes that team's most
recently recorded point row and every score recalculates itself.

Lifecycle: active -> completed, or active -> (abandoned/deleted).
- `create_game` inserts a row with status='active' (single-active guard).
- `add_point` inserts a point (recorded_at = now(), server-assigned) and
  keeps `started_at` = MIN(recorded_at).
- `remove_last_point` deletes the most recent point of ONE team only
  (each side undoes its own last point) and recomputes `started_at`.
- `tag_last_point` (006-002) annotates the most recently recorded point
  (whichever team scored it) with a notable-play event_type; it never
  creates a point and overwriting the last point's tag is allowed.
- `end_game` sets status='completed' and `completed_at` = MAX(recorded_at).
- `abandon_game` deletes the game (points cascade).

Single-active invariant (block-and-guide): creating a game while another is
'active' raises VolleyballActiveGameExistsError carrying the blocking game,
so the API can surface it to the UI.
"""

from typing import Any, Dict, Optional, Sequence

from backend_functions.database_functions import (
    qec,
    sql_to_dict,
    sql_insert_returning,
    one_sql_result,
)


class VolleyballError(Exception):
    """Base error for volleyball scorekeeping operations."""


class VolleyballActiveGameExistsError(VolleyballError):
    """Raised when a game is created while another game is active."""

    def __init__(self, blocking_game: Optional[Dict[str, Any]]) -> None:
        self.blocking_game = blocking_game
        super().__init__("A volleyball game is already active.")


class VolleyballStateError(VolleyballError):
    """Raised when an action is invalid for the game's current status."""


class VolleyballNotFoundError(VolleyballError):
    """Raised when a referenced game does not exist."""


VALID_SCORING_TEAMS = ("SR", "OPPONENT")

# Notable-play tags writable onto a point (006-002).
VALID_EVENT_TYPES = ("Ace", "Block", "Spike", "Dive")

# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

_GAME_COLUMNS = """
            game_id,
            team_b_name,
            status,
            started_at,
            completed_at,
            label,
            notes,
            partner_number,
            partner_name,
            created_at
"""

_POINT_COLUMNS = """
            point_id,
            game_id,
            scoring_team,
            event_type,
            recorded_at,
            created_at
"""


def get_game(game_id: int) -> Optional[Dict[str, Any]]:
    """Return a single game row, or None if it does not exist."""
    sql = f"""
        SELECT
            {_GAME_COLUMNS}
        FROM volleyball.games
        WHERE game_id = %s
    """
    rows = sql_to_dict(sql, (game_id,))
    return rows[0] if rows else None


def get_active_game() -> Optional[Dict[str, Any]]:
    """Return the current 'active' game, or None when no game is active."""
    sql = f"""
        SELECT
            {_GAME_COLUMNS}
        FROM volleyball.games
        WHERE status = 'active'
        ORDER BY created_at DESC, game_id DESC
        LIMIT 1
    """
    rows = sql_to_dict(sql)
    return rows[0] if rows else None


def get_game_points(game_id: int) -> Sequence[Dict[str, Any]]:
    """Return all points for a game, in recorded order (oldest first)."""
    sql = f"""
        SELECT
            {_POINT_COLUMNS}
        FROM volleyball.points
        WHERE game_id = %s
        ORDER BY recorded_at, point_id
    """
    return sql_to_dict(sql, (game_id,))


def get_game_score(game_id: int) -> Dict[str, int]:
    """
    Derive the score for a game by COUNT-ing points per team (event sourcing).
    Aggregation is pushed down to SQL; no stored score is consulted.
    """
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE scoring_team = 'SR') AS sr,
            COUNT(*) FILTER (WHERE scoring_team = 'OPPONENT') AS opponent
        FROM volleyball.points
        WHERE game_id = %s
    """
    rows = sql_to_dict(sql, (game_id,))
    if not rows:
        return {"sr": 0, "opponent": 0}
    return {"sr": int(rows[0]["sr"] or 0), "opponent": int(rows[0]["opponent"] or 0)}


def get_game_detail(game_id: int) -> Optional[Dict[str, Any]]:
    """Return a game with its points (oldest first) and derived score."""
    game = get_game(game_id)
    if game is None:
        return None
    return {
        "game": game,
        "points": list(get_game_points(game_id)),
        "score": get_game_score(game_id),
    }



def list_game_history() -> Sequence[Dict[str, Any]]:
    """
    List completed games with their final derived score, sorted descending
    by completed_at (most recently finished first).
    """
    sql = """
        SELECT
            g.game_id,
            g.team_b_name,
            g.status,
            g.started_at,
            g.completed_at,
            g.label,
            g.notes,
            g.partner_number,
            g.partner_name,
            g.created_at,
            COUNT(p.*) FILTER (WHERE p.scoring_team = 'SR') AS sr,
            COUNT(p.*) FILTER (WHERE p.scoring_team = 'OPPONENT') AS opponent
        FROM volleyball.games g
        LEFT JOIN volleyball.points p ON p.game_id = g.game_id
        WHERE g.status = 'completed'
        GROUP BY g.game_id
        ORDER BY g.completed_at DESC, g.game_id DESC
    """
    rows = sql_to_dict(sql)
    return [
        {
            "game": {
                "game_id": r["game_id"],
                "team_b_name": r["team_b_name"],
                "status": r["status"],
                "started_at": r["started_at"],
                "completed_at": r["completed_at"],
                "label": r["label"],
                "notes": r["notes"],
                "partner_number": r["partner_number"],
                "partner_name": r["partner_name"],
                "created_at": r["created_at"],
            },
            "score": {"sr": int(r["sr"] or 0), "opponent": int(r["opponent"] or 0)},
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def create_game(
    team_b_name: str,
    partner_number: int,
    partner_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new game in status='active'. Team A is implicitly "SR".
    Records the SR side's partner for the match (006-002): the jersey
    number is mandatory (validated upstream by the API schema), the name
    is optional (None stored as NULL).

    Raises:
        VolleyballActiveGameExistsError: another game is already active
            (block-and-guide; the blocking game is carried on the exception).
    """
    blocking = get_active_game()
    if blocking is not None:
        raise VolleyballActiveGameExistsError(blocking)

    rows = sql_insert_returning(
        f"""
            INSERT INTO volleyball.games (team_b_name, status, partner_number, partner_name)
            VALUES (%s, 'active', %s, %s)
            RETURNING
                {_GAME_COLUMNS}
        """,
        (team_b_name, partner_number, partner_name),
    )
    if not rows:
        raise VolleyballError("Failed to create game.")
    return rows[0]


def add_point(
    game_id: int,
    scoring_team: str,
    event_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record one point for a team, optionally carrying a notable-play
    event_type written at creation (006-002, Bug T08-3: the UI holds a
    selected event and writes it alongside the next point). Only valid
    while the game is 'active'. The point's recorded_at is server-assigned
    (DEFAULT now()); started_at is kept equal to MIN(recorded_at) of the
    game's points.

    Raises:
        VolleyballNotFoundError: game does not exist.
        VolleyballStateError: game is not 'active', bad scoring_team, or bad
            event_type (must be one of VALID_EVENT_TYPES when provided).
    """
    if scoring_team not in VALID_SCORING_TEAMS:
        raise VolleyballStateError(
            f"Invalid scoring_team '{scoring_team}' (expected one of {VALID_SCORING_TEAMS})."
        )
    if event_type is not None and event_type not in VALID_EVENT_TYPES:
        raise VolleyballStateError(
            f"Invalid event_type '{event_type}' (expected one of {VALID_EVENT_TYPES})."
        )
    game = get_game(game_id)
    if game is None:
        raise VolleyballNotFoundError(f"Volleyball game {game_id} not found.")
    if game["status"] != "active":
        raise VolleyballStateError(
            f"Cannot add a point to game {game_id} in status "
            f"'{game['status']}' (expected 'active')."
        )

    rows = sql_insert_returning(
        f"""
            INSERT INTO volleyball.points (game_id, scoring_team, event_type)
            VALUES (%s, %s, %s)
            RETURNING
                {_POINT_COLUMNS}
        """,
        (game_id, scoring_team, event_type),
    )
    if not rows:
        raise VolleyballError("Failed to record point.")

    err = qec(
        """
            UPDATE volleyball.games
            SET started_at = (
                SELECT MIN(recorded_at) FROM volleyball.points WHERE game_id = %s
            )
            WHERE game_id = %s
        """,
        (game_id, game_id),
    )
    if err:
        raise VolleyballError(f"Failed to refresh started_at: {err}")
    return rows[0]


def remove_last_point(game_id: int, scoring_team: str) -> bool:
    """
    Undo one point: delete the most recent point recorded by ONE team only.
    The other side's last point is never touched. started_at is recomputed
    (NULL when no points remain). Only valid while the game is 'active'.

    Raises:
        VolleyballNotFoundError: game does not exist.
        VolleyballStateError: game is not 'active', or bad scoring_team, or
            the team has no points to remove.
    """
    if scoring_team not in VALID_SCORING_TEAMS:
        raise VolleyballStateError(
            f"Invalid scoring_team '{scoring_team}' (expected one of {VALID_SCORING_TEAMS})."
        )
    game = get_game(game_id)
    if game is None:
        raise VolleyballNotFoundError(f"Volleyball game {game_id} not found.")
    if game["status"] != "active":
        raise VolleyballStateError(
            f"Cannot remove a point from game {game_id} in status "
            f"'{game['status']}' (expected 'active')."
        )

    last = one_sql_result(
        """
            SELECT point_id
            FROM volleyball.points
            WHERE game_id = %s AND scoring_team = %s
            ORDER BY recorded_at DESC, point_id DESC
            LIMIT 1
        """,
        (game_id, scoring_team),
    )
    if last is None:
        raise VolleyballStateError(
            f"No points recorded for {scoring_team} in game {game_id}."
        )

    err = qec("DELETE FROM volleyball.points WHERE point_id = %s", (last,))
    if err:
        raise VolleyballError(f"Failed to remove point: {err}")

    err = qec(
        """
            UPDATE volleyball.games
            SET started_at = (
                SELECT MIN(recorded_at) FROM volleyball.points WHERE game_id = %s
            )
            WHERE game_id = %s
        """,
        (game_id, game_id),
    )
    if err:
        raise VolleyballError(f"Failed to refresh started_at: {err}")
    return True


def tag_last_point(game_id: int, event_type: str) -> Dict[str, Any]:
    """
    Tag the most recently recorded point of the game (whichever team scored
    it) with a notable-play event_type (006-002). This annotates an existing
    row — it never creates a point. Re-pressing an event button while the
    same point is still last overwrites its tag (overwrite allowed; there is
    deliberately no undo UI). Only valid while the game is 'active'.

    Raises:
        VolleyballNotFoundError: game does not exist, or has no points to
            tag (AC-11: no-points maps to HTTP 404).
        VolleyballStateError: game is not 'active', or bad event_type.
    """
    if event_type not in VALID_EVENT_TYPES:
        raise VolleyballStateError(
            f"Invalid event_type '{event_type}' (expected one of {VALID_EVENT_TYPES})."
        )
    game = get_game(game_id)
    if game is None:
        raise VolleyballNotFoundError(f"Volleyball game {game_id} not found.")
    if game["status"] != "active":
        raise VolleyballStateError(
            f"Cannot tag a point on game {game_id} in status "
            f"'{game['status']}' (expected 'active')."
        )

    last = one_sql_result(
        """
            SELECT point_id
            FROM volleyball.points
            WHERE game_id = %s
            ORDER BY recorded_at DESC, point_id DESC
            LIMIT 1
        """,
        (game_id,),
    )
    if last is None:
        # Contract (AC-11): no point to tag is a 404-class absence (the
        # resource this action targets does not exist), not a 409 conflict.
        raise VolleyballNotFoundError(
            f"No points recorded in game {game_id} — nothing to tag."
        )

    err = qec(
        "UPDATE volleyball.points SET event_type = %s WHERE point_id = %s",
        (event_type, last),
    )
    if err:
        raise VolleyballError(f"Failed to tag point: {err}")

    rows = sql_to_dict(
        f"""
            SELECT
                {_POINT_COLUMNS}
            FROM volleyball.points
            WHERE point_id = %s
        """,
        (last,),
    )
    if not rows:
        raise VolleyballError("Tagged point disappeared after update.")
    return rows[0]


def end_game(game_id: int) -> Dict[str, Any]:
    """
    End the game: status='completed' and completed_at = MAX(recorded_at)
    of the game's points (derived from the data, never the button-press
    time). completed_at is NULL when the game ended 0-0.

    Raises:
        VolleyballNotFoundError: game does not exist.
        VolleyballStateError: game is not 'active'.
    """
    game = get_game(game_id)
    if game is None:
        raise VolleyballNotFoundError(f"Volleyball game {game_id} not found.")
    if game["status"] != "active":
        raise VolleyballStateError(
            f"Cannot end game {game_id} in status '{game['status']}' (expected 'active')."
        )

    err = qec(
        """
            UPDATE volleyball.games
            SET status = 'completed',
                completed_at = (
                    SELECT MAX(recorded_at) FROM volleyball.points WHERE game_id = %s
                )
            WHERE game_id = %s
        """,
        (game_id, game_id),
    )
    if err:
        raise VolleyballError(f"Failed to end game: {err}")

    updated = get_game(game_id)
    if updated is None:
        raise VolleyballError("Game disappeared after ending.")
    return updated


def abandon_game(game_id: int) -> bool:
    """
    Abandon the game: delete the row entirely; its points are removed by
    FK ON DELETE CASCADE (OQ-3: the entire match is dropped).

    Raises:
        VolleyballNotFoundError: game does not exist.
    """
    game = get_game(game_id)
    if game is None:
        raise VolleyballNotFoundError(f"Volleyball game {game_id} not found.")

    err = qec("DELETE FROM volleyball.games WHERE game_id = %s", (game_id,))
    if err:
        raise VolleyballError(f"Failed to abandon game: {err}")
    return True

