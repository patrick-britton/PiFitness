"""
Volleyball Scorekeeping Schemas
===============================

Pydantic request models for the Beach Volleyball scorekeeping feature.

These mirror the frontend contract types in
`frontend/pifitness/src/lib/types/volleyball.ts`. Responses are returned as
plain dicts shaped by the router serializers (matching the contract exactly),
so only request bodies are strictly validated here.
"""

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


VolleyballScoringTeam = Literal["SR", "OPPONENT"]

# Notable-play tags writable onto a point (006-002; mirrors the frontend
# VolleyballEventType union and the VALID_EVENT_TYPES guard in
# volleyball_queries).
VolleyballEventType = Literal["Ace", "Block", "Spike", "Dive"]


class VolleyballCreateGameRequest(BaseModel):
    """Request body for POST /api/sports/volleyball (create a game)."""
    team_b_name: str = Field(
        ..., min_length=1, max_length=120,
        description="Opponent team name (Team B). Team A is always 'SR' and is not prompted.",
    )
    partner_number: int = Field(
        ..., ge=0,
        description="SR side's partner jersey number (mandatory; 006-002).",
    )
    partner_name: Optional[str] = Field(
        None, min_length=1, max_length=120,
        description="SR side's partner name (optional; 006-002). The UI sends null when absent.",
    )


class VolleyballAddPointRequest(BaseModel):
    """Request body for POST /api/sports/volleyball/{id}/points."""
    scoring_team: VolleyballScoringTeam = Field(
        ..., description="Which team scored: 'SR' or 'OPPONENT'."
    )
    event_type: Optional[VolleyballEventType] = Field(
        None,
        description="Optional notable-play tag written onto this point at creation (006-002).",
    )


class VolleyballTagEventRequest(BaseModel):
    """Request body for POST /api/sports/volleyball/{id}/points/latest/event (006-002)."""
    event_type: VolleyballEventType = Field(
        ...,
        description="Notable-play tag written onto the most recently recorded point (either team's).",
    )


class VolleyballMessageRequest(BaseModel):
    """Request body for POST /api/sports/volleyball/{id}/message (006-003)."""
    message: str = Field(
        ..., min_length=1, max_length=240, strip_whitespace=True,
        description="Scorekeeper text for the log as 'Scorekeeper: {message}' "
                    "(blank after trim -> 422; mirrors GAME_EVENT_MESSAGE_MAX).",
    )

    @field_validator("message", mode="before")
    @classmethod
    def _trim_message(cls, value: str) -> str:
        """Trim so a blank-after-trim message fails min_length -> 422 (T04 verify / AC-3)."""
        return value.strip() if isinstance(value, str) else value
