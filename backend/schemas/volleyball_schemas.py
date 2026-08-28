"""
Volleyball Scorekeeping Schemas
===============================

Pydantic request models for the Beach Volleyball scorekeeping feature.

These mirror the frontend contract types in
`frontend/pifitness/src/lib/types/volleyball.ts`. Responses are returned as
plain dicts shaped by the router serializers (matching the contract exactly),
so only request bodies are strictly validated here.
"""

from typing import Literal
from pydantic import BaseModel, Field


VolleyballScoringTeam = Literal["SR", "OPPONENT"]


class VolleyballCreateGameRequest(BaseModel):
    """Request body for POST /api/sports/volleyball (create a game)."""
    team_b_name: str = Field(
        ..., min_length=1, max_length=120,
        description="Opponent team name (Team B). Team A is always 'SR' and is not prompted.",
    )


class VolleyballAddPointRequest(BaseModel):
    """Request body for POST /api/sports/volleyball/{id}/points."""
    scoring_team: VolleyballScoringTeam = Field(
        ..., description="Which team scored: 'SR' or 'OPPONENT'."
    )
