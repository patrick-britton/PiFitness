"""Pydantic schemas for the music domain."""

from pydantic import BaseModel, Field


class Track(BaseModel):
    pass


class Playlist(BaseModel):
    pass


class TrackRecommendation(BaseModel):
    pass

