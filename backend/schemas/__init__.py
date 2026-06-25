"""
PiFitness API Schemas
====================

Pydantic models for the PiFitness API that mirror the database structure.
These models define the API contract for request/response payloads.
"""

from .activity_schemas import Activity, ActivityDetail, ActivityMetric, Segment
from .health_schemas import HeartRate, SleepData, BodyComposition
from .music_schemas import Track, TrackDetail, Playlist, PlaylistDetail, TrackRecommendation
from .admin_schemas import TaskExecution, TaskConfig, DBStats

__all__ = [
    'Activity', 'ActivityDetail', 'ActivityMetric', 'Segment',
    'HeartRate', 'SleepData', 'BodyComposition',
    'Track', 'TrackDetail', 'Playlist', 'PlaylistDetail', 'TrackRecommendation',
    'TaskExecution', 'TaskConfig', 'DBStats'
]
