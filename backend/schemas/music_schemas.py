"""
Music Schemas
=============

Pydantic models for music-related data structures.
These models now exactly match the database schema based on validation.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Track(BaseModel):
    """Music track information matching the music.all_tracks table (15 fields)
    Exact match to database columns: track_id, track_name, track_name_clean, track_name_search,
    track_isrc, duration_ms, artist_id, artist_display_name, album_id, release_date,
    available_in_us, total_markets, popularity, updated_at_utc, id_synced_at_utc"""
    track_id: str = Field(..., description="Unique track identifier (Spotify ID)")
    track_name: str = Field(..., description="Name of the track")
    track_name_clean: Optional[str] = Field(None, description="Clean track name (lowercase, no special chars)")
    track_name_search: Optional[str] = Field(None, description="Search-optimized track name")
    track_isrc: Optional[str] = Field(None, description="International Standard Recording Code")
    duration_ms: int = Field(..., description="Duration in milliseconds")
    artist_id: str = Field(..., description="Artist identifier")
    artist_display_name: Optional[str] = Field(None, description="Artist display name")
    album_id: Optional[str] = Field(None, description="Album identifier")
    release_date: Optional[str] = Field(None, description="Release date (YYYY-MM-DD)")
    available_in_us: Optional[bool] = Field(None, description="Whether available in US market")
    total_markets: Optional[int] = Field(None, description="Total number of markets available")
    popularity: Optional[int] = Field(None, description="Popularity score (0-100)")
    updated_at_utc: Optional[str] = Field(None, description="When track was last updated (ISO timestamp)")
    id_synced_at_utc: Optional[str] = Field(None, description="When ID was synced (ISO timestamp)")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "track_id": "123456789",
                "track_name": "Sample Track",
                "track_name_clean": "sample track",
                "track_name_search": "sample track",
                "track_isrc": "USABC1234567",
                "duration_ms": 180000,
                "artist_id": "artist123",
                "artist_display_name": "Sample Artist",
                "album_id": "album456",
                "release_date": "2023-01-15",
                "available_in_us": True,
                "total_markets": 50,
                "popularity": 75,
                "updated_at_utc": "2023-06-24T09:00:00",
                "id_synced_at_utc": "2023-06-24T08:00:00"
            }
        }

class TrackDetail(BaseModel):
    """Extended music track information combining all_tracks with audio features (32 fields)
    This represents a JOIN of music.all_tracks + music.track_recommendations + other views"""
    track_id: str = Field(..., description="Unique track identifier (Spotify ID)")
    track_name: str = Field(..., description="Name of the track")
    artist_id: str = Field(..., description="Artist identifier")
    artist_name: str = Field(..., description="Artist name")
    album_id: Optional[str] = Field(None, description="Album identifier")
    album_name: Optional[str] = Field(None, description="Album name")
    duration_ms: int = Field(..., description="Duration in milliseconds")
    popularity: Optional[int] = Field(None, description="Popularity score (0-100)")
    explicit: Optional[bool] = Field(None, description="Whether track is explicit")
    danceability: Optional[float] = Field(None, description="Danceability score (0-1)")
    energy: Optional[float] = Field(None, description="Energy score (0-1)")
    key: Optional[int] = Field(None, description="Musical key (0-11)")
    loudness: Optional[float] = Field(None, description="Loudness in dB")
    mode: Optional[int] = Field(None, description="Mode (0=minor, 1=major)")
    speechiness: Optional[float] = Field(None, description="Speechiness score (0-1)")
    acousticness: Optional[float] = Field(None, description="Acousticness score (0-1)")
    instrumentalness: Optional[float] = Field(None, description="Instrumentalness score (0-1)")
    liveness: Optional[float] = Field(None, description="Liveness score (0-1)")
    valence: Optional[float] = Field(None, description="Valence score (0-1)")
    tempo: Optional[float] = Field(None, description="Tempo in BPM")
    time_signature: Optional[int] = Field(None, description="Time signature")
    isrc: Optional[str] = Field(None, description="International Standard Recording Code")
    added_at: Optional[datetime] = Field(None, description="When track was added to library")
    artist_display_name: Optional[str] = Field(None, description="Artist display name")
    available_in_us: Optional[bool] = Field(None, description="Whether available in US")
    id_synced_at_utc: Optional[datetime] = Field(None, description="When ID was synced")
    release_date: Optional[datetime] = Field(None, description="Release date")
    total_markets: Optional[int] = Field(None, description="Total markets available")
    track_isrc: Optional[str] = Field(None, description="Track ISRC")
    track_name_clean: Optional[str] = Field(None, description="Clean track name")
    track_name_search: Optional[str] = Field(None, description="Search track name")
    updated_at_utc: Optional[datetime] = Field(None, description="When track was last updated")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "track_id": "123456789",
                "track_name": "Sample Track",
                "artist_id": "artist123",
                "artist_name": "Sample Artist",
                "album_id": "album456",
                "album_name": "Sample Album",
                "duration_ms": 180000,
                "popularity": 75,
                "explicit": False,
                "danceability": 0.75,
                "energy": 0.85,
                "key": 5,
                "loudness": -8.5,
                "mode": 1,
                "speechiness": 0.05,
                "acousticness": 0.15,
                "instrumentalness": 0.01,
                "liveness": 0.12,
                "valence": 0.65,
                "tempo": 120.5,
                "time_signature": 4,
                "isrc": "USABC1234567",
                "added_at": "2023-06-24T07:30:00",
                "artist_display_name": "Sample Artist",
                "available_in_us": True,
                "id_synced_at_utc": "2023-06-24T08:00:00",
                "release_date": "2023-01-15T00:00:00",
                "total_markets": 50,
                "track_isrc": "USABC1234567",
                "track_name_clean": "sample track",
                "track_name_search": "sample track",
                "updated_at_utc": "2023-06-24T09:00:00"
            }
        }

class Playlist(BaseModel):
    """Playlist information matching the music.playlist_config table"""
    playlist_id: str = Field(..., description="Unique playlist identifier (Spotify ID)")
    playlist_name: str = Field(..., description="Name of the playlist")
    playlist_description: Optional[str] = Field(None, description="Playlist description")
    track_count: int = Field(..., description="Total number of tracks")
    last_verified_utc: Optional[datetime] = Field(None, description="When playlist was last verified")
    is_active: bool = Field(..., description="Whether playlist is active")
    auto_shuffle: Optional[bool] = Field(None, description="Whether auto shuffle is enabled")
    last_auto_shuffled_utc: Optional[datetime] = Field(None, description="When auto shuffle was last run")
    last_synced_utc: Optional[datetime] = Field(None, description="When playlist was last synced")
    make_recs: Optional[bool] = Field(None, description="Whether to make recommendations")
    manual_shuffle: Optional[bool] = Field(None, description="Whether manual shuffle is enabled")
    minutes_to_sync: Optional[int] = Field(None, description="Minutes to sync")
    prior_track_count: Optional[int] = Field(None, description="Prior track count")
    randomness_weight: Optional[float] = Field(None, description="Randomness weight")
    ratings_weight: Optional[float] = Field(None, description="Ratings weight")
    recency_weight: Optional[float] = Field(None, description="Recency weight")
    seeds_only: Optional[bool] = Field(None, description="Whether to use seeds only")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "playlist_id": "playlist123",
                "playlist_name": "My Workout Mix",
                "playlist_description": "High energy workout tracks",
                "track_count": 50,
                "last_verified_utc": "2023-06-24T07:30:00",
                "is_active": True,
                "auto_shuffle": True,
                "last_auto_shuffled_utc": "2023-06-24T08:00:00",
                "last_synced_utc": "2023-06-24T09:00:00",
                "make_recs": True,
                "manual_shuffle": False,
                "minutes_to_sync": 30,
                "prior_track_count": 45,
                "randomness_weight": 0.3,
                "ratings_weight": 0.5,
                "recency_weight": 0.2,
                "seeds_only": False
            }
        }

class PlaylistDetail(BaseModel):
    """Detailed playlist information with tracks"""
    playlist_id: str = Field(..., description="Unique playlist identifier")
    track_id: str = Field(..., description="Track identifier")
    position: int = Field(..., description="Position in playlist")
    added_at: datetime = Field(..., description="When track was added to playlist")
    added_by: Optional[str] = Field(None, description="User who added the track")
    track_name: str = Field(..., description="Name of the track")
    artist_name: str = Field(..., description="Name of the artist")

    class Config:
        from_attributes = True

class TrackRecommendation(BaseModel):
    """Track recommendation with ELO rating"""
    recommendation_id: int = Field(..., description="Unique recommendation identifier")
    track_id: str = Field(..., description="Track identifier")
    elo_rating: float = Field(..., description="Current ELO rating")
    initial_elo: float = Field(..., description="Initial ELO rating")
    wins: int = Field(..., description="Number of times track has won comparisons")
    losses: int = Field(..., description="Number of times track has lost comparisons")
    last_played: Optional[datetime] = Field(None, description="When track was last played")
    play_count: int = Field(..., description="Number of times track has been played")
    skip_count: int = Field(..., description="Number of times track has been skipped")
    rating: Optional[float] = Field(None, description="User rating (1-5)")
    recency_score: float = Field(..., description="Recency score for recommendation")
    randomness_score: float = Field(..., description="Randomness score for recommendation")
    last_updated: datetime = Field(..., description="When recommendation was last updated")

    class Config:
        from_attributes = True

class ListeningHistory(BaseModel):
    """User listening history"""
    history_id: int = Field(..., description="Unique history record identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    track_id: str = Field(..., description="Track identifier")
    play_timestamp: datetime = Field(..., description="When track was played")
    duration_ms: Optional[int] = Field(None, description="Duration played in milliseconds")
    source: Optional[str] = Field(None, description="Source of playback")
    device: Optional[str] = Field(None, description="Device used for playback")

    class Config:
        from_attributes = True

class SmartShuffleConfig(BaseModel):
    """Smart shuffle configuration"""
    config_id: int = Field(..., description="Unique configuration identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    elo_weight: float = Field(..., description="Weight for ELO rating (0-1)")
    recency_weight: float = Field(..., description="Weight for recency (0-1)")
    randomness_weight: float = Field(..., description="Weight for randomness (0-1)")
    min_elo: Optional[float] = Field(None, description="Minimum ELO rating to include")
    max_elo: Optional[float] = Field(None, description="Maximum ELO rating to include")
    time_window_days: Optional[int] = Field(None, description="Time window for recency in days")
    playlist_size: int = Field(..., description="Target playlist size")
    created_at: datetime = Field(..., description="When configuration was created")
    updated_at: datetime = Field(..., description="When configuration was last updated")

    class Config:
        from_attributes = True