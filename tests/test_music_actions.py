import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient
from backend.main import app
from backend_functions.music_functions import album_image_retrieval
from backend_functions.file_handlers import album_art_path

client = TestClient(app)


def test_now_playing_skip_route_exists():
    with patch('backend.api.music._get_spotify_client', return_value=None):
        response = client.post("/api/music/now-playing/skip")
        assert response.status_code == 503


def test_now_playing_promote_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/promote")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert "No track" in data["message"]


def test_now_playing_promote_wrong_context():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackId": "tid1",
            "trackName": "Test Track",
            "context": {"relationshipType": "regular", "parentPlaylistId": None}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state):
        response = client.post("/api/music/now-playing/promote")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert "only applies to recommendations" in data["message"]


def test_now_playing_soft_reject_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/soft-reject")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False


def test_now_playing_hard_reject_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/hard-reject")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False


def test_now_playing_remove_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/remove")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False


def test_now_playing_remove_family_miss():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.sql_to_dict', return_value=[]):
        response = client.post("/api/music/now-playing/remove")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert data["message"] == "the playlist cannot be found"


def test_now_playing_rank_up_wrong_context():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "context": {"relationshipType": "recommendation", "playlistId": "pl123"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state):
        response = client.post("/api/music/now-playing/rank-up")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert "only applies to playlist tracks" in data["message"]


def test_now_playing_rank_down_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/rank-down")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False


def test_now_playing_add_to_playlist_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.post("/api/music/now-playing/add-to-playlist?playlist_id=pl123")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False


def test_now_playing_add_targets_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.get("/api/music/now-playing/add-targets")
        assert response.status_code == 200
        data = response.json()
        assert data["eligible"] is False
        assert data["playlists"] == []

def test_now_playing_add_targets_no_track():
    with patch('backend.api.music.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.get("/api/music/now-playing/add-targets")
        assert response.status_code == 200
        data = response.json()
        assert data["eligible"] is False
        assert data["playlists"] == []


def test_now_playing_add_targets_with_track():
    mock_state = {
        "playing": True,
        "track": {"isrc": "TEST123"}
    }
    mock_playlists = [
        {"playlist_id": "pl1", "playlist_name": "Playlist One"},
        {"playlist_id": "pl2", "playlist_name": "Playlist Two"},
    ]
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.get_playlists_not_containing_isrc', return_value=mock_playlists):
        response = client.get("/api/music/now-playing/add-targets")
        assert response.status_code == 200
        data = response.json()
        assert data["eligible"] is True
        assert len(data["playlists"]) == 2
        assert data["playlists"][0]["playlist_id"] == "pl1"


def test_now_playing_promote_full_flow():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackId": "tid1",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "recommendation", "parentPlaylistId": "parent1"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.add_isrc_to_local_playlist') as mock_add, \
         patch('backend.api.music._get_spotify_client', return_value=MagicMock()), \
         patch('backend.api.music.record_recommendation_decision') as mock_record, \
         patch('backend.api.music.remove_recommendation') as mock_remove, \
         patch('backend.api.music.add_into_current_ratings') as mock_seed:
        response = client.post("/api/music/now-playing/promote")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "added to playlist" in data["message"]
        mock_add.assert_called_once_with("parent1", "TEST123")
        mock_record.assert_called_once_with("parent1", "TEST123", was_promoted=True)
        mock_remove.assert_called_once_with("parent1", "TEST123")
        mock_seed.assert_called_once_with("parent1", "TEST123", 1500)


def test_now_playing_rank_up_full_flow():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.save_matchup_results') as mock_save:
        response = client.post("/api/music/now-playing/rank-up")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "ranked up" in data["message"]
        mock_save.assert_called_once_with(
            hd={"isrc": "TEST123", "playlistId": "pl123", "currentELO": 1500},
            ad=None,
            mr=2,
        )


def test_now_playing_rank_down_full_flow():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.save_matchup_results') as mock_save:
        response = client.post("/api/music/now-playing/rank-down")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "ranked down" in data["message"]
        mock_save.assert_called_once_with(
            hd={"isrc": "TEST123", "playlistId": "pl123", "currentELO": 1500},
            ad=None,
            mr=-2,
        )
def test_now_playing_rank_down_full_flow():
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.save_matchup_results') as mock_save:
        response = client.post("/api/music/now-playing/rank-down")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "ranked down" in data["message"]
        mock_save.assert_called_once_with(
            hd={"isrc": "TEST123", "playlistId": "pl123", "currentELO": 1500},
            ad=None,
            mr=-2,
        )


# ---------------------------------------------------------------------------
# T05: Recent plays rework + service status
# ---------------------------------------------------------------------------


def test_recent_plays_default_limit():
    """Recent plays returns {plays, scale} with default limit."""
    mock_rows = [
        {
            "isrc": "ISRC1",
            "lastPlayedAtUtc": "2026-09-01T00:00:00",
            "trackName": "Track One",
            "artistName": "Artist One",
            "playlistName": "Playlist One",
            "rating": 1500,
            "playcountLast30": 5,
            "playcountTotal": 20,
            "minRating": 1400,
            "maxRating": 1600,
            "maxPlaycountLast30": 10,
            "maxPlaycountTotal": 50,
        },
    ]
    with patch('backend.api.music.get_recent_plays', return_value=mock_rows):
        response = client.get("/api/music/recent-plays")
        assert response.status_code == 200
        data = response.json()
        assert "plays" in data
        assert "scale" in data
        assert len(data["plays"]) == 1
        assert data["plays"][0]["isrc"] == "ISRC1"
        assert data["scale"]["minRating"] == 1400
        assert data["scale"]["maxRating"] == 1600


def test_recent_plays_custom_limit():
    """Recent plays accepts custom limit within range."""
    with patch('backend.api.music.get_recent_plays', return_value=[]) as mock_get:
        response = client.get("/api/music/recent-plays?limit=50")
        assert response.status_code == 200
        mock_get.assert_called_once_with(limit=50)


def test_recent_plays_rejects_too_small():
    """Recent plays rejects limit < 10."""
    response = client.get("/api/music/recent-plays?limit=5")
    assert response.status_code == 422


def test_recent_plays_rejects_too_large():
    """Recent plays rejects limit > 100."""
    response = client.get("/api/music/recent-plays?limit=105")
    assert response.status_code == 422


def test_recent_plays_rejects_not_step():
    """Recent plays rejects limit not a multiple of 10."""
    response = client.get("/api/music/recent-plays?limit=15")
    assert response.status_code == 422


def test_recent_plays_empty():
    """Recent plays returns empty plays with baseline scale."""
    with patch('backend.api.music.get_recent_plays', return_value=[]):
        response = client.get("/api/music/recent-plays")
        assert response.status_code == 200
        data = response.json()
        assert data["plays"] == []
        assert data["scale"]["minRating"] == 1500
        assert data["scale"]["maxRating"] == 1500
        assert data["scale"]["maxPlaycountLast30"] == 0
        assert data["scale"]["maxPlaycountTotal"] == 0


def test_service_status_not_limited():
    """Service status reports not rate-limited."""
    with patch('backend.api.music.sql_rate_limited', return_value=False):
        response = client.get("/api/music/service-status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"]["rateLimited"] is False
        assert data["spotify"]["rateLimitClearedUtc"] is None


def test_service_status_limited():
    """Service status reports rate-limited with clearance time."""
    with patch('backend.api.music.sql_rate_limited', return_value=True), \
         patch('backend.api.music.one_sql_result', return_value="2026-09-02T00:00:00"):
        response = client.get("/api/music/service-status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"]["rateLimited"] is True
        assert data["spotify"]["rateLimitClearedUtc"] == "2026-09-02T00:00:00"


# ---------------------------------------------------------------------------
# T06: Album art serving
# ---------------------------------------------------------------------------


def test_album_art_cached():
    """Album art serves cached file without re-download."""
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(b"fake_jpeg_data")
        tmp_path = Path(f.name)
    with patch('backend.api.music.album_image_retrieval', return_value=str(tmp_path)):
        response = client.get(f"/api/music/album-art/test_album_id")
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/jpeg"
    tmp_path.unlink()


def test_album_art_not_found():
    """Album art returns 404 when image cannot be obtained."""
    with patch('backend.api.music.album_image_retrieval', return_value=None):
        response = client.get("/api/music/album-art/nonexistent_id")
        assert response.status_code == 404


def test_album_image_retrieval_cached():
    """album_image_retrieval returns filepath when file exists."""
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(b"fake")
        tmp_path = f.name
    album_id = os.path.basename(tmp_path).replace(".jpg", "")
    with patch('os.path.exists', return_value=True), \
         patch('os.path.join', return_value=tmp_path), \
         patch('backend_functions.file_handlers.album_art_path', return_value=os.path.dirname(tmp_path)):
        result = album_image_retrieval(album_id)
        assert result == tmp_path
    os.unlink(tmp_path)


def test_album_image_retrieval_no_client():
    """album_image_retrieval returns None when Spotify client unavailable and file not cached."""
    with patch('os.path.exists', return_value=False), \
         patch('backend_functions.music_functions.get_spotify_client', return_value=None):
        result = album_image_retrieval("nonexistent")
        assert result is None