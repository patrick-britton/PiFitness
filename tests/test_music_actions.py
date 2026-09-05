import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, call

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


def test_now_playing_remove_full_flow():
    """Remove succeeds when family is found and track exists in playlist (FR-6/AC-7).

    This test covers the success path that was previously untested — it exercises
    the sql_to_list call with params that caused the 500 TypeError bug. It also
    verifies that Spotify removal targets every family playlist where the track is
    present locally (child AND parent), not just the currently-playing child.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "context": {"relationshipType": "regular", "playlistId": "pl123"},
        },
    }
    # Child playlist pl123 (currently playing) + parent playlist plParent
    family_rows = [{"child_playlist_id": "pl123"}, {"child_playlist_id": "plParent"}]
    name_rows = [
        {"playlist_id": "pl123", "playlist_name": "Child Playlist"},
        {"playlist_id": "plParent", "playlist_name": "Parent Playlist"},
    ]
    mock_spotify = MagicMock()
    with patch('backend.api.music.resolve_now_playing', return_value=mock_state), \
         patch('backend.api.music.sql_to_dict', side_effect=[family_rows, name_rows]), \
         patch('backend.api.music.one_sql_result', return_value=1), \
         patch('backend.api.music.qec'), \
         patch('backend.api.music.sql_to_list', return_value=["tid1"]) as mock_sql_to_list, \
         patch('backend.api.music._get_spotify_client', return_value=mock_spotify):
        response = client.post("/api/music/now-playing/remove")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "Test Track removed from playlist" in data["message"]
        # Regression guard: sql_to_list must be called with the ISRC params tuple
        mock_sql_to_list.assert_called_once()
        args, kwargs = mock_sql_to_list.call_args
        assert args[1] == ("TEST123",)
        # Spotify removal must be called for every family playlist where the track
        # was found locally — the child (currently playing) AND the parent.
        mock_spotify.playlist_remove_all_occurrences_of_items.assert_has_calls(
            [call("pl123", ["tid1"]), call("plParent", ["tid1"])],
            any_order=True,
        )
        assert mock_spotify.playlist_remove_all_occurrences_of_items.call_count == 2


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


def test_shuffle_flags_updates_playlist_config():
    """POST /api/music/shuffle/flags reconciles boolean flags to the source playlist."""
    with patch('backend.api.music.qec') as mock_qec:
        response = client.post(
            "/api/music/shuffle/flags?playlist_id=pl123",
            json={
                "autoShuffle": True,
                "manualShuffle": False,
                "makeRecs": True,
                "seedsOnly": False,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "flags updated" in data["message"]
        # Ensure the UPDATE was executed
        args, kwargs = mock_qec.call_args
        assert "UPDATE music.playlist_config SET" in args[0]
        assert "auto_shuffle = %s" in args[0]
        assert "manual_shuffle = %s" in args[0]
        assert "make_recs = %s" in args[0]
        assert "seeds_only = %s" in args[0]
        assert "WHERE playlist_id = %s" in args[0]
        # Parameter order: auto, manual, recs, seeds, playlist_id
        assert args[1] == (True, False, True, False, "pl123")


def test_shuffle_flags_defaults_to_false():
    """POST /api/music/shuffle/flags accepts missing flags as False."""
    with patch('backend.api.music.qec') as mock_qec:
        response = client.post(
            "/api/music/shuffle/flags?playlist_id=pl123",
            json={},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        args, kwargs = mock_qec.call_args
        assert args[1] == (False, False, False, False, "pl123")


# ---------------------------------------------------------------------------
# database_functions tests
# ---------------------------------------------------------------------------


def test_sql_to_list_accepts_params():
    """sql_to_list must accept an optional params argument and pass it to cursor.execute.

    Regression test for 008-001 Remove bug: now_playing_remove called
    sql_to_list with a params tuple, but the function only accepted query_str,
    causing a TypeError -> 500 Internal Server Error.
    """
    from backend_functions.database_functions import sql_to_list

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("track1",), ("track2",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch('backend_functions.database_functions.get_conn', return_value=mock_conn):
        result = sql_to_list(
            "SELECT DISTINCT track_id FROM music.all_tracks WHERE track_isrc = %s",
            ("TEST123",),
        )

    assert result == ["track1", "track2"]
    mock_cursor.execute.assert_called_once_with(
        "SELECT DISTINCT track_id FROM music.all_tracks WHERE track_isrc = %s",
        ("TEST123",),
    )


def test_sql_to_list_no_params_still_works():
    """sql_to_list must still work when called without params (backward compat)."""
    from backend_functions.database_functions import sql_to_list

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("a",), ("b",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch('backend_functions.database_functions.get_conn', return_value=mock_conn):
        result = sql_to_list("SELECT DISTINCT activity_id from activities.activity_processing_queue order by activity_id desc LIMIT 5")

    assert result == ["a", "b"]
    mock_cursor.execute.assert_called_once_with(
        "SELECT DISTINCT activity_id from activities.activity_processing_queue order by activity_id desc LIMIT 5"
    )