"""Music router actions, recent plays, album art, playlist payload validation and
related helpers with databases and Spotify mocked.
"""

import pytest
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


@pytest.mark.contract
def test_now_playing_skip_route_exists():
    """POST /api/music/now-playing/skip returns 503 when no Spotify client is available.
    """
    with patch('pifitness.music.router.get_spotify_client_for_music', return_value=None):
        response = client.post("/api/music/now-playing/skip")
        assert response.status_code == 503


@pytest.mark.contract
def test_now_playing_promote_wrong_context():
    """POST /api/music/now-playing/promote returns ok false saying it applies only to
    recommendations when the track is not a recommendation.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackId": "tid1",
            "trackName": "Test Track",
            "context": {"relationshipType": "regular", "parentPlaylistId": None}
        }
    }
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state):
        response = client.post("/api/music/now-playing/promote")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert "only applies to recommendations" in data["message"]


@pytest.mark.contract
def test_now_playing_remove_family_miss():
    """POST /api/music/now-playing/remove returns ok false saying the playlist cannot be
    found when no playlist family rows match.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.sql_to_dict', return_value=[]):
        response = client.post("/api/music/now-playing/remove")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert data["message"] == "the playlist cannot be found"


@pytest.mark.regression
def test_now_playing_remove_full_flow():
    """POST /api/music/now-playing/remove removes the track from Spotify in every family
    playlist that contains it, parent and child, and reports success.
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
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.sql_to_dict', side_effect=[family_rows, name_rows]), \
         patch('pifitness.music.router.one_sql_result', return_value=1), \
         patch('pifitness.music.router.qec'), \
         patch('pifitness.music.router.sql_to_list', return_value=["tid1"]) as mock_sql_to_list, \
         patch('pifitness.music.router.get_spotify_client_for_music', return_value=mock_spotify):
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


@pytest.mark.contract
def test_now_playing_rank_up_wrong_context():
    """POST /api/music/now-playing/rank-up returns ok false saying it applies only to
    playlist tracks when the track is a recommendation.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "context": {"relationshipType": "recommendation", "playlistId": "pl123"}
        }
    }
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state):
        response = client.post("/api/music/now-playing/rank-up")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is False
        assert "only applies to playlist tracks" in data["message"]


@pytest.mark.contract
def test_now_playing_add_targets_no_track():
    """GET /api/music/now-playing/add-targets returns eligible false and an empty
    playlist list when nothing is playing.
    """
    with patch('pifitness.music.router.resolve_now_playing', return_value={"playing": False, "track": None}):
        response = client.get("/api/music/now-playing/add-targets")
        assert response.status_code == 200
        data = response.json()
        assert data["eligible"] is False
        assert data["playlists"] == []



@pytest.mark.contract
def test_now_playing_add_targets_with_track():
    """GET /api/music/now-playing/add-targets returns eligible true with the playlists
    that do not contain the current track.
    """
    mock_state = {
        "playing": True,
        "track": {"isrc": "TEST123"}
    }
    mock_playlists = [
        {"playlist_id": "pl1", "playlist_name": "Playlist One"},
        {"playlist_id": "pl2", "playlist_name": "Playlist Two"},
    ]
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.get_playlists_not_containing_isrc', return_value=mock_playlists):
        response = client.get("/api/music/now-playing/add-targets")
        assert response.status_code == 200
        data = response.json()
        assert data["eligible"] is True
        assert len(data["playlists"]) == 2
        assert data["playlists"][0]["playlist_id"] == "pl1"


@pytest.mark.contract
def test_now_playing_promote_full_flow():
    """POST /api/music/now-playing/promote adds the recommendation to the parent
    playlist, records the decision, removes the recommendation and seeds its rating.
    """
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
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.add_isrc_to_local_playlist') as mock_add, \
         patch('pifitness.music.router.get_spotify_client_for_music', return_value=MagicMock()), \
         patch('pifitness.music.router.record_recommendation_decision') as mock_record, \
         patch('pifitness.music.router.remove_recommendation') as mock_remove, \
         patch('pifitness.music.router.add_into_current_ratings') as mock_seed:
        response = client.post("/api/music/now-playing/promote")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "added to playlist" in data["message"]
        mock_add.assert_called_once_with("parent1", "TEST123")
        mock_record.assert_called_once_with("parent1", "TEST123", was_promoted=True)
        mock_remove.assert_called_once_with("parent1", "TEST123")
        mock_seed.assert_called_once_with("parent1", "TEST123", 1500)


@pytest.mark.contract
def test_now_playing_rank_up_full_flow():
    """POST /api/music/now-playing/rank-up records a matchup result of plus two seeded
    from the track's current rating.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.save_matchup_results') as mock_save:
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


@pytest.mark.contract
def test_now_playing_rank_down_full_flow():
    """POST /api/music/now-playing/rank-down records a matchup result of minus two
    seeded from the track's current rating.
    """
    mock_state = {
        "playing": True,
        "track": {
            "isrc": "TEST123",
            "trackName": "Test Track",
            "rating": {"value": 1500},
            "context": {"relationshipType": "regular", "playlistId": "pl123"}
        }
    }
    with patch('pifitness.music.router.resolve_now_playing', return_value=mock_state), \
         patch('pifitness.music.router.save_matchup_results') as mock_save:
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


@pytest.mark.contract
def test_recent_plays_default_limit():
    """GET /api/music/recent-plays returns plays and a rating scale built from the
    returned rows.
    """
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
    with patch('pifitness.music.router.get_recent_plays', return_value=mock_rows):
        response = client.get("/api/music/recent-plays")
        assert response.status_code == 200
        data = response.json()
        assert "plays" in data
        assert "scale" in data
        assert len(data["plays"]) == 1
        assert data["plays"][0]["isrc"] == "ISRC1"
        assert data["scale"]["minRating"] == 1400
        assert data["scale"]["maxRating"] == 1600


@pytest.mark.contract
def test_recent_plays_custom_limit():
    """GET /api/music/recent-plays forwards a valid limit to the query helper."""
    with patch('pifitness.music.router.get_recent_plays', return_value=[]) as mock_get:
        response = client.get("/api/music/recent-plays?limit=50")
        assert response.status_code == 200
        mock_get.assert_called_once_with(limit=50)


@pytest.mark.contract
def test_recent_plays_empty():
    """GET /api/music/recent-plays returns no plays and a baseline rating scale of 1500
    when there is no history.
    """
    with patch('pifitness.music.router.get_recent_plays', return_value=[]):
        response = client.get("/api/music/recent-plays")
        assert response.status_code == 200
        data = response.json()
        assert data["plays"] == []
        assert data["scale"]["minRating"] == 1500
        assert data["scale"]["maxRating"] == 1500
        assert data["scale"]["maxPlaycountLast30"] == 0
        assert data["scale"]["maxPlaycountTotal"] == 0


@pytest.mark.contract
def test_service_status_not_limited():
    """GET /api/music/service-status reports Spotify as not rate limited with a null
    clearance time.
    """
    with patch('pifitness.music.router.sql_rate_limited', return_value=False):
        response = client.get("/api/music/service-status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"]["rateLimited"] is False
        assert data["spotify"]["rateLimitClearedUtc"] is None


@pytest.mark.contract
def test_service_status_limited():
    """GET /api/music/service-status reports Spotify as rate limited together with its
    clearance time.
    """
    with patch('pifitness.music.router.sql_rate_limited', return_value=True), \
         patch('pifitness.music.router.one_sql_result', return_value="2026-09-02T00:00:00"):
        response = client.get("/api/music/service-status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"]["rateLimited"] is True
        assert data["spotify"]["rateLimitClearedUtc"] == "2026-09-02T00:00:00"


# ---------------------------------------------------------------------------
# T06: Album art serving
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_album_art_cached():
    """GET /api/music/album-art/{id} serves the retrieved image file as image/jpeg."""
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(b"fake_jpeg_data")
        tmp_path = Path(f.name)
    with patch('pifitness.music.router.album_image_retrieval', return_value=str(tmp_path)):
        response = client.get(f"/api/music/album-art/test_album_id")
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/jpeg"
    tmp_path.unlink()


@pytest.mark.contract
def test_album_art_not_found():
    """GET /api/music/album-art/{id} returns 404 when no image can be obtained."""
    with patch('pifitness.music.router.album_image_retrieval', return_value=None):
        response = client.get("/api/music/album-art/nonexistent_id")
        assert response.status_code == 404


@pytest.mark.regression
def test_album_image_retrieval_no_client():
    """album_image_retrieval returns None when the image is not cached and no Spotify
    client is available.
    """
    with patch('os.path.exists', return_value=False), \
         patch('backend_functions.music_functions.get_spotify_client', return_value=None):
        result = album_image_retrieval("nonexistent")
        assert result is None


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="asserts SQL fragments and positional parameter order",
    exit="rewrite_against_db",
)
def test_shuffle_flags_updates_playlist_config():
    """POST /api/music/shuffle/flags writes the four shuffle flags to the playlist's
    configuration row and reports success.
    """
    with patch('pifitness.music.router.qec') as mock_qec:
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


@pytest.mark.contract
def test_shuffle_flags_defaults_to_false():
    """POST /api/music/shuffle/flags treats missing flags as false."""
    with patch('pifitness.music.router.qec') as mock_qec:
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


@pytest.mark.regression
def test_sql_to_list_accepts_params():
    """sql_to_list passes an optional parameter tuple to the cursor and returns the
    first column of every row.
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


@pytest.mark.regression
def test_sql_to_list_no_params_still_works():
    """sql_to_list still executes a bare query without parameters."""
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


# ---------------------------------------------------------------------------
# Playlist sync fail-loud guards (000-001 T08, AC-8)
# ---------------------------------------------------------------------------

BARE_ID = '1Rfae5pyaheNsvr7ajdeBR'  # 22 chars, matches the PRD-documented id
PAGE0_HREF = (f"https://api.spotify.com/v1/playlists/{BARE_ID}"
              "/items?offset=0&limit=50&additional_types=track")
PAGE1_HREF = (f"https://api.spotify.com/v1/playlists/{BARE_ID}"
              "/items?offset=50&limit=50&additional_types=track")
NORM0_HREF = PAGE0_HREF.replace('/items?', '/tracks?')
NORM1_HREF = PAGE1_HREF.replace('/items?', '/tracks?')

DETAIL_TD = {'task_id': 16, 'task_name': 'Playlist Detail Sync', 'api_function_name': 'playlist_items'}
HEADER_TD = {'task_id': 17, 'task_name': 'Playlist Header Sync', 'api_function_name': 'current_user_playlists'}


@pytest.mark.regression
def test_validate_playlist_payload_empty_aborts():
    """validate_playlist_payload returns False and logs once, without touching the
    database, when the payload is empty.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log, \
            patch('pifitness.worker.json_extractors.qec') as mock_qec:
        result = validate_playlist_payload(None, HEADER_TD)
    assert result is False
    mock_log.assert_called_once()
    mock_qec.assert_not_called()  # guard performs no DB work, so no deletes


@pytest.mark.regression
def test_validate_playlist_payload_incomplete_paging_aborts():
    """validate_playlist_payload returns False for a page that advertises a next page
    with no successor.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    truncated_page = {
        'href': NORM0_HREF,
        'next': f"https://api.spotify.com/v1/playlists/{BARE_ID}/tracks?offset=50",
        'items': [{'track': {'id': 't1'}}],
    }
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log, \
            patch('pifitness.worker.json_extractors.qec') as mock_qec:
        result = validate_playlist_payload([truncated_page], DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'incomplete paging' in mock_log.call_args[1].get('desc', '')
    mock_qec.assert_not_called()


@pytest.mark.regression
def test_validate_playlist_payload_truncated_chain_before_next_playlist_aborts():
    """validate_playlist_payload returns False when a paging chain is cut short by a
    page belonging to another playlist.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    other_id = '7LPPIzdYgJZgj2QTSXCCNy'
    pages = [
        {'href': NORM0_HREF, 'next': 'offset=50 page',
         'items': [{'track': {'id': 't1'}}]},
        {'href': f"https://api.spotify.com/v1/playlists/{other_id}/tracks?offset=0",
         'items': [{'track': {'id': 't2'}}]},
    ]
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log:
        result = validate_playlist_payload(pages, DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'incomplete paging' in mock_log.call_args[1].get('desc', '')


@pytest.mark.regression
def test_validate_playlist_payload_complete_paged_chain_passes():
    """validate_playlist_payload returns True for a fully fetched paging chain whose
    intermediate pages still carry next pointers.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    pages = [
        {'href': NORM0_HREF, 'next': f".../playlists/{BARE_ID}/tracks?offset=50",
         'items': [{'track': {'id': 't1'}}]},
        {'href': NORM1_HREF, 'next': None, 'items': [{'track': {'id': 't2'}}]},
    ]
    assert validate_playlist_payload(pages, DETAIL_TD) is True


@pytest.mark.regression
def test_validate_playlist_payload_malformed_href_id_aborts():
    """validate_playlist_payload returns False and names the offender when a page href
    yields an id that is not a bare 22-character playlist id.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    bad_page = {
        'href': 'https://api.spotify.com/v1/playlists/abc/playlist/items',
        'items': [{'track': {'id': 't1'}}],
    }
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log:
        result = validate_playlist_payload([bad_page], DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'non-22-char playlist_id' in mock_log.call_args[1].get('desc', '')


@pytest.mark.regression
def test_validate_playlist_payload_detail_missing_href_aborts():
    """validate_playlist_payload returns False for a detail page whose href cannot yield
    a playlist id.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log:
        result = validate_playlist_payload([{'items': [{'track': {'id': 't1'}}]}], DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'non-22-char playlist_id' in mock_log.call_args[1].get('desc', '')


@pytest.mark.regression
def test_validate_playlist_payload_detail_item_ids_are_not_playlist_ids():
    """validate_playlist_payload accepts a detail page whose item ids are not playlist
    ids, such as local files.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    page = {
        'href': NORM0_HREF,
        'items': [{'id': 'spotify:local:artist:album:track:180', 'track': {'id': 't1'}}],
    }
    assert validate_playlist_payload([page], DETAIL_TD) is True


@pytest.mark.regression
def test_validate_playlist_payload_unnormalized_items_href_aborts():
    """validate_playlist_payload returns False for an unnormalized items href that would
    persist a polluted playlist id.
    """
    from pifitness.worker.json_extractors import (
        derive_playlist_id_from_href,
        validate_playlist_payload,
    )
    assert derive_playlist_id_from_href(PAGE1_HREF) == f"{BARE_ID}/items"
    unnormalized_page = {'href': PAGE1_HREF, 'items': [{'track': {'id': 't2'}}]}
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log, \
            patch('pifitness.worker.json_extractors.qec') as mock_qec:
        result = validate_playlist_payload([unnormalized_page], DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'non-22-char playlist_id' in mock_log.call_args[1].get('desc', '')
    mock_qec.assert_not_called()


@pytest.mark.regression
def test_validate_playlist_payload_malformed_item_id_aborts():
    """validate_playlist_payload returns False for a header payload whose item id is not
    a bare 22-character id.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    bad_page = {
        'href': 'https://api.spotify.com/v1/users/someuser/playlists?offset=0&limit=50',
        'items': [{'id': 'not_22_chars', 'name': 'Bad Playlist'}],
    }
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log, \
            patch('pifitness.worker.json_extractors.qec') as mock_qec:
        result = validate_playlist_payload([bad_page], HEADER_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'in items' in mock_log.call_args[1].get('desc', '')
    mock_qec.assert_not_called()


@pytest.mark.regression
def test_validate_playlist_payload_valid_header_payload_passes():
    """validate_playlist_payload returns True for a complete header chain whose item ids
    are 22-character playlist ids.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    pages = [
        {'href': 'https://api.spotify.com/v1/users/someuser/playlists?offset=0&limit=50',
         'next': 'https://api.spotify.com/v1/users/someuser/playlists?offset=50&limit=50',
         'items': [{'id': BARE_ID, 'name': 'Good Playlist'}]},
        {'href': 'https://api.spotify.com/v1/users/someuser/playlists?offset=50&limit=50',
         'next': None,
         'items': [{'id': '7LPPIzdYgJZgj2QTSXCCNy', 'name': 'Another'}]},
    ]
    assert validate_playlist_payload(pages, HEADER_TD) is True


@pytest.mark.regression
def test_validate_playlist_payload_all_pages_empty_aborts():
    """validate_playlist_payload returns False when every page has zero items."""
    from pifitness.worker.json_extractors import validate_playlist_payload
    empty_page = {'href': NORM0_HREF, 'items': []}
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log:
        result = validate_playlist_payload([empty_page], DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'all pages empty' in mock_log.call_args[1].get('desc', '')


@pytest.mark.regression
def test_validate_playlist_payload_unexpected_type_aborts():
    """validate_playlist_payload returns False instead of raising when given a payload
    that is not a list of pages.
    """
    from pifitness.worker.json_extractors import validate_playlist_payload
    with patch('pifitness.worker.json_extractors.log_app_event') as mock_log:
        result = validate_playlist_payload(object(), DETAIL_TD)
    assert result is False
    mock_log.assert_called_once()
    assert 'unexpected payload type' in mock_log.call_args[1].get('desc', '')


class _FakeSpotifyClient:
    """Minimal two-page playlist-items client (no network)."""

    def __init__(self, pages):
        self._pages = list(pages)

    def playlist_items(self, playlist_id=None, additional_types=None):
        assert playlist_id == BARE_ID
        return dict(self._pages[0])

    def next(self, results):
        if results.get('next') == 'page1':
            return dict(self._pages[1])
        return None


@pytest.mark.regression
def test_playlist_details_extractor_normalizes_hrefs_and_passes_guard():
    """extract_json_playlist_details rewrites every page href to the tracks form so the
    derived playlist id is the bare 22-character id and the payload passes validation.
    """
    from pifitness.worker.json_extractors import (
        derive_playlist_id_from_href,
        extract_json_playlist_details,
        validate_playlist_payload,
    )
    pages = [
        {'href': PAGE0_HREF, 'next': 'page1', 'items': [{'track': {'id': 't1'}}]},
        {'href': PAGE1_HREF, 'next': None, 'items': [{'track': {'id': 't2'}}]},
    ]
    with patch('pifitness.worker.json_extractors.time.sleep'), \
            patch('pifitness.worker.json_extractors.log_app_event'):
        payload = extract_json_playlist_details(
            client=_FakeSpotifyClient(pages), list_id=BARE_ID, td=DETAIL_TD
        )

    assert len(payload) == 2
    for page in payload:
        assert '/items?' not in page['href'], f"unnormalized href: {page['href']}"
        assert '/tracks?' in page['href'], f"expected normalized href: {page['href']}"
        assert derive_playlist_id_from_href(page['href']) == BARE_ID
    assert validate_playlist_payload(payload, DETAIL_TD) is True


@pytest.mark.regression
def test_playlist_details_extractor_no_client_aborts_loudly():
    """extract_json_playlist_details returns an empty list and logs a loud error,
    without listing playlists, when no usable Spotify client exists.
    """
    from pifitness.worker.json_extractors import extract_json_playlist_details
    with patch('pifitness.worker.json_extractors.get_spotify_client', return_value={}), \
            patch('pifitness.worker.json_extractors.log_app_event') as mock_log, \
            patch('pifitness.worker.json_extractors.get_playlist_list') as mock_list:
        result = extract_json_playlist_details(client=None, list_id=None, td=DETAIL_TD)
    assert result == []
    mock_log.assert_called_once()
    assert 'no usable Spotify client' in mock_log.call_args[1].get('desc', '')
    mock_list.assert_not_called()


NOTHING_PLAYING = {"playing": False, "track": None}


@pytest.mark.contract
@pytest.mark.parametrize("path", [
    "/api/music/now-playing/promote",
    "/api/music/now-playing/soft-reject",
    "/api/music/now-playing/hard-reject",
    "/api/music/now-playing/remove",
    "/api/music/now-playing/rank-down",
    "/api/music/now-playing/add-to-playlist?playlist_id=pl123",
])
def test_now_playing_actions_report_not_ok_when_nothing_is_playing(path):
    """Every now-playing action endpoint returns ok false with status 200 when nothing
    is playing.
    """
    with patch('pifitness.music.router.resolve_now_playing', return_value=NOTHING_PLAYING):
        response = client.post(path)
    assert response.status_code == 200
    assert response.json()["ok"] is False


@pytest.mark.contract
@pytest.mark.parametrize("limit", [5, 105, 15])
def test_recent_plays_rejects_invalid_limit(limit):
    """GET /api/music/recent-plays rejects a limit below 10, above 100 or not a multiple
    of 10 with 422.
    """
    response = client.get(f"/api/music/recent-plays?limit={limit}")
    assert response.status_code == 422
