"""ISRC duplicate-review endpoints over mocked query helpers."""

import pytest
import sys
import os
import inspect
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app, raise_server_exceptions=False)

MATCH_ROW = {
    "isrc1": "ISRC-A",
    "isrc2": "ISRC-B",
    "track_name1": "Same Song",
    "track_name2": "Same Song",
    "artist_name1": "Band",
    "artist_name2": "Band",
    "album_name1": "Album One",
    "album_name2": "Album Two",
    "duration_1": 210,
    "duration_2": 211,
    "match_score": 0.92,
    "track_score": 1.0,
    "artist_score": 1.0,
    "album_score": 0.75,
    "duration_score": 0.99,
    "preferred_isrc": "ISRC-A",
}


@pytest.mark.contract
def test_count_returns_pair_count():
    """GET /api/music/isrc-dupes/count returns the pair count from the query helper."""
    with patch(
        'pifitness.music.router.get_isrc_dupe_count', return_value=4
    ):
        response = client.get("/api/music/isrc-dupes/count")
        assert response.status_code == 200
        assert response.json() == {"count": 4}


@pytest.mark.contract
def test_match_shapes_row_to_camel_case():
    """GET /api/music/isrc-dupes/match returns the next duplicate pair with its keys
    converted from snake_case to camelCase.
    """
    with patch(
        'pifitness.music.router.get_isrc_dupe_match', return_value=[MATCH_ROW]
    ):
        response = client.get("/api/music/isrc-dupes/match")
        assert response.status_code == 200
        match = response.json()["match"]
        assert match["isrc1"] == "ISRC-A"
        assert match["trackName1"] == "Same Song"
        assert match["artistName2"] == "Band"
        assert match["albumName1"] == "Album One"
        assert match["duration1"] == 210
        assert match["duration2"] == 211
        assert match["matchScore"] == 0.92
        assert match["preferredIsrc"] == "ISRC-A"


@pytest.mark.contract
def test_match_empty_queue_returns_null():
    """GET /api/music/isrc-dupes/match returns a null match when no duplicate pairs are
    queued.
    """
    with patch(
        'pifitness.music.router.get_isrc_dupe_match', return_value=[]
    ):
        response = client.get("/api/music/isrc-dupes/match")
        assert response.status_code == 200
        assert response.json() == {"match": None}


@pytest.mark.contract
def test_decision_accept_calls_helper_and_confirms():
    """POST /api/music/isrc-dupes/decision with accept true passes both ISRCs, the
    preferred ISRC and the flag to the helper and confirms acceptance.
    """
    with patch(
        'pifitness.music.router.process_isrc_dupe_decision'
    ) as mock_decide:
        response = client.post("/api/music/isrc-dupes/decision", json={
            "isrc1": "ISRC-A",
            "isrc2": "ISRC-B",
            "preferredIsrc": "ISRC-A",
            "accept": True,
        })
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "accepted" in data["message"]
        mock_decide.assert_called_once_with(
            "ISRC-A", "ISRC-B", "ISRC-A", True
        )


@pytest.mark.contract
def test_decision_reject_calls_helper_and_confirms():
    """POST /api/music/isrc-dupes/decision with accept false passes both ISRCs, the
    preferred ISRC and the flag to the helper and confirms rejection.
    """
    with patch(
        'pifitness.music.router.process_isrc_dupe_decision'
    ) as mock_decide:
        response = client.post("/api/music/isrc-dupes/decision", json={
            "isrc1": "ISRC-A",
            "isrc2": "ISRC-B",
            "preferredIsrc": "ISRC-A",
            "accept": False,
        })
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "rejected" in data["message"]
        mock_decide.assert_called_once_with(
            "ISRC-A", "ISRC-B", "ISRC-A", False
        )


@pytest.mark.contract
def test_decision_missing_field_returns_422():
    """POST /api/music/isrc-dupes/decision returns 422 when preferredIsrc is missing."""
    response = client.post("/api/music/isrc-dupes/decision", json={
        "isrc1": "ISRC-A",
        "isrc2": "ISRC-B",
        "accept": True,
    })
    assert response.status_code == 422


@pytest.mark.contract
def test_rescan_runs_finder_and_reports_count():
    """POST /api/music/isrc-dupes/rescan runs the duplicate finder once and reports the
    resulting pair count.
    """
    with patch(
        'pifitness.music.router.run_isrc_duplicate_finder'
    ) as mock_run, patch(
        'pifitness.music.router.get_isrc_dupe_count', return_value=3
    ):
        response = client.post("/api/music/isrc-dupes/rescan")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "3 potential duplicate isrcs found" in data["message"]
        mock_run.assert_called_once_with()


@pytest.mark.contract
def test_rescan_empty_reports_no_dupes():
    """POST /api/music/isrc-dupes/rescan reports that no duplicates were found when the
    count is zero.
    """
    with patch(
        'pifitness.music.router.run_isrc_duplicate_finder'
    ), patch(
        'pifitness.music.router.get_isrc_dupe_count', return_value=0
    ):
        response = client.post("/api/music/isrc-dupes/rescan")
        assert response.status_code == 200
        assert response.json() == {
            "ok": True, "message": "No duplicate isrcs found"
        }


@pytest.mark.regression
def test_decision_write_failure_returns_500():
    """POST /api/music/isrc-dupes/decision returns 500 instead of ok when recording the
    decision fails.
    """
    # T08 / Bug 008-005-T03-1: a failed write must not be reported as {ok: true}.
    with patch(
        'pifitness.music.router.process_isrc_dupe_decision',
        side_effect=RuntimeError('write failed'),
    ):
        response = client.post("/api/music/isrc-dupes/decision", json={
            "isrc1": "ISRC-A",
            "isrc2": "ISRC-B",
            "preferredIsrc": "ISRC-A",
            "accept": True,
        })
        assert response.status_code == 500
        assert "Failed to record ISRC dupe decision" in response.json()["detail"]


@pytest.mark.regression
def test_rescan_failure_returns_500():
    """POST /api/music/isrc-dupes/rescan returns 500 instead of success when the finder
    raises.
    """
    # T08 / Bug 008-005-T03-1: a failed CALL must not be reported as a success.
    with patch(
        'pifitness.music.router.run_isrc_duplicate_finder',
        side_effect=RuntimeError('call failed'),
    ):
        response = client.post("/api/music/isrc-dupes/rescan")
        assert response.status_code == 500
        assert "Failed to rescan ISRC dupes" in response.json()["detail"]
@pytest.mark.regression
def test_rescan_route_runs_off_the_event_loop():
    """The rescan endpoint is registered as a plain synchronous function so its long
    database call runs in the threadpool instead of blocking the event loop.
    """
    # T09 / Bug 008-005-T08-1: the blocking ~25 s CALL must run in FastAPI's
    # threadpool, never awaited on the event loop. Starlette dispatches
    # non-coroutine endpoints via run_in_threadpool, so the registered handler
    # must be a plain sync function.
    #
    # NOTE: app.routes cannot be introspected here — this FastAPI version stores
    # included routers as lazy `_IncludedRouter` wrappers with no reachable
    # routes. So the music router itself is checked; the URL contract is covered
    # separately by the TestClient tests above (200/500 on this same path).
    from pifitness.music.router import router as music_router

    paths = [getattr(r, "path", None) for r in music_router.routes]
    route = next(
        (
            r for r in music_router.routes
            if str(getattr(r, "path", "")).endswith("/isrc-dupes/rescan")
        ),
        None,
    )
    assert route is not None, f"rescan route missing from music router: {paths}"
    assert route.endpoint.__name__ == "rescan_isrc_dupes_endpoint"
    assert not inspect.iscoroutinefunction(route.endpoint)
