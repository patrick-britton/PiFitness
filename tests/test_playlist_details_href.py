"""playlist_to_db normalizes every playlist-items page href so staging derives bare
playlist ids.
"""
import pytest

import backend_functions.music_functions as mf

BARE_ID = "1Rfae5pyaheNsvr7ajdeBR"
PAGE0_HREF = (
    f"https://api.spotify.com/v1/playlists/{BARE_ID}"
    "/items?offset=0&limit=50&additional_types=track"
)
PAGE1_HREF = (
    f"https://api.spotify.com/v1/playlists/{BARE_ID}"
    "/items?offset=50&limit=50&additional_types=track"
)


def _make_pages():
    return [
        {"href": PAGE0_HREF, "items": [{"track": {"id": "t1"}}], "next": "page1"},
        {"href": PAGE1_HREF, "items": [{"track": {"id": "t2"}}], "next": None},
    ]


class _FakeSP:
    def __init__(self, pages):
        self._pages = list(pages)

    def playlist_items(self, playlist_id=None, additional_types=None):
        assert playlist_id == BARE_ID
        return dict(self._pages[0])

    def next(self, results):
        if results.get("next") == "page1":
            return dict(self._pages[1])
        return None


def _patch(monkeypatch, captured):
    monkeypatch.setattr(mf, "log_app_event", lambda *a, **k: None)
    monkeypatch.setattr(mf, "task_log", lambda *a, **k: None)
    monkeypatch.setattr(mf, "qec", lambda *a, **k: None)
    monkeypatch.setattr(
        mf, "get_spotify_client", lambda client=None: {"client": _FakeSP(_make_pages())}
    )

    def fake_json_loading(payload, name):
        captured["payload"] = payload
        captured["name"] = name

    monkeypatch.setattr(mf, "json_loading", fake_json_loading)


@pytest.mark.regression
def test_all_pages_normalized_before_json_loading(monkeypatch):
    """playlist_to_db rewrites every page's href from the items form to the tracks form
    before json_loading is called.
    """
    captured = {}
    _patch(monkeypatch, captured)
    mf.playlist_to_db(client={"client": None}, list_id=BARE_ID)
    pages = captured["payload"]
    assert captured["name"] == "playlist_details"
    assert len(pages) == 2
    for page in pages:
        href = page.get("href") or ""
        assert "/items?" not in href, f"unnormalized href passed to json_loading: {href}"
        assert "/tracks?" in href, f"expected normalized href: {href}"
