"""000-001 T02: playlist-items paging-href normalization regression test (no network, no DB).

Covers `playlist_to_db` in backend_functions/music_functions.py: every page of a
playlist-items payload must be normalized (`/items?` -> `/tracks?`) before
`json_loading`, so the downstream SPROC derivation
`split_part(split_part(href,'/playlists/',2),'/tracks',1)` yields the bare
22-char playlist id for every page (offset=0 and offset=50 page-2+ with
`additional_types=track`).

T01 context: the task-wired extractor `extract_json_playlist_details`
(json_extractors.py) has NO such normalization -- only `playlist_to_db` does.
This test locks `playlist_to_db` behaviour; T05/T08 fix the SPROC + extractor.
"""
import re

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

BARE_RE = re.compile(r"^[A-Za-z0-9]{22}$")


def _sproc_derive(href):
    """Replicate staging.flatten_playlist_details derivation in Python."""
    after = href.split("/playlists/", 1)[1] if "/playlists/" in href else href
    return after.split("/tracks", 1)[0]


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


def test_all_pages_normalized_before_json_loading(monkeypatch):
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


def test_derived_id_is_bare_22char_for_every_page(monkeypatch):
    captured = {}
    _patch(monkeypatch, captured)
    mf.playlist_to_db(client={"client": None}, list_id=BARE_ID)
    for page in captured["payload"]:
        derived = _sproc_derive(page["href"])
        assert derived == BARE_ID, f"derived {derived!r} != {BARE_ID!r}"
        assert BARE_RE.match(derived), f"derived id not bare 22-char: {derived!r}"


def test_raw_href_would_pollute_proves_sensitivity():
    """Raw (un-normalized) page-1 and page-2 hrefs derive malformed ids.

    Documents test sensitivity: without the `/items?`->`/tracks?` rewrite in
    `playlist_to_db`, the SPROC derivation yields the polluted shapes seen in
    PRD (Bug 000-001-T01-1), so the tests above would fail.
    """
    for raw in (PAGE0_HREF, PAGE1_HREF):
        derived = _sproc_derive(raw)
        assert derived != BARE_ID
        assert not BARE_RE.match(derived)
