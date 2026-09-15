"""000-001 T11: auto_shuffle_playlists dataframe-free parity + no-pandas guard.

Mocks all DB/Spotify interactions — no live calls.
"""
import re
from pathlib import Path

import backend_functions.music_functions as mf


def test_music_functions_module_has_no_pandas():
    src = Path(mf.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\bimport pandas\b|pd\.", src), "pandas/pd. reference found in music_functions.py"


def test_auto_shuffle_parity_target_id_and_ordered_track_list(monkeypatch):
    # Rows in view order (default_new_order asc) — parity expects this exact order.
    rows = [
        {"target_playlist_id": "TGT0000000000000000000a", "track_id": "t3", "default_new_order": 3},
        {"target_playlist_id": "TGT0000000000000000000a", "track_id": "t1", "default_new_order": 1},
        {"target_playlist_id": "TGT0000000000000000000a", "track_id": "t2", "default_new_order": 2},
    ]
    captured = {}

    def fake_sql_to_dict(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [dict(r, _order=r["default_new_order"]) for r in rows]

    calls = []

    def fake_upload(client, target_id, track_list):
        calls.append((target_id, list(track_list)))
        return client

    monkeypatch.setattr(mf, "sql_to_dict", fake_sql_to_dict)
    monkeypatch.setattr(mf, "playlist_upload", fake_upload)
    monkeypatch.setattr(mf, "get_spotify_client", lambda c: {"client": object()})
    monkeypatch.setattr(mf, "log_app_event", lambda **kw: None)
    monkeypatch.setattr(mf, "qec", lambda *a, **kw: None)

    mf.auto_shuffle_playlists(list_id="PRNT000000000000000000b")

    assert calls, "playlist_upload was not called"
    target_id, track_list = calls[0]
    # Same target id as the dataframe path (first row) and same ordered list
    assert target_id == "TGT0000000000000000000a"
    assert track_list == ["t3", "t1", "t2"]
    # Parameterized SQL, no f-string interpolation of the playlist id
    assert "%s" in captured["sql"]
    assert captured["params"] == ["PRNT000000000000000000b"]


def test_auto_shuffle_limit_minutes_parameterized(monkeypatch):
    captured = {}

    def fake_sql_to_dict(sql, params=None):
        captured["sql"] = sql
        return []

    monkeypatch.setattr(mf, "sql_to_dict", fake_sql_to_dict)
    monkeypatch.setattr(mf, "get_spotify_client", lambda c: {"client": object()})
    monkeypatch.setattr(mf, "log_app_event", lambda **kw: None)

    mf.auto_shuffle_playlists(list_id="PRNT000000000000000000b", limit_minutes=45)
    assert "cumulative_duration_s/60 < minutes_to_sync" in captured["sql"]
