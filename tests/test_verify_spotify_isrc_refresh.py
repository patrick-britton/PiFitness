"""
Live integration test — 004-004 Bug T10-6 fix (FR-8.6 / FR-10).

Validates the exact methodology the rebuilt pipeline uses to refresh
music.playlist_isrcs by querying Spotify, via the feature's own step function
`backend.api.activities._verify_spotify`, against the Running target playlist
'7LPPIzdYgJZgj2QTSXCCNy'.

Methodology under test (matches the pipeline's verify_spotify step):
  1. get_spotify_client() must yield a usable client (rate-limit client=None
     path must fail loudly, not silently no-op).
  2. playlist_to_db(client, list_id=...) → sp.playlist_items (paged) →
     json_loading(..., 'playlist_details') → CALL staging.flatten_playlist_details()
     → music.playlist_isrcs rows refreshed with fresh updated_at_utc.
  3. FR-10 verification: max(updated_at_utc) must be < 5 minutes old.

Side effect (same as a live pipeline run): the target playlist's rows in
music.playlist_isrcs are refreshed. No schema is created or altered.

Run from the repository root:
    python -m pytest tests/test_verify_spotify_isrc_refresh.py -v
"""
from pathlib import Path

import pytest

from backend_functions.database_functions import one_sql_result, sql_to_list
from backend_functions.service_logins import get_spotify_client
from backend.api.activities import _verify_spotify

TARGET_PLAYLIST_ID = '7LPPIzdYgJZgj2QTSXCCNy'

REPO_ROOT = Path(__file__).resolve().parents[1]
ACTIVITIES_PY = REPO_ROOT / 'backend' / 'api' / 'activities.py'
TYPES_TS = REPO_ROOT / 'frontend' / 'pifitness' / 'src' / 'lib' / 'types' / 'activity-processing.ts'


def _max_updated_at(playlist_id):
    return one_sql_result(
        "SELECT max(updated_at_utc) FROM music.playlist_isrcs WHERE playlist_id = %s",
        (playlist_id,),
    )


def _row_count(playlist_id):
    return one_sql_result(
        "SELECT count(*) FROM music.playlist_isrcs WHERE playlist_id = %s",
        (playlist_id,),
    )


# ---------------------------------------------------------------------------
# Static: the final shuffle step exists and is wired in the process
# ---------------------------------------------------------------------------

def test_verify_spotify_step_exists_in_pipeline():
    """The verify_spotify re-read step must be wired into the pipeline between
    send_to_spotify and report_shuffle (FR-8.6), with the frontend contract
    including the id and a label."""
    src = ACTIVITIES_PY.read_text(encoding='utf-8')

    # Step function exists and is executed as a pipeline step.
    assert 'def _verify_spotify(' in src
    assert '_run_step("verify_spotify", _verify_spotify, playlist_id)' in src

    # Sequence order: send → verify → report.
    i_send = src.index('_run_step("send_to_spotify"')
    i_verify = src.index('_run_step("verify_spotify"')
    i_report = src.index('_run_step_data("report_shuffle"')
    assert i_send < i_verify < i_report

    # FR-10 guard present: client pre-check + DB freshness verification.
    assert 'could not obtain a usable Spotify client' in src
    assert "max(updated_at_utc) >= (CURRENT_TIMESTAMP - interval '5 minutes')" in src

    # Frontend contract: step id in STEP_ORDER and a human label present.
    ts = TYPES_TS.read_text(encoding='utf-8')
    assert "'verify_spotify'" in ts
    assert "verify_spotify: 'Verifying on Spotify'" in ts


# ---------------------------------------------------------------------------
# Live: Spotify client acquisition
# ---------------------------------------------------------------------------

def test_spotify_client_is_usable():
    """T10-6 pre-check: a real client must be obtainable (not None client)."""
    client = get_spotify_client()
    assert client is not None, "get_spotify_client() returned None"
    assert client.get("client") is not None, (
        "Spotify client is None (rate-limited or token unavailable) — "
        "the T10-6 soft-fail condition is currently active"
    )


# ---------------------------------------------------------------------------
# Negative: no usable client must fail loudly (T10-6 regression guard)
# ---------------------------------------------------------------------------

def test_verify_spotify_fails_without_client(monkeypatch):
    """With no usable client the step must raise instead of soft-succeeding
    (the original T10-6 bug silently no-oped)."""
    from backend.api import activities as acts

    for broken in (None, {"token": "x", "token_age": 0, "client": None}):
        monkeypatch.setattr(acts, "get_spotify_client", lambda b=broken: b)
        with pytest.raises(RuntimeError, match="could not obtain a usable Spotify client"):
            acts._verify_spotify(TARGET_PLAYLIST_ID)


# ---------------------------------------------------------------------------
# Live: the core methodology — Spotify query refreshes music.playlist_isrcs
# ---------------------------------------------------------------------------

def test_verify_spotify_refreshes_playlist_isrcs():
    """Run _verify_spotify(TARGET) live: it must not raise (client usable +
    music.playlist_isrcs refreshed within 5 minutes) and the table state must
    be sane afterwards."""
    pre_ts = _max_updated_at(TARGET_PLAYLIST_ID)
    pre_count = _row_count(TARGET_PLAYLIST_ID)

    # Raises RuntimeError on any failure (client unusable or DB not refreshed).
    _verify_spotify(TARGET_PLAYLIST_ID)

    post_ts = _max_updated_at(TARGET_PLAYLIST_ID)
    post_count = _row_count(TARGET_PLAYLIST_ID)

    assert post_ts is not None, "no rows in music.playlist_isrcs for the target"
    assert post_count == pre_count and pre_count > 0, (
        f"row count changed unexpectedly: {pre_count} -> {post_count}"
    )
    assert post_ts >= pre_ts, (
        f"updated_at_utc did not advance: {pre_ts} -> {post_ts}"
    )


def test_stored_order_matches_spotify():
    """Order parity: the track_order sequence stored in music.playlist_isrcs
    must equal the playlist's live order on Spotify (by ISRC)."""
    client = get_spotify_client()
    sp = client.get("client")
    assert sp is not None, "no usable Spotify client for order parity check"

    sp_isrcs = []
    results = sp.playlist_items(TARGET_PLAYLIST_ID, additional_types=['track'])
    while results:
        for item in results.get('items', []):
            track = item.get('track') or {}
            isrc = (track.get('external_ids') or {}).get('isrc')
            if isrc:
                sp_isrcs.append(isrc)
        results = sp.next(results)

    db_isrcs = sql_to_list(
        "SELECT isrc FROM music.playlist_isrcs WHERE playlist_id = %s ORDER BY track_order",
        (TARGET_PLAYLIST_ID,),
    )

    assert len(sp_isrcs) > 0, "Spotify returned no tracks"
    assert db_isrcs == sp_isrcs, (
        "music.playlist_isrcs order does not match the live Spotify order\n"
        f"  db ({len(db_isrcs)}): {db_isrcs[:5]}...\n"
        f"  spotify ({len(sp_isrcs)}): {sp_isrcs[:5]}..."
    )
