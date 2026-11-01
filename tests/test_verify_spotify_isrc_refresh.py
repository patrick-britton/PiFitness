"""verify_spotify fails loudly without a Spotify client; the live refresh checks are
quarantined as manual gates.
"""

import pytest

from backend_functions.database_functions import one_sql_result, sql_to_list
from backend_functions.service_logins import get_spotify_client
from backend.pifitness.activities.services import verify_spotify

TARGET_PLAYLIST_ID = '7LPPIzdYgJZgj2QTSXCCNy'


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


# ---------------------------------------------------------------------------
# Live: Spotify client acquisition
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Negative: no usable client must fail loudly (T10-6 regression guard)
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_verify_spotify_fails_without_client(monkeypatch):
    """verify_spotify raises RuntimeError instead of silently succeeding when no usable
    Spotify client is available.
    """
    from backend.pifitness.activities.services import verify_spotify

    for broken in (None, {"token": "x", "token_age": 0, "client": None}):
        monkeypatch.setattr("backend.pifitness.activities.services.get_spotify_client", lambda b=broken: b)
        with pytest.raises(RuntimeError, match="could not obtain a usable Spotify client"):
            verify_spotify(TARGET_PLAYLIST_ID)


# ---------------------------------------------------------------------------
# Live: the core methodology — Spotify query refreshes music.playlist_isrcs
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.live
@pytest.mark.quarantine(
    reason="mutates real database rows for a hard-coded playlist id",
    exit="manual_gate",
)
def test_verify_spotify_refreshes_playlist_isrcs():
    """verify_spotify refreshes the target playlist's rows in music.playlist_isrcs
    without changing their count.
    """
    pre_ts = _max_updated_at(TARGET_PLAYLIST_ID)
    pre_count = _row_count(TARGET_PLAYLIST_ID)

    # Raises RuntimeError on any failure (client unusable or DB not refreshed).
    verify_spotify(TARGET_PLAYLIST_ID)

    post_ts = _max_updated_at(TARGET_PLAYLIST_ID)
    post_count = _row_count(TARGET_PLAYLIST_ID)

    assert post_ts is not None, "no rows in music.playlist_isrcs for the target"
    assert post_count == pre_count and pre_count > 0, (
        f"row count changed unexpectedly: {pre_count} -> {post_count}"
    )
    assert post_ts >= pre_ts, (
        f"updated_at_utc did not advance: {pre_ts} -> {post_ts}"
    )


@pytest.mark.integration
@pytest.mark.live
@pytest.mark.quarantine(
    reason="compares live Spotify state with the database, so it can fail without any code change",
    exit="manual_gate",
)
def test_stored_order_matches_spotify():
    """The track order stored in music.playlist_isrcs equals the playlist's live order
    on Spotify.
    """
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
