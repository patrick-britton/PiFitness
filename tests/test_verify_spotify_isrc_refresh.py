"""verify_spotify fails loudly without a Spotify client; the live refresh checks are
quarantined as manual gates.
"""

import pytest

from pifitness.core.db import one_sql_result, sql_to_list
from pifitness.integrations.spotify.provider import get_spotify_client
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

# VERIFIED: Regression — verify_spotify raises RuntimeError without usable Spotify client instead of silent success
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



