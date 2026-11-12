"""Tests for the pure TEST database reuse/rebuild policy."""
import pytest

from tools.db.test_db_policy import DatabaseObservation, decide_test_database

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("observation", "refresh", "action", "offer"),
    [
        (DatabaseObservation(exists=False), False, "create_from_prd", False),
        (DatabaseObservation(exists=True, marker=None), False, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="dev"), False, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="",), False, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=False), False, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="different"), False, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="identical"), False, "reuse_test", True),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="identical"), True, "refresh_from_prd", False),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="error"), False, "abort", False),
        (DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status=None), False, "abort", False),
        (DatabaseObservation(exists=True, marker="test", schema_status="identical"), False, "abort", False),
    ],
)
def test_decision_matrix(observation, refresh, action, offer):
    decision = decide_test_database(observation, refresh_from_prd=refresh)
    assert decision.action == action
    assert decision.offer_refresh is offer
    assert decision.reason


def test_inspection_error_aborts_even_when_refresh_was_requested():
    decision = decide_test_database(
        DatabaseObservation(
            exists=True, marker="test", permissions_ok=True, schema_status="identical",
            inspection_error="connection identity could not be verified",
        ),
        refresh_from_prd=True,
    )
    assert decision.action == "abort"
    assert not decision.offer_refresh


def test_missing_database_does_not_require_an_old_database_snapshot():
    decision = decide_test_database(DatabaseObservation(exists=False))
    assert decision.action == "create_from_prd"


def test_wrong_marker_refreshes_without_preserving_old_database():
    decision = decide_test_database(DatabaseObservation(exists=True, marker="prod"))
    assert decision.action == "refresh_from_prd"
    assert not decision.offer_refresh


def test_compatible_test_offers_freshness_but_defaults_to_reuse():
    decision = decide_test_database(
        DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="identical")
    )
    assert decision.action == "reuse_test"
    assert decision.offer_refresh


def test_unknown_schema_status_fails_closed():
    decision = decide_test_database(
        DatabaseObservation(exists=True, marker="test", permissions_ok=True, schema_status="unknown")  # type: ignore[arg-type]
    )
    assert decision.action == "abort"


def test_verified_backup_provenance_reuses_test_even_when_schema_differs_from_live_prd():
    decision = decide_test_database(
        DatabaseObservation(
            exists=True,
            marker="test",
            permissions_ok=True,
            schema_status=None,
            baseline_provenance_valid=True,
        )
    )
    assert decision.action == "reuse_test"
    assert decision.offer_refresh is True
    assert "ledger is safe" in decision.reason
