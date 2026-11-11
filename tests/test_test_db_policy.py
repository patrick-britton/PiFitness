"""Tests for the pure TEST database reuse/refresh policy."""

import pytest

from tools.db.test_db_policy import (
    DatabaseObservation,
    decide_test_database,
)

# Every test in this module is a deterministic, pure decision-logic test.
pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("observation", "refresh", "action", "snapshot", "offer"),
    [
        (DatabaseObservation(exists=False), False, "create_from_prd", False, False),
        (DatabaseObservation(exists=True, marker=None), False, "refresh_from_prd", True, False),
        (DatabaseObservation(exists=True, marker="dev"), False, "refresh_from_prd", True, False),
        (DatabaseObservation(exists=True, marker=""), False, "refresh_from_prd", True, False),
        (DatabaseObservation(exists=True, marker="test", schema_status="different"), False, "refresh_from_prd", True, False),
        (DatabaseObservation(exists=True, marker="test", schema_status="identical"), False, "reuse_test", True, True),
        (DatabaseObservation(exists=True, marker="test", schema_status="identical"), True, "refresh_from_prd", True, False),
        (DatabaseObservation(exists=True, marker="test", schema_status="error"), False, "abort", False, False),
        (DatabaseObservation(exists=True, marker="test", schema_status=None), False, "abort", False, False),
    ],
)
def test_decision_matrix(observation, refresh, action, snapshot, offer):
    decision = decide_test_database(observation, refresh_from_prd=refresh)
    assert decision.action == action
    assert decision.requires_rollback_snapshot is snapshot
    assert decision.offer_refresh is offer
    assert decision.reason


def test_inspection_error_aborts_even_when_refresh_was_requested():
    decision = decide_test_database(
        DatabaseObservation(
            exists=True,
            marker="test",
            schema_status="identical",
            inspection_error="connection identity could not be verified",
        ),
        refresh_from_prd=True,
    )
    assert decision.action == "abort"
    assert not decision.requires_rollback_snapshot


def test_missing_database_does_not_require_a_rollback_snapshot():
    decision = decide_test_database(DatabaseObservation(exists=False))
    assert decision.action == "create_from_prd"
    assert not decision.requires_rollback_snapshot


def test_wrong_marker_refresh_requires_preserving_existing_database_first():
    decision = decide_test_database(
        DatabaseObservation(exists=True, marker="prod")
    )
    assert decision.action == "refresh_from_prd"
    assert decision.requires_rollback_snapshot
    assert not decision.offer_refresh


def test_compatible_test_offers_freshness_but_defaults_to_reuse():
    decision = decide_test_database(
        DatabaseObservation(exists=True, marker="test", schema_status="identical")
    )
    assert decision.action == "reuse_test"
    assert decision.offer_refresh
    assert decision.requires_rollback_snapshot


def test_unknown_schema_status_fails_closed():
    decision = decide_test_database(
        DatabaseObservation(exists=True, marker="test", schema_status="unknown")  # type: ignore[arg-type]
    )
    assert decision.action == "abort"
