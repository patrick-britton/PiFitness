#!/usr/bin/env python3
"""Pure decision policy for reusing or rebuilding PiFitness TEST.

A TEST database that fails identity, permissions, or schema checks is replaced
from the latest validated PRD backup. The old TEST database is not retained
as a rollback copy. An identical compatible database is reused by default,
with an optional operator choice to refresh its data from the nightly backup.
"""

from __future__ import annotations

# This is an application policy module, not a pytest test module.
__test__ = False

from dataclasses import asdict, dataclass
from typing import Literal

SchemaStatus = Literal["identical", "different", "error"]
Action = Literal["create_from_prd", "refresh_from_prd", "reuse_test", "abort"]


@dataclass(frozen=True)
class DatabaseObservation:
    """Facts established by read-only TEST inspection and schema comparison."""
    exists: bool
    marker: str | None = None
    permissions_ok: bool | None = None
    schema_status: SchemaStatus | None = None
    baseline_provenance_valid: bool = False
    inspection_error: str | None = None


@dataclass(frozen=True)
class TestDatabaseDecision:
    """Deterministic policy result; safe for JSON serialization/logging."""
    action: Action
    reason: str
    offer_refresh: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def decide_test_database(
    observation: DatabaseObservation, *, refresh_from_prd: bool = False,
) -> TestDatabaseDecision:
    """Choose TEST reuse/rebuild without making any database changes."""
    if observation.inspection_error:
        return TestDatabaseDecision(
            action="abort",
            reason="TEST database inspection failed: %s" % observation.inspection_error,
            offer_refresh=False,
        )
    if not observation.exists:
        return TestDatabaseDecision(
            action="create_from_prd", reason="TEST database does not exist.", offer_refresh=False,
        )
    if observation.marker != "test":
        return TestDatabaseDecision(
            action="refresh_from_prd",
            reason="TEST database marker is %r, not exactly 'test'." % observation.marker,
            offer_refresh=False,
        )
    if observation.permissions_ok is False:
        return TestDatabaseDecision(
            action="refresh_from_prd",
            reason="TEST database ownership or required application-role permissions are incorrect.",
            offer_refresh=False,
        )
    if observation.permissions_ok is not True:
        return TestDatabaseDecision(
            action="abort",
            reason="TEST database permissions could not be established safely.",
            offer_refresh=False,
        )
    if observation.baseline_provenance_valid:
        if refresh_from_prd:
            return TestDatabaseDecision(
                action="refresh_from_prd",
                reason="Operator explicitly requested a fresh PRD data baseline, replacing the migration-managed TEST database.",
                offer_refresh=False,
            )
        return TestDatabaseDecision(
            action="reuse_test",
            reason=(
                "TEST matches a previously verified PRD backup baseline, and its migration "
                "ledger is safe against the current package; reuse it without re-restoring "
                "the large PRD backup."
            ),
            offer_refresh=False,
        )
    if observation.schema_status == "error" or observation.schema_status is None:
        return TestDatabaseDecision(
            action="abort",
            reason=(
                "TEST/PRD schema compatibility could not be established; "
                "refusing to refresh or reuse TEST."
            ),
            offer_refresh=False,
        )
    if observation.schema_status == "different":
        return TestDatabaseDecision(
            action="refresh_from_prd", reason="TEST and PRD normalized schemas differ.",
            offer_refresh=False,
        )
    if observation.schema_status != "identical":
        return TestDatabaseDecision(
            action="abort", reason="Unsupported schema comparison status %r." % observation.schema_status,
            offer_refresh=False,
        )
    if refresh_from_prd:
        return TestDatabaseDecision(
            action="refresh_from_prd", reason="Operator explicitly requested a fresh PRD data baseline.",
            offer_refresh=False,
        )
    return TestDatabaseDecision(
        action="reuse_test",
        reason="TEST marker, ownership, permissions, and schema are valid; reuse TEST by default.",
        offer_refresh=True,
    )
