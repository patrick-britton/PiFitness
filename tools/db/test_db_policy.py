#!/usr/bin/env python3
"""Pure decision policy for choosing whether to reuse or refresh PiFitness TEST.

This module deliberately performs no database connections, DDL, file access,
or migration SQL inspection. The deployment orchestrator supplies verified
observations from the TEST database inspector and schema comparator.

Policy:
- Missing TEST database -> create from PRD.
- Missing/wrong TEST marker -> preserve old TEST for rollback, then refresh.
- Different normalized schemas -> preserve old TEST for rollback, then refresh.
- Identical schemas -> reuse unless operator explicitly requests PRD freshness.
- Inspection/comparison error -> abort; never interpret an inability to inspect
  as evidence that it is safe to replace the database.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

SchemaStatus = Literal["identical", "different", "error"]
Action = Literal["create_from_prd", "refresh_from_prd", "reuse_test", "abort"]


@dataclass(frozen=True)
class DatabaseObservation:
    """Facts established by read-only TEST inspection and schema comparison."""

    exists: bool
    marker: str | None = None
    schema_status: SchemaStatus | None = None
    inspection_error: str | None = None


@dataclass(frozen=True)
class TestDatabaseDecision:
    """Deterministic policy result; safe for JSON serialization/logging."""

    action: Action
    reason: str
    offer_refresh: bool
    requires_rollback_snapshot: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def decide_test_database(
    observation: DatabaseObservation,
    *,
    refresh_from_prd: bool = False,
) -> TestDatabaseDecision:
    """Choose TEST reuse/refresh without making any database changes.

    `schema_status` is the normalized schema comparator result. For an
    existing, correctly marked TEST DB it must be `identical`, `different`,
    or `error`. Marker mismatch is decided before schema comparison, because
    the normal comparator intentionally refuses a non-TEST marker.

    A mismatched marker or schema is not permission to immediately drop the
    original DB. `requires_rollback_snapshot=True` tells the orchestrator to
    preserve it first. If observation failed, action is `abort`, not refresh.
    """
    if observation.inspection_error:
        return TestDatabaseDecision(
            action="abort",
            reason="TEST database inspection failed: %s" % observation.inspection_error,
            offer_refresh=False,
            requires_rollback_snapshot=False,
        )

    if not observation.exists:
        return TestDatabaseDecision(
            action="create_from_prd",
            reason="TEST database does not exist.",
            offer_refresh=False,
            requires_rollback_snapshot=False,
        )

    if observation.marker != "test":
        return TestDatabaseDecision(
            action="refresh_from_prd",
            reason="TEST database marker is %r, not exactly 'test'." % observation.marker,
            offer_refresh=False,
            requires_rollback_snapshot=True,
        )

    if observation.schema_status == "error" or observation.schema_status is None:
        return TestDatabaseDecision(
            action="abort",
            reason=(
                "TEST/PRD schema compatibility could not be established; "
                "refusing to refresh or reuse TEST."
            ),
            offer_refresh=False,
            requires_rollback_snapshot=False,
        )

    if observation.schema_status == "different":
        return TestDatabaseDecision(
            action="refresh_from_prd",
            reason="TEST and PRD normalized schemas differ.",
            offer_refresh=False,
            requires_rollback_snapshot=True,
        )

    if observation.schema_status != "identical":
        return TestDatabaseDecision(
            action="abort",
            reason="Unsupported schema comparison status %r." % observation.schema_status,
            offer_refresh=False,
            requires_rollback_snapshot=False,
        )

    if refresh_from_prd:
        return TestDatabaseDecision(
            action="refresh_from_prd",
            reason="Operator explicitly requested a fresh clone from PRD.",
            offer_refresh=False,
            requires_rollback_snapshot=True,
        )

    return TestDatabaseDecision(
        action="reuse_test",
        reason="TEST and PRD normalized schemas are identical; preserve TEST state.",
        offer_refresh=True,
        requires_rollback_snapshot=True,
    )
