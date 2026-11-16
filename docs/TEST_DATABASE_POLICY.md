# PiFitness TEST database selection policy

`tools/db/test_db_policy.py` is a pure decision module. It has no PostgreSQL
connection logic and performs no database or filesystem operations. The
orchestrator supplies verified observations and consumes the decision.

| Verified TEST state | Decision | Preserve existing TEST first? |
|---|---|---:|
| Database absent | `create_from_prd` | No |
| Marker absent or not exactly `test` | `refresh_from_prd` | Yes |
| Marker is `test`, schemas differ | `refresh_from_prd` | Yes |
| Marker is `test`, schemas identical, no refresh requested | `reuse_test` and offer refresh | Yes, before migration/deployment mutation |
| Marker is `test`, schemas identical, operator requested refresh | `refresh_from_prd` | Yes |
| Inspection or schema comparison failed | `abort` | No replacement allowed |

## Meaning of compatible

For this implementation, `compatible` means the normalized structural
comparison reports `identical`. The comparison is not permitted to infer that
an arbitrary difference is explained by a migration. Successful TEST changes
are expected to be promoted to PRD; if a later run finds different structures,
the deterministic policy refreshes TEST from PRD. Any future exception for a
TEST schema intentionally ahead of PRD must be backed by verifiable migration
lineage/integrity metadata, not guessed from SQL filenames or error text.

## Refresh interaction

When schemas are identical, the default is `reuse_test`; the orchestrator
should offer the operator an optional PRD refresh. A non-interactive invocation
should default to reuse unless the explicit `--refresh-from-prd` option is
present. Missing TEST, a non-TEST marker, or a structural difference are not
optional refresh cases under this policy.

## Rollback requirement

`requires_rollback_snapshot` is an instruction to the orchestrator, not a
license to drop the original DB. Before any operation that mutates or replaces
an existing TEST database, it must create and verify a recoverable snapshot.
On deployment success the snapshot can be discarded. On failure the exact
pre-deployment TEST database must be restored. If the snapshot cannot be
created or verified, deployment must stop before changing TEST.

An inspection or comparator error is not evidence of a schema mismatch. It
must abort rather than trigger an automatic clone.
