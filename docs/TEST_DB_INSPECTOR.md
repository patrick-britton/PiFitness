# PiFitness TEST database inspector

## Purpose

`tools/db/inspect_test_db.py` is a read-only decision-input tool for the
TEST deployment workflow. It observes TEST relative to PRD and invokes the
pure decision policy in `tools/db/test_db_policy.py`. It never creates,
renames, drops, refreshes, or modifies a database and never applies generated
SQL.

## Required inputs

The inspector requires two explicit migration environment files:

- TEST: `/home/god/.config/pifitness/test-migrate.env`
- PRD: `/home/god/.config/pifitness/prd-migrate.env`

Both files must target the same local PostgreSQL endpoint. TEST must have
`APP_ENV=test`; PRD must have `APP_ENV=prod`. Their database names must be
different. The actual database names, server address, and port are verified
from PostgreSQL rather than inferred from file names.

The PRD remains intentionally unmarked. TEST is expected to carry the exact
`pifitness.env='test'` marker.

## Example

Read-only inspection, defaulting to reuse when TEST is structurally identical:

```bash
python tools/db/inspect_test_db.py \
  --test-env-file /home/god/.config/pifitness/test-migrate.env \
  --prd-env-file /home/god/.config/pifitness/prd-migrate.env \
  --allow-prd \
  --diff-file /home/god/Documents/PiFitness_Test_Evidence/schema-diff.json
```

To record an explicit operator preference for refreshing TEST from PRD when
schemas are compatible, add `--refresh-from-prd`. This changes only the
reported decision; the inspector still performs no database modification.

## Decision results

| Observation | Reported action | Schema comparison |
|---|---|---|
| TEST database absent | `create_from_prd` | Not run |
| TEST marker missing or incorrect | `refresh_from_prd` | Not run; normal comparator intentionally rejects wrong markers |
| TEST marker correct; schema identical | `reuse_test`, with `offer_refresh=true` | Run |
| TEST marker correct; schema different | `refresh_from_prd` | Run |
| Identity, connection, or comparison error | `abort` | Not interpreted as permission to refresh |

A `refresh_from_prd` decision for an existing TEST database has
`requires_rollback_snapshot=true`. The later deployment orchestrator must
preserve the current TEST database before it replaces anything.

A database that exists but disallows connections, or is a PostgreSQL template,
is treated as an inspection error. If the TEST migration identity cannot
connect to inspect a wrong/missing marker, the inspector aborts; it does not
assume that the database is safe to replace.

## Read-only guarantees

- Both direct inspection sessions use `connection.set_session(readonly=True)`.
- MigraDiff is run without `--apply`.
- The MigraDiff child process receives
  `PGOPTIONS=-c default_transaction_read_only=on` so its own PostgreSQL
  transactions are read-only as well.
- The only SQL issued by this inspector is `SELECT` against database identity
  and the PostgreSQL database catalog.
- TEST and PRD must resolve to the same loopback server address and port.
- PRD must be unmarked; TEST marker is observed without applying the TEST
  marker guard so a wrong marker can be reported to the policy.

## Output and exit status

The inspector emits one JSON object with `event=test_database_inspection`.
The `decision.action` is one of:

- `create_from_prd`
- `refresh_from_prd`
- `reuse_test`
- `abort`

Exit code is `0` for a completed decision and `1` when inspection cannot be
completed safely or the decision is `abort`.

The command does not display credentials. The optional diff file is delegated
to the comparator, which creates it exclusively with restrictive permissions
and does not overwrite an existing file.

## Deliberately out of scope

The inspector does not provision cluster roles, database ownership, schema
privileges, or the TEST marker; it does not preserve/restore TEST; and it does
not perform migrations or service deployment. Those operations belong to the
subsequent provisioning and orchestration components.
