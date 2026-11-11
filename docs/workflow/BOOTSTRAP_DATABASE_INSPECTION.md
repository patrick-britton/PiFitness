# TEST bootstrap: read-only database inspection integration

## Scope of this increment

This increment wires the existing read-only TEST/PRD database inspector and
pure database policy into `deploy/scripts/bootstrap.sh`. It does not yet
restore a database, apply migrations, run the regression suite, suspend or
restart services/timers, or implement the GO/NO-GO acceptance window. Those
remain later workflow stages and must preserve the operator-approved order.

## Configuration

The script uses only the two existing environment files:

- TEST: `/home/god/.config/pifitness/test.env`
- PRD: `/home/god/PiFitness/backend/.env`

No migration-specific `.env` files or PostgreSQL roles are created.

## Execution added

After the candidate worktree is built and MigraDiff 1.7.2 is installed into
the persistent TEST venv from the pinned
`tools/db/requirements-schema-compare.txt`, bootstrap runs:

```bash
"$VENV_DIR/bin/python" "$STAGE_DIR/tools/db/inspect_test_db.py" \
    --test-env-file "$ENV_FILE" \
    --prd-env-file "$PRD_ENV_FILE" \
    --allow-prd \
    --diff-file "$SCHEMA_DIFF_FILE"
```

The inspector verifies both database identities and environment markers,
then compares TEST to PRD using MigraDiff in read-only mode. The JSON report
and full schema diff are retained under:

```text
/home/god/Documents/PiFitness_Test_Evidence
```

with private file/directory permissions.

## Decision handling in this increment

- `reuse_test`: prompt for optional freshness refresh. A `no` response allows
  the checkout-preparation step to continue. A `yes` response exits safely
  because backup restore and rollback are not implemented in this increment.
- `create_from_prd`: exits safely; the missing-TEST restore workflow is a
  subsequent increment.
- `refresh_from_prd`: exits safely; the latest-nightly-backup restore and
  rollback workflow is a subsequent increment.
- `abort`, malformed output, failed connection, or comparison error: exits
  nonzero and leaves the existing TEST database and checkout untouched.

No database DDL, database rename/drop, migration execution, or service action
is performed by this database-inspection stage.

## Checkout failure handling

The EXIT cleanup trap removes only the staging worktree. It never promotes a
partially prepared staging checkout after a failure. Promotion of the staging
worktree into the TEST path occurs only after all checks and the database
inspection decision have passed.

## Validation

Run locally:

```powershell
& .\.venv\Scripts\python.exe -m pytest tests\test_bootstrap_database_inspection.py -q
```

On the Pi, the first integration check should use the current expected state
(TEST and PRD schemas compatible), choose **no** to the optional refresh, and
verify that the JSON report says `reuse_test` and no database/service state
was modified by the inspector.
