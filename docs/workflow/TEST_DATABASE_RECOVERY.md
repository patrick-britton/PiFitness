# TEST database recovery from the PRD nightly backup

## Policy

The read-only TEST inspector selects recovery when the TEST database is missing,
its `pifitness.env` marker is missing/wrong, required application-role ownership
or privileges are incorrect, or its normalized schema is incompatible with PRD.
A compatible TEST database is reused by default; the bootstrap offers an optional
fresh-data restore from the newest PRD nightly backup. The user may choose that
refresh when the TEST data is too old even if its schema is compatible.

Recovery is a **direct replacement**, not a second-database staging swap. The
old TEST database has already been classified as unsuitable. Keeping a second
1.2-GB database during restore would add disk consumption and another large I/O
pass without improving the desired final state. The helper validates the backup
and non-destructive PostgreSQL prerequisites before dropping TEST. It does not
dump the existing TEST database and does not create a new PRD backup.

## Backup selection and preflight

Defaults follow the established TEST deployment convention:

- Directory: `~/Documents/DB_Backups`
- Filename pattern: `pifitness_prd_*.dump`
- Format: PostgreSQL custom archive (`pg_dump -Fc`)
- Source database recorded in the archive: `personal_fitness`
- Maximum age: 24 hours

Bootstrap can override the directory and filename pattern with
`PIFITNESS_PRD_BACKUP_DIR` and `PIFITNESS_PRD_BACKUP_GLOB`. The timestamp in
`pifitness_prd_YYYYMMDD_HHMMSS.dump` is interpreted in the Pi's local timezone,
matching the existing nightly job's use of `date`. A filename without that
stamp uses file modification time.

Only the newest matching archive is considered. Before dropping TEST, recovery
requires that it exists, is a non-empty regular file (not a symlink), is no
older than 24 hours, passes `pg_restore --list`, identifies the configured PRD
database in its archive header, and is a custom-format archive. It also checks
that the configured PRD user is a PostgreSQL superuser, the TEST role exists,
is LOGIN-enabled and is not a superuser, PRD and TEST point to the same local
server/port, the PRD database exists, and every PRD extension version is
available on that server. When the TEST role has CONNECT on the maintenance
`postgres` database, its configured credentials are also tested before TEST is
dropped; otherwise they are verified against TEST after restore.

If any preflight check fails, TEST has not been dropped.

## Direct replacement sequence

After preflight succeeds, the helper:

1. Drops the existing TEST database (if present), terminating existing sessions.
2. Creates `personal_fitness_test` from `template0`, preserving PRD encoding and
   locale settings and assigning the configured TEST role as database owner.
3. If PRD uses TimescaleDB, creates the same installed extension version and
   calls `timescaledb_pre_restore()` before `pg_restore`, then
   `timescaledb_post_restore()` after it. These are the hooks used by the
   existing `test-up.sh` archive-restore path.
4. Restores the selected PRD archive using the configured PRD administrator
   credentials, without importing source ownership or ACLs.
5. Reassigns restored objects owned by the restore administrator to the TEST
   application role, sets the `public` schema and database owner to that role,
   and grants database CONNECT/TEMPORARY plus schema USAGE/CREATE.
6. Sets `pifitness.env = 'test'` on the TEST database. PRD remains unmarked.
7. Opens a fresh connection using the TEST environment file and verifies the
   actual database/server/port, marker, application role, database/schema
   ownership, required privileges, and a rolled-back DDL probe.
8. Records the new database OID and the archive path, timestamp, size, and
   SHA-256 in a private recovery journal.

No manual marker SQL is required. Recovery stamps and verifies the marker as
part of the sequence; a marker is never added to the existing suspect database
just to bypass inspection.

## Schema comparison and the restored baseline

The live-PRD schema comparison is recorded as **diagnostic evidence**, not a
hard gate for accepting the backup. A nightly archive can be less than 24 hours
old and still predate a schema change subsequently applied to live PRD. Rejecting
that archive solely because live PRD is newer could discard a valid snapshot and
leave TEST without a usable baseline. The trusted provenance checks are the
archive source identity and age, successful archive restore, TimescaleDB restore
hooks (when applicable), TEST-specific ownership/permissions, marker, and a
fresh-connection DDL probe.

The comparison result (`identical`, `different`, or `error`) is retained in the
journal for diagnosis. It does not undo a successful restore. The application
migration ledger determines whether subsequent schema changes are known and
safe to resume.

## Failure and retry behavior

The old TEST database is intentionally **not preserved**. If `pg_restore`,
provisioning, marker verification, or the DDL verification fails, the helper
removes the partial replacement. TEST may then be absent until the next run
retries from a validated PRD backup. This is preferable to leaving a partially
restored database looking usable or restoring the already-rejected old TEST
database.

A private mode-0600 journal is written atomically in the bootstrap evidence
directory. It records the source archive path, timestamp, byte size and SHA-256,
PostgreSQL endpoint, prior TEST database OID (for evidence only), replacement
OID, recovery phase, verification, and diagnostic schema comparison. It
contains no database passwords.

Once the journal reaches `baseline_ready`, the PRD-derived TEST database is the
new canonical baseline. A later checkout/build/migration/test/bootstrap failure
must not drop it or reload PRD. On the next run, recovery reconciliation checks
the database OID and re-verifies marker/ownership/permissions. Incomplete
restore journals may clean up only a partial database whose OID matches that
journal; an older interrupted journal cannot delete a newer successful TEST
baseline.

## Avoiding another large restore after migrations

The inspector receives the evidence directory and looks for a `baseline_ready`
journal matching the current TEST database OID. It also independently checks
the current marker and grants. When the matching baseline is found, the inspector
runs the existing read-only `migrate.py status` operation—even when no
migrations have yet been applied. If the ledger is safe for the current checkout
and every applied migration filename/checksum matches, it classifies TEST as:

- `restored_baseline` when zero migrations have been applied; or
- `migration_managed` when one or more migrations have been applied.

In either case, it reuses TEST rather than comparing the post-restore schema to
live PRD and cloning the 1.2-GB backup again. A later bootstrap can resume the
pending migrations. A failed migration is rolled back by the runner's per-file
transaction and remains pending for the next attempt; successfully applied
migrations remain recorded with checksums.

If the migration ledger has checksum mismatches, missing migration files, or
legacy rows without verified checksums, inspection aborts. It does not erase the
migration history by automatically restoring PRD. If the current database OID
does not match a completed baseline journal, normal TEST-vs-PRD schema
comparison applies. That handles existing TEST databases that did not originate
from this recovery process.

This retry rule assumes TEST schema changes are made through the checked-in
migration runner. Manual schema edits are not covered by its migration ledger.

## Current scope boundary

This integration performs checkout/dependency/frontend preflight and TEST
identity/schema recovery only. It does not yet apply migrations, run regression
tests, suspend/reactivate PRD services, start TEST services, or run the GO/NO-GO
acceptance window. Those remain later bootstrap stages and are not silently
introduced by this recovery change.

## Verification performed for this update

- Python compilation and `bash -n deploy/scripts/bootstrap.sh` passed.
- Focused unit/contract tests exercise archive validation, direct replacement,
  automatic TEST marker assignment, permissions, TimescaleDB hooks, partial
  restore cleanup, recovery journals, and migration-ledger-aware retries.
- Database connections, `pg_restore`, and TimescaleDB behavior are mocked in
  local tests. This does not replace the next local-project and live-Pi restore
  verification.
