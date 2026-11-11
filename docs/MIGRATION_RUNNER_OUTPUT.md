# PiFitness migration runner output contract

The migration runner now supports human-readable output (default) and JSON Lines for deployment orchestration.

## Commands

```bash
python tools/db/migrate.py status --env-file PATH --format jsonl
python tools/db/migrate.py up --env-file PATH --format jsonl
```

`--env-file` points directly to the existing application configuration; it does not require a separate migration-specific file. On the Pi, use:

- TEST: `/home/god/.config/pifitness/test.env` (`APP_ENV=test`)
- PRD: `/home/god/PiFitness/backend/.env` (`APP_ENV=prod`)

For example:

```bash
python tools/db/migrate.py status --env-file /home/god/.config/pifitness/test.env --format jsonl
python tools/db/migrate.py up --env-file /home/god/.config/pifitness/test.env --format jsonl
python tools/db/migrate.py status --env-file /home/god/PiFitness/backend/.env --allow-prd --format jsonl
```

PRD still requires `--allow-prd`. The explicit `--env-file` requirement and all host, database identity, port, and marker checks remain in force. Each command reads the connection settings and `PG_PASSWORD` from the selected existing file; inherited shell values do not override them.

## JSON Lines events

`status` emits one `migration_status` object per migration, followed by `migration_summary`. Status records include version, filename, pending/applied status, SHA-256 checksum, and integrity status. Summary includes total/applied/pending counts, legacy checksum versions, integrity errors, `has_pending`, and `safe_to_apply`.

`up` emits `migration_started` before attempting a file, then either `migration_succeeded` or `migration_failed`. A failure record includes the version, exact filename, SHA-256, exception type, PostgreSQL SQLSTATE when available, message, and any rollback error. A final summary identifies the failed file. The process exits nonzero on failure.

Deployment orchestration should parse stdout as JSONL and use the process exit code as the final success/failure signal. Stderr is reserved for human-readable fatal diagnostics.

## Checksum ledger upgrade

Newly applied migrations store version, filename, and SHA-256 in `schema_migrations`. Checksums are computed over migration contents after normalizing CRLF and lone CR line endings to LF; all other file content remains significant. This makes the checksum stable across Windows and Linux checkouts. Existing ledgers are upgraded with nullable `filename` and `checksum` columns only during a write-capable command; `status` remains read-only.

Previously applied rows from the old ledger have no trustworthy checksum. `status` labels these `legacy_unverified` and sets `safe_to_apply` false. `up` refuses to proceed until an operator has reviewed the current migration files and explicitly runs:

```bash
python tools/db/migrate.py up --env-file PATH --adopt-legacy-checksums
```

That option records the current filename/hash for existing applied rows. It is an explicit one-time trust decision; it is not automatically performed by `status` or ordinary `up`.

Applied migrations with a recorded filename/checksum that no longer match the checked-out package are rejected. Applied versions absent from the package are also rejected. A numbered SQL file without a non-empty `-- migrate:up` section is a hard failure associated with that file; it is no longer silently skipped.
