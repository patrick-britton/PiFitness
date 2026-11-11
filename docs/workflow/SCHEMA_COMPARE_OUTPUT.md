# TEST/PRD Schema Comparison

`tools/db/compare_schema.py` is a read-only structural comparator. It does not
read or parse migration SQL, does not alter either database, and does not
apply migrations.

## Invocation

```bash
python tools/db/compare_schema.py \
  --test-env-file /home/god/.config/pifitness/test-migrate.env \
  --prd-env-file /home/god/.config/pifitness/prd-migrate.env \
  --allow-prd \
  --diff-file /home/god/.local/state/pifitness/schema-diff-<run-id>.diff
```

The TEST env file must set `APP_ENV=test`; the PRD env file must set
`APP_ENV=prod`. Both must target distinct databases on the same local
PostgreSQL endpoint. TEST must carry `pifitness.env='test'`; PRD must remain
unmarked. PRD inspection requires explicit `--allow-prd` even though this
operation is read-only.

## Output contract

One JSON object is written to stdout:

- `ok`: whether comparison completed safely
- `status`: `identical`, `different`, or `error`
- `test` / `prd`: verified database and server identity (never credentials)
- `test_fingerprint` / `prd_fingerprint`: SHA-256 of normalized schema dumps
- `difference_line_count` and `diff_preview`: concise structural diff
- `scope`: objects included and intentionally excluded
- `diff_file`: included when a full diff artifact was requested

Exit code 0 means the comparison completed. It does not mean the schemas are
equal; callers must inspect `status`. Exit code 1 means the comparison could
not be completed safely.

## Comparison semantics

The comparator invokes `pg_dump --schema-only` for each database. It excludes
object ownership, ACLs/grants, comments, tablespace placement, and
`schema_migrations` bookkeeping;
it includes schema-level DDL such as tables, columns, defaults, constraints,
indexes, sequences, views, routines, types, triggers, row-security policies,
and extensions as represented by `pg_dump`.

It compares normalized dump output, not table data. Volatile version/date
headers, pg_dump restriction tokens, line-ending differences, and trailing
line whitespace are normalized. Other DDL differences remain significant.

Database roles and role memberships are not part of a database schema dump;
those must be validated separately by the post-clone role/privilege provisioner.
This utility is read-only and does not decide whether the deployment should
reuse TEST or refresh it. That policy belongs to the deployment orchestrator.
