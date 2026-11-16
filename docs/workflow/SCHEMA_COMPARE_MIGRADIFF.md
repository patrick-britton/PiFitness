# PiFitness schema comparison — MigraDiff integration

## Dependency

`tools/db/requirements-schema-compare.txt` pins `migradiff[pg]==1.7.2`.
The package distribution is named `migradiff`; its console command is `migra`.
This wrapper requires exactly 1.7.2 and refuses other versions until reviewed.

Install in the Python environment that runs the comparison:

```powershell
& .\.venv\Scripts\python.exe -m pip install -r tools\db\requirements-schema-compare.txt
```

## Execution

```powershell
& .\.venv\Scripts\python.exe tools\db\compare_schema.py `
  --test-env-file C:\path\to\test-migrate.env `
  --prd-env-file C:\path\to\prd-migrate.env `
  --allow-prd `
  --diff-file C:\path\to\private-evidence\schema-diff.json
```

The command checks the TEST and PRD endpoint identities and markers using the
existing migration-runner safety logic. It then invokes MigraDiff against the
live local databases. Passwords are placed in a temporary libpq password file
(mode 0600 on POSIX) and are never put into command-line arguments or logs.
The password file is removed immediately after MigraDiff exits.

The command is read-only. It requests MigraDiff JSON output and uses
`--force-destructive` only so that destructive structural differences are
reported instead of making the comparison exit early. It does **not** pass
`--apply`, and generated SQL is never executed.

## Interpretation

- `status=identical`: no structural differences remain after exclusions.
- `status=different`: one or more structural differences were found.
- `status=error`: comparison could not be completed safely. The caller must
  abort rather than treating an error as permission to refresh TEST.

The wrapper excludes `COMMENT ON` statements and the `schema_migrations`
bookkeeping table. It does not ask MigraDiff to compare ACLs/privileges. Database
roles and role memberships are cluster-level objects and are verified by the
separate role-provisioning process.

The comparison is directional: it asks MigraDiff for the SQL that would
transform TEST toward PRD. The SQL is retained only as diagnostic evidence.

## Package release and licensing notes

The integration pins version 1.7.2. The PyPI wheel SHA-256 published for that
release is:

`9e23921ac4bf31a5666e84d482880ddde36813bc0e2d3bd4ae87cdc76c72c24f`

PyPI currently reports that the 1.7.2 wheel has no provenance attestation.
This is why the version is pinned rather than left open-ended. Verify the wheel
hash during dependency intake if a fully hash-locked deployment requirements
file is introduced.

There is a licensing inconsistency worth recording before commercial use:
PyPI metadata identifies MIT, the repository `LICENSE` file contains the
Unlicense, and the repository's `LICENSING.md` describes an additional Business
License Agreement for teams shipping to production commercially. This document
is not legal advice. Confirm applicability for the intended use before adopting
MigraDiff in a commercial production workflow.

## Scope of this step

This is only the comparison-engine integration. It does not yet change
`test-up.sh`, database reuse/refresh decisions, database provisioning, or
rollback behavior. Those will be integrated after the comparator passes local
unit tests and a controlled read-only comparison against the actual TEST/PRD
PostgreSQL instances.
