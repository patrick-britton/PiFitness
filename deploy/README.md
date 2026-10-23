# Deployment

This directory contains the SQL that ships to production, plus the tooling to review it before applying.

It is **not** a migration framework. There is no tracking table, no automatic runner, no `alembic upgrade`. Files are applied manually. The discipline below is what makes that safe.

## Directory layout

```
deploy/
├── README.md            # this file
├── list-sql.ps1         # review helper (lists files, checks pairing)
└── sql/                 # forward + rollback SQL files
```

## Naming convention

`NNN-MMM_description.sql`

- `NNN` — feature number (matches `.features/` design docs)
- `MMM` — sequential migration number within that feature (`001`, `002`, …)
- `description` — short `snake_case` label

Rollback files use the same `NNN-MMM` prefix with `rollback` in the description:

| Forward | Rollback |
|---|---|
| `009-009_t17_fix_recency_bucket_ranks.sql` | `009-009_t17_rollback_restore_ranks.sql` |
| `010-005_t02_recipe_box.sql` | `010-005_t02_rollback_recipe_box.sql` |

Not every forward change needs a rollback — additive columns you would never remove are fine without one. Destructive changes (dropped columns, renamed tables, truncated data) **must** have a rollback.

## Workflow

1. **Author both files together.** Write the forward SQL and the rollback at the same time. If you cannot articulate how to undo it, you do not yet understand what it changes.
2. **Test forward against `pifitness_dev`.** This is the local sanitized copy of production running in the `pifitness-dev-db` container on `localhost:15432`. If it errors or produces unexpected results, fix the file before it ever touches prod.
3. **Test rollback against `pifitness_dev`.** Apply the forward change, verify the result, then apply the rollback and verify the state is restored.
4. **Test against a fresh staging restore for destructive changes.** If the migration drops or renames anything, run it against a fresh `pifitness_staging` built from the most recent `prod-latest.dump`. This catches cases where your dev DB has drifted from production.
5. **Apply the same file, unmodified, to production.** Not a "slightly edited" version, not a "prod-specific" variant. The file you tested is the file that runs.
6. **Never edit a file after it has been applied to production.** If the applied SQL was wrong, write a new file that corrects it. Editing history makes it impossible to reason about what ran.

The "same file, unmodified" rule is the entire point. A modified copy for production defeats the test you just ran in step 2.

## Applying to production

```powershell
# Copy the file to the Pi
scp deploy/sql/NNN-MMM_description.sql god@pfpi:/home/god/deploy-pending/

# Apply it
ssh god@pfpi "psql -U god -d personal_fitness -v ON_ERROR_STOP=1 -f /home/god/deploy-pending/NNN-MMM_description.sql"
```

`-v ON_ERROR_STOP=1` is not optional. Without it, `psql` continues past errors and you end up with a half-applied migration and no clear failure signal.

## Pre-deployment review

Run this before every deploy to see what is pending:

```powershell
.\deploy\list-sql.ps1
```

To see only what has changed since your last deploy:

```powershell
.\deploy\list-sql.ps1 -Since '2026-10-23'
```

## What belongs here

- Schema changes (DDL: `CREATE`, `ALTER`, `DROP`)
- Data migrations (`UPDATE`, `INSERT`, backfills)
- Seed data that must exist in production (dimension tables, lookup rows)

## What does not belong here

- One-off diagnostic queries → `scripts/`
- Exploratory or throwaway SQL → nowhere; run it in DataGrip, do not save it
- Anything already applied and past its verification window → delete it, or move to `scripts/history/` if you want a record

## Sanitization note

`pifitness_dev` was restored from `prod-sanitized.dump`, which is a snapshot of production with `_migration.api_tokens` and `_migration.spotify_auth_metadata` truncated. Any migration you test locally runs against real-shaped data, but with no live credentials. If a migration touches either of those tables, test the specific statement in isolation rather than relying on the empty-table behavior.