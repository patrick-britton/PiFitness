# Data dictionary tooling

All scripts are read-only with respect to PostgreSQL. Run `python <script> --help` for options.

| Script | Purpose | Database required |
|---|---|---:|
| `schema_inventory.py inventory` | Parse generated Markdown into an exact object/column inventory JSON | No |
| `schema_inventory.py compare` | Compare dictionary object headings with the generated inventory | No |
| `repo_refs.py` | Find candidate references to schema objects in repository files | No |
| `audit.py` | Audit object coverage, duplicates, placeholders, column-table coverage, and SQL comments | No |
| `db_catalog.py` | Capture local DEV relations, columns, constraints, indexes, view/routine/trigger definitions, enum labels, and optional schema-only `pg_dump` DDL | Yes |
| `profile_data.py` | Aggregate safe low-cardinality categories from sampled blocks; never outputs raw rows | Yes |

Database commands require `--env-file backend/.env` and must resolve to exactly `localhost:15432`, `pifitness_dev`, user `dev`. `psycopg` or `psycopg2` must already be installed. Do not relax the allowlist for convenience.

## Expected sequencing

1. `schema_inventory.py inventory`
2. `repo_refs.py`
3. `db_catalog.py` (only after verifying current local DEV config)
4. `profile_data.py` (optional; only if safe categorical evidence would materially help)
5. `audit.py` after each dictionary batch
