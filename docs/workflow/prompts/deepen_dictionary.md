# Prompt: Deepen Data Dictionary (3.3 COMMENT backfill + 3.4.x)

You are working in the PiFitness repo (DEV DB only, localhost). Complete tasks 3.3 (COMMENT backfill) and 3.4.x (data dictionary deepening) with high quality.

## Goal
For every domain, produce:
- DDL comments on tables/columns (migration via dbmate), and
- A dictionary entry per table/view with real Grain, Canonical source, and Rules (enforced vs convention + one-line why).

## Process (per domain)
1. **Schema inventory.** From `docs/data/schema.generated.md`, list tables/views in the domain. Cross-check with `\d+` via psql on DEV.
2. **DDL deep dive.** For each table/view, capture:
   - Columns, types, PK, FKs, indexes, check constraints (from schema.generated.md and `\d <schema>.<table>`).
   - View definitions (`SELECT definition FROM pg_views`) and any stored functions/procedures in the schema.
   - TimescaleDB specifics: hypertables, continuous aggregates, compression policies.
3. **Data sampling.** Run `SELECT * ... LIMIT 5` (and counts, distinct values for key columns) on DEV to understand actual content and cardinality. Never on PRD.
4. **Code provenance.** Search `backend/` and migrations for each table name:
   - ORM models, queries, inserts/updates, migrations.
   - Endpoints that read/write it; background tasks/schedulers that touch it.
   - Frontend consumers if relevant.
5. **Infer grain & canonical source.** Grain = one row per what. Canonical source = which table/view is authoritative for a concept; note duplicates or derived views.
6. **Rules.** Mark `enforced` (DB constraint) vs `convention` (app agreement), with one-line why. Include cross-table invariants found in code.
7. **COMMENT backfill (3.3).** Draft comments for columns lacking them; apply via a dbmate migration per domain batch; human reviews before PRD.
8. **Dictionary write (3.4.x).** Replace stub entries for the domain with the real content. Keep references to schema.generated.md for columns.
9. **Verification.** Regenerate schema.generated.md and diff (no drift); run `dd.py find <term>` sanity check; human review.

## Guardrails
- DEV DB only; verify PG_HOST=localhost and marker `pifitness.env=dev`.
- Do not modify PRD. Migrations additive/backward compatible.
- One domain per commit; paste command + last 20 output lines to PROGRESS.md.
