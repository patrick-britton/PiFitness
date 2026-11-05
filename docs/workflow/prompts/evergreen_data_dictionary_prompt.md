# Evergreen Cline task: build / resume / complete the application data dictionary

You are responsible for producing a complete, source-backed data dictionary for this repository. This is a large multi-domain task and may span many Cline sessions. Begin by assessing the repository's current state and continue from the durable worklog. Do not assume this is a fresh task; do not restart completed work unnecessarily.

## Mission

Maintain `docs/data/dictionary.md` as a useful reference for future agents and developers. It must explain the business meaning, grain, lifecycle, field semantics, value sets, units, transformations, and cross-layer lineage of the database objects—not merely repeat table/column names or types.

Inputs include:

- `docs/data/schema.generated.md` (generated structural inventory; do not edit by hand)
- current `docs/data/dictionary.md` (draft; audit and repair)
- SQL migrations, including the supplied `013__comment_backfill_staging.sql` if present in the repository
- local DEV database catalog: table/view/materialized-view definitions, constraints, indexes, comments, functions/procedures, triggers, and enum/domain types
- all code that writes, reads, transforms, serializes, or consumes the data: API routes, backend modules, workers, imports, frontend clients, tests, fixtures, and SQL

The database connection is read-only and strictly restricted by `.clinerules/20-database-readonly.md`. Follow that file exactly. Do not access PRD or personal records.

## Phase 0 — assess, never assume

1. Inspect and read every active instruction source visible to Cline: `.clinerules/`, `.cline/rules/`, root `AGENTS.md`, `.cursorrules`, `.windsurfrules`, and (where exposed) Cline global rules / `~/.agents/AGENTS.md`. Identify contradictions, especially task-ID-only scope, one-session limits, commit ceilings, fixed terminal timeouts, forced commits/branches, and database environment contradictions. Workspace rules should take precedence, but do not assume a stale global instruction disappeared. Never modify global rules without explicit authorization.
2. Inspect `git status --short`, current branch, repository root, and relevant directory layout. Preserve pre-existing changes.
3. Locate the current generated schema, data dictionary draft, migration directories, database access configuration, API entry points, worker modules, SQL definitions, and tests. Do not ask the user for paths; search the repository.
4. Read `docs/data/_evidence/DATA_DICTIONARY_WORKLOG.md` if it exists. Determine whether the project is `NOT_STARTED`, `IN_PROGRESS`, `AUDIT_REQUIRED`, or `COMPLETE` based on artifacts and the current schema.
5. Check that `tools/data_dictionary/` exists and tests pass. If missing, install the bundled kit first only when this prompt is being run from a repository that already contains it; otherwise explain the absent tooling and use native read/search safely.
6. Regenerate the structural inventory and run the audit using the scripts' `--help` output as needed. Save JSON under `docs/data/_evidence/`.
7. Compare current artifacts to the worklog and schema. Reopen completed items if the schema or relevant source files changed.

Do not stop after assessment. If the environment is safe and no essential clarification is needed, begin/resume the next batch.

## Phase 1 — establish the authoritative inventory

1. Parse every object in `docs/data/schema.generated.md`; include all schemas and relation kinds emitted by the generator. Do not silently omit system extension objects: classify each as `in-scope`, `extension/system`, `temporary`, or `excluded` and provide a reason for excluded objects.
2. Inspect the live local database catalog only if the connection passes the exact allowlist. Capture current definitions and compare object/column/constraint differences with the generated snapshot. A mismatch must be reported, not silently reconciled by editing the generated file.
3. Inventory all columns, nullability, physical types, defaults, identity/generated columns, keys, FKs, checks, indexes, comments, enum labels, view definitions, function/procedure definitions, and triggers where present.
4. Run repository reference discovery to identify candidate read/write/transform locations for each object. Use results as navigation aids; verify each claim in source.
5. Extract SQL COMMENT metadata and migration changes. Classify generic/boilerplate comments separately from useful business descriptions.

## Phase 2 — reconstruct data flows

For each object or tightly related object group, investigate both directions:

**Writes:** ingress API route / job / worker / import -> validation/normalization -> service/repository/SQL -> table/view/function. Record insert/update/delete/upsert semantics, key assignment, conflict handling, soft delete, transaction boundaries, and relevant ordering/temporal rules.

**Reads:** table/view/function -> repository/service -> API response schema/serializer -> frontend client or report. Identify which object/columns are actually exposed and any aliases, conversions, defaults, or formatting.

Also investigate SQL-layer transformations:

- view dependencies and full view definition;
- function/procedure definition and callers;
- triggers and side effects (describe only; never invoke them);
- staging-to-canonical flows and migrations;
- lookup/reference/enum/check-constraint value mappings;
- calculations, conversions, filters, joins, aggregation grain, window functions, date/time rules, deduplication and sentinel handling.

Use symbol-level code search and inspect surrounding implementation. Do not infer that a field is written because a model defines it, or that a view is used because it exists. Find actual read/write call sites.

## Phase 3 — author the dictionary in bounded batches

Work by schema/domain, usually 5–15 objects per batch. Maintain consistent object and column formats as defined in `.clinerules/10-data-dictionary.md`.

For every object, capture:

- object purpose and type;
- grain (including temporal grain and key dimensions);
- database-enforced PK/FK/unique/check constraints, separately from application conventions;
- canonical source, upstream systems, write paths, update/delete lifecycle;
- downstream consumers and read paths;
- relationships and dependencies;
- operational/business invariants and edge cases;
- all columns with meaning, units, allowed values, null/default/sentinel behavior, transformations, and evidence;
- unresolved questions and confidence.

For views, explain output grain, dependencies, filters/joins/aggregations, derived aliases, and why the view exists. For functions/procedures/triggers, document important behavior and the tables/views they read or mutate. They may get a dedicated section if there is no generated relation entry.

A description such as “identifier”, “value”, or “staging table” is not sufficient unless it is a literal and genuinely complete definition. Do not invent a grain from the object name. Mark unknowns explicitly and cite the search trail.

## Phase 4 — verify and reconcile values

Use structural constraints and SQL/code evidence before data profiles. If needed, use `profile_data.py` only for aggregate, low-cardinality, non-sensitive category values. Never export raw records or inspect health/location/identity fields. Do not infer closed enumerations from a small sample; use `open set`, `observed values`, or `unknown` as appropriate.

Cross-check migration `COMMENT ON` values against actual DDL/code. Record whether each comment is specific, generic, stale, or contradictory. The attached SQL file is not a reason to copy weak autogenerated comments into the dictionary.

## Phase 5 — audit, continue, and finish

After each batch:

1. Update `docs/data/dictionary.md`.
2. Run `schema_inventory.py` and `audit.py`; save machine-readable reports.
3. Review missing, duplicate, unexpected, placeholder, or corrupted headings and missing column documentation.
4. Update the durable worklog with object statuses and evidence references.
5. Inspect the diff to ensure only in-scope files changed.
6. Continue to the next batch if work remains.

At session start and end, ensure the worklog includes:

- status and timestamp;
- inventory source/version or relevant git commit/hash where available;
- counts for objects/columns by schema and kind;
- statuses (`complete`, `partial`, `unknown`, `excluded`) per object;
- evidence files/definitions/endpoints inspected;
- unresolved issues and reason;
- next concrete batch;
- latest audit result and command.

Completion requires exact unique coverage of all in-scope generated objects, column coverage, useful view/routine lineage, documented values/units/rules where evidenced, explicit unknowns, and no boilerplate placeholders or replacement-character corruption. If some objects cannot be fully established after reasonable investigation, finish the rest and leave a precise unresolved register rather than fabricating claims.

## Operating constraints

- Follow `.clinerules/` and especially its strict local DEV read-only rule.
- No schema/application/runtime changes; do not run migrations or stored procedures; do not deploy.
- Do not stage/commit/push or switch branches.
- No plan-task ID, one-task-per-session constraint, or three-file commit ceiling applies to this documentation initiative.
- Use the repository's actual Cline CLI/runtime tools. Do not assume extension-only tools or fixed timeout values.
- Do not repeatedly retry an identical failure. Change the command, reduce scope, or record a blocker and continue with independent work.

## Final handoff

When the audit meets the completion standard, provide a concise completion report with coverage counts by schema and object kind, tools run, remaining unknowns/exclusions, files changed, and exact audit commands/results. Keep the worklog as the durable source of truth. Do not claim full completion if any in-scope object or column is missing.
