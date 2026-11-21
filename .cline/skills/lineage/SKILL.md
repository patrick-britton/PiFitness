---
name: lineage
description: For every /lineage request or question about how a database table, view, column, endpoint, or feature data flow is populated, transformed, consumed, or exposed, activate this skill as the FIRST tool action. Query the pre-built lineage graph before repository search, file reads, grep, or schema-document exploration. Use for all activities.* tables, ETL, and feature-impact investigations.
---

# Data lineage

Use `tools/data_dictionary/lineage.py` to query the pre-built database, backend, API, and frontend dependency graph. The graph is the primary discovery mechanism. Source evidence is retrieved separately and selectively.

## 0. Mandatory first-action and shell rules

For any `/lineage` request or any question covered by this skill:

1. The **first tool action MUST be `skills(lineage)`**. Do not call `search_codebase`, `read_files`, `run_commands`, grep, or another tool first.
2. After activation, the first repository command MUST be `lineage.py describe` for the requested node.
3. Do not perform repository-wide search before `describe` has been run and its coverage/warnings inspected.
4. Once `describe` resolves the node and returns a usable report, do not run broad searches to rediscover relationships already present in the graph.

This skill takes precedence over general instructions to locate repository resources using `search_codebase`. Use the graph first; use `search_codebase` only for a specific gap identified by the report or when a reported source path cannot be resolved.

Environment: Windows PowerShell 5.1. Run from the repository root and use the project's fully qualified Python interpreter:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py describe table:activities.activity_details_distance
```

For lineage CLI calls, execute one plain command per `run_commands` call. Do not append `2>&1`, use PowerShell pipelines, or redirect output to files. Use the CLI's own `--section`, `--direction`, `--max-nodes`, and `--max-items` options to control output. Do not rely on `cd`, variables, or jobs persisting between `run_commands` calls. For commands registered as slow or otherwise covered by `.clinerules/01-terminal-use.md`, follow that file's `run_long.ps1` / `poll_long.ps1` procedure.

## 1. Start with one compact report

Use the exact node ID when the object type is known. For example:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py describe table:activities.activity_details_distance
```

The default report is intentionally compact. Do not add `--include-source` (the `describe` command does not provide source dumps) or increase limits substantially on the first call. Default traversal is bounded; keep the initial request near the defaults (`--depth 8`, `--max-nodes 80`, `--max-items 20`).

The report includes:
- Object identity, columns, primary key, indexes, and comment when captured at build time.
- Population writers, ambiguous ORM persistence, secondary mutations, and unclassified writer relationships.
- Invocation paths for identified writers, separately from data dependencies.
- Upstream data dependencies.
- Downstream database, backend, API, and frontend consumers.
- A bounded relationship map.
- Graph freshness, build coverage, and known warnings.

Do not run separate `search`, `show`, or `trace` commands when `describe` resolves the requested node and returns a usable report.

### Narrow the report only when necessary

Supported directions are `both`, `up`, and `down`. Supported sections are `identity`, `population`, `upstream`, `consumers`, `map`, and `coverage`.

Examples:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py describe table:activities.activity_details_distance --section population,upstream,coverage
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py describe table:activities.activity_details_distance --section consumers,coverage
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py describe table:activities.activity_details_distance --direction down --depth 6
```

`--max-nodes` limits visited nodes per direction; `--max-items` limits entries rendered in each group. Do not start with large limits such as `--max-nodes 300`. Increase limits only when the report explicitly indicates neighborhood truncation and the missing scope matters to the question.

Use `--format json` when structured output materially helps. Text is the default and usually best for agent interpretation.

## 2. Retrieve source evidence selectively

After `describe`, identify the few writers, mutators, or consumers whose implementation details matter. Use `evidence` for those specific nodes or relationships.

Population example:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py evidence procedure:activities.resample_activity_to_distance --related-to table:activities.activity_details_distance --operation INSERT --max-lines 120 --max-items 6
```

Secondary mutation example:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py evidence procedure:activities.assign_elevation_reference_distance --related-to table:activities.activity_details_distance --operation UPDATE --max-lines 120
```

Consumer example:

```powershell
C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py evidence <consumer-node-id> --related-to table:activities.activity_details_distance --max-lines 160
```

`--operation` accepts comma-separated operation names, and `--related-to` restricts evidence to the adjacent node. `--max-lines` and `--max-items` bound the result. Use `--format json` only when needed.

Do not retrieve evidence for every node in the neighborhood. Start with the primary population writer and the most important consumers; expand only when a material question remains unresolved.

### Verify source locations

- Prefer the exact repository `file:line` locations reported by `describe` or `evidence`.
- Read the specific ranges using `read_files` when the excerpt is missing, truncated, or insufficient to establish a transformation or runtime behavior.
- Do not guess generated paths such as `docs/lineage/defs/<object>.sql`. A `live_def` path may be generated or absent in the working tree; use it only when the report supplies that exact path and it exists. Prefer `defined_in` repository source locations for verification.
- If a reported path does not exist, do not invent nearby paths. Check the other exact definition locations in the report. Use `search_codebase` only to resolve that specific missing source location or a clearly identified coverage gap.
- Do not mechanically reopen every file already represented by sufficient evidence.

## 3. Recovery when output is incomplete

### No graph

If the CLI reports `No graph at ...`:
- Tell the user to run `C:\\users\\rickb\\vsprojects\\pifitness\\.venv\\Scripts\\python.exe tools\\data_dictionary\\lineage.py build`.
- Do not run `build` yourself; it may require database credentials.
- Do not read `.env` files or connect to the database.

### Node not found or ambiguous

Only when `describe` cannot resolve the requested node:

1. Run `search` with a distinctive partial name and the likely type:

   ```powershell
   C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools\data_dictionary\lineage.py search activity_details_distance --type table
   ```

2. Retry `describe` with the exact node ID if there is one unambiguous match.
3. If multiple candidates remain plausible, ask the user which object they mean.

Never invent a node ID. An unresolved graph node is not permission to launch a repository-wide search.

### Expected writer or consumer is missing

If the relevant section is empty or suspiciously incomplete:

1. Inspect `coverage` and warnings first: dynamic SQL, unresolved relations, ORM inference, missing database catalog, skipped TypeScript analysis, parser errors, excluded tests, stale graph, or traversal truncation.
2. Retry only the relevant section/direction once with `--min-conf low`. Treat relationships appearing only at LOW confidence as unverified.
3. Identify the specific unsupported mechanism or missing surface. Search only for that gap; do not repeat broad searches for the table name.

### Report is too large or the tool display truncates it

- Do **not** repeat the same command with the same parameters.
- Do **not** guess what unseen sections contain or invent section names.
- Split the report into `--section population,upstream,coverage` and `--section consumers,coverage`, or use `--direction up` and `--direction down`.
- Reduce `--depth`, `--max-nodes`, or `--max-items` before considering an increase.
- Retrieve source for one specific node/relationship with `evidence`, using a smaller `--max-lines` or `--max-items`.
- Do not use shell pipelines or output redirection as the first recovery strategy. Inspect the actual CLI error and simplify the command. Do not speculate about PowerShell behavior without evidence.

## 4. Interpret relationships correctly

### Population versus modification

Distinguish the operation classifications reported by the CLI:

- `INSERT`, `COPY_FROM`, `MERGE`, `PANDAS_TO_SQL`, and explicit `ORM_INSERT`: population operations.
- `ORM_ADD (insert/update depends on ORM state)` and `ORM_MERGE`: ambiguous persistence; do not claim they necessarily insert.
- `UPDATE`: modifies existing rows; it is not by itself proof of initial population.
- `DELETE`, `TRUNCATE`, `COPY_TO`, `REFRESH`, `ORM_UPDATE`, and `ORM_DELETE`: mutation/removal operations.
- Writer edges without operation evidence: unclassified, not proof of population.

Identify primary population separately from secondary mutations. Do not describe every `written_by` relationship as a population operation.

### Invocation versus data dependency

A caller explains how a procedure/function is initiated. A `reads` edge explains which data it consumes. These are different relationships. Explain the invocation chain separately from the source-data chain; do not describe an orchestrator as a consumer merely because it calls a writer.

### Consumption versus presentation

Distinguish direct readers, derived database views, backend processing, API exposure, frontend requests/components/hooks, and pages. A frontend request to an endpoint is not proof that the frontend reads the target table directly. Preserve the intervening layers shown by the graph.

### Confidence

- `HIGH`: exact catalog or source-level relationship.
- `MEDIUM`: resolved by name/path heuristics or conservative static analysis.
- `LOW`: inferred or guessed; explicitly label it unverified.

Never present LOW-confidence relationships as established facts.

## 5. Coverage and freshness

Pay attention to:
- Dynamic SQL and unresolved relation names.
- ORM patterns not recognized by the conservative static analyzer.
- Missing database catalog information.
- Excluded test files. Tests are excluded by default; the report discloses this. A human can rebuild with `--include-tests` when test surfaces matter.
- Failed Python/TypeScript parsing or skipped frontend analysis.
- You can always assume the graph is a current representation of the repo, ignoring head and timestamp values.

The ORM analyzer recognizes explicit `__tablename__` mappings and selected common SQLAlchemy/SQLModel calls. Runtime-generated models, implicit table naming, complex query composition, and arbitrary object-state inference may be missed.

Never read `docs/lineage/graph.json` directly. Never run `build` or inspect environment files during an agent lineage investigation.

## 6. Answer requirements

For population questions:
- Explain source data -> loader/writer -> transformations -> target.
- Distinguish primary population writers, secondary mutations, and ambiguous/unclassified writers.
- Explain the invocation chain separately from data dependencies.
- Describe important transformations only when supported by source evidence.
- Include verified `file:line` references.

For consumption or feature-impact questions:
- Identify database views and derived relations.
- Identify backend readers and processing functions.
- Follow API routes and query functions.
- Identify frontend clients, components, hooks, and pages.
- Group consumers by functional purpose where useful.

For broad requests, provide the complete **available** upstream and downstream picture, with a readable data-flow diagram when useful. Always disclose graph freshness, traversal truncation, and material coverage limitations. Do not invent missing consumers, writers, or report sections.

The objective is an actionable understanding of the affected feature surface before code changes begin—not a repository-wide search transcript.
