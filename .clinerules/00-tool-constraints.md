# Tool and File-Writing Constraints

These rules cover `run_commands`, `read_files`, `search_codebase`, `write_to_file`, `replace_in_file`, and `skills`. Use only tool names and schemas the runtime exposes. On `NoSuchToolError`, check the available-tool list and pick the real tool.

## 1. Reading and searching

1. Read a known path directly. Use `search_codebase` only for an unknown location or the callers of a specific symbol: one distinctive query, scoped to the smallest directory. Never search for a filename you already have, a conventional config file (`pytest.ini`, `pyproject.toml`, `conftest.py`, `package.json`, `tsconfig.json`), or a relationship lineage already reported.
2. Read at most 200 lines of a file at a time. For a larger file, find the region with `Select-String` on the exact path, then read only that range. Read independent known files in one `read_files` call.
3. Every read answers one named question. Do not re-read a file or range you already have unless the file changed, the read was truncated, or you must check an edit anchor or the changed lines. Never repeat an identical tool call that already succeeded.
4. If `read_files` returns 0-2 lines for a file you expect to be larger, the path or range is wrong. Correct that one path; do not widen the search.
5. Keep output small: use tool filters and `Select-Object -First/-Last`.
6. Stop exploring when you know the current behavior, the edit point, its direct dependencies, and how the change will be verified.

## 2. Running commands

1. One logical operation per `run_commands` call. Independent read-only checks may share one labelled call. Keep file edits, test runs, and database operations in separate calls when the next step depends on the result.
2. Read `$LASTEXITCODE` immediately after each native executable. Do not use `$?`.
3. Each call is a new PowerShell process. Variables, working directory, environment changes, and jobs do not persist.
4. Slow commands (over 30 seconds): follow `.clinerules/01-terminal-use.md`.

## 3. Failure recovery

1. Never retry an identical failed payload. Classify the failure first: wrong tool or schema, malformed payload, wrong path or range, PowerShell parsing, editor SEARCH mismatch, timeout, or real command/test failure. Fix the known cause. If the cause is unclear, inspect the schema, the exact path, or one small source region, once.
2. **Editor SEARCH mismatch:** after the first `text not found` or `multiple occurrences`, read the surrounding source once and build a new unique SEARCH block. If the next replacement on the same objective also fails, stop. Do not switch to shell-generated source; follow the workflow's blocked procedure.
3. **PowerShell parse or payload failure:** fix the dialect from the actual error. No base64, heredocs, here-strings, or multiline `python -c`.
4. If the same failure happens twice on one objective with no new information, change to a materially different safe approach or stop as blocked. A 30,000 ms timeout followed by the launcher, or a `RUNNING` poll, is not a repeated failure.
5. Never weaken code, skip a required check, or edit a test expectation to make a failure disappear.

## 4. Writing files

1. **New file up to about 200 lines:** one `write_to_file` call with the complete file.
2. **New file over about 200 lines:** write a skeleton (imports, constants, signatures, `pass` bodies), then one `replace_in_file` per body in file order. Each SEARCH block holds the signature plus 2-3 surrounding lines.
3. **Existing file:** `replace_in_file` with a unique SEARCH block that includes enough context to identify the location. Never use a whole-file SEARCH when a local block works.
4. **Insertion:** at most one `insert_line` per file per task. Prefer one replacement that carries the whole block.
5. **No shell-generated source.** Do not use `Set-Content`, `Add-Content`, redirection, Python one-liners, or concatenated fragments to create or change repository source. Shell output is for disposable diagnostics only.
6. **Check after the last write:** Python `py_compile` or the relevant test; Bash `bash -n`; TypeScript `npx tsc --noEmit`. The write is not done until the check passes.
7. On the first syntax failure, read the diagnostic and fix that defect only. On a second failure in the same file, stop editing. A newly created file may be restarted once from its skeleton. Never delete or rebuild an existing tracked file; follow the blocked procedure.
8. Confirm the final edit with a targeted read or `git diff`, not a full re-read.

## 5. PowerShell 5.1 syntax

- `run_commands` takes a plain, unquoted command string. Do not wrap it in quotes or pass a JSON array.
- Quote arguments that contain spaces. Call a quoted executable path with `&`.
- Use `Select-Object` instead of `head`/`tail` and `Select-String` instead of `grep`. Chain with `;` and explicit `if` blocks, never `&&` or `||`.
- Do not use `2>&1` in `run_commands`. Trim output in memory with `Select-Object`.
- Run `.ps1` files with PowerShell and `.py` files with PY. Never pass a `.ps1` to Python.

## 6. Specialist skills (load only when the task needs them)

- Task adds or changes a shell script, provisioning, build, or deployment command: activate skill `shell-command-verification`.
- Task adds or changes tests for scripts, subprocess orchestration, migrations, or service startup: activate skill `test-harness-discipline`.
