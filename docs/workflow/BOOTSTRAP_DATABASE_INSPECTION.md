# Bootstrap TEST/PRD inspection: IPv6 endpoint and diagnostic improvement

## Root cause of the Pi failure

PostgreSQL returned `inet_server_addr()` as `::1/128`. That is IPv6 loopback in
PostgreSQL's `inet` representation. The previous safety check passed the full
string to Python's `ipaddress.ip_address()`, which accepts bare addresses but
not CIDR/prefix notation. It therefore rejected a legitimate local endpoint.

## Fix

`tools/db/migrate.py` now provides `_normalize_server_address()`. It accepts
both bare IP addresses and PostgreSQL `inet` values with a prefix, using
`ip_address()` first and `ip_interface().ip` as the prefix-aware fallback.
The resulting canonical address is then checked for loopback.

The inspector and MigraDiff adapter use the same normalization and compare
canonical addresses, so `::1`, `::1/128`, and equivalent representations are
not mistaken for different servers.

## Clearer inspection failures

The inspector's JSON report now includes a `failure` object when inspection
fails:

- `code`: stable machine-readable failure category;
- `stage`: the inspection stage where it stopped;
- `summary`: concise problem statement;
- `detail`: the original redacted diagnostic;
- `next_action`: actionable operator guidance.

The inspector always writes machine-readable JSON to stdout. On failure it
also writes a concise human-readable explanation to stderr. Bootstrap stores
the JSON report and stderr separately under the private evidence directory.
On failure it displays the human-readable explanation and the report path; it
does not dump raw JSON into the terminal. On success it prints a short
summary of PRD identity, TEST identity/marker, schema result, and policy action.

## Safety

This is a read-only inspection fix. It does not change database selection
policy, perform database DDL, clone/restore/drop/rename a database, apply
migrations, or start/stop any service. The failing inspection left the current
TEST database and checkout unchanged, and this fix preserves that property.

## Files in this update

- `tools/db/migrate.py`
- `tools/db/compare_schema.py`
- `tools/db/inspect_test_db.py`
- `deploy/scripts/bootstrap.sh`
- focused tests for the migration runner, comparator, inspector, and bootstrap
  integration contract.

## Local verification performed in the isolated build workspace

- Python compilation passed for the changed Python modules and tests.
- `bash -n deploy/scripts/bootstrap.sh` passed.
- 121 focused tests passed.

The isolated build workspace did not have `psycopg2` installed, so a minimal
import stub was used while running mocked database tests. This is not a live
PostgreSQL or Pi integration test. The Pi retry remains the next runtime gate.
