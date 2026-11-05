# Post-install fixes (database identity + unittest discovery)

This update fixes two defects found during the first real Windows install.

## 1. PostgreSQL/Docker port check

The configured connection endpoint is the **Windows host endpoint** `localhost:15432`.
With a normal Docker publish rule `15432:5432`, PostgreSQL itself reports
`inet_server_port() = 5432` from inside the container. The original scripts
incorrectly compared that server-side listener port with the host-side published
port `15432`, so they rejected a valid connection.

The corrected scripts:

- Keep the strict pre-connect allowlist: loopback host, port `15432`, database
  `pifitness_dev`, user `dev`.
- After connecting, independently verify `current_database()`, `current_user`,
  and `transaction_read_only = on`.
- Record `inet_server_addr()` and `inet_server_port()` as diagnostic metadata,
  without confusing the internal listener port with the client endpoint.
- Include the actual observed identity in a failure message, without exposing
  passwords or other secrets.

Do **not** change `backend/.env`, add a port override, or relax the host/database/user
allowlist just to work around this error.

## 2. unittest import path

The documented command uses unittest discovery with `-s tools/data_dictionary/tests`.
That makes the `tests` directory the discovery root; the sibling scripts directory
is not automatically importable as a top-level module. The corrected test modules
add their parent `tools/data_dictionary` directory to `sys.path`, so the existing
command works from the repository root.

## Apply the update

Copy the updated `tools/data_dictionary/` directory from this kit over the installed
`tools/data_dictionary/` directory, allowing the updated files to replace their
counterparts. No application source, database object, environment file, or production
configuration needs to change.

## Rerun from the repository root (PowerShell)

```powershell
& $py -m unittest discover -s tools/data_dictionary/tests -v
& $py tools/data_dictionary/db_catalog.py --env-file backend/.env --out-dir docs/data/_evidence/database
& $py tools/data_dictionary/profile_data.py --env-file backend/.env --out docs/data/_evidence/data_profiles.json
```

Expected first result: nine unit tests pass. The two database commands require the
local DEV database container to be running. The catalog command may warn that
`pg_dump` is unavailable; catalog JSON and view/routine definitions are still useful.

If a database command still refuses the connection, preserve the full diagnostic
message. It reports the configured client endpoint and the observed database, role,
server address, server listener port, and read-only state. Those values are sufficient
to diagnose the mismatch; never paste credentials or the contents of `.env`.
