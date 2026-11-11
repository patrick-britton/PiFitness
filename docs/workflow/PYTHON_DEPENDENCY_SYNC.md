# Bootstrap Python dependency synchronization

## Why this change is needed

The prior TEST bootstrap synchronized the persistent virtualenv from
`backend/uv.lock`, then separately ran `uv pip install` against
`tools/db/requirements-schema-compare.txt`. On the next bootstrap, `uv sync`
removed the packages that were not represented in the lockfile, after which
the separate installer added them again.

## New source of truth

`backend/pyproject.toml` declares `migradiff[pg]==1.7.2` in the `deployment`
dependency group. The bootstrap installs that group as part of the same
`uv sync --locked` operation used for the application. MigraDiff is therefore
managed by `backend/uv.lock`; it is not installed afterward by `uv pip`.

## Required local lockfile update

After applying this update, regenerate the committed lockfile from the backend
directory:

```powershell
Set-Location backend
uv lock
uv lock --check
Set-Location ..
```

Review `backend/uv.lock` in source control and commit it together with
`backend/pyproject.toml` and `deploy/scripts/bootstrap.sh`. Do not run
`uv sync --no-dev` against a development virtualenv merely to regenerate the
lockfile; `uv lock` updates the lock without reconciling that environment.

The bootstrap command is now:

```bash
uv sync --locked --no-dev --group deployment --no-editable --python /usr/bin/python3
```

There should be no following `uv pip install` command. A normal subsequent
bootstrap may replace the local `pifitness` installation because the new
staging worktree has a different path. It should not remove and reinstall the
MigraDiff dependency set.

## Validation

Run the project's normal test command, the focused bootstrap contract tests,
and `bash -n deploy/scripts/bootstrap.sh`. After commit/push, the Pi run should
show `migradiff==1.7.2` present as part of the one synchronized environment,
with no standalone six-package reinstall step.
