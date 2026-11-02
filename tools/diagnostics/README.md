# Diagnostics — retired

This directory previously held `NEEDS_ATTENTION.md`, a hand-maintained list of stale imports, missing modules, and other diagnostics.

It was accurate when written but was never refreshed after the reorg fixed the underlying issues, so it became stale debt.

**Detection of stale imports is now the test suite's job** — `pytest` fails on broken imports. `pytest -m contract` fails on API drift.

If a future diagnostic need arises, either add a test or add a script — do not revive the hand-maintained list pattern.