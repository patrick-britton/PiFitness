# Test Strategy — PiFitness

## Tiers

| Tier | Tag | Runtime target | Run when |
|---|---|---|---|
| 0 — Smoke | @pytest.mark.smoke | <1s/file | Every commit |
| 1 — Unit | (untagged) | <30s total | Default dev loop; every block |
| 2 — Integration | @pytest.mark.integration | <60s | Merge-to-main; blocks touching DB/network paths |
| 3 — Live | @pytest.mark.live | <5min | Nightly; manual |

Cross-cutting: @pytest.mark.slow — anything >1s for reasons not yet fixed (debt tracker, INT-T4 removes).

## Commands

    pytest -m "not slow and not integration and not live"   # fast lane — default dev loop
    pytest -m "not live"                                    # everything but network
    pytest -m "slow or integration or live"                 # slow lane
    pytest tests/test_foo.py::test_bar                      # single test

## Block verification policy

Run full pytest ONLY if the block adds/removes module-level imports in
widely-imported files, moves code between modules, adds/deletes test files,
or modifies pytest.ini / conftest.py / shared fixtures.
Otherwise: fast lane + smoke import of every module the block touched.

## Introduction standard (enforced by INT-T2)

Every test function has a docstring whose first line is a contract statement —
a complete sentence ending in a period. Every test carries a tier marker.

## Retirement policy (enforced by INT-T2)

When a block deletes or renames a source module, the same block deletes or
migrates every test whose sole purpose was that module. No orphaned tests.

## Metrics (as of INT-T1)

- Total tests: 228 (fast lane 199, slow/integration/live 29)
- Docstring coverage: 59% (135/228) — INT-T2 target: 100%
- Zero-test files: 4 (test_admin_failure_message, test_backup, test_playlist_details, test_shuffle) — audit in INT-T2