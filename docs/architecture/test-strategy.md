# Test Strategy (`test-strategy.md`)

This document defines the test tiers, execution commands, verification policies, and authoring standards for the PiFitness test suite.

---

## 1. Test Tiers & Execution Targets

| Tier | Marker | Runtime Target | Run When |
|---|---|---|---|
| **Tier 0 — Smoke** | `@pytest.mark.smoke` | <1s per file | Every commit / pre-push |
| **Tier 1 — Unit** | (untagged) | <30s total | Default dev loop; task completion |
| **Tier 2 — Integration** | `@pytest.mark.integration` | <60s | Merge to main; tasks touching database or network |
| **Tier 3 — Live** | `@pytest.mark.live` | <5min | Nightly runs; manual verification |

*Cross-cutting marker:* `@pytest.mark.slow` flags any test taking >1s, keeping the default fast loop responsive.

---

## 2. Standard Test Commands

```bash
# Fast dev loop (smoke + unit, excludes slow, integration, and live)
pytest -m "not slow and not integration and not live" -q

# Offline suite (everything except live external network calls)
pytest -m "not live"

# Single test target
pytest tests/test_foo.py::test_bar
```

---

## 3. Verification & Retirement Policies

### Block Verification Policy
* Run the fast-lane test suite (`not slow and not integration and not live`) plus smoke imports of modified modules for standard task verification.
* Run the full pytest suite only when altering module-level imports in widely-shared modules (`core/`), adding/deleting test fixtures, or modifying `conftest.py`.

### Authoring Standards
* Every test function must have a docstring whose first line is a clear contract statement (a complete sentence ending with a period).
* Every test that interacts with real database instances or network endpoints must carry its appropriate tier marker (`@pytest.mark.integration` or `@pytest.mark.live`).

### Retirement Policy
* When a feature refactors, renames, or deletes a source module, the modifying task is responsible for migrating or removing associated tests. Orphaned or skipped tests must not linger in the test directory.
