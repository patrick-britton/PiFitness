import pytest

PURPOSE_MARKERS = ("unit", "contract", "regression", "integration", "smoke")


def pytest_addoption(parser):
    parser.addoption("--run-quarantine", action="store_true",
                     help="also run quarantined tests")


def pytest_collection_modifyitems(config, items):
    run_quarantine = config.getoption("--run-quarantine")
    problems = []
    for item in items:
        purposes = [m for m in PURPOSE_MARKERS if item.get_closest_marker(m)]
        if len(purposes) != 1:
            problems.append(f"{item.nodeid}: needs exactly one purpose marker, "
                            f"has {purposes or 'none'}")
        slow = item.get_closest_marker("slow")
        if slow and not slow.kwargs.get("reason"):
            problems.append(f"{item.nodeid}: slow needs reason=...")
        quarantine = item.get_closest_marker("quarantine")
        if quarantine and not run_quarantine:
            item.add_marker(pytest.mark.skip(
                reason="quarantined: " + quarantine.kwargs.get("reason", "")))
    if problems:
        raise pytest.UsageError("test classification problems:\n  " + "\n  ".join(problems))

# --- INT-19b: test DB isolation guard ---
import pytest as _pytest

@_pytest.fixture(scope="session", autouse=True)
def _guard_test_db():
    import os
    pg_db = os.environ.get("PG_DB", "").strip()
    if not pg_db:
        return
    allowed = ("dev", "test", "local", "localhost")
    if not any(k in pg_db.lower() for k in allowed):
        raise RuntimeError(
            f"Tests refuse to run: PG_DB={pg_db!r} is not a dev/test database. "
            f"Allowed substrings: {allowed}. Set PG_DB=pifitness_dev or unset."
        )
