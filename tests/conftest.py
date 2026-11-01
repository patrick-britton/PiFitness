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
