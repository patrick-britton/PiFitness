"""Public service functions for the admin domain.

Extracted from router modules so that tests and other callers import from a
service layer rather than reaching into router private names (PY-005).
"""


def failed_run_message(failed) -> str:
""""""Aggregate per-task reasons into the async-run error_message (004-005

Keeps the count prefix the UI already parses while appending the
underlying `<task_name>: <error>` detail so a failed run surfaces its
real reason instead of a bare count""" """
    details = "; ".join(
        f"{r.get('task_name', r.get('task_id', '?'))}: {r.get('error', 'failed')}"
        for r in failed
    )
    return f"Task execution completed with {len(failed)} failure(s): {details}"