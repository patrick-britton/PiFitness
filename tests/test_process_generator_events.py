"""Post-processing sub-steps announce a running event before they execute, not after
they finish.
"""
import pytest

import backend_functions.activity_smoothing as sm


# ---------------------------------------------------------------------------
# 1. Real generator: start marker yielded BEFORE the sub-step runs
# ---------------------------------------------------------------------------

# VERIFIED: Regression — activity_post_processing_steps yields start marker before executing step function
@pytest.mark.regression
def test_post_processing_steps_yields_marker_before_execution(monkeypatch):
    """activity_post_processing_steps yields each sub-step's start marker before the
    sub-step runs and its terminal result after it.
    """
    executed = []
    monkeypatch.setattr(sm, 'insert_heartrate_for_activity',
                        lambda aid: executed.append(('insert_heartrate', aid)))

    gen = sm.activity_post_processing_steps(123)
    try:
        marker = next(gen)
        # Marker for the FIRST sub-step arrives before any sub-step ran.
        assert marker == ('insert_heartrate', None, None)
        assert executed == [], "start marker must be yielded before the sub-step executes"

        terminal = next(gen)
        step_id, ms, error = terminal
        assert step_id == 'insert_heartrate'
        assert error is None
        assert isinstance(ms, int) and ms >= 0
        # Terminal arrives only AFTER execution.
        assert executed == [('insert_heartrate', 123)]
    finally:
        gen.close()


# ---------------------------------------------------------------------------
# 2. process_generator: running event emitted before the sub-step executes
# ---------------------------------------------------------------------------





