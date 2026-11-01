"""create_segment sequencing and error mapping with every database interaction mocked.
"""
import pytest
import unittest.mock as mock

import pifitness.activities.queries as aq


def _run_create(qec_calls, new_id=96):
    with mock.patch(
        "pifitness.activities.queries.sql_to_dict",
        return_value=[{"activity_id": 24652871872, "distance_m": 7500.0}],
    ), mock.patch(
        "pifitness.activities.queries.one_sql_result",
        return_value=new_id,
    ), mock.patch(
        "pifitness.activities.queries.qec",
        side_effect=qec_calls,
    ) as qec_mock:
        result = aq.create_segment(
            segment_name="Probe Course",
            activity_id=24652871872,
            start_m=0,
            end_m=7000,
            is_course=True,
        )
    return result, qec_mock


@pytest.mark.regression
@pytest.mark.quarantine(
    reason="asserts the SQL text of three qec calls by index",
    exit="rewrite_against_db",
)
def test_create_segment_writes_details_row():
    """create_segment creates the segment, finalizes the match, then writes details
    scoped to the reference activity only.
    """
    result, qec_mock = _run_create([None, None, None])
    assert result == 96
    assert qec_mock.call_count == 3
    calls = [c[0] for c in qec_mock.call_args_list]
    assert "segment_matching_segment_creation" in calls[0][0]
    assert "segment_matching_finalize_match(TRUE" in calls[1][0]
    assert calls[1][1] == [24652871872, 96, 0, 7000]
    # Scoped per-activity details write — never the unscoped NULL variant.
    assert "staging.update_segment_details(%s)" in calls[2][0]
    assert calls[2][1] == [24652871872]


@pytest.mark.regression
def test_create_segment_details_failure_maps_to_value_error():
    """create_segment raises ValueError naming the details write when the per-activity
    details write fails.
    """
    try:
        _run_create([None, None, "boom"])
    except ValueError as e:
        assert "update_segment_details failed" in str(e)
    else:
        raise AssertionError("expected ValueError from details-write failure")


@pytest.mark.regression
def test_create_segment_finalize_failure_skips_details_write():
    """create_segment raises ValueError and performs no details write when finalizing
    the match fails.
    """
    try:
        _run_create([None, "boom"])
    except ValueError as e:
        assert "finalize match failed" in str(e)
    else:
        raise AssertionError("expected ValueError from finalize failure")
