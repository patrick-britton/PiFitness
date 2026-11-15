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



# VERIFIED: Regression — create_segment raises ValueError naming the details write when per-activity details write fails
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


# VERIFIED: Regression — create_segment raises ValueError and skips details write when finalizing match fails
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
