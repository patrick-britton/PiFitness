"""
Failure-propagation tests — 004-005 T02 (OQ-1 / OQ-2 option (a)).

Covers Bug 004-005-2 (silent false-success) and Chain A2 (`_run_step`
discarding the executioner's results dict):

  1. `activity_post_processing` processes ALL queued activities, then raises
     an aggregate failure carrying the failed activity ids + step + reason
     (per-activity halt on first sub-step error unchanged).
  2. `execute_python` surfaces that failure message to the caller and no
     longer reconciles internally (single reconcile, no double
     consecutive_failures increment, detailed message preserved).
  3. `ultimate_task_executioner` reconciles the task run as failed exactly
     once with the failed ids in the message (→ last_failure_message) and
     reports the failure in `results[]` (consumed by admin.py / _run_step).
  4. `_run_step` marks the step `error` with the aggregated failure messages
     when a forced task reports failure; unrelated return shapes unaffected.

Everything is mocked — no DB writes, no network, no pandas.

Run from the repository root:
    python -m pytest tests/test_failure_propagation.py -v
"""
import sys
import types

import pytest

import backend_functions.activity_smoothing as sm
import backend_functions.ultimate_task_executioner_v2 as ute
from backend.api.activities import _run_step, _task_failure_message


def _noop(*args, **kwargs):
    return None


# ---------------------------------------------------------------------------
# 1. activity_post_processing: all queued activities processed, aggregate raise
# ---------------------------------------------------------------------------

def test_post_processing_processes_all_then_raises_aggregate(monkeypatch):
    """A failing activity must not stop the queue (OQ-1c); the wrapper must
    raise afterwards with the failed ids, step and reason (Bug 004-005-2)."""
    processed = []

    def fake_steps(activity_id):
        processed.append(activity_id)
        yield ('insert_heartrate', 1, None)
        if activity_id == 2:
            yield ('resample_activity_to_distance', 2,
                   'cannot affect row a second time')
            return
        yield ('build_activity_path', 1, None)

    monkeypatch.setattr(sm, 'sql_to_list', lambda sql: [1, 2, 3])
    monkeypatch.setattr(sm, 'activity_post_processing_steps', fake_steps)
    monkeypatch.setattr(sm, 'log_app_event', _noop)

    with pytest.raises(RuntimeError) as excinfo:
        sm.activity_post_processing()

    # Every queued activity ran, including the ones after the failure.
    assert processed == [1, 2, 3]

    msg = str(excinfo.value)
    assert 'failed for 1/3 activities' in msg
    assert 'activity 2' in msg
    assert 'resample_activity_to_distance' in msg
    assert 'cannot affect row a second time' in msg


def test_post_processing_success_returns_true(monkeypatch):
    """All-green queue still returns True (no spurious task failure)."""
    processed = []

    def fake_steps(activity_id):
        processed.append(activity_id)
        yield ('insert_heartrate', 1, None)
        yield ('build_activity_path', 1, None)

    monkeypatch.setattr(sm, 'sql_to_list', lambda sql: [7, 8])
    monkeypatch.setattr(sm, 'activity_post_processing_steps', fake_steps)
    monkeypatch.setattr(sm, 'log_app_event', _noop)

    assert sm.activity_post_processing() is True
    assert processed == [7, 8]


def test_post_processing_empty_queue_returns_true(monkeypatch):
    monkeypatch.setattr(sm, 'sql_to_list', lambda sql: [])
    monkeypatch.setattr(sm, 'log_app_event', _noop)
    assert sm.activity_post_processing() is True


# ---------------------------------------------------------------------------
# 2. execute_python: failure message returned, no internal reconcile
# ---------------------------------------------------------------------------

AGGREGATE = (
    'Activity Post Processing failed for 1/3 activities -> '
    'activity 2 [resample_activity_to_distance]: cannot affect row a second time'
)


def _install_fake_python_function(monkeypatch, fn):
    """Expose `fn` as python_execution_function 'fake_pp.run'."""
    mod = types.ModuleType('fake_pp')
    mod.run = fn
    monkeypatch.setitem(sys.modules, 'fake_pp', mod)


def test_execute_python_returns_failure_message_without_reconciling(monkeypatch):
    calls = []

    def boom():
        raise RuntimeError(AGGREGATE)

    _install_fake_python_function(monkeypatch, boom)
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e))
    )

    task = {'task_id': 99, 'task_name': 'Activity Detail Post Processing',
            'python_execution_function': 'fake_pp.run'}

    result = ute.execute_python(d=task)

    assert isinstance(result, str)
    assert 'activity 2' in result
    assert 'cannot affect row a second time' in result
    # The caller reconciles — execute_python must not (single increment,
    # detailed message preserved as the final last_failure_message).
    assert calls == []


def test_execute_python_success_returns_none(monkeypatch):
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    _install_fake_python_function(monkeypatch, lambda: None)

    task = {'task_id': 99, 'task_name': 'Activity Detail Post Processing',
            'python_execution_function': 'fake_pp.run'}
    assert ute.execute_python(d=task) is None



# ---------------------------------------------------------------------------
# 3. ultimate_task_executioner: reconciles failed with ids in the message
# ---------------------------------------------------------------------------

def _run_single_task(monkeypatch, fn):
    """Run the executioner against one mocked python task; return
    (reconcile_calls, returned_summary)."""
    task = {
        'task_id': 99,
        'task_name': 'Activity Detail Post Processing',
        'run_extract': None,
        'run_python': 1,
        'python_execution_function': 'fake_pp.run',
    }
    _install_fake_python_function(monkeypatch, fn)
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e))
    )
    summary = ute.ultimate_task_executioner(force_task_id=99)
    return calls, summary


def test_executioner_reconciles_failed_with_ids_in_message(monkeypatch):
    """Failed sub-step → task reconciled failed exactly once, failed activity
    ids in the message written to last_failure_message (AC-1), and the
    returned results[] carries success=False for admin.py / _run_step."""
    def boom():
        raise RuntimeError(AGGREGATE)

    calls, summary = _run_single_task(monkeypatch, boom)

    # Exactly one reconcile — no double consecutive_failures increment.
    assert len(calls) == 1
    task_fail, message = calls[0]
    assert task_fail is True
    assert 'activity 2' in message
    assert 'cannot affect row a second time' in message

    # results[] marks the task failed (consumed by admin.py:244).
    assert summary['status'] == 'complete'
    entry = summary['results'][0]
    assert entry['success'] is False
    assert 'activity 2' in entry['error']


def test_executioner_reports_success_when_all_pass(monkeypatch):
    calls, summary = _run_single_task(monkeypatch, lambda: None)

    assert len(calls) == 1
    assert calls[0] == (False, None)  # success reconcile (task_fail=False)
    assert summary['results'][0]['success'] is True
    assert summary['results'][0]['error'] is None


# ---------------------------------------------------------------------------
# 4. _run_step: forced-task failure marks the step error (OQ-2a, AC-2)
# ---------------------------------------------------------------------------

def test_run_step_marks_error_on_failed_results():
    def failed_runner(force_task_id=None):
        return {
            'tasks_processed': 1,
            'results': [{
                'task_id': force_task_id,
                'task_name': 'Sync Activities',
                'success': False,
                'error': 'extraction error: 401 unauthorized',
            }],
            'status': 'complete',
        }

    result = _run_step('sync_activities', failed_runner, force_task_id=4)

    assert result.status == 'error'
    assert 'Sync Activities' in result.error
    assert 'extraction error: 401 unauthorized' in result.error


def test_run_step_complete_on_all_success_results():
    def ok_runner(force_task_id=None):
        return {
            'tasks_processed': 1,
            'results': [{'task_id': force_task_id, 'task_name': 'Sync Activities',
                         'success': True, 'error': None}],
            'status': 'complete',
        }

    result = _run_step('sync_activities', ok_runner, force_task_id=4)
    assert result.status == 'complete'
    assert result.error is None


def test_run_step_complete_on_non_task_return_shapes():
    # No return value.
    assert _run_step('x', lambda: None).status == 'complete'
    # Unrelated dict without a results list.
    assert _run_step('x', lambda: {'status': 'ok'}).status == 'complete'
    # results entries without an explicit success flag are not failures.
    assert _run_step('x', lambda: {'results': [{'task_id': 1}]}).status == 'complete'


def test_run_step_error_on_exception_unchanged():
    def boom():
        raise ValueError('kaput')

    result = _run_step('x', boom)
    assert result.status == 'error'
    assert result.error == 'kaput'


def test_task_failure_message_is_conservative():
    assert _task_failure_message(None) is None
    assert _task_failure_message([{'results': []}]) is None
    assert _task_failure_message({'results': 'nope'}) is None
    assert _task_failure_message(
        {'results': [{'success': True}, {'success': None}]}
    ) is None
    assert _task_failure_message(
        {'results': [{'success': False, 'task_name': 'T', 'error': 'e'}]}
    ) == 'T: e'


# ---------------------------------------------------------------------------
# 5. extract/load/SPROC paths: single reconcile with the detailed message
#    (004-005 T09 — Bug 004-005-4: helpers no longer reconcile internally;
#    the executioner's single outer reconcile writes the surfaced message)
# ---------------------------------------------------------------------------

def _run_single_extract_task(monkeypatch, task_overrides, extract_retval):
    """Run the executioner against one mocked extract task; return
    (reconcile_calls, returned_summary)."""
    task = {
        'task_id': 42,
        'task_name': 'Extract Task',
        'run_extract': 1,
        'run_python': None,
        'api_service_name': 'Svc',
        'python_login_function': None,
    }
    task.update(task_overrides)
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e))
    )
    monkeypatch.setattr(
        ute, 'extract_load_flatten', lambda cd, td: (extract_retval, cd))
    summary = ute.ultimate_task_executioner(force_task_id=42)
    return calls, summary


def test_executioner_single_reconcile_on_extract_failure(monkeypatch):
    """Helper-surfaced message → exactly one failure reconcile carrying it,
    results[] success False (Bug 004-005-4)."""
    calls, summary = _run_single_extract_task(
        monkeypatch, {}, 'extraction error: 401 unauthorized')

    assert len(calls) == 1
    task_fail, message = calls[0]
    assert task_fail is True
    assert message == 'extraction error: 401 unauthorized'

    entry = summary['results'][0]
    assert entry['success'] is False
    assert entry['error'] == 'extraction error: 401 unauthorized'


def test_executioner_single_reconcile_on_load_failure(monkeypatch):
    calls, summary = _run_single_extract_task(
        monkeypatch, {}, 'Load Failure: staging blew up')

    assert len(calls) == 1
    assert calls[0] == (True, 'Load Failure: staging blew up')
    assert summary['results'][0]['success'] is False


def test_executioner_single_reconcile_on_sproc_failure(monkeypatch):
    calls, summary = _run_single_extract_task(
        monkeypatch, {}, 'Failed to execute sql: boom')

    assert len(calls) == 1
    assert calls[0] == (True, 'Failed to execute sql: boom')
    assert summary['results'][0]['success'] is False


def test_executioner_single_success_reconcile_on_extract_success(monkeypatch):
    """Success path unchanged: single success reconcile, results[] green."""
    calls, summary = _run_single_extract_task(monkeypatch, {}, None)

    assert len(calls) == 1
    assert calls[0] == (False, None)
    assert summary['results'][0]['success'] is True
    assert summary['results'][0]['error'] is None


def test_extract_load_flatten_does_not_reconcile(monkeypatch):
    """The helper itself never reconciles — login failure surfaces the
    detailed message for the single outer reconcile."""
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e))
    )
    monkeypatch.setattr(ute, 'log_app_event', _noop)

    fail_msg, _cd = ute.extract_load_flatten(
        None, {'task_id': 7, 'task_name': 'T', 'api_service_name': 'Garmin'})

    assert fail_msg == (
        'Garmin login failed — check credentials or pirate-garmin CLI status.')
    assert calls == []


def test_execute_sproc_does_not_reconcile(monkeypatch):
    """SPROC failure surfaces the message; success returns None — no
    reconcile inside the helper in either case."""
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e))
    )
    monkeypatch.setattr(ute, 'log_app_event', _noop)

    msg = ute.execute_sproc({'task_id': 7, 'task_name': 'T'}, 'flatten')
    assert msg == 'Failed to extract key: flatten_sproc'
    assert calls == []

    monkeypatch.setattr(ute, 'qec', lambda sql: None)
    assert ute.execute_sproc(
        {'task_id': 7, 'task_name': 'T',
         'flatten_sproc': 'activities.noop()'}, 'flatten') is None
    assert calls == []
