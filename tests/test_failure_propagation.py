"""A failed sub-step is never reported as success, and each task run is reconciled
exactly once with the detailed message.
"""
import sys
import types

import pytest

import backend_functions.activity_smoothing as sm
import backend_functions.ultimate_task_executioner_v2 as ute
from backend.pifitness.activities.services import run_step, task_failure_message


def _noop(*args, **kwargs):
    return None


# ---------------------------------------------------------------------------
# 1. activity_post_processing: all queued activities processed, aggregate raise
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_post_processing_processes_all_then_raises_aggregate(monkeypatch):
    """activity_post_processing processes every queued activity after a failure, then
    raises one error naming the failed activities, step and reason.
    """
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


@pytest.mark.regression
def test_post_processing_success_returns_true(monkeypatch):
    """activity_post_processing returns True after processing every queued activity when
    no sub-step fails.
    """
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


@pytest.mark.regression
def test_post_processing_empty_queue_returns_true(monkeypatch):
    """activity_post_processing returns True without processing anything when the queue
    is empty.
    """
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


@pytest.mark.regression
def test_execute_python_returns_failure_message_without_reconciling(monkeypatch):
    """execute_python returns a failing function's error text and does not reconcile
    task dates itself.
    """
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


@pytest.mark.regression
def test_executioner_reconciles_failed_with_ids_in_message(monkeypatch):
    """ultimate_task_executioner reconciles a failed python task exactly once with the
    failure message and reports success False with that error.
    """
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


@pytest.mark.regression
def test_executioner_reports_success_when_all_pass(monkeypatch):
    """ultimate_task_executioner reconciles a passing python task exactly once as a
    success and reports success True with no error.
    """
    calls, summary = _run_single_task(monkeypatch, lambda: None)

    assert len(calls) == 1
    assert calls[0] == (False, None)  # success reconcile (task_fail=False)
    assert summary['results'][0]['success'] is True
    assert summary['results'][0]['error'] is None


# ---------------------------------------------------------------------------
# 4. run_step: forced-task failure marks the step error (OQ-2a, AC-2)
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_run_step_marks_error_on_failed_results():
    """run_step returns status error naming the task and its error when the runner's
    results include a failed task.
    """
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

    result = run_step('sync_activities', failed_runner, force_task_id=4)

    assert result.status == 'error'
    assert 'Sync Activities' in result.error
    assert 'extraction error: 401 unauthorized' in result.error


@pytest.mark.regression
def test_run_step_complete_on_all_success_results():
    """run_step returns status complete with no error when every runner result
    succeeded.
    """
    def ok_runner(force_task_id=None):
        return {
            'tasks_processed': 1,
            'results': [{'task_id': force_task_id, 'task_name': 'Sync Activities',
                         'success': True, 'error': None}],
            'status': 'complete',
        }

    result = run_step('sync_activities', ok_runner, force_task_id=4)
    assert result.status == 'complete'
    assert result.error is None


@pytest.mark.regression
def test_run_step_complete_on_non_task_return_shapes():
    """run_step treats a None return, a dict without results and results lacking a
    success flag as complete.
    """
    # No return value.
    assert run_step('x', lambda: None).status == 'complete'
    # Unrelated dict without a results list.
    assert run_step('x', lambda: {'status': 'ok'}).status == 'complete'
    # results entries without an explicit success flag are not failures.
    assert run_step('x', lambda: {'results': [{'task_id': 1}]}).status == 'complete'


@pytest.mark.regression
def test_run_step_error_on_exception_unchanged():
    """run_step converts a raised exception into status error carrying the exception
    message.
    """
    def boom():
        raise ValueError('kaput')

    result = run_step('x', boom)
    assert result.status == 'error'
    assert result.error == 'kaput'


@pytest.mark.unit
def test_task_failure_message_is_conservative():
    """task_failure_message returns None unless some result has success False, and then
    returns the task name and error joined by a colon.
    """
    assert task_failure_message(None) is None
    assert task_failure_message([{'results': []}]) is None
    assert task_failure_message({'results': 'nope'}) is None
    assert task_failure_message(
        {'results': [{'success': True}, {'success': None}]}
    ) is None
    assert task_failure_message(
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


@pytest.mark.regression
def test_executioner_single_reconcile_on_extract_failure(monkeypatch):
    """ultimate_task_executioner reconciles an extract-phase failure exactly once with
    the surfaced message and reports success False.
    """
    calls, summary = _run_single_extract_task(
        monkeypatch, {}, 'extraction error: 401 unauthorized')

    assert len(calls) == 1
    task_fail, message = calls[0]
    assert task_fail is True
    assert message == 'extraction error: 401 unauthorized'

    entry = summary['results'][0]
    assert entry['success'] is False
    assert entry['error'] == 'extraction error: 401 unauthorized'


@pytest.mark.regression
def test_extract_load_flatten_does_not_reconcile(monkeypatch):
    """extract_load_flatten returns the login failure message without reconciling task
    dates itself.
    """
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


@pytest.mark.regression
def test_execute_sproc_does_not_reconcile(monkeypatch):
    """execute_sproc returns a failure message for a missing sproc key and None on
    success, without reconciling task dates itself.
    """
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
