"""An empty work queue is a successful no-op, while a non-empty queue whose API calls
all fail still fails.
"""
import pytest
import backend_functions.json_extractors as je
import backend_functions.ultimate_task_executioner_v2 as ute


def _noop(*args, **kwargs):
    return None


def _task(**over):
    t = {'task_id': 19, 'task_name': 'Activity Details',
         'api_function_name': 'get_activity_details',
         'python_extraction_function': 'extract_garmin_universal',
         'loop_strategy': 'activity',
         'api_service_name': 'Garmin'}
    t.update(over)
    return t


@pytest.mark.regression
def test_empty_iterator_raises_no_work(monkeypatch):
    """extract_garmin_universal raises NoWorkToSync, with a message distinct from
    API-failure text, when its work queue is empty.
    """
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [])
    td = _task()
    try:
        je.extract_garmin_universal(client=object(), td=td)
    except je.NoWorkToSync as e:
        msg = str(e)
    else:
        raise AssertionError('expected NoWorkToSync')
    assert 'No work to sync' in msg
    assert 'No API response' not in msg


@pytest.mark.regression
def test_executioner_reconciles_no_work_as_success(monkeypatch):
    """ultimate_task_executioner reconciles an empty work queue as one successful run
    and reports the task as successful.
    """
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [])
    monkeypatch.setattr('backend_functions.json_extractors.log_app_event', _noop)
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e)))
    task = _task(run_extract=1, run_python=None,
                 python_login_function='pifitness.integrations.garmin.provider.garmin_login')
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    import pifitness.integrations.garmin.provider as gp
    monkeypatch.setattr(gp, 'garmin_login',
                        lambda cd=None: {'client': object()})

    summary = ute.ultimate_task_executioner(force_task_id=19)

    assert len(calls) == 1
    assert calls[0][0] is False  # success reconcile — lockout recovers
    entry = summary['results'][0]
    assert entry['success'] is True
    assert entry['error'] is None


@pytest.mark.regression
def test_nonempty_all_failed_still_fails(monkeypatch):
    """ultimate_task_executioner reconciles a non-empty queue whose API calls all fail
    as one failure carrying the API error and reports the task as failed.
    """
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [111, 222])
    monkeypatch.setattr(je, 'get_garmin_data',
                        lambda **kw: (None, '401 unauthorized'))
    monkeypatch.setattr('backend_functions.json_extractors.log_app_event', _noop)
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e)))
    task = _task(run_extract=1, run_python=None,
                 python_login_function='pifitness.integrations.garmin.provider.garmin_login')
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    import pifitness.integrations.garmin.provider as gp
    monkeypatch.setattr(gp, 'garmin_login',
                        lambda cd=None: {'client': object()})

    summary = ute.ultimate_task_executioner(force_task_id=19)

    assert len(calls) == 1
    assert calls[0][0] is True
    assert calls[0][1] == 'No API response to load'
    entry = summary['results'][0]
    assert entry['success'] is False
