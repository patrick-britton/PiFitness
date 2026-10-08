"""T13 regression — 004-005 OQ-4a / Bug 004-005-6.

An empty work queue (e.g. vw_activity_ids_to_sync has no eligible rows) is a
successful no-op, NOT 'No API response to load':

  1. extract_pirate_universal raises NoWorkToSync when its iterator is empty
     (no API calls attempted), with a canonical message distinct from any
     API-failure string.
  2. extract_load_flatten catches it → informational 'No work to sync' event,
     returns success (no failure reconcile inside the helper).
  3. The executioner reconciles success (no consecutive_failures increment)
     and reports results[] success=True.
  4. A non-empty iterator whose API calls all fail still fails with the
     underlying message ('No API response to load' path unchanged).

Pure unit tests — no DB, no network, no pandas.
"""
import backend_functions.json_extractors as je
import backend_functions.ultimate_task_executioner_v2 as ute


def _noop(*args, **kwargs):
    return None


def _task(**over):
    t = {'task_id': 19, 'task_name': 'Activity Details',
         'api_function_name': 'get_activity_details',
         'python_extraction_function': 'extract_pirate_universal',
         'loop_strategy': 'activity',
         'api_service_name': 'Garmin'}
    t.update(over)
    return t


def test_empty_iterator_raises_no_work(monkeypatch):
    """Empty queue → NoWorkToSync with the canonical informational message."""
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [])
    td = _task()
    try:
        je.extract_pirate_universal(client=object(), td=td)
    except je.NoWorkToSync as e:
        msg = str(e)
    else:
        raise AssertionError('expected NoWorkToSync')
    assert 'No work to sync' in msg
    assert 'No API response' not in msg


def test_extract_load_flatten_treats_no_work_as_success(monkeypatch):
    """Helper logs an informational event and returns success — no reconcile."""
    import backend_functions.ultimate_task_executioner_v2 as ute_mod

    def boom(*args, **kwargs):
        raise je.NoWorkToSync(
            'No work to sync for Task #19: Activity Details '
            '(loop_strategy=activity; work queue is empty)')

    monkeypatch.setattr(ute_mod, 'log_app_event', _noop)
    monkeypatch.setattr('backend_functions.json_extractors.log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute_mod, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e)))
    monkeypatch.setattr('backend_functions.json_extractors.sql_to_list', lambda **kw: [])

    import importlib
    import backend_functions.json_extractors as je_mod
    monkeypatch.setattr(je_mod, 'extract_pirate_universal', boom)

    td = _task()
    fail_msg, _cd = ute_mod.extract_load_flatten({'client': object()}, td)
    assert fail_msg is None
    assert calls == []


def test_executioner_reconciles_no_work_as_success(monkeypatch):
    """Full path: empty queue → single success reconcile, results green."""
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [])
    monkeypatch.setattr('backend_functions.json_extractors.log_app_event', _noop)
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e)))
    task = _task(run_extract=1, run_python=None,
                 python_login_function='backend_functions.service_logins.pirate_garmin_login')
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    import backend_functions.service_logins as sl
    monkeypatch.setattr(sl, 'pirate_garmin_login',
                        lambda cd=None: {'client': object()})

    summary = ute.ultimate_task_executioner(force_task_id=19)

    assert len(calls) == 1
    assert calls[0][0] is False  # success reconcile — lockout recovers
    entry = summary['results'][0]
    assert entry['success'] is True
    assert entry['error'] is None


def test_nonempty_all_failed_still_fails(monkeypatch):
    """Non-empty queue with every API call failing still fails loudly."""
    monkeypatch.setattr(je, 'gen_activity_list', lambda aid=None: [111, 222])
    monkeypatch.setattr(je, 'get_pirate_data',
                        lambda **kw: (None, '401 unauthorized'))
    monkeypatch.setattr('backend_functions.json_extractors.log_app_event', _noop)
    monkeypatch.setattr(ute, 'log_app_event', _noop)
    calls = []
    monkeypatch.setattr(
        ute, 'reconcile_task_dates',
        lambda td, task_fail=False, e=None: calls.append((task_fail, e)))
    task = _task(run_extract=1, run_python=None,
                 python_login_function='backend_functions.service_logins.pirate_garmin_login')
    monkeypatch.setattr(ute, 'sql_to_dict', lambda sql, params=None: [task])
    import backend_functions.service_logins as sl
    monkeypatch.setattr(sl, 'pirate_garmin_login',
                        lambda cd=None: {'client': object()})

    summary = ute.ultimate_task_executioner(force_task_id=19)

    assert len(calls) == 1
    assert calls[0][0] is True
    assert calls[0][1] == 'No API response to load'
    entry = summary['results'][0]
    assert entry['success'] is False
