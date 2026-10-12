"""
004-007 T09 / Bug 004-007-7-5 — post-processing sub-steps must stream a
'running' event BEFORE the sub-step executes, not after it completes.

Two verified mechanisms were fixed; each gets a test here:

  1. `activity_post_processing_steps` yields a start marker
     (step_id, None, None) before invoking each sub-step function and the
     terminal (step_id, ms, error) after — marker-before-execution is
     asserted against the REAL generator with a recording sub-step.

  2. `_process_generator` maps each marker to an NDJSON 'running' event and
     emits it while the sub-step has NOT yet run (asserted by interleaving
     the stream pull with a fake generator that executes on resume), then
     emits the terminal event afterwards.

Everything is mocked — no DB writes, no network, no pandas.

Run from the repository root:
    python -m pytest tests/test_process_generator_events.py -v
"""
import json

import backend_functions.activity_smoothing as sm
import backend.api.activities as act
from backend.schemas.activity_schemas import ProcessActivityRequest


def _events(req):
    """Drain _process_generator, returning parsed NDJSON events in order."""
    return [json.loads(line) for line in act._process_generator(req)]


def _ok_executioner(**kwargs):
    """Sync step success shape (no failures for _task_failure_message)."""
    return {
        'tasks_processed': 1,
        'results': [{'task_id': kwargs.get('force_task_id'),
                     'task_name': 'stub', 'success': True}],
        'status': 'success',
    }


# ---------------------------------------------------------------------------
# 1. Real generator: start marker yielded BEFORE the sub-step runs
# ---------------------------------------------------------------------------

def test_post_processing_steps_yields_marker_before_execution(monkeypatch):
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


def test_post_processing_steps_marker_terminal_interleaved(monkeypatch):
    """Marker/terminal pairing stays strictly interleaved: start, end, start, end…"""
    # First sub-step only — close the generator after its terminal so no
    # un-mocked sub-step or DB lookup ever runs.
    monkeypatch.setattr(sm, 'insert_heartrate_for_activity', lambda aid: None)

    gen = sm.activity_post_processing_steps(7)
    try:
        pairs = [next(gen), next(gen)]
    finally:
        gen.close()
    assert pairs[0][0] == 'insert_heartrate' and pairs[0][1] is None and pairs[0][2] is None
    assert pairs[1][0] == 'insert_heartrate' and isinstance(pairs[1][1], int)


# ---------------------------------------------------------------------------
# 2. _process_generator: running event emitted before the sub-step executes
# ---------------------------------------------------------------------------

def test_process_generator_running_event_precedes_substep_execution(monkeypatch):
    executed = []

    def fake_steps(aid):
        # Start marker → the API must emit 'running' now…
        yield ('insert_heartrate', None, None)
        # …the sub-step only executes when the consumer pulls past the marker.
        executed.append('insert_heartrate')
        yield ('insert_heartrate', 5, None)
        yield ('build_activity_path', None, None)
        executed.append('build_activity_path')
        yield ('build_activity_path', 7, None)

    monkeypatch.setattr(sm, 'activity_post_processing_steps', fake_steps)
    monkeypatch.setattr(act, 'ultimate_task_executioner', _ok_executioner)
    monkeypatch.setattr(act, '_resolve_activity_id', lambda kind: 123)
    monkeypatch.setattr(act, 'one_sql_result', lambda *a, **k: 1000.0)
    monkeypatch.setattr(act, '_build_process_summary', lambda *a, **k: None)

    req = ProcessActivityRequest(mode='last_walk')
    seen = []
    for line in act._process_generator(req):
        ev = json.loads(line)
        seen.append(ev)
        # The 'running' event must leave the stream while the sub-step it
        # announces has NOT yet executed — this is the Bug 004-007-7-5 fix.
        if ev.get('step_id') == 'insert_heartrate' and ev.get('status') == 'running':
            assert executed == [], (
                "running event was emitted only after the sub-step executed"
            )
        if ev.get('step_id') == 'build_activity_path' and ev.get('status') == 'running':
            assert executed == ['insert_heartrate']

    # Ordering: for each post-processing step, running strictly precedes terminal.
    for sid, ms in (('insert_heartrate', 5), ('build_activity_path', 7)):
        step_evs = [e for e in seen if e.get('step_id') == sid]
        statuses = [e['status'] for e in step_evs]
        assert statuses == ['running', 'complete'], f"{sid}: {statuses}"
        assert step_evs[1]['elapsed_ms'] == ms

    # Pipeline still completes successfully (terminal event last).
    assert seen[-1].get('complete') is True
    assert seen[-1].get('success') is True


def test_process_generator_error_terminal_still_emits_running_first(monkeypatch):
    """Halt-on-error path: running marker is streamed, then the error terminal."""
    def failing_steps(aid):
        yield ('insert_heartrate', None, None)
        yield ('insert_heartrate', 3, 'boom')

    monkeypatch.setattr(sm, 'activity_post_processing_steps', failing_steps)
    monkeypatch.setattr(act, 'ultimate_task_executioner', _ok_executioner)
    monkeypatch.setattr(act, '_resolve_activity_id', lambda kind: 123)
    monkeypatch.setattr(act, 'one_sql_result', lambda *a, **k: 1000.0)
    monkeypatch.setattr(act, '_build_process_summary', lambda *a, **k: None)

    req = ProcessActivityRequest(mode='last_walk')
    seen = _events(req)

    hr = [e for e in seen if e.get('step_id') == 'insert_heartrate']
    assert [e['status'] for e in hr] == ['running', 'error']
    assert hr[1]['error'] == 'boom'
    # No post-processing sub-step starts after the error…
    assert not any(
        e.get('status') == 'running'
        for e in seen
        if e.get('step_id') not in ('sync_activities', 'sync_details',
                                    'resolve_activity', 'insert_heartrate')
    )
    # …and the terminal reports failure.
    assert seen[-1].get('complete') is True
    assert seen[-1].get('success') is False
