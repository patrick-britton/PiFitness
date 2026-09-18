"""ISRC dupe review query helpers (008-005, T03; single-transaction rewrite T08).

Accept writes swaps (arbiter: PK original_isrc, valid_mapping TRUE); reject
writes non-swaps (bare ON CONFLICT DO NOTHING — no unique); both delete the
decided pair bidirectionally; rescan CALLs the finder. Every path runs on one
connection inside one transaction and rolls back + re-raises on failure.
"""

import sys
import os
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend_functions.queries.music_queries import (
    get_isrc_dupe_count,
    process_isrc_dupe_decision,
    run_isrc_duplicate_finder,
)


class FakeCursor:
    """Records statements; optionally raises on a chosen call type."""

    def __init__(self, fail_on=None):
        self.executed = []
        self.executemany_calls = []
        self.closed = False
        self._fail_on = fail_on

    def execute(self, sql, params=None):
        if self._fail_on == 'execute':
            raise RuntimeError('execute failed')
        self.executed.append((sql, params))

    def executemany(self, sql, seq_of_params):
        if self._fail_on == 'executemany':
            raise RuntimeError('executemany failed')
        self.executemany_calls.append((sql, list(seq_of_params)))

    def close(self):
        self.closed = True


class FakeConn:
    """Minimal psycopg2 connection stand-in recording transaction lifecycle."""

    def __init__(self, fail_on=None):
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.cursor_obj = FakeCursor(fail_on=fail_on)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_dupe_count_halves_rows_to_pairs():
    with patch(
        'backend_functions.queries.music_queries.one_sql_result',
        return_value=7,
    ):
        assert get_isrc_dupe_count() == 3


def test_dupe_count_empty_is_zero():
    with patch(
        'backend_functions.queries.music_queries.one_sql_result',
        return_value=None,
    ):
        assert get_isrc_dupe_count() == 0


def test_accept_writes_swaps_and_deletes_pair_in_one_transaction():
    fake = FakeConn()
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ) as mock_get_conn:
        process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', True)

    mock_get_conn.assert_called_once_with()          # one connection, not three
    assert fake.commits == 1                         # one commit, not three
    assert fake.rollbacks == 0
    assert fake.closed is True and fake.cursor_obj.closed is True

    # Both ISRCs are written by a single statement.
    assert len(fake.cursor_obj.executemany_calls) == 1
    insert_sql, insert_rows = fake.cursor_obj.executemany_calls[0]
    assert 'music.isrc_swaps' in insert_sql
    assert 'valid_mapping' in insert_sql
    assert 'TRUE' in insert_sql
    assert '(original_isrc)' in insert_sql
    # Non-swaps table must never appear on the accept path
    assert 'isrc_non_swaps' not in insert_sql
    assert insert_rows == [('ISRC-A', 'ISRC-A'), ('ISRC-B', 'ISRC-A')]

    del_sql, del_params = fake.cursor_obj.executed[0]
    assert 'music.isrc_possible_dupes' in del_sql
    assert del_params == ('ISRC-A', 'ISRC-B', 'ISRC-B', 'ISRC-A')


def test_reject_writes_non_swaps_and_deletes_pair_in_one_transaction():
    fake = FakeConn()
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ):
        process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', False)

    assert fake.commits == 1
    assert fake.rollbacks == 0
    assert len(fake.cursor_obj.executemany_calls) == 1
    insert_sql, insert_rows = fake.cursor_obj.executemany_calls[0]
    assert 'music.isrc_non_swaps' in insert_sql
    assert 'ON CONFLICT' in insert_sql
    assert 'DO NOTHING' in insert_sql
    # No arbiter may be named: the table has no unique constraint
    assert '(original_isrc' not in insert_sql
    assert 'DO UPDATE' not in insert_sql
    assert insert_rows == [('ISRC-A', 'ISRC-A'), ('ISRC-B', 'ISRC-A')]
    del_sql, del_params = fake.cursor_obj.executed[0]
    assert 'music.isrc_possible_dupes' in del_sql
    assert del_params == ('ISRC-A', 'ISRC-B', 'ISRC-B', 'ISRC-A')


def test_decision_insert_failure_rolls_back_and_raises():
    fake = FakeConn(fail_on='executemany')
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', True)

    assert fake.commits == 0            # nothing committed
    assert fake.rollbacks == 1
    assert fake.closed is True


def test_decision_delete_failure_rolls_back_and_raises():
    # Inserts succeed, the pool delete fails -> the whole decision is rolled back.
    fake = FakeConn(fail_on='execute')
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', False)

    assert len(fake.cursor_obj.executemany_calls) == 1
    assert fake.commits == 0
    assert fake.rollbacks == 1
    assert fake.closed is True


def test_rescan_issues_call_in_one_transaction():
    fake = FakeConn()
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ) as mock_get_conn:
        run_isrc_duplicate_finder()

    mock_get_conn.assert_called_once_with()
    assert fake.cursor_obj.executed == [
        ('CALL music.isrc_duplicate_finder();', None)
    ]
    assert fake.commits == 1
    assert fake.rollbacks == 0
    assert fake.closed is True


def test_rescan_failure_rolls_back_and_raises():
    # T08: a failed CALL must not look like a successful re-scan.
    fake = FakeConn(fail_on='execute')
    with patch(
        'backend_functions.queries.music_queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            run_isrc_duplicate_finder()

    assert fake.commits == 0
    assert fake.rollbacks == 1
    assert fake.closed is True
