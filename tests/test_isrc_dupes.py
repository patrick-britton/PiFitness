"""ISRC duplicate-review query helpers run each decision or rescan in a single
transaction that rolls back and re-raises on failure.
"""

import sys
import os
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pifitness.music.queries import (
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


@pytest.mark.unit
def test_dupe_count_halves_rows_to_pairs():
    """get_isrc_dupe_count returns the stored row count divided by two."""
    with patch(
        'pifitness.music.queries.one_sql_result',
        return_value=7,
    ):
        assert get_isrc_dupe_count() == 3


@pytest.mark.unit
def test_dupe_count_empty_is_zero():
    """get_isrc_dupe_count returns 0 when the count query returns nothing."""
    with patch(
        'pifitness.music.queries.one_sql_result',
        return_value=None,
    ):
        assert get_isrc_dupe_count() == 0


@pytest.mark.regression
def test_accept_writes_swaps_and_deletes_pair_in_one_transaction():
    """process_isrc_dupe_decision with accept true records both ISRCs as swaps to the
    preferred ISRC and deletes the pair in one committed transaction.
    """
    fake = FakeConn()
    with patch(
        'pifitness.music.queries.get_conn',
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


@pytest.mark.regression
def test_reject_writes_non_swaps_and_deletes_pair_in_one_transaction():
    """process_isrc_dupe_decision with accept false records both ISRCs as non-swaps and
    deletes the pair in one committed transaction.
    """
    fake = FakeConn()
    with patch(
        'pifitness.music.queries.get_conn',
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


@pytest.mark.regression
def test_decision_insert_failure_rolls_back_and_raises():
    """process_isrc_dupe_decision rolls back, commits nothing and re-raises when the
    insert fails.
    """
    fake = FakeConn(fail_on='executemany')
    with patch(
        'pifitness.music.queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', True)

    assert fake.commits == 0            # nothing committed
    assert fake.rollbacks == 1
    assert fake.closed is True


@pytest.mark.regression
def test_decision_delete_failure_rolls_back_and_raises():
    """process_isrc_dupe_decision rolls back the earlier insert and re-raises when
    deleting the pair fails.
    """
    # Inserts succeed, the pool delete fails -> the whole decision is rolled back.
    fake = FakeConn(fail_on='execute')
    with patch(
        'pifitness.music.queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            process_isrc_dupe_decision('ISRC-A', 'ISRC-B', 'ISRC-A', False)

    assert len(fake.cursor_obj.executemany_calls) == 1
    assert fake.commits == 0
    assert fake.rollbacks == 1
    assert fake.closed is True


@pytest.mark.regression
def test_rescan_issues_call_in_one_transaction():
    """run_isrc_duplicate_finder runs the finder procedure on one connection and commits
    once.
    """
    fake = FakeConn()
    with patch(
        'pifitness.music.queries.get_conn',
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


@pytest.mark.regression
def test_rescan_failure_rolls_back_and_raises():
    """run_isrc_duplicate_finder rolls back and re-raises when the procedure call fails.
    """
    # T08: a failed CALL must not look like a successful re-scan.
    fake = FakeConn(fail_on='execute')
    with patch(
        'pifitness.music.queries.get_conn',
        return_value=fake,
    ):
        with pytest.raises(RuntimeError):
            run_isrc_duplicate_finder()

    assert fake.commits == 0
    assert fake.rollbacks == 1
    assert fake.closed is True
