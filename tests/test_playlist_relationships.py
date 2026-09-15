"""Unit tests for ensure_playlist_relationships (000-001 T06).

Verifies AC-5/AC-8: the relationship re-establishment path is non-destructive —
it never issues a DELETE, whether the missing_relationships view is empty or
returns rows. All DB and Spotify calls are mocked.
"""
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_ensure_playlist_relationships_no_delete_on_empty():
    """AC-5/AC-8: when missing_relationships is empty, the function returns
    immediately and never calls qec (no DELETE, no INSERT).
    """
    from backend_functions.music_functions import ensure_playlist_relationships

    with patch('backend_functions.music_functions.sql_to_dict', return_value=[]) as mock_sql, \
         patch('backend_functions.music_functions.qec') as mock_qec:
        result = ensure_playlist_relationships(None)

    assert result is None
    mock_sql.assert_called_once()
    mock_qec.assert_not_called()


def test_ensure_playlist_relationships_no_delete_on_rows():
    """AC-5/AC-8: even when missing_relationships returns rows (children need
    creation), the function must NOT issue any DELETE — only INSERTs via
    parameterized qec calls.
    """
    from backend_functions.music_functions import ensure_playlist_relationships

    mock_missing = [
        {"playlist_id": "parent1", "playlist_name": "Running",
         "needs_auto": True, "needs_manual": False, "needs_recs": False},
    ]

    with patch('backend_functions.music_functions.sql_to_dict', return_value=mock_missing), \
         patch('backend_functions.music_functions.get_spotify_client', return_value={"client": MagicMock()}), \
         patch('backend_functions.music_functions.gen_playlist', return_value=("client", "newchild123")) as mock_gen, \
         patch('backend_functions.music_functions.qec') as mock_qec:
        result = ensure_playlist_relationships(None)

    # No DELETE statements on qec — only INSERTs
    for db_call in mock_qec.call_args_list:
        args, kwargs = db_call
        sql_text = args[0] if args else kwargs.get('t_sql', '')
        assert 'DELETE' not in sql_text.upper(), f"Unexpected DELETE in: {sql_text}"

    # gen_playlist was called to create the missing child
    mock_gen.assert_called_once()

    # qec was called for the INSERT with parameterized values (not interpolated)
    assert len(mock_qec.call_args_list) >= 1
    insert_call = mock_qec.call_args_list[0]
    insert_args, insert_kwargs = insert_call
    assert 'INSERT' in insert_args[0]
    assert insert_kwargs['p'] == ("parent1", "newchild123", "auto")


def test_ensure_playlist_relationships_uses_parameterized_insert():
    """The INSERT must use %s placeholders, not f-string interpolation
    (SQL injection prevention — backend-engineer directive).
    """
    from backend_functions.music_functions import ensure_playlist_relationships

    mock_missing = [
        {"playlist_id": "parent1", "playlist_name": "Running",
         "needs_manual": True, "needs_auto": False, "needs_recs": False},
    ]

    with patch('backend_functions.music_functions.sql_to_dict', return_value=mock_missing), \
         patch('backend_functions.music_functions.get_spotify_client', return_value={"client": MagicMock()}), \
         patch('backend_functions.music_functions.gen_playlist', return_value=("client", "newchild123")), \
         patch('backend_functions.music_functions.qec') as mock_qec:
        ensure_playlist_relationships(None)

    insert_call = mock_qec.call_args_list[0]
    args, kwargs = insert_call
    sql_text = args[0]
    # Must use %s placeholders, not quoted string interpolation
    assert '%s' in sql_text
    assert "'{id}'" not in sql_text
    assert "'{new_id}'" not in sql_text
