"""ensure_playlist_relationships re-creates missing relationships without ever issuing a
DELETE.
"""
import pytest
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.mark.regression
def test_ensure_playlist_relationships_no_delete_on_empty():
    """ensure_playlist_relationships returns None and performs no database writes when
    no relationships are missing.
    """
    from backend_functions.music_functions import ensure_playlist_relationships

    with patch('backend_functions.music_functions.sql_to_dict', return_value=[]) as mock_sql, \
         patch('backend_functions.music_functions.qec') as mock_qec:
        result = ensure_playlist_relationships(None)

    assert result is None
    mock_sql.assert_called_once()
    mock_qec.assert_not_called()


@pytest.mark.regression
def test_ensure_playlist_relationships_no_delete_on_rows():
    """ensure_playlist_relationships creates each missing child playlist and records it
    with a parameterized insert, never issuing a DELETE.
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
