from unittest.mock import MagicMock
from pifitness.integrations.garmin.endpoints import resolve_endpoint
from backend_functions.json_extractors import get_pirate_data


def test_get_pirate_data_happy_path():
    endpoint = 'usersummary.daily'
    ep = resolve_endpoint(endpoint)
    client = MagicMock()
    client.request_json.return_value = {"fake": "data"}

    result, error = get_pirate_data(client, endpoint)

    assert result == {"fake": "data"}
    assert error is None
    client.request_json.assert_called_once_with(
        host=ep.host,
        path=ep.path,
        params=dict(ep.defaults) if ep.defaults else None,
    )


def test_get_pirate_data_invalid_endpoint_returns_error():
    endpoint = 'nonexistent.fake'
    client = MagicMock()

    result, error = get_pirate_data(client, endpoint)

    assert result is None
    assert endpoint in error


def test_get_pirate_data_client_exception_returns_error():
    endpoint = 'usersummary.daily'
    client = MagicMock()
    client.request_json.side_effect = RuntimeError("boom")

    result, error = get_pirate_data(client, endpoint)

    assert result is None
    assert "boom" in error


def test_no_pirate_garmin_imports_in_json_extractors():
    import ast
    import pathlib

    src = pathlib.Path(
        'backend/backend_functions/json_extractors.py'
    ).read_text(encoding='utf-8')
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert 'pirate_garmin' not in (
                node.module or ''
            ), f'FAIL: pirate_garmin import found: {node.module}'
            for alias in node.names:
                assert 'pirate_garmin' not in alias.name
        elif isinstance(node, ast.Import):
            for alias in node.names:
                assert 'pirate_garmin' not in alias.name