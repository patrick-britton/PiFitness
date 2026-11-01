"""get_pirate_data returns a (data, error) pair and never raises, whatever the endpoint
or client does.
"""

import pytest
from unittest.mock import MagicMock
from pifitness.integrations.garmin.endpoints import resolve_endpoint
from backend_functions.json_extractors import get_pirate_data


@pytest.mark.contract
def test_get_pirate_data_happy_path():
    """get_pirate_data returns the client's JSON and no error, requesting the resolved
    endpoint's host, path and default parameters.
    """
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


@pytest.mark.contract
def test_get_pirate_data_invalid_endpoint_returns_error():
    """get_pirate_data returns no data and an error naming the endpoint when the
    endpoint is unknown.
    """
    endpoint = 'nonexistent.fake'
    client = MagicMock()

    result, error = get_pirate_data(client, endpoint)

    assert result is None
    assert endpoint in error


@pytest.mark.contract
def test_get_pirate_data_client_exception_returns_error():
    """get_pirate_data returns no data and an error containing the message when the
    client raises.
    """
    endpoint = 'usersummary.daily'
    client = MagicMock()
    client.request_json.side_effect = RuntimeError("boom")

    result, error = get_pirate_data(client, endpoint)

    assert result is None
    assert "boom" in error
