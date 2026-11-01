"""
Unit tests for pifitness.integrations._common.save_token_to_db.

Fail-loud contract: qec failure raises RuntimeError. Silent failure is
unacceptable -- cross-device session sync depends on this write.
"""

import pytest
from unittest.mock import patch

from pifitness.integrations._common import save_token_to_db


def _garmin_session(di_expires=None, it_expires=None):
    s = {"di": {"token": {"access_token": "a1", "refresh_token": "r1"}},
         "it": {"token": {"access_token": "a2", "refresh_token": "r2"}}}
    if di_expires is not None:
        s["di"]["token"]["refresh_token_expires_at"] = di_expires
    if it_expires is not None:
        s["it"]["token"]["refresh_token_expires_at"] = it_expires
    return s


@pytest.mark.unit
def test_save_garmin_with_both_expires_uses_min():
    s = _garmin_session(di_expires=2000, it_expires=1000)
    with patch("backend_functions.database_functions.qec", return_value=None) as m:
        save_token_to_db("Garmin", s)
    assert m.call_count == 2
    insert_sql = m.call_args_list[1].args[0]
    assert "INSERT INTO api_services.api_tokens" in insert_sql
    assert "is_active" in insert_sql
    assert "refresh_token_expires_at_utc" in insert_sql


@pytest.mark.unit
def test_save_garmin_no_expires_3col():
    s = _garmin_session()
    with patch("backend_functions.database_functions.qec", return_value=None) as m:
        save_token_to_db("Garmin", s)
    insert_sql = m.call_args_list[1].args[0]
    assert "refresh_token_expires_at_utc" not in insert_sql
    assert "is_active" in insert_sql


@pytest.mark.unit
def test_save_raises_on_deactivate_fail():
    s = _garmin_session()
    err = ["Query failure: boom", "SQL: ...", "Params: ..."]
    with patch("backend_functions.database_functions.qec", return_value=err):
        with pytest.raises(RuntimeError, match="deactivate failed"):
            save_token_to_db("Garmin", s)


@pytest.mark.unit
def test_save_raises_on_insert_fail():
    s = _garmin_session()
    err = ["Query failure: insert boom", "SQL: ...", "Params: ..."]
    with patch("backend_functions.database_functions.qec", side_effect=[None, err]):
        with pytest.raises(RuntimeError, match="insert failed"):
            save_token_to_db("Garmin", s)
