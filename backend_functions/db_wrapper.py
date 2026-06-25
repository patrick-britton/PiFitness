"""
Async Database Wrapper
=====================

Wraps synchronous database functions with asyncio.to_thread for use in
FastAPI async endpoint handlers. Prevents blocking the event loop.

Usage:
    from backend_functions.db_wrapper import async_sql_to_dict

    @router.get("/api/activities")
    async def list_activities():
        return await async_sql_to_dict("SELECT * FROM activities.activities LIMIT 10")
"""

import asyncio
from functools import partial
from typing import Any, Dict, List, Optional, Sequence, Union

from backend_functions.database_functions import (
    qec,
    sql_to_dict,
    sql_to_list,
    one_sql_result,
)


async def async_sql_to_dict(
    query: str, params: Optional[tuple] = None
) -> Sequence[Dict[str, Any]]:
    """
    Execute a SQL query asynchronously and return results as a list of dicts.

    Args:
        query (str): The SQL query string.
        params (Optional[tuple]): Query parameters for parameterized execution.

    Returns:
        Sequence[Dict[str, Any]]: Query results as a list of dictionaries.
    """
    loop = asyncio.get_running_loop()
    if params is not None:
        fn = partial(sql_to_dict, query, params)
    else:
        fn = partial(sql_to_dict, query)
    return await loop.run_in_executor(None, fn)


async def async_sql_to_list(query: str) -> list:
    """
    Execute a SQL query asynchronously and return a flat list of first-column values.

    Args:
        query (str): The SQL query string.

    Returns:
        list: Flat list of values from the first column.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(sql_to_list, query))


async def async_one_sql_result(query: str) -> Any:
    """
    Execute a SQL query asynchronously and return a single scalar value.

    Args:
        query (str): The SQL query string.

    Returns:
        Any: The first column of the first row, or None if no results.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(one_sql_result, query))


async def async_qec(
    query: str, params: Optional[Union[tuple, list]] = None
) -> Union[str, List[str], None]:
    """
    Execute a SQL query asynchronously with commit (INSERT/UPDATE/DELETE).

    Args:
        query (str): The SQL query string.
        params (Optional[Union[tuple, list]]): Query parameters.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(qec, query, params))


__all__ = [
    "async_sql_to_dict",
    "async_sql_to_list",
    "async_one_sql_result",
    "async_qec",
]