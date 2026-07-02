"""
Admin Query Functions
=====================

Database query functions extracted from frontend_functions/admin_module.py,
admin_task_management.py, and admin_control_panel.py.
These functions return plain Python data structures with no Streamlit dependencies.
"""

from typing import List, Dict, Any, Optional, Union, Sequence, cast
from datetime import datetime
import math
from backend_functions.database_functions import qec, sql_to_dict, sql_to_list


# ---------------------------------------------------------------------------
# Service Management
# ---------------------------------------------------------------------------

def get_api_service_list() -> Sequence[Dict[str, Any]]:
    """
    Retrieve all API services from the database.

    Returns:
        Sequence[Dict[str, Any]]: List of API service records containing
        api_service_name and related configuration fields.
    """
    sql = "SELECT * FROM api_services.api_service_list"
    result = sql_to_dict(sql)
    return result if result else []


def get_distinct_api_service_names() -> List[str]:
    """
    Get distinct API service names.

    Returns:
        List[str]: List of service names.
    """
    sql = "SELECT DISTINCT api_service_name FROM api_services.api_service_list"
    return sql_to_list(sql)


def insert_api_service(service_name: str) -> Union[str, List[str], None]:
    """
    Insert a new API service.

    Args:
        service_name (str): Name of the service to create.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "INSERT INTO api_services.api_service_list (api_service_name) VALUES (%s)"
    return qec(sql, [service_name])


def delete_api_service(service_name: str) -> Union[str, List[str], None]:
    """
    Delete an API service by name.

    Args:
        service_name (str): Name of the service to delete.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "DELETE FROM api_services.api_service_list WHERE api_service_name = %s"
    return qec(sql, [service_name])


def get_function_library() -> Sequence[Dict[str, Any]]:
    """
    Retrieve the API function library, ordered by service and function name.

    Returns:
        Sequence[Dict[str, Any]]: List of function library records.
    """
    sql = """
        SELECT * FROM api_services.function_library
        ORDER BY api_service_name, python_extraction_function, friendly_name
    """
    result = sql_to_dict(sql)
    return result if result else []


def insert_function_library_entry(fields: Dict[str, Any]) -> Union[str, List[str], None]:
    """
    Insert a new entry into the function library.

    Args:
        fields (Dict[str, Any]): Dictionary of column_name -> value pairs.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    columns = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    params = list(fields.values())
    sql = f"INSERT INTO api_services.function_library ({columns}) VALUES ({placeholders})"
    return qec(sql, params)


def update_function_library_entry(friendly_name: str, updates: Dict[str, Any]) -> Union[str, List[str], None]:
    """
    Update an existing entry in the function library.

    Args:
        friendly_name (str): The friendly_name identifying the row to update.
        updates (Dict[str, Any]): Dictionary of column_name -> new value pairs.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    set_clause = ", ".join(f"{key} = %s" for key in updates.keys())
    params = list(updates.values()) + [friendly_name]
    sql = f"UPDATE api_services.function_library SET {set_clause} WHERE friendly_name = %s"
    return qec(sql, params)


def delete_function_library_entry(friendly_name: str) -> Union[str, List[str], None]:
    """
    Delete an entry from the function library.

    Args:
        friendly_name (str): The friendly_name of the entry to delete.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "DELETE FROM api_services.function_library WHERE friendly_name = %s"
    return qec(sql, [friendly_name])


# ---------------------------------------------------------------------------
# Credential Management
# ---------------------------------------------------------------------------

def get_credential_requirements() -> Sequence[Dict[str, Any]]:
    """
    Retrieve credential requirements for all API services.

    Returns:
        Sequence[Dict[str, Any]]: List of records with
        api_service_name and api_credential_requirements.
    """
    sql = "SELECT api_service_name, api_credential_requirements FROM api_services.api_service_list"
    result = sql_to_dict(sql)
    return result if result else []


def upsert_credentials(service_name: str, encrypted_credentials: str) -> Union[str, List[str], None]:
    """
    Insert or update credentials for a service.

    Args:
        service_name (str): The API service name.
        encrypted_credentials (str): The encrypted credential blob.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = """
        INSERT INTO api_services.credentials (api_service_name, api_credentials)
        VALUES (%s, %s)
        ON CONFLICT (api_service_name)
        DO UPDATE SET api_credentials = EXCLUDED.api_credentials
    """
    return qec(sql, (service_name, encrypted_credentials))


def delete_credentials(service_name: str) -> Union[str, List[str], None]:
    """
    Delete credentials for a service.

    Args:
        service_name (str): The API service name.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "DELETE FROM api_services.credentials WHERE api_service_name = %s"
    return qec(sql, [service_name])


# ---------------------------------------------------------------------------
# Task Management
# ---------------------------------------------------------------------------

def get_task_configuration(task_id: Optional[int] = None) -> Sequence[Dict[str, Any]]:
    """
    Retrieve task configuration records.

    Args:
        task_id (Optional[int]): If provided, return only the matching task.

    Returns:
        Sequence[Dict[str, Any]]: List of task configuration records.
    """
    if task_id is not None:
        sql = "SELECT * FROM tasks.task_configuration WHERE task_id = %s"
        result = sql_to_dict(sql, (task_id,))
    else:
        sql = "SELECT task_id, task_name, display_icon, task_frequency FROM tasks.task_configuration ORDER BY task_name"
        result = sql_to_dict(sql)
    return result if result else []


def get_placeholder_task_id() -> Optional[int]:
    """
    Get the task_id for the 'placeholder_task' configuration entry.

    Returns:
        Optional[int]: The task_id if found, None otherwise.
    """
    from backend_functions.database_functions import one_sql_result
    sql = "SELECT MIN(task_id) FROM tasks.task_configuration WHERE task_name = 'placeholder_task'"
    return one_sql_result(sql)


def insert_placeholder_task() -> Union[str, List[str], None]:
    """
    Insert a placeholder task into task_configuration.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "INSERT INTO tasks.task_configuration (task_name) VALUES (%s)"
    return qec(sql, ['placeholder_task'])


def get_task_config_by_id(task_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single task configuration by its ID.

    Args:
        task_id (int): The task ID.

    Returns:
        Optional[Dict[str, Any]]: The task configuration record, or None.
    """
    sql = "SELECT * FROM tasks.task_configuration WHERE task_id = %s"
    result = sql_to_dict(sql, (task_id,))
    return result[0] if result else None


def insert_task_configuration(fields: Dict[str, Any]) -> Union[str, List[str], None]:
    """
    Insert a new task configuration record.

    Args:
        fields (Dict[str, Any]): Column-value pairs for the task configuration.
            Expected fields include: task_name, description, display_icon, task_frequency,
            priority, hours, interval_minutes, api_function, python_function.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    columns = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    params = list(fields.values())
    sql = f"INSERT INTO tasks.task_configuration ({columns}) VALUES ({placeholders})"
    return qec(sql, params)


def delete_task_configuration(task_id: int) -> Union[str, List[str], None]:
    """
    Delete a task configuration entry.

    Args:
        task_id (int): The task ID to delete.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "DELETE FROM tasks.task_configuration WHERE task_id = %s"
    return qec(sql, (task_id,))


def delete_fact_configuration(fact_id: int) -> Union[str, List[str], None]:
    """
    Delete a fact configuration entry.

    Args:
        fact_id (int): The fact ID to delete.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "DELETE FROM tasks.fact_configuration WHERE fact_id = %s"
    return qec(sql, (int(fact_id),))


def update_task_configuration(task_id: int, is_active: bool, task_frequency: str, **kwargs) -> Union[str, List[str], None]:
    """
    Update a task configuration entry.

    NOTE: The tasks.task_configuration table does NOT have an 'is_active' column.
    Instead, the active/inactive state is represented by the task_frequency value:
    - Active tasks: task_frequency = 'Hourly', 'Daily', 'Weekly', 'Monthly'
    - Inactive tasks: task_frequency = 'Inactive'

    Args:
        task_id (int): The task ID to update.
        is_active (bool): Whether the task should be active.
            If True, task_frequency is used as provided.
            If False, task_frequency is forced to 'Inactive'.
        task_frequency (str): The new frequency for the task.
        **kwargs: Additional fields to update (frontend names, mapped below).

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    set_clauses = ["task_frequency = %s"]
    # When is_active is False, force frequency to 'Inactive'
    resolved_frequency = 'Inactive' if not is_active else task_frequency
    params = [resolved_frequency]
    
    # Map frontend field names to database column names
    # The task_configuration table uses legacy column names different from the React UI
    field_mapping = {
        'description': 'task_description',
        'display_icon': 'display_icon',
        'priority': 'task_priority',
        'hours': 'task_start_hour',
        'interval_minutes': 'task_interval',
        'api_function': 'api_function_name',
        'python_function': 'python_execution_function',
    }
    
    for key, db_field in field_mapping.items():
        if key in kwargs:
            set_clauses.append(f"{db_field} = %s")
            params.append(kwargs[key])
    
    sql = f"UPDATE tasks.task_configuration SET {', '.join(set_clauses)} WHERE task_id = %s"
    params.append(str(task_id))
    
    return qec(sql, params)


def upsert_fact_configuration(fields: Dict[str, Any], is_insert: bool = True,
                              fact_id: Optional[int] = None) -> Union[str, List[str], None]:
    """
    Insert or update a fact configuration record.

    Args:
        fields (Dict[str, Any]): Column-value pairs for the fact record.
        is_insert (bool): True for INSERT, False for UPDATE.
        fact_id (Optional[int]): Required for UPDATE operations.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    if is_insert:
        columns = ", ".join(fields.keys())
        placeholders = ", ".join(["%s"] * len(fields))
        params = list(fields.values())
        sql = f"INSERT INTO tasks.fact_configuration ({columns}) VALUES ({placeholders})"
        return qec(sql, params)
    else:
        set_clause = ", ".join(f"{key} = %s" for key in fields.keys())
        params = list(fields.values()) + [fact_id]
        sql = f"UPDATE tasks.fact_configuration SET {set_clause} WHERE fact_id = %s"
        return qec(sql, params)


def get_distinct_task_names() -> Sequence[str]:
    """
    Get distinct task names from the task execution view.

    Returns:
        Sequence[str]: List of distinct task names.
    """
    return sql_to_list("SELECT DISTINCT task_name FROM tasks.vw_task_execution ORDER BY task_name")


def get_task_execution_view() -> Sequence[Dict[str, Any]]:
    """
    Get the full task execution view.

    Returns:
        Sequence[Dict[str, Any]]: List of task execution records.
    """
    sql = "SELECT * FROM tasks.vw_task_execution"
    result = sql_to_dict(sql)
    return result if result else []


def get_task_scheduling_view() -> Sequence[Dict[str, Any]]:
    """
    Get the task scheduling view data (task_config table).

    Returns:
        Sequence[Dict[str, Any]]: List of task config records for scheduling.
    """
    sql = "SELECT * FROM tasks.task_config"
    result = sql_to_dict(sql)
    return result if result else []


# ---------------------------------------------------------------------------
# Database Session Monitoring
# ---------------------------------------------------------------------------

def get_active_db_sessions() -> Sequence[Dict[str, Any]]:
    """
    Retrieve currently active database sessions (non-idle queries).

    Returns:
        Sequence[Dict[str, Any]]: List of active session records with
        pid, state, query, and run_length.
    """
    sql = """
        SELECT pid, state, query,
            current_timestamp - query_start AS run_length
        FROM pg_stat_activity
        WHERE state != 'idle'
            AND query NOT LIKE 'SELECT pid%'
        ORDER BY query_start
    """
    result = sql_to_dict(sql)
    return result if result else []


def kill_db_session(pid: int) -> Union[str, List[str], None]:
    """
    Terminate a database session by PID.

    Args:
        pid (int): The process ID to terminate.

    Returns:
        Union[str, List[str], None]: Error messages if any, None on success.
    """
    sql = "SELECT pg_terminate_backend(%s)"
    return qec(sql, (int(pid),))


# ---------------------------------------------------------------------------
# DB Info — Task Summary Chart
# ---------------------------------------------------------------------------

def get_task_summary_chart() -> Sequence[Dict[str, Any]]:
    """
    Retrieve task summary chart data from the database.

    Returns:
        Sequence[Dict[str, Any]]: List of task summary records with timing,
        execution count, and recency details from tasks.vw_task_summary_chart.
    """
    sql = "SELECT * FROM tasks.vw_task_summary_chart"
    result = sql_to_dict(sql)
    return result if result else []


def get_task_logs(task_id: int, limit: int = 100) -> Sequence[Dict[str, Any]]:
    """
    Retrieve execution log entries for a specific task.

    Args:
        task_id (int): The task ID to filter logs by.
        limit (int): Maximum number of rows to return (default: 100).

    Returns:
        Sequence[Dict[str, Any]]: List of task execution log records from application_events.
    """
    sql = """SELECT * FROM logging.application_events 
             WHERE event_category LIKE %s 
             ORDER BY event_time_utc DESC 
             LIMIT %s"""
    result = sql_to_dict(sql, (f'%Task #{task_id}%', limit))
    return result if result else []


# ---------------------------------------------------------------------------
# DB Info — Database Size
# ---------------------------------------------------------------------------

def get_db_size_chart() -> Sequence[Dict[str, Any]]:
    """
    Retrieve historical database size growth data.

    Returns:
        Sequence[Dict[str, Any]]: List of records with date_utc, table_size_mb,
        index_size_mb, other_size_mb, total_size_mb from logging.vw_db_size_chart.
    """
    sql = "SELECT * FROM logging.vw_db_size_chart ORDER BY date_utc ASC"
    result = sql_to_dict(sql)
    return result if result else []


def get_db_size_breakdown() -> Sequence[Dict[str, Any]]:
    """
    Retrieve current database size breakdown by table.

    Returns:
        Sequence[Dict[str, Any]]: List of records with table_name, table_size_mb,
        index_size_mb, other_size_mb, total_size_mb from logging.vw_db_size.
    """
    sql = "SELECT * FROM logging.vw_db_size ORDER BY total_size_mb DESC"
    result = sql_to_dict(sql)
    return result if result else []


# ---------------------------------------------------------------------------
# Log & Event History
# ---------------------------------------------------------------------------

def get_event_history(search_val: Optional[str] = None,
                       errors_only: bool = False,
                       ignore_skips: bool = False,
                       event_type: Optional[str] = None,
                       limit: int = 250) -> Sequence[Dict[str, Any]]:
    """
    Retrieve event history from the logging view with optional filters.

    Args:
        search_val (Optional[str]): Text search across event_type, description, error_text.
        errors_only (bool): If True, show only error events.
        ignore_skips (bool): If True, filter out skipped rows.
        event_type (Optional[str]): Filter by specific event type.
        limit (int): Maximum number of rows to return.

    Returns:
        Sequence[Dict[str, Any]]: List of event history records.
    """
    sql = "SELECT * FROM logging.vw_all_event_history WHERE 1=1"
    params = []

    if search_val and len(search_val) > 2:
        sql += """ AND COALESCE(event_type,'') || COALESCE(description,'') || COALESCE(error_text,'')
                   LIKE %s"""
        params.append(f"%{search_val}%")

    if errors_only:
        sql += " AND is_error"

    if ignore_skips:
        sql += " AND not_skip_row"

    if event_type and event_type != 'All':
        sql += " AND event_type = %s"
        params.append(event_type)

    sql += f" LIMIT {limit}"
    result = sql_to_dict(sql, params)
    return result if result else []


def get_log_tables_simple() -> Sequence[str]:
    """
    Get list of log table names from the logging schema.

    Returns:
        Sequence[str]: List of log table names.
    """
    from backend_functions.database_functions import get_log_tables
    return list(get_log_tables())


def get_log_data_simple(log_table: str, limit: int = 100) -> Sequence[Dict[str, Any]]:
    """
    Get log data from a specific log table.

    Args:
        log_table (str): The log table name.
        limit (int): Maximum number of rows to return.

    Returns:
        Sequence[Dict[str, Any]]: List of log records.
        NULL database values are returned as None (no NaN conversion needed).
    """
    from backend_functions.database_functions import get_log_data
    logs = get_log_data(log_table)
    # sql_to_dict() returns None for NULL values, no pandas NaN cleanup needed
    return logs[:limit] if logs else []


# ---------------------------------------------------------------------------
# Task Execution (direct table queries)
# ---------------------------------------------------------------------------

def insert_api_service_list_entry(service_name: str):
    """
    Insert a new API service (simple wrapper, same as insert_api_service).

    Args:
        service_name (str): Name of the service.
    """
    return insert_api_service(service_name)


__all__ = [
    'get_api_service_list',
    'get_distinct_api_service_names',
    'insert_api_service',
    'delete_api_service',
    'get_function_library',
    'insert_function_library_entry',
    'update_function_library_entry',
    'delete_function_library_entry',
    'get_credential_requirements',
    'upsert_credentials',
    'delete_credentials',
    'get_task_configuration',
    'get_placeholder_task_id',
    'insert_placeholder_task',
    'get_task_config_by_id',
    'delete_task_configuration',
    'delete_fact_configuration',
    'upsert_fact_configuration',
    'get_distinct_task_names',
    'get_task_execution_view',
    'get_task_scheduling_view',
    'get_active_db_sessions',
    'kill_db_session',
    'get_event_history',
    'get_log_tables_simple',
    'get_log_data_simple',
    'update_task_configuration',
    'get_task_summary_chart',
    'get_db_size_chart',
    'get_db_size_breakdown',
    'get_task_logs',
]
