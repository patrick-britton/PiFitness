import importlib
import json
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import pytz
from fastapi import HTTPException
from psycopg2.extras import execute_values

from backend_functions.database_functions import sql_to_dict, qec, con_cur, get_table_row_count, one_sql_result, \
    get_conn, sql_to_list
from backend_functions.helper_functions import get_sync_dates, get_last_date
from backend_functions.logging_functions import start_timer, log_app_event, elapsed_ms
from backend_functions.service_logins import sql_rate_limited

def ultimate_task_executioner(force_task_name=None, force_task_id=None):
    """
    Enhanced task execution engine with improved security, error handling, and modular design.
    Maintains exact compatibility with legacy system while addressing critical issues.

    Args:
        force_task_name (str, optional): Force execution of tasks matching this name
        force_task_id (int, optional): Force execution of specific task ID

    Returns:
        None
    """
    t0 = start_timer()
    client_dict = None

    # Obtain the list of tasks using parameterized query to prevent SQL injection
    sql = "SELECT * FROM tasks.vw_task_info"
    params = []

    if force_task_id:
        sql = f"{sql} WHERE task_id = %s"
        params = [force_task_id]
    elif force_task_name:
        sql = f"{sql} WHERE task_name LIKE %s"
        params = [f'%{force_task_name}%']
    else:
        sql = f"{sql} WHERE should_execute"

    sql = f"{sql} ORDER BY api_service_name, next_planned_execution_utc"

    try:
        task_list = sql_to_dict(sql, params)
    except Exception as e:
        log_app_event(cat='Task Executioner',
                      desc='Failed to fetch task list',
                      err=f'Database error: {str(e)}',
                      data_event='Critical')
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task list: {str(e)}"
        )

    print(f"{len(task_list)} tasks found: {task_list}")
    log_app_event(cat='Task Executioner',
                  desc=f'Planned execution of {len(task_list)} tasks')

    # Default the api service name to none -- will trigger a fresh login
    api_service_name = None
    failed_ids = set()
    failure_reasons = {}
    if not task_list:
        print('Task List empty, breaking')
        return

    for task_dict in task_list:
        task_t0 = start_timer()
        task_id = task_dict.get('task_id')
        task_name = task_dict.get('task_name')
        print(f"Starting task #{task_id} : {task_name}")

        try:
            log_app_event(cat=f"Task #{task_id}: {task_name}",
                          desc='Task Started',
                          task_id=task_id,
                          data_event='Begin')

            run_elt = task_dict.get('run_extract')
            run_python = task_dict.get('run_python')
            task_fail = False
            fail_msg = None

            # Validate that the task has at least one executable action configured
            if not run_elt and not run_python:
                error_msg = 'No valid configuration found.'
                log_app_event(cat=f"Task #{task_id}: {task_name}",
                              desc='No valid configuration found.',
                              err=error_msg,
                              task_id=task_id,
                              data_event='Validation')
                reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
                failed_ids.add(task_id)
                failure_reasons[task_id] = error_msg
                continue

            if run_elt:
                # Defensive programming: check for None values
                if not task_dict or not task_dict.get('api_service_name'):
                    error_msg = "Task dictionary missing required api_service_name"
                    log_app_event(cat=f"Task #{task_id}: {task_name}",
                                  desc='Missing API service name',
                                  err=error_msg,
                                  task_id=task_id,
                                  data_event='Validation')
                    reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
                    failed_ids.add(task_id)
                    failure_reasons[task_id] = error_msg
                    continue

                if api_service_name != task_dict.get('api_service_name'):
                    client_dict = None
                    api_service_name = task_dict.get('api_service_name')

                module_function = task_dict.get('python_login_function')
                
                # If no login function configured, skip login and proceed to extraction
                # This is used by Pirate Garmin tasks which handle auth internally
                if not module_function:
                    print(f"No login function configured for {api_service_name} — skipping login")
                    log_app_event(cat=f"Task #{task_id}: {task_name}",
                                  desc='No login function — skipping',
                                  task_id=task_id,
                                  data_event='Login')
                else:
                    print(f"Refreshing client for {api_service_name}")

                    try:
                        if '.' not in module_function:
                            raise ValueError(f"Invalid module function format: {module_function}")
                        module_name, login_function_name = module_function.rsplit('.', 1)
                        module = importlib.import_module(module_name)
                        login_function = getattr(module, login_function_name)
                        l_t0 = start_timer()
                        client_dict = login_function(client_dict)

                        log_app_event(cat=f"Task #{task_id}: {task_name}",
                                      desc='Login Success',
                                      task_id=task_id,
                                      exec_time=elapsed_ms(l_t0),
                                      data_event='Login')
                    except Exception as e:
                        error_msg = f"Client error: {str(e)}"
                        log_app_event(cat=f"Task #{task_id}: {task_name}",
                                      desc='Failed to establish client',
                                      task_id=task_id,
                                      err=error_msg,
                                      data_event='Login')
                        reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
                        failed_ids.add(task_id)
                        failure_reasons[task_id] = error_msg
                        client_dict = None
                        print(f"Failed client initialization for task #{task_id}: {task_name}")
                        continue

                task_fail, client_dict = extract_load_flatten(client_dict, task_dict)

            if run_python and not task_fail:
                print(f"Starting Python Execution for task #{task_id} : {task_name}")
                task_fail = execute_python(d=task_dict)

            if not task_fail:
                print(f"{task_name}: Successful")
                log_app_event(cat=f"Task #{task_id}: {task_name}",
                              desc=f"Successful Completion",
                              exec_time=elapsed_ms(task_t0),
                              task_id=task_id,
                              data_event='Complete')
                reconcile_task_dates(task_dict)
            else:
                error_msg = f"Task execution failed for {task_name}"
                log_app_event(cat=f"Task #{task_id}: {task_name}",
                              desc=f"Task Failed",
                              err=error_msg,
                              task_id=task_id,
                              data_event='Complete')
                reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
                failed_ids.add(task_id)
                failure_reasons[task_id] = error_msg

        except Exception as e:
            error_msg = f"Unexpected error in task execution: {str(e)}"
            log_app_event(cat=f"Task #{task_id}: {task_name}",
                          desc='Unexpected Task Error',
                          err=error_msg,
                          task_id=task_id,
                          data_event='Critical')
            reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
            failed_ids.add(task_id)
            failure_reasons[task_id] = error_msg
            continue

    log_app_event(cat='Task Executioner',
                  desc=f'Execution complete {len(task_list)} tasks',
                  exec_time=elapsed_ms(t0))


    # Return summary including outcomes for frontend feedback
    all_results = [{
        'task_id': t.get('task_id'),
        'task_name': t.get('task_name'),
        'success': t.get('task_id') not in failed_ids,
        'error': t.get('task_id') in failure_reasons and failure_reasons[t.get('task_id')] or None
    } for t in task_list]

    return {
        'tasks_processed': len(task_list),
        'results': all_results,
        'status': 'complete'
    }

def extract_load_flatten(cd, td):
    """
    Enhanced ETL pipeline with improved error handling and validation.

    Args:
        cd: Client dictionary
        td: Task dictionary

    Returns:
        tuple: (task_fail, client_dict)
    """
    # Ensure client is established
    if not cd:
        service = td.get('api_service_name', 'Unknown')
        if service == 'Spotify':
            err_msg = 'Spotify login failed — token may be expired. Re-authorization required.'
        elif service == 'Pirate Garmin':
            err_msg = 'Garmin login failed — check credentials or pirate-garmin CLI status.'
        elif service == 'Garmin':
            err_msg = 'Garmin login failed — check credentials or pirate-garmin CLI status.'
        else:
            err_msg = f'Login failed for {service}'
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err=err_msg,
                      task_id=td.get('task_id'),
                      data_event='Login')
        reconcile_task_dates(td, task_fail=True, e=err_msg)
        return True, None

    if not cd.get('client'):
        error_msg = 'No client within dictionary'
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err=error_msg,
                      task_id=td.get('task_id'),
                      data_event='Login')
        reconcile_task_dates(td, task_fail=True, e=error_msg)
        return True, None

    extract_function_name = td.get('python_extraction_function')
    module_name = 'backend_functions.json_extractors'

    try:
        module = importlib.import_module(module_name)
        local_function = getattr(module, extract_function_name)
    except Exception as e:
        error_msg = f"Module Error: {str(e)}"
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed to get extraction function",
                      exec_time=0,
                      err=error_msg,
                      task_id=td.get('task_id'),
                      data_event='Module')
        reconcile_task_dates(td, task_fail=True, e=f"Failed To get extraction Function {e}")
        return True, cd

    # Extract JSON
    print(f"Extracting data for Task #{td.get('task_id')}: {td.get('task_name')}: Function: {local_function}")
    t0 = start_timer()

    try:
        json_data = local_function(client=cd.get('client'), td=td)
        if json_data:
            log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                          desc=f"Valid Extraction",
                          exec_time=elapsed_ms(t0),
                          task_id=td.get('task_id'),
                          data_event='Extract')
        print(f"Data Extraction Success for Task #{td.get('task_id')}: {td.get('task_name')}")
    except Exception as e:
        error_msg = f"Extraction error: {str(e)}"
        print(f"Data Extraction Failed for Task #{td.get('task_id')}: {td.get('task_name')} : {e}")
        json_data = None
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err=error_msg,
                      task_id=td.get('task_id'),
                      data_event='Extract')
        reconcile_task_dates(td, task_fail=True, e=f"Failed Extraction {e}")
        return True, cd

    if not json_data:
        error_msg = 'No API response to load'
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Loading ignored",
                      err=error_msg,
                      task_id=td.get('task_id'),
                      data_event='No data from API'
                      )
        reconcile_task_dates(td, task_fail=True, e=error_msg)
        return True, cd

    print(f"Loading data for Task #{td.get('task_id')}: {td.get('task_name')}")
    t0 = start_timer()

    try:
        json_loading(json_data, td)
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Successful Load",
                      exec_time=elapsed_ms(t0),
                      task_id=td.get('task_id'),
                      data_event='Load')
    except Exception as e:
        error_msg = f"Load Failure: {str(e)}"
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Load",
                      exec_time=elapsed_ms(t0),
                      err=error_msg,
                      task_id=td.get('task_id'),
                      data_event='Load')
        reconcile_task_dates(td, task_fail=True, e=f"Failed TO Load {e}")
        return True, cd

    t0 = start_timer()
    flatten_failure = execute_sproc(d=td, sproc_type='flatten')

    return flatten_failure, cd

def execute_sproc(d, sproc_type):
    """
    Execute stored procedure with improved error handling and parameterized queries.

    Args:
        d: Task dictionary
        sproc_type: Type of stored procedure ('flatten', 'interpolation', etc.)

    Returns:
        bool: True if failed, False if successful
    """
    print(f"Starting SPROC {sproc_type} for #{d.get('task_id')}: {d.get('task_name')}")
    retrieval_key = f"{sproc_type}_sproc"
    sproc_sql = d.get(retrieval_key)
    fail = False

    if not sproc_sql:
        fail = True
    elif sproc_sql in ['None', 'N/A', '']:
        fail = True

    sproc_type = sproc_type.capitalize()

    if fail:
        error_msg = f'Failed to extract key: {retrieval_key}'
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"SPROC Failure: {sproc_type}",
                      err=error_msg,
                      task_id=d.get('task_id'),
                      data_event=sproc_type)
        reconcile_task_dates(d, task_fail=True, e=error_msg)
        return True

    t0 = start_timer()

    try:
        sql = f"CALL {sproc_sql};"
        returns = qec(sql)

        if returns:
            log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                          desc=f"SPROC Failure: {sproc_type}",
                          err=returns,
                          task_id=d.get('task_id'),
                          data_event=sproc_type
                          )
            reconcile_task_dates(d, task_fail=True, e=f'Failed to execute sql: {returns}')
            return True
        else:
            log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                          desc=f"SPROC Success: {sproc_type}",
                          exec_time=elapsed_ms(t0),
                          task_id=d.get('task_id'),
                          data_event=sproc_type)
            return False
    except Exception as e:
        error_msg = f"SPROC execution error: {str(e)}"
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"SPROC Exception: {sproc_type}",
                      err=error_msg,
                      task_id=d.get('task_id'),
                      data_event=sproc_type)
        reconcile_task_dates(d, task_fail=True, e=error_msg)
        return True

def execute_python(d=None):
    """
    Execute Python function with enhanced error handling.

    Args:
        d: Task dictionary

    Returns:
        bool: True if failed, False if successful
    """
    print(f"Starting Python Execution for #{d.get('task_id')}: {d.get('task_name')}")
    module_function = d.get('python_execution_function')

    try:
        module_name, svc_function_name = module_function.rsplit('.', 1)
        module = importlib.import_module(module_name)
        local_function = getattr(module, svc_function_name)
    except Exception as e:
        error_msg = f"Module Error: {str(e)}"
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"Python Function Failure",
                      err=error_msg,
                      task_id=d.get('task_id'),
                      data_event='Python'
                      )
        reconcile_task_dates(d, task_fail=True, e=f"Python Failure: {e}")
        return True

    t0 = start_timer()

    try:
        local_function()
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"Python Function Completion",
                      exec_time=elapsed_ms(t0),
                      task_id=d.get('task_id'),
                      data_event='Python')
        return False

    except Exception as e:
        error_msg = f"Python Failure: {str(e)}"
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"Python Function Failure",
                      exec_time=elapsed_ms(t0),
                      err=error_msg,
                      task_id=d.get('task_id'),
                      data_event='Python')
        reconcile_task_dates(d, task_fail=True, e=f"Python Function Failure {e}")
        return True

def reconcile_task_dates(task_dict, task_fail=False, e=None):
    """
    Update task scheduling information with parameterized queries.

    Args:
        task_dict: Task dictionary
        task_fail: Boolean indicating if task failed
        e: Error message (if applicable)
    """
    task_id = int(task_dict.get('task_id'))

    if task_fail:
        raw_failures = task_dict.get('consecutive_failures')
        cons_failures = (int(raw_failures) if raw_failures is not None else 0) + 1
        interval_str = f'{cons_failures * 60} minutes'
        up_sql = """UPDATE tasks.task_configuration SET
                    last_executed_utc = CURRENT_TIMESTAMP,
                    last_failed_utc = CURRENT_TIMESTAMP,
                    next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL %s,
                    last_failure_message = %s,
                    consecutive_failures = consecutive_failures + 1
                    WHERE task_id = %s;"""
        params = [interval_str, e, task_id]
    else:
        freq = task_dict.get('task_frequency')
        friendly_name = task_dict.get('friendly_name')
        is_extract = task_dict.get('python_extraction_function') is not None

        if is_extract and friendly_name:
            value_current_sql = """SELECT CURRENT_TIMESTAMP::DATE = (SELECT MAX(value_recency) FROM api_services.function_library
                                WHERE friendly_name = %s)::DATE"""
            value_current = one_sql_result(value_current_sql, [friendly_name])
        else:
            value_current = True

        if not value_current:
            int_sql = "next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '2 hours'"
        elif freq == 'Hourly':
            interval_val = task_dict.get('task_interval', 1)
            if interval_val is None:
                interval_val = 1
            int_sql = f"next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '{int(interval_val)} hours'"
        elif freq == 'Daily':
            start_hour = task_dict.get('task_start_hour', 8)
            if start_hour is None:
                start_hour = 8
            int_sql = f"""next_planned_execution_utc =
                        (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '1 day')
                        + INTERVAL '{int(start_hour)} hours')
                        AT TIME ZONE 'America/Los_Angeles'"""
        elif freq == 'Weekly':
            start_hour = task_dict.get('task_start_hour', 8)
            if start_hour is None:
                start_hour = 8
            int_sql = f"""next_planned_execution_utc =
                            (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '7 days')
                            + INTERVAL '{int(start_hour)} hours')
                            AT TIME ZONE 'America/Los_Angeles'"""
        elif freq == 'Monthly':
            start_hour = task_dict.get('task_start_hour', 8)
            if start_hour is None:
                start_hour = 8
            int_sql = f"""next_planned_execution_utc =
                            (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '30 days')
                            + INTERVAL '{int(start_hour)} hours')
                            AT TIME ZONE 'America/Los_Angeles'"""
        else:
            int_sql = "next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '24 hours'"

        up_sql = f"""UPDATE tasks.task_configuration SET
                        last_executed_utc = CURRENT_TIMESTAMP,
                        last_succeeded_utc = CURRENT_TIMESTAMP,
                        {int_sql},
                        consecutive_failures = 0
                        WHERE task_id = %s;"""
        params = [task_id]

    result = qec(up_sql, params)
    if result:
        # qec returns a list [error_msg, failing_sql, failing_params] on failure
        error_msg = result[0] if result else "Unknown qec failure"
        log_app_event(
            cat=f"Task #{task_id}",
            desc='reconcile_task_dates UPDATE failed',
            err=error_msg,
            task_id=task_id,
            data_event='Reconcile'
        )
        raise RuntimeError(f"reconcile_task_dates failed for task {task_id}: {error_msg}")
    return

def metric_interpolation(task_dict):
    """
    Perform metric interpolation with enhanced error handling.

    Args:
        task_dict: Task dictionary

    Returns:
        bool: True if failed, False if successful
    """
    if not task_dict.get('interpolation_sproc'):
        error_msg = "task.get() returned None"
        log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                      desc=f"No Interpolation SPROC",
                      err=error_msg,
                      task_id=task_dict.get('task_id'),
                      data_event='Interpolation'
                      )
        reconcile_task_dates(task_dict, task_fail=True, e=f'Interpolation task.get() returned None')
        return True

    it0 = start_timer()
    src_table_schema = task_dict.get('interpolation_sproc')

    try:
        sch, tab, infer = src_table_schema.split('.')
    except Exception as e:
        error_msg = f"Invalid interpolation SPROC format: {str(e)}"
        log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                      desc=f"Interpolation Format Error",
                      err=error_msg,
                      task_id=task_dict.get('task_id'),
                      data_event='Interpolation')
        reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
        return True

    sql = f"""WITH ts_col as
            (SELECT column_name from information_schema.columns
            WHERE table_schema = %s and table_name = %s
            AND data_type = 'timestamp with time zone'
            ORDER BY ordinal_position LIMIT 1)

             select
             table_schema as src_schema,
             table_name as src_table,
             column_name as src_col,
             (SELECT * FROM ts_col) as src_ts_col
             FROM information_schema.columns
             WHERE table_schema = %s and table_name = %s
             and data_type in ('numeric',
                             'bigint',
                             'smallint',
                             'double precision',
                             'integer',
                             'int')"""

    try:
        numeric_cols = sql_to_dict(sql, [sch, tab, sch, tab])
    except Exception as e:
        error_msg = f"Interpolation query failed: {str(e)}"
        log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                      desc=f"Interpolation Query Failure",
                      err=error_msg,
                      task_id=task_dict.get('task_id'),
                      data_event='Interpolation')
        reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
        return True

    if not numeric_cols:
        error_msg = 'No Interpolation columns found'
        print(f"No interpolation columns for Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}")
        log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                      desc=f"Interpolation Failure",
                      err=error_msg,
                      task_id=task_dict.get('task_id'),
                      data_event='Interpolation'
                      )
        reconcile_task_dates(task_dict, task_fail=True, e='No Interpolation Columns')
        return True

    print(f"Starting interpolation Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}")
    print(f"{len(numeric_cols)} numeric columns: {numeric_cols}")

    for col in numeric_cols:
        sproc_sql = f"""CALL metrics.interpolate_metric(%s, %s, %s, %s, %s)"""
        print(f"Interpolating: {col.get('src_col')} :{sproc_sql}")

        try:
            returns = qec(sproc_sql, [
                col.get('src_schema'),
                col.get('src_table'),
                col.get('src_col'),
                col.get('src_ts_col'),
                infer
            ])

            if returns:
                print(f'Interpolation returns: {returns}')
                log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                              desc=f"Interpolation Failure: {col.get('src_col')}",
                              err=returns,
                              task_id=task_dict.get('task_id'),
                              data_event='Interpolation'
                              )
                reconcile_task_dates(task_dict, task_fail=True, e=f'Failed to execute sql: {returns}')
                return True
            else:
                log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                              desc=f"Interpolation Success: {col.get('src_col')}",
                              exec_time=elapsed_ms(it0),
                              task_id=task_dict.get('task_id'),
                              data_event='PartialInterpolation')

        except Exception as e:
            error_msg = f"Interpolation execution error: {str(e)}"
            log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                          desc=f"Interpolation Exception: {col.get('src_col')}",
                          err=error_msg,
                          task_id=task_dict.get('task_id'),
                          data_event='Interpolation')
            reconcile_task_dates(task_dict, task_fail=True, e=error_msg)
            return True

    log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                  desc=f"Interpolation Success",
                  exec_time=elapsed_ms(it0),
                  task_id=task_dict.get('task_id'),
                  data_event='PartialInterpolation')
    return False

def json_loading(json_data, d):
    """
    Load JSON data to staging schema with enhanced error handling.

    Args:
        json_data: JSON data to load
        d: Task dictionary

    Returns:
        None
    """
    task_id = d.get("task_id")
    if isinstance(json_data, dict):
        json_data = [json_data]

    if not json_data:
        print("No data to load.")
        return

    # --- Step 2: Establish DB connection ---
    conn, cur = con_cur()  # assumes you have your con_cur() returning (conn, cur)

    # --- Step 3: Prepare data for insertion ---
    values = [(task_id, json.dumps(record)) for record in json_data]

    # --- Step 4: Choose optimized insert strategy ---
    if len(values) == 1:
        # Single insert - minimal overhead
        cur.execute(
            """
            INSERT INTO staging.api_imports (task_id, payload)
            VALUES (%s, %s);
            """,
            values[0],
        )
    else:
        # Bulk insert - efficient for many records
        execute_values(
            cur,
            """
            INSERT INTO staging.api_imports (task_id, payload)
            VALUES %s;
            """,
            values,
            page_size=1000,  # can tune based on memory/network
        )

    conn.commit()
    cur.close()
    conn.close()
    return