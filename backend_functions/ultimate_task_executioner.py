import importlib
import json
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import pytz
from psycopg2.extras import execute_values

from backend_functions.database_functions import sql_to_dict, qec, con_cur, get_table_row_count, one_sql_result, \
    get_conn, sql_to_list
from backend_functions.helper_functions import get_sync_dates, get_last_date
from backend_functions.logging_functions import start_timer, log_app_event, elapsed_ms
from backend_functions.service_logins import sql_rate_limited



def ultimate_task_executioner(force_task_name=None, force_task_id=None):
    t0 = start_timer()
    client_dict = None

    #Obtain the list of tasks
    sql = "SELECT * FROM tasks.vw_task_info"
    if force_task_id:
        sql = f"{sql} WHERE task_id = {force_task_id} "
    elif force_task_name:
        sql = f"{sql} WHERE task_name LIKE '%{force_task_name}%' "
    else:
        sql = f"{sql} WHERE should_execute "

    sql = f"{sql} ORDER BY api_service_name, next_planned_execution_utc"

    task_list = sql_to_dict(sql)
    print(f"{len(task_list)} tasks found : {task_list}")
    log_app_event(cat='Task Executioner',
                  desc=f'Planned execution of {len(task_list)} tasks')

    # Default the api service name to none -- will trigger a fresh login
    api_service_name = None
    if not task_list:
        return

    for task_dict in task_list:
        task_t0 = start_timer()
        task_id = task_dict.get('task_id')
        task_name = task_dict.get('task_name')
        print(f"Starting task #{task_id} : {task_name}")
        log_app_event(cat=f"Task #{task_id}: {task_name}",
                      desc='Task Started',
                      task_id=task_id,
                      data_event='Begin')
        run_elt= task_dict.get('run_extract')
        run_interpolation = task_dict.get('run_interpolation')
        run_forecasting = task_dict.get('run_forecasting')
        run_python = task_dict.get('run_python')
        run_parsing = task_dict.get('run_parsing')

        task_fail = False
        fail_msg=None
        if run_elt:
            if api_service_name != task_dict.get('api_service_name'):
                client_dict = None
                api_service_name = task_dict.get('api_service_name')

            module_function = task_dict.get('python_login_function')
            print(f"Refreshing client for {api_service_name}")
            try:
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
                log_app_event(cat=f"Task #{task_id}: {task_name}",
                              desc='Failed to establish client',
                              task_id=task_id,
                              err=f"Client error: {e}",
                              data_event='Login')
                reconcile_task_dates(task_dict, task_fail=True, e=e)
                client_dict = None
                print(f"Failed client initialization for task #{task_id}: {task_name}")
                continue

            task_fail, client_dict = extract_load_flatten(client_dict, task_dict)

        if run_parsing and not task_fail:
            print(f"Starting Parsing for task #{task_id} : {task_name}")
            task_fail = execute_sproc(d=task_dict, sproc_type='parsing')

        if run_interpolation and not task_fail:
            print(f"Starting Interpolation for task #{task_id} : {task_name}")
            task_fail = metric_interpolation(task_dict)

        if run_forecasting and not task_fail:
            print(f"Starting Forecasting for task #{task_id} : {task_name}")
            task_fail = execute_sproc(d=task_dict, sproc_type='forecasting')

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

    log_app_event(cat='Task Executioner',
                  desc=f'Execution complete {len(task_list)} tasks',
                  exec_time=elapsed_ms(t0))
    return


def extract_load_flatten(cd, td):
    # Ensure client is established
    if not cd:
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err='No Client dictionary',
                      task_id=td.get('task_id'),
                      data_event='Login')
        reconcile_task_dates(td, task_fail=True, e='No Client Dictionary')
        return True, None

    if not cd.get('client'):
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err='No client within dictionary',
                      task_id=td.get('task_id'),
                      data_event='Login')
        reconcile_task_dates(td, task_fail=True, e='No Client within client dictionary')
        return True, None

    extract_function_name = td.get('python_extraction_function')
    module_name = 'backend_functions.json_extractors'


    try:
        module = importlib.import_module(module_name)
        local_function = getattr(module, extract_function_name)
    except Exception as e:
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed to get extraction function",
                      exec_time=0,
                      err=f"Module Error: {e}",
                      task_id=td.get('task_id'),
                      data_event='Module')
        reconcile_task_dates(td, task_fail=True, e=f"Failed To get extraction Function {e}")
        return True, cd

    # Extract JSON
    print(f"Extracting data for Task #{td.get('task_id')}: {td.get('task_name')}: Function: {local_function}")
    t0=start_timer()
    try:
        json_data = local_function(client=cd.get('client'), td=td)
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Valid Extraction",
                      exec_time=elapsed_ms(t0),
                      task_id=td.get('task_id'),
                      data_event='Extract')
        print(f"Data Extraction Success for Task #{td.get('task_id')}: {td.get('task_name')}")
    except Exception as e:
        print(f"Data Extraction Failed for Task #{td.get('task_id')}: {td.get('task_name')} : {e}")
        json_data = None
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Extraction",
                      err=f"Extraction error: {e}",
                      task_id=td.get('task_id'),
                      data_event='Extract')
        reconcile_task_dates(td, task_fail=True, e=f"Failed Extraction {e}")
        return True, cd

    if not json_data:
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Loading ignored",
                      err='No API response to load',
                      task_id=td.get('task_id'),
                      data_event='No data from API'
                      )
        # reconcile_task_dates(td, task_fail=True, e='No API response to load')
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
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Failed Load",
                      exec_time=elapsed_ms(t0),
                      err=f"Load Failure: {e}",
                      task_id=td.get('task_id'),
                      data_event='Load')
        reconcile_task_dates(td, task_fail=True, e=f"Failed TO Load {e}")
        return True, cd

    t0 = start_timer()
    flatten_failure = execute_sproc(d=td, sproc_type='flatten')

    return flatten_failure, cd


def execute_sproc(d, sproc_type):
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
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"SPROC Failure: {sproc_type}",
                      err=f'Failed to extract key: {retrieval_key}',
                      task_id=d.get('task_id'),
                      data_event=sproc_type)
        reconcile_task_dates(d, task_fail=True, e=f'Failed to SPROC key: {retrieval_key}')
        return True
    t0 = start_timer()
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


def execute_python(d=None):
    print(f"Starting Python Execution for #{d.get('task_id')}: {d.get('task_name')}")
    module_function = d.get('python_execution_function')
    module_name, svc_function_name = module_function.rsplit('.', 1)
    try:
        module = importlib.import_module(module_name)
        local_function = getattr(module, svc_function_name)
    except Exception as e:
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"Python Function Failure",
                      err=f"Module Error: {e}",
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
        log_app_event(cat=f"Task #{d.get('task_id')}: {d.get('task_name')}",
                      desc=f"Python Function Failure",
                      exec_time=elapsed_ms(t0),
                      err=f"Python Failure: {e}",
                      task_id=d.get('task_id'),
                      data_event='Python')
        reconcile_task_dates(d, task_fail=True, e=f"Python Function Failure {e}")
        return True


def reconcile_task_dates(task_dict, task_fail=False, e=None):

    task_id = int(task_dict.get('task_id'))

    if task_fail:
        # e= e.replace("'", "")
        cons_failures = int(task_dict.get('consecutive_failures')) + 1
        up_sql = f"""UPDATE tasks.task_configuration SET
                last_executed_utc = CURRENT_TIMESTAMP,
                last_failed_utc = CURRENT_TIMESTAMP,
                next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '{cons_failures*60} Minutes',
                last_failure_message = %s,
                consecutive_failures = consecutive_failures + 1
                WHERE task_id = %s;
                """
        params = [e, task_id]
    else:
        freq = task_dict.get('task_frequency')
        friendly_name = task_dict.get('friendly_name')
        is_extract = task_dict.get('python_extraction_function') is not None
        if is_extract:
            value_current_sql = f"""SELECT CURRENT_TIMESTAMP::DATE = (SELECT MAX(value_recency) FROM api_services.function_library
                                WHERE friendly_name = '{friendly_name}')::DATE"""
            value_current = one_sql_result(value_current_sql)
        else:
            value_current = True
        if not value_current:
            # Run again in an hour because values aren't current.
            int_sql = f"next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '2 hours'"
        elif freq == 'Hourly':
            # Run again in {interval} hours
            int_sql = f"next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '{task_dict.get('task_interval')} hours'"
        elif freq == 'Daily':
            # Run again tomorrow at the interval
            int_sql = f"""next_planned_execution_utc = 
                    (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '1 day') 
                    + INTERVAL '{task_dict.get('task_start_hour')} hours')
                    AT TIME ZONE 'America/Los_Angeles'"""
        elif freq == 'Weekly':
            # Run again in 7 days at the interval
            int_sql = f"""next_planned_execution_utc = 
                                (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '7 days') 
                                + INTERVAL '{task_dict.get('task_start_hour')} hours')
                                AT TIME ZONE 'America/Los_Angeles'"""
        elif freq == 'Monthly':
            # Run again in 30 days at the interval.
            int_sql = f"""next_planned_execution_utc = 
                                (date_trunc('day', NOW() AT TIME ZONE 'America/Los_Angeles' + INTERVAL '30 days') 
                                + INTERVAL '{task_dict.get('task_start_hour')} hours')
                                AT TIME ZONE 'America/Los_Angeles'"""
        else:
            int_sql = """next_planned_execution_utc = CURRENT_TIMESTAMP + INTERVAL '24 hours'"""

        up_sql = f"""UPDATE tasks.task_configuration SET
                        last_executed_utc = CURRENT_TIMESTAMP,
                        last_succeeded_utc = CURRENT_TIMESTAMP,
                        {int_sql},
                        consecutive_failures = 0
                        WHERE task_id = %s;
                        """
        params = [task_id,]

    qec(up_sql, params)
    return


def metric_interpolation(task_dict):
    # only continue when interpolating
    if not task_dict.get('interpolation_sproc'):
        return
    it0 = start_timer()

    src_table_schema = task_dict.get('interpolation_sproc')
    sch, tab, infer = src_table_schema.split('.')

    sql = f"""WITH ts_col as 
            (SELECT column_name from information_schema.columns
            WHERE table_schema = '{sch}' and table_name = '{tab}'
            AND data_type = 'timestamp with time zone'
            ORDER BY ordinal_position LIMIT 1)
            
            
            select 
            table_schema as src_schema,
            table_name as src_table,
            column_name as src_col,
            (SELECT * FROM ts_col) as src_ts_col
            FROM information_schema.columns
            WHERE table_schema = '{sch}' and table_name = '{tab}'
            and data_type in ('numeric',
                            'bigint',
                            'smallint',
                            'double precision',
                            'integer',
                            'int')"""

    numeric_cols = sql_to_dict(sql)
    success=True
    for col in numeric_cols:
        sproc_sql = f"CALL metrics.interpolate_metric({col.get('src_schema')}, {col.get('src_table')}, {col.get('src_col')}, {col.get('src_ts_col')}, {infer})"
        t0 = start_timer()
        returns = qec(sproc_sql)
        if returns:
            log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                          desc=f"Interpolation Failure: {col.get('src_col')}",
                          err=returns,
                          task_id=task_dict.get('task_id'),
                          data_event='Interpolation'
                          )
            reconcile_task_dates(task_dict, task_fail=True, e=f'Failed to execute sql: {returns}')
            success=False
            break
        else:
            log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                          desc=f"Interpolation Success: {col.get('src_col')}",
                          exec_time=elapsed_ms(t0),
                          task_id=task_dict.get('task_id'),
                          data_event='PartialInterpolation')
            continue
    log_app_event(cat=f"Task #{task_dict.get('task_id')}: {task_dict.get('task_name')}",
                  desc=f"Interpolation Success",
                  exec_time=elapsed_ms(it0),
                  task_id=task_dict.get('task_id'),
                  data_event='PartialInterpolation')
    return success






def json_loading(json_data, d):
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