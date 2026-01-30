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
            task_fail = execute_sproc(d=task_dict, sproc_type='parsing')

        if run_interpolation and not task_fail:
            task_fail = execute_sproc(d=task_dict, sproc_type='interpolation')

        if run_forecasting and not task_fail:
            task_fail = execute_sproc(d=task_dict, sproc_type='forecasting')

        if run_python and not task_fail:
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
    if not task_dict.get('interpolate_values'):
        return

    task_id = int(task_dict.get('task_id'))
    # Get the staging dictionary only for tasks that have interpolation requirements
    sel_sql = f"SELECT sc.* from tasks.staging_configuration sc"
    sel_sql = f"{sel_sql} INNER JOIN tasks.fact_configuration fc on fc.staging_id = sc.staging_id and fc.task_id = sc.task_id"
    sel_sql = f"{sel_sql} where sc.task_id = {task_id} AND fc.interpolate_values"

    stg_list = sql_to_dict(sel_sql)
    print(f'{len(stg_list)} interpolation stages for task #{task_id}')
    # Loop through staging events
    for s in stg_list:
        stg_id = int(s.get('staging_id'))
        sel_sql = f"SELECT * from tasks.fact_configuration WHERE task_id = {task_id} and staging_id = {stg_id};"
        fact_list = sql_to_dict(sel_sql)

        # identify the timestamp column
        ts_col = None
        int_dict_list = []
        for f in fact_list:
            if f.get('interpolation_ts'):
                ts_col = f.get('fact_name')
            if f.get('interpolate_values'):
                int_dict_list.append(f)

        # stop if no ts col is identified
        if not ts_col:
            continue

        # stop if there are no interpolatable columns
        if not int_dict_list:
            continue

        # Pull the list of interpolation columns
        for int_d in int_dict_list:
            src = s.get('destination_table')
            dest = int_d.get('interpolation_destination_table')
            sch = dest.split(".")[0]
            tb = dest.split(".")[1]
            int_col = int_d.get('fact_name')
            data_type = int_d.get('data_type')
            infer_values = int_d.get('infer_values')
            precision = 0 if data_type == 'INTEGER' else int(data_type[data_type.find(',') + 1 : data_type.find(')')])
            is_max = False
            col_list = sql_to_list(f"""SELECT DISTINCT column_name from information_schema.columns
                                                    WHERE table_schema = '{sch}' and table_name = '{tb}'""")
            conn = get_conn()
            cur = conn.cursor()
            if int_col not in col_list:
                a_sql = f"ALTER TABLE {dest} ADD COLUMN {int_col} {data_type};"
                cur.execute(a_sql)
                conn.commit()

            cur.execute(f"TRUNCATE staging.interpolation_anchors")
            conn.commit()

            cur.execute(f"ALTER TABLE staging.interpolation_anchors ALTER COLUMN load_value TYPE {data_type}")
            conn.commit()

            cur.execute(f"ALTER TABLE staging.interpolation_anchors ALTER COLUMN prev_value TYPE {data_type}")
            conn.commit()

            cur.execute(f"ALTER TABLE staging.interpolation_anchors ALTER COLUMN next_value TYPE {data_type}")
            conn.commit()

            cur.execute(f"TRUNCATE staging.interpolation_load_values")
            conn.commit()

            cur.execute(f"ALTER TABLE staging.interpolation_load_values ALTER COLUMN load_value TYPE {data_type}")
            conn.commit()
            cur.close()
            conn.close()
            ts_to_old = None
            default_interval = 48

            while not is_max:
                # --1 Get the time Boundaries
                conn = get_conn()
                cur = conn.cursor()
                t0= start_timer()
                from_sql = f"""SELECT MAX(COALESCE({ts_col}, '2026-01-01'::TIMESTAMPTZ) - INTERVAL '6 hours')
                 FROM {dest} WHERE {int_col} IS NOT NULL;"""
                ts_from = one_sql_result(from_sql)
                if not ts_from:
                    ts_from = one_sql_result(f"SELECT MIN({ts_col}) FROM {src}")
                to_sql = f"""SELECT MAX(
                               CASE WHEN {ts_col} < '{ts_from}'::TIMESTAMPTZ + INTERVAL '{default_interval} hours'
                                THEN {ts_col}
                                ELSE NULL END) as to_ts_window,
                                 MAX({ts_col}) as to_ts_max FROM {src};"""
                to_dict = sql_to_dict(to_sql)
                ts_to = to_dict[0].get('to_ts_window')
                is_max = ts_to == to_dict[0].get('to_ts_max')
                while ts_to == ts_to_old and not is_max:
                    default_interval += 48

                    to_sql = f"""SELECT MAX(
                               CASE WHEN {ts_col} < '{ts_from}'::TIMESTAMPTZ + INTERVAL '{default_interval} hours'
                                THEN {ts_col}
                                ELSE NULL END) as to_ts_window,
                                 MAX({ts_col}) as to_ts_max FROM {src};"""
                    to_dict = sql_to_dict(to_sql)
                    ts_to = to_dict[0].get('to_ts_window')
                ts_to_old = ts_to
                default_interval = 48
                is_max = ts_to == to_dict[0].get('to_ts_max')
                # Re-load the interpolation table with second-by-second information
                cur.execute("TRUNCATE staging.interpolation")
                conn.commit()
                cur.execute(f"""INSERT INTO staging.interpolation (ts_utc) 
                    SELECT generate_series('{ts_from}'::TIMESTAMPTZ, '{ts_to}'::TIMESTAMPTZ, '1 second'::interval);""")
                conn.commit()
                # Calculate Inferred values, if necessary

                cur.execute(f"TRUNCATE staging.interpolation_load_values")
                conn.commit()


                inf_query = f"""INSERT INTO staging.interpolation_load_values 
                                (ts_utc, load_value) 
                                SELECT 
                                b.ts_utc,"""
                if infer_values:
                    inf_query = f"""{inf_query} 
                                    CASE WHEN b.infer_value THEN COALESCE({int_col}, d30_val, d90_val, d_val)
                                    ELSE {int_col}
                                    END as load_value"""
                    footer_sql = f"""LEFT JOIN (
                                    SELECT 
                                    extract(HOUR FROM {ts_col}) * 60 + EXTRACT(MINUTE from {ts_col}) as dm,
                                    ROUND(avg(CASE 
                                    WHEN {ts_col} > CURRENT_TIMESTAMP - INTERVAL '30 days'
                                    THEN {int_col}
                                    ELSE NULL END),{precision})::{data_type} as d30_val,
                                    ROUND(avg(CASE 
                                    WHEN {ts_col} > CURRENT_TIMESTAMP - INTERVAL '90 days'
                                    THEN {int_col}
                                    ELSE NULL END),{precision})::{data_type} as d90_val,
                                    ROUND(avg({int_col}), {precision})::{data_type} as d_val
                                    FROM {src} WHERE {int_col} IS NOT NULL
                                    GROUP BY
                                    extract(HOUR FROM {ts_col}) * 60 + EXTRACT(MINUTE from {ts_col})) t on t.dm = b.minute_of_day"""
                else:
                    inf_query = f"""{inf_query} 
                                    {int_col} as load_value"""
                    footer_sql = ''

                inf_query = f"""{inf_query} 
                            FROM staging.vw_interpolation b
                                LEFT JOIN {src} src on src.{ts_col} = b.ts_utc
                                {footer_sql};"""
                ms = start_timer()
                cur.execute(inf_query)
                conn.commit()
                ms = start_timer()
                cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_load_ts ON staging.interpolation_load_values (ts_utc);")
                conn.commit()
                # Load the anchors table
                ms = start_timer()
                cur.execute(f"TRUNCATE staging.interpolation_anchors")
                conn.commit()


                ins_sql = f"""
                            WITH forward_pass AS (
                                SELECT 
                                    ts_utc,
                                    load_value,
                                    -- Standard Forward Scan for PREVIOUS values
                                    MAX(load_value) OVER (ORDER BY ts_utc) as prev_value,
                                    MAX(ts_utc) FILTER (WHERE load_value IS NOT NULL) OVER (ORDER BY ts_utc) as prev_ts
                                FROM staging.interpolation_load_values
                            ),
                            backward_pass AS (
                                SELECT 
                                    ts_utc,
                                    -- Reverse Scan for NEXT values (Using DESC sort turns "Future" into "Past")
                                    MIN(load_value) OVER (ORDER BY ts_utc DESC) as next_value,
                                    MIN(ts_utc) FILTER (WHERE load_value IS NOT NULL) OVER (ORDER BY ts_utc DESC) as next_ts
                                FROM staging.interpolation_load_values
                            )
                            INSERT INTO staging.interpolation_anchors
                            (ts_utc, load_value, prev_value, prev_ts, next_value, next_ts)
                            SELECT
                                f.ts_utc,
                                f.load_value,
                                f.prev_value,
                                f.prev_ts,
                                b.next_value,
                                b.next_ts
                            FROM forward_pass f
                            JOIN backward_pass b ON f.ts_utc = b.ts_utc;
                        """
                cur.execute(ins_sql)
                conn.commit()
                # Ensure the column exists on the destination table


                # Interpolate and load into destination table
                load_sql = f"""INSERT INTO {dest} (ts_utc, {int_col})
                               SELECT 
                               date_trunc('hour', ts_utc) + (extract(minute from ts_utc)::int / 30) * interval '30 minutes' as ts_utc,
                               ROUND(avg(CASE
                                    WHEN load_value IS NOT NULL THEN load_value
                                    WHEN prev_value IS NULL OR next_value IS NULL THEN NULL
                                    ELSE
                                        prev_value +
                                        (
                                            (EXTRACT(EPOCH FROM (ts_utc - prev_ts)) /
                                             EXTRACT(EPOCH FROM (next_ts - prev_ts)))
                                            * (next_value - prev_value)
                                        )
                                END), {precision})::{data_type} AS {int_col}
                                FROM staging.interpolation_anchors
                                GROUP BY date_trunc('hour', ts_utc) + (extract(minute from ts_utc)::int / 30) * interval '30 minutes'
                                ON CONFLICT ({ts_col}) DO UPDATE
                                SET {int_col} = EXCLUDED.{int_col}
                                WHERE {dest}.{int_col} IS DISTINCT FROM EXCLUDED.{int_col}"""
                # print(f"Load Sql: {load_sql}")


                cur.execute(load_sql)
                conn.commit()
                cur.close()
                conn.close()

            cur.close()
            conn.close()
    return


def json_flattening(task_dict):
    # Get the fact dictionary
    task_id = int(task_dict.get('task_id'))
    staging_sql = f"SELECT * FROM tasks.staging_configuration where task_id = {task_id}"
    staging_dict = sql_to_dict(staging_sql)

    for s in staging_dict:

        staging_id = int(s.get('staging_id'))
        fact_sql = f"SELECT *, "
        fact_sql = f"{fact_sql} COUNT(CASE WHEN is_unique_constraint then fact_id else NULL END)"
        fact_sql = f"{fact_sql} OVER (PARTITION BY task_id, staging_id) as pk_count"
        fact_sql = f"{fact_sql} FROM tasks.fact_configuration where task_id = {task_id} and staging_id = {staging_id}"
        fact_sql = f"{fact_sql} ORDER BY is_unique_constraint DESC, fact_id;"
        fact_dict_list = sql_to_dict(fact_sql)

        destination_table = s.get('destination_table')
        ins_sql = f"""INSERT INTO {destination_table} ("""
        select_sql = f") SELECT "
        create_sql = f"CREATE TABLE IF NOT EXISTS {destination_table} ("
        pk_list = []
        pk_count = int(fact_dict_list[0].get('pk_count'))
        print(f"pk_count: {pk_count}")
        do_sql = "DO UPDATE SET"
        conflict_where = "WHERE ("
        ts_col = None
        for fact_dict in fact_dict_list:
            col_name = fact_dict.get('fact_name')
            is_unique = fact_dict.get('is_unique_constraint')
            ins_sql = f"{ins_sql} {col_name},"
            if fact_dict.get('interpolation_ts'):
                ts_col = col_name
            extraction_sql = fact_dict.get('extraction_sql')
            data_type = fact_dict.get('data_type')
            create_sql = f"{create_sql} {col_name} {data_type}"
            if pk_count == 1 and is_unique:
                create_sql = f"{create_sql} PRIMARY KEY,"
            else:
                create_sql = f"{create_sql},"
            if pk_count > 0 and is_unique:
                pk_list.append(col_name)
            else:
                do_sql = f"{do_sql} {col_name}=EXCLUDED.{col_name},"
                conflict_where = f"{conflict_where} {destination_table}.{col_name} IS DISTINCT FROM EXCLUDED.{col_name} OR"
            select_sql = f"{select_sql} {extraction_sql}::{data_type} as {col_name},"

        # Remove trailing commas
        ins_sql = ins_sql[:-1]

        select_sql = select_sql[:-1]
        do_sql = do_sql[:-1]
        conflict_where = conflict_where[:-3]

        #Build final sql
        if pk_count > 1:
            create_sql = f"{create_sql} CONSTRAINT"
            key_sql = f"PRIMARY KEY ("
            pk_sql = f"{destination_table}_"
            for pk in pk_list:
                key_sql = f"{key_sql} {pk},"
                pk_sql = f"{pk_sql}_{pk}"
            key_sql = key_sql[:-1]
            create_sql = f"{create_sql} {pk_sql} {key_sql})"
        else:
            create_sql = create_sql[:-1]
        create_sql = f"{create_sql});"
        conflict_where = f"{conflict_where});"
        cj_sql = s.get('cross_join_condition')
        filter_sql = s.get('filter_condition')
        source_table = s.get('source_table')
        from_sql = f"FROM {source_table} src {cj_sql}" if cj_sql else f"FROM {source_table} src"
        final_sql = f"""{ins_sql} {select_sql} {from_sql}"""
        if source_table == 'staging.api_imports':
            final_sql = f"{final_sql} WHERE src.task_id = {task_id}"""
            operator = 'AND'
        else:
            operator = 'WHERE'
        if filter_sql:
            final_sql = f"{final_sql} {operator} {filter_sql} "
        if pk_count > 0:
            final_sql = f"{final_sql} ON CONFLICT("
            for pk in pk_list:
                final_sql = f"{final_sql} {pk},"
            final_sql = final_sql[:-1]
            final_sql = f"{final_sql}) {do_sql} {conflict_where}"
        # Update value recency

        qec(create_sql)
        qec(final_sql)
        if ts_col:
            ts_sql = f"""UPDATE tasks.fact_configuration set value_recency = 
                        (SELECT MAX({ts_col}) FROM {destination_table}) WHERE task_id = {task_id};"""
            qec(ts_sql)


def extract_json(d, client_dict):
    # Establish the client
    raw_api_function = d.get("api_function_name")
    module_name = 'backend_functions.service_logins'
    svc_function_name = f'get_{d.get('api_service_name').lower()}_client'
    module = importlib.import_module(module_name)
    svc_function = getattr(module, svc_function_name)
    client_dict = svc_function(client_dict)
    client = client_dict.get("client")
    # skip task if there's an invalid client
    if not client:
        task_log(d.get("task_name"),
                 fail_type='Connection', fail_text='No Valid client')
        return None, None

    loop_type = d.get("api_loop_type")

    if loop_type is None or loop_type=='N/A':
        json_data = json_no_loop(d, client)
        return json_data, client_dict

    if loop_type == 'Next':
        return json_next_loop(client, raw_api_function)

    if loop_type in ['Day', 'Range']:
        date_list = get_sync_dates(value_recency(d.get('task_id')), loop_type)
        # print(f"Parameters = {d.get('api_parameters')}")
        json_data = json_date_loop(client,
                                   raw_api_function,
                                   loop_type,
                                   date_list,
                                   d.get("api_parameters"))
        return json_data, client_dict

    print('No valid APi loop found, no json returned')

    return None, client



def value_recency(task_id):
    return one_sql_result(f"SELECT MIN(value_recency) FROM tasks.fact_configuration WHERE task_id = {task_id}")


def verify_execution(task_id, task_dict, force_task_name):
    if force_task_name:
        return True

    dt = datetime.now(timezone.utc)

    msg = f"{task_dict.get('task_name')}:"
    if task_dict.get('consecutive_failures') >= 5:
        msg = f"{msg} 5+ Consecutive failures, skipping"
        print(msg)
        return False


    freq = task_dict.get('task_frequency')
    if freq == 'Inactive':
        msg = f"{msg} Inactive, skipping"
        print(msg)
        return False

    if task_dict.get('api_service_name') == 'Spotify' and sql_rate_limited():
        msg = f"{msg} skipped due to rate limitation"
        print(msg)
        return False

    recency_utc = value_recency(task_id)

    if dt > recency_utc:
        msg = f"{msg} Requires Catchup, forcing execution"
        print(msg)
        return True

    if freq == 'Hourly':
        if not valid_hour(hour = dt.hour, d=task_dict):
            msg = f"{msg} Invalid hour, skipping"
            print(msg)
            return False

        if task_dict.get('next_planned_execution') > dt:
            msg = f"{msg} Not yet scheduled, skipping"
            print(msg)
            return False

        return True

    if dt.hour < task_dict.get('task_start_hour'):
        msg = f"{msg} Not time to start, skipping"
        print(msg)
        return False

    if task_dict.get('next_planned_execution') > dt:
        msg = f"{msg} Not yet scheduled, skipping"
        print(msg)
        return False

    msg = f"{msg} Earliest execution possibility"
    print(msg)
    return True


def valid_hour(hour, d):
    return d.get('task_start_hour') <= hour <= d.get('task_end_hour')


def task_log(task_id=None, e_time=None, l_time=None, t_time=None, i_time=None, f_time=None, fail_type=None, fail_text=None):
    insert_sql = """INSERT INTO logging.task_executions (
                              task_id,
                              extract_time_ms,
                              load_time_ms,
                              transform_time_ms,
                              interpolation_time_ms,
                              forecast_time_ms,
                              failure_type,
                              error_text) VALUES (
                                   %s, %s, %s, %s, %s, %s, %s, %s)"""
    params = (task_id, e_time, l_time, t_time, i_time, f_time, fail_type, fail_text)
    qec(insert_sql, params)
    return

def json_no_loop(d, client):
    api_params = d.get("api_parameters")
    curr_ts = int(datetime.now(pytz.UTC).timestamp() * 1000)
    args = to_params(param_list=api_params,
                     search_val='*CURR_TS*',
                     replace_val=curr_ts,
                     return_type='dict')
    print(f"DEBUG: args: {args}")
    if args:
        json_data = getattr(client, d.get("api_function"))(**args)
    else:
        json_data = getattr(client, d.get("api_function"))()
    return json_data


def json_next_loop(client, function, api_parameters=None):
    print("DEBUG: INSIDE NEXT LOOP")
    all_json = []
    # Fetch & loop through API results
    offset = 0
    limit = 50
    if api_parameters:
        param_list = [param.strip() for param in api_parameters.split(',')]

        # Replace placeholders with actual date values
        args = []
        for param in param_list:
            args.append(param)

    while True:
        # Ensure we're being good API Citizens
        if offset != 0:
            time.sleep(1)

        if api_parameters:
            raw_json = getattr(client, function)(*args)
        else:
            raw_json = getattr(client, function)()

        if  raw_json is None:
            raw_json = {}

        print('Next loop DEBUG raw JSON')
        print(raw_json)
        # Append the results
        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            break

        # Break the loop as necessary
        try:
            if raw_json['next'] is None:
                break
        except:
            break


    return all_json


def json_date_loop(client, function, loop_type, date_list, api_parameters=None):

    all_json = []
    for date_val in date_list:
        # pause for 2 seconds during each loop
        if date_val != date_list[0]:
            time.sleep(2)

        # If I can pull a range of values, the result will be a tuple.
        if loop_type == 'Range':
            if not isinstance(date_val, (list, tuple)) or len(date_val) != 2:
                d1, d2 = default_range()
            d1, d2 = date_val
            if d1 is None or d2 is None:
                d1, d2 = default_range()
        else:
            d1 = date_val
            d2 = None

        # Build the arguments list
        param_list = [param.strip() for param in api_parameters.split(',')]
        if api_parameters:
            # Replace placeholders with actual date values
            args = []
            for param in param_list:
                if param == '*D1*':
                    args.append(d1)
                elif param == '*D2*':
                    args.append(d2)
                else:
                    # Keep other parameters as-is
                    args.append(param)
            # print(args, client, function)
            raw_json = getattr(client, function)(*args)
        else:
            # Fallback to original behavior if no api_parameters specified
            if loop_type == 'date_range':
                raw_json = getattr(client, function)(d1, d2)
            else:
                raw_json = getattr(client, function)(date_val)

        # Append the results
        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            break

    return all_json



def to_params(param_list=None, search_val=None, replace_val=None, return_type='list'):
    if isinstance(param_list, list):
        temp_list = param_list
    else:
        temp_list = [param.strip() for param in param_list.split(',')]

    rb_list = []
    for p in temp_list:
        if search_val in p:
            rb_list.append(p.replace(search_val, str(replace_val)))
        else:
            rb_list.append(p)

    if return_type == 'list':
        return rb_list
    elif return_type == 'dict':
        return dict(p.split("=", 1) for p in rb_list)
    else:
        return ", ".join(rb_list)

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