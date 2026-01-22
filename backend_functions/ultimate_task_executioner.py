import importlib
import json
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import pytz
from psycopg2.extras import execute_values

from backend_functions.database_functions import sql_to_dict, qec, con_cur, get_table_row_count, one_sql_result
from backend_functions.helper_functions import get_sync_dates, get_last_date
from backend_functions.logging_functions import start_timer, log_app_event, elapsed_ms
from backend_functions.service_logins import sql_rate_limited



def ultimate_task_executioner(force_task_name=None):
    t0 = start_timer()
    client_dict = None

    #Obtain the list of tasks
    sql = "SELECT * FROM tasks.task_configuration "
    if force_task_name:
        sql = f"{sql} WHERE task_name = '{force_task_name}' "

    sql = f"{sql} ORDER BY api_service_name, task_priority"

    task_list = sql_to_dict(sql)
    print(f"{len(task_list)} tasks found")

    if not task_list:
        return

    for task_dict in task_list:
        task_t0 = start_timer()
        task_id = task_dict.get('task_id')
        task_name = task_dict.get('task_name')
        continue_execution = verify_execution(task_id, task_dict, force_task_name)

        if not continue_execution:
            continue

        # Determine what type of execution
        api_service_name = task_dict.get('api_service_name')
        run_elt = api_service_name and api_service_name != 'N/A'
        python_function = task_dict.get('python_function')
        run_python = python_function and python_function != 'N/A'
        task_fail = False
        if run_elt:
            sub_t0 = start_timer()
            try:
                json_blob, client_dict = extract_json(task_dict.get('json'), client_dict)
                extract_time_ms = elapsed_ms(sub_t0)
            except Exception as e:
                print(f"{task_name}: Extract failure: {e}")
                task_log(task_name, e_time=elapsed_ms(sub_t0), fail_type='Extract', fail_text=e)
                task_fail=True
                continue
            sub_t0 = start_timer()
            try:
                json_loading(json_blob, task_id)
                load_time_ms = elapsed_ms(sub_t0)
            except Exception as e:
                print(f"{task_name}: Load failure: {e}")
                task_log(task_name, e_time=extract_time_ms, l_time=elapsed_ms(sub_t0), fail_type='Load', fail_text=e)
                task_fail = True
                continue
            sub_t0 = start_timer()
            try:
                json_flattening(task_dict)
                transform_time_ms = elapsed_ms(sub_t0)
            except Exception as e:
                print(f"{task_name}: Load failure: {e}")
                task_log(task_name,
                         e_time=extract_time_ms,
                         l_time=transform_time_ms,
                         t_Time=elapsed_ms(sub_t0),
                         fail_type='Load', fail_text=e)
                task_fail = True
                continue
            # postgres_flattening()



        print(task_dict)
    return

def json_flattening(task_dict):
    # Get the fact dictionary
    task_id = int(task_dict.get('task_id'))
    staging_sql = f"SELECT * FROM tasks.staging_configuration where task_id = {task_id}"
    staging_dict = sql_to_dict(staging_sql)

    for s in staging_dict:

        staging_id = int(s.get('staging_id'))
        fact_sql = f"SELECT * FROM tasks.fact_configuration where task_id = {task_id} and staging_id = {staging_id}"
        fact_dict_list = sql_to_dict(fact_sql)

        destination_table = s.get('destination_table')
        timestamp_extraction_sql = s.get('timestamp_extraction_sql')
        ins_sql = f"""INSERT INTO {destination_table} (ts_utc"""
        select_sql = f") SELECT {timestamp_extraction_sql} as ts_utc "
        create_sql = f"CREATE TABLE IF NOT EXISTS {destination_table} (ts_utc TIMESTAMPTZ PRIMARY KEY"

        do_sql = "DO UPDATE SET"
        conflict_where = "WHERE ("
        for fact_dict in fact_dict_list:
            col_name = fact_dict.get('fact_name')
            ins_sql = f"{ins_sql}, {col_name} "
            extraction_sql = fact_dict.get('extraction_sql')
            data_type = fact_dict.get('data_type')
            create_sql = f"{create_sql}, {col_name} {data_type} "
            select_sql = f"{select_sql}, {extraction_sql}::{data_type} as {col_name} "
            do_sql = f"{do_sql} {col_name}=EXCLUDED.{col_name},"
            conflict_where = f"{conflict_where} {destination_table}.{col_name} IS DISTINCT FROM EXCLUDED.{col_name},"
        create_sql = f"{create_sql});"
        do_sql = do_sql[:-1]
        conflict_where = conflict_where[:-1]
        conflict_where = f"{conflict_where});"
        cj_sql = s.get('cross_join_condition')
        filter_sql = s.get('filter_condition')
        from_sql = f"FROM staging.api_imports{cj_sql}" if cj_sql else "FROM staging.api_imports"
        final_sql = f"""{ins_sql} {select_sql} {from_sql} WHERE task_id = {task_id}"""
        if filter_sql:
            final_sql = f"{final_sql} AND {filter_sql} "
        final_sql = f"{final_sql} ON CONFLICT(ts_utc) {do_sql} {conflict_where}"
        # qec(create_sql)
        # qec(final_sql)
        print(final_sql)
        print(create_sql)







def extract_json(d, client_dict):
    # Establish the client
    raw_api_function = d.get("api_service_function")
    module_name, svc_function_name = raw_api_function.rsplit('.', 1)
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
        return json_next_loop(client, d.get("api_function"))

    if loop_type in ['Day', 'Range']:
        date_list = get_sync_dates(value_recency(d.get('task_id')), loop_type)
        json_data = json_date_loop(client,
                                   d.get("api_function"),
                                   loop_type,
                                   date_list,
                                   d.get("api_parameters"))
        return json_data, client_dict

    print('No valid APi loop found, no json returned')

    return None, client



def value_recency(task_id):
    return one_sql_result(f"SELECT MIN(max_ts_utc::DATE) FROM tasks.fact_configuration WHERE task_id = {task_id}")


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
            print(args)
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

def default_range():
    d2 = date.today()
    d1 = d2 - timedelta(days=1)
    return d1, d2

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

def json_loading(json_data, task_id):

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