import json
import time
import importlib
from datetime import date, datetime, timedelta

import pytz
from psycopg2.extras import execute_values

from backend_functions.database_functions import sql_to_dict, qec, con_cur, get_table_row_count
from backend_functions.helper_functions import get_sync_dates, get_last_date
from backend_functions.logging_functions import start_timer, log_app_event, elapsed_ms
from backend_functions.service_logins import sql_rate_limited


def task_executioner(force_task_name=None, force_task=False):
    task_sql = "SELECT * FROM tasks.vw_task_execution"
    task_dict = sql_to_dict(query_str=task_sql)
    all_task_start = start_timer()

    client_dict = None
    recency_ctr = 0
    failure_ctr = 0
    timing_ctr = 0
    execution_ctr = 0
    for task in task_dict:
        ####################################
        # DETERMINE IF A RUN SHOULD BE MADE
        ###################################
        task_name = task.get('task_name')
        if force_task and force_task_name is not None and force_task_name != task_name:
            msg = f"{task_name} skipped during force launch of {force_task_name}"
            log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
            continue

        # If it's a Spotify task and we're under rate limitations, skip:
        if task.get('api_service_name') == 'Spotify':
            if sql_rate_limited():
                msg = f"{task_name} skipped due to rate limitation"
                log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
                continue

        if force_task and task_name == force_task_name:
            execution_type = 'forced'
            print(f"Forced execution of {force_task_name}")
            msg = f"{task_name} forcibly launched"
            log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
        elif task.get("task_frequency") == 'Retired':
            execution_type = 'Retired'
        else:
            execution_type = task.get("execution_logic")


        # Skip any retired/inactive tasks
        if execution_type == 'Retired':
            print(f"Skipping {task_name} : retired")
            msg = f"{task_name} skipped as retired"
            log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
            continue

        # Skip the too-recently-executed tasks
        if execution_type == 'recency':
            recency_ctr += 1
            print(f"Skipping {task_name} : Recency")
            msg = f"{task_name} skipped due to recency"
            log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
            continue

        # Skip tasks with too many consecutive failures
        if execution_type == 'failures':
            log_app_event(cat='Task Failure', desc=task_name, err='Skipped due to consecutive Failures')
            failure_ctr += 1
            print(f"Skipping {task_name} : failures")
            continue

        # SKip tasks that fail for scheduling reasons
        if execution_type == 'timing' and not task.get("do_execute"):
            timing_ctr += 1
            print(f"Skipping {task_name} : scheduling")
            msg = f"{task_name} skipped due to scheduling"
            log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
            continue
        ############################################################
        # Execute the prescribed function (e.g. database cleanup)
        ############################################################
        msg = f"{task_name} execution beginning"
        log_app_event(cat="Task Executioner", desc=msg, exec_time=elapsed_ms(all_task_start))
        if task.get("api_function") is None or task.get("api_function") == 'N/A':
            pf_t0 = start_timer()
            independent_logging_functions = ['playlist_sync_seeds',
                                             'playlist_sync_one_time',
                                             'playlist_sync_auto']
            try:

                local_function_str = task.get("python_function")
                module_name, svc_function_name = local_function_str.rsplit('.', 1)
                module = importlib.import_module(module_name)
                local_function = getattr(module, svc_function_name)
                local_function()
                if svc_function_name not in independent_logging_functions:
                    task_log(task.get("task_name"),
                             e_time=None,
                             l_time=None,
                             t_time=elapsed_ms(pf_t0))
                execution_ctr += 1

                update_task_through_date(task_name)
                print(f"Logging Success fpr {task_name}")
            except Exception as e:
                print(f"Logging Failure for {task_name}: {e}")

                task_log(task.get("task_name"),
                         e_time=None,
                         l_time=None,
                         t_time=elapsed_ms(pf_t0),
                         fail_type='transform',
                         fail_text=str(e))
                failure_ctr += 1


        # Extract data from API
        else:
            extract_start = start_timer()
            raw_api_function = task.get("api_service_function")
            ############################################################
            # EXTRACT DATA FROM API
            ############################################################
            try:
                # Establish the client
                module_name, svc_function_name = raw_api_function .rsplit('.', 1)
                module = importlib.import_module(module_name)
                svc_function = getattr(module, svc_function_name)
                client_dict = svc_function(client_dict)
                client = client_dict.get("client")
                # skip task if there's an invalid client
                if not client:
                    task_log(task.get("task_name"),
                         fail_type='Connection', fail_text='No Valid client')
                    continue

                loop_type = task.get("api_loop_type")
                print(f"DEBUG: Loop type: {loop_type}")

                # If no loop type, just pull once
                if loop_type is None or loop_type == 'N/A':
                    api_params = task.get("api_parameters")
                    curr_ts = int(datetime.now(pytz.UTC).timestamp() * 1000)
                    args = to_params(param_list=api_params,
                                     search_val='*CURR_TS*',
                                     replace_val=curr_ts,
                                     return_type='dict')
                    print(f"DEBUG: args: {args}")
                    if args:
                        json_data = getattr(client, task.get("api_function"))(**args)
                    else:
                        json_data = getattr(client, task.get("api_function"))()

                # if the api result is paginated
                elif loop_type == 'Next':
                    json_data = json_next_loop(client, task.get("api_function"))

                # If I need to make repeated api calls with different dates
                else:
                    cal_col = task.get("last_calendar_field")
                    if cal_col:
                        pg_schema, pg_table, ts_col, fact_col = cal_col.split(',')
                        rc_before = get_table_row_count(pg_schema, pg_table, fact_col)
                    else:
                        rc_before = 0

                    date_list = get_sync_dates(task.get("updated_through_utc"), loop_type)
                    json_data = json_date_loop(client,
                                               task.get("api_function"),
                                               loop_type,
                                               date_list,
                                               task.get("api_parameters"))
                extract_time = elapsed_ms(extract_start)
            except Exception as e:
                extract_time = elapsed_ms(extract_start)
                task_log(task.get("task_name"), e_time=extract_time, fail_type='extract', fail_text=str(e))
                failure_ctr += 1
                continue
            print(f"Extract: {extract_time}")
            # load the data to postgres

            ############################################################
            # LOAD JSON TO POSTGRES
            ############################################################
            load_start = start_timer()
            try:
                json_loading(json_data, task.get("api_function"))
                load_time = elapsed_ms(load_start)
            except Exception as e:
                load_time = elapsed_ms(load_start)
                task_log(task.get("task_name"),
                         e_time=extract_time,
                         l_time=load_time,
                         fail_type='load_time', fail_text=str(e))
                failure_ctr += 1
                continue
            print(f"Extract: {extract_time}, Load Time: {load_time}")

            ############################################################
            # TRANSFORM DATA IN POSTGRES
            ############################################################

            t_start = start_timer()
            sproc = task.get('api_post_processing')
            if sproc is None:
                t_time = elapsed_ms(t_start)
                task_log(task.get("task_name"),
                         e_time=extract_time,
                         l_time=load_time,
                         t_time=t_time)
                execution_ctr += 1
            else:
                try:
                    call_sql = f"CALL staging.{sproc}();"
                    qec(call_sql, auto_commit=True)
                    t_time = elapsed_ms(t_start)
                    task_log(task.get("task_name"),
                         e_time=extract_time,
                         l_time=load_time,
                         t_time=t_time)
                    execution_ctr += 1
                except Exception as e:
                    t_time = elapsed_ms(t_start)
                    task_log(task.get("task_name"),
                             e_time=extract_time,
                             l_time=load_time,
                             t_time=t_time,
                             fail_type='transform',
                             fail_text=str(e))
                    failure_ctr += 1
                    continue

            print(f"Extract: {extract_time}, Load Time: {load_time}, Transform: {t_time}")

            #############################################################
            # UPDATE CALENDAR DATE
            ############################################################
            if loop_type in ('Day', 'Range'):
                cal_col = task.get("last_calendar_field")
                if cal_col:
                    print(f"Sending Updated value to task.config")
                    print(f"Range Used: {date_list}")
                    pg_schema, pg_table, ts_col, fact_col = cal_col.split(',')

                    rc_after = get_table_row_count(pg_schema, pg_table, fact_col)
                    if rc_after > rc_before:
                        print(f"Row Delta {rc_after-rc_before} : {rc_before}-->{rc_after}")
                        update_sql = f"""UPDATE tasks.task_config
                                    SET updated_through_date = (SELECT MAX({ts_col}) FROM {pg_schema}.{pg_table}) 
                                    WHERE task_name = '{task_name}' AND {fact_col} IS NOT NULL;"""
                        qec(update_sql)
                        print(update_sql)
                    else:
                        print(f"Row Delta {rc_after - rc_before} : {rc_before}-->{rc_after}")
                        through_date = get_last_date(date_list)
                        update_sql = f"""UPDATE tasks.task_config 
                                    SET updated_through_date = %s::DATE 
                                    WHERE task_name = %s"""
                        params = (through_date, task_name)
                        qec(update_sql, params)
                    print("Update Complete")
            else:
                # non-range task, update task with today
                update_task_through_date(task_name)
    try:
        if execution_ctr + failure_ctr > 0:
            sql = """REFRESH MATERIALIZED VIEW tasks.vw_task_summary_chart_materialized"""
            qec(sql)
    except Exception as e:
        log_app_event(cat="Task Executioner",
                      desc=f"Staging View Did not refresh: {e}",
                      exec_time=elapsed_ms(all_task_start))

    all_task_time = elapsed_ms(all_task_start)
    msg = f"Attempts: E: {execution_ctr} F: {failure_ctr} || Skips: T: {timing_ctr} R: {recency_ctr}  "
    log_app_event(cat="Task Executioner", desc=msg, exec_time=all_task_time)
    return


def task_log(task_name=None, e_time=None, l_time=None, t_time=None, fail_type=None, fail_text=None):
    insert_sql = """INSERT INTO logging.task_executions (
                              task_name,
                              extract_time_ms,
                              load_time_ms,
                              transform_time_ms,
                              failure_type,
                              error_text) VALUES (
     %s, %s, %s, %s, %s, %s)"""
    params = (task_name, e_time, l_time, t_time, fail_type, fail_text)
    qec(insert_sql, params)
    return


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


def json_loading(json_data, function_name):

    if isinstance(json_data, dict):
        json_data = [json_data]

    if not json_data:
        print("No data to load.")
        return

    # --- Step 2: Establish DB connection ---
    conn, cur = con_cur()  # assumes you have your con_cur() returning (conn, cur)

    # --- Step 3: Prepare data for insertion ---
    values = [(function_name, json.dumps(record)) for record in json_data]

    # --- Step 4: Choose optimized insert strategy ---
    if len(values) == 1:
        # Single insert - minimal overhead
        cur.execute(
            """
            INSERT INTO staging.api_imports (api_function_name, payload)
            VALUES (%s, %s);
            """,
            values[0],
        )
    else:
        # Bulk insert - efficient for many records
        execute_values(
            cur,
            """
            INSERT INTO staging.api_imports (api_function_name, payload)
            VALUES %s;
            """,
            values,
            page_size=1000,  # can tune based on memory/network
        )

    conn.commit()
    cur.close()
    conn.close()
    return


def reset_and_reload():
    task_sql = """SELECT DISTINCT task_name FROM tasks.task_config 
                where task_name != 'Sync Garmin Activities' AND api_function is not NULL"""

    task_list = sql_to_dict(task_sql)
    for task_dict in task_list:
        task_name = task_dict.get("task_name")
        print(task_name)
        reset_sql = f"""UPDATE tasks.task_config
        	    SET updated_through_date = '2020-01-01'
        	    WHERE task_name = '{task_name}';"""
        qec(reset_sql)

    stg_sql = """SELECT relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'staging'
                  AND c.relkind = 'r' 
                  AND relname != 'stg_activitites'-- ordinary tables
                ORDER BY relname;
                        """

    stg_list = sql_to_dict(stg_sql)

    for stg in stg_list:
        stg_table = stg.get("table_name")
        print(f"Deleting from: {stg_table}")
        del_sql = f"TRUNCATE staging.{stg_table}"
        qec(del_sql)


def update_task_through_date(task_name):
    today = date.today()
    update_sql = f"""UPDATE tasks.task_config
                                SET updated_through_date = %s 
                                WHERE task_name = %s;"""
    params = (today, task_name)
    qec(update_sql, params)
    return


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

def default_range():
    d2 = date.today()
    d1 = d2 - timedelta(days=1)
    return d1, d2
