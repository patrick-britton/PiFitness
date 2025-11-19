import json
import time
import importlib
from datetime import date
from psycopg2.extras import execute_values

from backend_functions.database_functions import sql_to_dict, qec, con_cur
from backend_functions.helper_functions import get_sync_dates
from backend_functions.logging_functions import start_timer, log_app_event, elapsed_ms





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
            continue

        if force_task and task_name == force_task_name:
            execution_type = 'forced'
            print(f"Forced execution of {force_task_name}")
        elif task.get("task_frequency") == 'Retired':
            execution_type = 'Retired'
        else:
            execution_type = task.get("execution_logic")

        # Skip any retired/inactive tasks
        if execution_type == 'Retired':
            continue

        # Skip the too-recently-executed tasks
        if execution_type == 'recency':
            recency_ctr += 1
            continue

        # Skip tasks with too many consecutive failures
        if execution_type == 'failures':
            log_app_event(cat='Task Failure', desc=task_name, err='Skipped due to consecutive Failures')
            failure_ctr += 1
            continue

        # SKip tasks that fail for scheduling reasons
        if execution_type == 'timing' and not task.get("execute_task"):
            timing_ctr += 1
            continue
        ############################################################
        # Execute the prescribed function (e.g. database cleanup)
        ############################################################
        if task.get("api_function") is None:
            pf_t0 = start_timer()
            try:
                local_function_str = task.get("python_function")
                module_name, svc_function_name = local_function_str.rsplit('.', 1)
                module = importlib.import_module(module_name)
                local_function = getattr(module, svc_function_name)
                local_function()
                task_log(task.get("task_name"),
                         e_time=None,
                         l_time=None,
                         t_time=elapsed_ms(pf_t0))
            except Exception as e:
                task_log(task.get("task_name"),
                         e_time=None,
                         l_time=None,
                         t_time=elapsed_ms(pf_t0),
                         fail_type='transform',
                         fail_text=str(e))
            continue

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
                loop_type = task.get("api_loop_type")
                if loop_type is None:
                    json_data = getattr(client, task.get("api_function"))()
                elif loop_type == 'Next':
                    json_data = json_next_loop(client, task.get("api_function"))
                else:
                    date_list = get_sync_dates(task.get("sync_from_date"), loop_type)
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
                    qec(call_sql)
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
                    pg_schema_table, pg_field_name = cal_col.rsplit('.', 1)
                    update_sql = f"""UPDATE tasks.task_config
                                SET updated_through_date = (SELECT MAX({pg_field_name}) FROM {pg_schema_table}) 
                                WHERE task_name = '{task_name}';"""
                    qec(update_sql)
            else:
                today = date.today()
                update_sql = f"""UPDATE tasks.task_config
                            SET updated_through_date = %s 
                            WHERE task_name = %s;"""
                params = (today, task_name)
                qec(update_sql, params)


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
    all_json = []
    # Fetch & loop through API results
    offset = 0
    limit = 50
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
        if date_val != date_list[0]:
            time.sleep(2)

        if loop_type == 'Range':
            d1 = date_val[0]
            d2 = date_val[1]
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