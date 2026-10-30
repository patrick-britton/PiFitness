import time

from backend_functions.database_functions import qec


def log_app_event(cat, desc, err=None, exec_time=None, task_id=None, data_event=None):
    # creates a new log entry into eventLog table
    cat = str(cat)
    cat = cat.replace("'", "")
    desc = str(desc)
    desc = desc.replace("'", "")

    ins_sql = f"""INSERT INTO logging.application_events (event_category, event_description"""
    values_sql = ") VALUES (%s, %s"
    params = [cat, desc]


    if err is not None:
        err = str(err)
        err = err.replace("'", "")
        ins_sql = f"{ins_sql}, error_text"
        values_sql = f"{values_sql}, %s"
        params.append(err)

    if exec_time is not None:
        ins_sql = f"{ins_sql}, execution_time_ms"
        values_sql = f"{values_sql}, %s"
        params.append(int(exec_time))

    if task_id is not None:
        ins_sql = f"{ins_sql}, task_id"
        values_sql = f"{values_sql}, %s"
        params.append(int(task_id))

    if data_event is not None:
        data_event = str(data_event)
        data_event = data_event.replace("'", "")
        ins_sql = f"{ins_sql}, data_event"
        values_sql = f"{values_sql}, %s"
        params.append(data_event)


    statement = f"{ins_sql}{values_sql});"
    qec(statement, params)
    return

def log_api_event(service, event, token_age=None, err=None):
    insert_sql = """INSERT INTO logging.api_logins  
        (api_service_name, event_name, token_age_s, error_text) VALUES
        (%s, %s, %s, %s); """
    params = (service, event, token_age, str(err))
    qec(insert_sql, params)
    return


def start_timer():
    return time.perf_counter()


def elapsed_ms(start_time):
    return int((time.perf_counter() - start_time) * 1000)