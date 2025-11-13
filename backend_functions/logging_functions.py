import time

from backend_functions.database_functions import qec


def log_app_event(cat, desc, err=None, exec_time=None):
    # creates a new log entry into eventLog table
    cat = str(cat)
    cat = cat.replace("'", "")
    desc = str(desc)
    desc = desc.replace("'", "")
    if err is not None:
        err = str(err)
        err = err.replace("'", "")

    statement = (f"""INSERT INTO logging.application_events
                (event_category, event_description, execution_time_ms, error_text)
                VALUES (%s, %s, %s, %s);
                         """)
    params = (cat, desc, exec_time, err)

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