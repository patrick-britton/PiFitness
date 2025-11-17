import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import socket
from psycopg2.extras import RealDictCursor, execute_values
import time
from backend_functions.logging_functions import start_timer

load_dotenv()

def get_conn(alchemy=False):
    # returns the raw psycopg2 connection unless pandas/alchemy is requested.

    if alchemy:
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT")
        dbname = os.getenv("PG_DB")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(conn_str)
        return engine
    else:
        c = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            sslmode=os.getenv("PGSSLMODE", "disable")
        )
        return c


def con_cur():
    c = get_conn()
    cr= c.cursor()
    return c, cr


def qec(t_sql=None, p=None):
    # takes in sql, connects, executes, commits, and closes
    if not t_sql:
        return
    try:
        conn, cur = con_cur()
        if p is None:
            cur.execute(t_sql)
        else:
            cur.execute(t_sql, p)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Query Execution failure: {e}")


    return


def one_sql_result(sql=None):
    # returns a single value from a sql query
    if not sql:
        return None
    conn, cursor = con_cur()
    cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return None

    # row is a tuple — return the first element
    return row[0]


def sql_to_dict(query_str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query_str)
    rows = cur.fetchall()  # list of dicts if using RealDictCursor
    cur.close()
    conn.close()
    return rows


def get_sproc_list(append_option=None):
    # Returns the known api services as a list
    sql="""SELECT 
            routine_name,
            routine_type,
            data_type AS return_type,
            routine_definition,
            routine_schema
        FROM information_schema.routines
        WHERE routine_type = 'PROCEDURE'
        ORDER BY routine_name"""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    sproc_list = [row[0] for row in cursor.fetchall()]
    if append_option:
        sproc_list.append(append_option)
    return sproc_list


def nightly_maintenance(days_to_keep=180):
    # Truncates Log files
    # Vacuums the database
    # Optimizes weekly
    # Reindexes and does a full vacuum monthly

    st = start_timer() # Track elapsed seconds

    conn, cursor = con_cur()

    try:
        conn.autocommit = True
        # 2. Delete old eventLog rows (>48h)
        cutoff = int(time.time() * 1000) - (days_to_keep*24) * 3600 * 1000  # ms since epoch

        # Pull a list of all logging tables in the database
        log_table_list = []

        for log_table in log_table_list:
            tsql = f"DELETE FROM logging.{log_table} WHERE event_time_utc < %s"
            cursor.execute(tsql, (cutoff,))
            conn.commit()
        # log_entry(cat="DB Maintenance",
        #           desc=f"Deleted {deleted} old eventLog rows (cutoff={cutoff})",
        #           sql=sql,
        #           sql_p=(cutoff,))

        # Log stats before VACUUM
        tsql = "SELECT SUM(total_size_mb) from public.vw_db_size"
        size_before = one_sql_result(tsql)


        # 3. Vacuum

        cursor.execute("VACUUM;")
        maintenance_type = 'daily'

        if datetime.today().weekday() == 6:
            cursor.execute("ANALYZE;")
            maintenance_type = 'weekly'

        if datetime.today().day == 1:
            cursor.execute("REINDEX DATABASE personal_fitness;")
            cursor.execute("VACUUM FULL;")
            maintenance_type = 'monthly'


        # Performance Testing
        tsql = "SELECT * FROM public.vw_db_performance_test"
        perf_start = int(datetime.now(pytz.UTC).timestamp() * 1000)
        cursor.execute(tsql)
        _ = cursor.fetchall()
        perf_end = int(datetime.now(pytz.UTC).timestamp() * 1000)
        elapsed_ms = perf_end - perf_start

        # 4. Log results
        tsql = """INSERT INTO public.db_size_log SELECT * FROM public.vw_db_size"""
        qec(tsql)

        tsql = "SELECT SUM(total_size_mb) from public.vw_db_size"
        size_after = one_sql_result(tsql)



        # 5. Record total elapsed time
        elapsed = int(datetime.now(pytz.UTC).timestamp() * 1000) - st
        log_entry(cat="DB Maintenance",
                  desc=f"Time {elapsed / 1000:.2f}s | Size {size_before:.1f} → {size_after:.1f}MB",
                  exec_time=elapsed)

        tsql = """INSERT into public.db_performance_results (timestamp_utc, size_before, size_after, execution_time, 
                        maintenance_time, maintenance_type) 
                        VALUES (%s, %s, %s, %s, %s, %s);"""

        qec(tsql, p=(st, size_before, size_after, elapsed_ms, elapsed, maintenance_type))
        print('Nightly Maintenance success')


    except Exception as e:
        log_entry(cat="DB Maintenance", desc="Error during maintenance", err=e)
        print(f"Nightly Maintenance failure: {e}")
        conn.close()
        return False

    conn.close()



    return True
