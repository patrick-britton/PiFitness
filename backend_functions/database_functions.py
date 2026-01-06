import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import socket
from psycopg2.extras import RealDictCursor, execute_values
import time


from backend_functions.helper_functions import list_to_dict_by_key

load_dotenv()


def start_timer():
    return time.perf_counter()


def elapsed_ms(start_time):
    return int((time.perf_counter() - start_time) * 1000)


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


def qec(t_sql=None, p=None, auto_commit=False):
    # takes in sql, connects, executes, commits, and closes
    if not t_sql:
        return
    try:
        conn, cur = con_cur()
        if auto_commit:
            conn.autocommit = True
        if p is None:
            cur.execute(t_sql)
        else:
            cur.execute(t_sql, p)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Query Execution failure: {e}")
        print(f"Failing SQL: {t_sql}")
        print(f"Failing Params: {p}")


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



def get_log_tables(as_list=False):
    logging_sql = """SELECT c.relname as table_name
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                    WHERE 
                        n.nspname = 'logging'
                        AND a.attname = 'event_time_utc'
                        AND c.relkind = 'r'     -- only real tables
                    ORDER BY 
                        c.relname;"""
    if as_list:
        return list(list_to_dict_by_key(sql_to_dict(logging_sql), 'table_name').keys())
    else:
        return list_to_dict_by_key(sql_to_dict(logging_sql), 'table_name').keys()


def get_log_data(table_name):
    sql = f"""SELECT *, time_ago(event_time_utc) as event_age FROM logging.{table_name} ORDER BY event_time_utc DESC"""
    df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    return df


def get_table_row_count(pg_schema, pg_table):
    q_sql = f"""SELECT COUNT(*) FROM {pg_schema}.{pg_table};"""
    return one_sql_result(q_sql)


