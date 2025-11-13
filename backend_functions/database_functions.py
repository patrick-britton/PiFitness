import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import socket
from psycopg2.extras import RealDictCursor, execute_values

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