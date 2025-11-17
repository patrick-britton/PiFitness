import subprocess
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from backend_functions.database_functions import qec, one_sql_result, con_cur, sql_to_dict
from backend_functions.helper_functions import list_to_dict_by_key
from backend_functions.logging_functions import log_app_event, elapsed_ms, start_timer
import os

load_dotenv()


def nightly_maintenance(days_to_keep=365):
    # Truncates Log files
    # Vacuums the database
    # Optimizes weekly
    # Reindexes and does a full vacuum monthly

    st = start_timer() # Track elapsed seconds

    conn, cursor = con_cur()

    try:
        conn.autocommit = True
        # 2. Delete old eventLog rows (>48h)

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


        logging_tables = list_to_dict_by_key(sql_to_dict(logging_sql), 'table_name').keys()

        for log_table in logging_tables:
            del_sql = f"""
                        DELETE FROM {log_table}
                        WHERE event_timestamp_utc < NOW() - INTERVAL %s;
                    """
            interval = f"{days_to_keep} days"
            qec(del_sql, (interval,))

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


        # # Performance Testing
        # tsql = "SELECT * FROM public.vw_db_performance_test"
        # perf_start = start_timer()
        # cursor.execute(tsql)
        # _ = cursor.fetchall()
        # elapsed_ms = elapsed_ms(perf_start)

        # 4. Log results
        tsql = """INSERT INTO logging.db_size_log (table_name, total_size_mb, table_size_mb, index_size_mb) 
                SELECT * FROM logging.vw_db_size"""
        qec(tsql)

        tsql = "SELECT SUM(total_size_mb) from public.vw_db_size"
        size_after = one_sql_result(tsql)

        # 5. Record total elapsed time
        elapsed = elapsed_ms(st)
        log_app_event(cat="DB Maintenance",
                  desc=f"Time {elapsed / 1000:.2f}s | Size {size_before:.1f} → {size_after:.1f}MB",
                  exec_time=elapsed)

        tsql = """INSERT into logging.db_stats (size_before, size_after, execution_time, 
                        maintenance_time, maintenance_type) 
                        VALUES (%s, %s, %s, %s, %s);"""

        qec(tsql, p=(st, size_before, size_after, elapsed_ms, elapsed, maintenance_type))
        print('Nightly Maintenance success')


    except Exception as e:
        log_app_event(cat="DB Maintenance", desc="Error during maintenance", err=e)
        print(f"Nightly Maintenance failure: {e}")
        conn.close()
        return False

    conn.close()

    return True


def backup_database(keep=7):
    # Creates a backup and keeps the most recent 7

    for var in ["PG_BACKUP_LOCATION", "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"]:
        if os.getenv(var) is None:
            raise ValueError(f"Missing required environment variable: {var}")


    backup_dir = Path(os.getenv("PG_BACKUP_LOCATION"))
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    dbname = os.getenv("PG_DB")
    user = os.getenv("PG_USER")


    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"{dbname}_{timestamp}.dump")

    cmd = [
        "pg_dump",
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", dbname,
        "-F", "c",
        "-f", backup_file
    ]

    env = os.environ.copy()

    env["PGPASSWORD"] = os.getenv("PG_PASSWORD")

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        raise RuntimeError(f"Backup failed: {result.stderr}")

    backups = sorted(Path(backup_dir).glob("*.dump"))
    while len(backups) > keep:
        old = backups.pop(0)
        old.unlink()

    return backup_file