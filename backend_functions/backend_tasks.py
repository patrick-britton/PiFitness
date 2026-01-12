import subprocess
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from backend_functions.database_functions import qec, one_sql_result, con_cur, sql_to_dict, get_log_tables
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

        logging_tables = get_log_tables()

        for log_table in logging_tables:
            del_sql = f"""
                        DELETE FROM logging.{log_table}
                        WHERE event_time_utc < NOW() - INTERVAL %s;
                    """
            interval = f"{days_to_keep} days"
            qec(del_sql, (interval,))

        # Log stats before VACUUM
        tsql = "SELECT SUM(total_size_mb) from logging.vw_db_size"
        size_before = one_sql_result(tsql)


        # 3. Vacuum
        maint_start = start_timer()
        cursor.execute("VACUUM;")
        maintenance_type = 'daily'

        if datetime.today().weekday() == 6:
            cursor.execute("ANALYZE;")
            maintenance_type = 'weekly'

        if datetime.today().day == 1:
            cursor.execute("REINDEX DATABASE personal_fitness;")
            cursor.execute("VACUUM FULL;")
            maintenance_type = 'monthly'

        maint_elapsed_ms = elapsed_ms(maint_start)
        # # Performance Testing
        # tsql = "SELECT * FROM public.vw_db_performance_test"
        # perf_start = start_timer()
        # cursor.execute(tsql)
        # _ = cursor.fetchall()
        # elapsed_ms = elapsed_ms(perf_start)

        # 4. Log results
        tsql = """INSERT INTO logging.db_size_log (table_name, total_size_mb, table_size_mb, index_size_mb) 
                SELECT table_name, total_size_mb, table_size_mb, index_size_mb FROM logging.vw_db_size"""
        qec(tsql)

        tsql = "SELECT SUM(total_size_mb) from logging.vw_db_size"
        size_after = one_sql_result(tsql)

        # 5. Record total elapsed time
        total_elapsed = elapsed_ms(st)
        log_app_event(cat="DB Maintenance",
                  desc=f"Time {total_elapsed / 1000:.2f}s | Size {size_before:.1f} → {size_after:.1f}MB",
                  exec_time=total_elapsed)

        tsql = """INSERT into logging.db_stats (size_before_mb, size_after_mb, maintenance_time_ms, 
                        total_time_ms, maintenance_type) 
                        VALUES (%s, %s, %s, %s, %s);"""

        qec(tsql, p=(size_before, size_after, maint_elapsed_ms, total_elapsed, maintenance_type))
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
            if var == 'PG_BACKUP_LOCATION':
                print('skipping backup, being run locally')
                return None
            else:
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