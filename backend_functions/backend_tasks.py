import subprocess
from datetime import datetime, timezone
from pathlib import Path
import json
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

    st = start_timer()

    for var in ["PG_BACKUP_LOCATION", "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"]:
        if os.getenv(var) is None:
            if var == 'PG_BACKUP_LOCATION':
                log_app_event(cat="DB Backup", desc="Skipped backup: PG_BACKUP_LOCATION not set (running locally?)")
                print('skipping backup, being run locally')
                return None
            else:
                log_app_event(cat="DB Backup", desc=f"Missing required environment variable: {var}", err=f"Missing required environment variable: {var}")
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

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    except FileNotFoundError as e:
        log_app_event(cat="DB Backup", desc="pg_dump binary not found on PATH", err=e,
                      exec_time=elapsed_ms(st))
        raise

    dump_elapsed_ms = elapsed_ms(st)

    if result.returncode != 0:
        log_app_event(cat="DB Backup",
                      desc=f"pg_dump failed (rc={result.returncode}): {backup_file}",
                      err=result.stderr, exec_time=dump_elapsed_ms)
        raise RuntimeError(f"Backup failed: {result.stderr}")

    backups = sorted(Path(backup_dir).glob("*.dump"))
    pruned = []
    while len(backups) > keep:
        old = backups.pop(0)
        pruned.append(old.name)
        old.unlink()

    total_elapsed = elapsed_ms(st)
    desc = f"Backup created: {Path(backup_file).name} ({dump_elapsed_ms / 1000:.2f}s dump)"
    if pruned:
        desc += f" | Pruned: {', '.join(pruned)}"
    log_app_event(cat="DB Backup", desc=desc, exec_time=total_elapsed)

    return backup_file


def run_health_checks():
    """Returns (overall_status, list_of_check_dicts). Also writes health_checks rows and health_status.json."""
    checks = []
    PiFitness = Path("/home/god/Documents/PiFitness_Local")
    BackupDir = Path("/home/god/Documents/DB_Backups")

    # 1. DB checksum failures
    row = one_sql_result("""
        SELECT checksum_failures FROM pg_stat_database
        WHERE datname = current_database()
    """) or 0
    checks.append({
        "name": "pg_checksum_failures",
        "status": "ok" if row == 0 else "critical",
        "value": row,
        "message": f"{row} checksum failures reported by PostgreSQL"
    })

    # 2. Long-running queries (>5 min)
    row = one_sql_result("""
        SELECT count(*) FROM pg_stat_activity
        WHERE state <> 'idle' AND now() - query_start > interval '5 minutes'
          AND pid <> pg_backend_pid()
    """) or 0
    checks.append({
        "name": "long_running_queries",
        "status": "ok" if row == 0 else "warn",
        "value": row,
        "message": f"{row} queries running longer than 5 minutes"
    })

    # 3. Backup freshness
    dumps = sorted(BackupDir.glob("*.dump"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not dumps:
        checks.append({"name":"backup_freshness","status":"critical","value":None,
                       "message":"No backup files found"})
    else:
        newest = dumps[0]
        age_h = (time.time() - newest.stat().st_mtime) / 3600
        size_mb = newest.stat().st_size / 1024 / 1024
        status = "ok" if age_h < 25 and size_mb > 100 else "warn"
        checks.append({"name":"backup_freshness","status":status,"value":round(age_h,1),
                       "message":f"Newest dump {newest.name}: {age_h:.1f}h old, {size_mb:.0f}MB"})

    # 4. Hardware health (from root helper)
    hw_file = PiFitness / "hardware_health.json"
    if hw_file.exists():
        hw = json.loads(hw_file.read_text())
        # Stale check
        hw_age_min = (time.time() - hw_file.stat().st_mtime) / 60
        if hw_age_min > 180:
            checks.append({"name":"hw_helper_fresh","status":"warn","value":round(hw_age_min),
                           "message":f"hardware_health.json is {hw_age_min:.0f} min old"})
        # Undervoltage
        checks.append({"name":"undervoltage","status":"ok" if hw.get("throttled_ok") else "critical",
                       "value":hw.get("throttled"), "message":f"throttled={hw.get('throttled')}"})
        # NVMe media errors
        media = int(hw.get("nvme",{}).get("media_errors","0").split()[0] or 0)
        spare = hw.get("nvme",{}).get("available_spare","100%")
        checks.append({"name":"nvme_media_errors","status":"ok" if media==0 else "critical",
                       "value":media, "message":f"media_errors={media}, spare={spare}"})
    else:
        checks.append({"name":"hw_helper_fresh","status":"warn","value":None,
                       "message":"hardware_health.json not found"})

    overall = "ok"
    for c in checks:
        if c["status"] == "critical": overall = "critical"; break
        if c["status"] == "warn" and overall == "ok": overall = "warn"

    # Persist to DB
    for c in checks:
        qec("""INSERT INTO logging.health_checks
               (check_name, status, value, message)
               VALUES (%s, %s, %s, %s)""",
            p=(c["name"], c["status"], c["value"], c["message"]))

    # Persist for the frontend (atomic write)
    status_file = PiFitness / "health_status.json"
    tmp = status_file.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "checks": checks,
    }, indent=2))
    tmp.replace(status_file)

    return overall, checks