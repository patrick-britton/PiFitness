"""Round-trips a throwaway row through logging.application_events: insert, read on fresh connection, delete."""
import psycopg2

DESC = 'throwaway round-trip probe row'

def conn():
    return psycopg2.connect(host="localhost", port=15432, dbname="pifitness_dev", user="dev", password="devpassword")

c1 = conn()
cur = c1.cursor()
cur.execute("""INSERT INTO logging.application_events
    (event_time_utc, event_category, event_description, execution_time_ms, error_text, task_id, data_event)
    VALUES (now(), 'probe', %s, 1, NULL, NULL, 'probe')""", (DESC,))
c1.commit()
print("INSERT_OK")

c2 = conn()
cur2 = c2.cursor()
cur2.execute("SELECT count(*) FROM logging.application_events WHERE event_description=%s", (DESC,))
print("READ_BACK_COUNT", cur2.fetchone()[0])
c2.close()

cur.execute("DELETE FROM logging.application_events WHERE event_description=%s", (DESC,))
c1.commit()
cur.execute("SELECT count(*) FROM logging.application_events WHERE event_description=%s", (DESC,))
print("AFTER_DELETE_COUNT", cur.fetchone()[0])
c1.close()
print("PROBE_ROUNDTRIP_OK")
