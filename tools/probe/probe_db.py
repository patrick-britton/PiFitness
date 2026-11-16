"""Checks DEV database identity: configured host/name and SELECT current_database()."""
import os
import psycopg2

print("PG_HOST=", os.environ.get("PG_HOST", "<unset>"))
print("PG_DB=", os.environ.get("PG_DB", "<unset>"))
print("PG_PORT=", os.environ.get("PG_PORT", "<unset>"))
print("PG_USER=", os.environ.get("PG_USER", "<unset>"))

conn = psycopg2.connect(host="localhost", port=15432, dbname="pifitness_dev", user="dev", password="devpassword", connect_timeout=5)
cur = conn.cursor()
cur.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()")
print("DB:", cur.fetchone())
conn.close()
print("PROBE_DB_OK")
