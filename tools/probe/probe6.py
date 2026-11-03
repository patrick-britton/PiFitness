"""Checks database COMMENT coverage across all tables and columns (D2 evidence)."""
import psycopg2

c = psycopg2.connect(host="localhost", port=15432, dbname="pifitness_dev", user="dev", password="devpassword")
cur = c.cursor()
cur.execute("""select count(*), count(nullif(col_description(c.oid, a.attnum), '')) from pg_class c join pg_attribute a on a.attrelid = c.oid join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and n.nspname not in ('pg_catalog','information_schema') and a.attnum > 0 and not a.attisdropped""")
total, commented = cur.fetchone()
print("TABLE_COLUMNS", total, "COMMENTED", commented, "MISSING", total - commented)
cur.execute("""select nspname, relname, obj_description(c.oid) from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and n.nspname not in ('pg_catalog','information_schema') and obj_description(c.oid) is null order by 1, 2 limit 10""")
print("TABLES_WITHOUT_COMMENT:", cur.fetchall())
c.close()
print("PROBE_D2_OK")
