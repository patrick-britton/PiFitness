import psycopg2

DSN = dict(host="192.168.86.104", port=5432, dbname="personal_fitness",
           user="devuser", password="god7armdqeN", sslmode="disable")

try:
    conn = psycopg2.connect(**DSN)
except psycopg2.OperationalError as e:
    print("plain/ssl=disable failed:", e)
    DSN["sslmode"] = "require"
    conn = psycopg2.connect(**DSN)
    print("connected with sslmode=require")

conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor()

def q(sql, label):
    print(f"\n--- {label} ---")
    try:
        cur.execute(sql)
        for row in cur.fetchall():
            print(row)
    except Exception as e:
        print("ERROR:", e)

q("select current_database(), current_user, version()", "connection")
q("select datname from pg_database order by 1", "databases")
q("select schema_name from information_schema.schemata order by 1", "schemas")
q("""select table_schema, table_name, table_type
     from information_schema.tables
     where table_name like 'vw_activity_summary%'""", "vw_activity_summary locations")
q("""select grantee, privilege_type
     from information_schema.role_table_grants
     where table_schema='activities' and table_name='vw_activity_summary'""", "grants on activities.vw_activity_summary")
q("""select rolname, rolsuper, rolcanlogin from pg_roles
     where rolname in ('devuser','god','postgres')""", "roles")
q("""select viewowner from pg_views
     where schemaname='activities' and viewname='vw_activity_summary'""", "view owner")
q("select count(*) from activities.vw_activity_summary", "select from activities.vw_activity_summary")
q("select count(*) from activities.vw_last_activity_id_by_type", "select from activities.vw_last_activity_id_by_type")

cur.close(); conn.close()
