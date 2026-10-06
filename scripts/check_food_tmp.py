import psycopg2

DSN = dict(host="192.168.86.104", port=5432, dbname="personal_fitness",
           user="devuser", password="god7armdqeN", sslmode="disable")
conn = psycopg2.connect(**DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='food' ORDER BY 1")
print('TABLES:', [r[0] for r in cur.fetchall()])
cur.execute("SELECT column_name, data_type, is_nullable FROM "
            "information_schema.columns WHERE table_schema='food' "
            "AND table_name='foods' ORDER BY ordinal_position")
for r in cur.fetchall():
    print(r)
for t in ['diary_entries', 'recipes', 'recipe_steps', 'recipe_step_foods']:
    cur.execute("SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='food' AND table_name=%s", (t,))
    print(t, 'exists:', cur.fetchone()[0])
cur.execute("SELECT count(*) FROM food.foods")
print('FOODS-COUNT:', cur.fetchone())
cur.close(); conn.close()
