import psycopg
from psycopg import sql
from backend_functions.database_functions import get_conn

DSN = "host=localhost dbname=personal_fitness user=god"  # add password=... or use ~/.pgpass


def identify_relfilenode(conn, relfilenode):
    """Look up what a relfilenode from an error message actually is."""
    return conn.execute(
        """SELECT n.nspname, c.relname, c.relkind
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE c.relfilenode = %s""", (relfilenode,)).fetchall()


def reindex(conn, schema, index):
    """Rebuild an index from its (readable) table. Fails loudly if the heap is bad too."""
    try:
        conn.execute(sql.SQL("REINDEX INDEX {}.{}").format(
            sql.Identifier(schema), sql.Identifier(index)))
        print(f"OK: reindexed {schema}.{index}")
        return True
    except psycopg.errors.DataCorrupted as e:  # SQLSTATE XX001
        print(f"FAILED {schema}.{index}: {e}")
        print("  -> heap likely damaged too; use salvage_table() on the owning table.")
        return False


def salvage_table(conn, schema, table, swap=False):
    """
    Copy every readable row into <table>_salvage, skipping damaged pages.
    With swap=True, also swap it in and rebuild indexes/constraints.
    Requires superuser (zero_damaged_pages is superuser-only).
    """
    assert conn.autocommit, "open the connection with autocommit=True"
    if conn.execute("SHOW is_superuser").fetchone()[0] != "on":
        raise SystemExit("zero_damaged_pages needs a superuser connection")

    old = sql.Identifier(schema, table)
    new_name = f"{table}_salvage"
    new = sql.Identifier(schema, new_name)

    # 1. Save index/constraint definitions BEFORE touching anything.
    constraints = conn.execute("""
        SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid = %s::regclass AND contype IN ('p','u','x')""",
        (f"{schema}.{table}",)).fetchall()
    indexes = conn.execute("""
        SELECT c.relname, pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = %s::regclass AND i.indisvalid
          AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)""",
        (f"{schema}.{table}",)).fetchall()
    all_index_names = [r[0] for r in conn.execute(
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "WHERE i.indrelid = %s::regclass", (f"{schema}.{table}",))]

    # 2. Copy readable rows into a new table (columns/defaults/checks, no indexes yet).
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(new))
    conn.execute(sql.SQL(
        "CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING GENERATED "
        "INCLUDING IDENTITY INCLUDING CONSTRAINTS INCLUDING STORAGE)").format(new, old))
    conn.execute("SET zero_damaged_pages = on")
    conn.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(new, old))
    conn.execute("RESET zero_damaged_pages")
    n = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(new)).fetchone()[0]
    print(f"Salvaged {n} rows into {schema}.{new_name}")

    if not swap:
        print("swap=False: stopping here. Inspect the salvage table, then rerun with swap=True.")
        return n

    # 3. Swap + rebuild, atomically.
    with conn.transaction():
        conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            old, sql.Identifier(f"{table}_corrupt")))
        for name in all_index_names:  # free up the original index/constraint names
            conn.execute(sql.SQL("ALTER INDEX {}.{} RENAME TO {}").format(
                sql.Identifier(schema), sql.Identifier(name),
                sql.Identifier(name[:54] + "_corrupt")))
        conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            new, sql.Identifier(table)))
        for name, definition in constraints:
            conn.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} ").format(
                old, sql.Identifier(name)) + sql.SQL(definition))
        for name, definition in indexes:
            conn.execute(definition)  # definition already targets the original table name
    print(f"Swapped. Old data kept as {schema}.{table}_corrupt")
    return n


if __name__ == "__main__":
    conn = get_conn

        # Heap tables you want to salvage rather than truncate:
    salvage_table(conn, "logging", "db_size_log", swap=False)