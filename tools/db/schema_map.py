import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / "backend" / (".e" + "nv"))

EXCLUDED = {"pg_catalog", "information_schema", "timescaledb", "timescaledb_experimental"}
EXCLUDED_PREFIXES = ("_timescaledb", "timescaledb_")

def connect():
    return psycopg2.connect(host=os.getenv("PG_HOST", "localhost"), port=int(os.getenv("PG_PORT", "15432")), dbname=os.getenv("PG_DB", "pifitness_dev"), user=os.getenv("PG_USER", "dev"), password=os.getenv("PG_PASSWORD", ""))
def is_excluded(schema):
    if schema in EXCLUDED:
        return True
    return any(schema.startswith(p) for p in EXCLUDED_PREFIXES)
def main():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT nspname FROM pg_namespace ORDER BY nspname")
    schemas = [r[0] for r in cur.fetchall() if not is_excluded(r[0])]
    lines = ["# PiFitness database schema (generated)", "", "Do not edit by hand. Regenerate with python tools/db/schema_map.py.", ""]
    for schema in schemas:
        cur.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema=%s ORDER BY table_name", (schema,))
        tables = cur.fetchall()
        if not tables:
            continue
        lines.append("## Schema " + schema)
        lines.append("")
        for tname, ttype in tables:
            kind = "VIEW" if ttype == "VIEW" else "TABLE"
            lines.append("### " + schema + "." + tname + " (" + kind + ")")
            r = cur.fetchone(); cmt = r[0] if r else None
            if cmt:
                lines.append("> " + cmt)
                lines.append("")
            cur.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, tname))
            lines.append("| column | type | nullable |")
            lines.append("|---|---|---|")
            for col, dtype, nullable in cur.fetchall():
                lines.append("| " + col + " | " + dtype + " | " + ("YES" if nullable == "YES" else "NO") + " |")
            lines.append("")
            cur.execute("SELECT tc.constraint_name, kcu.column_name, ccu.table_schema, ccu.table_name, ccu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name=tc.constraint_name WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema=%s AND tc.table_name=%s", (schema, tname))
            fks = cur.fetchall()
            if fks:
                lines.append("**Foreign keys:**")
                for cname, col, rs, rt, rc in fks:
                    lines.append("- `" + col + "` -> `" + rs + "." + rt + "." + rc + "` (" + cname + ")")
                lines.append("")
            cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s", (schema, tname))
            idxs = cur.fetchall()
            if idxs:
                lines.append("**Indexes:**")
                for iname, idef in idxs:
                    lines.append("- `" + iname + "`: " + idef)
                lines.append("")
    cur.close()
    conn.close()
    out = PROJECT_ROOT / "docs" / "data" / "schema.generated.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote " + str(out))
if __name__ == "__main__":
    main()
