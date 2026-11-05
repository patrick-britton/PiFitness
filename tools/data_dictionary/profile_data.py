"""Aggregate-only profile of safe low-cardinality category values; never emits records."""
from __future__ import annotations
import argparse
from pathlib import Path
from common import connect_readonly, database_config, fetch_dicts, quote_ident, qualified_name, safe_category, validate_connection_identity, write_json

REL_SQL = """SELECT n.nspname AS schema_name,c.relname AS object_name,
       c.reltuples::bigint AS estimated_rows,a.attname AS column_name,
       format_type(a.atttypid,a.atttypmod) AS data_type
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY n.nspname,c.relname,a.attnum"""

def main() -> int:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--env-file',default='backend/.env'); p.add_argument('--out',default='docs/data/_evidence/data_profiles.json'); p.add_argument('--sample-percent',type=float,default=0.5); p.add_argument('--repeatable-seed',type=int,default=11); p.add_argument('--max-values',type=int,default=15); p.add_argument('--max-relations',type=int,default=500); args=p.parse_args()
    if not (0.01 <= args.sample_percent <= 5): raise SystemExit('--sample-percent must be between 0.01 and 5')
    cfg=database_config(args.env_file)
    conn=connect_readonly(args.env_file); result={'read_only':True,'mode':'aggregate-only TABLESAMPLE; no raw rows','sample_percent':args.sample_percent,'relations':[]}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database() AS database, current_user AS role, inet_server_addr()::text AS server_address, inet_server_port() AS server_port, current_setting('transaction_read_only') AS transaction_read_only")
            identity=fetch_dicts(cur)[0]
            validate_connection_identity(identity, cfg)
            print(
                f"Connected read-only to {cfg['host']}:{cfg['port']}/"
                f"{identity['database']} as {identity['role']} "
                f"(PostgreSQL listener port inside server: {identity['server_port']})."
            )
            cur.execute(REL_SQL); cols=fetch_dicts(cur)
            by_rel={}
            for row in cols: by_rel.setdefault((row['schema_name'],row['object_name'],row['estimated_rows']),[]).append(row)
            for idx,((schema,name,estimate),columns) in enumerate(by_rel.items()):
                if idx>=args.max_relations: print('Reached --max-relations cap'); break
                safe=[c for c in columns if safe_category(c['column_name'],c['data_type'])]
                if not safe: continue
                rel=qualified_name(schema,name)
                entry={'schema':schema,'table':name,'estimated_rows':estimate,'category_columns':[]}
                for col in safe:
                    ident=quote_ident(col['column_name'])
                    # Sample only category-like fields. Output category labels/counts, never whole records.
                    sql=(f"SELECT {ident}::text AS value, count(*) AS sampled_count "
                         f"FROM {rel} TABLESAMPLE SYSTEM ({args.sample_percent}) REPEATABLE ({args.repeatable_seed}) "
                         f"WHERE {ident} IS NOT NULL GROUP BY {ident} ORDER BY count(*) DESC LIMIT %s")
                    cur.execute(sql,(args.max_values,)); values=fetch_dicts(cur)
                    # Restrict labels to short scalar values and defensively redact unexpected long strings.
                    safe_values=[]
                    for item in values:
                        value=item['value']
                        if value is not None and len(str(value))>80: value='<omitted-long-category-label>'
                        safe_values.append({'value':value,'sampled_count':item['sampled_count']})
                    if safe_values:
                        entry['category_columns'].append({'column':col['column_name'],'data_type':col['data_type'],'observed_categories':safe_values})
                if entry['category_columns']: result['relations'].append(entry)
                if idx % 20 == 0: print(f"Profiled relation {idx+1}/{len(by_rel)}")
        write_json(args.out,result); print(f"Wrote aggregate-only profiles for {len(result['relations'])} relations -> {args.out}")
    finally:
        conn.rollback(); conn.close()
    return 0
if __name__=='__main__': raise SystemExit(main())
