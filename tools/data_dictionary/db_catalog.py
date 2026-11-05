"""Capture PostgreSQL catalog metadata and view/routine/trigger definitions (read-only)."""
from __future__ import annotations
import argparse, json, os, shutil, subprocess
from pathlib import Path
from common import connect_readonly, database_config, fetch_dicts, validate_connection_identity, write_json

RELATIONS = r"""
SELECT n.nspname AS schema_name, c.relname AS object_name,
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
            WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'f' THEN 'foreign table' END AS object_type,
       obj_description(c.oid, 'pg_class') AS comment,
       c.reltuples::bigint AS estimated_rows,
       CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid, true) END AS definition
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f')
  AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname,c.relname
"""
COLUMNS = r"""
SELECT n.nspname AS schema_name, c.relname AS object_name, a.attnum AS ordinal_position,
       a.attname AS column_name, format_type(a.atttypid,a.atttypmod) AS data_type,
       NOT a.attnotnull AS is_nullable, pg_get_expr(ad.adbin,ad.adrelid) AS column_default,
       a.attidentity AS identity_kind, a.attgenerated AS generated_kind,
       col_description(c.oid,a.attnum) AS comment
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
WHERE c.relkind IN ('r','p','v','m','f')
  AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname,c.relname,a.attnum
"""
CONSTRAINTS = r"""
SELECT n.nspname AS schema_name,c.relname AS object_name,con.conname AS constraint_name,
       con.contype AS constraint_type,pg_get_constraintdef(con.oid,true) AS definition
FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY n.nspname,c.relname,con.conname
"""
INDEXES = r"""
SELECT schemaname AS schema_name,tablename AS object_name,indexname AS index_name,indexdef AS definition
FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY schemaname,tablename,indexname
"""
ROUTINES = r"""
SELECT n.nspname AS schema_name,p.proname AS routine_name,
       CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'f' THEN 'function' WHEN 'a' THEN 'aggregate' ELSE p.prokind::text END AS routine_kind,
       pg_get_function_identity_arguments(p.oid) AS identity_arguments,
       pg_get_function_result(p.oid) AS return_type,
       pg_get_functiondef(p.oid) AS definition
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND p.prokind IN ('f','p')
ORDER BY n.nspname,p.proname,pg_get_function_identity_arguments(p.oid)
"""
TRIGGERS = r"""
SELECT n.nspname AS schema_name,c.relname AS object_name,t.tgname AS trigger_name,
       pg_get_triggerdef(t.oid,true) AS definition
FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE NOT t.tgisinternal AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY n.nspname,c.relname,t.tgname
"""
ENUMS = r"""
SELECT n.nspname AS schema_name,t.typname AS type_name,e.enumlabel AS enum_label,e.enumsortorder AS sort_order
FROM pg_type t JOIN pg_enum e ON e.enumtypid=t.oid JOIN pg_namespace n ON n.oid=t.typnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY n.nspname,t.typname,e.enumsortorder
"""


def main() -> int:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--env-file',default='backend/.env'); p.add_argument('--out-dir',default='docs/data/_evidence/database'); p.add_argument('--skip-pg-dump',action='store_true',help='Do not run schema-only pg_dump'); p.add_argument('--pg-dump-timeout',type=int,default=180); args=p.parse_args()
    out=Path(args.out_dir); out.mkdir(parents=True,exist_ok=True)
    cfg=database_config(args.env_file)
    conn=connect_readonly(args.env_file)
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
            payload={'connection_identity':identity,'client_endpoint':f"{cfg['host']}:{cfg['port']}",'read_only':True,'objects':{},'captured_by':'tools/data_dictionary/db_catalog.py'}
            for key,sql in [('relations',RELATIONS),('columns',COLUMNS),('constraints',CONSTRAINTS),('indexes',INDEXES),('routines',ROUTINES),('triggers',TRIGGERS),('enum_values',ENUMS)]:
                cur.execute(sql); payload['objects'][key]=fetch_dicts(cur); print(f"Captured {len(payload['objects'][key])} {key}")
        write_json(out/'catalog.json',payload)
        parts=['-- Read-only evidence capture. Do not execute this file.','-- Generated by tools/data_dictionary/db_catalog.py.','']
        for obj in payload['objects']['relations']:
            if obj.get('definition'):
                parts.append(f"-- {obj['schema_name']}.{obj['object_name']} ({obj['object_type']})\n{obj['definition'].rstrip(';')};\n")
        for routine in payload['objects']['routines']:
            parts.append(f"-- {routine['schema_name']}.{routine['routine_name']} ({routine['routine_kind']})\n{routine['definition'].rstrip(';')};\n")
        for trigger in payload['objects']['triggers']:
            parts.append(f"-- Trigger {trigger['schema_name']}.{trigger['object_name']}.{trigger['trigger_name']}\n{trigger['definition']};\n")
        (out/'definitions.sql').write_text('\n'.join(parts),encoding='utf-8')
        if not args.skip_pg_dump:
            pg_dump=shutil.which('pg_dump')
            if not pg_dump:
                print('WARNING: pg_dump not found; catalog JSON and view/routine definitions were captured, but schema_only.sql was not produced.')
            else:
                dump_path=out/'schema_only.sql'; temp_path=out/'schema_only.sql.tmp'
                env=os.environ.copy()
                if cfg.get('password'):
                    env['PGPASSWORD']=cfg['password']
                cmd=[pg_dump,'--host',cfg['host'],'--port',str(cfg['port']),'--username',cfg['user'],
                     '--dbname',cfg['dbname'],'--no-password','--schema-only','--no-owner','--no-privileges','--file',str(temp_path)]
                try:
                    completed=subprocess.run(cmd,env=env,capture_output=True,text=True,timeout=args.pg_dump_timeout,check=False)
                    if completed.returncode==0:
                        temp_path.replace(dump_path)
                        print(f'Schema-only table/type/constraint DDL written to {dump_path}')
                    else:
                        temp_path.unlink(missing_ok=True)
                        print('WARNING: pg_dump failed; catalog capture remains available. Error: '+completed.stderr[-1500:])
                except subprocess.TimeoutExpired:
                    temp_path.unlink(missing_ok=True)
                    print(f'WARNING: schema-only pg_dump exceeded {args.pg_dump_timeout}s; catalog capture remains available.')
        print(f"Read-only catalog written to {out}")
    finally:
        conn.rollback(); conn.close()
    return 0
if __name__=='__main__': raise SystemExit(main())
