"""Audit dictionary coverage, placeholders, and SQL COMMENT metadata quality."""
from __future__ import annotations
import argparse,json,re
from collections import Counter
from pathlib import Path
from schema_inventory import parse_schema, compare
from common import write_json
TABLE_RE=re.compile(r"^COMMENT\s+ON\s+TABLE\s+(.+?)\s+IS\s+'((?:''|[^'])*)'\s*;",re.I)
COLUMN_RE=re.compile(r"^COMMENT\s+ON\s+COLUMN\s+(.+?)\s+IS\s+'((?:''|[^'])*)'\s*;",re.I)

def migration_comments(path:Path|None):
    if not path or not path.exists(): return {'table_comments':[],'column_comments':[],'table_comment_statement_count':0,'table_comment_null_count':0,'column_comment_statement_count':0,'column_comment_null_count':0,'generic_count':0,'line_count':0}
    tables=[]; columns=[]; generic=0; table_statements=0; table_nulls=0; column_statements=0; column_nulls=0; lines=path.read_text(encoding='utf-8-sig',errors='replace').splitlines()
    for n,line in enumerate(lines,1):
        stripped=line.strip()
        if re.match(r'^COMMENT\s+ON\s+TABLE\b',stripped,re.I):
            table_statements+=1
            if re.search(r'\s+IS\s+NULL\s*;\s*$',stripped,re.I): table_nulls+=1
        if re.match(r'^COMMENT\s+ON\s+COLUMN\b',stripped,re.I):
            column_statements+=1
            if re.search(r'\s+IS\s+NULL\s*;\s*$',stripped,re.I): column_nulls+=1
        for rx,target in ((TABLE_RE,tables),(COLUMN_RE,columns)):
            m=rx.match(stripped)
            if m:
                obj,comment=m.groups(); comment=comment.replace("''", "'")
                target.append({'object':obj,'comment':comment,'line':n})
                if re.fullmatch(r'(identifier|staging table|[a-z0-9_ ]+ value|count|timestamp \(utc\)|length in (meters|millimeters)|duration in (seconds|milliseconds))\.?',comment.strip(),re.I): generic+=1
    return {'table_comments':tables,'column_comments':columns,'table_comment_statement_count':table_statements,'table_comment_null_count':table_nulls,'column_comment_statement_count':column_statements,'column_comment_null_count':column_nulls,'generic_count':generic,'line_count':len(lines)}

def main()->int:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--schema-file',default='docs/data/schema.generated.md'); p.add_argument('--dictionary-file',default='docs/data/dictionary.md'); p.add_argument('--sql-file',default=''); p.add_argument('--out',default='docs/data/_evidence/dictionary_audit.json'); args=p.parse_args()
    coverage=compare(args.schema_file,args.dictionary_file); inv=parse_schema(args.schema_file); text=Path(args.dictionary_file).read_text(encoding='utf-8-sig',errors='replace')
    expected_cols={o['qualified_name']:{c['name'] for c in o['columns']} for o in inv['objects']}
    # The dictionary's optional explicit column table uses a first-column field name; avoid counting prose as coverage.
    covered={k:set() for k in expected_cols}; current=None
    for line in text.splitlines():
        m=re.match(r'^###\s+(.+?)\s+\((?:TABLE|VIEW|MATERIALIZED VIEW|FOREIGN TABLE|PARTITIONED TABLE)\)\s*$',line,re.I)
        if m: current=m.group(1).strip().replace('"',''); continue
        if current in covered and line.startswith('|'):
            cells=[x.strip().strip('`') for x in line.strip().strip('|').split('|')]
            if cells and cells[0] in expected_cols[current]: covered[current].add(cells[0])
    missing_columns={k:sorted(v-covered.get(k,set())) for k,v in expected_cols.items() if v-covered.get(k,set())}
    comments=migration_comments(Path(args.sql_file) if args.sql_file else None)
    report={'coverage':coverage,'column_coverage':{'documented_column_count':sum(map(len,covered.values())), 'expected_column_count':sum(map(len,expected_cols.values())), 'objects_with_missing_columns':len(missing_columns), 'missing_columns':missing_columns},'sql_comments':comments,
            'dictionary_line_count':len(text.splitlines()),'ready_for_completion':not any((coverage['schema_only'],coverage['dictionary_only'],coverage['duplicate_dictionary_headings'],coverage['schema_duplicates'],coverage['replacement_character_count'],missing_columns)) and coverage['placeholder_count']==0}
    write_json(args.out,report); print(json.dumps(report,indent=2,ensure_ascii=False)); return 0 if report['ready_for_completion'] else 1
if __name__=='__main__': raise SystemExit(main())
