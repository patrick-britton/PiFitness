"""Parse schema.generated.md and compare exact object/column coverage with dictionary.md."""
from __future__ import annotations
import argparse, json, re
from collections import Counter
from pathlib import Path
HEADER_RE = re.compile(r"^###\s+(.+?)\s+\((TABLE|VIEW|MATERIALIZED VIEW|FOREIGN TABLE|PARTITIONED TABLE)\)\s*$", re.I)
COLUMN_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*(YES|NO)\s*\|\s*$", re.I)


def parse_schema(path: str | Path) -> dict:
    lines = Path(path).read_text(encoding="utf-8-sig", errors="replace").splitlines()
    objects, current, section = [], None, None
    for n, line in enumerate(lines, 1):
        m = HEADER_RE.match(line)
        if m:
            raw, kind = m.groups()
            raw = raw.strip().replace('"', '')
            if "." not in raw:
                current = None
                continue
            schema, name = raw.split(".", 1)
            current = {"schema": schema, "name": name, "qualified_name": f"{schema}.{name}",
                       "kind": kind.upper(), "header_line": n, "columns": [], "foreign_keys": [], "indexes": []}
            objects.append(current); section = None
            continue
        if current is None:
            continue
        if line.startswith("**Foreign keys:"):
            section = "foreign_keys"; continue
        if line.startswith("**Indexes:"):
            section = "indexes"; continue
        if line.startswith("**"):
            section = None
        m = COLUMN_RE.match(line)
        if m and m.group(1).strip().lower() != "column":
            col, typ, nullable = m.groups()
            current["columns"].append({"name": col.strip(), "data_type": typ.strip(),
                                       "nullable": nullable.upper() == "YES", "line": n})
        elif section in {"foreign_keys", "indexes"} and line.startswith("-"):
            current[section].append(line[1:].strip())
    counts = Counter(o["qualified_name"] for o in objects)
    return {"source_file": str(path), "object_count": len(objects),
            "unique_object_count": len(counts), "column_count": sum(len(o["columns"]) for o in objects),
            "schema_count": len({o["schema"] for o in objects}),
            "schemas": sorted({o["schema"] for o in objects}),
            "duplicates": sorted(k for k, v in counts.items() if v > 1), "objects": objects}


def headings(path: str | Path) -> list[str]:
    out = []
    for line in Path(path).read_text(encoding="utf-8-sig", errors="replace").splitlines():
        m = HEADER_RE.match(line)
        if m:
            name = m.group(1).strip().replace('"', '')
            if "." in name: out.append(name)
    return out


def compare(schema_path: str | Path, dictionary_path: str | Path) -> dict:
    inv = parse_schema(schema_path); names = [o["qualified_name"] for o in inv["objects"]]
    actual = headings(dictionary_path); expected_set = set(names); actual_set = set(actual)
    text = Path(dictionary_path).read_text(encoding="utf-8-sig", errors="replace")
    counts = Counter(actual)
    return {"schema_object_count": len(expected_set), "schema_column_count": inv["column_count"],
            "dictionary_heading_count": len(actual), "dictionary_unique_heading_count": len(actual_set),
            "schema_only": sorted(expected_set - actual_set), "dictionary_only": sorted(actual_set - expected_set),
            "duplicate_dictionary_headings": {k:v for k,v in counts.items() if v > 1},
            "placeholder_count": sum(text.lower().count(s) for s in ("not yet individually catalogued", "investigation incomplete")),
            "replacement_character_count": text.count("\ufffd"),
            "schema_duplicates": inv["duplicates"]}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__); sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("inventory"); a.add_argument("--schema-file", default="docs/data/schema.generated.md"); a.add_argument("--out", default="docs/data/_evidence/schema_inventory.json")
    b = sub.add_parser("compare"); b.add_argument("--schema-file", default="docs/data/schema.generated.md"); b.add_argument("--dictionary-file", default="docs/data/dictionary.md"); b.add_argument("--out", default="")
    args = p.parse_args()
    if args.cmd == "inventory":
        result = parse_schema(args.schema_file); from common import write_json
        write_json(args.out, result); print(f"{result['object_count']} objects; {result['column_count']} columns; {result['schema_count']} schemas -> {args.out}")
        return 0 if not result["duplicates"] else 1
    result = compare(args.schema_file, args.dictionary_file); rendered = json.dumps(result, indent=2, ensure_ascii=False)
    if args.out:
        dest=Path(args.out); dest.parent.mkdir(parents=True, exist_ok=True); dest.write_text(rendered+"\n", encoding="utf-8")
    print(rendered)
    good = not any((result["schema_only"], result["dictionary_only"], result["duplicate_dictionary_headings"], result["schema_duplicates"])) and not result["replacement_character_count"]
    return 0 if good else 1
if __name__ == "__main__": raise SystemExit(main())
