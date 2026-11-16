#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lineage.py - build and query a data-lineage graph for a Python / Postgres / FastAPI / React repo.

BUILD (human-triggered; needs read access to the DEV database catalog):
    python tools/data_dictionary/lineage.py build [--dsn URL] [--schemas public,etl] [--no-db] [--include-tests]

QUERY (agents; no database or credentials needed, reads docs/lineage/graph.json):
    python tools/data_dictionary/lineage.py search <words...> [--type T] [-n 25]
    python tools/data_dictionary/lineage.py show   <node>
    python tools/data_dictionary/lineage.py trace  <node> [--up|--down] [--depth 6] [--files] [--flat] [--only t1,t2]
                                          [--kinds k1,k2] [--min-conf low|medium|high] [--max-lines 150]
    python tools/data_dictionary/lineage.py describe <node> [--direction both|up|down] [--depth 8]
                                           [--section identity,population,upstream,consumers,coverage]
                                           [--min-conf medium] [--format text|json] [--max-nodes 80]
    python tools/data_dictionary/lineage.py evidence <node> [--related-to NODE] [--operation INSERT,UPDATE]
                                           [--max-lines 240] [--max-items 12]
    python tools/data_dictionary/lineage.py check

GRAPH MODEL
    Every edge is  src --kind--> dst  and means "src DEPENDS ON dst".
      upstream   = what a node depends on (follow edges forward)  -> "where does this come from?"
      downstream = what depends on a node (follow edges backward) -> "what breaks / where is it shown?"
    Node ids look like  table:public.x  view:public.v  function:public.f  procedure:public.p
                        py:backend/app/x.py::Class.method  endpoint:GET /api/runs
                        ts:frontend/src/lib/api.ts::getRuns  component:...  page:/runs  source:garmin
    Edge kinds: reads, written_by, derives_from, calls, triggered_by, fk, handled_by, requests,
                pulls_from, renders, mentions
    Confidence: high (catalog / exact), ~ medium (resolved by name or path heuristics), ? low (guess).
"""
from __future__ import annotations

import argparse
import ast
import collections
import fnmatch
import functools
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv as _dotenv_load
except ImportError:  # keep the tool dependency-free if dotenv isn't installed
    _dotenv_load = None



OUT_DIR = "docs/lineage"
DYN = "__dyn__"
CONF = {"low": 1, "medium": 2, "high": 3}
MARK = {1: "?", 2: "~", 3: ""}
TS_EXT = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}

DEFAULT_CFG = {
    "exclude_dirs": ["node_modules", "venv", "env", "dist", "build", "__pycache__", "site-packages", "coverage"],
    "exclude_globs": ["docs/lineage/defs/**"],
    "schemas": None,  # None = every non-system schema
    "sources": {
        "garmin": {"imports": ["garminconnect", "garth", "garmin-connect"], "hosts": ["garmin.com"]},
        "spotify": {"imports": ["spotipy", "spotify-web-api-node", "@spotify/web-api-ts-sdk"], "hosts": ["spotify.com"]},
    },
}

# label shown when walking upstream (parent depends on child) / downstream (child depends on parent)
EDGE_UP = {"reads": "reads", "written_by": "written_by", "derives_from": "derives_from", "calls": "calls",
           "triggered_by": "triggered_by", "fk": "fk_to", "handled_by": "handled_by", "requests": "requests",
           "pulls_from": "pulls_from", "renders": "renders", "mentions": "mentions"}
EDGE_DOWN = {"reads": "read_by", "written_by": "writes", "derives_from": "feeds", "calls": "called_by",
             "triggered_by": "triggers", "fk": "referenced_by", "handled_by": "handles", "requests": "requested_by",
             "pulls_from": "feeds", "renders": "rendered_by", "mentions": "mentions"}
SYMMETRIC = {"mentions"}
KIND_ORDER = ["pulls_from", "written_by", "derives_from", "reads", "triggered_by", "calls", "handled_by",
              "requests", "renders", "mentions", "fk"]


def warn(msg: str) -> None:
    print(f"  ! {msg}", file=sys.stderr)


def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


# --------------------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------------------
class Graph:
    def __init__(self):
        self.nodes: dict = {}
        self.edges: dict = {}

    def node(self, nid, ntype, name, **kw):
        n = self.nodes.get(nid)
        if n is None:
            n = {"id": nid, "type": ntype, "name": name}
            self.nodes[nid] = n
        for k, v in kw.items():
            if v is not None and v != "" and v != []:
                n[k] = v
        return n

    def edge(self, src, dst, kind, conf="high", **attrs):
        if src == dst or src not in self.nodes or dst not in self.nodes:
            return
        c = CONF[conf] if isinstance(conf, str) else conf
        evidence = attrs.pop("evidence", None)
        key = (src, dst, kind)
        cur = self.edges.get(key)
        if cur is None:
            cur = {"conf": c, "attrs": {}}
            self.edges[key] = cur
        else:
            cur["conf"] = max(cur["conf"], c)
        cur["attrs"].update(attrs)
        if evidence:
            if isinstance(evidence, dict):
                evidence = [evidence]
            bucket = cur["attrs"].setdefault("evidence", [])
            seen = {json.dumps(x, sort_keys=True, default=str) for x in bucket}
            for item in evidence:
                key_text = json.dumps(item, sort_keys=True, default=str)
                if key_text not in seen:
                    bucket.append(item)
                    seen.add(key_text)

    def dump(self):
        out = []
        for (s, d, k), e in sorted(self.edges.items()):
            row = [s, d, k, e["conf"]]
            if e["attrs"]:
                row.append(e["attrs"])
            out.append(row)
        return out


class Registry:
    """Lookup of known DB relations and functions by (optionally schema-qualified) name."""

    def __init__(self):
        self.rels = collections.defaultdict(list)
        self.funcs = collections.defaultdict(list)

    @staticmethod
    def _add(d, schema, name, nid):
        name, schema = name.lower(), (schema or "").lower()
        d[name].append((schema, nid))
        if schema:
            d[f"{schema}.{name}"].append((schema, nid))

    def add_rel(self, schema, name, nid):
        self._add(self.rels, schema, name, nid)

    def add_func(self, schema, name, nid):
        self._add(self.funcs, schema, name, nid)

    @staticmethod
    def _find(d, parts):
        ids = d.get(".".join(parts[-2:]))
        if not ids:
            return []
        if len(parts) == 1:
            pub = [i for s, i in ids if s == "public"]
            if pub:
                return list(dict.fromkeys(pub))
            if len({s for s, _ in ids}) > 1:
                return [sorted(ids)[0][1]]
        return list(dict.fromkeys(i for _, i in ids))

    def find_rel(self, parts):
        return self._find(self.rels, parts)

    def find_func(self, parts):
        return self._find(self.funcs, parts)

    def rel_names(self):
        return set(self.rels.keys())


# --------------------------------------------------------------------------------------
# SQL reference extraction (used for function bodies and SQL embedded in Python)
# --------------------------------------------------------------------------------------
_SQL_SCAN = re.compile(
    r"--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'|\$(?P<tag>\w*)\$.*?\$(?P=tag)\$|\"(?:[^\"]|\"\")*\"", re.S)
_TOK = re.compile(
    r'''(?P<id>(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)(?:\s*\.\s*(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*))*)'''
    r'''|(?P<p>[(),;])|(?P<o>[^\sA-Za-z_"(),;]+)''')
_PART = re.compile(r'"((?:[^"]|"")*)"|([A-Za-z_][A-Za-z0-9_$]*)')
SQLISH = re.compile(
    r"^\s*(?:--[^\n]*\n\s*)*(?:SELECT|INSERT|UPDATE|DELETE|WITH|MERGE|CALL|REFRESH|TRUNCATE|COPY|EXPLAIN|PERFORM|"
    r"FROM|JOIN|LEFT|INNER|RIGHT|FULL|CROSS|INTO|USING)\b", re.I)

READ_KW = {"FROM", "JOIN", "USING"}
RESET_KW = {"WHERE", "GROUP", "ORDER", "HAVING", "SELECT", "ON", "SET", "VALUES", "RETURNING", "LIMIT", "UNION",
            "INTERSECT", "EXCEPT", "WINDOW", "OFFSET", "FETCH", "WITH", "WHEN", "THEN", "ELSE", "LOOP"}
FILL_READ = {"ONLY", "LATERAL"}
FILL_WRITE = {"TABLE", "ONLY", "MATERIALIZED", "VIEW", "CONCURRENTLY", "IF", "EXISTS"}
NOFROM_FN = {"EXTRACT", "SUBSTRING", "TRIM", "OVERLAY", "POSITION"}
NOT_UPDATE_PREV = {"DO", "FOR", "ON", "OR", "KEY", "OF"}


def split_sql(text: str):
    """Blank comments, replace string literals with 'S'. Returns (code, [string contents])."""
    strings: list = []

    def repl(m):
        s = m.group(0)
        if s.startswith("--") or s.startswith("/*"):
            # Blank comments without changing source line numbers.
            return "".join("\n" if ch == "\n" else " " for ch in s)
        if s[0] == '"':
            return s
        if s[0] == "'":
            strings.append(s[1:-1].replace("''", "'"))
        else:
            k = len(m.group("tag")) + 2
            strings.append(s[k:-k])
        # Keep the placeholder token but preserve any newlines inside the literal so
        # statement evidence still points at the original source lines.
        return "'S'" + ("\n" * s.count("\n"))

    return _SQL_SCAN.sub(repl, text), strings


def _parts(tok: str):
    return [(m.group(1).replace('""', '"') if m.group(1) is not None else m.group(2)).lower()
            for m in _PART.finditer(tok)]


def _operation_for_statement(statement: str, edge_kind: str) -> str:
    """Best-effort operation label for a SQL statement containing a lineage reference."""
    text = statement.strip()
    if edge_kind == "write":
        patterns = (
            (r"\bINSERT\s+INTO\b", "INSERT"),
            (r"\bUPDATE\b", "UPDATE"),
            (r"\bDELETE\s+FROM\b", "DELETE"),
            (r"\bTRUNCATE\b", "TRUNCATE"),
            (r"\bMERGE\s+INTO\b", "MERGE"),
            (r"\bCOPY\b", "COPY"),
            (r"\bREFRESH\b", "REFRESH"),
        )
        hits = [(re.search(rx, text, re.I), op) for rx, op in patterns]
        hits = [(m, op) for m, op in hits if m]
        if hits:
            m, op = min(hits, key=lambda x: x[0].start())
            if op == "COPY":
                tail = text[m.end():]
                return "COPY_FROM" if re.search(r"\bFROM\b", tail, re.I) else "COPY_TO"
            return op
        return "WRITE_UNKNOWN"
    if edge_kind == "read":
        if re.search(r"\bCOPY\b.*\bTO\b", text, re.I | re.S):
            return "COPY_TO"
        return "SELECT"
    if edge_kind == "call":
        if re.search(r"\bCALL\b", text, re.I):
            return "CALL"
        if re.search(r"\bPERFORM\b", text, re.I):
            return "PERFORM"
        return "FUNCTION_CALL"
    return edge_kind.upper()


def _operation_offset(statement: str, edge_kind: str) -> int:
    patterns = {
        "write": (r"\bINSERT\s+INTO\b", r"\bUPDATE\b", r"\bDELETE\s+FROM\b",
                  r"\bTRUNCATE\b", r"\bMERGE\s+INTO\b", r"\bCOPY\b", r"\bREFRESH\b"),
        "read": (r"\bSELECT\b", r"\bCOPY\b"),
        "call": (r"\bCALL\b", r"\bPERFORM\b"),
    }.get(edge_kind, ())
    hits = [m.start() for pattern in patterns if (m := re.search(pattern, statement, re.I))]
    return min(hits) if hits else 0


def extract_sql_evidence(text: str, reg: Registry, *, file=None, base_line=None,
                         mechanism="sql", confidence="medium") -> dict:
    """Return {(reference_kind, node_id): [evidence...]} for semicolon-delimited SQL chunks.

    This intentionally complements, rather than replaces, extract_refs(): the latter remains
    the authoritative whole-body reference extractor. Evidence is best-effort and is always
    accompanied by the graph edge's confidence level.
    """
    s2 = re.sub(r"%\(\w+\)s|%s|\{[^{}]*\}|\$\d+|(?<!:):[A-Za-z_]\w*", DYN, text)
    code, _ = split_sql(s2)
    out = collections.defaultdict(list)
    start = 0
    for match in list(re.finditer(r";", code)) + [None]:
        end = match.end() if match else len(code)
        chunk = code[start:end]
        if chunk.strip():
            refs, _, _ = extract_refs(chunk, reg)
            for kind, target in refs:
                op_offset = _operation_offset(chunk, kind)
                evidence_pos = start + op_offset
                start_line = (base_line + code.count("\n", 0, evidence_pos)) if base_line is not None else None
                end_line = (base_line + code.count("\n", 0, max(start, end - 1))) if base_line is not None else None
                statement = re.sub(r"\s+", " ", chunk.strip())[:280]
                item = {
                    "operation": _operation_for_statement(chunk, kind),
                    "mechanism": mechanism,
                    "confidence": confidence,
                    "statement": statement,
                }
                if file:
                    item["file"] = file
                if start_line is not None:
                    item["line"] = start_line
                if end_line is not None:
                    item["end"] = max(start_line, end_line)
                out[(kind, target)].append(item)
        start = end
    return dict(out)


def extract_refs(code: str, reg: Registry):
    """Tokenise SQL/plpgsql code. Returns (refs, unknown_relations, dynamic).
    refs is a set of ('read'|'write'|'call', node_id)."""
    toks = []
    for m in _TOK.finditer(code):
        if m.group("id"):
            raw = m.group("id")
            parts = _parts(raw)
            simple = len(parts) == 1 and not raw.startswith('"')
            toks.append(("id", raw, parts, parts[0].upper() if simple else ""))
        elif m.group("p"):
            toks.append(("p", m.group("p"), None, ""))
    n = len(toks)
    ctes = set()
    for i in range(1, n - 2):
        k, _, parts, up = toks[i]
        if k == "id" and len(parts) == 1 and toks[i + 1][3] == "AS":
            j = i + 2
            while j < n and toks[j][3] in ("MATERIALIZED", "NOT"):
                j += 1
            pk, pv, _, pup = toks[i - 1]
            if j < n and toks[j][:2] == ("p", "(") and (pup in ("WITH", "RECURSIVE") or (pk == "p" and pv == ",")):
                ctes.add(parts[0])

    refs, unknown = set(), set()
    dynamic = False
    expect = lst = None
    stack: list = []
    nofrom = in_copy = False
    prev = ""
    for i, (kind, val, parts, up) in enumerate(toks):
        if kind == "p":
            if val == "(":
                stack.append((lst, nofrom))
                nofrom = prev in NOFROM_FN
                if prev == "COPY":
                    in_copy = False
                expect = lst = None
            elif val == ")":
                if stack:
                    lst, nofrom = stack.pop()
                expect = None
            elif val == ",":
                if lst:
                    expect = lst
            else:  # ;
                expect = lst = None
                stack.clear()
                nofrom = in_copy = False
            prev = val
            continue

        nxt = toks[i + 1] if i + 1 < n else None
        is_call = nxt is not None and nxt[0] == "p" and nxt[1] == "("
        if up:
            if up in READ_KW:
                if not (in_copy or nofrom or prev == "DISTINCT"):
                    if up == "FROM" and prev == "DELETE":
                        expect = "write"
                    else:
                        expect = "read"
                        if up != "JOIN":
                            lst = "read"
                prev = up
                continue
            if up == "INTO":
                expect = "write" if prev in ("INSERT", "MERGE") else None
                lst = None
                prev = up
                continue
            if up == "UPDATE":
                if prev not in NOT_UPDATE_PREV:
                    expect, lst = "write", None
                prev = up
                continue
            if up == "TRUNCATE":
                expect = lst = "write"
                prev = up
                continue
            if up == "REFRESH":
                expect, lst = "write", None
                prev = up
                continue
            if up == "COPY":
                in_copy = True
                is_from = True
                for t2 in toks[i + 1:]:
                    if t2[3] in ("FROM", "TO"):
                        is_from = t2[3] == "FROM"
                        break
                expect, lst = ("write" if is_from else "read"), None
                prev = up
                continue
            if expect and up in (FILL_WRITE if expect == "write" else FILL_READ):
                prev = up
                continue
            if up in RESET_KW:
                expect = lst = None
                prev = up
                continue

        if expect:
            if parts == [DYN]:
                dynamic = True
            else:
                ids = [] if (len(parts) == 1 and parts[0] in ctes) else reg.find_rel(parts)
                if ids:
                    for x in ids:
                        refs.add((expect, x))
                else:
                    fids = reg.find_func(parts) if is_call else []
                    if fids:
                        for x in fids:
                            refs.add(("call", x))
                    elif not is_call and not (len(parts) == 1 and parts[0] in ctes) and up not in ("SELECT", "S") \
                            and not parts[0].startswith(("pg_", "information_schema")):
                        unknown.add(".".join(parts))
            expect = None
        elif is_call:
            for x in reg.find_func(parts):
                refs.add(("call", x))
        prev = up
    return refs, unknown, dynamic


# --------------------------------------------------------------------------------------
# File discovery
# --------------------------------------------------------------------------------------
def is_test(rel: str) -> bool:
    parts = rel.split("/")
    name = parts[-1]
    return (any(p in ("tests", "test", "__tests__") for p in parts[:-1])
            or name.startswith("test_") or name.endswith("_test.py") or name == "conftest.py"
            or re.search(r"\.(test|spec)\.[jt]sx?$", name) is not None)


def discover(root: Path, cfg: dict, include_tests: bool) -> dict:
    out = {"py": [], "ts": [], "sql": [], "tests_excluded": []}
    skip = set(cfg["exclude_dirs"])
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in skip and not d.startswith("."))
        for f in sorted(filenames):
            ext = os.path.splitext(f)[1].lower()
            kind = "py" if ext == ".py" else "sql" if ext == ".sql" else "ts" if ext in TS_EXT else None
            if not kind or f.endswith((".d.ts", ".min.js")):
                continue
            rel = (Path(dirpath) / f).relative_to(root).as_posix()
            if not include_tests and is_test(rel):
                out["tests_excluded"].append(rel)
                continue
            if any(fnmatch.fnmatch(rel, g) for g in cfg["exclude_globs"]):
                continue
            out[kind].append(rel)
    return out


# --------------------------------------------------------------------------------------
# Repo DDL locations (where a table/view/function is defined in the repo)
# --------------------------------------------------------------------------------------
CREATE_RE = re.compile(
    r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(MATERIALIZED\s+)?(TABLE|VIEW|FUNCTION|PROCEDURE)\s+(?:IF\s+NOT\s+EXISTS\s+)?'
    r'((?:"[^"]+"|\w+)(?:\s*\.\s*(?:"[^"]+"|\w+))?)', re.I)
_STMT_SCAN = re.compile(r"'(?:[^']|'')*'|\$(?P<tag>\w*)\$.*?\$(?P=tag)\$|;", re.S)


def _blank_comments(t: str) -> str:
    return re.sub(r"--[^\n]*|/\*.*?\*/", lambda m: re.sub(r"[^\n]", " ", m.group(0)), t, flags=re.S)


def scan_sql_defs(root: Path, files: list) -> dict:
    """(class, schema, name) -> [{file, line, end}] ; class is 'rel' or 'func'; schema may be ''."""
    out = collections.defaultdict(list)
    for rel in files:
        if not rel.endswith((".sql", ".py")):
            continue
        text = _blank_comments(read_text(root / rel))
        for m in CREATE_RE.finditer(text):
            parts = _parts(m.group(3))
            schema, name = (parts[-2] if len(parts) > 1 else ""), parts[-1]
            cls = "func" if m.group(2).upper() in ("FUNCTION", "PROCEDURE") else "rel"
            line = text.count("\n", 0, m.start()) + 1
            end = None
            if rel.endswith(".sql"):
                for sm in _STMT_SCAN.finditer(text, m.start()):
                    if sm.group(0) == ";":
                        end = text.count("\n", 0, sm.end()) + 1
                        break
            kind = "matview" if m.group(1) else m.group(2).lower()
            out[(cls, schema, name)].append({"file": rel, "line": line, "end": end, "kind": kind})
    return out


def attach_defs(g: Graph, defs: dict):
    for n in g.nodes.values():
        if n["type"] in ("table", "view", "matview", "function", "procedure") and "schema" in n:
            cls = "func" if n["type"] in ("function", "procedure") else "rel"
            nm = n["name"].split(".")[-1].split("(")[0].lower()
            found = defs.get((cls, n["schema"].lower(), nm), []) + defs.get((cls, "", nm), [])
            uniq = list({(d["file"], d["line"]): d for d in found}.values())
            if uniq:
                n["defined_in"] = sorted(uniq, key=lambda d: (d["file"], d["line"]))


# --------------------------------------------------------------------------------------
# Database catalog (read-only)
# --------------------------------------------------------------------------------------
def env_dsn() -> str:
    """Build a libpq conninfo from PG_* / PG* env vars; '' if none set.
    Supports the project's non-standard PG_HOST / PG_PORT / PG_DB / PG_USER names."""
    def get(*names):
        for n in names:
            v = os.environ.get(n)
            if v:
                return v
        return None
    def q(v):
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    pairs = [
        ("host",     get("PG_HOST", "PGHOST")),
        ("port",     get("PG_PORT", "PGPORT")),
        ("dbname",   get("PG_DB", "PGDATABASE")),
        ("user",     get("PG_USER", "PGUSER")),
        ("password", get("PG_PASSWORD", "PGPASSWORD")),
        ("sslmode",  get("PGSSLMODE", "PG_SSLMODE")),
    ]
    return " ".join(f"{k}={q(v)}" for k, v in pairs if v)


def connect(dsn: str):
    dsn = re.sub(r"^(postgres(?:ql)?)\+\w+://", r"\1://", dsn or "")
    try:
        import psycopg  # type: ignore
        return psycopg.connect(dsn)
    except ImportError:
        pass
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(dsn)
    except ImportError:
        raise SystemExit("Reading the database catalog needs psycopg or psycopg2 "
                         "(pip install \"psycopg[binary]\"), or run with --no-db.")


def _rows(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, list(params))
        return cur.fetchall() if cur.description else []


def _schema_clause(schemas, col):
    if schemas:
        return f"{col} = ANY(%s)", [list(schemas)]
    return f"{col} !~ '^(pg_|_timescaledb|timescaledb_|information_schema$|topology$|tiger)'", []


def _ext(classid, objid):
    return (f"NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = '{classid}'::regclass "
            f"AND d.objid = {objid} AND d.deptype = 'e')")


def _safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


def load_db(dsn, schemas, g: Graph, reg: Registry, out: Path, warnings: list) -> dict:
    conn = connect(dsn)
    info = {}
    try:
        _rows(conn, "SET TRANSACTION READ ONLY")
        db, ver = _rows(conn, "SELECT current_database(), current_setting('server_version')")[0]
        info = {"database": db, "server_version": ver}
        sch_rel, p_rel = _schema_clause(schemas, "n.nspname")
        sch_fn, p_fn = _schema_clause(schemas, "n.nspname")
        defs_dir = out / "defs"
        if defs_dir.exists():
            shutil.rmtree(defs_dir)
        defs_dir.mkdir(parents=True, exist_ok=True)

        rel_where = (f"c.relkind IN ('r','p','v','m') AND NOT c.relispartition AND {sch_rel} AND "
                     + _ext("pg_class", "c.oid"))
        rels = _rows(conn, f"""
            SELECT c.oid::bigint, n.nspname, c.relname, c.relkind::text, obj_description(c.oid, 'pg_class'),
                   CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid, true) END
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE {rel_where} ORDER BY 2, 3""", p_rel)
        cols = collections.defaultdict(list)
        for oid, an, at, ac in _rows(conn, f"""
            SELECT c.oid::bigint, a.attname, format_type(a.atttypid, a.atttypmod), col_description(c.oid, a.attnum)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
            WHERE {rel_where} ORDER BY c.oid, a.attnum""", p_rel):
            cols[oid].append([an, at, ac] if ac else [an, at])

        indexes = collections.defaultdict(list)
        index_sql = f"""
            SELECT c.oid::bigint, ic.relname, am.amname, i.indisprimary, i.indisunique,
                   pg_get_indexdef(i.indexrelid),
                   ARRAY(SELECT a.attname
                         FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         WHERE k.ord <= i.indnkeyatts AND k.attnum > 0 ORDER BY k.ord),
                   ARRAY(SELECT a.attname
                         FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         WHERE k.ord > i.indnkeyatts AND k.attnum > 0 ORDER BY k.ord)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index i ON i.indrelid = c.oid
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_am am ON am.oid = ic.relam
            WHERE {rel_where} AND i.indisvalid
            ORDER BY c.oid, ic.relname
        """
        for oid, idx_name, access_method, is_primary, is_unique, definition, key_cols, include_cols in _rows(conn, index_sql, p_rel):
            indexes[oid].append({
                "name": idx_name,
                "access_method": access_method,
                "is_primary": bool(is_primary),
                "is_unique": bool(is_unique),
                "columns": list(key_cols or []),
                "included_columns": list(include_cols or []),
                "definition": definition,
            })

        rel_by_oid = {}
        tmap = {"r": "table", "p": "table", "v": "view", "m": "matview"}
        for oid, sc, nm, kd, cm, vdef in rels:
            t = tmap[kd]
            nid = f"{t}:{sc}.{nm}"
            relation_indexes = indexes.get(oid, [])
            primary_key = next((ix["columns"] for ix in relation_indexes if ix["is_primary"]), [])
            node = g.node(nid, t, f"{sc}.{nm}", schema=sc, comment=cm, columns=cols.get(oid),
                          indexes=relation_indexes, primary_key=primary_key, origin="db")
            rel_by_oid[oid] = nid
            reg.add_rel(sc, nm, nid)
            if vdef:
                p = defs_dir / f"{t}.{_safe(sc)}.{_safe(nm)}.sql"
                head = "MATERIALIZED VIEW" if kd == "m" else "VIEW"
                p.write_text(f"-- live definition from pg_get_viewdef (comments from the original DDL are not kept)\n"
                             f"CREATE OR REPLACE {head} {sc}.{nm} AS\n{vdef}\n", encoding="utf-8")
                node["live_def"] = p.relative_to(out.parent).as_posix()

        fn_rows = _rows(conn, f"""
            SELECT p.oid::bigint, n.nspname, p.proname, p.prokind::text, pg_get_function_identity_arguments(p.oid),
                   l.lanname, p.prosrc, obj_description(p.oid, 'pg_proc'), pg_get_functiondef(p.oid)
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang
            WHERE p.prokind IN ('f','p') AND l.lanname NOT IN ('c','internal') AND {sch_fn}
              AND {_ext("pg_proc", "p.oid")} ORDER BY 2, 3, 5""", p_fn)
        counts = collections.Counter((r[1], r[2]) for r in fn_rows)
        fn_by_oid, bodies = {}, {}
        for oid, sc, nm, pk, args, lang, src, cm, fdef in fn_rows:
            t = "procedure" if pk == "p" else "function"
            nid = f"{t}:{sc}.{nm}" + (f"({args})" if counts[(sc, nm)] > 1 else "")
            node = g.node(nid, t, f"{sc}.{nm}", schema=sc, args=args, lang=lang, comment=cm, origin="db")
            fn_by_oid[oid] = nid
            reg.add_func(sc, nm, nid)
            suffix = "" if counts[(sc, nm)] == 1 else f".{hashlib.sha1(args.encode()).hexdigest()[:6]}"
            p = defs_dir / f"{t}.{_safe(sc)}.{_safe(nm)}{suffix}.sql"
            p.write_text(fdef + "\n", encoding="utf-8")
            node["live_def"] = p.relative_to(out.parent).as_posix()
            if lang in ("plpgsql", "sql"):
                body_text = src or fdef
                body_pos = fdef.find(body_text) if src else 0
                body_line = fdef[:body_pos].count("\n") + 1 if body_pos >= 0 else None
                bodies[nid] = {"text": body_text, "file": node.get("live_def"), "base_line": body_line}
            else:
                node["note"] = f"language {lang}: body not analysed"

        # function / procedure bodies
        rx_exec = re.compile(r"\bEXECUTE\b(?!\s+(?:FUNCTION|PROCEDURE))", re.I)
        for nid, body_info in bodies.items():
            body = body_info["text"]
            code, strs = split_sql(body)
            refs, unk, dyn = extract_refs(code, reg)
            evidence_by_ref = extract_sql_evidence(
                body, reg, file=body_info.get("file"), base_line=body_info.get("base_line"),
                mechanism="postgresql_routine", confidence="high")
            for s in strs:
                if SQLISH.match(s):
                    r2, _, d2 = extract_refs(split_sql(re.sub(r"%[IsL]|%\d*\$[IsL]", DYN, s))[0], reg)
                    for kd, x in r2:
                        if (kd, x) not in refs:
                            _emit_sql_edge(g, nid, kd, x, "low", evidence=[{
                                "operation": _operation_for_statement(s, kd),
                                "mechanism": "dynamic_sql_literal",
                                "confidence": "low",
                                "statement": re.sub(r"\s+", " ", s.strip())[:280],
                            }])
                    dyn = dyn or d2
            for kd, x in refs:
                _emit_sql_edge(g, nid, kd, x, "high", evidence=evidence_by_ref.get((kd, x), []))
            node = g.nodes[nid]
            if dyn or rx_exec.search(code):
                node["dynamic_sql"] = True
            if unk:
                node["unknown_rels"] = sorted(unk)[:10]

        # views / materialized views (authoritative: pg_depend on the rewrite rule)
        for vo, refcls, ro in _rows(conn, """
            SELECT r.ev_class::bigint, d.refclassid::regclass::text, d.refobjid::bigint
            FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid
            WHERE d.classid = 'pg_rewrite'::regclass AND d.refobjid <> r.ev_class
              AND d.refclassid IN ('pg_class'::regclass, 'pg_proc'::regclass)"""):
            src = rel_by_oid.get(vo)
            if not src:
                continue
            if refcls == "pg_class" and ro in rel_by_oid:
                g.edge(src, rel_by_oid[ro], "derives_from", "high", evidence={
                    "operation": "VIEW_DEPENDENCY", "mechanism": "postgresql_catalog_pg_depend",
                    "confidence": "high", "file": g.nodes[src].get("live_def")})
            elif refcls == "pg_proc" and ro in fn_by_oid:
                g.edge(src, fn_by_oid[ro], "calls", "high", evidence={
                    "operation": "VIEW_FUNCTION_DEPENDENCY", "mechanism": "postgresql_catalog_pg_depend",
                    "confidence": "high", "file": g.nodes[src].get("live_def")})

        for tgrel, tgfn, tgname in _rows(conn, "SELECT tgrelid::bigint, tgfoid::bigint, tgname FROM pg_trigger "
                                              "WHERE NOT tgisinternal"):
            if tgrel in rel_by_oid and tgfn in fn_by_oid:
                g.edge(fn_by_oid[tgfn], rel_by_oid[tgrel], "triggered_by", "high", trigger=tgname,
                       evidence={"operation": "TRIGGER", "mechanism": "postgresql_catalog",
                                 "confidence": "high", "statement": tgname})
        for child, parent in _rows(conn, "SELECT conrelid::bigint, confrelid::bigint FROM pg_constraint "
                                         "WHERE contype = 'f'"):
            if child in rel_by_oid and parent in rel_by_oid:
                g.edge(rel_by_oid[child], rel_by_oid[parent], "fk", "high")

        if _rows(conn, "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"):
            try:
                for vs, vn, vdef in _rows(conn, "SELECT view_schema, view_name, view_definition "
                                                "FROM timescaledb_information.continuous_aggregates"):
                    vid = f"view:{vs}.{vn}"
                    if vid in g.nodes:
                        g.nodes[vid]["note"] = "TimescaleDB continuous aggregate"
                        refs, _, _ = extract_refs(split_sql(vdef or "")[0], reg)
                        for kd, x in refs:
                            if kd == "read":
                                g.edge(vid, x, "derives_from", "medium", evidence={
                                    "operation": "CONTINUOUS_AGGREGATE_DEPENDENCY",
                                    "mechanism": "timescaledb_catalog", "confidence": "medium"})
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                warnings.append(f"continuous aggregate lookup failed: {e}")
        blob = "".join(sorted(f"{r[1]}.{r[2]}({r[4]}){r[8] or ''}" for r in fn_rows))
        blob += "".join(sorted(f"{r[1]}.{r[2]}{r[5] or ''}" for r in rels))
        info["fingerprint"] = hashlib.sha1(blob.encode()).hexdigest()[:12]
    finally:
        conn.rollback()
        conn.close()
    return info


def _emit_sql_edge(g: Graph, owner: str, kind: str, target: str, conf: str, evidence=None):
    if kind == "read":
        g.edge(owner, target, "reads", conf, evidence=evidence)
    elif kind == "write":
        g.edge(target, owner, "written_by", conf, evidence=evidence)
    elif kind == "call":
        g.edge(owner, target, "calls", conf, evidence=evidence)


def names_from_defs(g: Graph, reg: Registry, defs: dict):
    """--no-db mode: register objects declared in repo DDL (names only, no body analysis)."""
    for (cls, sc, nm), locs in defs.items():
        sc = sc or "public"
        if cls == "rel":
            kind = locs[-1].get("kind", "table")
            nid = f"{kind}:{sc}.{nm}"
            g.node(nid, kind, f"{sc}.{nm}", schema=sc, origin="repo")
            reg.add_rel(sc, nm, nid)
        else:
            object_kind = locs[-1].get("kind", "function")
            node_type = "procedure" if object_kind == "procedure" else "function"
            nid = f"{node_type}:{sc}.{nm}"
            g.node(nid, node_type, f"{sc}.{nm}", schema=sc, origin="repo")
            reg.add_func(sc, nm, nid)


# --------------------------------------------------------------------------------------
# Python analysis (stdlib ast)
# --------------------------------------------------------------------------------------
COMMON_METHODS = {"execute", "executemany", "fetchone", "fetchall", "fetchmany", "commit", "rollback", "close",
                  "append", "extend", "update", "items", "keys", "values", "format", "split", "strip", "join",
                  "replace", "write", "read", "start", "stop", "model_dump", "dict", "json", "cursor", "connect",
                  "setdefault", "remove", "insert", "pop", "sort", "copy", "clear", "count", "index", "encode",
                  "decode", "lower", "upper", "startswith", "endswith", "isoformat", "strftime", "strptime"}
HTTP_METHODS = {"get", "post", "put", "delete", "patch", "head", "options"}


def flatten(n):
    """String value of a str constant / f-string / concatenation (unknown parts -> DYN), else None."""
    if isinstance(n, ast.Constant):
        return n.value if isinstance(n.value, str) else None
    if isinstance(n, ast.JoinedStr):
        return "".join(str(v.value) if isinstance(v, ast.Constant) else DYN for v in n.values)
    if isinstance(n, ast.BinOp):
        if isinstance(n.op, ast.Add):
            left, right = flatten(n.left), flatten(n.right)
            if left is None and right is None:
                return None
            return (left if left is not None else DYN) + (right if right is not None else DYN)
        if isinstance(n.op, ast.Mod):
            return flatten(n.left)
    return None


def collect_strings(node, out: list):
    s = flatten(node)
    if s is not None:
        out.append(s)
        return
    for c in ast.iter_child_nodes(node):
        collect_strings(c, out)


def const_str(n):
    return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None


def kw(call: ast.Call, name: str):
    for k in call.keywords:
        if k.arg == name:
            return k.value
    return None


def chain_of(n):
    parts = []
    while isinstance(n, ast.Attribute):
        parts.append(n.attr)
        n = n.value
    if isinstance(n, ast.Name):
        parts.append(n.id)
        return parts[::-1]
    return None


def join_path(*segs):
    s = "/".join(x.strip("/") for x in segs if x and x.strip("/"))
    return "/" + s


def analyze_python(root: Path, files: list, g: Graph, reg: Registry, cfg: dict, sql_files: list, warnings: list):
    src_by_import = {imp: s for s, v in cfg["sources"].items() for imp in v.get("imports", [])}
    hosts = [(s, h) for s, v in cfg["sources"].items() for h in v.get("hosts", [])]
    rel_names = reg.rel_names()
    sql_by_base = collections.defaultdict(list)
    for p in sql_files:
        sql_by_base[p.rsplit("/", 1)[-1].lower()].append(p)

    mods, full_idx = {}, {}
    suf_idx = collections.defaultdict(list)
    parse_errors = 0
    for rel in files:
        try:
            tree = ast.parse(read_text(root / rel), filename=rel)
        except (SyntaxError, ValueError) as e:
            parse_errors += 1
            warnings.append(f"python parse failed: {rel}: {e}")
            continue
        parts = rel[:-3].split("/")
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if not parts:
            continue
        mods[rel] = {"rel": rel, "tree": tree, "parts": parts, "imports": {}, "src_alias": {}, "defs": {},
                     "consts": {}, "classes": {}, "fnodes": [], "orm_models": {}}
        full_idx[".".join(parts)] = rel
        for k in range(len(parts)):
            suf_idx[".".join(parts[k:])].append(rel)

    def resolve_mod(dotted, frm):
        if not dotted:
            return None
        if dotted in full_idx:
            return full_idx[dotted]
        c = suf_idx.get(dotted)
        if not c:
            return None
        return c[0] if len(c) == 1 else max(c, key=lambda r: len(os.path.commonprefix([r, frm])))

    # ---- pass 1: imports, definitions, constants
    meth_idx = collections.defaultdict(list)
    for m in mods.values():
        rel = m["rel"]
        for node in ast.walk(m["tree"]):
            if isinstance(node, ast.Import):
                for a in node.names:
                    top = a.name.split(".")[0]
                    if a.asname:
                        m["imports"][a.asname] = {"k": "module", "mod": resolve_mod(a.name, rel), "top": top}
                        alias = a.asname
                    else:
                        m["imports"][top] = {"k": "dotted", "mod": resolve_mod(top, rel), "top": top}
                        alias = top
                    if top in src_by_import:
                        m["src_alias"][alias] = src_by_import[top]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    pkg = m["parts"][:] if rel.endswith("__init__.py") else m["parts"][:-1]
                    if node.level - 1 > len(pkg):
                        continue
                    pkg = pkg[:len(pkg) - (node.level - 1)]
                    base = ".".join(pkg + (node.module.split(".") if node.module else []))
                else:
                    base = node.module or ""
                top = base.split(".")[0]
                for a in node.names:
                    if a.name == "*":
                        continue
                    alias = a.asname or a.name
                    sub = resolve_mod(f"{base}.{a.name}" if base else a.name, rel)
                    if sub:
                        m["imports"][alias] = {"k": "module", "mod": sub, "top": top}
                    else:
                        m["imports"][alias] = {"k": "name", "mod": resolve_mod(base, rel), "name": a.name, "top": top}
                    if top in src_by_import:
                        m["src_alias"][alias] = src_by_import[top]

        def add_def(fn, qual, cls):
            nid = f"py:{rel}::{qual}"
            g.node(nid, "py", qual, file=rel, line=fn.lineno, end=getattr(fn, "end_lineno", None),
                   doc=((ast.get_docstring(fn) or "").strip().split("\n")[0][:160] or None))
            m["defs"][qual] = nid
            m["fnodes"].append((fn, qual, cls, nid))
            if cls:
                m["classes"].setdefault(cls, []).append(qual)
                meth_idx[fn.name].append(nid)

        for st in m["tree"].body:
            if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                add_def(st, st.name, None)
            elif isinstance(st, ast.ClassDef):
                m["classes"].setdefault(st.name, [])
                for sub in st.body:
                    if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        add_def(sub, f"{st.name}.{sub.name}", st.name)
            if isinstance(st, (ast.Assign, ast.AnnAssign)) and st.value is not None:
                targets = st.targets if isinstance(st, ast.Assign) else [st.target]
                leaves: list = []
                collect_strings(st.value, leaves)
                if leaves:
                    for t in targets:
                        if isinstance(t, ast.Name):
                            m["consts"][t.id] = leaves

    # ---- ORM model discovery (conservative, explicit __tablename__ mappings only)
    # This supplements SQL parsing for common SQLAlchemy/SQLModel patterns. It does not
    # attempt runtime type inference or infer relationships from arbitrary object access.
    def class_table_decl(class_node):
        table_name, schema_name = None, None
        for st in class_node.body:
            value = None
            targets = []
            if isinstance(st, ast.Assign):
                targets, value = st.targets, st.value
            elif isinstance(st, ast.AnnAssign):
                targets, value = [st.target], st.value
            for target in targets:
                if not isinstance(target, ast.Name) or value is None:
                    continue
                if target.id == "__tablename__":
                    table_name = const_str(value)
                elif target.id == "__table_args__":
                    candidates = [value]
                    if isinstance(value, (ast.Tuple, ast.List)):
                        candidates.extend(value.elts)
                    for candidate in candidates:
                        if isinstance(candidate, ast.Dict):
                            for key, val in zip(candidate.keys, candidate.values):
                                if const_str(key) == "schema":
                                    schema_name = const_str(val)
        return table_name, schema_name

    orm_model_names = collections.defaultdict(dict)
    for rel, module in mods.items():
        for st in module["tree"].body:
            if not isinstance(st, ast.ClassDef):
                continue
            table_name, schema_name = class_table_decl(st)
            if not table_name:
                continue
            candidates = reg.find_rel(([schema_name] if schema_name else []) + [table_name])
            if len(candidates) == 1:
                table_id = candidates[0]
            elif len(candidates) > 1:
                # Avoid guessing when multiple schemas contain the same unqualified table.
                continue
            else:
                inferred_schema = schema_name or "public"
                table_id = f"table:{inferred_schema}.{table_name}"
                g.node(table_id, "table", f"{inferred_schema}.{table_name}", schema=inferred_schema,
                       origin="orm_model", note="Inferred from an explicit ORM __tablename__; verify against the database catalog")
                reg.add_rel(inferred_schema, table_name, table_id)
            module["orm_models"][st.name] = table_id
            orm_model_names[rel][st.name] = table_id

    def resolve_model_expr(module, expr):
        if isinstance(expr, ast.Name):
            if expr.id in module["orm_models"]:
                return module["orm_models"][expr.id]
            imp = module["imports"].get(expr.id)
            if imp and imp.get("k") == "name" and imp.get("mod") in mods:
                return mods[imp["mod"]]["orm_models"].get(imp.get("name"))
            return None
        parts = chain_of(expr)
        if not parts or len(parts) < 2:
            return None
        imp = module["imports"].get(parts[0])
        if imp and imp.get("k") == "module" and imp.get("mod") in mods:
            return mods[imp["mod"]]["orm_models"].get(parts[1])
        if imp and imp.get("k") == "dotted":
            target_mod = resolve_mod(".".join(parts[:-1]), module["rel"])
            if target_mod in mods:
                return mods[target_mod]["orm_models"].get(parts[-1])
        return None

    # ---- SQL string analysis (cached)
    @functools.lru_cache(maxsize=None)
    def sql_refs(s: str):
        s2 = re.sub(r"%\(\w+\)s|%s|\{[^{}]*\}|\$\d+|(?<!:):[A-Za-z_]\w*", DYN, s)
        refs, unk, dyn = extract_refs(split_sql(s2)[0], reg)
        return tuple(refs), tuple(unk), dyn

    @functools.lru_cache(maxsize=None)
    def sql_file_text(rel):
        return read_text(root / rel)

    def resolve_chain(m, parts, cls):
        def lookup_in(tm, rest):
            if len(rest) == 1:
                nm = rest[0]
                if nm in tm["defs"]:
                    return tm["defs"][nm], None
                if nm in tm["consts"]:
                    return None, tm["consts"][nm]
                if f"{nm}.__init__" in tm["defs"]:
                    return tm["defs"][f"{nm}.__init__"], None
            elif len(rest) == 2 and ".".join(rest) in tm["defs"]:
                return tm["defs"][".".join(rest)], None
            return None, None

        head = parts[0]
        if head in ("self", "cls") and cls and len(parts) == 2:
            return m["defs"].get(f"{cls}.{parts[1]}"), None
        if len(parts) == 1:
            if head in m["defs"]:
                return m["defs"][head], None
            if head in m["consts"]:
                return None, m["consts"][head]
            if f"{head}.__init__" in m["defs"]:
                return m["defs"][f"{head}.__init__"], None
        elif head in m["classes"] and len(parts) == 2:
            return m["defs"].get(".".join(parts)), None
        imp = m["imports"].get(head)
        if not imp:
            return None, None
        if imp["k"] == "name" and imp["mod"] in mods:
            return lookup_in(mods[imp["mod"]], [imp["name"]] + parts[1:])
        if len(parts) > 1:
            if imp["k"] == "module" and imp["mod"] in mods:
                return lookup_in(mods[imp["mod"]], parts[1:])
            if imp["k"] == "dotted":
                for j in range(len(parts) - 1, 0, -1):
                    tmod = resolve_mod(".".join(parts[:j]), m["rel"])
                    if tmod in mods:
                        return lookup_in(mods[tmod], parts[j:])
        return None, None

    def analyze(m, nodes, cls, self_id):
        ctx = {"reads": {}, "writes": {}, "calls": {}, "mentions": set(), "sources": {}, "dynamic": False,
               "unknown": set(), "evidence": collections.defaultdict(list)}
        skip: set = set()
        local_models = {}
        for root_node in nodes:
            for candidate in ast.walk(root_node):
                if not isinstance(candidate, (ast.Assign, ast.AnnAssign)) or candidate.value is None:
                    continue
                value = candidate.value
                if not isinstance(value, ast.Call):
                    continue
                table_id = resolve_model_expr(m, value.func)
                if not table_id:
                    continue
                targets = candidate.targets if isinstance(candidate, ast.Assign) else [candidate.target]
                for target in targets:
                    if isinstance(target, ast.Name):
                        local_models[target.id] = table_id

        def put(d, k, c):
            d[k] = max(d.get(k, 0), c)

        def on_string(s, source_node=None):
            if SQLISH.match(s):
                refs, unk, dyn = sql_refs(s)
                for kd, x in refs:
                    put({"read": ctx["reads"], "write": ctx["writes"], "call": ctx["calls"]}[kd], x, 2)
                line = getattr(source_node, "lineno", None)
                evidence_map = extract_sql_evidence(
                    s, reg, file=m["rel"], base_line=line, mechanism="python_embedded_sql", confidence="medium")
                for key, items in evidence_map.items():
                    ctx["evidence"][key].extend(items)
                ctx["unknown"].update(unk)
                ctx["dynamic"] = ctx["dynamic"] or dyn
                return
            st = s.strip()
            if st.lower() in rel_names:
                ctx["mentions"].add(reg.find_rel(st.lower().split("."))[0] if reg.find_rel(st.lower().split(".")) else None)
            elif st.lower().endswith(".sql"):
                cands = sql_by_base.get(st.replace("\\", "/").rsplit("/", 1)[-1].lower(), [])
                if len(cands) == 1:
                    file_sql = sql_file_text(cands[0])
                    refs, unk, dyn = sql_refs(file_sql)
                    for kd, x in refs:
                        put({"read": ctx["reads"], "write": ctx["writes"], "call": ctx["calls"]}[kd], x, 2)
                    evidence_map = extract_sql_evidence(
                        file_sql, reg, file=cands[0], base_line=1,
                        mechanism="python_referenced_sql_file", confidence="medium")
                    for key, items in evidence_map.items():
                        ctx["evidence"][key].extend(items)
                    ctx["dynamic"] = ctx["dynamic"] or dyn
            if " " not in st:
                low = st.lower()
                for sname, h in hosts:
                    if h in low:
                        put(ctx["sources"], sname, 2)

        def on_ref(parts, call_node=None, source_node=None):
            if parts[0] in m["src_alias"]:
                put(ctx["sources"], m["src_alias"][parts[0]], 3)
            fid, strs = resolve_chain(m, parts, cls)
            if fid and fid != self_id:
                put(ctx["calls"], fid, 3)
                if source_node is not None:
                    try:
                        statement = ast.unparse(source_node)[:280]
                    except Exception:  # noqa: BLE001
                        statement = "Python function reference"
                    ctx["evidence"][("call", fid)].append({
                        "operation": "PYTHON_CALL", "mechanism": "python_static_analysis",
                        "confidence": "high", "file": m["rel"],
                        "line": getattr(source_node, "lineno", None),
                        "end": getattr(source_node, "end_lineno", None),
                        "statement": statement,
                    })
            for s in strs or []:
                on_string(s, source_node)
            return fid or strs

        def models_in_expr(expr):
            found = set()
            if expr is None:
                return found
            direct = resolve_model_expr(m, expr)
            if direct:
                found.add(direct)
            if isinstance(expr, ast.Name) and expr.id in local_models:
                found.add(local_models[expr.id])
            for item in ast.walk(expr):
                if isinstance(item, ast.Call):
                    table_id = resolve_model_expr(m, item.func)
                    if table_id:
                        found.add(table_id)
                elif isinstance(item, ast.Name) and item.id in local_models:
                    found.add(local_models[item.id])
            return found

        def add_orm_evidence(table_id, operation, call_node):
            is_read = operation == "ORM_READ"
            put(ctx["reads"] if is_read else ctx["writes"], table_id, 2)
            try:
                statement = ast.unparse(call_node)[:280]
            except Exception:  # noqa: BLE001
                statement = ast.dump(call_node, include_attributes=False)[:280]
            ctx["evidence"][("read" if is_read else "write", table_id)].append({
                "operation": operation,
                "mechanism": "python_orm_static_analysis",
                "confidence": "medium",
                "file": m["rel"],
                "line": getattr(call_node, "lineno", None),
                "end": getattr(call_node, "end_lineno", None),
                "statement": statement,
            })

        def visit_stringy(n):
            if isinstance(n, ast.JoinedStr):
                for v in n.values:
                    if isinstance(v, ast.FormattedValue):
                        visit(v.value)
            elif isinstance(n, ast.BinOp):
                for ch in (n.left, n.right):
                    if flatten(ch) is None:
                        visit(ch)
                    elif isinstance(ch, (ast.BinOp, ast.JoinedStr)):
                        visit_stringy(ch)

        def on_call(n):
            f = n.func
            if isinstance(f, ast.Attribute):
                nm = f.attr
                if nm in ("to_sql", "callproc", "copy_from", "copy_to"):
                    arg = const_str(n.args[0]) if n.args else const_str(kw(n, "name"))
                    if nm == "copy_from" and len(n.args) > 1:
                        arg = const_str(n.args[1])
                    if arg:
                        low = arg.lower().split(".")
                        if nm == "callproc":
                            for x in reg.find_func(low):
                                put(ctx["calls"], x, 2)
                                ctx["evidence"][("call", x)].append({
                                    "operation": "PYTHON_CALLPROC", "mechanism": "python_database_api",
                                    "confidence": "medium", "file": m["rel"],
                                    "line": getattr(n, "lineno", None), "end": getattr(n, "end_lineno", None),
                                    "statement": ast.unparse(n)[:280] if hasattr(ast, "unparse") else "callproc",
                                })
                        else:
                            for x in reg.find_rel(low):
                                is_read = nm == "copy_to"
                                put(ctx["reads"] if is_read else ctx["writes"], x, 2)
                                ctx["evidence"][("read" if is_read else "write", x)].append({
                                    "operation": {"to_sql": "PANDAS_TO_SQL", "copy_from": "COPY_FROM", "copy_to": "COPY_TO"}.get(nm, nm.upper()),
                                    "mechanism": "python_database_api", "confidence": "medium",
                                    "file": m["rel"], "line": getattr(n, "lineno", None),
                                    "end": getattr(n, "end_lineno", None),
                                    "statement": ast.unparse(n)[:280] if hasattr(ast, "unparse") else nm,
                                })
                ch = chain_of(f)
                if ch:
                    skip.add(id(f))
                    res = on_ref(ch, source_node=n)
                    head_imported = ch[0] in m["imports"]
                else:
                    res, head_imported = None, False
                if not res and not head_imported and len(nm) >= 5 and nm not in COMMON_METHODS \
                        and not nm.startswith("__"):
                    cand = meth_idx.get(nm, [])
                    if len(cand) == 1 and cand[0] != self_id:
                        put(ctx["calls"], cand[0], 1)
                # Explicit, common SQLAlchemy / SQLModel patterns. These are heuristic medium-
                # confidence edges; arbitrary ORM expression composition is not fully resolved.
                if nm in {"query", "get"}:
                    for arg in n.args[:1]:
                        for table_id in models_in_expr(arg):
                            add_orm_evidence(table_id, "ORM_READ", n)
                elif nm in {"add", "add_all", "merge", "bulk_save_objects", "bulk_insert_mappings", "bulk_update_mappings"}:
                    for arg in n.args[:1]:
                        for table_id in models_in_expr(arg):
                            operation = {
                                "merge": "ORM_MERGE",
                                "bulk_insert_mappings": "ORM_INSERT",
                                "bulk_update_mappings": "ORM_UPDATE",
                            }.get(nm, "ORM_ADD (insert/update depends on ORM state)")
                            add_orm_evidence(table_id, operation, n)
                elif nm in {"delete"}:
                    for arg in n.args[:1]:
                        for table_id in models_in_expr(arg):
                            add_orm_evidence(table_id, "ORM_DELETE", n)
            elif isinstance(f, ast.Name):
                nm = f.id.lower()
                operation = {"select": "ORM_READ", "update": "ORM_UPDATE", "insert": "ORM_INSERT", "delete": "ORM_DELETE"}.get(nm)
                if operation:
                    for arg in n.args[:1]:
                        for table_id in models_in_expr(arg):
                            add_orm_evidence(table_id, operation, n)

        def visit(n):
            if isinstance(n, (ast.Constant, ast.JoinedStr, ast.BinOp)):
                s = flatten(n)
                if s is not None:
                    on_string(s, n)
                    visit_stringy(n)
                    return
                if isinstance(n, ast.Constant):
                    return
            if isinstance(n, ast.Call):
                on_call(n)
            elif isinstance(n, ast.Name):
                if isinstance(n.ctx, ast.Load):
                    on_ref([n.id], source_node=n)
            elif isinstance(n, ast.Attribute):
                if id(n) not in skip:
                    ch = chain_of(n)
                    if ch:
                        on_ref(ch, source_node=n)
            for c in ast.iter_child_nodes(n):
                visit(c)

        for nd in nodes:
            visit(nd)
        ctx["mentions"].discard(None)
        return ctx

    def emit(nid, ctx):
        node = g.nodes[nid]
        for x, c in ctx["reads"].items():
            g.edge(nid, x, "reads", c, evidence=ctx["evidence"].get(("read", x)))
        for x, c in ctx["writes"].items():
            g.edge(x, nid, "written_by", c, evidence=ctx["evidence"].get(("write", x)))
        for x, c in ctx["calls"].items():
            g.edge(nid, x, "calls", c, evidence=ctx["evidence"].get(("call", x)))
        for x in ctx["mentions"]:
            if x not in ctx["reads"] and x not in ctx["writes"] and x not in ctx["calls"]:
                g.edge(nid, x, "mentions", "low")
        for s, c in ctx["sources"].items():
            g.node(f"source:{s}", "source", s)
            g.edge(nid, f"source:{s}", "pulls_from", c)
        if ctx["dynamic"]:
            node["dynamic_sql"] = True
        if ctx["unknown"]:
            node["unknown_rels"] = sorted(ctx["unknown"])[:10]

    # ---- pass 2: per-function analysis
    routes = []
    class_src: dict = {}
    for m in mods.values():
        for fn, qual, cls, nid in m["fnodes"]:
            body = list(fn.body)
            if body and isinstance(body[0], ast.Expr) and const_str(body[0].value) is not None:
                body = body[1:]
            ctx = analyze(m, body + list(fn.args.defaults) + [d for d in fn.decorator_list], cls, nid)
            emit(nid, ctx)
            if cls and ctx["sources"]:
                class_src.setdefault((m["rel"], cls), {}).update(ctx["sources"])
            for dec in fn.decorator_list:
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute) \
                        and isinstance(dec.func.value, ast.Name) and (dec.func.attr in HTTP_METHODS or dec.func.attr == "api_route"):
                    path = const_str(dec.args[0]) if dec.args else const_str(kw(dec, "path"))
                    if path is None:
                        continue
                    if dec.func.attr == "api_route":
                        ms = kw(dec, "methods")
                        methods = [const_str(e).upper() for e in getattr(ms, "elts", []) if const_str(e)] or ["GET"]
                    else:
                        methods = [dec.func.attr.upper()]
                    routes.append((m["rel"], dec.func.value.id, methods, path, nid))
        # same-class fallback for source clients: sibling public methods get a low-confidence link
        # (module-level pseudo function below)
        top = [st for st in m["tree"].body if not isinstance(
            st, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom))
            and not (isinstance(st, (ast.Assign, ast.AnnAssign)) and isinstance(getattr(st, "value", None),
                                                                            (ast.Constant, ast.Dict, ast.List)))]
        if top:
            ctx = analyze(m, top, None, None)
            if ctx["calls"] or ctx["reads"] or ctx["writes"] or ctx["sources"]:
                nid = f"py:{m['rel']}::<module>"
                g.node(nid, "py", f"<module {m['rel'].rsplit('/', 1)[-1]}>", file=m["rel"], line=1,
                       doc="module-level statements (scripts, app wiring, scheduled jobs)")
                emit(nid, ctx)
    for (rel, cls), srcs in class_src.items():
        for qual in mods[rel]["classes"].get(cls, []):
            nid = mods[rel]["defs"][qual]
            if qual.split(".")[-1].startswith("_"):
                continue
            for s, _ in srcs.items():
                if (nid, f"source:{s}", "pulls_from") not in g.edges:
                    g.edge(nid, f"source:{s}", "pulls_from", "low", via="same class as a source client")

    # ---- FastAPI routes
    routers, includes = {}, []
    for m in mods.values():
        for st in m["tree"].body:
            tgt = st.targets[0] if isinstance(st, ast.Assign) and st.targets else getattr(st, "target", None)
            val = getattr(st, "value", None)
            if isinstance(tgt, ast.Name) and isinstance(val, ast.Call):
                fname = val.func.attr if isinstance(val.func, ast.Attribute) else getattr(val.func, "id", "")
                if fname == "APIRouter":
                    routers[(m["rel"], tgt.id)] = const_str(kw(val, "prefix")) or ""
                elif fname == "FastAPI":
                    routers[(m["rel"], tgt.id)] = ""
        for node in ast.walk(m["tree"]):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "include_router":
                child = node.args[0] if node.args else kw(node, "router")
                if child is not None:
                    includes.append((m["rel"], node.func.value, child, const_str(kw(node, "prefix")) or ""))

    def expr_ref(rel, expr):
        m = mods[rel]
        if isinstance(expr, ast.Name):
            imp = m["imports"].get(expr.id)
            if imp and imp["k"] == "name" and imp["mod"]:
                return (imp["mod"], imp["name"])
            return (rel, expr.id)
        if isinstance(expr, ast.Attribute) and isinstance(expr.value, ast.Name):
            imp = m["imports"].get(expr.value.id)
            if imp and imp["k"] == "module" and imp["mod"]:
                return (imp["mod"], expr.attr)
        return None

    inc_by_child = collections.defaultdict(list)
    for rel, parent, child, prefix in includes:
        c, p = expr_ref(rel, child), expr_ref(rel, parent)
        if c and p:
            inc_by_child[c].append((p, prefix))

    def total_prefix(key, seen=()):
        own = routers.get(key, "")
        for parent, inc in inc_by_child.get(key, [])[:1]:
            if parent in seen or parent == key:
                break
            return join_path(total_prefix(parent, seen + (key,)), inc, own) if (inc or own or True) else own
        return own

    for rel, var, methods, path, hid in routes:
        full = join_path(total_prefix((rel, var)), path)
        if full != "/" and full.endswith("/") and not path.endswith("/"):
            full = full.rstrip("/")
        h = g.nodes[hid]
        for method in methods:
            eid = f"endpoint:{method} {full}"
            g.node(eid, "endpoint", f"{method} {full}", file=h["file"], line=h["line"], end=h.get("end"),
                   route=full, method=method)
            g.edge(eid, hid, "handled_by", "high")
    return {"modules": len(mods), "routes": len(routes), "parse_errors": parse_errors,
            "orm_models_detected": sum(len(module.get("orm_models", {})) for module in mods.values())}


# --------------------------------------------------------------------------------------
# TypeScript / React analysis (parse-only, via the repo's own `typescript` package through node)
# --------------------------------------------------------------------------------------
TS_EXTRACT_JS = r'''
"use strict";
const fs = require("fs");
const path = require("path");
const [inPath, outPath] = process.argv.slice(2);
const input = JSON.parse(fs.readFileSync(inPath, "utf8"));
const root = input.root;
let ts = null;
for (const dir of [].concat(input.searchDirs || [], [root])) {
  try { ts = require(require.resolve("typescript", { paths: [dir] })); break; } catch (e) { /* next */ }
}
if (!ts) {
  fs.writeFileSync(outPath, JSON.stringify({ error: "typescript package not found (run npm install in the frontend folder)" }));
  process.exit(0);
}
const SK = ts.SyntaxKind;
const optCache = new Map();
function optionsFor(abs) {
  const dir = path.dirname(abs);
  const cfg = ts.findConfigFile(dir, ts.sys.fileExists, "tsconfig.json") || ts.findConfigFile(dir, ts.sys.fileExists, "jsconfig.json") || "";
  if (optCache.has(cfg)) return optCache.get(cfg);
  let opts = { allowJs: true };
  if (cfg) {
    const raw = ts.readConfigFile(cfg, ts.sys.readFile);
    if (!raw.error) {
      const parsed = ts.parseJsonConfigFileContent(raw.config, ts.sys, path.dirname(cfg));
      opts = Object.assign({}, parsed.options, { allowJs: true });
    }
  }
  optCache.set(cfg, opts);
  return opts;
}
function resolveSpec(spec, fromAbs) {
  try {
    const r = ts.resolveModuleName(spec, fromAbs, optionsFor(fromAbs), ts.sys).resolvedModule;
    if (!r || r.isExternalLibraryImport) return null;
    const rel = path.relative(root, r.resolvedFileName).split(path.sep).join("/");
    if (rel.startsWith("..") || rel.includes("node_modules/") || rel.endsWith(".d.ts")) return null;
    return rel;
  } catch (e) { return null; }
}
const BASE_RE = /url|base|host|origin|api|endpoint|server/i;
const HTTPISH = /^(fetch|axios|ky|got|useSWR|useQuery|useMutation|XMLHttpRequest)$/;
const HTTP_NAMES = new Set(["get", "post", "put", "patch", "delete"]);
const keepSubs = input.keep || [];
function hasMod(node, kind) {
  const mods = (ts.getModifiers ? ts.getModifiers(node) : node.modifiers) || [];
  return mods.some((m) => m.kind === kind);
}
function unwrap(n) {
  while (n && (ts.isParenthesizedExpression(n) || ts.isAsExpression(n) || ts.isNonNullExpression(n) ||
         (ts.isSatisfiesExpression && ts.isSatisfiesExpression(n)))) n = n.expression;
  return n;
}
const isFn = (n) => !!n && (ts.isArrowFunction(n) || ts.isFunctionExpression(n));
function calleeName(e) {
  if (ts.isIdentifier(e)) return e.text;
  if (ts.isPropertyAccessExpression(e)) return e.name.text;
  if (ts.isCallExpression(e)) return calleeName(e.expression);
  return "";
}
function analyzeNode(node, sf) {
  const idents = new Set(), members = new Set(), strs = new Set(), methods = new Set(), dyn = new Set();
  let http = false;
  const keepStr = (s) => /^(https?:)?\/\//.test(s) || s.startsWith("/") || keepSubs.some((k) => s.includes(k));
  function pathLike(a) {
    if (!a) return false;
    if (ts.isStringLiteralLike(a)) return a.text.startsWith("/") || /^https?:/.test(a.text);
    if (ts.isTemplateExpression(a)) {
      const h = a.head.text;
      return h.startsWith("/") || /^https?:/.test(h) || (h === "" && a.templateSpans.length > 0 && BASE_RE.test(a.templateSpans[0].expression.getText(sf)));
    }
    if (ts.isIdentifier(a)) return BASE_RE.test(a.text);
    return false;
  }
  function tmpl(t) {
    let out = t.head.text;
    t.templateSpans.forEach((sp, i) => {
      out += (i === 0 && out === "" && BASE_RE.test(sp.expression.getText(sf))) ? "" : "{}";
      out += sp.literal.text;
    });
    return out;
  }
  function visit(n) {
    if (ts.isTypeNode(n)) return;
    switch (n.kind) {
      case SK.PropertyAccessExpression:
        if (ts.isIdentifier(n.expression)) members.add(n.expression.text + "." + n.name.text);
        visit(n.expression);
        return;
      case SK.PropertyAssignment:
        if (ts.isIdentifier(n.name) && n.name.text === "method" && ts.isStringLiteralLike(n.initializer)) methods.add(n.initializer.text.toUpperCase());
        visit(n.initializer);
        return;
      case SK.JsxAttribute: if (n.initializer) visit(n.initializer); return;
      case SK.VariableDeclaration: case SK.Parameter: case SK.BindingElement:
        if (n.initializer) visit(n.initializer);
        return;
      case SK.Identifier: idents.add(n.text); return;
      case SK.StringLiteral: case SK.NoSubstitutionTemplateLiteral:
        if (keepStr(n.text)) strs.add(n.text);
        return;
      case SK.TemplateExpression: {
        const t = tmpl(n);
        if (keepStr(t)) strs.add(t);
        n.templateSpans.forEach((sp) => visit(sp.expression));
        return;
      }
      case SK.CallExpression: {
        const c = n.expression;
        if (c.kind === SK.ImportKeyword) {
          const a = n.arguments[0];
          if (a && ts.isStringLiteralLike(a)) { const r = resolveSpec(a.text, sf.fileName); if (r) dyn.add(r); }
        } else if (ts.isPropertyAccessExpression(c)) {
          const nm = c.name.text;
          if (HTTP_NAMES.has(nm) && pathLike(n.arguments[0])) { http = true; methods.add(nm.toUpperCase()); }
        } else if (ts.isIdentifier(c) && pathLike(n.arguments[0])) {
          http = true;
          const mm = c.text.toLowerCase().match(/(get|post|put|patch|delete)/);
          if (mm) methods.add(mm[1].toUpperCase());
        }
        break;
      }
    }
    ts.forEachChild(n, visit);
  }
  visit(node);
  for (const i of idents) if (HTTPISH.test(i)) http = true;
  return { idents: [...idents], members: [...members], strings: [...strs].slice(0, 80), methods: [...methods], http, dyn: [...dyn] };
}
const result = { files: {}, errors: [] };
for (const rel of input.files) {
  const abs = path.join(root, rel);
  try {
    const text = fs.readFileSync(abs, "utf8");
    const ext = path.extname(abs).toLowerCase();
    const kind = ext === ".tsx" ? ts.ScriptKind.TSX : ext === ".ts" ? ts.ScriptKind.TS : ext === ".jsx" ? ts.ScriptKind.JSX : ts.ScriptKind.JS;
    const sf = ts.createSourceFile(abs, text, ts.ScriptTarget.Latest, true, kind);
    const out = { imports: [], reexports: [], localExports: [], defaultExport: null, functions: [] };
    const line = (pos) => sf.getLineAndCharacterOfPosition(pos).line + 1;
    const addFn = (qual, node, declNode) => {
      const mods = declNode || node;
      out.functions.push(Object.assign({
        qual, line: line(node.getStart(sf)), end: line(node.getEnd()),
        exported: hasMod(mods, SK.ExportKeyword), isDefault: hasMod(mods, SK.DefaultKeyword),
      }, analyzeNode(node, sf)));
    };
    for (const st of sf.statements) {
      if (ts.isImportDeclaration(st) && ts.isStringLiteral(st.moduleSpecifier)) {
        const spec = st.moduleSpecifier.text;
        const rec = { spec, resolved: resolveSpec(spec, abs), default: null, ns: null, named: [] };
        const cl = st.importClause;
        if (cl) {
          if (cl.name) rec.default = cl.name.text;
          const nb = cl.namedBindings;
          if (nb) {
            if (ts.isNamespaceImport(nb)) rec.ns = nb.name.text;
            else for (const el of nb.elements) rec.named.push({ name: (el.propertyName || el.name).text, local: el.name.text });
          }
        }
        out.imports.push(rec);
      } else if (ts.isExportDeclaration(st)) {
        const names = st.exportClause && ts.isNamedExports(st.exportClause)
          ? st.exportClause.elements.map((el) => ({ name: (el.propertyName || el.name).text, as: el.name.text })) : [];
        if (st.moduleSpecifier && ts.isStringLiteral(st.moduleSpecifier)) {
          out.reexports.push({ spec: st.moduleSpecifier.text, resolved: resolveSpec(st.moduleSpecifier.text, abs), star: !st.exportClause, names });
        } else for (const nm of names) out.localExports.push(nm);
      } else if (ts.isExportAssignment(st)) {
        const e = unwrap(st.expression);
        if (ts.isIdentifier(e)) out.defaultExport = e.text;
        else if (ts.isCallExpression(e) && e.arguments.length && ts.isIdentifier(unwrap(e.arguments[0]))) out.defaultExport = unwrap(e.arguments[0]).text;
        else if (isFn(e) || ts.isCallExpression(e)) addFn("default", st, st), out.functions[out.functions.length - 1].isDefault = true;
      } else if (ts.isFunctionDeclaration(st)) {
        if (st.name) addFn(st.name.text, st, st);
        else if (hasMod(st, SK.DefaultKeyword)) addFn("default", st, st);
      } else if (ts.isClassDeclaration(st)) {
        const cn = st.name ? st.name.text : "default";
        addFn(cn, st, st);
        for (const mem of st.members) if (ts.isMethodDeclaration(mem) && ts.isIdentifier(mem.name)) addFn(cn + "." + mem.name.text, mem, mem);
      } else if (ts.isVariableStatement(st)) {
        for (const d of st.declarationList.declarations) {
          if (!ts.isIdentifier(d.name) || !d.initializer) continue;
          const nm = d.name.text, init = unwrap(d.initializer);
          if (isFn(init)) addFn(nm, d, st);
          else if (ts.isCallExpression(init)) {
            if (init.arguments.some((a) => isFn(unwrap(a))) || /^(memo|forwardRef|observer|withAuth|connect|dynamic|lazy)$/.test(calleeName(init))) addFn(nm, d, st);
          } else if (ts.isObjectLiteralExpression(init)) {
            for (const p of init.properties) {
              const pn = p.name && (ts.isIdentifier(p.name) || ts.isStringLiteral(p.name)) ? p.name.text : null;
              if (!pn) continue;
              if (ts.isMethodDeclaration(p) || (ts.isPropertyAssignment(p) && isFn(unwrap(p.initializer)))) addFn(nm + "." + pn, p, p);
            }
          }
        }
      }
    }
    result.files[rel] = out;
  } catch (e) { result.errors.push(rel + ": " + e.message); }
}
fs.writeFileSync(outPath, JSON.stringify(result));
'''


def analyze_typescript(root: Path, files: list, g: Graph, cfg: dict, out: Path, warnings: list) -> dict:
    node_bin = shutil.which("node")
    if not node_bin:
        warnings.append("frontend skipped: `node` not found on PATH")
        return {}
    hosts = sorted({h for v in cfg["sources"].values() for h in v.get("hosts", [])})
    search = set()
    for rel in files:
        d = (root / rel).parent
        while True:
            if (d / "package.json").exists():
                search.add(str(d))
                break
            if d == root or d.parent == d:
                break
            d = d.parent
    js, inp, outp = out / "_ts_extract.js", out / "_ts_input.json", out / "_ts_output.json"
    js.write_text(TS_EXTRACT_JS, encoding="utf-8")
    inp.write_text(json.dumps({"root": str(root), "files": files, "searchDirs": sorted(search), "keep": hosts}),
                   encoding="utf-8")
    try:
        r = subprocess.run([node_bin, str(js), str(inp), str(outp)], capture_output=True, text=True, timeout=900)
        if r.returncode != 0 or not outp.exists():
            warnings.append(f"frontend skipped: node extractor failed: {(r.stderr or r.stdout).strip()[:300]}")
            return {}
        data = json.loads(outp.read_text(encoding="utf-8"))
    finally:
        for p in (js, inp, outp):
            try:
                p.unlink()
            except OSError:
                pass
    if data.get("error"):
        warnings.append(f"frontend skipped: {data['error']}")
        return {}
    for e in data.get("errors", []):
        warnings.append(f"ts parse: {e}")
    tsf = data["files"]
    src_by_import = {imp: s for s, v in cfg["sources"].items() for imp in v.get("imports", [])}
    host_list = [(s, h) for s, v in cfg["sources"].items() for h in v.get("hosts", [])]

    # nodes
    fn_id = {}
    for rel, fi in tsf.items():
        for fn in fi["functions"]:
            q = fn["qual"]
            base = q.split(".")[0]
            if "." in q:
                ntype = "ts"
            elif base[:1].isupper() and rel.endswith((".tsx", ".jsx")):
                ntype = "component"
            elif re.match(r"use[A-Z0-9]", base):
                ntype = "hook"
            else:
                ntype = "ts"
            nid = f"{ntype}:{rel}::{q}"
            g.node(nid, ntype, q, file=rel, line=fn["line"], end=fn["end"])
            fn_id[(rel, q)] = nid
            fn["_id"] = nid

    def export_of(rel, name, depth=0):
        fi = tsf.get(rel)
        if not fi or depth > 4:
            return None
        if name == "default":
            d = [f for f in fi["functions"] if f["isDefault"]]
            if d:
                return d[0]["_id"]
            if fi["defaultExport"] and (rel, fi["defaultExport"]) in fn_id:
                return fn_id[(rel, fi["defaultExport"])]
            for rx in fi["reexports"]:
                for nm in rx["names"]:
                    if nm["as"] == "default" and rx["resolved"]:
                        return export_of(rx["resolved"], nm["name"], depth + 1)
            return None
        if (rel, name) in fn_id:
            return fn_id[(rel, name)]
        for le in fi["localExports"]:
            if le["as"] == name and (rel, le["name"]) in fn_id:
                return fn_id[(rel, le["name"])]
        for rx in fi["reexports"]:
            if not rx["resolved"]:
                continue
            if rx["star"]:
                r = export_of(rx["resolved"], name, depth + 1)
            else:
                r = next((export_of(rx["resolved"], nm["name"], depth + 1) for nm in rx["names"] if nm["as"] == name), None)
            if r:
                return r
        return None

    routes = [(n["method"], n["route"], nid) for nid, n in g.nodes.items() if n["type"] == "endpoint"]

    def segs(p):
        return [s for s in p.split("/") if s]

    def seg_eq(a, b):
        return a == b or (a.startswith("{") and a.endswith("}")) or (b.startswith("{") and b.endswith("}"))

    def seq_eq(a, b):
        return len(a) == len(b) and all(seg_eq(x, y) for x, y in zip(a, b))

    def match_endpoints(path, methods, http):
        ps = segs(path)
        if not ps:
            return []
        forms = [(ps, 3)]
        if ps[0] != "api":
            forms.append((["api"] + ps, 2))
        else:
            forms.append((ps[1:], 2))
        found = {}
        for form, conf in forms:
            for mth, rp, nid in routes:
                if seq_eq(segs(rp), form):
                    found[nid] = max(found.get(nid, 0), conf)
        if not found and http:
            def lit_hit(rs):
                return any(x == y and not x.startswith("{") for x, y in zip(rs, ps))
            suffix = [nid for mth, rp, nid in routes
                      if len(segs(rp)) > len(ps) and seq_eq(segs(rp)[-len(ps):], ps) and lit_hit(segs(rp)[-len(ps):])]
            for nid in suffix:
                found[nid] = 2 if len(suffix) == 1 else 1
        want = set(methods) or {"GET"}
        filtered = {k: v for k, v in found.items() if g.nodes[k]["method"] in want}
        return list((filtered or {k: min(v, 1) for k, v in found.items()}).items())

    # edges
    for rel, fi in tsf.items():
        local = {f["qual"]: f["_id"] for f in fi["functions"]}
        imp_local = {}
        for im in fi["imports"]:
            if im["default"]:
                imp_local[im["default"]] = (im, "default")
            for nm in im["named"]:
                imp_local[nm["local"]] = (im, nm["name"])
            if im["ns"]:
                imp_local[im["ns"]] = (im, "*")
        src_alias = {}
        for loc, (im, _) in imp_local.items():
            parts = im["spec"].split("/")
            top = "/".join(parts[:2]) if im["spec"].startswith("@") else parts[0]
            if top in src_by_import:
                src_alias[loc] = src_by_import[top]
        for fn in fi["functions"]:
            me = fn["_id"]
            for ident in fn["idents"]:
                if ident in src_alias:
                    g.node(f"source:{src_alias[ident]}", "source", src_alias[ident])
                    g.edge(me, f"source:{src_alias[ident]}", "pulls_from", "high")
                if ident in local and local[ident] != me:
                    g.edge(me, local[ident], "calls", "high")
                elif ident in imp_local:
                    im, orig = imp_local[ident]
                    if im["resolved"] and orig != "*":
                        t = export_of(im["resolved"], orig)
                        if t:
                            g.edge(me, t, "calls", "high")
            for mem in fn["members"]:
                obj, prop = mem.split(".", 1)
                if f"{obj}.{prop}" in local:
                    g.edge(me, local[f"{obj}.{prop}"], "calls", "high")
                elif obj in imp_local:
                    im, orig = imp_local[obj]
                    if not im["resolved"]:
                        continue
                    if orig == "*":
                        t = export_of(im["resolved"], prop)
                    elif orig == "default":
                        dn = (tsf.get(im["resolved"]) or {}).get("defaultExport")
                        t = export_of(im["resolved"], f"{dn}.{prop}") if dn else None
                    else:
                        t = export_of(im["resolved"], f"{orig}.{prop}")
                    if t:
                        g.edge(me, t, "calls", "high")
            for dr in fn["dyn"]:
                t = export_of(dr, "default")
                if t:
                    g.edge(me, t, "calls", "medium")
            seen_paths = set()
            for s in fn["strings"]:
                low = s.lower()
                if " " not in s:
                    for sname, h in host_list:
                        if h in low:
                            g.node(f"source:{sname}", "source", sname)
                            g.edge(me, f"source:{sname}", "pulls_from", "medium")
                path = re.sub(r"^https?://[^/]+", "", s.split("?")[0].split("#")[0])
                if not re.fullmatch(r"/[A-Za-z0-9_\-./{}~%:]*", path) or path in seen_paths:
                    continue
                seen_paths.add(path)
                for eid, c in match_endpoints(path.rstrip("/") or "/", fn["methods"], fn["http"]):
                    g.edge(me, eid, "requests", c, path=path)

        # Next.js pages
        parts = rel.split("/")
        fname = parts[-1]
        m = re.match(r"page\.(tsx|jsx|js|ts)$", fname)
        route = None
        if m and "app" in parts[:-1]:
            route = parts[parts.index("app") + 1:-1]
        elif "pages" in parts[:-1] and not (len(parts) > parts.index("pages") + 1 and parts[parts.index("pages") + 1] == "api") \
                and re.match(r"[^_].*\.(tsx|jsx|js|ts)$", fname):
            route = parts[parts.index("pages") + 1:-1] + ([] if fname.startswith("index.") else [fname.rsplit(".", 1)[0]])
        if route is not None:
            route = [re.sub(r"^\[(?:\.\.\.)?(.+?)\]$", r"{\1}", s) for s in route if not (s.startswith("(") and s.endswith(")"))]
            pid = "page:/" + "/".join(route)
            g.node(pid, "page", "/" + "/".join(route), file=rel, line=1)
            t = export_of(rel, "default")
            if t:
                g.edge(pid, t, "renders", "high")
            else:
                for f in fi["functions"]:
                    if "." not in f["qual"]:
                        g.edge(pid, f["_id"], "renders", "medium")
    return {"ts_files": len(tsf), "parse_errors": len(data.get("errors", [])), "analysis_available": True}


# --------------------------------------------------------------------------------------
# Query side
# --------------------------------------------------------------------------------------
class View:
    def __init__(self, data, min_conf=1, kinds=None):
        self.nodes = data["nodes"]
        self.deps = collections.defaultdict(list)
        self.rdeps = collections.defaultdict(list)
        for row in data["edges"]:
            s, d, k, c = row[:4]
            if c < min_conf or (kinds and k not in kinds):
                continue
            self.deps[s].append((d, k, c))
            self.rdeps[d].append((s, k, c))
            if k in SYMMETRIC:
                self.deps[d].append((s, k, c))
                self.rdeps[s].append((d, k, c))

    def nbrs(self, nid, direction):
        lst = self.deps[nid] if direction == "up" else self.rdeps[nid]
        return sorted(lst, key=lambda e: (KIND_ORDER.index(e[1]) if e[1] in KIND_ORDER else 99,
                                          self.nodes[e[0]]["type"], self.nodes[e[0]]["name"]))


def loc(n, full=False):
    if n.get("defined_in"):
        d = n["defined_in"][0]
        s = f"{d['file']}:{d['line']}" + (f"-{d['end']}" if d.get("end") else "")
        if len(n["defined_in"]) > 1:
            s += f" (+{len(n['defined_in']) - 1} more defs)"
        if full and n.get("live_def"):
            s += f" | live: {n['live_def']}"
        return s
    if n.get("live_def"):
        return f"live: {n['live_def']}"
    if n.get("file"):
        return f"{n['file']}:{n.get('line', 1)}" + (f"-{n['end']}" if n.get("end") else "")
    return ""


def find_root(arg):
    if arg:
        return Path(arg).resolve()
    try:
        r = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            return Path(r.stdout.strip()).resolve()
    except (OSError, subprocess.SubprocessError):
        pass
    return Path.cwd().resolve()


def git_head(root):
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=root, timeout=10)
        return r.stdout.strip() if r.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def load_graph(root, path_arg):
    p = Path(path_arg) if path_arg else root / OUT_DIR / "graph.json"
    if not p.exists():
        raise SystemExit(f"No graph at {p}. Build it first:  python tools/data_dictionary/lineage.py build")
    data = json.loads(p.read_text(encoding="utf-8"))
    meta = data["meta"]
    changed = []
    for rel, mt in meta.get("files", {}).items():
        try:
            current_ns = (root / rel).stat().st_mtime_ns
            if isinstance(mt, int) and mt > 10**12:
                if current_ns != mt:
                    changed.append(rel)
            elif (root / rel).stat().st_mtime > meta["built_at"] and current_ns // 1_000_000_000 != mt:
                changed.append(rel)
        except OSError:
            changed.append(rel)
    head = git_head(root)
    notes = []
    if changed:
        notes.append(f"{len(changed)} source file(s) changed since the build (e.g. {changed[0]})")
    if head and meta.get("git_head") and head != meta["git_head"]:
        notes.append(f"git HEAD moved {meta['git_head']} -> {head}")
    when = time.strftime("%Y-%m-%d %H:%M", time.localtime(meta["built_at"]))
    print(f"# graph built {when}" + (f", git {meta['git_head']}" if meta.get("git_head") else "")
          + (f", db {meta['db']['database']}" if meta.get("db") else ", no db"), file=sys.stderr)
    if notes:
        print("# WARNING graph may be stale: " + "; ".join(notes) + ". Ask the user to re-run `lineage.py build`.",
              file=sys.stderr)
    data["_runtime"] = {"is_stale": bool(notes), "stale_reasons": notes,
                        "checked_git_head": head, "built_at": meta.get("built_at"),
                        "built_git_head": meta.get("git_head")}
    return data


def resolve_node(data, q, exit_on_fail=True):
    nodes = data["nodes"]
    if q in nodes:
        return q
    ql = q.lower()
    exact = [i for i, n in nodes.items() if n["name"].lower() == ql or i.lower() == ql
             or n["name"].lower().endswith("." + ql) or i.lower().endswith("::" + ql)]
    if len(exact) == 1:
        return exact[0]
    cands = exact or [i for i, n in nodes.items() if ql in i.lower()]
    if len(cands) == 1:
        return cands[0]
    if not cands:
        print(f"No node matches '{q}'. Try: lineage.py search {q}", file=sys.stderr)
    else:
        print(f"'{q}' is ambiguous ({len(cands)} matches); re-run with one of these ids:", file=sys.stderr)
        for i in sorted(cands)[:15]:
            print(f"  {i}", file=sys.stderr)
    if exit_on_fail:
        sys.exit(2)
    return None


def cmd_search(data, a):
    terms = [t.lower() for t in a.terms]
    scored = []
    for nid, n in data["nodes"].items():
        if a.type and n["type"] != a.type:
            continue
        cols = " ".join(c[0] for c in n.get("columns", []))
        hay = " ".join([nid, n.get("doc") or "", n.get("comment") or "", cols]).lower()
        if not all(t in hay for t in terms):
            continue
        name = n["name"].lower()
        s = 0 if any(name == t or name.endswith("." + t) for t in terms) else 1 if any(name.startswith(t) for t in terms) \
            else 2 if all(t in nid.lower() for t in terms) else 3
        scored.append((s, nid))
    scored.sort()
    for _, nid in scored[:a.n]:
        n = data["nodes"][nid]
        doc = (n.get("doc") or n.get("comment") or "")[:90]
        print(f"{nid}  [{n['type']}]  {loc(n)}" + (f"  - {doc}" if doc else ""))
    if len(scored) > a.n:
        print(f"... {len(scored) - a.n} more; narrow the query or raise -n")
    if not scored:
        import difflib
        keys = {}
        for nid, n in data["nodes"].items():
            if a.type and n["type"] != a.type:
                continue
            keys[nid.split(":", 1)[-1].lower()] = nid
        close = difflib.get_close_matches(" ".join(terms), list(keys), n=5, cutoff=0.6)
        print("no matches")
        if close:
            print("did you mean:")
            for c in close:
                print(f"  {keys[c]}")


def cmd_show(data, a):
    nid = resolve_node(data, a.node)
    n = data["nodes"][nid]
    v = View(data)
    print(f"{nid}  [{n['type']}]")
    print(f"  location : {loc(n, full=True) or '-'}")
    for d in n.get("defined_in", [])[1:6]:
        print(f"             also {d['file']}:{d['line']}" + (f"-{d['end']}" if d.get("end") else ""))
    for k in ("doc", "comment", "note", "args", "lang"):
        if n.get(k):
            print(f"  {k:9}: {n[k]}")
    if n.get("dynamic_sql"):
        print("  WARNING  : builds SQL dynamically - some references may be missing; read the definition")
    if n.get("unknown_rels"):
        print(f"  unresolved relations referenced: {', '.join(n['unknown_rels'])}")
    if n.get("columns"):
        print("  columns  : " + ", ".join(f"{c[0]} {c[1]}" for c in n["columns"][:60]))
    if n.get("primary_key"):
        print("  primary key: " + ", ".join(n["primary_key"]))
    if n.get("indexes"):
        for ix in n["indexes"][:30]:
            flags = " PRIMARY" if ix.get("is_primary") else " UNIQUE" if ix.get("is_unique") else ""
            print(f"  index    : {ix.get('name')} [{ix.get('access_method')}] ({', '.join(ix.get('columns', []))}){flags}")
    for direction, title, table in (("up", "depends on", EDGE_UP), ("down", "used by", EDGE_DOWN)):
        rows = v.nbrs(nid, direction)
        if rows:
            print(f"  {title}:")
            for nb, k, c in rows[:40]:
                nn = data["nodes"][nb]
                print(f"    [{table.get(k, k)}{MARK[c]}] {nn['type']} {nn['name']}  {loc(nn)}")
            if len(rows) > 40:
                print(f"    ... {len(rows) - 40} more")


def bfs(view, start, direction, depth):
    dist, order, q = {start: 0}, [], collections.deque([start])
    while q:
        cur = q.popleft()
        if dist[cur] >= depth:
            continue
        for nb, k, c in view.nbrs(cur, direction):
            if nb not in dist:
                dist[nb] = dist[cur] + 1
                order.append(nb)
                q.append(nb)
    return dist, order


def _confidence_name(value):
    return {1: "low", 2: "medium", 3: "high"}.get(value, "unknown")


def _edge_attrs(row):
    return row[4] if len(row) > 4 and isinstance(row[4], dict) else {}


def _node_summary(nid, node, depth=None, direction=None, details=False):
    out = {
        "id": nid,
        "type": node.get("type"),
        "name": node.get("name"),
        "schema": node.get("schema"),
        "location": loc(node, full=True) or None,
    }
    if depth is not None:
        out["depth"] = depth
    if direction:
        out["direction"] = direction
    common_fields = ("comment", "doc", "note", "args", "lang", "origin", "dynamic_sql", "unknown_rels")
    detail_fields = ("columns", "primary_key", "indexes", "defined_in", "live_def") if details else ()
    for key in common_fields + detail_fields:
        if node.get(key) not in (None, "", [], False):
            out[key] = node[key]
    return out


def _collect_direction(data, start, direction, depth, min_conf, kinds, max_nodes):
    view = View(data, min_conf, kinds)
    edge_map = {(row[0], row[1], row[2]): row for row in data.get("edges", [])}
    distances = {start: 0}
    queue = collections.deque([start])
    collected_edges = {}
    truncated = False
    while queue:
        current = queue.popleft()
        current_depth = distances[current]
        if current_depth >= depth:
            continue
        for neighbor, kind, conf in view.nbrs(current, direction):
            is_new = neighbor not in distances
            if is_new and len(distances) >= max_nodes:
                truncated = True
                continue
            source, target = (current, neighbor) if direction == "up" else (neighbor, current)
            key = (source, target, kind)
            row = edge_map.get(key)
            if row:
                collected_edges[key] = row
            if is_new:
                distances[neighbor] = current_depth + 1
                queue.append(neighbor)
    nodes = [
        _node_summary(nid, data["nodes"][nid], depth=dist, direction=direction)
        for nid, dist in sorted(distances.items(), key=lambda item: (item[1], data["nodes"][item[0]].get("type", ""),
                                                                      data["nodes"][item[0]].get("name", "")))
        if nid != start
    ]
    edges = []
    for (source, target, kind), row in sorted(collected_edges.items()):
        conf = row[3]
        edges.append({
            "source": source,
            "target": target,
            "kind": kind,
            "confidence": _confidence_name(conf),
            "confidence_value": conf,
            "attributes": _edge_attrs(row),
        })
    return {"nodes": nodes, "edges": edges, "distances": distances, "truncated": truncated}


def _writer_operation_groups(target_id, upstream_edges, nodes):
    primary, possible, mutations, unclassified = [], [], [], []
    seen = set()
    for edge in upstream_edges:
        if edge["source"] != target_id or edge["kind"] != "written_by":
            continue
        writer_id = edge["target"]
        if writer_id in seen:
            continue
        seen.add(writer_id)
        evidence = edge["attributes"].get("evidence", [])
        operations = sorted({str(ev.get("operation", "UNKNOWN")) for ev in evidence if isinstance(ev, dict)})
        population_ops = {"INSERT", "COPY_FROM", "MERGE", "PANDAS_TO_SQL", "ORM_INSERT"}
        possible_ops = {"ORM_ADD (insert/update depends on ORM state)", "ORM_MERGE"}
        mutation_ops = {"UPDATE", "DELETE", "TRUNCATE", "COPY_TO", "REFRESH", "ORM_UPDATE", "ORM_DELETE"}
        info = {
            "node": _node_summary(writer_id, nodes[writer_id]),
            "confidence": edge["confidence"],
            "operations": operations,
            "population_operations": sorted(set(operations) & population_ops),
            "ambiguous_operations": sorted(set(operations) & possible_ops),
            "mutation_operations": sorted(set(operations) & mutation_ops),
            "evidence": evidence,
            "classification": "unclassified",
        }
        if any(op in population_ops for op in operations):
            info["classification"] = "population"
            primary.append(info)
        elif any(op in possible_ops for op in operations):
            info["classification"] = "possible_population_or_mutation"
            possible.append(info)
        elif any(op in mutation_ops for op in operations):
            info["classification"] = "mutation"
            mutations.append(info)
        else:
            unclassified.append(info)
    return {"primary_population_writers": primary, "ambiguous_writers": possible,
            "secondary_mutators": mutations, "unclassified_writers": unclassified}


def _collect_invocation_paths(data, writer_ids, min_conf, max_depth=6, max_paths=30):
    """Find repository callers of writer routines without confusing calls with data lineage."""
    view = View(data, min_conf, {"calls", "handled_by"})
    paths = []

    def walk(callee, reverse_path, seen, depth):
        if len(paths) >= max_paths:
            return
        if depth >= max_depth:
            paths.append({"writer": reverse_path[0], "path": list(reversed(reverse_path)), "truncated": True})
            return
        callers = [(nid, kind) for nid, kind, _ in view.nbrs(callee, "down") if kind == "calls" and nid not in seen]
        if not callers:
            # The outermost repository function may be registered as a FastAPI route handler.
            entrypoints = [(nid, kind) for nid, kind, _ in view.nbrs(callee, "down") if kind == "handled_by"]
            if entrypoints:
                for endpoint_id, _ in entrypoints:
                    paths.append({"writer": reverse_path[0], "path": [endpoint_id] + list(reversed(reverse_path)),
                                  "truncated": False})
            else:
                paths.append({"writer": reverse_path[0], "path": list(reversed(reverse_path)), "truncated": False})
            return
        for caller, _ in callers:
            walk(caller, reverse_path + [caller], seen | {caller}, depth + 1)

    for writer in writer_ids:
        if writer in data["nodes"]:
            walk(writer, [writer], {writer}, 0)
    # Deduplicate and make output stable.
    unique = {}
    for item in paths:
        key = tuple(item["path"])
        unique[key] = item
    return list(unique.values())[:max_paths]


def _read_source_excerpts(root, candidates, max_lines):
    root = root.resolve()
    excerpts, warnings = [], []
    used = 0
    truncated = False
    for candidate in candidates:
        if used >= max_lines:
            truncated = True
            break
        rel = candidate["file"]
        try:
            path = (root / rel).resolve()
            path.relative_to(root)
            if not path.is_file():
                warnings.append(f"source file not found: {rel}")
                continue
            lines = read_text(path).splitlines()
        except (OSError, ValueError):
            warnings.append(f"source file unavailable or outside repository: {rel}")
            continue
        if not lines:
            continue
        start = max(1, int(candidate.get("start_line") or 1))
        if start > len(lines):
            warnings.append(f"source line {start} is beyond end of file: {rel}")
            continue
        end_value = candidate.get("end_line")
        end = int(end_value) if end_value else min(len(lines), start + 39)
        end = min(max(start, end), len(lines))
        remaining = max_lines - used
        if end - start + 1 > remaining:
            end = start + remaining - 1
            truncated = True
        excerpt_text = "\n".join(f"{idx}: {lines[idx - 1]}" for idx in range(start, end + 1))
        excerpts.append({
            **candidate,
            "end_line": end,
            "text": excerpt_text,
        })
        used += end - start + 1
        if truncated:
            break
    return excerpts, warnings, {"used_lines": used, "max_lines": max_lines, "truncated": truncated}


def build_describe_report(data, node_id, *, direction="both", depth=8, min_conf="medium",
                          max_nodes=80):
    """Build a deterministic, structured report from the precomputed graph."""
    target_id = resolve_node(data, node_id)
    min_conf_value = CONF[min_conf]
    kinds = {kind for kind in EDGE_UP if kind != "fk"}
    upstream = {"nodes": [], "edges": [], "distances": {}, "truncated": False}
    downstream = {"nodes": [], "edges": [], "distances": {}, "truncated": False}
    if direction in ("both", "up"):
        upstream = _collect_direction(data, target_id, "up", depth, min_conf_value, kinds, max_nodes)
    if direction in ("both", "down"):
        downstream = _collect_direction(data, target_id, "down", depth, min_conf_value, kinds, max_nodes)

    all_nodes = {target_id: data["nodes"][target_id]}
    for group in (upstream, downstream):
        for item in group["nodes"]:
            all_nodes[item["id"]] = data["nodes"][item["id"]]
    all_edges_by_key = {}
    for group in (upstream, downstream):
        for edge in group["edges"]:
            all_edges_by_key[(edge["source"], edge["target"], edge["kind"])] = edge
    all_edges = list(all_edges_by_key.values())

    writers = _writer_operation_groups(target_id, upstream["edges"], all_nodes)
    writer_ids = []
    for key in ("primary_population_writers", "ambiguous_writers", "secondary_mutators", "unclassified_writers"):
        writer_ids.extend(item["node"]["id"] for item in writers[key])
    invocation_paths = _collect_invocation_paths(data, writer_ids, min_conf_value)
    for path_item in invocation_paths:
        for invocation_id in path_item["path"]:
            if invocation_id in data["nodes"]:
                all_nodes.setdefault(invocation_id, data["nodes"][invocation_id])
    raw_edge_map = {(row[0], row[1], row[2]): row for row in data.get("edges", [])}
    for path_item in invocation_paths:
        path_ids = path_item["path"]
        for index in range(len(path_ids) - 1):
            source_id, target_id2 = path_ids[index], path_ids[index + 1]
            kind = "handled_by" if data["nodes"].get(source_id, {}).get("type") == "endpoint" else "calls"
            row = raw_edge_map.get((source_id, target_id2, kind))
            if row:
                conf_value = row[3]
                all_edges_by_key[(source_id, target_id2, kind)] = {
                    "source": source_id, "target": target_id2, "kind": kind,
                    "confidence": _confidence_name(conf_value), "confidence_value": conf_value,
                    "attributes": _edge_attrs(row),
                }
    all_edges = list(all_edges_by_key.values())
    consumers = collections.defaultdict(list)
    for item in downstream["nodes"]:
        consumers[item["type"]].append(item)
    consumer_groups = {
        "database_views": consumers.get("view", []) + consumers.get("matview", []),
        "backend_readers_and_processors": consumers.get("py", []) + consumers.get("function", []) + consumers.get("procedure", []),
        "api_endpoints": consumers.get("endpoint", []),
        "frontend_clients_components_and_hooks": consumers.get("ts", []) + consumers.get("component", []) + consumers.get("hook", []),
        "pages": consumers.get("page", []),
        "other": [item for t, values in consumers.items()
                   if t not in {"view", "matview", "py", "function", "procedure", "endpoint", "ts", "component", "hook", "page"}
                   for item in values],
    }

    meta = data.get("meta", {})
    coverage = meta.get("coverage", {})
    dynamic_nodes = [nid for nid, n in all_nodes.items() if n.get("dynamic_sql")]
    unknown_nodes = [{"id": nid, "relations": n.get("unknown_rels", [])}
                     for nid, n in all_nodes.items() if n.get("unknown_rels")]
    warnings = []
    runtime = data.get("_runtime", {})
    if runtime.get("is_stale"):
        warnings.append({"kind": "stale_graph", "details": runtime.get("stale_reasons", [])})
    if not coverage:
        warnings.append({"kind": "legacy_graph_metadata", "message": "This graph predates coverage metadata; completeness cannot be established."})
    if not meta.get("db"):
        warnings.append({"kind": "database_catalog_unavailable", "message": "No database catalog was captured; live columns, indexes, and catalog dependencies may be incomplete."})
    if coverage.get("tests_included") is False:
        warnings.append({"kind": "tests_excluded", "count": coverage.get("tests_excluded_count", 0),
                         "message": f"Test scanning was disabled for this build; {coverage.get('tests_excluded_count', 0)} discovered test file(s) were excluded. Rebuild with --include-tests to include test surfaces."})
    if coverage.get("typescript_analysis") == "skipped":
        warnings.append({"kind": "typescript_analysis_skipped", "message": "Frontend analysis did not complete; frontend dependencies may be missing."})
    if coverage.get("python_parse_errors", 0) or coverage.get("typescript_parse_errors", 0):
        warnings.append({"kind": "parse_errors", "python": coverage.get("python_parse_errors", 0),
                         "typescript": coverage.get("typescript_parse_errors", 0)})
    if dynamic_nodes:
        warnings.append({"kind": "dynamic_sql", "nodes": dynamic_nodes,
                         "message": "References in dynamically constructed SQL may be incomplete."})
    if unknown_nodes:
        warnings.append({"kind": "unknown_relations", "nodes": unknown_nodes,
                         "message": "Some relation references could not be resolved to catalog or repository nodes."})
    if any(n.get("origin") == "orm_model" for n in all_nodes.values()):
        warnings.append({"kind": "orm_inferred_table", "message": "At least one table node was inferred from an ORM declaration rather than confirmed by the database catalog."})
    if upstream["truncated"] or downstream["truncated"]:
        warnings.append({"kind": "neighborhood_truncated", "message": f"The traversal reached the --max-nodes limit ({max_nodes}). Increase it to inspect additional nodes."})

    report = {
        "report_version": 1,
        "target": _node_summary(target_id, data["nodes"][target_id], details=True),
        "graph": {
            "built_at": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(meta["built_at"]))
                         if meta.get("built_at") else None),
            "built_at_epoch": meta.get("built_at"),
            "git_head": meta.get("git_head"),
            "database": (meta.get("db") or {}).get("database"),
            "server_version": (meta.get("db") or {}).get("server_version"),
            "is_stale": bool(runtime.get("is_stale")),
            "stale_reasons": runtime.get("stale_reasons", []),
            "graph_version": meta.get("version"),
        },
        "coverage": coverage,
        "population": writers,
        "invocation_paths": invocation_paths,
        "upstream": {"nodes": upstream["nodes"], "edges": upstream["edges"], "truncated": upstream["truncated"]},
        "downstream": {"nodes": downstream["nodes"], "edges": downstream["edges"], "truncated": downstream["truncated"]},
        "consumption": consumer_groups,
        "relationship_map": {"nodes": [_node_summary(nid, node) for nid, node in all_nodes.items()],
                             "edges": all_edges},
        "warnings": warnings,
    }
    return report


def report_node_name(report, node_id):
    for node in report.get("relationship_map", {}).get("nodes", []):
        if node["id"] == node_id:
            return node.get("name", node_id)
    for group in (report.get("upstream", {}).get("nodes", []), report.get("downstream", {}).get("nodes", [])):
        for node in group:
            if node["id"] == node_id:
                return node.get("name", node_id)
    return report.get("target", {}).get("name") if report.get("target", {}).get("id") == node_id else node_id


def cmd_describe(data, a, root):
    sections = parse_describe_sections(a.section)
    report = build_describe_report(
        data, a.node, direction=a.direction, depth=a.depth, min_conf=a.min_conf,
        max_nodes=a.max_nodes)
    if a.format == "json":
        print(json.dumps(compact_describe_report(report, sections, max_items=a.max_items), indent=2, sort_keys=True, default=str))
    else:
        print(render_describe_text(report, sections=sections, max_items=a.max_items))



DESCRIBE_SECTIONS = ("identity", "population", "upstream", "consumers", "map", "coverage")
DEFAULT_DESCRIBE_SECTIONS = ("identity", "population", "upstream", "consumers", "map", "coverage")


def parse_describe_sections(values):
    """Parse repeatable/comma-separated --section values."""
    if not values:
        return list(DEFAULT_DESCRIBE_SECTIONS)
    selected = []
    for value in values:
        selected.extend(part.strip().lower() for part in value.split(",") if part.strip())
    if "all" in selected:
        return list(DESCRIBE_SECTIONS)
    unknown = sorted(set(selected) - set(DESCRIBE_SECTIONS))
    if unknown:
        raise SystemExit(f"Unknown describe section(s): {', '.join(unknown)}. Choose: {', '.join(DESCRIBE_SECTIONS)} or all.")
    return [section for section in DESCRIBE_SECTIONS if section in set(selected)]


def _limited(items, limit):
    return list(items[:limit])


def _render_compact_nodes(items, limit=20):
    if not items:
        return ["  (none in the selected graph neighborhood)"]
    lines = []
    for item in _limited(items, limit):
        dynamic = " !dynamic-sql" if item.get("dynamic_sql") else ""
        loc_text = item.get("location") or "no source location"
        lines.append(f"  - d{item.get('depth', '?')} [{item.get('type')}] {item.get('name')}{dynamic} — {loc_text}")
    if len(items) > limit:
        lines.append(f"  ... {len(items) - limit} more; narrow with --direction/--depth or request a section")
    return lines


def _render_compact_population(report, limit=12):
    lines = []
    pop = report.get("population", {})
    groups = (
        ("primary_population_writers", "Population writers"),
        ("ambiguous_writers", "Ambiguous ORM persistence"),
        ("secondary_mutators", "Secondary mutations"),
        ("unclassified_writers", "Unclassified writer relationships"),
    )
    count = 0
    for key, title in groups:
        lines.append(title + ":")
        items = pop.get(key, [])
        if not items:
            lines.append("  (none)")
            continue
        for item in items:
            if count >= limit:
                break
            node = item["node"]
            ops = ", ".join(item.get("operations", [])) or "operation evidence unavailable"
            lines.append(f"  - [{item.get('confidence', 'unknown').upper()}] {node.get('type')} {node.get('name')} — {node.get('location') or 'no source location'}")
            lines.append(f"    operations: {ops}")
            if item.get("population_operations"):
                lines.append("    population operation(s): " + ", ".join(item["population_operations"]))
            if item.get("ambiguous_operations"):
                lines.append("    ambiguous persistence: " + ", ".join(item["ambiguous_operations"]))
            if item.get("mutation_operations") and item.get("classification") == "population":
                lines.append("    same routine also mutates/removes: " + ", ".join(item["mutation_operations"]))
            evidence = item.get("evidence", [])
            for ev in evidence[:3]:
                operation = ev.get("operation", "UNKNOWN")
                source_loc = f"{ev.get('file') or '?'}:{ev.get('line') or '?'}"
                lines.append(f"    evidence: {operation} at {source_loc} [{ev.get('mechanism', 'unknown')}]")
            count += 1
        if len(items) > max(0, limit - count):
            lines.append("  ... additional writer relationships omitted by --max-items")
    lines.append("Invocation paths (how identified writers are reached; not data dependencies):")
    paths = report.get("invocation_paths", [])
    if not paths:
        lines.append("  (no caller chain resolved at the selected confidence threshold)")
    for path_item in paths[:limit]:
        names = [report_node_name(report, nid) for nid in path_item.get("path", [])]
        lines.append("  - " + " -> ".join(names) + (" [truncated]" if path_item.get("truncated") else ""))
    if len(paths) > limit:
        lines.append(f"  ... {len(paths) - limit} additional paths omitted")
    return lines


def _render_compact_consumers(report, limit=20):
    lines = []
    groups = report.get("consumption", {})
    titles = (
        ("database_views", "Database views and derived relations"),
        ("backend_readers_and_processors", "Backend readers and processors"),
        ("api_endpoints", "API endpoints"),
        ("frontend_clients_components_and_hooks", "Frontend clients, components and hooks"),
        ("pages", "Pages"),
        ("other", "Other consumers"),
    )
    total = 0
    for key, title in titles:
        items = groups.get(key, [])
        lines.append(title + ":")
        if not items:
            lines.append("  (none)")
            continue
        remaining = max(0, limit - total)
        lines.extend(_render_compact_nodes(items, remaining))
        total += min(len(items), remaining)
        if total >= limit and sum(len(groups.get(k, [])) for k, _ in titles) > total:
            lines.append("  ... consumer list capped by --max-items")
            break
    return lines


def compact_describe_report(report, sections, max_items=20):
    """Return a section-filtered and size-bounded JSON object."""
    out = {
        "report_version": report.get("report_version"),
        "sections_included": list(sections),
        "target": {k: v for k, v in report.get("target", {}).items()
                   if k not in {"columns", "primary_key", "indexes", "defined_in", "live_def"}},
        "graph": report.get("graph"),
    }
    if "identity" in sections:
        out["identity"] = {k: report.get("target", {}).get(k) for k in
                            ("comment", "columns", "primary_key", "indexes", "location")
                            if report.get("target", {}).get(k) not in (None, [], "")}
    if "population" in sections:
        population = report.get("population", {})
        out["population"] = {
            key: population.get(key, [])[:max_items]
            for key in ("primary_population_writers", "ambiguous_writers", "secondary_mutators", "unclassified_writers")
        }
        out["invocation_paths"] = report.get("invocation_paths", [])[:max_items]
    if "upstream" in sections:
        group = report.get("upstream", {})
        nodes, edges = group.get("nodes", []), group.get("edges", [])
        out["upstream"] = {**group, "nodes": nodes[:max_items], "edges": edges[:max_items * 2],
                            "truncated": bool(group.get("truncated") or len(nodes) > max_items or len(edges) > max_items * 2)}
    if "consumers" in sections:
        group = report.get("downstream", {})
        nodes, edges = group.get("nodes", []), group.get("edges", [])
        out["downstream"] = {**group, "nodes": nodes[:max_items * 2], "edges": edges[:max_items * 3],
                              "truncated": bool(group.get("truncated") or len(nodes) > max_items * 2 or len(edges) > max_items * 3)}
        out["consumption"] = {key: values[:max_items] for key, values in report.get("consumption", {}).items()}
    if "map" in sections:
        relationship_map = report.get("relationship_map", {})
        edges = relationship_map.get("edges", [])
        selected_edges = edges[:max_items * 3]
        referenced = {edge[k] for edge in selected_edges for k in ("source", "target")}
        referenced.add(report.get("target", {}).get("id"))
        nodes = [node for node in relationship_map.get("nodes", []) if node.get("id") in referenced]
        out["relationship_map"] = {"nodes": nodes, "edges": selected_edges,
                                   "truncated": len(edges) > max_items * 3}
    if "coverage" in sections:
        out["coverage"] = report.get("coverage", {})
        out["warnings"] = report.get("warnings", [])[:30]
    return out


def render_describe_text(report, sections=None, max_items=20):
    """Render a compact agent-first report; source excerpts belong to `evidence`."""
    sections = sections or list(DEFAULT_DESCRIBE_SECTIONS)
    target = report["target"]
    lines = [
        f"LINEAGE: {target.get('name')}",
        f"Node: {target.get('id')} [{target.get('type')}]",
    ]
    graph = report.get("graph", {})
    lines.append(f"Graph: {graph.get('built_at') or 'unknown build'} | git {graph.get('git_head') or 'unknown'} | "
                 f"db {graph.get('database') or 'not captured'} | stale={'YES' if graph.get('is_stale') else 'no'}")
    lines.append("Confidence: HIGH = exact/catalog; MEDIUM = static heuristic; LOW = unverified.")

    if "identity" in sections:
        lines.extend(["", "[IDENTITY]"])
        if target.get("comment"):
            lines.append("Comment: " + target["comment"])
        if target.get("primary_key"):
            lines.append("Primary key: " + ", ".join(target["primary_key"]))
        if target.get("columns"):
            lines.append(f"Columns ({len(target['columns'])}):")
            for col in target["columns"][:40]:
                lines.append("  - " + " ".join(str(x) for x in col[:2]) + (f" — {col[2]}" if len(col) > 2 else ""))
            if len(target["columns"]) > 40:
                lines.append(f"  ... {len(target['columns']) - 40} more columns")
        if target.get("indexes"):
            lines.append(f"Indexes ({len(target['indexes'])}):")
            for index in target["indexes"][:12]:
                flags = ", ".join(x for x, yes in (("PRIMARY KEY", index.get("is_primary")), ("UNIQUE", index.get("is_unique"))) if yes)
                desc = f" [{flags}]" if flags else ""
                lines.append(f"  - {index.get('name')} ({index.get('access_method')}; {', '.join(index.get('columns', []))}){desc}")
            if len(target["indexes"]) > 12:
                lines.append(f"  ... {len(target['indexes']) - 12} more indexes")

    if "population" in sections:
        lines.extend(["", "[POPULATION AND MODIFICATION]"])
        lines.extend(_render_compact_population(report, max_items))

    if "upstream" in sections:
        lines.extend(["", "[UPSTREAM DATA DEPENDENCIES]"])
        lines.extend(_render_compact_nodes(report.get("upstream", {}).get("nodes", []), max_items))
        if report.get("upstream", {}).get("truncated"):
            lines.append("  WARNING: upstream traversal hit --max-nodes; increase the limit for more coverage.")

    if "consumers" in sections:
        lines.extend(["", "[DOWNSTREAM CONSUMPTION]"])
        lines.extend(_render_compact_consumers(report, max_items))
        if report.get("downstream", {}).get("truncated"):
            lines.append("  WARNING: downstream traversal hit --max-nodes; increase the limit for more coverage.")

    if "map" in sections:
        lines.extend(["", "[RELATIONSHIP MAP]"])
        edges = report.get("relationship_map", {}).get("edges", [])
        for edge in edges[:max_items * 3]:
            src, dst = report_node_name(report, edge["source"]), report_node_name(report, edge["target"])
            lines.append(f"  - {src} --{edge['kind']}--> {dst} [{edge['confidence'].upper()}]")
        if len(edges) > max_items * 3:
            lines.append(f"  ... {len(edges) - max_items * 3} more edges")

    if "coverage" in sections:
        lines.extend(["", "[COVERAGE]"])
        if report.get("coverage"):
            lines.append(json.dumps(report["coverage"], sort_keys=True))
        else:
            lines.append("Coverage metadata unavailable (legacy graph).")
        warnings = report.get("warnings", [])
        if warnings:
            for warning in warnings[:30]:
                message = warning.get("message") or warning.get("details") or ""
                if warning.get("details") and isinstance(warning["details"], list):
                    message = "; ".join(str(x) for x in warning["details"])
                lines.append(f"  - [{warning['kind']}] {message}")
        else:
            lines.append("  No known coverage warnings.")
    lines.extend(["", "Source excerpts are deliberately separate. Use `lineage.py evidence <node>` for targeted source verification."])
    return "\n".join(lines)


def _evidence_source_candidates(data, node_id, edge_rows, max_items, *, include_definitions=True):
    """Prioritize exact relationship statements before whole function definitions."""
    candidates = []
    node = data["nodes"][node_id]

    # First: precise operation/call evidence attached to the selected graph edges.
    related_ids = []
    for row in edge_rows:
        for ev in _edge_attrs(row).get("evidence", []):
            if isinstance(ev, dict) and ev.get("file") and ev.get("line"):
                # Operation evidence belongs to the code-bearing endpoint of the edge.
                evidence_node_id = row[1] if row[2] == "written_by" else row[0]
                candidates.append({"node_id": evidence_node_id, "file": ev["file"],
                                   "start_line": ev.get("line", 1), "end_line": ev.get("end"),
                                   "source_kind": ev.get("mechanism", "relationship_evidence")})
        other_id = row[1] if row[0] == node_id else row[0]
        if other_id not in related_ids:
            related_ids.append(other_id)

    # Second: the selected node's own source definitions, useful for understanding context.
    if include_definitions:
        for definition in node.get("defined_in", []):
            if definition.get("file"):
                candidates.append({"node_id": node_id, "file": definition["file"],
                                   "start_line": definition.get("line", 1), "end_line": definition.get("end"),
                                   "source_kind": "repository_definition"})
        if node.get("file"):
            candidates.append({"node_id": node_id, "file": node["file"],
                               "start_line": node.get("line", 1), "end_line": node.get("end"),
                               "source_kind": "source_node"})
        if node.get("live_def"):
            candidates.append({"node_id": node_id, "file": node["live_def"], "start_line": 1,
                               "end_line": None, "source_kind": "live_database_definition"})

        # Third: definitions of adjacent objects when an edge has no usable file evidence.
        for other_id in related_ids:
            other = data["nodes"].get(other_id, {})
            for definition in other.get("defined_in", [])[:1]:
                if definition.get("file"):
                    candidates.append({"node_id": other_id, "file": definition["file"],
                                       "start_line": definition.get("line", 1), "end_line": definition.get("end"),
                                       "source_kind": "related_repository_definition"})
            if other.get("live_def"):
                candidates.append({"node_id": other_id, "file": other["live_def"], "start_line": 1,
                                   "end_line": None, "source_kind": "related_live_database_definition"})

    unique, result = set(), []
    for item in candidates:
        key = (item["node_id"], item["file"], item["start_line"], item["end_line"], item["source_kind"])
        if key not in unique:
            unique.add(key)
            result.append(item)
        if len(result) >= max_items:
            break
    return result


def build_evidence_report(data, root, node, *, related_to=None, operations=None, max_lines=240, max_items=12):
    """Retrieve bounded source evidence for one node, optionally narrowed to a relation/operation."""
    node_id = resolve_node(data, node)
    related_id = resolve_node(data, related_to) if related_to else None
    operation_filter = {x.strip().upper() for x in (operations or []) if x.strip()}
    selected_edges = []
    for row in data.get("edges", []):
        if node_id not in (row[0], row[1]):
            continue
        if related_id and related_id not in (row[0], row[1]):
            continue
        attrs = _edge_attrs(row)
        evidence = attrs.get("evidence", [])
        if operation_filter:
            evidence = [ev for ev in evidence if isinstance(ev, dict) and str(ev.get("operation", "UNKNOWN")).upper() in operation_filter]
            if not evidence:
                continue
            # Preserve only matching evidence in the report and source selection.
            filtered_row = list(row[:4]) + [{**attrs, "evidence": evidence}]
            selected_edges.append(filtered_row)
        else:
            selected_edges.append(row)
    selected_edges.sort(key=lambda r: (r[2], r[0], r[1]))
    candidates = _evidence_source_candidates(
        data, node_id, selected_edges, max_items,
        include_definitions=not (operation_filter and selected_edges))
    excerpts, source_warnings, usage = _read_source_excerpts(root, candidates, max_lines)
    edge_summaries = []
    for row in selected_edges[:max_items * 2]:
        edge_summaries.append({
            "source": row[0], "target": row[1], "kind": row[2],
            "confidence": _confidence_name(row[3]),
            "evidence": _edge_attrs(row).get("evidence", []),
        })
    meta = data.get("meta", {})
    runtime = data.get("_runtime", {})
    return {
        "report_version": 1,
        "node": _node_summary(node_id, data["nodes"][node_id], details=True),
        "related_to": related_id,
        "operation_filter": sorted(operation_filter),
        "graph": {"built_at": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(meta["built_at"])) if meta.get("built_at") else None),
                  "git_head": meta.get("git_head"), "database": (meta.get("db") or {}).get("database"),
                  "is_stale": bool(runtime.get("is_stale")), "stale_reasons": runtime.get("stale_reasons", [])},
        "edges": edge_summaries,
        "source_excerpts": excerpts,
        "source_usage": usage,
        "warnings": source_warnings,
    }


def render_evidence_text(report):
    node = report["node"]
    lines = [f"LINEAGE EVIDENCE: {node.get('name')}", f"Node: {node.get('id')} [{node.get('type')}]"]
    if report.get("related_to"):
        lines.append(f"Relationship filter: {report['related_to']}")
    if report.get("operation_filter"):
        lines.append("Operation filter: " + ", ".join(report["operation_filter"]))
    graph = report.get("graph", {})
    lines.append(f"Graph: built {graph.get('built_at') or 'unknown'} | git {graph.get('git_head') or 'unknown'} | stale={'YES' if graph.get('is_stale') else 'no'}")
    lines.extend(["", "[RELATIONSHIP EVIDENCE]"])
    if not report.get("edges"):
        lines.append("  (no matching relationship edges/evidence in this graph)")
    for edge in report.get("edges", []):
        lines.append(f"  {edge['source']} --{edge['kind']}--> {edge['target']} [{edge['confidence'].upper()}]")
        evidence = edge.get("evidence", [])
        if not evidence:
            lines.append("    (edge has no operation-level evidence; inspect the definition)")
        for ev in evidence[:8]:
            location = f"{ev.get('file') or '?'}:{ev.get('line') or '?'}" + (f"-{ev['end']}" if ev.get("end") else "")
            lines.append(f"    {ev.get('operation', 'UNKNOWN')} [{ev.get('mechanism', 'unknown')}] {location}")
            if ev.get("statement"):
                lines.append("      " + ev["statement"])
    lines.extend(["", "[SOURCE EXCERPTS]"])
    if not report.get("source_excerpts"):
        lines.append("  (no readable source ranges were found; use the locations above to inspect manually)")
    for excerpt in report.get("source_excerpts", []):
        lines.append(f"--- {excerpt['file']}:{excerpt['start_line']}-{excerpt['end_line']} [{excerpt['source_kind']} | {excerpt['node_id']}] ---")
        lines.append(excerpt["text"])
    if report.get("source_usage", {}).get("truncated"):
        lines.append(f"[Excerpt budget reached: {report['source_usage']['used_lines']}/{report['source_usage']['max_lines']} lines]")
    for warning in report.get("warnings", []):
        lines.append("WARNING: " + warning)
    return "\n".join(lines)


def cmd_evidence(data, a, root):
    operations = [part for value in a.operation or [] for part in value.split(",")]
    report = build_evidence_report(data, root, a.node, related_to=a.related_to,
                                   operations=operations, max_lines=a.max_lines, max_items=a.max_items)
    if a.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(render_evidence_text(report))

def cmd_trace(data, a):
    nid = resolve_node(data, a.node)
    kinds = set(a.kinds.split(",")) if a.kinds else {k for k in EDGE_UP if k != "fk"}
    view = View(data, CONF[a.min_conf], kinds)
    dirs = ["up"] if a.up and not a.down else ["down"] if a.down and not a.up else ["up", "down"]
    only = set(a.only.split(",")) if a.only else None
    if a.depth is None:
        a.depth = 12 if (a.files or a.flat or only) else 6
    n0 = data["nodes"][nid]
    print(f"TRACE {nid}  [{n0['type']}]  {loc(n0, full=True)}")
    print("legend: [label] = relation to the line above; ~ medium confidence, ? low confidence (guess); "
          "upstream = what it depends on, downstream = what depends on it")
    for d in dirs:
        title = "UPSTREAM (depends on)" if d == "up" else "DOWNSTREAM (depended on by)"
        table = EDGE_UP if d == "up" else EDGE_DOWN
        print(f"\n== {title} ==")
        if a.files or a.flat or only:
            dist, order = bfs(view, nid, d, a.depth)
            sel = [x for x in order if not only or data["nodes"][x]["type"] in only]
            if a.files:
                by_file = collections.defaultdict(list)
                for x in sel:
                    nn = data["nodes"][x]
                    srcs = nn.get("defined_in") or ([{"file": nn["file"], "line": nn.get("line", 1), "end": nn.get("end")}]
                                                    if nn.get("file") else [])
                    if srcs:
                        s0 = srcs[0]
                        by_file[s0["file"]].append((s0["line"], s0.get("end"), nn["name"]))
                    elif nn.get("live_def"):
                        by_file[nn["live_def"]].append((1, None, nn["name"]))
                for f in sorted(by_file):
                    spans = ", ".join(f"{ln}" + (f"-{e}" if e else "") + f" ({nm})" for ln, e, nm in sorted(by_file[f]))
                    print(f"{f}: {spans}")
            else:
                for x in sel[:a.max_lines]:
                    nn = data["nodes"][x]
                    print(f"d{dist[x]} {nn['type']:9} {nn['name']}  {loc(nn)}")
            if not sel:
                print("(none)")
            continue
        lines, seen = [], {nid}
        truncated = [False]

        def rec(cur, depth, indent):
            for nb, k, c in view.nbrs(cur, d):
                if len(lines) >= a.max_lines:
                    truncated[0] = True
                    return
                nn = data["nodes"][nb]
                tag = f"[{table.get(k, k)}{MARK[c]}] {nn['type']} {nn['name']}"
                if nb in seen:
                    lines.append(f"{indent}- {tag}  (shown above)")
                    continue
                seen.add(nb)
                lines.append(f"{indent}- {tag}  {loc(nn)}" + ("  !dynamic-sql" if nn.get("dynamic_sql") else ""))
                if depth + 1 < a.depth:
                    rec(nb, depth + 1, indent + "  ")

        rec(nid, 0, "")
        print("\n".join(lines) if lines else "(none)")
        if truncated[0]:
            print(f"... truncated at {a.max_lines} lines; use --depth, --kinds, --only or --files to narrow")


def cmd_check(data, a):
    nodes = data["nodes"]
    out_e, in_e = collections.defaultdict(set), collections.defaultdict(set)
    for s, d, k, c, *_ in data["edges"]:
        out_e[s].add((k, d, c))
        in_e[d].add((k, s, c))
    def has(m, nid, kind, conf=1):
        return any(k == kind and c >= conf for k, _, c in m[nid])
    print("== tables with no population operation identified ==")
    for nid, n in sorted(nodes.items()):
        if n["type"] != "table":
            continue
        writer_rows = [row for row in data["edges"] if row[0] == nid and row[2] == "written_by"]
        operations = set()
        for row in writer_rows:
            for ev in _edge_attrs(row).get("evidence", []):
                if isinstance(ev, dict):
                    operations.add(str(ev.get("operation", "UNKNOWN")))
        population_ops = {"INSERT", "COPY_FROM", "MERGE", "PANDAS_TO_SQL", "ORM_INSERT"}
        possible_ops = {"ORM_ADD (insert/update depends on ORM state)", "ORM_MERGE"}
        if not operations.intersection(population_ops | possible_ops):
            note = ", ".join(sorted(operations)) if operations else (
                "writer edge exists but operation evidence is unavailable" if writer_rows else "no writer edge found")
            print(f"  {n['name']}  ({note})")
    print("\n== tables/views nothing reads (dead, or only used via dynamic SQL) ==")
    for nid, n in sorted(nodes.items()):
        if n["type"] in ("table", "view", "matview") and not any(k in ("reads", "derives_from", "mentions") for k, _, _ in in_e[nid]):
            print(f"  {n['name']}")
    print("\n== endpoints no UI code requests ==")
    for nid, n in sorted(nodes.items()):
        if n["type"] == "endpoint" and not has(in_e, nid, "requests"):
            print(f"  {n['name']}  {loc(n)}")
    print("\n== nodes building SQL dynamically (references may be incomplete) ==")
    for nid, n in sorted(nodes.items()):
        if n.get("dynamic_sql"):
            print(f"  {nid}  {loc(n)}")
    print("\n== references to relations that are not in the database (drift, temp tables, typos?) ==")
    for nid, n in sorted(nodes.items()):
        if n.get("unknown_rels"):
            print(f"  {nid}: {', '.join(n['unknown_rels'])}")
    print("\n== database objects defined in more than one repo file ==")
    for nid, n in sorted(nodes.items()):
        if len({d['file'] for d in n.get("defined_in", [])}) > 1:
            print(f"  {n['name']}: " + ", ".join(sorted({d['file'] for d in n['defined_in']})[:6]))
    print("\n== database objects with no definition in the repo (live catalog only) ==")
    for nid, n in sorted(nodes.items()):
        if n.get("origin") == "db" and not n.get("defined_in") and n["type"] != "table":
            print(f"  {n['name']}  {n.get('live_def', '')}")


# --------------------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------------------
def load_cfg(root: Path, path_arg):
    cfg = json.loads(json.dumps(DEFAULT_CFG))
    p = Path(path_arg) if path_arg else root / OUT_DIR / "lineage.config.json"
    if p.exists():
        user = json.loads(read_text(p))
        for k, v in user.items():
            if k in ("exclude_dirs", "exclude_globs"):
                cfg[k] = cfg[k] + list(v)
            elif k == "sources":
                cfg["sources"].update(v)
            else:
                cfg[k] = v
    return cfg


def write_readme(out: Path, data: dict, g: Graph):
    v = View({"nodes": g.nodes, "edges": data["edges"]}, 2, {k for k in EDGE_UP if k != "fk"})
    counts = collections.Counter(n["type"] for n in g.nodes.values())
    lines = ["# Data lineage (generated - do not edit)", "",
             "Query with `python tools/data_dictionary/lineage.py <command>` - never read graph.json directly.", "",
             "| command | use |", "|---|---|",
             "| `search <words>` | find node ids by name, doc/comment text or column name |",
             "| `show <node>` | location, columns, direct dependencies and dependents |",
             "| `trace <node> --up` | where the data comes from (UI -> endpoint -> view/table -> loader -> source) |",
             "| `trace <node> --down` | what depends on it / where it is shown |",
             "| `trace <node> --files` | just the file:line ranges you need to read for that chain |",
             "| `describe <node>` | compact bidirectional structure: population, dependencies, consumers, relationship map and coverage |",
             "| `evidence <node>` | bounded source excerpts and operation evidence for one node/relationship |",
             "| `check` | orphan tables, endpoints with no UI caller, dynamic SQL, drift |", "",
             "Edge `A -[kind]-> B` means A depends on B. `~` = medium confidence, `?` = low (name-based guess).",
             "Nodes flagged `!dynamic-sql` build SQL at runtime: read their definition, the graph may miss edges.",
             "If a file you are about to change is newer than the graph, tell the user to re-run `build`.", "",
             "Node counts: " + ", ".join(f"{k} {c}" for k, c in sorted(counts.items())), ""]
    srcs = [i for i, n in g.nodes.items() if n["type"] == "source"]
    if srcs:
        lines += ["## Sources -> tables they feed (medium+ confidence)", ""]
        for s in sorted(srcs):
            dist, order = bfs(v, s, "down", 12)
            tbls = sorted(g.nodes[x]["name"] for x in order if g.nodes[x]["type"] == "table")
            lines.append(f"- **{g.nodes[s]['name']}**: " + (", ".join(tbls[:40]) + (" ..." if len(tbls) > 40 else "") or "-"))
        lines.append("")
    pages = [i for i, n in g.nodes.items() if n["type"] == "page"]
    if pages:
        lines += ["## UI pages -> tables they read (medium+ confidence)", ""]
        for p in sorted(pages):
            dist, order = bfs(v, p, "up", 12)
            tbls = sorted(g.nodes[x]["name"] for x in order if g.nodes[x]["type"] in ("table", "view", "matview"))
            lines.append(f"- `{g.nodes[p]['name']}`: " + (", ".join(tbls[:30]) + (" ..." if len(tbls) > 30 else "") or "-"))
    (out / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def cmd_build(a):
    root = find_root(a.root)
    env_file = root / "backend" / ".env"
    if _dotenv_load is not None and env_file.exists():
        _dotenv_load(env_file)
    cfg = load_cfg(root, a.config)
    out = root / (a.out or OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    warnings: list = []
    files = discover(root, cfg, a.include_tests)
    print(f"root {root}\nfiles: {len(files['py'])} python, {len(files['ts'])} ts/js, {len(files['sql'])} sql")
    g, reg = Graph(), Registry()
    defs = scan_sql_defs(root, files["sql"] + files["py"])
    db_info = None
    if a.no_db:
        names_from_defs(g, reg, defs)
        warnings.append("--no-db: tables/functions come from repo DDL names only; view/function bodies are not analysed")
    else:
        dsn = (a.dsn
               or os.environ.get("LINEAGE_DSN")
               or env_dsn()
               or os.environ.get("DATABASE_URL")
               or "")
        schemas = [s.strip() for s in a.schemas.split(",")] if a.schemas else cfg.get("schemas")
        try:
            db_info = load_db(dsn, schemas, g, reg, out, warnings)
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            raise SystemExit(f"Database read failed: {e}\nUse --dsn / LINEAGE_DSN / DATABASE_URL (or PG* env vars), "
                             f"or pass --no-db to build the code-only graph.")
        print(f"database {db_info['database']} (PG {db_info['server_version']}): "
              f"{sum(1 for n in g.nodes.values() if n['type'] in ('table', 'view', 'matview'))} relations, "
              f"{sum(1 for n in g.nodes.values() if n['type'] in ('function', 'procedure'))} functions/procedures")
    attach_defs(g, defs)
    py_info = analyze_python(root, files["py"], g, reg, cfg, files["sql"], warnings)
    ts_info = analyze_typescript(root, files["ts"], g, cfg, out, warnings) if files["ts"] else {}

    # recompute per-file mtimes for staleness checks
    meta_files = {}
    for rel in files["py"] + files["ts"] + files["sql"]:
        try:
            meta_files[rel] = (root / rel).stat().st_mtime_ns
        except OSError:
            pass
    built_at = time.time()
    orm_edges = sum(1 for row in g.dump() if len(row) > 4 and any(
        ev.get("mechanism") == "python_orm_static_analysis"
        for ev in row[4].get("evidence", []) if isinstance(ev, dict)))
    dynamic_nodes = sum(1 for n in g.nodes.values() if n.get("dynamic_sql"))
    unknown_relations = sum(len(n.get("unknown_rels", [])) for n in g.nodes.values())
    coverage = {
        "database_catalog": db_info is not None,
        "python_analysis": True,
        "python_files_scanned": len(files["py"]),
        "typescript_files_discovered": len(files["ts"]),
        "sql_files_discovered": len(files["sql"]),
        "typescript_analysis": "complete" if ts_info.get("analysis_available") else (
            "not_applicable" if not files["ts"] else "skipped"),
        "tests_included": bool(a.include_tests),
        "tests_excluded_count": len(files.get("tests_excluded", [])),
        "python_parse_errors": py_info.get("parse_errors", 0),
        "typescript_parse_errors": ts_info.get("parse_errors", 0),
        "dynamic_sql_nodes": dynamic_nodes,
        "unknown_relation_references": unknown_relations,
        "orm_model_tables": sum(1 for n in g.nodes.values() if n.get("origin") == "orm_model"),
        "orm_models_detected": py_info.get("orm_models_detected", 0),
        "orm_evidence_edges": orm_edges,
        "orm_analysis": "conservative_static_patterns",
    }
    meta = {"version": 2, "built_at": built_at, "git_head": git_head(root), "db": db_info,
            "counts": dict(collections.Counter(n["type"] for n in g.nodes.values())),
            "coverage": coverage, "files": meta_files}
    data = {"meta": meta, "nodes": g.nodes, "edges": g.dump()}
    (out / "graph.json").write_text(json.dumps(data, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    write_readme(out, data, g)
    print(f"analysed {py_info.get('modules', 0)} python modules ({py_info.get('routes', 0)} FastAPI routes), "
          f"{ts_info.get('ts_files', 0)} ts/js files; excluded tests {len(files.get('tests_excluded', []))}")
    kinds = collections.Counter(k for (_, _, k) in g.edges)
    confs = collections.Counter(e["conf"] for e in g.edges.values())
    print(f"nodes {len(g.nodes)}: " + ", ".join(f"{k} {c}" for k, c in sorted(meta["counts"].items())))
    print(f"edges {len(g.edges)}: " + ", ".join(f"{k} {c}" for k, c in sorted(kinds.items())))
    print(f"confidence: high {confs[3]}, medium {confs[2]}, low {confs[1]}")
    print("coverage: " + json.dumps(coverage, sort_keys=True))
    for w in warnings:
        warn(w)
    print(f"wrote {out / 'graph.json'} in {time.time() - t0:.1f}s  (agent guide: {out / 'README.md'})")


def main():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    ap = argparse.ArgumentParser(description="Data lineage graph: build and query.")
    ap.add_argument("--root", help="repo root (default: git toplevel or cwd)")
    ap.add_argument("--graph", help="path to graph.json (default <root>/docs/lineage/graph.json)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="extract the graph (needs DB catalog read access unless --no-db)")
    b.add_argument("--dsn", help="postgres URL; else LINEAGE_DSN / DATABASE_URL / PG* env vars")
    b.add_argument("--schemas", help="comma-separated schemas (default: all non-system)")
    b.add_argument("--no-db", action="store_true", help="skip the database catalog (code-only graph)")
    b.add_argument("--include-tests", action="store_true", help="also scan test files (excluded by default)")
    b.add_argument("--config", help="JSON config (default <root>/docs/lineage/lineage.config.json)")
    b.add_argument("--out", help=f"output dir relative to root (default {OUT_DIR})")
    s = sub.add_parser("search", help="find nodes")
    s.add_argument("terms", nargs="+")
    s.add_argument("--type")
    s.add_argument("-n", type=int, default=25)
    sh = sub.add_parser("show", help="describe one node")
    sh.add_argument("node")
    t = sub.add_parser("trace", help="walk the lineage of a node")
    t.add_argument("node")
    t.add_argument("--up", action="store_true", help="only upstream (what it depends on)")
    t.add_argument("--down", action="store_true", help="only downstream (what depends on it)")
    t.add_argument("--depth", type=int, default=None, help="max hops (default 6 for trees, 12 for --files/--flat)")
    t.add_argument("--kinds", help="comma-separated edge kinds (default: all but fk)")
    t.add_argument("--min-conf", choices=list(CONF), default="low")
    t.add_argument("--files", action="store_true", help="list only the file:line ranges in the chain")
    t.add_argument("--flat", action="store_true", help="flat list with distances instead of a tree")
    t.add_argument("--only", help="comma-separated node types to list (implies flat)")
    t.add_argument("--max-lines", type=int, default=150)
    d = sub.add_parser("describe", help="compact upstream/downstream lineage overview")
    d.add_argument("node")
    d.add_argument("--direction", choices=("both", "up", "down"), default="both")
    d.add_argument("--depth", type=int, default=8)
    d.add_argument("--min-conf", choices=list(CONF), default="medium")
    d.add_argument("--format", choices=("text", "json"), default="text")
    d.add_argument("--section", action="append", help="comma-separated sections: identity,population,upstream,consumers,map,coverage (default: all)")
    d.add_argument("--max-nodes", type=int, default=80, help="maximum visited nodes per direction")
    d.add_argument("--max-items", type=int, default=20, help="maximum entries rendered per report group")

    ev = sub.add_parser("evidence", help="retrieve bounded source evidence for one node or relationship")
    ev.add_argument("node")
    ev.add_argument("--related-to", help="restrict relationship evidence to an adjacent node")
    ev.add_argument("--operation", action="append", help="comma-separated operation filter, e.g. INSERT,UPDATE")
    ev.add_argument("--max-lines", type=int, default=240, help="maximum source lines returned")
    ev.add_argument("--max-items", type=int, default=12, help="maximum source ranges/relationships")
    ev.add_argument("--format", choices=("text", "json"), default="text")
    sub.add_parser("check", help="lineage health report")
    a = ap.parse_args()
    if a.cmd == "build":
        return cmd_build(a)
    root = find_root(a.root)
    data = load_graph(root, a.graph)
    if a.cmd == "describe":
        if a.depth < 0 or a.max_nodes < 1 or a.max_items < 1:
            raise SystemExit("describe limits must be positive (depth may be zero)")
        return cmd_describe(data, a, root)
    if a.cmd == "evidence":
        if a.max_lines < 1 or a.max_items < 1:
            raise SystemExit("evidence limits must be positive")
        return cmd_evidence(data, a, root)
    {"search": cmd_search, "show": cmd_show, "trace": cmd_trace, "check": cmd_check}[a.cmd](data, a)


if __name__ == "__main__":
    main()
