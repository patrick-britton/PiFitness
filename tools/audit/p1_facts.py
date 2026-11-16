#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""p1_facts.py - evidence generators for Phase 1 (ground-truth documents).

Run from the repo root (Python 3.8+, standard library only, read-only):

    python tools/p1_facts.py all                 # everything below
    python tools/p1_facts.py inventory           # folders, sizes, REPO_MAP worksheet, import graph
    python tools/p1_facts.py backend             # FastAPI routes, SQL tables, env vars, wiring
    python tools/p1_facts.py frontend            # Next.js routes, API calls, mobile/PWA heuristics
    python tools/p1_facts.py refs                # stale references, duplicated text, sizes of docs/rules/skills
    python tools/p1_facts.py comments            # comment/docstring noise (IDs, history, agent dialog)

Options: --root PATH  --out DIR (default .audit/p1)  --depth N (folder roll-up, default 3)
         --skip a,b (path prefixes left out of scans; default skips TARGET_STATE.md, refactor fixtures, .features/tools/)
         --ignore a,b (extra folder names to skip)  --docs a,b (doc roots for `refs`)
         --terms a,b (stale terms for `refs`)  --max-rows N  --no-git

Nothing in the repo is modified. Output is Markdown (plus one TSV) under --out. Each fact carries a
path (and line where possible) so an agent drafting a document can cite it. Findings marked (h) are
heuristics: treat them as leads to verify, not as truth.
"""
import argparse
import ast
import datetime
import difflib
import io
import json
import os
import posixpath
import re
import subprocess
import sys
import tokenize
from collections import Counter, defaultdict
from pathlib import Path

IGNORE_DIRS = {
    ".git", "node_modules", ".venv", "venv", "env", "__pycache__", ".next", "dist", "build",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", "coverage", ".turbo", ".db_restore", ".idea",
    ".vscode", ".audit", ".cache", "site-packages", ".tox", ".nyc_output",
}
PY_EXT = (".py",)
TS_EXT = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs")
TEXT_LIMIT = 2000000
STDLIB_FALLBACK = set("""abc argparse array ast asyncio atexit base64 bisect calendar codecs collections concurrent
configparser contextlib copy csv ctypes dataclasses datetime decimal difflib email enum errno fnmatch fractions
functools getpass glob gzip hashlib heapq hmac html http importlib inspect io itertools json locale logging math
mimetypes multiprocessing numbers operator os pathlib pickle platform pprint queue random re sched secrets select
selectors shlex shutil signal smtplib socket sqlite3 ssl statistics string struct subprocess sys tarfile tempfile
textwrap threading time tokenize tomllib traceback types typing unicodedata unittest urllib uuid warnings weakref
xml zipfile zlib zoneinfo __future__""".split())


def is_stdlib(name):
    s = getattr(sys, "stdlib_module_names", None)
    return name in (s if s else STDLIB_FALLBACK)


def fkind(f):
    """Classify a file path: test, migration, tool or app."""
    low = f.lower()
    if low.startswith("tests/") or "/tests/" in low or "__tests__" in low or ".test." in low or ".spec." in low:
        return "test"
    if "/migrations/" in low or low.endswith(".sql"):
        return "migration"
    if low.startswith(("tools/", "scripts/", ".features/", "deploy/scripts/", "agents/")):
        return "tool"
    return "app"


def esc(c):
    return str(c).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def md_table(headers, rows):
    if not rows:
        return "none"
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(esc(c) for c in r) + " |")
    return "\n".join(out)


def capped(rows, n):
    if len(rows) <= n:
        return rows, ""
    return rows[:n], "\n_(showing %d of %d rows; raise --max-rows to see more)_\n" % (n, len(rows))


class Repo:
    def __init__(self, root, extra_ignore, skip=()):
        self.skip = tuple(x.strip().replace("\\", "/") for x in skip if x and x.strip())
        self.root = Path(root).resolve()
        self.ign = IGNORE_DIRS | set(x for x in extra_ignore if x)
        self.dirs = {}
        self.files = []
        self._cache = {}
        for dp, dns, fns in os.walk(str(self.root)):
            dns[:] = sorted(d for d in dns if d not in self.ign)
            rd = Path(dp).relative_to(self.root).as_posix()
            rd = "" if rd == "." else rd
            self.dirs[rd] = sorted(fns)
            for f in sorted(fns):
                self.files.append((rd + "/" + f) if rd else f)
        self.fileset = set(self.files)

    def path(self, rel):
        return self.root / rel

    def skipped(self, rel):
        return any(rel == p or rel.startswith(p) for p in self.skip)

    def read(self, rel):
        if rel in self._cache:
            return self._cache[rel]
        p = self.root / rel
        try:
            txt = "" if p.stat().st_size > TEXT_LIMIT else p.read_bytes().decode("utf-8-sig", "replace")
        except OSError:
            txt = ""
        if rel.endswith(PY_EXT + TS_EXT):
            self._cache[rel] = txt
        return txt

    def stat(self, rel):
        try:
            s = self.path(rel).stat()
            return s.st_size, s.st_mtime
        except OSError:
            return 0, 0.0


def header(title, cmd, repo):
    return "# %s\n\n_Generated %s by p1_facts.py %s. Repo: %s. Items marked (h) are heuristics: verify before relying on them._\n\n" % (
        title, datetime.date.today().isoformat(), cmd, repo.root)


def emit(out_dir, name, text, summary):
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / name
    with io.open(str(p), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    print("wrote %s  (%s)" % (p, summary))
    return summary


# ----------------------------------------------------------------------------- import graph

def build_graph(repo, depth):
    pyfiles = set(f for f in repo.files if f.endswith(PY_EXT))
    tsfiles = set(f for f in repo.files if f.endswith(TS_EXT))
    roots = sorted(d for d in repo.dirs if d.count("/") <= 1)
    pkg_roots = sorted((posixpath.dirname(f) for f in repo.files if posixpath.basename(f) == "package.json"),
                       key=lambda r: -len(r))
    edges = Counter()
    ext = defaultdict(Counter)
    unresolved = Counter()
    bad_syntax = []

    def roll(f):
        eff = depth
        for pr in pkg_roots:
            if pr and f.startswith(pr + "/"):
                eff = max(depth, len(pr.split("/")) + 2)
                break
        parts = f.split("/")[:-1][:eff]
        return "/".join(parts) or "."

    def resolve_py(mod, level, names, fdir):
        if level:
            base = fdir
            for _ in range(level - 1):
                base = posixpath.dirname(base)
            bases = [base]
        else:
            bases = roots
        modpath = mod.replace(".", "/") if mod else ""
        for b in bases:
            prefix = posixpath.join(b, modpath) if modpath else b
            trials = [posixpath.join(prefix, n) for n in names if n != "*"]
            trials.append(prefix)
            for t in trials:
                for c in (t + ".py", posixpath.join(t, "__init__.py")):
                    if c in pyfiles and c.strip("/"):
                        return c
        return None

    def try_ts(base):
        base = posixpath.normpath(base)
        if base == ".":
            base = ""
        for e in ("", ".ts", ".tsx", ".js", ".jsx", ".mjs"):
            if base + e in tsfiles:
                return base + e
        for idx in ("index.ts", "index.tsx", "index.js", "index.jsx"):
            c = posixpath.join(base, idx)
            if c in tsfiles:
                return c
        return None

    def resolve_ts(spec, f):
        if spec.startswith("."):
            return try_ts(posixpath.join(posixpath.dirname(f), spec))
        if spec.startswith("@/") or spec.startswith("~/"):
            rest = spec[2:]
            d = posixpath.dirname(f)
            while True:
                if posixpath.join(d, "tsconfig.json") in repo.fileset or posixpath.join(d, "package.json") in repo.fileset:
                    for c in (posixpath.join(d, "src", rest), posixpath.join(d, rest)):
                        r = try_ts(c)
                        if r:
                            return r
                if not d:
                    break
                d = posixpath.dirname(d)
        return None

    ts_re = re.compile(
        r"""(?:import|export)\s+(?:type\s+)?(?:[^'"`;]*?\s+from\s+)?['"]([^'"]+)['"]|require\(\s*['"]([^'"]+)['"]\s*\)|import\(\s*['"]([^'"]+)['"]\s*\)""")

    for f in sorted(pyfiles):
        if repo.skipped(f):
            continue
        try:
            tree = ast.parse(repo.read(f))
        except (SyntaxError, ValueError):
            bad_syntax.append(f)
            continue
        fdir = posixpath.dirname(f)
        for node in ast.walk(tree):
            items = []
            if isinstance(node, ast.Import):
                items = [(al.name, 0, []) for al in node.names]
            elif isinstance(node, ast.ImportFrom):
                items = [(node.module or "", node.level or 0, [al.name for al in node.names])]
            for mod, level, names in items:
                dst = resolve_py(mod, level, names, fdir)
                if dst:
                    a, b = roll(f), roll(dst)
                    if a != b:
                        edges[(a, b)] += 1
                elif level == 0 and mod:
                    top = mod.split(".")[0]
                    if not is_stdlib(top):
                        ext[roll(f)][top] += 1
    for f in sorted(tsfiles):
        if repo.skipped(f):
            continue
        text = repo.read(f)
        for m in ts_re.finditer(text):
            spec = m.group(1) or m.group(2) or m.group(3)
            if not spec or spec.startswith("node:"):
                continue
            if spec.startswith(".") or spec.startswith("@/") or spec.startswith("~/"):
                dst = resolve_ts(spec, f)
                if dst:
                    a, b = roll(f), roll(dst)
                    if a != b:
                        edges[(a, b)] += 1
                else:
                    unresolved[roll(f)] += 1
            else:
                name = "/".join(spec.split("/")[:2]) if spec.startswith("@") else spec.split("/")[0]
                ext[roll(f)][name] += 1
    return edges, ext, unresolved, bad_syntax


# ----------------------------------------------------------------------------- inventory

def git_last(repo, rel):
    try:
        r = subprocess.run(["git", "-C", str(repo.root), "log", "-1", "--format=%cs", "--", rel or "."],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        return r.stdout.decode("utf-8", "replace").strip() or "-"
    except (OSError, subprocess.SubprocessError):
        return "-"


def cmd_inventory(repo, a, out):
    stats = {}

    def get(k):
        if k not in stats:
            stats[k] = {"files": 0, "bytes": 0, "exts": Counter(), "newest": 0.0, "own": 0}
        return stats[k]

    for rd in repo.dirs:
        get(rd)
    for f in repo.files:
        parts = f.split("/")
        size, mt = repo.stat(f)
        ext = os.path.splitext(f)[1].lower() or "(none)"
        for i in range(len(parts)):
            s = get("/".join(parts[:i]))
            s["files"] += 1
            s["bytes"] += size
            s["exts"][ext] += 1
            s["newest"] = max(s["newest"], mt)
        get("/".join(parts[:-1]))["own"] += 1
    use_git = (not a.no_git) and (repo.root / ".git").exists()
    keys = sorted(k for k in stats if k and k.count("/") + 1 <= a.depth)
    markers = ("README", "__init__.py", "package.json", "pyproject.toml", "requirements", "tsconfig.json",
               "Dockerfile", "docker-compose", "pytest.ini", "SKILL.md")
    rows, empties = [], []
    for k in keys:
        s = stats[k]
        notable = [n for n in repo.dirs.get(k, []) if any(n.startswith(m) or n == m for m in markers)][:5]
        last = git_last(repo, k) if use_git and k.count("/") < 2 else ""
        if s["files"] == 0:
            empties.append(k)
        rows.append([k + "/", s["files"], "%.0f" % (s["bytes"] / 1024.0), s["own"],
                     ", ".join("%s:%d" % (e, c) for e, c in s["exts"].most_common(3)),
                     datetime.date.fromtimestamp(s["newest"]).isoformat() if s["newest"] else "-",
                     last, ", ".join(notable)])
    edges, ext, unresolved, bad_syntax = build_graph(repo, a.depth)
    r1, note1 = capped(rows, a.max_rows)
    txt = header("Repo inventory", "inventory", repo)
    txt += "Folders to depth %d. `files` is recursive; `own` counts files directly in the folder.\n\n" % a.depth
    txt += md_table(["folder", "files", "KB", "own", "top extensions", "newest file", "last commit", "notable files"], r1) + note1
    if empties:
        txt += "\n\n## Empty folders (no files at all)\n\n" + "\n".join("- `%s/`" % e for e in empties)
    top_files = [f for f in repo.files if "/" not in f]
    txt += "\n\n## Files at the repo root\n\n" + (", ".join("`%s`" % f for f in top_files) or "none")
    stray = [f for f in repo.files if os.path.splitext(f)[1].lower() in (".out", ".log", ".bak", ".tmp", ".orig", ".rej", ".swp")]
    txt += "\n\n## Possible stray files (.out .log .bak .tmp .orig) (h)\n\n" + (
        "\n".join("- `%s`" % f for f in stray[:40]) if stray else "none")
    big = sorted(((repo.stat(f)[0], f) for f in repo.files), reverse=True)[:12]
    txt += "\n\n## Largest files\n\n" + md_table(["KB", "file"], [["%.0f" % (sz / 1024.0), f] for sz, f in big])
    for ig in (".clineignore", ".gitignore"):
        if ig in repo.fileset:
            lines = [ln for ln in repo.read(ig).split("\n") if ln.strip() and not ln.strip().startswith("#")]
            txt += "\n\n## %s (non-comment lines, first 60)\n\n```\n%s\n```" % (ig, "\n".join(lines[:60]))
    emit(out, "inventory.md", txt, "%d folders, %d empty" % (len(keys), len(empties)))

    # dependencies
    erows = sorted(edges.items(), key=lambda kv: -kv[1])
    mutual = sorted(set(tuple(sorted((x, y))) for (x, y) in edges if (y, x) in edges))
    e1, enote = capped([[x + "/", y + "/", c] for (x, y), c in erows], a.max_rows)
    dtxt = header("Folder dependency graph (observed imports)", "inventory", repo)
    dtxt += "Rolled up to depth %d. Python and TS/JS imports that resolve to a file inside the repo.\n\n" % a.depth
    dtxt += md_table(["from folder", "imports from folder", "import count"], e1) + enote
    dtxt += "\n\n## Mutual dependencies (A imports B and B imports A)\n\n"
    dtxt += ("\n".join("- `%s/` <-> `%s/`" % m for m in mutual) if mutual else "none found")
    dtxt += "\n\n## External packages per folder (top 10)\n\n"
    erows2 = [[k + "/", ", ".join("%s (%d)" % (n, c) for n, c in v.most_common(10))] for k, v in sorted(ext.items())]
    dtxt += md_table(["folder", "packages"], erows2)
    if unresolved:
        dtxt += "\n\n## Unresolved internal-looking imports (h)\n\n" + "\n".join(
            "- `%s/`: %d" % (k, v) for k, v in sorted(unresolved.items()))
    if bad_syntax:
        dtxt += "\n\n## Python files that failed to parse\n\n" + "\n".join("- `%s`" % f for f in bad_syntax)
    emit(out, "deps.md", dtxt, "%d folder edges, %d mutual, %d unparseable py" % (len(edges), len(mutual), len(bad_syntax)))

    # REPO_MAP worksheet
    wtxt = header("REPO_MAP worksheet (fill from the code, then human-verify)", "inventory", repo)
    wtxt += ("Instructions for the drafting agent: for each row read the folder's files, then replace every TODO. "
             "Cite at least one file path per Purpose. `Observed depends on` is generated; do not edit it. "
             "Leave a TODO in place if you cannot verify it.\n\n")
    wrows = []
    for k in keys:
        if stats[k]["files"] == 0:
            continue
        outs = sorted(set(y for (x, y) in edges if (x == k or x.startswith(k + "/")) and not (y == k or y.startswith(k + "/"))))
        wrows.append([k + "/", "TODO", "TODO (human / agent / generated)", "TODO", ", ".join(o + "/" for o in outs[:8]) or "-"])
    w1, wnote = capped(wrows, max(a.max_rows, 300))
    wtxt += md_table(["folder", "purpose (cite a file)", "written by", "must not contain", "observed depends on"], w1) + wnote
    emit(out, "repo_map_worksheet.md", wtxt, "%d rows" % len(wrows))
    return "inventory: %d folders, %d empty, %d mutual deps" % (len(keys), len(empties), len(mutual))


# ----------------------------------------------------------------------------- backend facts

HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}
SQL_REF = re.compile(
    r"(?i)\b(from|join|into|update|truncate\s+table|delete\s+from)\s+(\"?[a-z_][\w]*\"?\s*\.\s*\"?[a-z_][\w]*\"?)")
SQL_CREATE = re.compile(
    r"(?i)\bcreate\s+(?:or\s+replace\s+)?(?:unlogged\s+)?(table|view|materialized\s+view)\s+(?:if\s+not\s+exists\s+)?([\w.\"]+)")
ENV_PY = re.compile(r"""os\.environ\[['"](\w+)['"]\]|os\.environ\.get\(\s*['"](\w+)['"]|os\.getenv\(\s*['"](\w+)['"]""")
BG_WORDS = re.compile(r"apscheduler|BackgroundTasks|asyncio\.create_task|threading\.Thread|schedule\.every|celery|\.on_event\(|lifespan")


def _call_name(call):
    f = call.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _kw_str(call, name):
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def _first_str(call):
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return _kw_str(call, "path")


def _stem(f):
    p = Path(f)
    return p.parent.name if p.stem == "__init__" else p.stem


def scan_backend(repo):
    routers, endpoints, includes, wiring = {}, [], [], []
    tables = defaultdict(lambda: {"read": set(), "write": set(), "defined": set(), "test": set(), "migration": set(), "tool": set()})
    envs = defaultdict(set)
    bg = []
    bad = []
    aliases = {}
    for f in repo.files:
        if repo.skipped(f):
            continue
        if f.endswith(PY_EXT):
            src = repo.read(f)
            try:
                tree = ast.parse(src)
            except (SyntaxError, ValueError):
                bad.append(f)
                tree = None
            if tree is not None:
                al = {}
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        for x in node.names:
                            al[x.asname or x.name] = (node.module or "", x.name)
                    elif isinstance(node, ast.Import):
                        for x in node.names:
                            al[x.asname or x.name.split(".")[0]] = (x.name, None)
                aliases[f] = al
                for node in ast.walk(tree):
                    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                        cn = _call_name(node.value)
                        if cn in ("APIRouter", "FastAPI"):
                            for t in node.targets:
                                if isinstance(t, ast.Name):
                                    routers[(f, t.id)] = {"kind": cn, "prefix": _kw_str(node.value, "prefix") or "",
                                                          "line": node.lineno, "include": None}
                    elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        for dec in node.decorator_list:
                            if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                                attr = dec.func.attr
                                base = dec.func.value.id if isinstance(dec.func.value, ast.Name) else "?"
                                if attr in HTTP_METHODS or attr in ("api_route", "websocket"):
                                    path = _first_str(dec)
                                    if path is None:
                                        continue
                                    endpoints.append({"file": f, "line": dec.lineno, "router": base,
                                                      "method": attr.upper() if attr in HTTP_METHODS else attr,
                                                      "path": path, "func": node.name})
                                elif attr in ("on_event", "middleware"):
                                    wiring.append((f, dec.lineno, "@%s.%s(...) on %s" % (base, attr, node.name)))
                    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                        attr = node.func.attr
                        if attr == "include_router" and node.args:
                            includes.append({"file": f, "line": node.lineno,
                                             "text": ast.get_source_segment(src, node.args[0]) or "?",
                                             "prefix": _kw_str(node, "prefix") or ""})
                        elif attr in ("add_middleware", "mount", "add_exception_handler"):
                            wiring.append((f, node.lineno, (ast.get_source_segment(src, node) or attr).split("\n")[0][:110]))
            for i, line in enumerate(src.split("\n"), 1):
                for m in ENV_PY.finditer(line):
                    envs[m.group(1) or m.group(2) or m.group(3)].add("%s:%d" % (f, i))
                if BG_WORDS.search(line):
                    bg.append((f, i, line.strip()[:110]))
        if f.endswith(PY_EXT) or f.endswith(".sql"):
            src = repo.read(f)
            for i, line in enumerate(src.split("\n"), 1):
                if f.endswith(".py") and re.match(r"^\s*(from\s+[\w.]+\s+import\b|import\s)", line):
                    continue
                for m in SQL_REF.finditer(line):
                    kw = m.group(1).lower()
                    name = re.sub(r"[\"\s]", "", m.group(2)).lower()
                    schema, _, tbl = name.partition(".")
                    if schema in ("self", "os", "np", "pd", "re", "json", "excluded", "information_schema", "pg_catalog", "pg_temp", "sys") or tbl.endswith((".py", ".ts", ".tsx")):
                        continue
                    kind = "write" if kw in ("into", "update") or kw.startswith(("truncate", "delete")) else "read"
                    fk = fkind(f)
                    tables[name][kind if fk == "app" else fk].add(f)
                for m in SQL_CREATE.finditer(line):
                    tables[m.group(2).replace('"', "").lower()]["defined"].add("%s:%d" % (f, i))
    # resolve include prefixes
    for inc in includes:
        text, af = inc["text"], aliases.get(inc["file"], {})
        target = None
        if "." in text:
            base, var = text.rsplit(".", 1)
            first = base.split(".")[0]
            if first in af:
                mod, nm = af[first]
                stem = nm if nm and "." not in base else (mod.split(".")[-1] if not nm else base.split(".")[-1])
            else:
                stem = base.split(".")[-1]
            target = (stem, var)
        elif text in af:
            mod, nm = af[text]
            target = (mod.split(".")[-1], nm)
        else:
            target = (_stem(inc["file"]), text)
        for (rf, rv), meta in routers.items():
            if meta["kind"] == "APIRouter" and rv == target[1] and (_stem(rf) == target[0]):
                meta["include"] = inc["prefix"]
    for e in endpoints:
        meta = routers.get((e["file"], e["router"]))
        if meta is None:
            e["full"], e["mounted"] = e["path"], "?"
        elif meta["kind"] == "FastAPI":
            e["full"], e["mounted"] = (meta["prefix"] + e["path"]), "app"
        else:
            inc = meta["include"]
            e["full"] = ((inc or "") + meta["prefix"] + e["path"])
            e["mounted"] = "yes" if inc is not None else "NO include found (h)"
        e["full"] = re.sub(r"/{2,}", "/", e["full"]) or "/"
    return {"routers": routers, "endpoints": endpoints, "includes": includes, "wiring": wiring,
            "tables": tables, "envs": envs, "bg": bg, "bad": bad}


def cmd_backend(repo, a, out, data=None):
    d = data or scan_backend(repo)
    txt = header("Backend facts", "backend", repo)
    eps = sorted(d["endpoints"], key=lambda e: (e["full"], e["method"]))
    rows = [[e["method"], e["full"], "%s:%d" % (e["file"], e["line"]), e["func"], e["mounted"]] for e in eps]
    r1, note = capped(rows, a.max_rows)
    txt += "## HTTP endpoints (%d)\n\nPath = include prefix + router prefix + route path, resolved by name (h).\n\n" % len(rows)
    txt += md_table(["method", "path", "defined at", "function", "mounted"], r1) + note
    unm = sorted(set("%s (%s)" % (rv, rf) for (rf, rv), m in d["routers"].items()
                     if m["kind"] == "APIRouter" and m["include"] is None))
    txt += "\n\n## Routers with no include_router found (h) - possibly dead or wired indirectly\n\n"
    txt += "\n".join("- `%s`" % u for u in unm) if unm else "none"
    txt += "\n\n## App wiring (middleware, mounts, handlers, lifecycle)\n\n"
    wr, wn = capped([["%s:%d" % (f, l), t] for f, l, t in d["wiring"]], a.max_rows)
    txt += md_table(["at", "what"], wr) + wn
    txt += "\n\n## Background work / scheduling mentions (h)\n\n"
    br, bn = capped([["%s:%d" % (f, l), t] for f, l, t in d["bg"]], a.max_rows)
    txt += md_table(["at", "line"], br) + bn
    txt += "\n\n## Environment variables read by Python code\n\n"
    er = [[k, len(v), ", ".join(sorted(v)[:3])] for k, v in sorted(d["envs"].items())]
    er1, en = capped(er, a.max_rows)
    txt += md_table(["variable", "uses", "first locations"], er1) + en
    txt += "\n\n## SQL tables referenced in code (h: schema-qualified names only)\n\n"
    txt += ("Readers and writers count only application files (backend and frontend code). Test files, migrations and tools are counted "
            "separately so they do not inflate the writer count. More than one app writer file is a candidate for duplicated or "
            "inconsistent logic. `defined in` comes from CREATE statements in code or .sql files.\n\n")
    schema_count = Counter(n.split(".")[0] for n in d["tables"])
    trs, low = [], []
    for name, v in sorted(d["tables"].items(), key=lambda kv: (-len(kv[1]["write"]), -len(kv[1]["read"]), kv[0])):
        row = [name, len(v["read"]), len(v["write"]), ", ".join(sorted(v["write"])[:3]) or "-",
               len(v["test"]), len(v["migration"]), ", ".join(sorted(v["defined"])[:2]) or "-"]
        if schema_count[name.split(".")[0]] == 1 and not v["defined"] and not v["migration"]:
            low.append(row)
        else:
            trs.append(row)
    heads = ["table", "app reader files", "app writer files", "app writers (first 3)", "test files", "migration files", "defined in"]
    tr1, tn = capped(trs, a.max_rows)
    txt += md_table(heads, tr1) + tn
    multi = [t[0] for t in trs if t[2] > 1]
    txt += "\n\n## Tables written from more than one application file (h)\n\n" + (
        "\n".join("- `%s` (%s)" % (t[0], t[3]) for t in trs if t[2] > 1) if multi else "none")
    txt += "\n\n## Low-confidence table references (schema appears only once, no CREATE found) (h)\n\n"
    txt += md_table(heads, low[:a.max_rows]) if low else "none"
    if d["bad"]:
        txt += "\n\n## Python files that failed to parse\n\n" + "\n".join("- `%s`" % f for f in d["bad"])
    emit(out, "backend.md", txt, "%d endpoints, %d tables, %d env vars, %d multi-writer tables"
         % (len(eps), len(trs), len(d["envs"]), len(multi)))
    return "backend: %d endpoints, %d unmounted routers, %d tables (%d multi-writer), %d env vars" % (
        len(eps), len(unm), len(trs), len(multi), len(d["envs"]))


# ----------------------------------------------------------------------------- frontend facts

API_LIT = re.compile(r"""([`'"])(/api/[^`'"\s]*)\1""")
API_BASE = re.compile(r"""`\$\{[A-Za-z_.]*(?:BASE|URL|API|api|base|url)[A-Za-z_.]*\}(/[^`\s]*)`""")
LOADING = re.compile(r"(?i)\b(loading|isLoading|pending|isPending|spinner|skeleton|saving|submitting|isFetching|isSaving)\b")
EXPORT_RE = re.compile(r"export\s+(?:default\s+)?(?:async\s+)?(?:const|function|type|interface|class|enum)\s+([A-Za-z_]\w*)")


def _api_ok(p):
    return not (":" in p or "*" in p) and p.count("/") >= 2


def _norm_api(p):
    p = p.split("?")[0]
    p = re.sub(r"\$\{[^}]*\}", "{}", p)
    p = re.sub(r"\{[^}]*\}", "{}", p).rstrip("/")
    return p or "/"


def _seg_match(x, y):
    sx, sy = x.strip("/").split("/"), y.strip("/").split("/")
    return len(sx) == len(sy) and all(p == q or p == "{}" or q == "{}" for p, q in zip(sx, sy))


def _api_match(fe, be):
    fvars = {fe}
    if fe.startswith("/api/"):
        fvars.add(fe[4:])
    bvars = {be}
    if not be.startswith("/api/"):
        bvars.add("/api" + be)
    return any(_seg_match(x, y) for x in fvars for y in bvars)


def cmd_frontend(repo, a, out, backend=None):
    pkgs = []
    for f in repo.files:
        if f.endswith("package.json") and posixpath.basename(f) == "package.json":
            try:
                j = json.loads(repo.read(f) or "{}")
            except ValueError:
                continue
            deps = dict(j.get("dependencies", {}))
            deps.update(j.get("devDependencies", {}))
            if "next" in deps:
                pkgs.append((posixpath.dirname(f), j, deps))
    txt = header("Frontend facts", "frontend", repo)
    if not pkgs:
        txt += "No package.json with a `next` dependency was found.\n"
        emit(out, "frontend.md", txt, "no Next.js package found")
        return "frontend: no Next.js package found"
    calls = defaultdict(set)
    summary_bits = []
    for root, j, deps in pkgs:
        pre = (root + "/") if root else ""
        files = [f for f in repo.files if f.startswith(pre) and f.endswith(TS_EXT + (".css",))]
        tsx = [f for f in files if f.endswith(TS_EXT)]
        txt += "## Package `%s/` (next %s, react %s)\n\n" % (root or ".", deps.get("next", "?"), deps.get("react", "?"))
        # routes
        routes, handlers, layouts = [], [], []
        for base in ("app", "src/app"):
            ab = pre + base + "/"
            for f in tsx:
                if not f.startswith(ab):
                    continue
                name = posixpath.basename(f)
                stem = name.rsplit(".", 1)[0]
                segs = [s for s in posixpath.dirname(f[len(ab):]).split("/") if s]
                segs = [s for s in segs if not (s.startswith("(") and s.endswith(")")) and not s.startswith("@")]
                route = "/" + "/".join(segs)
                client = "'use client'" in repo.read(f)[:300] or '"use client"' in repo.read(f)[:300]
                if stem == "page":
                    routes.append([route, f, "client" if client else "server", len(repo.read(f).split("\n"))])
                elif stem == "route":
                    handlers.append([route, f])
                elif stem == "layout":
                    layouts.append([route, f])
        for base in ("pages", "src/pages"):
            pb = pre + base + "/"
            for f in tsx:
                if f.startswith(pb):
                    rel = f[len(pb):].rsplit(".", 1)[0]
                    if rel.startswith("_"):
                        continue
                    (handlers if rel.startswith("api/") else routes).append(
                        ["/" + rel.replace("/index", "").replace("index", ""), f] if rel.startswith("api/")
                        else ["/" + rel.replace("/index", "").replace("index", ""), f, "pages", len(repo.read(f).split("\n"))])
        txt += "### Pages (%d)\n\n" % len(routes) + md_table(["route", "file", "mode", "lines"], sorted(routes)) + "\n\n"
        txt += "### Route handlers (%d)\n\n" % len(handlers) + (md_table(["route", "file"], sorted(handlers)) if handlers else "none") + "\n\n"
        txt += "### Layouts\n\n" + (md_table(["scope", "file"], sorted(layouts)) if layouts else "none") + "\n\n"
        # large files
        sizes = sorted(((len(repo.read(f).split("\n")), f) for f in tsx), reverse=True)[:12]
        txt += "### Largest source files (lines)\n\n" + md_table(["lines", "file"], sizes) + "\n\n"
        # key shared modules
        shared = [f for f in tsx if re.search(r"(?i)(store|contract|api[-_]?client|context|hooks?/|use[A-Z]\w+)", f)]
        srows = []
        for f in shared[:40]:
            names = sorted(set(EXPORT_RE.findall(repo.read(f))))
            srows.append([f, ", ".join(names[:20]) + (" ..." if len(names) > 20 else "")])
        txt += "### Shared modules by name pattern: stores, contracts, api client, contexts, hooks (h)\n\n"
        txt += (md_table(["file", "exports"], srows) if srows else "none") + "\n\n"
        # api calls
        for f in tsx:
            t = repo.read(f)
            for m in API_LIT.finditer(t):
                calls[_norm_api(m.group(2))].add(f)
            for m in API_BASE.finditer(t):
                calls[_norm_api(m.group(1))].add(f)
        # mobile / pwa heuristics
        hits = []

        def count(pattern, label, flags=0):
            rx = re.compile(pattern, flags)
            n, ex = 0, []
            for f in files:
                c = len(rx.findall(repo.read(f)))
                if c:
                    n += c
                    ex.append(f)
            hits.append([label, n, ", ".join(ex[:3])])

        manifests = [f for f in repo.files if f.startswith(pre) and re.search(r"(^|/)manifest\.(json|webmanifest|ts|js)$", f)]
        sw = [f for f in repo.files if f.startswith(pre) and re.search(r"(^|/)(sw|service-worker|serviceworker)\.(js|ts)$", f, re.I)]
        pwa_deps = [d for d in deps if re.search(r"pwa|serwist|workbox", d)]
        hits.append(["web app manifest files", len(manifests), ", ".join(manifests[:3])])
        hits.append(["service worker files", len(sw), ", ".join(sw[:3])])
        hits.append(["PWA-related dependencies", len(pwa_deps), ", ".join(pwa_deps)])
        count(r"viewport", "mentions of `viewport` (meta/export)")
        count(r"viewport-fit|viewportFit", "viewport-fit (edge-to-edge)")
        count(r"theme-color|themeColor", "theme-color")
        count(r"\b100vh\b|h-screen|min-h-screen", "100vh / h-screen usages (use dvh on mobile)")
        count(r"\bdvh\b|\bsvh\b", "dvh/svh usages")
        count(r"safe-area-inset|safe-area", "safe-area usages")
        count(r"<input\b", "<input> elements")
        count(r"inputMode\s*=", "inputMode attributes")
        count(r"<div[^>]*\bonClick\s*=", "div elements with onClick (not a button) (h)")
        count(r"\bhover:", "tailwind hover: classes")
        count(r"\bactive:", "tailwind active: classes (pressed state)")
        count(r"overscroll-behavior|overscroll-", "overscroll-behavior")
        count(r"touch-action|touch-manipulation", "touch-action manipulation")
        count(r"window\.(confirm|alert|prompt)\(", "window.confirm/alert/prompt (blocking dialogs)")
        txt += "### Mobile and installability signals (h)\n\n" + md_table(["signal", "count", "examples"], hits) + "\n\n"
        flagged = []
        for f in tsx:
            if not f.endswith((".tsx", ".jsx")) or fkind(f) == "test":
                continue
            t = repo.read(f)
            if re.search(r"\bfetch\(|\bAPI\.\w+|\baxios\.|\bawait\s", t) and re.search(r"onClick|onSubmit", t) and not LOADING.search(t):
                flagged.append(f)
        txt += "### Components that call the API from a handler but show no loading/saving state (h)\n\n"
        txt += ("\n".join("- `%s`" % f for f in flagged[:a.max_rows]) if flagged else "none") + "\n\n"
        envs = defaultdict(set)
        for f in tsx:
            for m in re.finditer(r"process\.env\.(\w+)|process\.env\[['\"](\w+)['\"]\]", repo.read(f)):
                envs[m.group(1) or m.group(2)].add(f)
        txt += "### Environment variables read by frontend code\n\n"
        txt += (md_table(["variable", "files"], [[k, ", ".join(sorted(v)[:3])] for k, v in sorted(envs.items())]) if envs else "none") + "\n\n"
        summary_bits.append("%s: %d pages, %d flagged for missing loading state" % (root or ".", len(routes), len(flagged)))
    # cross-check
    txt += "## Frontend API calls vs backend endpoints (h)\n\n"
    be = (backend or scan_backend(repo))["endpoints"]
    bnorm = [(_norm_api(e["full"]), e) for e in be]
    fe_unmatched = sorted(p for p in calls if _api_ok(p) and not any(_api_match(p, b) for b, _ in bnorm))
    be_unused = sorted(set((b, e["method"], "%s:%d" % (e["file"], e["line"])) for b, e in bnorm
                           if not any(_api_match(p, b) for p in calls)))
    txt += ("Matching is by path only (methods ignored), with `{}` as wildcard and an optional `/api` prefix on either side. "
            "A mismatch can also mean the URL is built dynamically.\n\n")
    txt += "### Frontend calls with no matching backend endpoint (%d) - possible broken calls\n\n" % len(fe_unmatched)
    txt += (md_table(["path", "used in"], [[p, ", ".join(sorted(calls[p])[:3])] for p in fe_unmatched[:a.max_rows]]) if fe_unmatched else "none") + "\n\n"
    txt += "### Backend endpoints never called by the frontend (%d) - dead code, other clients, or tests\n\n" % len(be_unused)
    txt += (md_table(["path", "method", "defined at"], [list(x) for x in be_unused[:a.max_rows]]) if be_unused else "none")
    emit(out, "frontend.md", txt, "; ".join(summary_bits) + "; %d unmatched calls, %d unused endpoints" % (len(fe_unmatched), len(be_unused)))
    return "frontend: " + "; ".join(summary_bits) + "; %d frontend calls unmatched, %d backend endpoints unused" % (len(fe_unmatched), len(be_unused))


# ----------------------------------------------------------------------------- docs / rules / skills

DOC_EXTS = (".md", ".mdc", ".txt")
PATH_EXTS = ("py", "md", "mdc", "ts", "tsx", "js", "jsx", "json", "sql", "sh", "ps1", "yml", "yaml", "txt", "toml", "ini", "env", "html", "css")
TICK = re.compile(r"`([^`\n]+)`")
MDLINK = re.compile(r"\]\(([^)\s]+)\)")


def doc_files(repo, a):
    if a.docs:
        prefixes = [p.strip().strip("/") for p in a.docs.split(",") if p.strip()]
    else:
        prefixes = ["docs", ".clinerules", ".agents", "memory-bank", ".features"]
    out = []
    for f in repo.files:
        if not f.endswith(DOC_EXTS):
            continue
        parts = f.split("/")
        if len(parts) == 1 and not a.docs:
            out.append(f)
            continue
        if parts[0] in prefixes or any(f.startswith(p + "/") for p in prefixes):
            if parts[0] == ".features" and any(p.startswith(("designs_", "zz_")) for p in parts[1:3]):
                continue
            out.append(f)
    return out


def cmd_refs(repo, a, out):
    docs = [d for d in doc_files(repo, a) if not repo.skipped(d)]
    top = set(k for k in repo.dirs if k and "/" not in k)
    base_index = defaultdict(list)
    for f in repo.files:
        base_index[posixpath.basename(f)].append(f)
    base_names = list(base_index)
    missing, inbound = [], Counter()
    stale_terms = [t.strip().lower() for t in a.terms.split(",") if t.strip()]
    stale = defaultdict(list)
    line_owner = defaultdict(set)
    ext_rx = re.compile(r"\.(%s)$" % "|".join(PATH_EXTS))

    def candidate(tok):
        t = tok.strip().rstrip(".,;:").lstrip(",;:")
        t = t.split("::")[0]
        if not t or t.startswith(("http://", "https://", "mailto:", "#", "/", "~", "%")) or re.match(r"^[A-Za-z]:[\\/]", t):
            return None
        t = re.sub(r"[:#]L?\d+(?:[-:]L?\d+)?$", "", t).split("#", 1)[0].replace("\\", "/")
        if any(c in t for c in " *<>{}$|()[]=,\"'%"):
            return None
        if t.startswith("./"):
            t = t[2:]
        if not t:
            return None
        if "/" in t:
            first = t.split("/")[0]
            if ext_rx.search(t) or t.endswith("/") or first in top or first.startswith("."):
                return t
            return None
        return t if ext_rx.search(t) and "." in t else None

    for d in docs:
        text = repo.read(d)
        lines = text.split("\n")
        for i, line in enumerate(lines, 1):
            low = line.lower()
            for term in stale_terms:
                if term in low:
                    stale[(d, term)].append(i)
            norm = re.sub(r"\s+", " ", line.strip().lower())
            if len(norm) >= 60 and not set(norm) <= set("-|: "):
                line_owner[norm].add(d)
            toks = [m.group(1) for m in TICK.finditer(line)] + [m.group(1) for m in MDLINK.finditer(line)]
            for tok in toks:
                c = candidate(tok)
                if not c:
                    continue
                if c.split("/")[0] in repo.ign:
                    continue
                found = None
                for cand in (c, posixpath.normpath(posixpath.join(posixpath.dirname(d), c))):
                    cc = cand.rstrip("/")
                    if cc in repo.fileset or cc in repo.dirs:
                        found = cc
                        break
                if found:
                    inbound[found] += 1
                    continue
                bn = posixpath.basename(c.rstrip("/"))
                if "/" not in c and bn in base_index:
                    continue
                hint = ""
                if bn in base_index:
                    hint = "exists at " + ", ".join(base_index[bn][:2])
                else:
                    close = difflib.get_close_matches(bn, base_names, n=1, cutoff=0.8)
                    if close:
                        hint = "did you mean " + base_index[close[0]][0]
                missing.append([d, i, tok, hint or "-"])
    txt = header("Docs, rules and skills: reference audit", "refs", repo)
    txt += "Scanned %d text files under: %s (+ root-level .md).\n\n" % (len(docs), a.docs or "docs, .clinerules, .agents, memory-bank, .features (minus designs_*/zz_*)")
    mr, mn = capped(sorted(missing), a.max_rows)
    txt += "## References to files or folders that do not exist (%d)\n\n" % len(missing)
    txt += (md_table(["doc", "line", "reference", "hint"], mr) + mn if missing else "none") + "\n\n"
    txt += "## Stale-technology terms (%s)\n\n" % ", ".join(stale_terms)
    srows = [[d, t, len(v), ", ".join(str(x) for x in v[:5])] for (d, t), v in sorted(stale.items())]
    txt += (md_table(["doc", "term", "hits", "first lines"], capped(srows, a.max_rows)[0]) if srows else "none") + "\n\n"
    dup = defaultdict(list)
    for norm, owners in line_owner.items():
        if len(owners) > 1:
            dup[tuple(sorted(owners))].append(norm)
    txt += "## Duplicated lines across docs (60+ characters, identical after normalising) (h)\n\n"
    drows = [[" + ".join(k), len(v), v[0][:90]] for k, v in sorted(dup.items(), key=lambda kv: -len(kv[1]))]
    txt += (md_table(["documents", "shared lines", "example"], capped(drows, a.max_rows)[0]) if drows else "none") + "\n\n"
    txt += "## Size of always-loaded rules and skill files\n\n"
    rs = [f for f in docs if f.startswith(".clinerules/") or "/skills/" in f or f.endswith("SKILL.md")]
    rrows = []
    for f in sorted(rs):
        size, _ = repo.stat(f)
        rrows.append([f, size, size // 4])
    txt += (md_table(["file", "bytes", "approx tokens"], rrows) if rrows else "none found") + "\n\n"
    txt += "## Docs under docs/ or memory-bank/ that nothing else links to (orphans) (h)\n\n"
    orphans = [f for f in docs if inbound[f] == 0 and f.split("/")[0] in ("docs", "memory-bank")]
    txt += ("\n".join("- `%s`" % f for f in orphans[:a.max_rows]) if orphans else "none") + "\n\n"
    txt += "## Doc inventory\n\n"
    irows = []
    for f in sorted(docs):
        size, mt = repo.stat(f)
        title = next((ln.lstrip("# ").strip() for ln in repo.read(f).split("\n")[:15] if ln.startswith("#")), "-")
        irows.append([f, size, datetime.date.fromtimestamp(mt).isoformat() if mt else "-", inbound[f], title[:70]])
    ir, inote = capped(irows, a.max_rows)
    txt += md_table(["file", "bytes", "modified", "inbound refs", "title"], ir) + inote
    emit(out, "refs.md", txt, "%d missing refs, %d stale-term hits, %d duplicate groups, %d orphans" % (
        len(missing), sum(len(v) for v in stale.values()), len(dup), len(orphans)))
    return "refs: %d broken references, %d stale-term hits, %d duplicate groups" % (len(missing), sum(len(v) for v in stale.values()), len(dup))


# ----------------------------------------------------------------------------- comment noise

PATTERNS = [
    ("feature-id (NNN-NNN)", re.compile(r"\b\d{3}-\d{3}\b")),
    ("task/AC/OQ/bug id", re.compile(r"\b(?:T\d{2}|AC-?\d+|OQ-?\d+|FR-?\d+|BUG-?\d+|bug[ _-]?\d+(?:-\d+)?)\b")),
    ("history wording", re.compile(r"(?i)\b(previously|used to|no longer|was changed|now uses|hotfix|workaround|fix for|fixed (?:in|by|bug)|per (?:the )?(?:review|design)|as discussed)\b")),
    ("agent/user dialog", re.compile(r"(?i)\b(as requested|user (?:asked|wants|said)|per user|requested by|the human)\b")),
]


def _py_comments(text):
    out = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type == tokenize.COMMENT:
                out.append((tok.start[0], "comment", tok.string.lstrip("#").strip()))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    try:
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                body = getattr(node, "body", [])
                if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                    for k, ln in enumerate(body[0].value.value.split("\n")):
                        out.append((body[0].lineno + k, "docstring", ln.strip()))
    except (SyntaxError, ValueError):
        pass
    return out


def _outside_string(prefix):
    for q in ("'", '"', "`"):
        if len(re.findall(r"(?<!\\)" + re.escape(q), prefix)) % 2:
            return False
    return True


def _ts_comments(text):
    out = []
    for i, line in enumerate(text.split("\n"), 1):
        idx = line.find("//")
        while idx != -1:
            if (idx == 0 or line[idx - 1] != ":") and _outside_string(line[:idx]):
                out.append((i, "comment", line[idx + 2:].strip()))
                break
            idx = line.find("//", idx + 2)
    for m in re.finditer(r"/\*.*?\*/", text, re.S):
        start = text.count("\n", 0, m.start()) + 1
        for k, ln in enumerate(m.group(0).split("\n")):
            out.append((start + k, "block", ln.strip(" */\t")))
    return out


def cmd_comments(repo, a, out):
    hits = []
    per_file = Counter()
    totals = Counter()
    scanned = 0
    for f in repo.files:
        if not f.endswith(PY_EXT + TS_EXT) or repo.skipped(f):
            continue
        scanned += 1
        text = repo.read(f)
        items = _py_comments(text) if f.endswith(".py") else _ts_comments(text)
        for line, kind, body in items:
            if not body:
                continue
            for name, rx in PATTERNS:
                if rx.search(body):
                    hits.append((f, line, kind, name, body[:140]))
                    per_file[f] += 1
                    totals[name] += 1
    txt = header("Comment and docstring noise", "comments", repo)
    txt += "Scanned %d code files. Full list: `comments_hits.tsv` (file, line, kind, pattern, text).\n\n" % scanned
    txt += md_table(["pattern", "hits"], [[k, v] for k, v in totals.most_common()] or [["none", 0]]) + "\n\n"
    txt += "## Files with the most hits\n\n" + md_table(["file", "hits"], [[f, c] for f, c in per_file.most_common(25)] or [["none", 0]]) + "\n\n"
    txt += "## Examples per pattern\n\n"
    for name, _ in PATTERNS:
        ex = [h for h in hits if h[3] == name][:8]
        txt += "### %s\n\n" % name
        txt += ("\n".join("- `%s:%d` %s" % (h[0], h[1], h[4]) for h in ex) if ex else "none") + "\n\n"
    emit(out, "comments.md", txt, "%d hits in %d files" % (len(hits), len(per_file)))
    with io.open(str(out / "comments_hits.tsv"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("file\tline\tkind\tpattern\ttext\n")
        for h in hits:
            fh.write("\t".join(str(x).replace("\t", " ") for x in h) + "\n")
    return "comments: %d hits in %d of %d code files" % (len(hits), len(per_file), scanned)


# ----------------------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description="Phase 1 evidence generators (read-only).")
    ap.add_argument("command", choices=["all", "inventory", "backend", "frontend", "refs", "comments"])
    ap.add_argument("--root", default=".")
    ap.add_argument("--out", default=".audit/p1")
    ap.add_argument("--depth", type=int, default=3)
    ap.add_argument("--ignore", default="")
    ap.add_argument("--docs", default="")
    ap.add_argument("--terms", default="streamlit,fastify,express")
    ap.add_argument("--max-rows", type=int, default=150)
    ap.add_argument("--skip", default="docs/workflow/TARGET_STATE.md,tools/refactor/tests/fixture/,.features/tools/",
                    help="comma-separated path prefixes to leave out of scans")
    ap.add_argument("--no-git", action="store_true")
    a = ap.parse_args(argv)
    repo = Repo(a.root, a.ignore.split(","), a.skip.split(","))
    out = Path(a.out)
    if not out.is_absolute():
        out = repo.root / out
    summaries = []
    todo = ["inventory", "backend", "frontend", "refs", "comments"] if a.command == "all" else [a.command]
    backend_data = None
    for c in todo:
        if c == "inventory":
            summaries.append(cmd_inventory(repo, a, out))
        elif c == "backend":
            backend_data = scan_backend(repo)
            summaries.append(cmd_backend(repo, a, out, backend_data))
        elif c == "frontend":
            summaries.append(cmd_frontend(repo, a, out, backend_data))
        elif c == "refs":
            summaries.append(cmd_refs(repo, a, out))
        elif c == "comments":
            summaries.append(cmd_comments(repo, a, out))
    if a.command == "all":
        idx = header("Phase 1 evidence index", "all", repo)
        idx += "\n".join("- " + s for s in summaries)
        idx += ("\n\nFiles: inventory.md, deps.md, repo_map_worksheet.md, backend.md, frontend.md, refs.md, comments.md, comments_hits.tsv.\n"
                "Drafting agents must cite these files or repo paths for every claim.\n")
        emit(out, "README.md", idx, "index")
    print("\n".join(summaries))
    return 0


if __name__ == "__main__":
    sys.exit(main())