"""Python analysis built on `ast` (not regex): import graph, layering, cycles, dangling imports,
private cross-module access, dynamic imports, orphans, FastAPI routes, complexity, test mapping.
"""
from __future__ import annotations

import ast
import collections
import os
import posixpath
import re
import sys
import tomllib

from .core import Section, domain_of, is_test_path, match_any

STDLIB = set(getattr(sys, "stdlib_module_names", ())) | {"__future__"}
HTTP = {"get", "post", "put", "delete", "patch", "options", "head", "api_route"}
DOTTED = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+(:[A-Za-z_]\w*)?$")
IDENT = re.compile(r"^[A-Za-z_]\w{3,}$")
CATCH_IMPORT = {"ImportError", "ModuleNotFoundError", "Exception", "BaseException"}


# ============================================================================ per-file scan
class Scanner(ast.NodeVisitor):
    def __init__(self):
        self.imports: list[dict] = []
        self.dynamic: list[dict] = []
        self.dotted: set[str] = set()
        self.str_idents: set[str] = set()
        self.funcs: list[dict] = []
        self.routes: list[dict] = []
        self.routers: dict[str, str] = {}
        self.includes: list[dict] = []
        self.bindings: dict[str, tuple] = {}
        self.attr_uses: list[tuple] = []
        self.top_funcs: list[str] = []
        self.has_main_guard = False
        self.uses_argparse = False
        self._ctx: list[str] = []
        self._scope: list[str] = []

    # -- context helpers
    def _c(self) -> str:
        for k in ("typing", "guarded", "lazy"):
            if k in self._ctx:
                return k
        return "top"

    def visit_FunctionDef(self, node):
        qual = ".".join(self._scope + [node.name])
        end = getattr(node, "end_lineno", node.lineno)
        cc = 1
        for n in ast.walk(node):
            if isinstance(n, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler, ast.IfExp)):
                cc += 1
            elif isinstance(n, ast.BoolOp):
                cc += len(n.values) - 1
            elif isinstance(n, ast.comprehension):
                cc += 1 + len(n.ifs)
            elif hasattr(ast, "match_case") and isinstance(n, ast.match_case):
                cc += 1
        self.funcs.append({"name": qual, "line": node.lineno, "loc": end - node.lineno + 1, "cc": cc})
        if not self._scope:
            self.top_funcs.append(node.name)
        self._route_decorators(node, end)
        self._ctx.append("lazy"); self._scope.append(node.name)
        self.generic_visit(node)
        self._ctx.pop(); self._scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_Try(self, node):
        guarded = any(h.type is None or any(
            (isinstance(t, ast.Name) and t.id in CATCH_IMPORT) for t in
            (h.type.elts if isinstance(h.type, ast.Tuple) else [h.type])) for h in node.handlers)
        if guarded:
            self._ctx.append("guarded")
            for b in node.body:
                self.visit(b)
            self._ctx.pop()
            for part in (node.handlers, node.orelse, node.finalbody):
                for b in part:
                    self.visit(b)
        else:
            self.generic_visit(node)

    visit_TryStar = visit_Try

    def visit_If(self, node):
        t = node.test
        is_tc = (isinstance(t, ast.Name) and t.id == "TYPE_CHECKING") or \
                (isinstance(t, ast.Attribute) and t.attr == "TYPE_CHECKING")
        is_main = (isinstance(t, ast.Compare) and isinstance(t.left, ast.Name) and t.left.id == "__name__")
        if is_main:
            self.has_main_guard = True
        if is_tc:
            self._ctx.append("typing")
            for b in node.body:
                self.visit(b)
            self._ctx.pop()
            for b in node.orelse:
                self.visit(b)
        else:
            self.generic_visit(node)

    # -- imports
    def visit_Import(self, node):
        for a in node.names:
            self.imports.append({"module": a.name, "names": [], "level": 0, "line": node.lineno, "ctx": self._c()})
            self.bindings[a.asname or a.name.split(".")[0]] = (a.name if a.asname else a.name.split(".")[0], 0, None)
            if a.name == "argparse":
                self.uses_argparse = True

    def visit_ImportFrom(self, node):
        names = [a.name for a in node.names]
        self.imports.append({"module": node.module or "", "names": names, "level": node.level,
                             "line": node.lineno, "ctx": self._c()})
        for a in node.names:
            if a.name != "*":
                self.bindings[a.asname or a.name] = (node.module or "", node.level, a.name)

    # -- calls / constants / attributes
    def visit_Call(self, node):
        f = node.func
        name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else None)
        if name in ("import_module", "__import__") and node.args:
            a = node.args[0]
            if isinstance(a, ast.Constant) and isinstance(a.value, str):
                self.dynamic.append({"line": node.lineno, "literal": a.value, "prefix": None})
            elif isinstance(a, ast.JoinedStr):
                pre = a.values[0].value if a.values and isinstance(a.values[0], ast.Constant) else ""
                self.dynamic.append({"line": node.lineno, "literal": None, "prefix": pre})
            else:
                self.dynamic.append({"line": node.lineno, "literal": None, "prefix": ""})
        if name == "include_router" and node.args:
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            prefix = kw["prefix"].value if isinstance(kw.get("prefix"), ast.Constant) else ""
            a0 = node.args[0]
            if isinstance(a0, ast.Attribute) and isinstance(a0.value, ast.Name):
                self.includes.append({"mod": a0.value.id, "var": a0.attr, "prefix": prefix})
            elif isinstance(a0, ast.Name):
                self.includes.append({"mod": None, "var": a0.id, "prefix": prefix})
        if name == "APIRouter":
            pass
        self.generic_visit(node)

    def visit_Assign(self, node):
        v = node.value
        if isinstance(v, ast.Call) and ((isinstance(v.func, ast.Name) and v.func.id == "APIRouter") or
                                        (isinstance(v.func, ast.Attribute) and v.func.attr == "APIRouter")):
            prefix = ""
            for k in v.keywords:
                if k.arg == "prefix" and isinstance(k.value, ast.Constant):
                    prefix = k.value.value
            for t in node.targets:
                if isinstance(t, ast.Name):
                    self.routers[t.id] = prefix
        self.generic_visit(node)

    def visit_Constant(self, node):
        if isinstance(node.value, str) and len(node.value) <= 200:
            s = node.value
            if DOTTED.match(s):
                self.dotted.add(s)
            elif IDENT.match(s):
                self.str_idents.add(s)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.attr.startswith("_") and not node.attr.startswith("__"):
            self.attr_uses.append((node.value.id, node.attr, node.lineno))
        self.generic_visit(node)

    # -- routes
    def _route_decorators(self, node, end):
        for d in node.decorator_list:
            if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr in HTTP \
                    and isinstance(d.func.value, ast.Name):
                path = None
                if d.args and isinstance(d.args[0], ast.Constant) and isinstance(d.args[0].value, str):
                    path = d.args[0].value
                for k in d.keywords:
                    if k.arg == "path" and isinstance(k.value, ast.Constant):
                        path = k.value.value
                if path is None:
                    continue
                called = set()
                for n in ast.walk(node):
                    if isinstance(n, ast.Call):
                        if isinstance(n.func, ast.Name):
                            called.add(n.func.id)
                        elif isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name):
                            called.add(f"{n.func.value.id}.{n.func.attr}")
                self.routes.append({"router": d.func.value.id, "method": d.func.attr.upper(), "path": path,
                                    "func": node.name, "line": node.lineno, "loc": end - node.lineno + 1,
                                    "called": sorted(called)})


# ============================================================================ resolution
def detect_roots(files: list[str]) -> list[str]:
    init_dirs = {posixpath.dirname(f) for f in files if posixpath.basename(f) == "__init__.py"}
    roots = {""}
    for d in init_dirs:
        top = d
        while posixpath.dirname(top) in init_dirs:
            top = posixpath.dirname(top)
        roots.add(posixpath.dirname(top))
    return sorted(roots)


class Resolver:
    def __init__(self, pyfiles: list[str], roots: list[str]):
        self.files = set(pyfiles)
        self.roots = roots
        self.dirs: set[str] = set()
        for f in pyfiles:
            d = posixpath.dirname(f)
            while d:
                self.dirs.add(d)
                d = posixpath.dirname(d)
        self.pkg_dirs = {d for d in self.dirs if f"{d}/__init__.py" in self.files}

    @staticmethod
    def _j(*parts) -> str:
        return "/".join(p for p in parts if p)

    def _probe(self, base: str):
        if f"{base}.py" in self.files:
            return ("file", f"{base}.py")
        if f"{base}/__init__.py" in self.files:
            return ("pkg", f"{base}/__init__.py")
        if base in self.dirs:
            return ("dir", None)
        return None

    def _top_exists(self, root: str, top: str) -> bool:
        b = self._j(root, top)
        return f"{b}.py" in self.files or b in self.dirs

    def _cands(self, importer: str) -> list[str]:
        d = posixpath.dirname(importer)
        cands = [] if (d == "" or d in self.pkg_dirs) else [d]  # script-directory semantics
        return cands + self.roots

    def resolve(self, importer: str, module: str, names: list[str], level: int) -> dict:
        out = {"targets": set(), "internal": False, "missing": None}
        if level:
            base = posixpath.dirname(importer)
            for _ in range(level - 1):
                base = posixpath.dirname(base)
            parts = module.split(".") if module else []
            self._walk(base, parts, names, out, importer, relative=True)
            return out
        parts = module.split(".")
        for root in self._cands(importer):
            if self._top_exists(root, parts[0]):
                self._walk(root, parts, names, out, importer, relative=False)
                return out
        return out  # external

    def _walk(self, root, parts, names, out, importer, relative):
        deepest, k = None, 0
        for i in range(1, len(parts) + 1):
            pr = self._probe(self._j(root, *parts[:i]))
            if not pr:
                break
            deepest, k = pr, i
        base = self._j(root, *parts)
        if not parts:  # `from . import x`
            out["internal"] = True
            for n in names:
                pr = self._probe(self._j(root, n))
                if pr and pr[1]:
                    out["targets"].add(pr[1])
            return
        out["internal"] = True
        if deepest and k == len(parts):
            if deepest[1]:
                out["targets"].add(deepest[1])
            for n in names:
                pr = self._probe(self._j(base, n))
                if pr and pr[1]:
                    out["targets"].add(pr[1])
        elif deepest and deepest[0] == "file":
            out["targets"].add(deepest[1])  # attribute access on a module
        else:
            out["missing"] = ".".join(parts) if not relative else "." * 1 + ".".join(parts)

    def module_name(self, rel: str) -> str:
        best = ""
        for r in self.roots:
            if r == "" or rel.startswith(r + "/"):
                if len(r) >= len(best):
                    best = r
        sub = rel[len(best) + 1:] if best else rel
        sub = re.sub(r"(/__init__)?\.py$", "", sub)
        return sub.replace("/", ".")


# ============================================================================ graph algorithms
def tarjan(nodes, adj) -> list[list[str]]:
    sys.setrecursionlimit(max(10000, sys.getrecursionlimit()))
    idx, low, on, st, out, c = {}, {}, set(), [], [], [0]

    def dfs(v):
        idx[v] = low[v] = c[0]; c[0] += 1
        st.append(v); on.add(v)
        for w in adj.get(v, ()):
            if w not in idx:
                dfs(w); low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], idx[w])
        if low[v] == idx[v]:
            comp = []
            while True:
                w = st.pop(); on.discard(w); comp.append(w)
                if w == v:
                    break
            if len(comp) > 1:
                out.append(sorted(comp))

    for n in nodes:
        if n not in idx:
            dfs(n)
    return out


def similar_files(repo, files, threshold=0.6, min_lines=15, max_pairs=40):
    sets = {}
    for f in files:
        t = repo.text(f) or ""
        lines = {l.strip() for l in t.splitlines() if len(l.strip()) > 3 and not l.strip().startswith(("#", "//", "*", "/*"))}
        if len(lines) >= min_lines:
            sets[f] = lines
    names = sorted(sets)
    pairs = []
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            la, lb = len(sets[a]), len(sets[b])
            if min(la, lb) / max(la, lb) < 0.4:
                continue
            j = len(sets[a] & sets[b]) / len(sets[a] | sets[b])
            if j >= threshold:
                pairs.append({"a": a, "b": b, "similarity": round(j, 2)})
    pairs.sort(key=lambda p: -p["similarity"])
    return pairs[:max_pairs]


# ============================================================================ manifests
def _norm_dist(n: str) -> str:
    return re.sub(r"[-_.]+", "-", n.strip().lower())


def declared_requirements(repo) -> dict[str, list[str]]:
    out: dict[str, list[str]] = collections.defaultdict(list)
    for rel in repo.files:
        name = rel.rsplit("/", 1)[-1]
        txt = repo.text(rel) or ""
        if re.match(r"(.*requirements.*)\.txt$", name):
            for ln in txt.splitlines():
                ln = ln.split("#")[0].strip()
                m = re.match(r"([A-Za-z0-9_.\-]+)", ln)
                if m and not ln.startswith("-"):
                    out[_norm_dist(m.group(1))].append(rel)
        elif name == "pyproject.toml":
            try:
                data = tomllib.loads(txt)
            except Exception:
                continue
            deps = list(data.get("project", {}).get("dependencies", []))
            for v in data.get("project", {}).get("optional-dependencies", {}).values():
                deps += v
            for d in deps:
                m = re.match(r"([A-Za-z0-9_.\-]+)", d)
                if m:
                    out[_norm_dist(m.group(1))].append(rel)
        elif name == "setup.py":
            for blk in re.findall(r"install_requires\s*=\s*\[(.*?)\]", txt, re.S):
                for d in re.findall(r"[\"']([A-Za-z0-9_.\-]+)", blk):
                    out[_norm_dist(d)].append(rel)
    return out


# ============================================================================ main
def analyze(repo) -> Section:
    sec = Section("python")
    cfg = repo.cfg
    pcfg = cfg["python"]
    pyfiles = repo.by_ext(".py")
    roots = pcfg["roots"] or detect_roots(pyfiles)
    res = Resolver(pyfiles, roots)
    scripts = [f for f in pyfiles if any(f.startswith(d + "/") for d in pcfg["script_dirs"])]
    scan: dict[str, Scanner] = {}
    parse_errors = []
    for f in pyfiles:
        try:
            tree = ast.parse(repo.text(f) or "", filename=f)
        except (SyntaxError, ValueError) as e:
            parse_errors.append(f"{f}: {e}")
            continue
        s = Scanner()
        s.visit(tree)
        scan[f] = s

    # ---- resolve imports ----------------------------------------------------
    edges: list[dict] = []
    dangling: list[dict] = []
    ext_use: dict[str, set] = collections.defaultdict(set)
    ext_line: dict[tuple, int] = {}
    private_imports: list[dict] = []
    for f, s in scan.items():
        for rec in s.imports:
            r = res.resolve(f, rec["module"], rec["names"], rec["level"])
            if r["internal"]:
                for t in sorted(r["targets"]):
                    if t != f:
                        edges.append({"src": f, "dst": t, "ctx": rec["ctx"], "line": rec["line"], "kind": "import"})
                if r["missing"]:
                    stale = _stale_pyc(repo, res, r["missing"], f)
                    dangling.append({"src": f, "module": rec["module"] or r["missing"], "line": rec["line"],
                                     "ctx": rec["ctx"], "stale_pyc": stale})
                for n in rec["names"]:
                    if n.startswith("_") and not n.startswith("__"):
                        for t in r["targets"]:
                            if t != f and posixpath.dirname(t) != posixpath.dirname(f) and not t.endswith("__init__.py"):
                                private_imports.append({"src": f, "dst": t, "name": n, "line": rec["line"]})
            elif rec["level"] == 0 and rec["module"]:
                top = rec["module"].split(".")[0]
                if top not in STDLIB:
                    ext_use[top].add(f)
                    ext_line.setdefault((f, top), rec["line"])
        # module-alias private attribute access: `import x.y as m; m._helper`
        for name, attr, line in s.attr_uses:
            b = s.bindings.get(name)
            if not b:
                continue
            mod, level, imported = b
            full = mod if imported is None else (f"{mod}.{imported}" if mod else imported)
            r = res.resolve(f, full, [], level)
            for t in r["targets"]:
                if t != f and not t.endswith("__init__.py") and posixpath.dirname(t) != posixpath.dirname(f):
                    private_imports.append({"src": f, "dst": t, "name": attr, "line": line, "via": "attribute"})
        # literal dynamic imports and dotted-string references become soft edges
        for d in s.dynamic:
            if d["literal"]:
                r = res.resolve(f, d["literal"], [], 0)
                for t in r["targets"]:
                    edges.append({"src": f, "dst": t, "ctx": "dynamic", "line": d["line"], "kind": "dynamic"})
        for ds in s.dotted:
            mod = ds.split(":")[0]
            r = res.resolve(f, mod, [], 0)
            for t in r["targets"]:
                if t != f:
                    edges.append({"src": f, "dst": t, "ctx": "string", "line": 0, "kind": "string"})

    hard = [e for e in edges if e["kind"] == "import" and e["ctx"] == "top"]
    all_imp = [e for e in edges if e["kind"] == "import"]
    # dedupe for graph purposes
    def adj_of(es):
        a = collections.defaultdict(set)
        for e in es:
            a[e["src"]].add(e["dst"])
        return a
    cycles_hard = tarjan(scan.keys(), adj_of(hard))
    cycles_all = tarjan(scan.keys(), adj_of(all_imp))
    pkg_adj = collections.defaultdict(set)
    for e in all_imp:
        a, b = e["src"].split("/")[0], e["dst"].split("/")[0]
        if a != b and "/" in e["src"] and "/" in e["dst"]:
            pkg_adj[a].add(b)
    pkg_cycles = tarjan(list(pkg_adj), pkg_adj)

    fan_in, fan_out = collections.Counter(), collections.Counter()
    for e in edges:
        fan_in[e["dst"]] += 1
        fan_out[e["src"]] += 1

    # ---- layer rules ---------------------------------------------------------
    violations = []
    for rule in cfg["forbid"]:
        if not rule.get("enabled", True):
            continue
        frm, to = rule.get("from", []), rule.get("to", [])
        ext, skip = rule.get("to_external", []), rule.get("except_from", [])
        for e in all_imp:
            if match_any(e["src"], frm) and not match_any(e["src"], skip) and to and match_any(e["dst"], to):
                violations.append({"rule": rule["name"], "src": e["src"], "dst": e["dst"], "line": e["line"], "sev": rule.get("severity", "high")})
        if ext:
            for top, users in ext_use.items():
                if any(top == x or top.startswith(x) for x in ext):
                    for u in users:
                        if match_any(u, frm) and not match_any(u, skip):
                            violations.append({"rule": rule["name"], "src": u, "dst": f"<{top}>", "line": ext_line.get((u, top), 0), "sev": rule.get("severity", "high")})

    # ---- orphans ---------------------------------------------------------------
    def is_entry(f):
        return match_any(f, pcfg["entrypoint_globs"])
    src_mods = [f for f in scan if not is_test_path(f) and f not in scripts and not f.endswith("__init__.py")]
    cand = [f for f in src_mods if fan_in[f] == 0 and not is_entry(f)]
    wanted = {posixpath.basename(f)[:-3] for f in cand}
    for f in cand:
        wanted.update(n for n in scan[f].top_funcs if not n.startswith("_") and len(n) > 4)
    refs = collections.defaultdict(lambda: collections.defaultdict(set))
    if wanted:
        rx = re.compile(r"\b(" + "|".join(re.escape(w) for w in sorted(wanted, key=len, reverse=True)) + r")\b")
        for rel in repo.files:
            if rel.endswith(".py") or repo.size(rel) > 2_000_000:
                continue
            if os.path.splitext(rel)[1].lower() in (".service", ".timer", ".sh", ".ps1", ".conf", ".toml", ".cfg", ".ini",
                                                    ".yaml", ".yml", ".json", ".sql", ".txt", ".md", ".env", ".html"):
                for w in set(rx.findall(repo.text(rel) or "")):
                    refs[w][rel].add("doc" if rel.endswith(".md") else "config")
        for f, s in scan.items():
            for w in s.str_idents & wanted:
                refs[w][f].add("py-string")
    dyn_prefixes = [(f, d["prefix"]) for f, s in scan.items() for d in s.dynamic if d["literal"] is None]
    orphans = []
    for f in cand:
        mod = res.module_name(f)
        names = [posixpath.basename(f)[:-3]] + [n for n in scan[f].top_funcs if not n.startswith("_") and len(n) > 4]
        hits = {}
        for n in names:
            for where, kinds in refs.get(n, {}).items():
                if where != f:
                    hits.setdefault(where, set()).update(kinds)
        dyn = [pf for pf, pre in dyn_prefixes if pre is not None and (pre == "" or mod.startswith(pre.rstrip(".")))]
        non_doc = {k: v for k, v in hits.items() if v - {"doc"}}
        status = "string-referenced" if non_doc else ("dynamic-possible" if dyn else ("doc-only" if hits else "likely-dead"))
        orphans.append({"path": f, "module": mod, "loc": _loc(repo, f), "domain": domain_of(f, cfg["domains"]),
                        "status": status, "referenced_in": sorted(hits)[:6], "dynamic_loaders": sorted(set(dyn))[:4]})
    orphans.sort(key=lambda o: (o["status"], o["path"]))
    script_unref = [f for f in scripts if fan_in[f] == 0]

    # ---- routes ----------------------------------------------------------------
    routes = _routes(repo, res, scan, pcfg)
    router_files = sorted({r["file"] for r in routes})
    direct_db = [r for r in routes if r["direct_db"]]
    threading_routers = [f for f in router_files if any(i["module"] == "threading" for i in scan[f].imports)]
    router_cross_domain = collections.defaultdict(set)
    for e in all_imp:
        if e["src"] in router_files:
            d1, d2 = domain_of(e["src"], cfg["domains"]), domain_of(e["dst"], cfg["domains"])
            if d1 and d2 and d1 != d2 and d2 not in cfg["shared_domains"]:
                router_cross_domain[e["src"]].add(f"{d2}:{e['dst']}")

    # ---- complexity ------------------------------------------------------------
    fat_loc, fat_cc = pcfg["fat_function_loc"], pcfg["fat_function_cc"]
    funcs = [{"path": f, **fn} for f, s in scan.items() for fn in s.funcs if not is_test_path(f)]
    fat_funcs = sorted((x for x in funcs if x["loc"] >= fat_loc or x["cc"] >= fat_cc), key=lambda x: -x["cc"] * 3 - x["loc"])
    fat_files = sorted(({"path": f, "loc": _loc(repo, f)} for f in scan if not is_test_path(f)), key=lambda x: -x["loc"])
    fat_files = [x for x in fat_files if x["loc"] >= pcfg["fat_file_loc"]]

    # ---- tests -----------------------------------------------------------------
    tests = [f for f in scan if is_test_path(f)]
    covered = {e["dst"] for e in all_imp if e["src"] in set(tests)}
    untested = [f for f in src_mods if f not in covered and f not in scripts and not f.endswith("main.py")]
    misplaced_tests = [f for f in scripts if posixpath.basename(f).startswith("test_")]
    tests_via_app = [f for f in tests if any(e["src"] == f and e["dst"].endswith("main.py") for e in all_imp)]

    # ---- dependencies ----------------------------------------------------------
    declared = declared_requirements(repo)
    local_top = {posixpath.basename(f).split(".")[0] if "/" not in f else f.split("/")[0] for f in pyfiles}
    local_top |= {res.module_name(f).split(".")[0] for f in pyfiles}
    used = {}
    for top, users in ext_use.items():
        if top in local_top:
            continue
        nonlocal_users = [u for u in users if not is_test_path(u) and u not in scripts]
        dist = _norm_dist(pcfg["dist_aliases"].get(top, top))
        used[dist] = {"import": top, "files": len(users), "prod_files": len(nonlocal_users)}
    undeclared = sorted(d for d in used if d not in declared and d.replace("-", "") not in {x.replace("-", "") for x in declared})
    never_imported = sorted(d for d in declared if d not in used and d.replace("-", "") not in {u.replace("-", "") for u in used})

    # ---- near-duplicates -------------------------------------------------------
    sim = similar_files(repo, [f for f in scan if not f.endswith("__init__.py") and not is_test_path(f)])

    # ---- module table ----------------------------------------------------------
    modules = []
    for f in sorted(scan):
        modules.append({"path": f, "module": res.module_name(f), "loc": _loc(repo, f),
                        "domain": domain_of(f, cfg["domains"]), "fan_in": fan_in[f], "fan_out": fan_out[f],
                        "is_test": is_test_path(f), "is_script": f in scripts})

    repo.cache["py_edges"] = [(e["src"], e["dst"]) for e in all_imp]
    repo.cache["routes"] = routes
    repo.cache["py_modules"] = {m["path"]: m for m in modules}

    sec.data = {
        "roots": roots, "files": len(scan), "parse_errors": parse_errors,
        "edges": [{k: e[k] for k in ("src", "dst", "ctx", "kind")} for e in edges],
        "top_fan_in": [{"path": p, "fan_in": n} for p, n in fan_in.most_common(15)],
        "top_fan_out": [{"path": p, "fan_out": n} for p, n in fan_out.most_common(15)],
        "cycles_module_level": cycles_hard, "cycles_any_import": cycles_all, "cycles_top_level_packages": pkg_cycles,
        "layer_violations": violations, "dangling_imports": dangling, "private_cross_imports": private_imports,
        "dynamic_imports": [{"file": f, **d} for f, s in scan.items() for d in s.dynamic],
        "orphans": orphans, "scripts_never_imported": script_unref,
        "routes": routes, "router_files": router_files,
        "routers_with_threading": threading_routers,
        "router_cross_domain_imports": {k: sorted(v) for k, v in router_cross_domain.items()},
        "fat_functions": fat_funcs[:pcfg["top_n"]], "fat_files": fat_files[:pcfg["top_n"]],
        "tests": {"count": len(tests), "misplaced_in_scripts": misplaced_tests,
                  "only_via_app_import": tests_via_app, "untested_modules": untested},
        "dependencies": {"imported_not_declared": undeclared, "declared_not_imported": never_imported,
                         "declared_in": {k: sorted(set(v)) for k, v in declared.items()}, "external_usage": used},
        "near_duplicates": sim, "modules": modules,
    }

    # ---- findings --------------------------------------------------------------
    if dangling:
        sec.add("PY-001", "high", "Imports of internal modules that do not exist in the tree", count=len(dangling),
                evidence=[f"{d['src']}:{d['line']} imports {d['module']} [{d['ctx']}]" + (" (stale .pyc present)" if d["stale_pyc"] else "")
                          for d in dangling],
                hint="Either the module was deleted (fix/remove the import) or it is guarded/lazy; confirm before moving anything.")
    if pkg_cycles:
        sec.add("PY-002", "high", "Cyclic dependencies between top-level packages", count=len(pkg_cycles),
                evidence=[" <-> ".join(c) for c in pkg_cycles], hint="Dependencies must point one way: api -> domains -> core.")
    if cycles_hard:
        sec.add("PY-003", "medium", "Module-level import cycles", count=len(cycles_hard), evidence=[" , ".join(c[:5]) for c in cycles_hard])
    if violations:
        by_rule = collections.Counter(v["rule"] for v in violations)
        sec.add("PY-004", "high", "Layer-rule violations (audit.toml [[forbid]])", count=len(violations),
                evidence=[f"[{v['rule']}] {v['src']}:{v['line']} -> {v['dst']}" for v in violations],
                detail="; ".join(f"{k}: {n}" for k, n in by_rule.items()))
    if private_imports:
        pairs = sorted({(p["src"], p["dst"], p["name"]) for p in private_imports})
        sec.add("PY-005", "medium", "Private (_underscore) names used across module/package boundaries", count=len(pairs),
                evidence=[f"{a} -> {b}.{n}" for a, b, n in pairs],
                hint="Logic in routers/helpers that others depend on belongs in a public service module.")
    if direct_db:
        sec.add("PY-006", "medium", "Route handlers calling the DB layer directly (no service layer)", count=len(direct_db),
                evidence=[f"{r['file']}:{r['line']} {r['method']} {r['full_path']} -> {', '.join(r['direct_db'][:4])}" for r in direct_db])
    if threading_routers:
        sec.add("PY-007", "low", "Routers that import threading (orchestration inside the web layer)", evidence=threading_routers)
    if router_cross_domain:
        sec.add("PY-008", "low", "Routers importing from another domain's modules", count=len(router_cross_domain),
                evidence=[f"{k} -> {', '.join(sorted(v)[:3])}" for k, v in router_cross_domain.items()])
    likely = [o for o in orphans if o["status"] == "likely-dead"]
    if orphans:
        by = collections.Counter(o["status"] for o in orphans)
        sec.add("PY-009", "medium" if likely else "info", "Modules with no importers", count=len(orphans),
                detail=", ".join(f"{k}: {v}" for k, v in by.items()),
                evidence=[f"{o['status']}: {o['path']} ({o['loc']} loc)" for o in orphans],
                hint="`likely-dead` = no import, string, config or doc reference anywhere. `dynamic-possible` = a non-literal importlib call could load it.")
    nonlit = [d for d in sec.data["dynamic_imports"] if d["literal"] is None]
    if nonlit:
        sec.add("PY-010", "medium", "Non-literal dynamic imports (importlib with computed names)", count=len(nonlit),
                evidence=[f"{d['file']}:{d['line']} prefix={d['prefix']!r}" for d in nonlit],
                hint="Static tools cannot see these targets. List the real targets (DB task config?) before moving modules.")
    if fat_funcs:
        sec.add("PY-011", "low", f"Functions >= {fat_loc} LOC or complexity >= {fat_cc}", count=len(fat_funcs),
                evidence=[f"{x['path']}:{x['line']} {x['name']} loc={x['loc']} cc={x['cc']}" for x in fat_funcs[:10]])
    if fat_files:
        sec.add("PY-012", "low", f"Python files >= {pcfg['fat_file_loc']} LOC", count=len(fat_files),
                evidence=[f"{x['path']} ({x['loc']})" for x in fat_files])
    if sim:
        sec.add("PY-013", "medium", "Near-duplicate Python files (possible vendored copies)", count=len(sim),
                evidence=[f"{p['similarity']:.2f} {p['a']} ~ {p['b']}" for p in sim])
    if undeclared:
        sec.add("PY-014", "low", "Imported third-party packages not declared in any manifest (heuristic)", evidence=undeclared)
    if never_imported:
        sec.add("PY-015", "low", "Declared requirements never imported (heuristic; tools/plugins may be legit)", evidence=never_imported)
    if misplaced_tests:
        sec.add("PY-016", "low", "test_*.py files living in scripts/", evidence=misplaced_tests)
    if parse_errors:
        sec.add("PY-017", "medium", "Python files that failed to parse", evidence=parse_errors)

    sec.metrics = {
        "py.files": len(scan), "py.dangling_imports": len(dangling), "py.pkg_cycles": len(pkg_cycles),
        "py.module_cycles": len(cycles_hard), "py.layer_violations": len(violations),
        "py.private_cross_imports": len({(p["src"], p["dst"], p["name"]) for p in private_imports}),
        "py.orphans_likely_dead": len(likely), "py.orphans_total": len(orphans),
        "py.nonliteral_dynamic_imports": len(nonlit), "py.router_direct_db_handlers": len(direct_db),
        "py.fat_functions": len(fat_funcs), "py.near_duplicate_pairs": len(sim),
        "py.parse_errors": len(parse_errors), "py.untested_modules": len(untested),
    }
    return sec


# ============================================================================ helpers
def _loc(repo, f) -> int:
    t = repo.text(f) or ""
    return t.count("\n") + (1 if t and not t.endswith("\n") else 0)


def _stale_pyc(repo, res, missing: str, importer: str) -> bool:
    """Is there a stale __pycache__/<name>.cpython-*.pyc for the missing module?"""
    parts = missing.lstrip(".").split(".")
    for root in res.roots:
        base = "/".join(p for p in [root] + parts[:-1] if p)
        d = repo.root / base / "__pycache__"
        if d.is_dir() and any(n.startswith(parts[-1] + ".") and n.endswith(".pyc") for n in os.listdir(d)):
            return True
    return False


def _routes(repo, res, scan, pcfg) -> list[dict]:
    db_mods = set(pcfg["db_modules"])
    prefixes: dict[tuple, str] = {}  # (file, var) -> include prefix
    for f, s in scan.items():
        for inc in s.includes:
            target = None
            if inc["mod"]:
                b = s.bindings.get(inc["mod"])
                if b:
                    mod, level, imported = b
                    full = f"{mod}.{imported}" if imported else mod
                    r = res.resolve(f, full, [], level)
                    target = next(iter(r["targets"]), None)
            else:
                b = s.bindings.get(inc["var"])
                if b:
                    mod, level, imported = b
                    r = res.resolve(f, mod, [], level)
                    target = next(iter(r["targets"]), None)
            if target:
                prefixes[(target, inc["var"] if inc["mod"] else (b[2] if b else inc["var"]))] = inc["prefix"]
    out = []
    for f, s in scan.items():
        if not s.routes:
            continue
        dbnames = {local for local, (mod, _, imp) in s.bindings.items()
                   if mod.split(".")[-1] in db_mods and imp}
        for r in s.routes:
            inc = prefixes.get((f, r["router"]), "")
            full = (inc + s.routers.get(r["router"], "") + r["path"]) or "/"
            direct = sorted(c for c in r["called"] if c in dbnames)
            out.append({"file": f, "method": r["method"], "path": r["path"], "full_path": full, "func": r["func"],
                        "line": r["line"], "loc": r["loc"], "direct_db": direct})
    return out
