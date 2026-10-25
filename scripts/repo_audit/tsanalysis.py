"""Frontend (TS/TSX/JS) analysis without Node: import graph, tsconfig aliases, Next.js entry-point
reachability, declared-vs-used npm packages, per-area coupling, single-consumer lib modules.

This replaces knip when knip is run from the wrong directory (every file looks 'unused').
"""
from __future__ import annotations

import collections
import json
import os
import posixpath
import re

from .core import Section, domain_of, is_test_path, layer_of, match_any
from .pyanalysis import similar_files

JS_EXT = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts")
RESOLVE_EXT = JS_EXT + (".json", ".css")
IMPORT_RX = re.compile(r"""\b(?:import|export)\b(?:[^;'"`]*?\bfrom\b)?\s*["']([^"']+)["']""")
DYN_RX = re.compile(r"""\b(?:import|require)\(\s*["']([^"']+)["']\s*\)""")
BLOCK_C = re.compile(r"/\*.*?\*/", re.S)
LINE_C = re.compile(r"(?<![:\"'\\])//[^\n]*")
API_LIT = re.compile(r"""["'`](/[A-Za-z0-9_\-./${}:?=&\[\]]*)["'`]""")
NODE_BUILTIN = {"fs", "path", "os", "url", "util", "crypto", "http", "https", "stream", "events", "child_process",
                "assert", "buffer", "zlib", "net", "tls", "readline", "process", "module", "worker_threads"}
# Packages that are consumed by tooling/compilers rather than imported from source; never reported as unused.
IMPLICIT = {"react", "react-dom", "typescript", "eslint", "eslint-config-next", "prettier", "vitest", "jest", "jsdom",
            "happy-dom", "postcss", "autoprefixer", "tailwindcss", "@tailwindcss/postcss", "@vitejs/plugin-react",
            "@testing-library/jest-dom", "@testing-library/react", "@testing-library/user-event", "ts-node", "tsx"}


def strip_comments(s: str) -> str:
    return LINE_C.sub("", BLOCK_C.sub("", s))


def _strip_json(txt: str) -> dict:
    txt = BLOCK_C.sub("", re.sub(r"(?m)^\s*//.*$", "", txt))
    txt = re.sub(r",(\s*[}\]])", r"\1", txt)
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return {}


def detect_roots(repo) -> list[str]:
    roots = []
    for rel in repo.files:
        if rel.endswith("package.json"):
            d = posixpath.dirname(rel)
            has = lambda n: (f"{d}/{n}" if d else n) in repo.fileset
            if has("tsconfig.json") or any(f.startswith((d + "/" if d else "") + "next.config.") for f in repo.files if posixpath.dirname(f) == d):
                roots.append(d)
    return roots


class Frontend:
    def __init__(self, repo, root: str):
        self.repo, self.root = repo, root
        pre = root + "/" if root else ""
        self.files = [f for f in repo.files if f.startswith(pre) and os.path.splitext(f)[1].lower() in RESOLVE_EXT]
        self.fileset = set(self.files)
        self.src = [f for f in self.files if f.endswith(JS_EXT)]
        ts = _strip_json(repo.text(self._p("tsconfig.json")) or "") if self._p("tsconfig.json") in repo.fileset else {}
        co = ts.get("compilerOptions", {})
        self.base = self._p(co.get("baseUrl", ".")) if "baseUrl" in co else self.root
        self.aliases = {}
        for k, v in (co.get("paths") or repo.cfg["frontend"]["alias_fallback"]).items():
            self.aliases[k.rstrip("*")] = [self._norm(posixpath.join(self.base if "paths" in co else self.root, x.rstrip("*"))) for x in v]
        pj = json.loads(repo.text(self._p("package.json")) or "{}") if self._p("package.json") in repo.fileset else {}
        self.pkg = pj

    def _p(self, rel):
        return f"{self.root}/{rel}" if self.root else rel

    @staticmethod
    def _norm(p):
        return posixpath.normpath(p).lstrip("./") if posixpath.normpath(p) != "." else ""

    def _probe(self, base: str):
        base = self._norm(base)
        if base in self.fileset and os.path.splitext(base)[1] in RESOLVE_EXT:
            return base
        for e in RESOLVE_EXT:
            if base + e in self.fileset:
                return base + e
        for e in RESOLVE_EXT:
            c = f"{base}/index{e}"
            if c in self.fileset:
                return c
        return None

    def resolve(self, importer: str, spec: str):
        """-> ('file', path) | ('pkg', name) | ('missing', spec)"""
        if spec.startswith("."):
            r = self._probe(posixpath.join(posixpath.dirname(importer), spec))
            return ("file", r) if r else ("missing", spec)
        for pre, bases in self.aliases.items():
            if pre and spec.startswith(pre):
                for b in bases:
                    r = self._probe(posixpath.join(b, spec[len(pre):]))
                    if r:
                        return ("file", r)
                return ("missing", spec)
        if spec.startswith("node:") or spec in NODE_BUILTIN:
            return ("builtin", spec)
        parts = spec.split("/")
        name = "/".join(parts[:2]) if spec.startswith("@") else parts[0]
        return ("pkg", name)


def analyze(repo) -> Section:
    sec = Section("frontend")
    cfg = repo.cfg
    fcfg = cfg["frontend"]
    roots = fcfg["roots"] or detect_roots(repo)
    all_pkg_users: dict = {}
    result = {}
    metrics = collections.Counter()
    for root in roots:
        fe = Frontend(repo, root)
        edges, pkgs_used, missing = [], collections.defaultdict(set), []
        api_literals = collections.defaultdict(set)
        for f in fe.src:
            raw = repo.text(f) or ""
            txt = strip_comments(raw)
            specs = set(IMPORT_RX.findall(txt)) | set(DYN_RX.findall(txt))
            for sp in specs:
                kind, tgt = fe.resolve(f, sp)
                if kind == "file":
                    edges.append((f, tgt))
                elif kind == "pkg":
                    pkgs_used[tgt].add(f)
                elif kind == "missing":
                    missing.append(f"{f} -> {sp}")
            if not is_test_path(f):
                for m in API_LIT.findall(raw):
                    if len(m) > 2:
                        api_literals[re.sub(r"\$\{[^}]*\}", "{}", m.split("?")[0])].add(f)
        adj = collections.defaultdict(set)
        rev = collections.defaultdict(set)
        for a, b in edges:
            adj[a].add(b); rev[b].add(a)

        # ---- reachability -----------------------------------------------------
        rel = lambda f: f[len(root) + 1:] if root else f
        entries = {f for f in fe.files if match_any(rel(f), fcfg["entry_globs"])}
        tests = {f for f in fe.src if is_test_path(f)}
        def reach(starts):
            seen, st = set(starts), list(starts)
            while st:
                for w in adj.get(st.pop(), ()):
                    if w not in seen:
                        seen.add(w); st.append(w)
            return seen
        live = reach(entries - tests)
        with_tests = reach(entries | tests)
        non_test_src = [f for f in fe.src if f not in tests and f not in entries and not f.endswith(".d.ts")]
        unreachable = sorted(f for f in non_test_src if f not in with_tests)
        test_only = sorted(f for f in non_test_src if f in with_tests and f not in live)

        # ---- npm deps -------------------------------------------------------------
        deps = {**fe.pkg.get("dependencies", {}), **fe.pkg.get("devDependencies", {}), **fe.pkg.get("peerDependencies", {})}
        cfg_text = json.dumps(fe.pkg.get("scripts", {})) + " ".join(
            (repo.text(f) or "") for f in fe.files
            if posixpath.dirname(f) == root and not f.endswith("package.json") and "lock" not in f)
        unused = []
        for d in sorted(deps):
            if d in pkgs_used:
                continue
            if d.startswith("@types/") and (d[7:] in pkgs_used or d[7:].replace("__", "/") in pkgs_used or d[7:] in deps):
                continue
            if d in cfg_text or d in IMPLICIT:
                continue
            unused.append(d)
        undeclared = sorted(p for p in pkgs_used if p not in deps and p not in NODE_BUILTIN and not p.startswith("next/")
                            and p != "next")
        undeclared = [p for p in undeclared if p not in ("react", "react-dom") or p not in deps]

        # ---- areas / coupling -----------------------------------------------------
        def area(f):
            r = rel(f)
            m = re.match(r"src/app/([^/]+)/", r)
            if m and "." not in m.group(1):
                return "app/" + m.group(1)
            if r.startswith("src/app/"):
                return "app/(root)"
            m = re.match(r"src/([^/]+)", r)
            return m.group(1) if m else (r.split("/")[0] if "/" in r else "(root)")
        area_edges = collections.Counter()
        for a, b in edges:
            if is_test_path(a):
                continue
            aa, ab = area(a), area(b)
            if aa != ab:
                area_edges[(aa, ab)] += 1
        consumers = {f: {area(x) for x in rev[f] if not is_test_path(x)} for f in fe.src}
        lib_single = []
        for f in fe.src:
            r = rel(f)
            if (r.startswith("src/lib/") or r.startswith("src/hooks/") or r.startswith("src/app/components/")) \
                    and not is_test_path(f) and len(consumers[f]) == 1:
                lib_single.append({"path": f, "only_used_by": sorted(consumers[f])[0]})
        # cross-route imports (app/X -> app/Y), excluding shared app/components and (root)
        cross_route = [(a, b, n) for (a, b), n in area_edges.items()
                       if a.startswith("app/") and b.startswith("app/") and "(root)" not in (a, b) and "app/components" not in (a, b)]
        domain_edges = collections.Counter()
        for a, b in edges:
            da, db = domain_of(a, cfg["domains"]), domain_of(b, cfg["domains"])
            if da and db and da != db and not is_test_path(a):
                domain_edges[(da, db)] += 1

        sim = similar_files(repo, [f for f in fe.src if not is_test_path(f)], threshold=0.7)
        result[root or "."] = {
            "files": len(fe.src), "tests": len(tests), "entries": len(entries),
            "manifest": {"name": fe.pkg.get("name"), "deps": len(fe.pkg.get("dependencies", {})),
                         "dev_deps": len(fe.pkg.get("devDependencies", {}))},
            "unreachable_from_entries": unreachable, "reachable_only_from_tests": test_only,
            "unresolved_imports": missing[:100],
            "npm_declared_not_used": unused, "npm_used_not_declared": undeclared,
            "area_edges": [{"from": a, "to": b, "count": n} for (a, b), n in area_edges.most_common(40)],
            "cross_route_imports": [{"from": a, "to": b, "count": n} for a, b, n in sorted(cross_route, key=lambda x: -x[2])],
            "single_consumer_modules": lib_single,
            "domain_edges": [{"from": a, "to": b, "count": n} for (a, b), n in domain_edges.most_common(30)],
            "near_duplicates": sim,
            "aliases": fe.aliases,
        }
        repo.cache.setdefault("ts_edges", []).extend(edges)
        repo.cache.setdefault("ts_api_literals", {}).update({k: sorted(v) for k, v in api_literals.items()})
        repo.cache.setdefault("ts_files", []).extend(fe.src)

        tag = root or "."
        if unreachable:
            sec.add("TS-001", "medium", f"[{tag}] Source files unreachable from any Next.js entry point or test", count=len(unreachable),
                    evidence=unreachable, hint="Real dead-code candidates (unlike knip run from the wrong dir). Verify no dynamic import()/next/dynamic with computed paths.")
        if test_only:
            sec.add("TS-002", "low", f"[{tag}] Modules reachable only from tests", count=len(test_only), evidence=test_only)
        if missing:
            sec.add("TS-003", "high", f"[{tag}] Relative/alias imports that do not resolve", count=len(missing), evidence=missing)
        if unused:
            sec.add("TS-004", "medium", f"[{tag}] npm packages declared but never imported or referenced by config", count=len(unused), evidence=unused)
        if undeclared:
            sec.add("TS-005", "medium", f"[{tag}] Imported npm packages not in this package.json", count=len(undeclared), evidence=undeclared,
                    hint="Often resolved from a parent/root node_modules — a sign of a stray root manifest.")
        if cross_route:
            sec.add("TS-006", "low", f"[{tag}] Route folders importing from other route folders", count=len(cross_route),
                    evidence=[f"{a} -> {b} ({n})" for a, b, n in sorted(cross_route, key=lambda x: -x[2])],
                    hint="Shared code should live in features/ or components/, not inside another route.")
        if lib_single:
            sec.add("TS-007", "low", f"[{tag}] lib/hooks/shared modules used by exactly one area (colocate them)", count=len(lib_single),
                    evidence=[f"{x['path']} <- {x['only_used_by']}" for x in lib_single])
        if sim:
            sec.add("TS-008", "low", f"[{tag}] Near-duplicate TS files", count=len(sim), evidence=[f"{p['similarity']} {p['a']} ~ {p['b']}" for p in sim])
        metrics["ts.files"] += len(fe.src)
        metrics["ts.unreachable_files"] += len(unreachable)
        metrics["ts.unresolved_imports"] += len(missing)
        metrics["ts.npm_unused"] += len(unused)
        metrics["ts.npm_undeclared"] += len(undeclared)
        metrics["ts.cross_route_imports"] += len(cross_route)
        metrics["ts.single_consumer_modules"] += len(lib_single)

    # root-level manifest with no source of its own (stray install)
    for rel_ in repo.files:
        if rel_.endswith("package.json") and posixpath.dirname(rel_) not in roots:
            d = posixpath.dirname(rel_)
            try:
                pj = json.loads(repo.text(rel_) or "{}")
            except json.JSONDecodeError:
                pj = {}
            frontend_deps = set()
            for r_ in roots:
                try:
                    p2 = json.loads(repo.text(f"{r_}/package.json" if r_ else "package.json") or "{}")
                except json.JSONDecodeError:
                    p2 = {}
                frontend_deps |= set(p2.get("dependencies", {}))
            overlap = sorted(set(pj.get("dependencies", {})) & frontend_deps)
            if overlap:
                sec.add("TS-009", "medium", f"{rel_} duplicates {len(overlap)} dependencies of the real frontend manifest",
                        count=len(overlap), evidence=overlap,
                        hint="Run knip/npm from the app dir; remove the stray manifest + node_modules once confirmed unused.")
    sec.data = {"roots": roots, "apps": result}
    sec.metrics = dict(metrics)
    return sec
