"""API contract drift: FastAPI routes (from the AST) vs path literals found in frontend sources.

Heuristic by design: frontends often build URLs dynamically, so the report states a match rate
and treats 'unused endpoint' as a *candidate*, never a verdict.
"""
from __future__ import annotations

import collections
import re

from .core import Section, domain_of

SKIP_PATHS = re.compile(r"^/(health|docs|openapi\.json|redoc|metrics)?$|/health")


def _rx(path: str) -> re.Pattern:
    out = re.sub(r"\{[^}/]*:path\}", r".+", path)
    out = re.sub(r"\{[^}]*\}", r"[^/]+", out)
    return re.compile("^" + out.rstrip("/") + "/?$")


def analyze(repo) -> Section:
    sec = Section("contracts")
    routes = repo.cache.get("routes") or []
    lits: dict = repo.cache.get("ts_api_literals") or {}
    if not routes or not lits:
        sec.data = {"note": "needs both Python routes and frontend sources"}
        return sec

    firsts = collections.Counter(r["full_path"].split("/")[1] for r in routes if r["full_path"].count("/") > 1)
    api_prefix = "/" + firsts.most_common(1)[0][0] if firsts else ""
    compiled = [(r, _rx(r["full_path"])) for r in routes]

    called: dict[int, set] = collections.defaultdict(set)
    unmatched = {}
    matched_lits = 0
    for lit, files in lits.items():
        cands = [lit, api_prefix + lit] if not lit.startswith(api_prefix + "/") else [lit]
        hit = False
        for i, (r, rx) in enumerate(compiled):
            if any(rx.match(c.replace("{}", "x")) for c in cands):
                called[i].update(files)
                hit = True
        if hit:
            matched_lits += 1
        elif lit.startswith(api_prefix + "/") or (api_prefix and lit.count("/") >= 2 and re.match(r"^/[a-z_\-]+/", lit)):
            unmatched[lit] = files

    never = [r for i, (r, _) in enumerate(compiled) if i not in called and not SKIP_PATHS.search(r["full_path"])]
    by_dom = collections.defaultdict(lambda: {"routes": 0, "called_by_frontend": 0})
    for i, (r, _) in enumerate(compiled):
        d = domain_of(r["file"], repo.cfg["domains"]) or "unassigned"
        by_dom[d]["routes"] += 1
        by_dom[d]["called_by_frontend"] += 1 if i in called else 0

    # only literals that really look like API paths (share the api prefix) count as 'frontend calls with no route'
    ghost = {k: v for k, v in unmatched.items() if api_prefix and k.startswith(api_prefix + "/")}
    sec.data = {
        "api_prefix_guess": api_prefix, "routes": len(routes), "frontend_literals": len(lits),
        "match_rate_pct": round(100 * sum(1 for i in range(len(compiled)) if i in called) / max(1, len(compiled)), 1),
        "routes_without_frontend_caller": [f"{r['method']} {r['full_path']}  ({r['file']}:{r['line']})" for r in never],
        "frontend_api_literals_without_route": {k: v[:3] if isinstance(v, list) else sorted(v)[:3] for k, v in ghost.items()},
        "by_domain": dict(by_dom),
    }
    if never:
        sec.add("CON-001", "info", "Backend routes no frontend source references (candidates only)", count=len(never),
                evidence=[f"{r['method']} {r['full_path']}" for r in never],
                hint="Check scripts, the agent/worker and external callers before deleting. Dynamic URL building hides callers.")
    if ghost:
        sec.add("CON-002", "high", "Frontend API paths with no matching backend route", count=len(ghost),
                evidence=[f"{k}  <- {', '.join(sorted(v)[:2])}" for k, v in ghost.items()])
    sec.metrics = {"contracts.routes": len(routes), "contracts.routes_no_caller": len(never),
                   "contracts.frontend_ghost_paths": len(ghost)}
    return sec
