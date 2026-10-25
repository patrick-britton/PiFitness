"""Move impact (what references a path/module that is about to move) and the domain x layer matrix.

`impact` is the safety net for every `git mv`: run it before to know what must be rewritten, and
after to prove nothing still points at an old location.
"""
from __future__ import annotations

import collections
import json
import os
import posixpath
import re

from .core import Section, domain_of, layer_of

SCAN_EXT = {".py", ".ts", ".tsx", ".js", ".mjs", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".sh",
            ".ps1", ".service", ".timer", ".conf", ".md", ".txt", ".sql", ".env", ".html", ".css", ".cmd", ".bat"}


def _variants(old: str) -> list[tuple[str, str]]:
    """(needle, kind) pairs for a repo-relative file or directory path."""
    old = old.replace("\\", "/").strip("/")
    out = [(old, "path"), (old.replace("/", "\\"), "path")]
    if old.endswith(".py"):
        mod = old[:-3].replace("/", ".")
        out.append((mod, "module"))
    elif "." not in posixpath.basename(old):
        out.append((old.replace("/", "."), "module"))
    return out


def analyze_moves(repo, moves: dict[str, str]) -> Section:
    sec = Section("impact")
    needles: dict[str, list] = {}
    for old, new in moves.items():
        for n, kind in _variants(old):
            if (old, kind) not in needles.setdefault(n, []):
                needles[n].append((old, kind))
    if not needles:
        sec.data = {"note": "no moves supplied"}
        return sec
    alt = "|".join(re.escape(n) for n in sorted(needles, key=len, reverse=True))
    rx = re.compile(r"(?<![\w./\\-])(" + alt + r")(?![\w])")
    hits = collections.defaultdict(list)
    seen = set()
    for rel in repo.files:
        if os.path.splitext(rel)[1].lower() not in SCAN_EXT or repo.size(rel) > 2_000_000:
            continue
        t = repo.text(rel)
        if not t:
            continue
        for i_line, line in enumerate(t.splitlines(), 1):
            for m in rx.finditer(line):
                text = m.group(1)
                # attribute the hit to every moving path that is a prefix of the matched text
                owners = {}
                for n, lst in needles.items():
                    if text == n or text.startswith((n + ".", n + "/", n + "\\")):
                        for old, kind in lst:
                            owners[old] = kind
                for old, kind in owners.items():
                    key = (old, rel, i_line, m.start())
                    if key in seen:
                        continue
                    seen.add(key)
                    hits[old].append({"file": rel, "line": i_line, "match": text, "kind": _classify(rel, line, kind),
                                      "text": line.strip()[:160]})
    summary = {}
    for old, new in moves.items():
        hs = [h for h in hits.get(old, []) if h["file"] != old]
        by = collections.Counter(h["kind"] for h in hs)
        summary[old] = {"new": new, "references": len(hs), "by_kind": dict(by),
                        "files": sorted({h["file"] for h in hs}), "samples": hs[:8]}
    sec.data = {"moves": summary}
    total = sum(s["references"] for s in summary.values())
    sec.add("IMP-001", "info", f"{total} references to {len(moves)} moving path(s)", count=total,
            evidence=[f"{o}: {s['references']} refs in {len(s['files'])} files {s['by_kind']}"
                      for o, s in summary.items() if s["references"]], cap=40)
    sec.metrics = {"impact.total_references": total}
    return sec


def _classify(rel: str, line: str, kind: str) -> str:
    ext = os.path.splitext(rel)[1].lower()
    if ext == ".py":
        return "python-import" if re.match(r"\s*(from|import)\s", line) else "python-string"
    if ext in (".ts", ".tsx", ".js", ".mjs"):
        return "js-import" if re.match(r"\s*(import|export)\b", line) or "require(" in line else "js-string"
    if ext in (".md", ".txt"):
        return "docs"
    if ext in (".service", ".timer", ".conf", ".sh", ".ps1", ".cmd", ".bat", ".yaml", ".yml"):
        return "deploy-config"
    return "config"


def load_moves(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8-sig") as fh:
        data = json.load(fh)
    if isinstance(data, list):  # [{"old":..,"new":..}]
        return {d["old"]: d.get("new", "") for d in data}
    return data


# ============================================================================ domain matrix
def analyze_domains(repo) -> Section:
    sec = Section("domains")
    domains = repo.cfg["domains"]
    if not domains:
        sec.data = {"note": "no [domains] configured in audit.toml"}
        return sec
    files = [f for f in repo.files if f.endswith((".py", ".ts", ".tsx", ".sql")) or f.endswith(".md")]
    matrix = collections.defaultdict(lambda: collections.Counter())
    assigned = {}
    for f in files:
        if f.endswith((".md", "__init__.py")) or ".config." in f or f.startswith(("scripts/", ".features/", "memory-bank/")):
            continue
        d = domain_of(f, domains) or "(unassigned)"
        assigned[f] = d
        matrix[d][layer_of(f)] += 1
    unassigned = sorted(f for f, d in assigned.items() if d == "(unassigned)")

    cross = collections.Counter()
    for src, dst in repo.cache.get("py_edges", []) + repo.cache.get("ts_edges", []):
        a, b = assigned.get(src), assigned.get(dst)
        if a and b and a != b and "(unassigned)" not in (a, b) and b not in repo.cfg["shared_domains"]:
            cross[(a, b)] += 1
    spread = {}
    for d, c in matrix.items():
        spread[d] = {"files": sum(c.values()), "layers": dict(c)}
    sec.data = {"matrix": spread, "unassigned": unassigned, "cross_domain_imports":
                [{"from": a, "to": b, "count": n} for (a, b), n in cross.most_common(30)],
                "assignment": assigned}
    if unassigned:
        sec.add("DOM-001", "medium", "Files that match no domain keyword (need a human decision)", count=len(unassigned),
                evidence=unassigned, hint="Add keywords to [domains] in audit.toml or classify as core/shared/integration.")
    thin = [d for d, v in spread.items() if d != "(unassigned)" and len(v["layers"]) <= 1]
    if cross:
        sec.add("DOM-002", "info", "Cross-domain import edges (coupling between slices)", count=sum(cross.values()),
                evidence=[f"{a} -> {b}: {n}" for (a, b), n in cross.most_common(12)])
    sec.metrics = {"domains.unassigned_files": len(unassigned), "domains.cross_edges": sum(cross.values())}
    return sec
