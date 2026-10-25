"""scripts/ triage: scores each script as durable tool vs ticket-specific one-off, and detects
scripts that are really integration tests. Evidence-based (names, references, age, imports)."""
from __future__ import annotations

import datetime as dt
import posixpath
import re

from .core import Section


def analyze(repo) -> Section:
    sec = Section("triage")
    tcfg = repo.cfg["triage"]
    py = repo.cache.get("py_modules", {})
    edges = repo.cache.get("py_edges", [])
    importers = {}
    for s, d in edges:
        importers.setdefault(d, set()).add(s)
    oneoff_rx = re.compile(tcfg["oneoff_regex"], re.I)
    # reference corpus: any non-py text mentioning a script's filename (PS1, sh, md, service, yaml)
    corpus = {}
    for rel in repo.files:
        if rel.endswith((".ps1", ".sh", ".yaml", ".yml", ".service", ".timer", ".md", ".json", ".toml", ".txt", ".conf")):
            corpus[rel] = repo.text(rel) or ""
    now = dt.datetime.now(dt.timezone.utc)

    rows = []
    for d in tcfg["dirs"]:
        for f in repo.files:
            if not f.startswith(d + "/") or f.count("/") != d.count("/") + 1 or not f.endswith((".py", ".mjs", ".sh", ".ps1")):
                continue
            name = posixpath.basename(f)
            stem = name.rsplit(".", 1)[0]
            score, why = 0, []
            if oneoff_rx.search(f):
                score += 3; why.append("ticket/temp id in name")
            if any(stem.lower().startswith(p) for p in tcfg["oneoff_prefixes"]):
                score += 1; why.append("diagnostic-style prefix")
            refs = [k for k, v in corpus.items() if name in v and k != f]
            if refs:
                score -= 3; why.append(f"referenced by {len(refs)} non-code file(s)")
            imp = importers.get(f, set())
            if imp:
                score -= 3; why.append(f"imported by {len(imp)} module(s)")
            text = repo.text(f) or ""
            if f.endswith(".py"):
                if re.search(r"argparse|click|typer", text):
                    score -= 1; why.append("has CLI args")
                if text.lstrip().startswith(('"""', "'''")):
                    score -= 1; why.append("has docstring")
                if re.search(r"TestClient|from backend\.main import app", text):
                    why.append("drives the app directly (integration-test-like)")
            age = None
            if repo.has_git:
                out = (repo.git("log", "-1", "--format=%aI", "--", f) or "").strip()
                if out:
                    age = (now - dt.datetime.fromisoformat(out)).days
                    if age > tcfg["stale_days"]:
                        score += 1; why.append(f"untouched {age}d")
            kind = "keep-tool"
            if name.startswith("test_"):
                kind = "misplaced-test"
            elif "integration-test-like" in " ".join(why) or "drives the app" in " ".join(why):
                kind = "convert-to-test-or-delete" if score >= 2 else "review"
            elif score >= 3:
                kind = "archive-or-delete"
            elif score >= 1:
                kind = "review"
            rows.append({"path": f, "kind": kind, "score": score, "age_days": age, "why": why,
                         "lines": text.count("\n") + 1, "domain_hint": None})
    by_kind = {}
    for r in rows:
        by_kind.setdefault(r["kind"], []).append(r["path"])
    sec.data = {"scripts": sorted(rows, key=lambda r: (r["kind"], -r["score"], r["path"])), "by_kind": by_kind}
    n_one = len(by_kind.get("archive-or-delete", []))
    if rows:
        sec.add("TRI-001", "medium" if n_one > 10 else "low", f"{n_one} of {len(rows)} scripts look like ticket-specific one-offs",
                count=n_one, evidence=by_kind.get("archive-or-delete", []),
                hint="Delete (git keeps history) or move to an `archive/` branch; keep only durable tools in tools/.")
    for k, fid, title in (("convert-to-test-or-delete", "TRI-002", "Scripts that drive the app like integration tests"),
                          ("misplaced-test", "TRI-003", "Test files inside scripts/"),
                          ("review", "TRI-004", "Scripts needing a human decision")):
        if by_kind.get(k):
            sec.add(fid, "low", title, count=len(by_kind[k]), evidence=by_kind[k])
    sec.metrics = {"scripts.total": len(rows), "scripts.oneoff": n_one,
                   "scripts.keep": len(by_kind.get("keep-tool", [])), "scripts.misplaced_tests": len(by_kind.get("misplaced-test", []))}
    return sec
