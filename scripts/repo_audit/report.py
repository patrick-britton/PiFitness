"""Report writers: sections/*.json, report.json, REPORT.md, TREE.txt; plus `diff` and `check`."""
from __future__ import annotations

import collections
import datetime as dt
import json
from pathlib import Path

from .core import SEV_ORDER

# Metrics that must never get worse between baseline and current (lower is better).
GATED = [
    "py.dangling_imports", "py.pkg_cycles", "py.module_cycles", "py.layer_violations", "py.private_cross_imports",
    "py.orphans_likely_dead", "py.parse_errors", "py.near_duplicate_pairs",
    "hygiene.sensitive_exposed", "hygiene.secret_hits", "hygiene.tracked_junk", "hygiene.pyc_husks",
    "hygiene.legacy_py_imports", "hygiene.legacy_leftover_dirs",
    "ts.unresolved_imports", "ts.unreachable_files", "ts.npm_undeclared", "contracts.frontend_ghost_paths",
    "inventory.duplicate_groups", "inventory.clutter_files", "inventory.stray_path_files", "inventory.npm_manifests",
    "sql.migrations_outside_db_dir", "scripts.oneoff", "scripts.misplaced_tests",
]


def _default(o):
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    if isinstance(o, Path):
        return str(o)
    return str(o)


def write_all(repo, sections: dict, out: Path, meta: dict) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    (out / "sections").mkdir(exist_ok=True)
    metrics, findings = {}, []
    for name, s in sections.items():
        metrics.update(s.metrics)
        for f in s.findings:
            d = f.to_dict(); d["section"] = name
            findings.append(d)
        (out / "sections" / f"{name}.json").write_text(
            json.dumps({"data": s.data, "findings": [f.to_dict() for f in s.findings], "metrics": s.metrics,
                        "error": s.error}, indent=1, default=_default), encoding="utf-8")
    findings.sort(key=lambda f: (SEV_ORDER[f["severity"]], f["id"]))
    full = {"meta": meta, "metrics": metrics, "findings": findings,
            "errors": {n: s.error for n, s in sections.items() if s.error}}
    (out / "report.json").write_text(json.dumps(full, indent=1, default=_default), encoding="utf-8")
    (out / "REPORT.md").write_text(render_md(repo, sections, full), encoding="utf-8")
    (out / "TREE.txt").write_text(render_tree(repo), encoding="utf-8")
    return full


# ----------------------------------------------------------------------------- markdown
def _short(s: str, n: int = 170) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def render_md(repo, sections, full) -> str:
    L = []
    m = full["meta"]
    L += [f"# Repo audit — {m['when']}", "",
          f"root: `{m['root']}` · git: `{m.get('commit') or 'n/a'}` · tool v{m['version']} · "
          f"sections: {', '.join(sections)}", ""]
    if full["errors"]:
        L += ["## ⚠ Section errors", ""] + [f"- **{k}**: {v}" for k, v in full["errors"].items()] + [""]
    inv = sections.get("inventory")
    if inv and inv.data:
        t = inv.data["totals"]
        L += ["## Size, honestly", "",
              f"{t['files']} files, {t['lines']:,} lines total, of which **{t['code_lines']:,} are code/tests/SQL "
              f"({t['code_share_pct']}%)**.", "",
              "| category | files | lines |", "|---|---:|---:|"]
        L += [f"| {k} | {v['files']} | {v['lines']:,} |" for k, v in inv.data["by_category"].items()]
        L += ["", "| code language | files | lines |", "|---|---:|---:|"]
        L += [f"| {k} | {v['files']} | {v['lines']:,} |" for k, v in inv.data["code_by_language"].items()]
        L += [""]
    L += ["## Scoreboard (lower is better unless noted)", "", "| metric | value |", "|---|---:|"]
    L += [f"| {k} | {v} |" for k, v in sorted(full["metrics"].items())]
    L += ["", "## Findings", "", "| sev | id | finding | count |", "|---|---|---|---:|"]
    L += [f"| {f['severity']} | {f['id']} | {_short(f['title'], 110)} | {f['count']} |" for f in full["findings"]]
    L += [""]
    for f in full["findings"]:
        cap = {"critical": 12, "high": 10, "medium": 6}.get(f["severity"], 3)
        L += [f"### {f['id']} · {f['severity']} · {f['title']}", ""]
        if f["detail"]:
            L += [f["detail"], ""]
        L += [f"- {_short(e)}" for e in f["evidence"][:cap]]
        if f["count"] > cap and f["evidence"]:
            L += [f"- … {f['count'] - cap} more (see sections/{f['section']}.json)"]
        if f["hint"]:
            L += ["", f"> {f['hint']}"]
        L += [""]
    dom = sections.get("domains")
    if dom and dom.data.get("matrix"):
        layers = sorted({l for v in dom.data["matrix"].values() for l in v["layers"]})
        L += ["## Domain × layer (file counts)", "", "| domain | " + " | ".join(layers) + " | total |",
              "|---|" + "---:|" * (len(layers) + 1)]
        for d, v in sorted(dom.data["matrix"].items(), key=lambda kv: -kv[1]["files"]):
            L.append(f"| {d} | " + " | ".join(str(v["layers"].get(l, "")) for l in layers) + f" | {v['files']} |")
        L += [""]
    py = sections.get("python")
    if py and py.data.get("routes"):
        c = collections.Counter(r["file"] for r in py.data["routes"])
        L += ["## Routes per router file", ""] + [f"- `{f}`: {n}" for f, n in c.most_common()] + [""]
    sql = sections.get("sql")
    if sql and sql.data.get("proposed_order"):
        L += ["## Proposed migration numbering (first 15; confirm vs prod)", "", "| old | new |", "|---|---|"]
        L += [f"| {p['old']} | {p['new']} |" for p in sql.data["proposed_order"][:15]] + [""]
    return "\n".join(L)


# ----------------------------------------------------------------------------- tree
def render_tree(repo, max_depth: int = 4, list_files_up_to: int = 14) -> str:
    dirs = collections.defaultdict(lambda: {"files": [], "n": 0})
    for f in repo.files:
        parts = f.split("/")
        for i in range(0, len(parts)):
            d = "/".join(parts[:i]) if i else ""
            dirs[d]["n"] += 1
        dirs["/".join(parts[:-1])]["files"].append(parts[-1])
    out = [f"{repo.root.name}/  ({dirs['']['n']} files)"]

    def walk(d: str, depth: int):
        kids = sorted({k for k in dirs if k and k.rsplit("/", 1)[0] == d and (k.count("/") == (d.count("/") + 1 if d else 0))})
        info = dirs[d]
        if depth > 0 and len(info["files"]) <= list_files_up_to and depth <= max_depth:
            for fn in sorted(info["files"]):
                out.append("  " * (depth + 1) + fn)
        elif depth > 0 and info["files"]:
            out.append("  " * (depth + 1) + f"({len(info['files'])} files)")
        if depth >= max_depth:
            return
        for k in kids:
            out.append("  " * (depth + 1) + k.rsplit("/", 1)[-1] + f"/  ({dirs[k]['n']})")
            walk(k, depth + 1)
    # root files
    for fn in sorted(dirs[""]["files"]):
        out.append("  " + fn)
    tops = sorted({f.split("/")[0] for f in repo.files if "/" in f})
    for t in tops:
        out.append(f"  {t}/  ({dirs[t]['n']})")
        walk(t, 1)
    return "\n".join(out) + "\n"


# ----------------------------------------------------------------------------- diff / check
def load_metrics(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")).get("metrics", {})


def diff_metrics(old: dict, new: dict) -> list[str]:
    rows = []
    for k in sorted(set(old) | set(new)):
        a, b = old.get(k), new.get(k)
        if a != b:
            arrow = "↓" if (b or 0) < (a or 0) else "↑"
            good = (k in GATED and (b or 0) < (a or 0))
            bad = (k in GATED and (b or 0) > (a or 0))
            rows.append(f"{'✔' if good else '✘' if bad else ' '} {k:42} {a!s:>8} -> {b!s:<8} {arrow}")
    return rows


def check_gate(old: dict, new: dict) -> list[str]:
    return [f"{k}: {old[k]} -> {new.get(k, 0)}" for k in GATED if k in old and (new.get(k, 0) or 0) > (old[k] or 0)]
