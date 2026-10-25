"""Inventory: what is actually in the repo, split into code / tests / docs / data / junk."""
from __future__ import annotations

import collections
import hashlib
import json
import os
import re

from .core import (Section, category, language, match_any, top_dir)

STRAY_PATH = re.compile(r"(^|/)(Users|home)/[^/]+/|(^|/)[A-Za-z]:(/|$)")


def _count_lines(repo, rel: str) -> int:
    try:
        with open(repo.root / rel, "rb") as fh:
            raw = fh.read(5_000_000)
    except OSError:
        return 0
    if b"\0" in raw[:4096] and raw[:2] not in (b"\xff\xfe", b"\xfe\xff"):
        return 0
    n = raw.count(b"\n")
    return n + (1 if raw and not raw.endswith(b"\n") else 0)


def analyze(repo) -> Section:
    sec = Section("inventory")
    cfg = repo.cfg
    icfg = cfg["inventory"]

    rows, hashes = [], collections.defaultdict(list)
    for rel in repo.files:
        size = repo.size(rel)
        cat = category(rel, size, cfg)
        lines = 0 if cat in ("asset", "dump") else _count_lines(repo, rel)
        rows.append({"path": rel, "bytes": size, "lines": lines, "lang": language(rel), "cat": cat})
        if 0 < size <= 20_000_000 and cat not in ("asset",):
            try:
                with open(repo.root / rel, "rb") as fh:
                    hashes[hashlib.sha1(fh.read()).hexdigest()].append(rel)
            except OSError:
                pass

    # ---- aggregates -------------------------------------------------------
    by_cat = collections.defaultdict(lambda: {"files": 0, "lines": 0, "bytes": 0})
    by_lang_code = collections.defaultdict(lambda: {"files": 0, "lines": 0})
    by_top = collections.defaultdict(lambda: {"files": 0, "lines": 0, "bytes": 0, "cats": collections.Counter()})
    for r in rows:
        c = by_cat[r["cat"]]
        c["files"] += 1; c["lines"] += r["lines"]; c["bytes"] += r["bytes"]
        if r["cat"] in ("code", "test", "sql"):
            l = by_lang_code[r["lang"]]
            l["files"] += 1; l["lines"] += r["lines"]
        t = by_top[top_dir(r["path"])]
        t["files"] += 1; t["lines"] += r["lines"]; t["bytes"] += r["bytes"]; t["cats"][r["cat"]] += 1

    total_lines = sum(r["lines"] for r in rows) or 1
    code_lines = sum(r["lines"] for r in rows if r["cat"] in ("code", "test", "sql"))

    # ---- data dirs on disk (ignored runtime data) --------------------------
    data_dirs = {}
    for d in cfg["data_dirs"]:
        p = repo.root / d
        if p.is_dir():
            n = b = 0
            for dp, _, fns in os.walk(p):
                for fn in fns:
                    n += 1
                    try:
                        b += os.path.getsize(os.path.join(dp, fn))
                    except OSError:
                        pass
            data_dirs[d] = {"files": n, "bytes": b, "tracked": sum(1 for t in (repo.tracked or ()) if t.startswith(d + "/"))}

    # ---- duplicates -------------------------------------------------------
    dups = [sorted(v) for v in hashes.values() if len(v) > 1]
    dups.sort(key=lambda g: (-len(g), g[0]))

    # ---- clutter ----------------------------------------------------------
    clutter = [r["path"] for r in rows if match_any(r["path"], icfg["clutter_globs"])]
    tracked_clutter = [p for p in clutter if repo.is_tracked(p)]

    # ---- manifests --------------------------------------------------------
    manifests = []
    for rel in repo.files:
        name = rel.rsplit("/", 1)[-1]
        if name == "package.json":
            try:
                pj = json.loads(repo.text(rel) or "{}")
            except json.JSONDecodeError:
                pj = {}
            manifests.append({"path": rel, "kind": "npm", "name": pj.get("name"),
                              "deps": len(pj.get("dependencies", {})), "dev_deps": len(pj.get("devDependencies", {}))})
        elif re.match(r"(requirements.*|.*requirements)\.txt$", name) or name in ("pyproject.toml", "setup.py", "setup.cfg", "Pipfile"):
            manifests.append({"path": rel, "kind": "python", "name": name})
        elif name in LOCKS:
            manifests.append({"path": rel, "kind": "lock", "name": name})
    npm = [m for m in manifests if m["kind"] == "npm"]
    pyman = [m for m in manifests if m["kind"] == "python"]

    stray = [r["path"] for r in rows if STRAY_PATH.search(r["path"])]
    root_files = [r["path"] for r in rows if "/" not in r["path"]]
    big = sorted(rows, key=lambda r: -r["bytes"])[:20]
    top_lines = sorted((r for r in rows if r["cat"] in ("code", "test")), key=lambda r: -r["lines"])[:20]

    sec.data = {
        "totals": {"files": len(rows), "lines": total_lines, "code_lines": code_lines,
                   "code_share_pct": round(100 * code_lines / total_lines, 1)},
        "by_category": {k: v for k, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["lines"])},
        "code_by_language": dict(sorted(by_lang_code.items(), key=lambda kv: -kv[1]["lines"])),
        "by_top_dir": {k: {**{kk: vv for kk, vv in v.items() if kk != "cats"}, "cats": dict(v["cats"])}
                       for k, v in sorted(by_top.items(), key=lambda kv: -kv[1]["files"])},
        "data_dirs_on_disk": data_dirs,
        "duplicate_groups": dups[:60],
        "clutter": clutter[:200],
        "manifests": manifests,
        "stray_path_mirrors": stray,
        "root_files": root_files,
        "largest_files": [{"path": r["path"], "bytes": r["bytes"], "cat": r["cat"]} for r in big],
        "longest_code_files": [{"path": r["path"], "lines": r["lines"]} for r in top_lines],
    }

    # ---- findings ---------------------------------------------------------
    non_code = round(100 - 100 * code_lines / total_lines, 1)
    sec.add("INV-001", "info", f"{non_code}% of all lines are not code/tests/SQL (data, docs, generated, config)",
            detail="Raw cloc-style totals overstate code size; use `code_lines` for reorg sizing.")
    if dups:
        sec.add("INV-002", "medium", "Byte-identical duplicate files", count=len(dups),
                evidence=[" == ".join(g[:4]) for g in dups],
                hint="Keep one canonical copy; delete the other or turn it into a reference.")
    if clutter:
        sev = "high" if tracked_clutter else "low"
        sec.add("INV-003", sev, "Logs/temp/backup/copy files in the repo", count=len(clutter),
                evidence=clutter, detail=f"{len(tracked_clutter)} are tracked by git." if repo.tracked is not None else "",
                hint="Delete and add patterns to .gitignore.")
    if len(npm) > 1:
        sec.add("INV-004", "medium", "Multiple package.json manifests", count=len(npm),
                evidence=[f"{m['path']} (deps={m['deps']}, dev={m['dev_deps']})" for m in npm],
                hint="One manifest per app; root manifest + nested app manifest usually means a stale root install.")
    if len(pyman) > 3:
        sec.add("INV-005", "medium", "Python dependencies declared in many places", count=len(pyman),
                evidence=[m["path"] for m in pyman], hint="Consolidate into one pyproject.toml.")
    if stray:
        sec.add("INV-006", "medium", "Files under tool-generated absolute-path mirrors (e.g. .codes/Users/...)",
                evidence=stray, hint="Merge into the real location or delete.")
    if len(root_files) > icfg["loose_root_files"]:
        sec.add("INV-007", "low", f"{len(root_files)} loose files at repo root", evidence=root_files,
                hint="Root should hold only manifests, README and top-level config.")
    for d, info in data_dirs.items():
        if info["tracked"]:
            sec.add("INV-008", "medium", f"Runtime data dir '{d}' has tracked files", count=info["tracked"],
                    hint="Runtime data belongs outside git (gitignore + configurable DATA_DIR).")
    big_tracked = [r for r in big if r["bytes"] > icfg["big_file_bytes"] and r["cat"] not in ("lock", "asset") and repo.is_tracked(r["path"])]
    if big_tracked:
        sec.add("INV-009", "low", "Large tracked non-lock files", evidence=[f"{r['path']} ({r['bytes']//1024} KB)" for r in big_tracked])

    sec.metrics = {
        "inventory.files": len(rows), "inventory.code_lines": code_lines,
        "inventory.duplicate_groups": len(dups), "inventory.clutter_files": len(clutter),
        "inventory.root_files": len(root_files), "inventory.npm_manifests": len(npm),
        "inventory.stray_path_files": len(stray),
    }
    return sec


LOCKS = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "uv.lock", "poetry.lock"}
