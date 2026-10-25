"""repo_audit CLI.

  python scripts/repo_audit                      # full audit -> .audit/
  python scripts/repo_audit python frontend      # only some sections (dependencies run automatically)
  python scripts/repo_audit baseline             # full audit, then freeze .audit/baseline.json
  python scripts/repo_audit impact --map moves.json
  python scripts/repo_audit diff                 # baseline vs latest report
  python scripts/repo_audit check                # exit 1 if any gated metric regressed vs baseline
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from . import (contracts, external, gitstats, hygiene, impact, inventory, pyanalysis, report, sqlmig, triage,
               tsanalysis)
from .core import Repo, Section, load_config

VERSION = "1.0"
ORDER = ["inventory", "python", "frontend", "contracts", "domains", "hygiene", "sql", "triage", "git", "external"]
DEPS = {"contracts": ["python", "frontend"], "domains": ["python", "frontend"], "triage": ["python"]}
RUN = {
    "inventory": inventory.analyze, "python": pyanalysis.analyze, "frontend": tsanalysis.analyze,
    "contracts": contracts.analyze, "domains": impact.analyze_domains, "hygiene": hygiene.analyze,
    "sql": sqlmig.analyze, "triage": triage.analyze, "git": gitstats.analyze,
}


def _expand(names: list[str]) -> list[str]:
    want = set()
    for n in names:
        want.add(n)
        want.update(DEPS.get(n, []))
    return [n for n in ORDER if n in want]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="repo_audit", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("commands", nargs="*", help="sections | all | baseline | impact | diff | check")
    ap.add_argument("--root", default=".")
    ap.add_argument("--out", default=None, help="output dir (default: <root>/.audit)")
    ap.add_argument("--config", default=None)
    ap.add_argument("--no-git", action="store_true")
    ap.add_argument("--map", help="impact: JSON file {old_path: new_path} or [{old,new}]")
    ap.add_argument("--paths", nargs="*", default=[], help="impact: old paths to check (no new location needed)")
    ap.add_argument("--baseline", default=None)
    ap.add_argument("--with-external", action="store_true", help="also run cloc/knip wrappers")
    ap.add_argument("-q", "--quiet", action="store_true")
    a = ap.parse_args(argv)

    root = Path(a.root).resolve()
    cfg = load_config(root, a.config)
    out = Path(a.out) if a.out else root / cfg["out_dir"]
    cmds = a.commands or ["all"]
    log = (lambda *x: None) if a.quiet else (lambda *x: print(*x, file=sys.stderr))

    if cmds[0] in ("diff", "check"):
        base = Path(a.baseline) if a.baseline else out / "baseline.json"
        cur = out / "report.json"
        if not base.is_file() or not cur.is_file():
            print(f"need {base} and {cur}; run `baseline` first, then `all`.", file=sys.stderr)
            return 2
        old, new = report.load_metrics(base), report.load_metrics(cur)
        if cmds[0] == "diff":
            print("\n".join(report.diff_metrics(old, new)) or "no metric changes")
            return 0
        bad = report.check_gate(old, new)
        print("REGRESSIONS:\n  " + "\n  ".join(bad) if bad else "gate OK: no gated metric regressed")
        return 1 if bad else 0

    repo = Repo(root, cfg, use_git=not a.no_git)
    log(f"[audit] {len(repo.files)} files analysed (git={'yes' if repo.has_git else 'no'}) in {root}")

    if cmds[0] == "impact":
        moves = impact.load_moves(a.map) if a.map else {}
        moves.update({p: "" for p in a.paths})
        sec = impact.analyze_moves(repo, moves)
        out.mkdir(parents=True, exist_ok=True)
        (out / "sections").mkdir(exist_ok=True)
        import json
        (out / "sections" / "impact.json").write_text(json.dumps(sec.data, indent=1), encoding="utf-8")
        for old, s in sec.data.get("moves", {}).items():
            print(f"{old} -> {s['new'] or '(?)'}: {s['references']} refs {s['by_kind']}")
            for h in s["samples"][:5]:
                print(f"    {h['file']}:{h['line']} [{h['kind']}] {h['text']}")
        total = sec.metrics.get("impact.total_references", 0)
        print(f"TOTAL references: {total}")
        return 0 if total == 0 else 1

    names = ORDER if cmds[0] in ("all", "baseline") else _expand([c for c in cmds if c in ORDER])
    if not a.with_external and "external" in names and cmds[0] in ("all", "baseline"):
        names = [n for n in names if n != "external"]
    unknown = [c for c in cmds if c not in ORDER + ["all", "baseline"]]
    if unknown:
        print(f"unknown command(s): {unknown}", file=sys.stderr)
        return 2

    sections: dict[str, Section] = {}
    for n in names:
        t0 = time.time()
        try:
            sec = external.analyze(repo, out) if n == "external" else RUN[n](repo)
        except Exception as e:  # never let one analyzer kill the audit
            sec = Section(n, error=f"{type(e).__name__}: {e}")
            log(traceback.format_exc())
        sec.seconds = round(time.time() - t0, 2)
        sections[n] = sec
        log(f"[audit] {n:10} {sec.seconds:6.2f}s  findings={len(sec.findings)}" + (f"  ERROR {sec.error}" if sec.error else ""))

    commit = (repo.git("rev-parse", "--short", "HEAD") or "").strip() if repo.has_git else ""
    meta = {"when": datetime.now().strftime("%Y-%m-%d %H:%M"), "root": str(root), "version": VERSION,
            "commit": commit, "dirty": bool((repo.git("status", "--porcelain") or "").strip()) if repo.has_git else None}
    full = report.write_all(repo, sections, out, meta)
    if cmds[0] == "baseline":
        shutil.copyfile(out / "report.json", out / "baseline.json")
        log(f"[audit] baseline frozen -> {out / 'baseline.json'}")
    sev = {}
    for f in full["findings"]:
        sev[f["severity"]] = sev.get(f["severity"], 0) + 1
    print(f"audit written to {out}  |  findings: " + ", ".join(f"{k}={v}" for k, v in sorted(sev.items())))
    return 0
