#!/usr/bin/env python3
"""check_docs.py: citation, heading and INDEX checks for repository documentation.

Subcommands:
  cites    R2: backticked paths exist; line ranges valid; factual bullets carry a path
  headings R3: duplicate-heading scan within each markdown file
  index    one INDEX row per file in docs/ and .clinerules/

Ratchet behaviour (docs/workflow/PLAN.md Appendix A): every violation is keyed
`check|path|rule-or-symbol|8-char-hash-of-the-normalized-message`. Baselines
live in tools/baselines/<check>.txt. Without --update-baseline the gate exits 1
on any violation not present in the baseline; stale baseline keys print PRUNE
but do not fail. --update-baseline writes the current violations and may only
remove keys unless --allow-grow is given.
"""

import argparse
import hashlib
import re
import sys
from pathlib import Path

REPO_ROOT = Path.cwd()
BASELINE_DIR = REPO_ROOT / "tools" / "baselines"

BACKTICK = re.compile(r"`([^`]+)`")
LINE_SPEC_RE = re.compile(r":((?:\d+(?:-\d+)?)(?:,(?:\d+(?:-\d+)?))*)$")
BULLET_RE = re.compile(r"^([*\-]|\d+\.)\s+")


def sha8(message: str) -> str:
    return hashlib.sha256(message.encode("utf-8")).hexdigest()[:8]


def violation_key(check: str, path: str, rule: str, detail: str) -> str:
    message = f"{path}|{rule}|{detail}"
    return f"{check}|{path}|{rule}|{sha8(message)}"


def load_baseline(path: Path):
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def write_baseline(path: Path, keys, grow: bool):
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    existing = load_baseline(path)
    keys = set(keys)
    if grow:
        keys = existing | keys
    else:
        keys = existing & keys
    path.write_text("\n".join(sorted(keys)) + ("\n" if keys else ""))


def gate(violations, baseline, check_name):
    v_keys = {key for key, _ in violations}
    new_violations = [(key, detail) for key, detail in violations if key not in baseline]
    stale = baseline - v_keys
    for _key, detail in new_violations:
        print(detail)
    for key in sorted(stale):
        print(f"{check_name}: PRUNE {key} (no longer occurs)")
    return 1 if new_violations else 0


def path_citation(s: str):
    if "://" in s or s.lower().startswith("http"):
        return None
    if s.startswith("/"):
        return None
    if re.match(r"^(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD) ", s):
        return None
    if re.match(r"^[A-Z_]+(?:/[A-Z_]+)+$", s):
        return None
    if "<" in s or ">" in s or "*" in s:
        return None
    if "/" not in s or s.endswith("/"):
        return None
    m = LINE_SPEC_RE.search(s)
    if m:
        path = s[: m.start()]
        specs = m.group(1).split(",")
    else:
        path = s
        specs = None
    if "." not in Path(path).name:
        return None
    return path, specs



def check_cites(files, baseline, update=False, grow=False):
    violations = []
    for f in files:
        text = Path(f).read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        for m in BACKTICK.finditer(text):
            cit = path_citation(m.group(1))
            if not cit:
                continue
            path, specs = cit
            full = REPO_ROOT / path
            if not full.exists():
                violations.append((violation_key("cites", f, "missing-path", f"{path}|missing"), f"backticked path does not exist: {path} (referenced in {f})"))
                continue
            if specs and full.is_file():
                try:
                    fcount = len(full.read_text(encoding="utf-8", errors="replace").splitlines())
                except OSError:
                    fcount = 0
                for spec in specs:
                    if "-" in spec:
                        a, b = spec.split("-", 1)
                        a, b = int(a), int(b)
                        if a < 1 or b > fcount or a > b:
                            violations.append((violation_key("cites", f, "bad-line-range", f"{path}|{spec}"), f"line range out of bounds: {path}:{spec} (file has {fcount} lines, referenced in {f})"))
                    else:
                        n = int(spec)
                        if n < 1 or n > fcount:
                            violations.append((violation_key("cites", f, "bad-line-range", f"{path}|{spec}"), f"line out of bounds: {path}:{spec} (file has {fcount} lines, referenced in {f})"))
        for i, line in enumerate(lines, start=1):
            stripped = line.lstrip()
            if not BULLET_RE.match(stripped):
                continue
            backticked = BACKTICK.findall(stripped)
            has_cite = any(path_citation(b) for b in backticked)
            naked = BACKTICK.sub("", stripped)
            has_naked_path = bool(re.search(r"[\w\-]+\/[\w\-\.]+\.\w+", naked))
            if has_naked_path and not has_cite:
                violations.append((violation_key("cites", f, "bullet-no-cite", f"L{i}"), f"factual bullet carries no path citation: line {i} (in {f})"))
    if update:
        write_baseline(BASELINE_DIR / "cites.txt", [key for key, _ in violations], grow)
        print(f"cites: {len(violations)} violation(s) recorded in tools/baselines/cites.txt")


def check_headings(files, baseline, update=False, grow=False):
    violations = []
    for f in files:
        if not f.lower().endswith(".md"):
            continue
        text = Path(f).read_text(encoding="utf-8", errors="replace")
        seen = {}
        for m in re.finditer(r"^\s*#{1,6}\s+(.*?)\s*#*\s*$", text, re.M):
            heading = m.group(2).strip()
            norm = re.sub(r"\s+", " ", heading).strip().lower()
            if norm in seen:
                violations.append((violation_key("headings", f, "duplicate-heading", norm), f"duplicate heading: {heading} (in {f})"))
            else:
                seen[norm] = True
    if update:
        write_baseline(BASELINE_DIR / "headings.txt", [key for key, _ in violations], grow)
        print(f"headings: {len(violations)} violation(s) recorded in tools/baselines/headings.txt")


def check_index(baseline, update=False, grow=False):
    violations = []
    index_path = REPO_ROOT / "docs" / "INDEX.md"
    if not index_path.exists():
        print("docs/INDEX.md not found")
        return 1
    index_text = index_path.read_text(encoding="utf-8", errors="replace")
    indexed = set(BACKTICK.findall(index_text))
    # INDEX rows whose backticked path ends in "/" cover every file beneath them.
    dir_prefixes = {item for item in indexed if item.endswith("/")}
    not_indexed_section = False
    not_indexed = []
    for line in index_text.splitlines():
        if line.strip().startswith("## Not indexed"):
            not_indexed_section = True
            continue
        if not_indexed_section:
            m = re.search(r"`([^`]+)`", line)
            if m:
                not_indexed.append(m.group(1))
            elif line.strip().startswith("- ") and "`" not in line:
                not_indexed_section = False
    actual = set()
    for root_dir in ("docs", ".clinerules"):
        base = REPO_ROOT / root_dir
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if p.is_file():
                actual.add(p.relative_to(REPO_ROOT).as_posix())
    for rel in sorted(actual):
        # docs/INDEX.md is the index itself; it is not required to index itself.
        if rel == "docs/INDEX.md":
            continue
        if f"`{rel}`" in indexed or rel in indexed:
            continue
        name = Path(rel).name
        if f"`{name}`" in indexed:
            continue
        if any(rel.startswith(d) for d in dir_prefixes):
            continue
        if any(rel.startswith(ni.rstrip("*")) or ni.rstrip("*") in rel for ni in not_indexed):
            continue
        violations.append((violation_key("index", rel, "not-indexed", rel), f"file not indexed in docs/INDEX.md: {rel}"))
    if update:
        write_baseline(BASELINE_DIR / "index.txt", [key for key, _ in violations], grow)
        print(f"index: {len(violations)} violation(s) recorded in tools/baselines/index.txt")
        return 0
    return gate(violations, baseline, "index")


def main():
    ap = argparse.ArgumentParser(prog="check_docs.py")
    sub = ap.add_subparsers(dest="command", required=True)
    for cmd in ("cites", "headings", "index"):
        p = sub.add_parser(cmd)
        p.add_argument("paths", nargs="*", help="files or directories to check")
        p.add_argument("--baseline", default=None, help="baseline file path")
        p.add_argument("--update-baseline", action="store_true", help="write current violations to the baseline")
        p.add_argument("--allow-grow", action="store_true", help="allow growing the baseline (human only)")
    args = ap.parse_args()
    baseline_path = Path(args.baseline) if args.baseline else (BASELINE_DIR / f"{args.command}.txt")
    baseline = load_baseline(baseline_path)
    if args.command == "cites":
        files = args.paths or ["docs/ARCHITECTURE.md", "docs/REPO_MAP.md"]
        return check_cites(files, baseline, args.update_baseline, args.allow_grow)
    if args.command == "headings":
        files = args.paths or sorted(str(p) for p in (REPO_ROOT / "docs").rglob("*.md"))
        return check_headings(files, baseline, args.update_baseline, args.allow_grow)
    if args.command == "index":
        return check_index(baseline, args.update_baseline, args.allow_grow)
    return 1



if __name__ == "__main__":
    sys.exit(main())
