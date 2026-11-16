#!/usr/bin/env python3
"""tools/lint_comments.py: flag feature, bug and task IDs and change-history comments (V3).

Scans source files for comments and docstrings containing:
  - Feature IDs:    FR-<digits>     (e.g., FR-1, FR-15)
  - Bug IDs:        BUG-<digits>    (e.g., BUG-42) or Bug T<digits>-<digits> (e.g., Bug T08-3)
  - Task IDs:       T<digits>       (e.g., T01, T08) or <digits>.<digits>+[<letter>] (e.g., 1.5.6, 2.1, 1.5.5b)
  - Change-history: YYYY-MM-DD dates or "Changed by"/"Modified by"/"Updated by"

Baseline: tools/baselines/lint_comments.txt (ratchet format: check|path|rule|hash)
Exit codes: 0 no new violations; 1 new violations; stale baseline keys print PRUNE but do not fail.
--update-baseline rewrites the baseline removing stale keys only; --allow-grow is human-only.
"""
import argparse
import hashlib
import re
import string
import sys
from pathlib import Path

REPO_ROOT = Path.cwd()
BASELINE_DIR = REPO_ROOT / "tools" / "baselines"

FEATURE_ID_RE = re.compile(r"\bFR-\d+\b")
BUG_ID_RE = re.compile(r"\bBUG-\d+\b")
BUG_TASK_RE = re.compile(r"\bBug\s+T\d+-\d+\b", re.IGNORECASE)
TASK_ID_RE = re.compile(r"\bT\d+\b")
PLAN_TASK_ID_RE = re.compile(r"\b\d+(?:\.\d+){2,}[a-zA-Z]?\b")
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
CHANGE_HISTORY_RE = re.compile(r"\b(?:changed by|modified by|updated by|change log|changelog)\b", re.IGNORECASE)


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


def extract_comment_spans(text: str):
    """Yield (line_number, comment_text) for each # comment and docstring span."""
    lines = text.splitlines()
    in_docstring = False
    docstring_quote = None

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        if not in_docstring:
            dq_start = -1
            dq_quote = None
            for quote in ('"""', "'''"):
                idx = stripped.find(quote)
                if idx >= 0:
                    after = stripped[idx + len(quote):]
                    if quote not in after:
                        dq_start = idx
                        dq_quote = quote
                        break

            if dq_start >= 0:
                content = stripped[dq_start + len(dq_quote):]
                in_docstring = True
                docstring_quote = dq_quote
                yield (i, content)
                continue

            for quote in ('"""', "'''"):
                first = stripped.find(quote)
                if first >= 0:
                    second = stripped.find(quote, first + len(quote))
                    if second >= 0:
                        content = stripped[first + len(quote):second]
                        yield (i, content)
                        break
            else:
                comment_idx = stripped.find('#')
                if comment_idx >= 0:
                    comment_text = stripped[comment_idx + 1:].strip()
                    if comment_text:
                        yield (i, comment_text)
        else:
            idx = stripped.find(docstring_quote)
            if idx >= 0:
                content = stripped[:idx]
                yield (i, content)
                in_docstring = False
                docstring_quote = None
            else:
                yield (i, stripped)


_PUNCT_TABLE = str.maketrans("", "", string.punctuation + " ")


def _strip_markers(comment: str) -> str:
    """Remove every ID / change-history marker from a comment, then all punctuation."""
    cleaned = BUG_TASK_RE.sub("", comment)
    cleaned = FEATURE_ID_RE.sub("", cleaned)
    cleaned = BUG_ID_RE.sub("", cleaned)
    cleaned = TASK_ID_RE.sub("", cleaned)
    cleaned = PLAN_TASK_ID_RE.sub("", cleaned)
    cleaned = DATE_RE.sub("", cleaned)
    cleaned = CHANGE_HISTORY_RE.sub("", cleaned)
    return cleaned.translate(_PUNCT_TABLE)


def _has_marker(comment: str) -> bool:
    """True when the comment contains at least one ID / change-history marker."""
    return any(pat.search(comment)
               for pat in (BUG_TASK_RE, FEATURE_ID_RE, BUG_ID_RE, TASK_ID_RE,
                           PLAN_TASK_ID_RE, DATE_RE, CHANGE_HISTORY_RE))


def is_tier_a(comment: str) -> bool:
    """True when a comment consists only of IDs / change-history notes.

    Such a comment carries no "what and how" content and may be deleted
    outright (tier A). A comment that also holds substantive wording is
    tier B and must be rewritten by hand. Pure decorative dividers or
    plain comments with no marker are NOT tier A.
    """
    if not comment.strip():
        return False
    return _has_marker(comment) and _strip_markers(comment) == ""


def _line_offsets(text: str):
    lines = text.splitlines(keepends=True)
    offsets = []
    o = 0
    for ln in lines:
        offsets.append(o)
        o += len(ln)
    return lines, offsets


def comment_spans(text: str):
    """Yield (start, end, line_number, comment_text) for each comment span."""
    lines, offsets = _line_offsets(text)
    in_docstring = False
    docstring_quote = None
    docstring_start_line = None
    docstring_start_offset = None
    for i, line in enumerate(lines):
        line_no = i + 1
        line_start = offsets[i]
        stripped = line.strip()
        if in_docstring:
            idx = stripped.find(docstring_quote)
            if idx >= 0:
                end_offset = line_start + idx + len(docstring_quote)
                yield (docstring_start_offset, end_offset, docstring_start_line,
                       text[docstring_start_offset:end_offset])
                in_docstring = False
                docstring_quote = None
            continue
        dq_start = -1
        dq_quote = None
        for quote in ('"""', "'''"):
            idx = stripped.find(quote)
            if idx >= 0:
                after = stripped[idx + len(quote):]
                if quote not in after:
                    dq_start = idx
                    dq_quote = quote
                    break
        if dq_start >= 0:
            content = stripped[dq_start + len(dq_quote):]
            if dq_quote in content:
                close_idx = content.find(dq_quote)
                end_offset = line_start + dq_start + len(dq_quote) + close_idx + len(dq_quote)
                yield (line_start + dq_start, end_offset, line_no, content[:close_idx])
            else:
                in_docstring = True
                docstring_quote = dq_quote
                docstring_start_line = line_no
                docstring_start_offset = line_start + dq_start
            continue
        cidx = stripped.find("#")
        if cidx >= 0:
            yield (line_start + cidx, line_start + len(stripped), line_no,
                   stripped[cidx + 1:].strip())


def _line_start_offset(text: str, offset: int) -> int:
    return text.rfind("\n", 0, offset) + 1


def _line_end_offset(text: str, offset: int) -> int:
    nl = text.find("\n", offset)
    return nl if nl >= 0 else len(text)


def fix_tier_a(path: Path) -> int:
    """Delete tier-A comments (IDs / change-history only) from one file."""
    text = path.read_text(encoding="utf-8", errors="replace")
    spans = list(comment_spans(text))
    new_text = text
    removed = 0
    for start, end, _line_no, comment in reversed(spans):
        if not is_tier_a(comment):
            continue
        line_start = _line_start_offset(new_text, start)
        line_end = _line_end_offset(new_text, end)
        before = new_text[line_start:start]
        if before.strip() == "":
            # Full-line comment: drop the line and its terminator.
            cut_start = line_start
            cut_end = line_end
            if cut_end < len(new_text) and new_text[cut_end] == "\r":
                cut_end += 1
            if cut_end < len(new_text) and new_text[cut_end] == "\n":
                cut_end += 1
            new_text = new_text[:cut_start] + new_text[cut_end:]
        else:
            # Inline comment: drop from the '#' to end of line.
            new_text = new_text[:start] + new_text[line_end:]
        removed += 1
    if removed:
        path.write_text(new_text, encoding="utf-8")
    return removed


def scan_file(path: Path):
    """Scan a file for comment violations. Returns list of (key, detail) tuples."""
    violations = []
    abs_path = path.resolve()
    text = abs_path.read_text(encoding="utf-8", errors="replace")
    rel_path = abs_path.relative_to(REPO_ROOT.resolve()).as_posix()

    for line_no, comment in extract_comment_spans(text):
        claimed_spans = []

        # Bug ID: Bug T<digits>-<digits>
        for m in BUG_TASK_RE.finditer(comment):
            claimed_spans.append((m.start(), m.end()))
            detail = f"L{line_no}: bug ID {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "bug-id", detail)
            violations.append((key, detail))

        # Bug ID: BUG-<digits>
        for m in BUG_ID_RE.finditer(comment):
            if any(m.start() >= cs and m.end() <= ce for cs, ce in claimed_spans):
                continue
            claimed_spans.append((m.start(), m.end()))
            detail = f"L{line_no}: bug ID {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "bug-id", detail)
            violations.append((key, detail))

        # Feature ID: FR-<digits>
        for m in FEATURE_ID_RE.finditer(comment):
            detail = f"L{line_no}: feature ID {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "feature-id", detail)
            violations.append((key, detail))

        # Task ID: T<digits> (skip if inside claimed span)
        for m in TASK_ID_RE.finditer(comment):
            if any(m.start() >= cs and m.end() <= ce for cs, ce in claimed_spans):
                continue
            detail = f"L{line_no}: task ID {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "task-id", detail)
            violations.append((key, detail))

        # Plan task ID: 1.5.6, 2.1, 1.5.5b
        for m in PLAN_TASK_ID_RE.finditer(comment):
            detail = f"L{line_no}: task ID {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "task-id", detail)
            violations.append((key, detail))

        # Change-history: date
        for m in DATE_RE.finditer(comment):
            detail = f"L{line_no}: change-history date {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "change-history", detail)
            violations.append((key, detail))

        # Change-history: keywords
        for m in CHANGE_HISTORY_RE.finditer(comment):
            detail = f"L{line_no}: change-history note {m.group(0)}"
            key = violation_key("lint_comments", rel_path, "change-history", detail)
            violations.append((key, detail))

    return violations


def gate(violations, baseline, check_name):
    v_keys = {key for key, _ in violations}
    new_violations = [(key, detail) for key, detail in violations if key not in baseline]
    stale = baseline - v_keys
    for _key, detail in new_violations:
        print(detail)
    for key in sorted(stale):
        print(f"{check_name}: PRUNE {key} (no longer occurs)")
    return 1 if new_violations else 0


def collect_files(paths):
    """Collect source files from paths (files or directories)."""
    files = []
    for p in paths:
        path = Path(p)
        if path.is_dir():
            for ext in ("*.py", "*.ts", "*.tsx"):
                files.extend(sorted(path.rglob(ext)))
        else:
            files.append(path)
    return [f.resolve() for f in files if f.is_file()]


def main():
    ap = argparse.ArgumentParser(prog="lint_comments.py")
    ap.add_argument("paths", nargs="*", help="files or directories to scan")
    ap.add_argument("--baseline", default=None, help="baseline file path")
    ap.add_argument("--update-baseline", action="store_true", help="write current violations to the baseline")
    ap.add_argument("--allow-grow", action="store_true", help="allow growing the baseline (human only)")
    ap.add_argument("--fix-tier-a", action="store_true", help="delete comments that consist only of an ID or history note")
    args = ap.parse_args()

    baseline_path = Path(args.baseline) if args.baseline else (BASELINE_DIR / "lint_comments.txt")
    baseline = load_baseline(baseline_path)

    if args.paths:
        files = collect_files(args.paths)
    else:
        backend = REPO_ROOT / "backend"
        files = sorted(backend.rglob("*.py")) if backend.exists() else []

    all_violations = []
    for f in files:
        all_violations.extend(scan_file(f))

    if args.fix_tier_a:
        total = 0
        for f in files:
            total += fix_tier_a(f)
        print(f"lint_comments: removed {total} tier-A comment(s)")
        return 0

    if args.update_baseline:
        write_baseline(baseline_path, [key for key, _ in all_violations], args.allow_grow)
        print(f"lint_comments: {len(all_violations)} violation(s) recorded in {baseline_path}")
        return 0

    return gate(all_violations, baseline, "lint_comments")


if __name__ == "__main__":
    sys.exit(main())