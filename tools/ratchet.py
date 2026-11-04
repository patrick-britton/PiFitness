#!/usr/bin/env python3
"""tools/ratchet.py - ratchet baseline comparison tool.

Subcommands:
  compare <check> [key...]   Compare violation keys against baseline; exit 1 on new violations
  prune <check>              Remove stale keys from baseline (shrink-only update)

Options:
  --baseline PATH            Override baseline file path (default: tools/baselines/<check>.txt)
  --update-baseline          Rewrite baseline removing stale keys only
  --allow-grow               Allow adding new keys to baseline (human only)

Key format: check|path|rule-or-symbol|8-char-hash-of-normalized-message
Baselines live in tools/baselines/<check>.txt, one key per line, sorted.
Exit codes: 0 no new violations; 1 new violations found; stale keys print PRUNE but do not fail.
"""
import argparse
import sys
from pathlib import Path

REPO_ROOT = Path.cwd()
BASELINE_DIR = REPO_ROOT / "tools" / "baselines"


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


def compare(check: str, keys, baseline_path: Path, update: bool, grow: bool):
    baseline = load_baseline(baseline_path)
    key_set = set(keys)
    new_violations = sorted(key_set - baseline)
    stale = sorted(baseline - key_set)
    for key in new_violations:
        print(key)
    for key in stale:
        print(f"{check}: PRUNE {key} (no longer occurs)")
    if update:
        write_baseline(baseline_path, key_set, grow)
        print(f"{check}: {len(key_set)} key(s) recorded in {baseline_path}")
        return 0
    return 1 if new_violations else 0


def prune(check: str, keys, baseline_path: Path):
    baseline = load_baseline(baseline_path)
    if not baseline:
        print(f"{check}: baseline is empty, nothing to prune")
        return 0
    key_set = set(keys)
    stale = sorted(baseline - key_set)
    for key in stale:
        print(f"{check}: PRUNE {key} (no longer occurs)")
    write_baseline(baseline_path, key_set, grow=False)
    print(f"{check}: pruned {len(stale)} stale key(s)")
    return 0


def main():
    ap = argparse.ArgumentParser(prog="ratchet.py")
    sub = ap.add_subparsers(dest="command", required=True)

    p_compare = sub.add_parser("compare", help="compare violation keys against baseline")
    p_compare.add_argument("check", help="check name (baseline file: tools/baselines/<check>.txt)")
    p_compare.add_argument("keys", nargs="*", help="violation keys (or read from stdin if omitted)")
    p_compare.add_argument("--baseline", default=None, help="baseline file path override")
    p_compare.add_argument("--update-baseline", action="store_true", help="rewrite baseline, removing stale keys only")
    p_compare.add_argument("--allow-grow", action="store_true", help="allow adding new keys (human only)")

    p_prune = sub.add_parser("prune", help="remove stale keys from baseline")
    p_prune.add_argument("check", help="check name")
    p_prune.add_argument("keys", nargs="*", help="current keys (or read from stdin if omitted)")
    p_prune.add_argument("--baseline", default=None, help="baseline file path override")

    args = ap.parse_args()

    if args.command == "compare":
        baseline_path = Path(args.baseline) if args.baseline else (BASELINE_DIR / f"{args.check}.txt")
        if args.keys:
            keys = args.keys
        else:
            keys = [line.strip() for line in sys.stdin if line.strip()]
        return compare(args.check, keys, baseline_path, args.update_baseline, args.allow_grow)

    if args.command == "prune":
        baseline_path = Path(args.baseline) if args.baseline else (BASELINE_DIR / f"{args.check}.txt")
        if args.keys:
            keys = args.keys
        else:
            keys = [line.strip() for line in sys.stdin if line.strip()]
        return prune(args.check, keys, baseline_path)

    return 1


if __name__ == "__main__":
    sys.exit(main())