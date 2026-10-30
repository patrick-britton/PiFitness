"""Move and/or rename Python modules, rewriting imports.

Manifest format: {"old.module.path": "new.module.path", ...}

Cases supported per entry:
  - Move only        : pkg_a.foo -> pkg_b.foo   (same basename)
  - Rename in place  : pkg_a.foo -> pkg_a.bar   (same package, new basename)
  - Move + rename    : pkg_a.foo -> pkg_b.bar   (new package AND new basename)
  - No-op            : rejected

Implementation:
  - Move  : rope.refactor.move.MoveModule  (AST-precise import rewriting)
  - Rename: os.rename + regex rewrite of `old.module.path` -> `new.module.path`
            across all .py files (word-boundary match). Rename is NOT AST-based
            because rope has no public module-rename API. Adequate for the
            pattern `from X.Y import` / `import X.Y` / `X.Y.attr`.
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

from rope.base.project import Project
from rope.refactor.move import MoveModule


def _split(mod: str) -> tuple[str, str]:
    head, _, tail = mod.rpartition(".")
    return head, tail


def _dest_folder(project: Project, new_pkg: str):
    return project.get_folder(new_pkg) if new_pkg else project.root


def _walk_ignore(path: str) -> bool:
    """Return True if path should be skipped during regex rewrite."""
    parts = Path(path).parts
    return any(p in parts for p in (".venv", ".git", "__pycache__", "node_modules", ".pytest_cache", "build", "dist"))


def _rewrite_imports_regex(project_root: Path, old_mod: str, new_mod: str) -> tuple[int, str | None]:
    """Regex-rewrite all references to old_mod -> new_mod. Returns (files_changed, error)."""
    pattern = re.compile(rf"\b{re.escape(old_mod)}\b")
    changed = 0
    for root, dirs, files in os.walk(project_root):
        dirs[:] = [d for d in dirs if not _walk_ignore(os.path.join(root, d))]
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = Path(root) / fname
            try:
                text = fpath.read_text(encoding="utf-8")
            except Exception:
                continue
            new_text = pattern.sub(new_mod, text)
            if new_text != text:
                try:
                    fpath.write_text(new_text, encoding="utf-8", newline="")
                except Exception as e:
                    return changed, f"write {fpath}: {e}"
                changed += 1
    return changed, None


def _apply_entry(project: Project, project_root: Path, old_mod: str, new_mod: str) -> tuple[list[str], str | None]:
    old_pkg, old_base = _split(old_mod)
    new_pkg, new_base = _split(new_mod)

    if old_pkg == new_pkg and old_base == new_base:
        return [], "no-op: identical paths"

    steps: list[str] = []
    current = old_mod

    # Step 1: move (rope)
    if old_pkg != new_pkg:
        try:
            resource = project.get_module(current).get_resource()
            dest = _dest_folder(project, new_pkg)
            change = MoveModule(project, resource).get_changes(dest)
            project.do(change)
        except Exception as e:
            return steps, f"move: {e}"
        current = f"{new_pkg}.{old_base}" if new_pkg else old_base
        steps.append(f"moved {old_mod} -> {current}")

    # Step 2: rename (fs + regex)
    if old_base != new_base:
        try:
            resource = project.get_module(current).get_resource()
        except Exception as e:
            return steps, f"resolve-for-rename: {e}"

        cur_pkg, _ = _split(current)
        final_mod = f"{cur_pkg}.{new_base}" if cur_pkg else new_base
        new_rel_path = (resource.parent.path + "/" + new_base + ".py") if resource.parent.path else (new_base + ".py")
        old_full = project_root / resource.path
        new_full = project_root / new_rel_path

        try:
            os.rename(old_full, new_full)
        except Exception as e:
            return steps, f"fs-rename: {e}"

        changed, err = _rewrite_imports_regex(project_root, current, final_mod)
        if err:
            return steps, f"regex-rewrite: {err}"
        steps.append(f"renamed {current} -> {final_mod} ({changed} files rewritten)")

    return steps, None


def _plan_entry(project: Project, old_mod: str, new_mod: str) -> tuple[list[str], str | None]:
    old_pkg, old_base = _split(old_mod)
    new_pkg, new_base = _split(new_mod)

    if old_pkg == new_pkg and old_base == new_base:
        return [], "no-op: identical paths"

    try:
        resource = project.get_module(old_mod).get_resource()
        if not resource.exists():
            return [], f"source module not found: {old_mod}"
    except Exception as e:
        return [], f"resolve: {e}"

    if old_pkg != new_pkg:
        try:
            _dest_folder(project, new_pkg)
        except Exception as e:
            return [], f"dest folder: {e}"

    plan: list[str] = []
    if old_pkg != new_pkg:
        target = f"{new_pkg}/{old_base}.py" if new_pkg else f"{old_base}.py"
        plan.append(f"WOULD move {old_mod} -> {target}")
    if old_base != new_base:
        plan.append(f"WOULD rename {old_base}.py -> {new_base}.py (fs + regex imports)")
    return plan, None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("manifest", help="JSON file: {old_module: new_module}")
    p.add_argument("--dry-run", action="store_true", help="preview only; do not apply")
    p.add_argument("--apply", action="store_true", help="apply the moves/renames")
    p.add_argument("--project-root", default=".", help="project root (default: cwd)")
    args = p.parse_args()

    if args.dry_run == args.apply:
        print("ERROR: pass exactly one of --dry-run or --apply", file=sys.stderr)
        return 2

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        return 2
    try:
        moves = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as e:
        print(f"ERROR: manifest is not valid JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(moves, dict) or not moves:
        print("ERROR: manifest must be a non-empty JSON object", file=sys.stderr)
        return 2

    project_root = Path(args.project_root).resolve()
    project = Project(str(project_root))
    applied = 0
    failed: list[tuple[str, str, str]] = []

    for old_mod, new_mod in moves.items():
        if args.dry_run:
            plan, err = _plan_entry(project, old_mod, new_mod)
            if err:
                failed.append((old_mod, new_mod, err))
                continue
            for line in plan:
                print(line)
            applied += 1
        else:
            steps, err = _apply_entry(project, project_root, old_mod, new_mod)
            if err:
                failed.append((old_mod, new_mod, err))
                continue
            for s in steps:
                print(s)
            applied += 1

    project.close()

    print()
    print(f"Applied {applied} of {len(moves)} move(s).")
    if failed:
        print(f"FAILED {len(failed)}:")
        for old, new, err in failed:
            print(f"  {old} -> {new}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())