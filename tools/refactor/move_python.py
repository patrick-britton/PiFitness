"""Move Python modules and rewrite imports via rope.

Manifest format: {"old.module.path": "new.module.path", ...}

Moves a module into the destination package folder. The module's basename
must be unchanged (health.py -> pkg/health.py is OK; health.py -> pkg/router.py
is NOT — use a separate rename task for that).
"""
import argparse
import json
import sys
from pathlib import Path

from rope.base.project import Project
from rope.refactor.move import MoveModule


def _dest_folder(project: Project, new_mod: str):
    parts = new_mod.split(".")
    folder_path = ".".join(parts[:-1])  # "" for top-level module
    if not folder_path:
        return project.root
    return project.get_folder(folder_path)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("manifest", help="JSON file: {old_module: new_module}")
    p.add_argument("--dry-run", action="store_true", help="print intended rewrites, do not apply")
    p.add_argument("--apply", action="store_true", help="apply the moves")
    p.add_argument("--project-root", default=".", help="project root (default: cwd)")
    args = p.parse_args()

    if args.dry_run == args.apply:  # both true or both false
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

    project = Project(args.project_root)
    applied = 0
    failed: list[tuple[str, str, str]] = []

    for old_mod, new_mod in moves.items():
        old_base = old_mod.rsplit(".", 1)[-1]
        new_base = new_mod.rsplit(".", 1)[-1]
        if old_base != new_base:
            failed.append((old_mod, new_mod,
                           f"basename change not supported by MoveModule "
                           f"({old_base} -> {new_base}); do move then rename separately"))
            continue

        try:
            source_resource = project.get_module(old_mod).get_resource()
        except Exception as e:
            failed.append((old_mod, new_mod, f"resolve: {e}"))
            continue

        try:
            dest_folder = _dest_folder(project, new_mod)
        except Exception as e:
            failed.append((old_mod, new_mod, f"dest folder: {e}"))
            continue

        try:
            change = MoveModule(project, source_resource).get_changes(dest_folder)
        except Exception as e:
            failed.append((old_mod, new_mod, f"get_changes: {e}"))
            continue

        if args.dry_run:
            for c in change.changes:
                res = getattr(c, "resource", None)
                print(f"WOULD {type(c).__name__:20s} {res.path if res else '<no resource>'}")
            applied += 1
        else:
            try:
                project.do(change)
                print(f"MOVED {old_mod} -> {new_mod}")
                applied += 1
            except Exception as e:
                failed.append((old_mod, new_mod, f"apply: {e}"))

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