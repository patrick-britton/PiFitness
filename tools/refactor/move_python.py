"""Move and/or rename Python modules, rewriting imports via libcst.

Manifest: {"old.module.path": "new.module.path", ...}

Package roots are mapped explicitly via --pkg-map so the tool can compute
physical file paths without relying on Python's import system.

Implementation:
  - File move     : os.rename + ensure __init__.py in dest folder
  - Import rewrite: libcst AST transformer (Python 3.14+ compatible)
    Only rewrites actual Import/ImportFrom statements. Strings and comments
    containing the module path are left byte-identical.

Handles:
  - from old_module import X       -> from new_module import X
  - from old_module.sub import X   -> from new_module.sub import X
  - import old_module              -> import new_module
  - import old_module.sub          -> import new_module.sub
  - import old_module as Y         -> import new_module as Y

Does NOT handle (fail loudly at test time if encountered):
  - from parent_pkg import child_module (importing a module from its parent)
  - Attribute access like module.X.Y() (fully-qualified names in call sites)
  - Dynamic imports (importlib.import_module("..."))

If a move produces ModuleNotFoundError in tests and the offending line looks
like one of these, that pattern needs to be added here or handled manually.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import libcst as cst

SKIP_DIRS = {".venv", ".git", "__pycache__", "node_modules",
             ".pytest_cache", "build", "dist", ".next"}


def parse_pkg_map(s: str) -> dict:
    mapping = {}
    for pair in s.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            raise ValueError(f"bad --pkg-map entry: {pair!r}")
        k, v = pair.split("=", 1)
        mapping[k.strip()] = v.strip()
    return mapping


def resolve_physical(mod: str, pkg_map: dict, project_root: Path) -> Path:
    for prefix in sorted(pkg_map.keys(), key=len, reverse=True):
        if mod == prefix:
            return project_root / pkg_map[prefix] / "__init__.py"
        if mod.startswith(prefix + "."):
            rest = mod[len(prefix) + 1:].replace(".", os.sep)
            return project_root / pkg_map[prefix] / (rest + ".py")
    raise ValueError(f"module {mod!r} does not match any --pkg-map prefix")


def ensure_package(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    init_py = folder / "__init__.py"
    if not init_py.exists():
        init_py.write_text("", encoding="utf-8", newline="")


class _ImportRewriter(cst.CSTTransformer):
    def __init__(self, old: str, new: str):
        self.old = old
        self.new = new

    @staticmethod
    def _get_dotted(node):
        if node is None:
            return None
        if isinstance(node, cst.Name):
            return node.value
        if isinstance(node, cst.Attribute):
            base = _ImportRewriter._get_dotted(node.value)
            if base is None:
                return None
            return f"{base}.{node.attr.value}"
        return None

    @staticmethod
    def _make_dotted(dotted: str):
        parts = dotted.split(".")
        node = cst.Name(parts[0])
        for part in parts[1:]:
            node = cst.Attribute(value=node, attr=cst.Name(part))
        return node

    def _rewrite_ref(self, dotted: str):
        """Return rewritten dotted path if matches self.old prefix, else None."""
        if dotted == self.old:
            return self.new
        if dotted.startswith(self.old + "."):
            return self.new + dotted[len(self.old):]
        return None

    def leave_ImportFrom(self, original_node, updated_node):
        if updated_node.module is None:
            return updated_node  # relative import
        mod = self._get_dotted(updated_node.module)
        if mod is None:
            return updated_node
        new_mod = self._rewrite_ref(mod)
        if new_mod is None:
            return updated_node
        return updated_node.with_changes(module=self._make_dotted(new_mod))

    def leave_Import(self, original_node, updated_node):
        new_names = []
        changed = False
        for name in updated_node.names:
            dotted = self._get_dotted(name.name)
            if dotted is None:
                new_names.append(name)
                continue
            new_dotted = self._rewrite_ref(dotted)
            if new_dotted is None:
                new_names.append(name)
                continue
            new_names.append(name.with_changes(name=self._make_dotted(new_dotted)))
            changed = True
        if changed:
            return updated_node.with_changes(names=tuple(new_names))
        return updated_node


def rewrite_imports_libcst(project_root: Path, old_mod: str, new_mod: str) -> int:
    changed = 0
    for root, dirs, files in os.walk(project_root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fp = Path(root) / fname
            try:
                text = fp.read_text(encoding="utf-8")
            except Exception:
                continue
            try:
                tree = cst.parse_module(text)
            except Exception:
                continue
            new_tree = tree.visit(_ImportRewriter(old_mod, new_mod))
            new_text = new_tree.code
            if new_text != text:
                fp.write_text(new_text, encoding="utf-8", newline="")
                changed += 1
    return changed


def plan_entry(project_root: Path, pkg_map: dict, old_mod: str, new_mod: str):
    try:
        src = resolve_physical(old_mod, pkg_map, project_root)
        dst = resolve_physical(new_mod, pkg_map, project_root)
    except ValueError as e:
        return [], str(e)
    if not src.exists():
        return [], f"source not found: {src.relative_to(project_root)}"
    if src == dst:
        return [], "no-op: identical paths"
    return [f"WOULD move {src.relative_to(project_root)} -> {dst.relative_to(project_root)}"], None


def apply_entry(project_root: Path, pkg_map: dict, old_mod: str, new_mod: str):
    try:
        src = resolve_physical(old_mod, pkg_map, project_root)
        dst = resolve_physical(new_mod, pkg_map, project_root)
    except ValueError as e:
        return [], str(e)
    if not src.exists():
        return [], f"source not found: {src.relative_to(project_root)}"
    if src == dst:
        return [], "no-op: identical paths"

    try:
        ensure_package(dst.parent)
        os.rename(src, dst)
    except Exception as e:
        return [], f"rename: {e}"

    steps = [f"moved {src.relative_to(project_root)} -> {dst.relative_to(project_root)}"]
    if old_mod != new_mod:
        n = rewrite_imports_libcst(project_root, old_mod, new_mod)
        steps.append(f"rewrote imports in {n} file(s)")
    return steps, None


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("manifest", help="JSON file: {old_module: new_module}")
    p.add_argument("--pkg-map", required=True,
                   help='comma-separated prefix=physical, e.g. "backend_functions=backend/backend_functions,pifitness=backend/pifitness"')
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--project-root", default=".")
    args = p.parse_args()

    if args.dry_run == args.apply:
        print("ERROR: pass exactly one of --dry-run or --apply", file=sys.stderr)
        return 2

    project_root = Path(args.project_root).resolve()
    try:
        pkg_map = parse_pkg_map(args.pkg_map)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    if not pkg_map:
        print("ERROR: --pkg-map is empty", file=sys.stderr)
        return 2

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        return 2
    try:
        moves = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as e:
        print(f"ERROR: manifest not valid JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(moves, dict) or not moves:
        print("ERROR: manifest must be a non-empty JSON object", file=sys.stderr)
        return 2

    applied = 0
    failed = []

    for old_mod, new_mod in moves.items():
        if args.dry_run:
            plan, err = plan_entry(project_root, pkg_map, old_mod, new_mod)
            if err:
                failed.append((old_mod, new_mod, err))
                continue
            for line in plan:
                print(line)
            applied += 1
        else:
            steps, err = apply_entry(project_root, pkg_map, old_mod, new_mod)
            if err:
                failed.append((old_mod, new_mod, err))
                continue
            for s in steps:
                print(s)
            applied += 1

    print()
    print(f"Applied {applied} of {len(moves)} move(s).")
    if failed:
        print(f"FAILED {len(failed)}:")
        for o, n, e in failed:
            print(f"  {o} -> {n}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())