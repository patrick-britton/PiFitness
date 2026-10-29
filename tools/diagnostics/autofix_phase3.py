#!/usr/bin/env python
"""Autofix for Phase 3 reorg regressions (Python side).

Fixes, in one pass per file:
  1. __init__.py shadowing: `from <pkg>.<submodule> import <submodule>` is removed
     (this is what makes `import pifitness.music.router as m` bind to the APIRouter
      instead of the module, and silently neuters mock.patch).
     Also prunes any __all__ entries that referred only to shadowed names.
  2. Caller imports: `from <domain_pkg> import router`  ->  `from <domain_pkg>.router import router`
     Only fires when <domain_pkg>/router.py exists. Idempotent.
  3. pytest.ini: `pythonpath = backend/pifitness` -> `pythonpath = backend`.
  4. monkeypatch.setattr / mock.patch string targets:
       "backend_functions.<d>_service.<attr>"  ->  "pifitness.<d>.services.<attr>"
     Only fires when pifitness/<d>/services.py exists AND <attr> is defined at module level.
  5. Test file literals: 'pifitness/...py' that exist under backend/ are prefixed with 'backend/'.

Everything the tool cannot prove (unresolved patch targets, stale backend.api.* imports,
Path(...)/'pifitness'/'...' constructions, multiline shadow imports) is reported in
tools/diagnostics/NEEDS_ATTENTION.md.

Usage:
  python tools/diagnostics/autofix_phase3.py            # dry run (default), shows diffs
  python tools/diagnostics/autofix_phase3.py --apply    # writes files
"""
from __future__ import annotations

import argparse
import ast
import difflib
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "backend"
PIFITNESS = BACKEND / "pifitness"
TESTS = REPO / "tests"
SCAN_DIRS = [BACKEND, TESTS, REPO / "scripts", REPO / "tools"]
SKIP_PARTS = {"__pycache__", ".venv", "node_modules", ".git", ".audit"}


# ---------- helpers ----------

def is_submodule(pkg_dir: Path, name: str) -> bool:
    if not pkg_dir.is_dir():
        return False
    return (pkg_dir / name / "__init__.py").exists() or (pkg_dir / f"{name}.py").exists()


def module_has_attr(module_path: Path, attr: str) -> bool:
    if not module_path.exists():
        return False
    try:
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
    except SyntaxError:
        return False
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name == attr:
                return True
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == attr:
                    return True
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bn = alias.asname or alias.name.split(".")[-1]
                if bn == attr:
                    return True
    return False


def _safe_parse(text: str) -> ast.AST | None:
    try:
        return ast.parse(text)
    except SyntaxError:
        return None


# ---------- Fix 1: __init__.py shadowing ----------

def fix_init_shadowing(text: str, path: Path) -> tuple[str, list[str], list[str]]:
    """Return (new_text, changes, attention)."""
    if path.name != "__init__.py":
        return text, [], []
    tree = _safe_parse(text)
    if not tree:
        return text, [], []

    pkg_dir = path.parent
    shadowed: set[str] = set()
    removable_lines: set[int] = set()
    attention: list[str] = []

    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        names_shadow = []
        for alias in node.names:
            bn = alias.asname or alias.name
            if is_submodule(pkg_dir, bn):
                names_shadow.append(bn)
        if not names_shadow:
            continue
        multiline = node.lineno != node.end_lineno
        whole = len(names_shadow) == len(node.names)
        if not multiline and whole:
            removable_lines.add(node.lineno)
            shadowed.update(names_shadow)
        else:
            for n in names_shadow:
                attention.append(f"{path}:{node.lineno}: shadow of {n!r} (multiline or mixed import) — fix manually")
                shadowed.add(n)  # still prune __all__

    if not shadowed:
        return text, [], attention

    # Prune __all__ (single-line only)
    lines = text.splitlines(keepends=True)
    all_lines_to_remove: set[int] = set()
    for node in tree.body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1):
            continue
        tgt = node.targets[0]
        if not (isinstance(tgt, ast.Name) and tgt.id == "__all__"):
            continue
        if not isinstance(node.value, (ast.List, ast.Tuple)):
            continue
        if node.lineno != node.end_lineno:
            attention.append(f"{path}:{node.lineno}: multiline __all__ — prune manually if it lists shadowed names")
            continue
        kept = [e.value for e in node.value.elts
                if isinstance(e, ast.Constant) and isinstance(e.value, str)
                and e.value not in shadowed]
        if len(kept) == len(node.value.elts):
            continue
        if not kept:
            all_lines_to_remove.add(node.lineno)
        else:
            lines[node.lineno - 1] = "__all__ = [" + ", ".join(f'"{k}"' for k in kept) + "]\n"
    text = "".join(lines)

    # Remove shadow-import lines (descending order preserves line numbers above)
    lines = text.splitlines(keepends=True)
    text = "".join(ln for i, ln in enumerate(lines, 1) if i not in removable_lines and i not in all_lines_to_remove)
    # Normalize trailing blank lines
    while text.endswith("\n\n"):
        text = text[:-1]
    if not text.endswith("\n"):
        text += "\n"

    changes = [f"drop shadow import(s): {', '.join(sorted(shadowed))}"]
    if all_lines_to_remove:
        changes.append("prune __all__")
    return text, changes, attention



# ---------- Fix 4: monkeypatch / patch targets ----------

SUFFIXES = ("_service", "_services", "_functions", "_helpers", "_queries")


def fix_patch_targets(text: str, path: Path) -> tuple[str, list[str], list[str]]:
    if path.suffix != ".py":
        return text, [], []
    tree = _safe_parse(text)
    if not tree:
        return text, [], []

    changes: list[str] = []
    attention: list[str] = []
    lines = text.splitlines(keepends=True)

    def rewrite(target: str) -> tuple[str | None, str | None]:
        if not target.startswith("backend_functions."):
            return None, None
        rest = target[len("backend_functions."):]
        if "." not in rest:
            return None, None
        mod_part, attr = rest.rsplit(".", 1)
        for suf in SUFFIXES:
            if not mod_part.endswith(suf):
                continue
            domain = mod_part[:-len(suf)]
            new_mod = "services" if suf in ("_service", "_services") else suf.lstrip("_")
            candidate = PIFITNESS / domain / f"{new_mod}.py"
            if candidate.exists() and module_has_attr(candidate, attr):
                return f"pifitness.{domain}.{new_mod}.{attr}", None
            if not candidate.exists():
                return None, f"{path}:{target}: pifitness/{domain}/{new_mod}.py not found"
            return None, f"{path}:{target}: {attr!r} not defined in pifitness/{domain}/{new_mod}.py"
        return None, f"{path}:{target}: backend_functions module {mod_part!r} has no known replacement"

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fname = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
        if fname not in ("patch", "setattr"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        if not isinstance(node.args[0].value, str):
            continue
        old_t = node.args[0].value
        new_t, note = rewrite(old_t)
        if note:
            attention.append(note)
            continue
        if not new_t:
            continue
        if node.lineno != node.end_lineno:
            attention.append(f"{path}:{node.lineno}: multiline call — rewrite {old_t!r} -> {new_t!r} manually")
            continue
        ln = lines[node.lineno - 1]
        upd = ln.replace(f'"{old_t}"', f'"{new_t}"', 1).replace(f"'{old_t}'", f"'{new_t}'", 1)
        if upd != ln:
            lines[node.lineno - 1] = upd
            changes.append(f"L{node.lineno}: {old_t} -> {new_t}")

    return "".join(lines), changes, attention


# ---------- Fix 5: test .py path literals ----------

def fix_test_path_literals(text: str, path: Path) -> tuple[str, list[str]]:
    if not path.name.startswith("test_") or path.suffix != ".py":
        return text, []
    tree = _safe_parse(text)
    if not tree:
        return text, []

    changes: list[str] = []
    lines = text.splitlines(keepends=True)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        s = node.value
        if not s.endswith(".py") or ("/" not in s and "\\" not in s):
            continue
        if (REPO / s).exists():
            continue
        alt = "backend/" + s.replace("\\", "/")
        if not (REPO / alt).exists():
            continue
        if node.lineno != node.end_lineno:
            continue
        ln = lines[node.lineno - 1]
        upd = ln.replace(f'"{s}"', f'"{alt}"', 1).replace(f"'{s}'", f"'{alt}'", 1)
        if upd != ln:
            lines[node.lineno - 1] = upd
            changes.append(f"L{node.lineno}: {s!r} -> {alt!r}")

    return "".join(lines), changes


# ---------- pytest.ini ----------

def fix_pytest_ini(text: str) -> tuple[str, list[str]]:
    new = re.sub(r"(?m)^pythonpath\s*=\s*backend/pifitness\s*$", "pythonpath = backend", text)
    if new != text:
        return new, ["pythonpath: backend/pifitness -> backend"]
    return text, []


# ---------- attention reporter ----------

def report_attention() -> list[str]:
    items: list[str] = []

    # Unresolved / shadowed patch targets in tests
    for py in sorted(TESTS.rglob("test_*.py")):
        tree = _safe_parse(py.read_text(encoding="utf-8"))
        if not tree:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fname = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if fname not in ("patch", "setattr"):
                continue
            if not node.args or not isinstance(node.args[0], ast.Constant):
                continue
            t = node.args[0].value
            if not isinstance(t, str) or "." not in t:
                continue
            modpath, attr = t.rsplit(".", 1)
            try:
                mod = __import__(modpath, fromlist=["__dummy__"])
            except Exception as exc:
                items.append(f"{py}:{node.lineno}: cannot import {modpath!r}: {type(exc).__name__}: {exc}")
                continue
            if "." in modpath:
                ppath, child = modpath.rsplit(".", 1)
                try:
                    parent = __import__(ppath, fromlist=[child])
                    via_parent = getattr(parent, child, None)
                except Exception:
                    via_parent = None
                if via_parent is not None and via_parent is not mod:
                    items.append(
                        f"{py}:{node.lineno}: SHADOWED: getattr({ppath!r}, {child!r}) = "
                        f"{type(via_parent).__name__}, not the module. patch({t!r}) hits wrong object."
                    )
                    continue
            if not hasattr(mod, attr):
                items.append(f"{py}:{node.lineno}: {modpath!r} has no attribute {attr!r}")

    # Stale imports
    STALE = ("backend.api", "backend_functions", "agents", "frontend_functions")
    for py in sorted(TESTS.rglob("test_*.py")):
        tree = _safe_parse(py.read_text(encoding="utf-8"))
        if not tree:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if any(node.module == s or node.module.startswith(s + ".") for s in STALE):
                    items.append(f"{py}:{node.lineno}: stale import-from {node.module!r}")
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if any(a.name == s or a.name.startswith(s + ".") for s in STALE):
                        items.append(f"{py}:{node.lineno}: stale import {a.name!r}")

    # Path(...) / 'a' / 'b.py' constructions whose joined form is missing but exists under backend/
    for py in sorted(TESTS.rglob("test_*.py")):
        tree = _safe_parse(py.read_text(encoding="utf-8"))
        if not tree:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)):
                continue
            segs = []
            cur: ast.AST = node
            while isinstance(cur, ast.BinOp) and isinstance(cur.op, ast.Div):
                segs.append(cur.right)
                cur = cur.left
            if not all(isinstance(s, ast.Constant) and isinstance(s.value, str) for s in segs):
                continue
            joined = "/".join(reversed([s.value for s in segs]))  # type: ignore[union-attr]
            if not joined.endswith(".py"):
                continue
            if (REPO / joined).exists():
                continue
            if (REPO / "backend" / joined).exists():
                items.append(
                    f"{py}:{node.lineno}: path chain joins to {joined!r} (missing) but backend/{joined} exists "
                    f"— prepend 'backend' or use Path(module.__file__)"
                )

    return items


# ---------- driver ----------

def iter_py_files():
    seen: set[Path] = set()
    for root in SCAN_DIRS:
        if not root.exists():
            continue
        for p in root.rglob("*.py"):
            if any(part in SKIP_PARTS for part in p.parts):
                continue
            if p in seen:
                continue
            seen.add(p)
            yield p


def process_file(path: Path) -> tuple[str, str, list[str], list[str]]:
    original = path.read_text(encoding="utf-8")
    text = original
    changes: list[str] = []
    attention: list[str] = []

    text, ch, at = fix_init_shadowing(text, path)
    changes += ch
    attention += at


    text, ch, at = fix_patch_targets(text, path)
    changes += ch
    attention += at

    text, ch = fix_test_path_literals(text, path)
    changes += ch

    return original, text, changes, attention


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run + diff)")
    args = ap.parse_args(argv)

    total_changes = 0
    touched_files = 0

    for py in sorted(iter_py_files()):
        old, new, changes, attention = process_file(py)
        if changes:
            touched_files += 1
            total_changes += len(changes)
            print(f"\n{'WRITE' if args.apply else 'DRY '} {py.relative_to(REPO)}")
            for c in changes:
                print(f"  - {c}")
            if not args.apply:
                for line in difflib.unified_diff(
                    old.splitlines(keepends=True), new.splitlines(keepends=True),
                    fromfile=str(py), tofile=str(py), lineterm="",
                ):
                    print(f"    {line.rstrip()}")
            else:
                py.write_text(new, encoding="utf-8")
        for a in attention:
            print(f"\n[ATTENTION] {a}")

    # pytest.ini
    ini = REPO / "pytest.ini"
    if ini.exists():
        old = ini.read_text(encoding="utf-8")
        new, changes = fix_pytest_ini(old)
        if changes:
            touched_files += 1
            total_changes += len(changes)
            print(f"\n{'WRITE' if args.apply else 'DRY '} pytest.ini")
            for c in changes:
                print(f"  - {c}")
            if args.apply:
                ini.write_text(new, encoding="utf-8")

    # Attention report
    report = report_attention()
    if report:
        report_path = REPO / "tools" / "diagnostics" / "NEEDS_ATTENTION.md"
        body = "# Needs attention (autofix could not prove these safe)\n\n"
        body += "\n".join(f"- {r}" for r in report) + "\n"
        if args.apply:
            report_path.write_text(body, encoding="utf-8")
            print(f"\nWrote {report_path.relative_to(REPO)} ({len(report)} item(s))")
        else:
            print(f"\n[{len(report)} additional item(s) — will be written to NEEDS_ATTENTION.md on --apply]")

    print(f"\nSummary: {total_changes} change(s) across {touched_files} file(s) "
          f"{'applied' if args.apply else 'proposed (dry-run)'}.")
    if not args.apply and total_changes:
        print("Review the diff above, then re-run with --apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())