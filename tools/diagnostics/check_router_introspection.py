#!/usr/bin/env python
"""Detect tests that bind a name to `pifitness.*.router` (module) and then
access an APIRouter-only attribute on that name.

These accesses worked before __init__ shadow removal (which rebound
pifitness.<d>.router to the APIRouter) and silently fail after. Two fixes
per occurrence:
  (a) safe, local:      X.attr        -> X.router.attr
  (b) cleaner, global:  change import so X is the APIRouter
                        (only valid if X has no other uses)

Usage:
  python tools/diagnostics/check_router_introspection.py            # report
  python tools/diagnostics/check_router_introspection.py --fix      # apply (a)
"""
from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path

# Attribute names that live on fastapi.routing.APIRouter, not on a module.
ROUTER_ATTRS = {
    "routes", "endpoint", "dependency_overrides", "include_router",
    "add_api_route", "add_api_websocket_route", "prefix", "tags",
    "default_response_class", "responses", "callbacks", "deprecated",
    "on_startup", "on_shutdown", "lifespan", "generate_unique_id_function",
    "default", "websocket_routes",
}


def bound_router_modules(tree: ast.AST) -> dict[str, str]:
    """name -> fully-qualified module path, for imports of pifitness.*.router."""
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("pifitness.") and a.name.endswith(".router"):
                    out[a.asname or a.name.rsplit(".", 1)[-1]] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            if not node.module.startswith("pifitness."):
                continue
            # from pifitness.<d> import router  -> binds submodule (post-shadow-removal)
            if not node.module.endswith(".router"):
                for a in node.names:
                    if a.name == "router":
                        out[a.asname or "router"] = node.module + ".router"
    return out


def find_bad_attr_accesses(tree: ast.AST, names: set[str]) -> list[tuple[int, str, str]]:
    """Return (lineno, name, attr) for X.attr where attr is APIRouter-only."""
    hits: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load)):
            continue
        if not isinstance(node.value, ast.Name) or node.value.id not in names:
            continue
        if node.attr in ROUTER_ATTRS:
            hits.append((node.lineno, node.value.id, node.attr))
    return hits


def all_name_uses(tree: ast.AST, name: str) -> list[int]:
    """All Load/Store uses of `name` — used to decide if form (b) is safe."""
    return [n.lineno for n in ast.walk(tree)
            if isinstance(n, ast.Name) and n.id == name]


def verify_import_paths(names: dict[str, str]) -> dict[str, bool]:
    """Confirm each module path actually imports and exposes `router`."""
    ok: dict[str, bool] = {}
    for name, modpath in names.items():
        try:
            mod = importlib.import_module(modpath)
            ok[name] = hasattr(mod, "router")
        except Exception as exc:  # noqa: BLE001
            print(f"  ! cannot import {modpath!r}: {type(exc).__name__}: {exc}")
            ok[name] = False
    return ok


def process(path: Path, apply: bool) -> tuple[int, list[str]]:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    names = bound_router_modules(tree)
    if not names:
        return 0, []

    verify_import_paths(names)
    bad = find_bad_attr_accesses(tree, set(names))
    if not bad:
        return 0, []

    issues: list[str] = []
    for lineno, name, attr in bad:
        uses = all_name_uses(tree, name)
        other_uses = [u for u in uses if u != lineno]
        suggestion_b = (
            f"change import so `{name}` is the APIRouter "
            f"(`from {names[name]} import router as {name}`) — only safe if "
            f"no other line uses `{name}` as a module"
            if len(other_uses) <= 1  # only import + this use
            else f"`{name}` has other uses (lines {other_uses}); use form (a)"
        )
        issues.append(
            f"{path}:{lineno}: `{name}.{attr}` — `{attr}` is APIRouter-only.\n"
            f"    (a) safe: `{name}.router.{attr}`\n"
            f"    (b) cleaner: {suggestion_b}"
        )

    if apply:
        lines = text.splitlines(keepends=True)
        # Replace from the bottom up so earlier lineno shifts don't break later ones
        for lineno, name, attr in sorted(bad, reverse=True):
            ln = lines[lineno - 1]
            # Guard: only replace the first occurrence per line (heuristic)
            old = f"{name}.{attr}"
            new = f"{name}.router.{attr}"
            if old in ln:
                lines[lineno - 1] = ln.replace(old, new, 1)
        path.write_text("".join(lines), encoding="utf-8")

    return len(bad), issues


def main(argv: list[str]) -> int:
    apply = "--fix" in argv
    roots = [Path("tests"), Path("backend"), Path("scripts")]
    total = 0
    for root in roots:
        if not root.exists():
            continue
        for py in sorted(root.rglob("*.py")):
            n, issues = process(py, apply)
            total += n
            for s in issues:
                print(s)
    print(f"\n{total} APIRouter-attribute access(es) on a module binding"
          f"{' (fixed)' if apply else ''}.")
    return 1 if total and not apply else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))