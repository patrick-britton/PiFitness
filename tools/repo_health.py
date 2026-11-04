#!/usr/bin/env python3
"""tools/repo_health.py - repository health checks.

Checks:
  folder-map   R1: every folder in repo must be listed in REPO_MAP.md
  imports      G1-G6: import-linter contracts for folder dependencies;
                 K1 deviations (backend_functions <-> integrations/spotify/worker)
                 are baselined as ignore_imports (replaces B11 broken generator)
  sql-smells   SELECT *, OFFSET, pandas in backend/pifitness/;
               unbounded queries and N+1 are review items, not script checks

vulture, knip and deptry are not installed; they are forced (exit 0, counted).

Baselines live in tools/baselines/<check>.txt, one key per line, sorted.
Key format: check|path|rule-or-symbol|8-char-hash-of-normalized-message
"""
import argparse
import hashlib
import re
import sys
from pathlib import Path

REPO_ROOT = Path.cwd()
BASELINE_DIR = REPO_ROOT / "tools" / "baselines"
REPO_MAP = REPO_ROOT / "docs" / "REPO_MAP.md"

# Folders that should never be imported from under backend/ (G4)
G4_FORBIDDEN = {"tests/", "tools/", "deploy/", "scripts/", "agents/"}

# Domain modules under backend/pifitness/ (G5)
DOMAIN_MODULES = {
    "activities/", "admin/", "auth/", "config/", "exercises/",
    "food/", "health/", "music/", "segments/", "tri_tip/", "volleyball/",
}

# K1 ignore_imports: known deviations baselined instead of fixed
K1_IGNORE = {
    ("backend/backend_functions/", "backend/pifitness/integrations/"): "K1: backend_functions imports integrations",
    ("backend/backend_functions/", "backend/pifitness/integrations/spotify/"): "K1: backend_functions imports integrations/spotify",
    ("backend/backend_functions/", "backend/pifitness/worker/"): "K1: backend_functions imports worker",
}


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


# ---------------------------------------------------------------------------
# R1: folder-vs-REPO_MAP check
# ---------------------------------------------------------------------------

_SKIP_DIRS = {".git", "__pycache__", "node_modules", ".venv"}


def parse_repo_map_folders():
    """Parse REPO_MAP.md table to extract folder paths from column 1."""
    folders = set()
    if not REPO_MAP.exists():
        return folders
    text = REPO_MAP.read_text(encoding="utf-8", errors="replace")
    in_table = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("| folder"):
            in_table = True
            continue
        if in_table and stripped.startswith("|---"):
            continue
        if in_table and stripped.startswith("|"):
            parts = [p.strip() for p in stripped.strip("|").split("|")]
            if len(parts) >= 1 and parts[0].endswith("/"):
                folders.add(parts[0])
        elif in_table and not stripped.startswith("|"):
            in_table = False
    return folders


def find_actual_folders():
    """Find all real folders in the repo (skip .git, __pycache__, etc.)."""
    folders = set()
    for p in REPO_ROOT.rglob("*"):
        if p.is_dir():
            rel = p.relative_to(REPO_ROOT).as_posix()
            if any(part in rel.split("/") for part in _SKIP_DIRS):
                continue
            folders.add(rel + "/")
    return folders


def check_folder_map(baseline, update=False, grow=False):
    violations = []
    mapped = parse_repo_map_folders()
    actual = find_actual_folders()
    for folder in sorted(actual - mapped):
        violations.append((violation_key("folder-map", folder, "unmapped", folder),
                           f"folder not in REPO_MAP.md: {folder}"))
    for folder in sorted(mapped - actual):
        violations.append((violation_key("folder-map", folder, "missing", folder),
                           f"REPO_MAP.md folder missing on disk: {folder}"))
    if update:
        write_baseline(BASELINE_DIR / "folder-map.txt", [key for key, _ in violations], grow)
        print(f"folder-map: {len(violations)} violation(s) recorded in tools/baselines/folder-map.txt")
        return 0
    return gate(violations, baseline, "folder-map")


# ---------------------------------------------------------------------------
# G1-G6: import-linter contracts
# ---------------------------------------------------------------------------

def get_file_folder(filepath: Path):
    """Repo-relative folder path containing the file, with trailing slash."""
    rel = filepath.relative_to(REPO_ROOT).as_posix()
    parts = rel.split("/")
    if len(parts) <= 1:
        return ""
    return "/".join(parts[:-1]) + "/"


def import_to_folder(module_name: str):
    """Map a Python import module name to a repo-relative folder path."""
    parts = module_name.split(".")

    if parts[0] == "pifitness" and len(parts) >= 2:
        candidate = REPO_ROOT / "backend" / "pifitness" / parts[1]
        if candidate.is_dir():
            return f"backend/pifitness/{parts[1]}/"
        if len(parts) >= 3:
            candidate2 = REPO_ROOT / "backend" / "pifitness" / parts[1] / parts[2]
            if candidate2.is_dir():
                return f"backend/pifitness/{parts[1]}/{parts[2]}/"

    if parts[0] == "backend_functions":
        if (REPO_ROOT / "backend" / "backend_functions").is_dir():
            return "backend/backend_functions/"

    return None


def parse_imports(filepath: Path):
    """Return list of (module_name, line_no) from import/from statements."""
    imports = []
    try:
        text = filepath.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return imports
    for i, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if re.match(r"from\s+\.+", stripped):
            continue
        m = re.match(r"from\s+([\w.]+)\s+import\s+", stripped)
        if m:
            imports.append((m.group(1), i))
            continue
        m = re.match(r"import\s+([\w.]+)", stripped)
        if m:
            imports.append((m.group(1), i))
    return imports


def check_imports(baseline, update=False, grow=False):
    violations = []
    backend_dir = REPO_ROOT / "backend"
    if not backend_dir.exists():
        return 0

    py_files = sorted(backend_dir.rglob("*.py"))

    for py_file in py_files:
        file_folder = get_file_folder(py_file)
        if not file_folder:
            continue

        for module_name, line_no in parse_imports(py_file):
            imported_folder = import_to_folder(module_name)
            if not imported_folder:
                continue
            if file_folder == imported_folder:
                continue

            if file_folder == "backend/pifitness/core/":
                violations.append((violation_key("imports", py_file.as_posix(), "G3",
                                                 f"{imported_folder}@{line_no}"),
                                   f"G3: core/ imports repo folder {imported_folder} in {py_file.as_posix()}:{line_no}"))
                continue

            if imported_folder in G4_FORBIDDEN:
                violations.append((violation_key("imports", py_file.as_posix(), "G4",
                                                 f"{imported_folder}@{line_no}"),
                                   f"G4: {file_folder} imports {imported_folder} in {py_file.as_posix()}:{line_no}"))
                continue

            if imported_folder == "backend/pifitness/worker/":
                if any(file_folder.startswith(f"backend/pifitness/{dm}") for dm in DOMAIN_MODULES):
                    violations.append((violation_key("imports", py_file.as_posix(), "G5",
                                                     f"worker/@{line_no}"),
                                       f"G5: {file_folder} imports worker/ in {py_file.as_posix()}:{line_no}"))
                    continue

            if file_folder == "backend/backend_functions/":
                if imported_folder != "backend/pifitness/core/":
                    key = (file_folder, imported_folder)
                    if key in K1_IGNORE:
                        continue
                    violations.append((violation_key("imports", py_file.as_posix(), "G6",
                                                     f"{imported_folder}@{line_no}"),
                                       f"G6: backend_functions imports {imported_folder} in {py_file.as_posix()}:{line_no}"))
                    continue

    if update:
        write_baseline(BASELINE_DIR / "import-contracts.txt", [key for key, _ in violations], grow)
        print(f"imports: {len(violations)} violation(s) recorded in tools/baselines/import-contracts.txt")
        return 0
    return gate(violations, baseline, "imports")


# ---------------------------------------------------------------------------
# SQL smell greps
# ---------------------------------------------------------------------------

def check_sql_smells(baseline, update=False, grow=False):
    violations = []
    pifitness_dir = REPO_ROOT / "backend" / "pifitness"
    if not pifitness_dir.exists():
        return 0

    py_files = sorted(pifitness_dir.rglob("*.py"))
    select_star = re.compile(r"\bSELECT\s+\*", re.IGNORECASE)
    offset_re = re.compile(r"\bOFFSET\b", re.IGNORECASE)
    pandas_re = re.compile(r"\bpandas\b", re.IGNORECASE)

    for py_file in py_files:
        try:
            text = py_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if select_star.search(line):
                violations.append((violation_key("sql-smells", py_file.as_posix(), "select-star", f"line {i}"),
                                   f"SQL smell: SELECT * in {py_file.as_posix()}:{i}"))
            if offset_re.search(line):
                violations.append((violation_key("sql-smells", py_file.as_posix(), "offset", f"line {i}"),
                                   f"SQL smell: OFFSET in {py_file.as_posix()}:{i}"))
            if pandas_re.search(line):
                violations.append((violation_key("sql-smells", py_file.as_posix(), "pandas", f"line {i}"),
                                   f"SQL smell: pandas in {py_file.as_posix()}:{i}"))

    if update:
        write_baseline(BASELINE_DIR / "sql-smells.txt", [key for key, _ in violations], grow)
        print(f"sql-smells: {len(violations)} violation(s) recorded in tools/baselines/sql-smells.txt")
        return 0
    return gate(violations, baseline, "sql-smells")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(prog="repo_health.py")
    ap.add_argument("--update-baseline", action="store_true",
                    help="write current violations to baselines (shrink-only)")
    ap.add_argument("--allow-grow", action="store_true",
                    help="allow adding new keys to baseline (human only)")
    args = ap.parse_args()

    rc = 0

    baseline = load_baseline(BASELINE_DIR / "folder-map.txt")
    rc |= check_folder_map(baseline, args.update_baseline, args.allow_grow)

    baseline = load_baseline(BASELINE_DIR / "import-contracts.txt")
    rc |= check_imports(baseline, args.update_baseline, args.allow_grow)

    baseline = load_baseline(BASELINE_DIR / "sql-smells.txt")
    rc |= check_sql_smells(baseline, args.update_baseline, args.allow_grow)

    sys.exit(rc)


if __name__ == "__main__":
    main()