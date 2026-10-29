"""Hygiene: secrets exposure (values are NEVER written to reports), tracked junk, legacy-framework
residue (Streamlit), stale .pyc husks, and unignored heavy directories.
"""
from __future__ import annotations

import collections
import os
import re

from .core import Section, match_any

SECRET_PATTERNS = [
    ("private-key-block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")),
    ("aws-access-key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")),
    ("url-with-password", re.compile(r"\b[a-z][a-z0-9+.-]*://[^/\s:@]+:([^/\s@]{4,})@")),
    ("assigned-secret", re.compile(
        r"""(?i)\b(password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret)\b["']?\s*[:=]\s*["']([^"'\s]{8,})["']""")),
]
PLACEHOLDER = re.compile(r"(?i)(your[_-]|changeme|example|placeholder|xxxx|<.*>|\$\{|\{\{|os\.environ|getenv|process\.env|\*{3,}|dummy|test|secret_key_here)")
TEXT_SCAN_EXT = {".py", ".ts", ".tsx", ".js", ".mjs", ".json", ".env", ".yaml", ".yml", ".toml", ".ini", ".cfg",
                 ".sh", ".ps1", ".service", ".conf", ".txt", ".md", ".sql", ".cache"}
TOKEN_KEYS = re.compile(r'"(access_token|refresh_token|oauth_token|oauth_token_secret|token|secret|password)"\s*:')


def analyze(repo) -> Section:
    sec = Section("hygiene")
    h = repo.cfg["hygiene"]
    disk = repo.disk_files()

    # ---- sensitive files ----------------------------------------------------
    sens = []
    for rel in disk:
        if match_any(rel, h["sensitive_globs"]) and not match_any(rel, h["sensitive_allow"]):
            tracked = repo.is_tracked(rel)
            ignored = repo.is_ignored(rel) if tracked is False else False
            state = ("TRACKED" if tracked else "ignored" if ignored else "UNTRACKED-NOT-IGNORED") if tracked is not None else "unknown(no git)"
            sens.append({"path": rel, "bytes": repo.size(rel), "state": state})
    exposed = [s for s in sens if s["state"] in ("TRACKED", "UNTRACKED-NOT-IGNORED")]

    # history: were any sensitive paths ever committed? (names only)
    ever = []
    if repo.has_git and sens:
        specs = ["*.dump", "*token*.json", "*.env", ".env", ".spotify_cache", "*.pem", "*.key", "*credentials*.json"]
        out = repo.git("log", "--all", "--diff-filter=A", "--name-only", "--pretty=format:%h|%as", "--", *specs) or ""
        cur = ""
        for ln in out.splitlines():
            if "|" in ln and re.match(r"^[0-9a-f]{6,}\|", ln):
                cur = ln
            elif ln.strip() and not match_any(ln.strip(), h["sensitive_allow"]):
                ever.append(f"{ln.strip()}  (added in {cur})")

    # ---- content scan (redacted) ---------------------------------------------
    hits = []
    for rel in repo.files:
        if os.path.splitext(rel)[1].lower() not in TEXT_SCAN_EXT or repo.size(rel) > h["max_scan_bytes"]:
            continue
        if rel.endswith(("package-lock.json", "uv.lock")) or "/__tests__/" in rel or "/tests/" in rel or ".test." in rel:
            continue
        t = repo.text(rel) or ""
        for i, line in enumerate(t.splitlines(), 1):
            if len(line) > 600:
                continue
            for kind, rx in SECRET_PATTERNS:
                m = rx.search(line)
                if m and not PLACEHOLDER.search(line):
                    hits.append({"path": rel, "line": i, "kind": kind})
                    break
        if rel.endswith(".json") and TOKEN_KEYS.search(t) and re.search(r"token|secret|credential", rel, re.I):
            hits.append({"path": rel, "line": 1, "kind": "token-json-content"})
    # ---- junk tracked by git ---------------------------------------------------
    junk_pat = ["**/*.pyc", "**/__pycache__/**", "**/*.log", "**/*.out", "**/*.tsbuildinfo", "**/.next/**",
                "**/node_modules/**", "**/*.egg-info/**", "**/.pytest_cache/**"]
    tracked_junk = [t for t in (repo.tracked or ()) if match_any(t, junk_pat)]

    # ---- heavy dirs that git would not ignore -----------------------------------
    unignored_heavy = []
    if repo.has_git:
        for d in (".venv", "venv", "node_modules", ".next", "__pycache__", ".pytest_cache", "local_storage"):
            for hit in _find_dirs(repo, d):
                if repo.is_ignored(hit + "/x") is False:
                    unignored_heavy.append(hit)

    # ---- stale pyc husks ----------------------------------------------------------
    husks, husk_dirs = [], []
    pyc_dirs = collections.defaultdict(list)
    for rel in repo.disk_files():
        if "/__pycache__/" in rel and rel.endswith(".pyc"):
            pc_dir = rel.rsplit("/__pycache__/", 1)[0]
            name = rel.rsplit("/", 1)[-1].split(".")[0]
            pyc_dirs[pc_dir].append(name)
            if f"{pc_dir}/{name}.py" not in repo.fileset and f"{pc_dir}/{name}/__init__.py" not in repo.fileset:
                husks.append(f"{pc_dir}/{name}.py")
    for d, names in pyc_dirs.items():
        pys = [f for f in repo.fileset if f.startswith(d + "/") and f.endswith(".py") and f.count("/") == d.count("/") + 1]
        if not pys:
            husk_dirs.append(d)

    # ---- legacy framework residue (Streamlit by default) ------------------------------
    terms = [t.lower() for t in h["legacy_terms"]]
    term_rx = re.compile("|".join(re.escape(t) for t in terms), re.I) if terms else None
    code_refs, doc_refs = [], []
    if term_rx:
        for rel in repo.files:
            if os.path.splitext(rel)[1].lower() not in TEXT_SCAN_EXT or repo.size(rel) > 1_500_000 or rel.endswith("lock.json"):
                continue
            n = len(term_rx.findall(repo.text(rel) or ""))
            if n:
                (doc_refs if rel.endswith(".md") else code_refs).append({"path": rel, "refs": n})
    legacy_present = []
    for d in h["legacy_dirs"]:
        present_tracked = [f for f in repo.files if f.startswith(d + "/") or f == d]
        on_disk = [f for f in disk if f.startswith(d + "/")]
        if present_tracked or on_disk:
            legacy_present.append({"dir": d, "tracked_or_visible_files": len(present_tracked), "on_disk_files": len(on_disk),
                                   "only_pyc": bool(on_disk) and all(x.endswith(".pyc") for x in on_disk)})
    legacy_files = [f for f in repo.files if re.search(r"(^|/)(streamlit[_-][^/]*|[^/]*streamlit[^/]*)\.(py|md|json|toml)$", f, re.I)
                    or f.endswith(".streamlit/config.toml")]
    venv_installed = []
    for d in legacy_dists(repo, h["legacy_dist"]):
        venv_installed.append(d)
    imports_in_py = []
    for f in repo.by_ext(".py"):
        t = repo.text(f) or ""
        if re.search(r"^\s*(import|from)\s+(%s)\b" % "|".join(h["legacy_dist"]), t, re.M):
            imports_in_py.append(f)

    sec.data = {
        "sensitive_files": sens, "ever_committed": ever[:50], "secret_pattern_hits": hits[:200],
        "tracked_junk": sorted(tracked_junk)[:200], "unignored_heavy_dirs": sorted(set(unignored_heavy)),
        "stale_pyc_sources_missing": sorted(set(husks))[:200], "pyc_only_dirs": sorted(set(husk_dirs)),
        "legacy": {"terms": h["legacy_terms"], "python_imports": imports_in_py, "code_and_config_refs": code_refs,
                   "doc_refs": doc_refs, "leftover_dirs": legacy_present, "leftover_files": legacy_files,
                   "installed_in_venv": venv_installed},
    }

    if exposed:
        sec.add("HYG-001", "critical", "Sensitive files tracked by git or visible to `git add .`", count=len(exposed),
                evidence=[f"{s['state']}: {s['path']}" for s in exposed],
                hint="Remove from index (git rm --cached), add to .gitignore, rotate any credentials that were ever committed. Do not print their contents.")
    ignored_ok = [s for s in sens if s["state"] == "ignored"]
    if ignored_ok:
        sec.add("HYG-002", "info", "Sensitive-looking files present on disk but correctly git-ignored", count=len(ignored_ok),
                evidence=[s["path"] for s in ignored_ok], hint="Keep them out of the repo tree long-term (e.g. ~/.config/pifitness or a data dir).")
    if ever:
        sec.add("HYG-003", "high", "Sensitive-named files exist in git history (names only; contents not inspected)", count=len(ever), evidence=ever,
                hint="Treat as compromised: rotate tokens/passwords. History rewrite is a separate, human-approved decision.")
    if hits:
        sec.add("HYG-004", "high", "Possible secrets in source/config (values redacted)", count=len(hits),
                evidence=[f"{x['path']}:{x['line']} [{x['kind']}]" for x in hits])
    if tracked_junk:
        sec.add("HYG-005", "medium", "Build/cache artifacts tracked by git", count=len(tracked_junk), evidence=sorted(tracked_junk))
    if unignored_heavy:
        sec.add("HYG-006", "medium", "Heavy/generated directories that .gitignore does not cover", evidence=sorted(set(unignored_heavy)))
    if husks or husk_dirs:
        sec.add("HYG-007", "low", "Stale __pycache__ entries whose source .py no longer exists", count=len(set(husks)),
                evidence=[f"dir with only .pyc: {d}" for d in sorted(set(husk_dirs))] + sorted(set(husks)),
                hint="Deleted-module husks (e.g. viz_factory, frontend_functions). Safe to delete; they can also mask ImportErrors.")
    if imports_in_py or code_refs or legacy_present or legacy_files or venv_installed:
        sec.add("HYG-008", "medium" if imports_in_py else "low",
                f"Legacy framework residue ({', '.join(h['legacy_terms'])})",
                count=len(imports_in_py) + len(code_refs) + len(legacy_present) + len(legacy_files),
                evidence=[f"python import: {f}" for f in imports_in_py] +
                         [f"code/config ref: {r['path']} x{r['refs']}" for r in code_refs] +
                         [f"leftover dir: {d['dir']} ({'only .pyc' if d['only_pyc'] else str(d['tracked_or_visible_files']) + ' files'})" for d in legacy_present] +
                         [f"leftover file: {f}" for f in legacy_files] +
                         [f"installed in venv: {v}" for v in venv_installed],
                detail=f"{len(doc_refs)} markdown docs mention it (see data.legacy.doc_refs).")
    sec.metrics = {
        "hygiene.sensitive_exposed": len(exposed), "hygiene.sensitive_in_history": len(ever),
        "hygiene.secret_hits": len(hits), "hygiene.tracked_junk": len(tracked_junk),
        "hygiene.pyc_husks": len(set(husks)), "hygiene.legacy_py_imports": len(imports_in_py),
        "hygiene.legacy_code_refs": len(code_refs), "hygiene.legacy_leftover_dirs": len(legacy_present),
    }
    return sec


def _find_dirs(repo, name: str) -> list[str]:
    found = []
    for dp, dns, _ in os.walk(repo.root):
        rel = os.path.relpath(dp, repo.root).replace("\\", "/")
        if ".git" in dns:
            dns.remove(".git")
        if name in dns:
            found.append(f"{rel}/{name}" if rel != "." else name)
            dns.remove(name)
        # do not descend into heavy trees
        dns[:] = [d for d in dns if d not in ("node_modules", ".venv", "venv", ".next", ".features")]
    return found


def legacy_dists(repo, names) -> list[str]:
    out = []
    for venv in (".venv", "venv"):
        for sp in (repo.root / venv / "Lib" / "site-packages", *((repo.root / venv / "lib").glob("python*/site-packages"))):
            if sp.is_dir():
                for n in names:
                    for p in sp.glob(f"{n}-*.dist-info"):
                        out.append(f"{venv}: {p.name}")
    return out
