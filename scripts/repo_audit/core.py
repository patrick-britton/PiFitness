"""Shared infrastructure: config, repo discovery, git access, classification, findings.

Everything here is stdlib-only so the audit runs in any venv (including a broken one).
"""
from __future__ import annotations

import copy
import fnmatch
import functools
import os
import re
import subprocess
from dataclasses import dataclass, field, asdict
from pathlib import Path

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None

SEV_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

DEFAULTS: dict = {
    # Directories never analysed (matched against every path segment).
    "exclude_dirs": [
        ".git", ".venv", "venv", "env", "node_modules", ".next", "__pycache__",
        ".pytest_cache", ".mypy_cache", ".ruff_cache", ".turbo", "dist", "build",
        "coverage", ".cache", "*.egg-info", ".idea", ".audit",
    ],
    # Runtime-data directories: summarised (count/bytes) but never parsed.
    "data_dirs": ["local_storage"],
    "out_dir": ".audit",
    # domain name -> keywords matched against file/dir names (deepest segment wins)
    "domains": {},
    # domains that everyone may depend on (not counted as cross-domain coupling)
    "shared_domains": ["core"],
    # layer rules: {name, from:[globs], to:[globs], to_external:[pkgs], reason, severity, enabled}
    "forbid": [],
    "inventory": {
        "clutter_globs": [
            "**/*.log", "**/*.out", "**/*.bak", "**/*_bak*", "**/* copy*", "**/*copy.*",
            "**/tmp_*", "**/_tmp*", "**/*.tmp", "**/_task*.txt", "**/_*.out",
            "**/build*.txt", "**/tsc*.txt", "**/vit*.txt", "**/*.tsbuildinfo",
            "**/.DS_Store", "**/Thumbs.db",
        ],
        "data_hints": ["responses", "fixtures", "samples", "snapshots"],
        "generated_hints": [
            "*_report.json", "*_report.md", "*_documentation.json", "*results*.json",
            "data_flow_diagrams.*", "cloc.*", "*-report.*", "*_summary.md",
        ],
        "big_file_bytes": 400_000,
        "loose_root_files": 12,
    },
    "python": {
        "roots": [],  # auto-detected when empty
        "entrypoint_globs": ["**/main.py", "**/__main__.py", "setup.py", "**/conftest.py",
                             "**/wsgi.py", "**/asgi.py"],
        "test_globs": ["tests/**", "**/tests/**", "**/test_*.py", "**/*_test.py"],
        "script_dirs": ["scripts"],
        "db_modules": ["database_functions", "db_wrapper", "db", "database"],
        "fat_function_loc": 80,
        "fat_function_cc": 15,
        "fat_file_loc": 600,
        "top_n": 25,
        "dist_aliases": {
            "dotenv": "python-dotenv", "psycopg2": "psycopg2-binary", "yaml": "pyyaml",
            "cv2": "opencv-python", "PIL": "pillow", "sklearn": "scikit-learn",
            "garminconnect": "garminconnect", "jose": "python-jose", "jwt": "pyjwt",
            "bs4": "beautifulsoup4", "dateutil": "python-dateutil", "multipart": "python-multipart",
        },
    },
    "frontend": {
        "roots": [],  # auto-detected when empty (package.json next to tsconfig/next.config)
        "entry_globs": [
            "src/app/**/page.*", "src/app/**/layout.*", "src/app/**/loading.*",
            "src/app/**/error.*", "src/app/**/global-error.*", "src/app/**/not-found.*",
            "src/app/**/template.*", "src/app/**/default.*", "src/app/**/route.*",
            "src/middleware.*", "src/instrumentation.*", "app/**/page.*", "app/**/layout.*",
            "app/**/route.*", "*.config.*", "scripts/**", "src/test/setup.*",
        ],
        "alias_fallback": {"@/*": ["src/*"]},
    },
    "triage": {
        "dirs": ["scripts"],
        "stale_days": 90,
        "oneoff_regex": r"\d{3}_\d{3}|_t\d{2}|(^|/)(tmp|temp)_|_tmp|_bak|scratch",
        "oneoff_prefixes": ["diag", "diagnose", "probe", "investigate", "check", "verify", "audit", "measure", "build", "tmp"],
    },
    "git": {"since_months": 18, "max_commit_files": 30, "min_cochange": 3},
    "hygiene": {
        "sensitive_globs": [
            "**/.env", "**/.env.*", "**/*.env", "**/*token*.json", "**/*secret*.json",
            "**/*credentials*.json", "**/.spotify_cache", "**/*.pem", "**/*.key", "**/id_rsa*",
            "**/*.dump", "**/*.sqlite", "**/*.db", "**/*.pfx", "**/.netrc",
        ],
        "sensitive_allow": ["**/sample_env*", "**/*.example", "**/.env.example", "**/.env.sample"],
        "legacy_terms": ["streamlit"],
        "legacy_dirs": [".streamlit", "frontend_functions", "viz_factory", "pages"],
        "legacy_dist": ["streamlit"],
        "max_scan_bytes": 1_000_000,
    },
}


def deep_merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(root: Path, path: str | None) -> dict:
    cand = Path(path) if path else None
    if cand is None:
        for p in (root / "audit.toml", Path(__file__).resolve().parent / "audit.toml"):
            if p.is_file():
                cand = p
                break
    if cand and cand.is_file() and tomllib:
        with open(cand, "rb") as fh:
            return deep_merge(DEFAULTS, tomllib.load(fh))
    return copy.deepcopy(DEFAULTS)


# ----------------------------------------------------------------------------- globbing
@functools.lru_cache(maxsize=None)
def glob_re(pat: str) -> re.Pattern:
    pat = pat.replace("\\", "/")
    out, i = [], 0
    while i < len(pat):
        if pat.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pat.startswith("**", i):
            out.append(".*")
            i += 2
        elif pat[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pat[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pat[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$", re.IGNORECASE)


def match_any(path: str, globs) -> bool:
    return any(glob_re(g).match(path) for g in globs)


# ----------------------------------------------------------------------------- classification
LANG = {
    ".py": "Python", ".ts": "TypeScript", ".tsx": "TypeScript", ".js": "JavaScript",
    ".jsx": "JavaScript", ".mjs": "JavaScript", ".cjs": "JavaScript", ".sql": "SQL",
    ".md": "Markdown", ".json": "JSON", ".css": "CSS", ".sh": "Shell", ".ps1": "PowerShell",
    ".toml": "TOML", ".yaml": "YAML", ".yml": "YAML", ".ini": "INI", ".cfg": "INI",
    ".txt": "Text", ".html": "HTML", ".svg": "SVG", ".csv": "CSV", ".service": "systemd",
    ".timer": "systemd", ".conf": "Config",
}
ASSET_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".ico", ".wasm", ".woff", ".woff2",
             ".ttf", ".otf", ".mp3", ".mp4", ".pdf", ".zip", ".gz", ".whl", ".pyd", ".dll", ".exe"}
DUMP_EXT = {".dump", ".bak", ".sqlite", ".db", ".sqlite3"}
LOCK_NAMES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "uv.lock", "poetry.lock", "Pipfile.lock"}
CODE_EXT = {".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".sh", ".ps1", ".css"}
TEXT_EXT = set(LANG) | {".env", ".gitignore", ".gitattributes", ".clineignore", ".rst", ".out", ".log", ".map"}


def language(rel: str) -> str:
    return LANG.get(os.path.splitext(rel)[1].lower(), "Other")


def is_test_path(rel: str) -> bool:
    low = rel.lower()
    name = low.rsplit("/", 1)[-1]
    return ("/__tests__/" in "/" + low or low.startswith("tests/") or "/tests/" in low
            or name.startswith("test_") or name.endswith("_test.py")
            or re.search(r"\.(test|spec)\.[jt]sx?$", name) is not None)


def category(rel: str, size: int, cfg: dict) -> str:
    low = rel.lower()
    name = low.rsplit("/", 1)[-1]
    ext = os.path.splitext(name)[1]
    icfg = cfg["inventory"]
    if name in LOCK_NAMES:
        return "lock"
    if ext in ASSET_EXT:
        return "asset"
    if ext in DUMP_EXT:
        return "dump"
    if ext in (".log", ".out") or name.endswith(".tsbuildinfo") or ext == ".pyc":
        return "log"
    if any(fnmatch.fnmatch(name, g) for g in icfg["generated_hints"]):
        return "generated"
    if is_test_path(rel) and ext in CODE_EXT:
        return "test"
    if ext in CODE_EXT:
        return "code"
    if ext == ".sql":
        return "sql"
    if ext in (".md", ".rst"):
        return "docs"
    if ext == ".csv" or (ext in (".json", ".txt") and (
            size > 100_000 or any(h in low for h in icfg["data_hints"]))):
        return "data"
    if ext in (".json", ".toml", ".yaml", ".yml", ".ini", ".cfg", ".service", ".timer", ".conf", ".env") \
            or name.startswith(".") or name.endswith(".env"):
        return "config"
    return "text" if ext in (".txt", "") else "other"


def top_dir(rel: str) -> str:
    return rel.split("/", 1)[0] if "/" in rel else "(root)"


def domain_of(path: str, domains: dict) -> str | None:
    """Deepest-name-first keyword match so `activities/beach/VolleyballX.tsx` -> volleyball."""
    if not domains:
        return None
    p = path.lower().replace("\\", "/")
    parts = p.split("/")
    norm = lambda s: re.sub(r"[-_ .]", "", s)
    for seg in [parts[-1]] + parts[:-1][::-1]:
        ns = norm(seg)
        for name, kws in domains.items():
            if any(norm(k) in ns for k in kws):
                return name
    return None


def layer_of(path: str) -> str:
    p = "/" + path.lower()
    if is_test_path(path):
        return "tests"
    pairs = [("/scripts/", "scripts"), ("/api/", "api"), ("/routers/", "api"), ("/routes/", "api"),
             ("/queries/", "queries"), ("/repositories/", "queries"), ("/schemas/", "schemas"),
             ("/models/", "schemas"), ("/hooks/", "hooks"), ("/lib/types/", "types"),
             ("/types/", "types"), ("/components/", "components"), ("/stores/", "stores"),
             ("/lib/", "lib"), ("/utils/", "lib"), ("/deployment/", "deploy"), ("/deploy/", "deploy")]
    for frag, name in pairs:
        if frag in p:
            return name
    base = p.rsplit("/", 1)[-1]
    if re.match(r"(page|layout|route)\.[jt]sx?$", base):
        return "route"
    if p.endswith(".py"):
        return "functions"
    return "other"


# ----------------------------------------------------------------------------- findings
@dataclass
class Finding:
    id: str
    severity: str
    title: str
    detail: str = ""
    hint: str = ""
    count: int = 0
    evidence: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Section:
    name: str
    data: dict = field(default_factory=dict)
    findings: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    error: str | None = None
    seconds: float = 0.0

    def add(self, fid, severity, title, count=0, evidence=None, detail="", hint="", cap=15):
        ev = list(evidence or [])
        self.findings.append(Finding(fid, severity, title, detail, hint, count or len(ev), ev[:cap]))


# ----------------------------------------------------------------------------- text helpers
def decode_bytes(raw: bytes) -> str | None:
    if b"\0" in raw[:4096]:
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            return raw.decode("utf-16", "replace")
        return None
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252", "replace")


# ----------------------------------------------------------------------------- repo
class Repo:
    def __init__(self, root: str | Path, cfg: dict, use_git: bool = True):
        self.root = Path(root).resolve()
        self.cfg = cfg
        self.cache: dict = {}
        self._text: dict = {}
        self._sizes: dict = {}
        self.has_git = bool(use_git) and (self.git("rev-parse", "--is-inside-work-tree") or "").strip() == "true"
        self.tracked: set | None = None
        if self.has_git:
            out = self.git("ls-files", "-z") or ""
            self.tracked = {p for p in out.split("\0") if p}
        self.files = self._discover()
        self.fileset = set(self.files)

    # -- git ---------------------------------------------------------------
    def git(self, *args, timeout: int = 90) -> str | None:
        try:
            p = subprocess.run(["git", "-c", "core.quotepath=off", *args], cwd=self.root,
                               capture_output=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError):
            return None
        return p.stdout.decode("utf-8", "replace") if p.returncode == 0 else None

    # -- discovery ---------------------------------------------------------
    def _excluded(self, rel: str) -> bool:
        parts = rel.split("/")[:-1]
        return any(fnmatch.fnmatch(seg, pat) for seg in parts for pat in self.cfg["exclude_dirs"])

    def _discover(self) -> list[str]:
        if self.has_git:
            out = self.git("ls-files", "-co", "--exclude-standard", "-z") or ""
            rels = [p for p in out.split("\0") if p]
        else:
            rels = list(self._walk(prune=True))
        return sorted(r for r in rels if not self._excluded(r) and (self.root / r).is_file())

    def _walk(self, prune: bool):
        ex = self.cfg["exclude_dirs"]
        for dp, dns, fns in os.walk(self.root):
            if prune:
                dns[:] = [d for d in dns if not any(fnmatch.fnmatch(d, p) for p in ex)]
            else:  # still skip the truly huge/irrelevant trees
                dns[:] = [d for d in dns if d not in (".git", "node_modules", ".venv", "venv", ".next")]
            for fn in fns:
                yield os.path.relpath(os.path.join(dp, fn), self.root).replace("\\", "/")

    def disk_files(self) -> list[str]:
        """Every file on disk (ignored or not), minus .git/node_modules/.venv/.next."""
        if "disk" not in self.cache:
            self.cache["disk"] = sorted(self._walk(prune=False))
        return self.cache["disk"]

    # -- file access -------------------------------------------------------
    def size(self, rel: str) -> int:
        if rel not in self._sizes:
            try:
                self._sizes[rel] = (self.root / rel).stat().st_size
            except OSError:
                self._sizes[rel] = 0
        return self._sizes[rel]

    def text(self, rel: str, limit: int = 3_000_000) -> str | None:
        if rel in self._text:
            return self._text[rel]
        try:
            with open(self.root / rel, "rb") as fh:
                raw = fh.read(limit)
        except OSError:
            raw = b""
        t = decode_bytes(raw) if raw else ""
        self._text[rel] = t
        return t

    def drop_text(self, rel: str) -> None:
        self._text.pop(rel, None)

    def is_tracked(self, rel: str) -> bool | None:
        return None if self.tracked is None else rel in self.tracked

    def by_ext(self, *exts) -> list[str]:
        return [f for f in self.files if os.path.splitext(f)[1].lower() in exts]

    def is_ignored(self, rel: str) -> bool | None:
        if not self.has_git:
            return None
        try:
            p = subprocess.run(["git", "check-ignore", "-q", "--", rel], cwd=self.root,
                               capture_output=True, timeout=20)
        except (OSError, subprocess.SubprocessError):
            return None
        return p.returncode == 0
