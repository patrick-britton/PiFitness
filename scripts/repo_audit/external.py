"""Optional wrappers for external tools. They fix the failure modes seen in the first audit:
knip is run from the frontend app dir (not the repo root) and output is captured as clean UTF-8
instead of PowerShell `2>&1 | Out-File` (which turns stderr into NativeCommandError noise)."""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from .core import Section
from . import tsanalysis


def _run(cmd: list[str], cwd: Path, timeout: int = 300) -> tuple[int, str, str]:
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        return -1, "", str(e)
    return p.returncode, p.stdout.decode("utf-8", "replace"), p.stderr.decode("utf-8", "replace")


def analyze(repo, out_dir: Path) -> Section:
    sec = Section("external")
    ext_dir = out_dir / "external"
    ext_dir.mkdir(parents=True, exist_ok=True)
    ran = {}
    cloc = shutil.which("cloc")
    if cloc:
        rc, so, se = _run([cloc, "--json", "--quiet", "--exclude-dir=node_modules,.venv,.next,.git,__pycache__", "."], repo.root)
        (ext_dir / "cloc.json").write_text(so, encoding="utf-8")
        ran["cloc"] = rc
    npx = shutil.which("npx") or shutil.which("npx.cmd")
    roots = repo.cfg["frontend"]["roots"] or tsanalysis.detect_roots(repo)
    for r in roots:
        cwd = repo.root / r if r else repo.root
        if not (cwd / "node_modules").exists() or not npx:
            ran[f"knip[{r or '.'}]"] = "skipped (no node_modules or npx)"
            continue
        rc, so, se = _run([npx, "--no-install", "knip", "--reporter", "json", "--no-progress"], cwd)
        name = (r or "root").replace("/", "_")
        (ext_dir / f"knip-{name}.json").write_text(so or se, encoding="utf-8")
        ran[f"knip[{r or '.'}]"] = rc
        try:
            data = json.loads(so)
            files = data.get("files", [])
            sec.add("EXT-001", "info", f"knip ({r or '.'}): {len(files)} unused files, run from the app dir", count=len(files),
                    evidence=files[:20], hint="Compare with the frontend section's import-graph result; disagreements need a look.")
        except (json.JSONDecodeError, AttributeError):
            pass
    sec.data = {"ran": ran, "outputs": str(ext_dir)}
    return sec
