#!/usr/bin/env python3
"""PiFitness Developer Hub Server (stdlib only).

Serves the unified PiFitness Developer Hub on localhost (default port 8760):
- Dashboard: http://localhost:8760/
- Request Form: http://localhost:8760/request
- UAT Form: http://localhost:8760/uat

Features:
1. Open Request Form (integrated into DevHub at /request)
2. Open UAT Form (integrated into DevHub at /uat)
3. Prep Dev Environment (tools/refresh_dev.ps1)
4. Launch Local Environment (backend uvicorn + frontend next dev) & open http://localhost:3000
5. Launch Bootstrapper (deploy/run-bootstrap.ps1)
6. Merge to PRD (git checkout main, pull, merge --no-ff, push, checkout feature-test) + Launch Bootstrapper
7. Complete Feature (move .features/descriptions & designs_active to zz_feature_archive + refresh_dev.ps1)

Usage:
    python tools/workflow/dev_hub.py
    python tools/workflow/dev_hub.py --port 8760 --no-browser
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Add tools/workflow to sys.path so we can import request_form and uat_launcher
WORKFLOW_DIR = Path(__file__).resolve().parent
REPO_ROOT = WORKFLOW_DIR.parents[1]
if str(WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_DIR))

import request_form
import uat_launcher

HTML_PATH = WORKFLOW_DIR / "dev_hub.html"
DESCRIPTIONS_DIR = REPO_ROOT / ".features" / "descriptions"
DESIGNS_ACTIVE_DIR = REPO_ROOT / ".features" / "designs_active"
ARCHIVE_DIR = REPO_ROOT / "zz_feature_archive"
REFRESH_DEV_PS1 = REPO_ROOT / "tools" / "refresh_dev.ps1"
BOOTSTRAP_PS1 = REPO_ROOT / "deploy" / "run-bootstrap.ps1"
PYTHON = sys.executable

MAX_BODY_BYTES = 256 * 1024


def is_port_open(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a port is currently accepting TCP connections."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            return s.connect_ex((host, port)) == 0
    except OSError:
        return False


def get_git_branch() -> str:
    """Get current git branch name."""
    try:
        res = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=3,
        )
        return res.stdout.strip() or "unknown"
    except Exception:
        return "error"


def get_docker_dev_db_status() -> str:
    """Check if pifitness-dev-db container is running."""
    try:
        res = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", "pifitness-dev-db"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        status = res.stdout.strip()
        return status if status else "not found"
    except Exception:
        return "docker unavailable"


def get_active_features() -> dict:
    """List active feature files in descriptions and designs_active."""
    descriptions: list[str] = []
    if DESCRIPTIONS_DIR.is_dir():
        for p in DESCRIPTIONS_DIR.iterdir():
            if p.is_file() and p.name != ".gitkeep":
                descriptions.append(p.name)

    designs: list[str] = []
    if DESIGNS_ACTIVE_DIR.is_dir():
        for p in DESIGNS_ACTIVE_DIR.iterdir():
            if p.is_file() and p.name != ".gitkeep":
                designs.append(p.name)

    return {
        "descriptions": sorted(descriptions),
        "designs_active": sorted(designs),
    }


def archive_feature_files() -> list[str]:
    """Move all files from descriptions and designs_active to zz_feature_archive."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    moved: list[str] = []

    for source_dir in (DESCRIPTIONS_DIR, DESIGNS_ACTIVE_DIR):
        if not source_dir.is_dir():
            continue
        for file_path in source_dir.iterdir():
            if file_path.is_file() and file_path.name != ".gitkeep":
                dest_path = ARCHIVE_DIR / file_path.name
                if dest_path.exists():
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    stem = file_path.stem
                    suffix = file_path.suffix
                    dest_path = ARCHIVE_DIR / f"{stem}_{timestamp}{suffix}"
                shutil.move(str(file_path), str(dest_path))
                moved.append(f"{file_path.name} -> {dest_path.name}")

    return moved


def inject_devhub_link(html_bytes: bytes, current_title: str) -> bytes:
    """Inject a small, subtle 'Back to DevHub' banner button into the integrated forms."""
    pill = (
        b'<div style="position:fixed;top:10px;right:14px;z-index:99999;">'
        b'<a href="/" style="display:inline-flex;align-items:center;gap:6px;'
        b'background:var(--surface,#ffffff);color:var(--accent,#0f6a6a);'
        b'border:1px solid var(--rule,#d6dbe0);padding:5px 12px;border-radius:20px;'
        b'font-family:system-ui,sans-serif;font-size:12px;font-weight:600;'
        b'text-decoration:none;box-shadow:0 2px 6px rgba(0,0,0,0.08);">'
        b'&larr; DevHub Dashboard</a></div>'
    )
    if b"</body>" in html_bytes:
        return html_bytes.replace(b"</body>", pill + b"</body>", 1)
    return html_bytes + pill


class DevHubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        """Silence standard request logging."""
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: object) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _host_allowed(self) -> bool:
        host = self.headers.get("Host", "").split(":")[0].strip().lower()
        return host in {"127.0.0.1", "localhost", "::1", "[::1]"}

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
            return

        clean_path = self.path.split("?")[0].rstrip("/") or "/"

        # 1. DevHub Dashboard
        if clean_path in ("/", "/index.html"):
            if not HTML_PATH.is_file():
                self._send(404, b"dev_hub.html not found.", "text/plain")
                return
            self._send(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
            return

        # 2. Integrated Request Form
        if clean_path in ("/request", "/request_form"):
            if not request_form.HTML_PATH.is_file():
                self._send(404, b"request_form.html not found.", "text/plain")
                return
            html = inject_devhub_link(request_form.HTML_PATH.read_bytes(), "Request Form")
            self._send(200, html, "text/html; charset=utf-8")
            return

        # 3. Integrated UAT Form
        if clean_path in ("/uat", "/uat_launcher"):
            if not uat_launcher.HTML_PATH.is_file():
                self._send(404, b"uat_launcher.html not found.", "text/plain")
                return
            html = inject_devhub_link(uat_launcher.HTML_PATH.read_bytes(), "UAT Form")
            self._send(200, html, "text/html; charset=utf-8")
            return

        # 4. DevHub Status API
        if clean_path == "/api/status":
            status_data = {
                "branch": get_git_branch(),
                "dev_db": get_docker_dev_db_status(),
                "ports": {
                    "backend": is_port_open(8000),
                    "frontend": is_port_open(3000),
                    "postgres": is_port_open(15432),
                },
                "active_features": get_active_features(),
            }
            self._json(200, status_data)
            return

        # 5. UAT Data API (called by uat_launcher.html)
        if clean_path == "/api/data":
            try:
                self._json(200, uat_launcher.load_uat_data())
            except RuntimeError as e:
                self._json(503, {"error": str(e)})
            return

        self._json(404, {"error": "Endpoint not found."})

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_BODY_BYTES:
                self._json(413, {"error": "Request payload too large."})
                return
            raw_body = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except Exception as e:
            self._json(400, {"error": f"Invalid JSON payload: {e}"})
            return

        clean_path = self.path.split("?")[0].rstrip("/")

        # 1. Request Form Preview (/api/preview)
        if clean_path == "/api/preview":
            req = request_form.normalise(payload)
            self._json(200, request_form.preview_response(req))
            return

        # 2. Save Endpoint (/api/save) - Handles both Request Form and UAT Form
        if clean_path == "/api/save":
            referer = self.headers.get("Referer", "")
            is_uat = ("verdicts" in payload) or ("/uat" in referer)
            if is_uat:
                self._handle_uat_save(payload)
            else:
                self._handle_request_save(payload)
            return

        # 3. DevHub Action Runner (/api/action)
        if clean_path == "/api/action":
            action = payload.get("action")
            self._handle_action_stream(action, payload)
            return

        self._json(404, {"error": "Endpoint not found."})

    def _handle_request_save(self, payload: dict) -> None:
        req = request_form.normalise(payload)
        errors = request_form.validate(req)
        if errors:
            self._json(422, {"errors": errors})
            return

        path = request_form.target_path(req)
        if path.exists() and not req.get("overwrite"):
            self._json(409, {
                "exists": True,
                "path": request_form.display_path(path),
            })
            return

        DESCRIPTIONS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(request_form.build_markdown(req), encoding="utf-8", newline="\n")
        self._json(200, {
            "saved": True,
            "path": request_form.display_path(path),
        })

    def _handle_uat_save(self, payload: dict) -> None:
        try:
            data = uat_launcher.load_uat_data()
        except RuntimeError as e:
            self._json(503, {"error": str(e)})
            return

        fid = data["feature_id"]
        verdicts = payload.get("verdicts", [])
        md = uat_launcher.build_feedback_md(data, verdicts)
        feedback_path = DESIGNS_ACTIVE_DIR / f"{fid}_feedback.md"

        overwrite = payload.get("overwrite", False)
        if feedback_path.exists() and not overwrite:
            self._json(409, {
                "exists": True,
                "path": feedback_path.relative_to(REPO_ROOT).as_posix(),
            })
            return

        DESIGNS_ACTIVE_DIR.mkdir(parents=True, exist_ok=True)
        feedback_path.write_text(md, encoding="utf-8", newline="\n")

        import_summary = uat_launcher.run_import(fid)
        self._json(200, {
            "saved": True,
            "path": feedback_path.relative_to(REPO_ROOT).as_posix(),
            "import_summary": import_summary,
        })

    def _send_sse_event(self, event_data: dict) -> bool:
        """Send a single SSE data frame to the client."""
        try:
            msg = f"data: {json.dumps(event_data, ensure_ascii=False)}\n\n".encode("utf-8")
            self.wfile.write(msg)
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError):
            return False

    def _handle_action_stream(self, action: str, payload: dict) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        port = self.server.server_address[1]

        def log(msg: str, kind: str = "info") -> None:
            self._send_sse_event({"type": "log", "line": msg, "kind": kind})

        def finish(success: bool, msg: str, extra: dict | None = None) -> None:
            data = {"type": "done", "success": success, "message": msg}
            if extra:
                data.update(extra)
            self._send_sse_event(data)

        # 1. Open Request Form (directly on DevHub server)
        if action == "open_request_form":
            url = f"http://127.0.0.1:{port}/request"
            log("Opening integrated Request Form...", "ok")
            finish(True, "Opened Request Form", {"url": url})
            return

        # 2. Open UAT Form (directly on DevHub server)
        if action == "open_uat_form":
            url = f"http://127.0.0.1:{port}/uat"
            log("Opening integrated UAT Form...", "ok")
            finish(True, "Opened UAT Form", {"url": url})
            return

        # 3. Prep Dev Environment
        if action == "prep_dev_env":
            log("Running tools/refresh_dev.ps1 ...", "heading")
            cmd = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(REFRESH_DEV_PS1)]
            self._run_streaming_cmd(cmd, log, finish)
            return

        # 4. Launch Local Environment
        if action == "launch_local_env":
            log("Launching Local Environment...", "heading")
            py_path = str(PYTHON)
            log("Starting FastAPI Backend (uvicorn) in a dedicated terminal...")
            backend_cmd = (
                f'Start-Process powershell -ArgumentList '
                f'"-NoExit", "-Command", "cd backend; & \'{py_path}\' -m uvicorn main:app --reload"'
            )
            subprocess.run(["powershell.exe", "-NoProfile", "-Command", backend_cmd], cwd=str(REPO_ROOT))
            log("Backend process spawned on port 8000.", "ok")

            log("Starting Next.js Frontend (npm run build; npm run dev) in a dedicated terminal...")
            frontend_cmd = (
                'Start-Process powershell -ArgumentList '
                '"-NoExit", "-Command", "cd frontend\\pifitness; npm run build; npm run dev"'
            )
            subprocess.run(["powershell.exe", "-NoProfile", "-Command", frontend_cmd], cwd=str(REPO_ROOT))
            log("Frontend process spawned on port 3000.", "ok")

            log("Opening http://localhost:3000/ in browser...")
            threading.Timer(2.0, lambda: webbrowser.open("http://localhost:3000/")).start()
            finish(True, "Local Environment Launched", {"url": "http://localhost:3000/"})
            return

        # 5. Launch Bootstrapper
        if action == "launch_bootstrapper":
            log("Launching PiFitness Bootstrapper...", "heading")
            log("Opening interactive SSH session in dedicated terminal (deploy/run-bootstrap.ps1)...")
            cmd = (
                f'Start-Process powershell -ArgumentList '
                f'"-NoExit", "-ExecutionPolicy", "Bypass", "-File", "\'{str(BOOTSTRAP_PS1)}\'"'
            )
            subprocess.run(["powershell.exe", "-NoProfile", "-Command", cmd], cwd=str(REPO_ROOT))
            log("Bootstrapper terminal launched.", "ok")
            log("Evergreen log will sync to deploy/bootstrap.log upon completion.")
            finish(True, "Bootstrapper Launched in Terminal")
            return

        # 6. Merge to PRD & Launch Bootstrapper
        if action == "merge_to_prd":
            log("=== Merge to PRD & Deploy Sequence ===", "heading")
            git_steps = [
                ("git checkout main", ["git", "checkout", "main"]),
                ("git pull origin main", ["git", "pull", "origin", "main"]),
                (
                    "git merge --no-ff feature-test",
                    ["git", "merge", "--no-ff", "feature-test", "-m", "Merge branch 'feature-test' into main"],
                ),
                ("git push origin main", ["git", "push", "origin", "main"]),
                ("git checkout feature-test", ["git", "checkout", "feature-test"]),
            ]

            success = True
            for desc, cmd in git_steps:
                log(f"> {desc} ...")
                proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
                if proc.stdout.strip():
                    for line in proc.stdout.splitlines():
                        log(f"  {line}")
                if proc.stderr.strip():
                    for line in proc.stderr.splitlines():
                        log(f"  {line}", "warn" if proc.returncode == 0 else "error")

                if proc.returncode != 0:
                    log(f"FAILED: {desc} exited with code {proc.returncode}. Aborting.", "error")
                    success = False
                    break

            if not success:
                finish(False, "Git merge sequence failed. Check logs above.")
                return

            log("Git merge to main completed successfully!", "ok")
            log("Now launching Bootstrapper in a dedicated terminal...")
            cmd = (
                f'Start-Process powershell -ArgumentList '
                f'"-NoExit", "-ExecutionPolicy", "Bypass", "-File", "\'{str(BOOTSTRAP_PS1)}\'"'
            )
            subprocess.run(["powershell.exe", "-NoProfile", "-Command", cmd], cwd=str(REPO_ROOT))
            log("Bootstrapper terminal opened.", "ok")
            finish(True, "Merged to main and launched Bootstrapper")
            return

        # 7. Complete Feature
        if action == "complete_feature":
            log("=== Complete Feature Workflow ===", "heading")
            log("Step 1: Moving active feature files to zz_feature_archive/ ...")
            try:
                moved = archive_feature_files()
                if moved:
                    for item in moved:
                        log(f"  Archived: {item}", "ok")
                else:
                    log("  No active feature files found to archive.", "warn")
            except Exception as e:
                log(f"Failed to archive files: {e}", "error")
                finish(False, f"Archiving failed: {e}")
                return

            log("\nStep 2: Refreshing DEV environment (tools/refresh_dev.ps1) ...", "heading")
            cmd = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(REFRESH_DEV_PS1)]
            self._run_streaming_cmd(cmd, log, finish)
            return

        log(f"Unknown action: {action}", "error")
        finish(False, f"Unknown action: {action}")

    def _run_streaming_cmd(self, cmd: list[str], log_fn, finish_fn) -> None:
        """Run a command and stream stdout/stderr line-by-line via SSE."""
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )

            if proc.stdout:
                for line in proc.stdout:
                    cleaned = line.rstrip("\r\n")
                    if cleaned:
                        log_fn(cleaned)

            proc.wait()
            if proc.returncode == 0:
                log_fn(f"Command succeeded with exit code 0.", "ok")
                finish_fn(True, "Command completed successfully")
            else:
                log_fn(f"Command failed with exit code {proc.returncode}.", "error")
                finish_fn(False, f"Command failed (exit code {proc.returncode})")
        except Exception as e:
            log_fn(f"Exception running command: {e}", "error")
            finish_fn(False, str(e))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8760, help="Port to listen on (default: 8760).")
    parser.add_argument("--no-browser", action="store_true", help="Do not open a browser window.")
    args = parser.parse_args()

    if not HTML_PATH.is_file():
        raise SystemExit(f"Missing {HTML_PATH.name} next to this script.")

    port = args.port
    server = None
    for attempt_port in range(port, port + 10):
        try:
            server = ThreadingHTTPServer(("127.0.0.1", attempt_port), DevHubHandler)
            port = attempt_port
            break
        except OSError:
            continue

    if not server:
        raise SystemExit(f"Could not bind to any port in range {port}-{port+9}.")

    url = f"http://127.0.0.1:{port}/"
    print(f"PiFitness DevHub running at: {url}")
    print(f"  - Dashboard:    {url}")
    print(f"  - Request Form: {url}request")
    print(f"  - UAT Form:     {url}uat")
    print("Press Ctrl+C to stop.")

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDevHub stopped.")


if __name__ == "__main__":
    main()
