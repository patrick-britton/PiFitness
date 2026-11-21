#!/usr/bin/env python3
"""PiFitness UAT launcher (stdlib only).

Reads tools/workflow/uat.json (written by `fd.py uat build <id>`), serves
tools/workflow/uat_launcher.html on localhost, receives the human's Pass/Fail
verdicts, and writes .features/designs_active/<id>_feedback.md directly.

After saving it also runs `fd.py uat import <id> --apply` and streams the
summary back to the browser so you can read it without leaving the page.

    python tools/workflow/uat_launcher.py
    python tools/workflow/uat_launcher.py --port 8766 --no-browser
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
UAT_JSON = Path(__file__).with_name("uat.json")
HTML_PATH = Path(__file__).with_name("uat_launcher.html")
DESIGNS_ACTIVE = REPO_ROOT / ".features" / "designs_active"
FD_PY = Path(__file__).with_name("fd.py")
PYTHON = sys.executable

MAX_BODY_BYTES = 256 * 1024


def load_uat_data() -> dict:
    """Load and return the current uat.json, raising RuntimeError on failure."""
    if not UAT_JSON.is_file():
        raise RuntimeError(
            "uat.json not found. Run: python tools/workflow/fd.py uat build <id>"
        )
    with open(UAT_JSON, encoding="utf-8") as fh:
        return json.load(fh)


def stamp_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def build_feedback_md(data: dict, verdicts: list[dict]) -> str:
    """Render the <id>_feedback.md table that fd.py uat import can parse."""
    fid = data["feature_id"]
    title = data.get("title", fid)
    ts = stamp_now()
    ac_map = {ac["id"]: ac for ac in data["acceptance_criteria"]}

    rows = []
    npass = nfail = npend = 0
    for v in verdicts:
        ac_id = v.get("id", "")
        result = v.get("result", "Pending").capitalize()
        if result not in ("Pass", "Fail", "Pending"):
            result = "Pending"
        note = str(v.get("notes", "")).replace("|", "").replace("\n", " ").strip()
        if result == "Pass":
            npass += 1
        elif result == "Fail":
            nfail += 1
        else:
            npend += 1
        rows.append(f"| {ac_id} | {result} | {note} |")

    lines = [
        "# UAT Feedback",
        "",
        f"**Feature:** {fid}  ({title})",
        "",
        f"**Exported:** {ts}",
        "",
        "| AC | Result | Notes |",
        "|---|---|---|",
        *rows,
        "",
        (
            f"Summary: {npass + nfail} of {len(verdicts)} checked "
            f"({npass} pass, {nfail} fail, {npend} pending)"
        ),
        "",
    ]
    return "\n".join(lines)


def run_import(fid: str) -> str:
    """Run fd.py uat import <id> --apply and return stdout+stderr combined."""
    result = subprocess.run(
        [PYTHON, str(FD_PY), "uat", "import", fid, "--apply"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    out = (result.stdout + result.stderr).strip()
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:  # silence access log
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
        port = self.server.server_address[1]
        host = self.headers.get("Host", "")
        return host in {f"127.0.0.1:{port}", f"localhost:{port}"}

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
            return
        if self.path in ("/", "/index.html"):
            self._send(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/data":
            try:
                self._json(200, load_uat_data())
            except RuntimeError as e:
                self._json(503, {"error": str(e)})
        else:
            self._json(404, {"error": "Not found."})

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
            return
        if self.path != "/api/save":
            self._json(404, {"error": "Not found."})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_BODY_BYTES:
                self._json(413, {"error": "Request too large."})
                return
            payload = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self._json(400, {"error": "Malformed request."})
            return

        try:
            data = load_uat_data()
        except RuntimeError as e:
            self._json(503, {"error": str(e)})
            return

        fid = data["feature_id"]
        verdicts = payload.get("verdicts", [])
        md = build_feedback_md(data, verdicts)
        feedback_path = DESIGNS_ACTIVE / f"{fid}_feedback.md"

        overwrite = payload.get("overwrite", False)
        if feedback_path.exists() and not overwrite:
            self._json(409, {
                "exists": True,
                "path": feedback_path.relative_to(REPO_ROOT).as_posix(),
            })
            return

        DESIGNS_ACTIVE.mkdir(parents=True, exist_ok=True)
        feedback_path.write_text(md, encoding="utf-8", newline="\n")

        import_summary = run_import(fid)
        self._json(200, {
            "saved": True,
            "path": feedback_path.relative_to(REPO_ROOT).as_posix(),
            "import_summary": import_summary,
        })


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0,
                        help="Port to listen on (default: any free port).")
    parser.add_argument("--no-browser", action="store_true",
                        help="Do not open a browser window.")
    args = parser.parse_args()

    if not HTML_PATH.is_file():
        raise SystemExit(f"Missing {HTML_PATH.name} next to this script.")

    # Warn early if uat.json is missing, but don't block — the browser shows the error.
    if not UAT_JSON.is_file():
        print(
            "Warning: uat.json not found. "
            "Run `python tools/workflow/fd.py uat build <id>` first.",
            file=sys.stderr,
        )

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    print(f"UAT launcher: {url}")
    print(f"Reading:  {UAT_JSON.relative_to(REPO_ROOT).as_posix()}")
    print(f"Writing:  .features/designs_active/<id>_feedback.md  (Ctrl+C to stop)")
    if not args.no_browser:
        threading.Timer(0.3, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
