#!/usr/bin/env python3
"""PiFitness feature request form launcher (stdlib only).

Serves tools/workflow/request_form.html on localhost and writes the finished
request to .features/descriptions/<id>.md. This file is the single home for
validation and Markdown generation; the browser only collects input.

    python tools/workflow/request_form.py
    python tools/workflow/request_form.py --port 8765 --no-browser
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import webbrowser
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DESCRIPTIONS_DIR = REPO_ROOT / ".features" / "descriptions"
HTML_PATH = Path(__file__).with_name("request_form.html")

SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{1,59}$")
CATEGORIES = ("New Capability", "Enhancement", "Bug Fix")
LIST_FIELDS = ("problems", "behaviors", "nongoals", "context", "questions")
MAX_BODY_BYTES = 256 * 1024
BULLET_PREFIX_RE = re.compile(r"^(?:[-*\u2022]\s+)+")


def clean_items(raw: object) -> list[str]:
    """Collapse each item to one line, drop typed bullet markers and blanks."""
    if not isinstance(raw, list):
        return []
    items: list[str] = []
    for entry in raw:
        if not isinstance(entry, str):
            continue
        text = BULLET_PREFIX_RE.sub("", " ".join(entry.split()))
        if text:
            items.append(text)
    return items


def normalise(payload: object) -> dict:
    data = payload if isinstance(payload, dict) else {}
    req = {
        "featureId": " ".join(str(data.get("featureId", "")).split()),
        "title": " ".join(str(data.get("title", "")).split()),
        "category": str(data.get("category", "")),
        "overwrite": data.get("overwrite") is True,
    }
    for field in LIST_FIELDS:
        req[field] = clean_items(data.get(field))
    return req


def validate(req: dict) -> list[str]:
    errors: list[str] = []
    if not req["title"]:
        errors.append("Add a title.")
    if not req["featureId"]:
        errors.append("Add a feature ID.")
    elif not SLUG_RE.match(req["featureId"]):
        errors.append(
            "Feature ID must be 2-60 characters: lowercase letters, digits and "
            "hyphens, starting with a letter (e.g. feat-music-search)."
        )
    if req["category"] not in CATEGORIES:
        errors.append("Pick a category.")
    if not req["problems"]:
        errors.append("Describe at least one problem.")
    if not req["behaviors"]:
        errors.append("Add at least one thing that should work when this is done.")
    return errors


def bullets(items: list[str], empty: str = "- None") -> list[str]:
    return [f"- {item}" for item in items] if items else [empty]


def build_markdown(req: dict) -> str:
    lines = [
        "---",
        f"feature-id: {req['featureId']}",
        f"title: {req['title']}",
        f"category: {req['category']}",
        f"created: {date.today().isoformat()}",
        "---",
        "",
        f"# Feature: {req['title']}",
        "",
        "## 1. Problem",
        *bullets(req["problems"]),
        "",
        "## 2. What Good Looks Like",
        *[f"- B{n}: {text}" for n, text in enumerate(req["behaviors"], start=1)],
        "",
        "## 3. Out of Scope",
        *bullets(req["nongoals"]),
        "",
        "## 4. Known Context (optional)",
        *bullets(req["context"]),
        "",
        "## 5. Open Questions (optional)",
        *bullets(req["questions"]),
        "",
    ]
    return "\n".join(lines)


def target_path(req: dict) -> Path:
    return DESCRIPTIONS_DIR / f"{req['featureId']}.md"


def display_path(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def preview_response(req: dict) -> dict:
    errors = validate(req)
    response = {"errors": errors, "markdown": build_markdown(req)}
    if req["featureId"] and SLUG_RE.match(req["featureId"]):
        path = target_path(req)
        response["path"] = display_path(path)
        response["exists"] = path.exists()
    return response


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: dict) -> None:
        self._send(status, json.dumps(obj).encode("utf-8"), "application/json; charset=utf-8")

    def _host_allowed(self) -> bool:
        port = self.server.server_address[1]
        return self.headers.get("Host", "") in {f"127.0.0.1:{port}", f"localhost:{port}"}

    def do_GET(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
        elif self.path in ("/", "/index.html"):
            self._send(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
        else:
            self._json(404, {"error": "Not found."})

    def do_POST(self) -> None:
        if not self._host_allowed():
            self._json(403, {"error": "Forbidden host."})
            return
        if self.path not in ("/api/preview", "/api/save"):
            self._json(404, {"error": "Not found."})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_BODY_BYTES:
                self._json(413, {"error": "Request too large."})
                return
            req = normalise(json.loads(self.rfile.read(length) or b"{}"))
        except (ValueError, json.JSONDecodeError):
            self._json(400, {"error": "Malformed request."})
            return

        if self.path == "/api/preview":
            self._json(200, preview_response(req))
        else:
            self._save(req)

    def _save(self, req: dict) -> None:
        errors = validate(req)
        if errors:
            self._json(422, {"errors": errors})
            return
        path = target_path(req)
        DESCRIPTIONS_DIR.mkdir(parents=True, exist_ok=True)
        try:
            with open(path, "w" if req["overwrite"] else "x", encoding="utf-8", newline="\n") as fh:
                fh.write(build_markdown(req))
        except FileExistsError:
            self._json(409, {"exists": True, "path": display_path(path)})
            return
        self._json(200, {"saved": True, "path": display_path(path)})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0, help="Port to listen on (default: any free port).")
    parser.add_argument("--no-browser", action="store_true", help="Do not open a browser window.")
    args = parser.parse_args()

    if not HTML_PATH.is_file():
        raise SystemExit(f"Missing {HTML_PATH.name} next to this script.")

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    print(f"Feature request form: {url}")
    print(f"Saving to: {display_path(DESCRIPTIONS_DIR)}/  (Ctrl+C to stop)")
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
