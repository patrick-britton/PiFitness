#!/usr/bin/env python3
"""
deploy/scripts/lib/step_dependencies.py  --  TEST Step 3 / PRD Step 4: reconcile dependencies.

    step_dependencies.py --run-dir <dir>

Makes the machine's dependencies match what the checked-out commit declares:

    1. dbmate            pinned in tools.conf; installed (or replaced) only when the version differs
    2. Python venv       `uv sync --locked` from uv.lock into VENV_DIR, then a second sync that must
                         change nothing, `uv pip check`, and a check that nothing extra is installed
    3. Node packages     `npm ci` next to package-lock.json
    4. Playwright        the browsers named in tools.conf, installed with the venv's own Playwright,
                         then launched once to prove they work

TEST and PRD run exactly this code. Everything that differs comes from the Pi env file
(APP_DIR, VENV_DIR). There is no `if environment == ...` anywhere in this file.

Reads
    Pi env file         APP_DIR, VENV_DIR
    autodeploy.conf     DEPENDENCY_GROUPS (may be empty), PRIVILEGE_CMD (may be empty)
    tools.conf          INSTALLABLE_TOOLS, PLAYWRIGHT_BROWSERS (both may be empty), DBMATE_VERSION,
                        DBMATE_SHA256_ARM64 (needed when dbmate is listed)
    The checkout        uv.lock (+ pyproject.toml beside it) and package-lock.json (+ package.json
                        beside it). Their directories are found by searching the checkout; there
                        must be exactly one of each lockfile, otherwise the step fails and lists
                        what it found. Nothing about the repo layout is configured.

Writes
    Nothing to run.state and no undo action: the venv, node_modules and the tools are not rolled
    back by the exit handler (the venv lives outside the app dir; node_modules is replaced with
    the app dir). A failed run leaves them in whatever state the failed command left; the next
    run reconciles them again.

Constants (they never vary, so they are not configuration)
    uv is run with --locked --no-editable --python /usr/bin/python3, UV_PROJECT_ENVIRONMENT=VENV_DIR
    dbmate is installed to /usr/local/bin/dbmate (via PRIVILEGE_CMD install), linux-arm64 only
    npm is run as `npm ci --no-audit --no-fund --include=dev`

Exit: 0 pass, 1 fail (REASON:/WHERE: printed).  Never 10 (reserved).
Python 3 standard library only.
"""
import hashlib
import json
import os
import platform
import re
import shutil
import sys
import tempfile
import tomllib
import urllib.error
import urllib.request

from common import (PYTHON, HelperError, Parser, check_keys, describe_gaps, evidence, info,
                    is_inside, load_context, parse_kv_file, run_cmd, step_main, warn, words)

TOOLS_CONF = "tools.conf"                      # sits beside autodeploy.conf in deploy/scripts
DBMATE_INSTALL_PATH = "/usr/local/bin/dbmate"
DBMATE_URL = "https://github.com/amacneil/dbmate/releases/download/v%s/dbmate-linux-arm64"
DBMATE_MACHINES = ("aarch64", "arm64")
KNOWN_INSTALLABLE = ("dbmate",)                # names INSTALLABLE_TOOLS may contain
BROWSERS = ("chromium", "firefox", "webkit")   # names PLAYWRIGHT_BROWSERS may contain
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "__pycache__", ".next"}
LONG_TIMEOUT = 1800                            # uv sync / npm ci / browser download, seconds
DOWNLOAD_TIMEOUT = 120
SAMPLE = 20
VERSION_RE = re.compile(r"\d+\.\d+\.\d+")

# Launches one Playwright browser and prints its version (argv[1] = chromium|firefox|webkit).
LAUNCH_CHECK = (
    "import sys\n"
    "from playwright.sync_api import sync_playwright\n"
    "with sync_playwright() as p:\n"
    "    b = getattr(p, sys.argv[1]).launch()\n"
    "    print(b.version)\n"
    "    b.close()\n")


def parse_args(argv):
    parser = Parser(prog="step_dependencies.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def norm(name):
    """Package names compare equal regardless of case and - _ . differences."""
    return re.sub(r"[-_.]+", "-", name).lower()


def sample(items):
    items = list(items)
    text = ", ".join(items[:SAMPLE])
    if len(items) > SAMPLE:
        text += ", ... (%d more)" % (len(items) - SAMPLE)
    return text


def sha256_file(path):
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        raise HelperError("cannot read file: %s" % exc.strerror, path)
    return digest.hexdigest()


def reason_from_output(result):
    """One-line cause for a command whose output was echoed (stdout and stderr merged)."""
    if result.timed_out:
        return "timed out after %.0fs" % result.seconds
    lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    for line in reversed(lines):
        if "error" in line.lower():
            return line[:200]
    return lines[-1][:200] if lines else "exit status %d" % result.rc


def first_version(text):
    match = VERSION_RE.search(text or "")
    return match.group(0) if match else None


class Dependencies:
    def __init__(self, ctx):
        self.ctx = ctx
        self.tools = {}
        self.installable = []
        self.browsers = []
        self.sync_args = []
        self.privilege = []
        self.uv_path = None

    # ------------------------------------------------------------------ run
    def run(self):
        stages = [("Configuration and paths", self.stage_config),
                  ("Non-pip tools", self.stage_tools),
                  ("Python environment", self.stage_python),
                  ("Node dependencies", self.stage_node),
                  ("Playwright browsers", self.stage_playwright)]
        total = len(stages)
        for number, (title, function) in enumerate(stages, 1):
            info("(%d/%d) %s ..." % (number, total, title))
            function()
        evidence("dependencies", "reconciled")

    # ------------------------------------------------------ 1. config, paths
    def stage_config(self):
        c = self.ctx
        c.require(env=["APP_DIR", "VENV_DIR"],
                  conf_may_be_empty=["DEPENDENCY_GROUPS", "PRIVILEGE_CMD"])
        self.app_dir = os.path.abspath(c.env_value("APP_DIR"))
        self.venv_dir = os.path.abspath(c.env_value("VENV_DIR"))
        self.venv_python = os.path.join(self.venv_dir, "bin", "python")
        self.privilege = c.conf_words("PRIVILEGE_CMD")
        self.sync_args = (["sync", "--locked", "--no-editable", "--python", PYTHON]
                          + c.conf_words("DEPENDENCY_GROUPS"))

        if not os.path.isdir(self.app_dir):
            raise HelperError("the application directory does not exist (the checkout step "
                              "should have created it)", self.app_dir)
        if is_inside(self.venv_dir, self.app_dir):
            raise HelperError("VENV_DIR is inside APP_DIR and would be destroyed by the "
                              "checkout swap", "VENV_DIR=%s APP_DIR=%s" % (self.venv_dir, self.app_dir))

        self.project_dir = self.find_one("uv.lock", "pyproject.toml")
        self.frontend_dir = self.find_one("package-lock.json", "package.json")
        self.pyproject = os.path.join(self.project_dir, "pyproject.toml")
        self.uv_lock = os.path.join(self.project_dir, "uv.lock")
        self.npm_lock = os.path.join(self.frontend_dir, "package-lock.json")

        self.load_tools()
        evidence("app_dir", self.app_dir)
        evidence("venv_dir", "%s (outside the app dir: yes)" % self.venv_dir)
        evidence("python_project_dir", self.project_dir)
        evidence("frontend_dir", self.frontend_dir)
        evidence("tools_file", os.path.join(c.script_dir, TOOLS_CONF))
        evidence("uv_sync_command", "uv " + " ".join(self.sync_args))

    def find_one(self, lockfile, companion):
        """The one directory of the checkout that holds `lockfile` (with `companion` beside it)."""
        found = []
        for dirpath, dirnames, filenames in os.walk(self.app_dir):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            if lockfile in filenames:
                found.append(dirpath)
        if len(found) != 1:
            raise HelperError("expected exactly one %s in the checkout, found %s"
                              % (lockfile, ", ".join(found) if found else "none"), self.app_dir)
        if not os.path.isfile(os.path.join(found[0], companion)):
            raise HelperError("%s has no %s beside it" % (lockfile, companion), found[0])
        return found[0]

    def load_tools(self):
        path = os.path.join(self.ctx.script_dir, TOOLS_CONF)
        values = parse_kv_file(path)
        absent, empty = check_keys(values, may_be_empty=("INSTALLABLE_TOOLS", "PLAYWRIGHT_BROWSERS"))
        if absent or empty:
            raise HelperError("tools.conf is incomplete (%s)" % describe_gaps(absent, empty), path)
        self.installable = words(values["INSTALLABLE_TOOLS"])
        self.browsers = words(values["PLAYWRIGHT_BROWSERS"])
        for name in self.installable:
            if name not in KNOWN_INSTALLABLE:
                raise HelperError("INSTALLABLE_TOOLS lists %r but this helper can only install: %s"
                                  % (name, ", ".join(KNOWN_INSTALLABLE)), path)
        for name in self.browsers:
            if name not in BROWSERS:
                raise HelperError("PLAYWRIGHT_BROWSERS lists %r; allowed: %s"
                                  % (name, ", ".join(BROWSERS)), path)
        if "dbmate" in self.installable:
            absent, empty = check_keys(values, required=("DBMATE_VERSION", "DBMATE_SHA256_ARM64"))
            if absent or empty:
                raise HelperError("tools.conf is incomplete for dbmate (%s)"
                                  % describe_gaps(absent, empty), path)
        self.tools = values

    # ---------------------------------------------------------- 2. tools
    def stage_tools(self):
        if not self.installable:
            evidence("installable_tools", "none listed in tools.conf")
            return
        evidence("installable_tools", " ".join(self.installable))
        for name in self.installable:
            getattr(self, "install_" + name)()

    def tool_version(self, path):
        result = run_cmd([path, "--version"], timeout=30)
        return first_version(result.stdout + " " + result.stderr)

    def install_dbmate(self):
        wanted = self.tools["DBMATE_VERSION"]
        expected_sha = self.tools["DBMATE_SHA256_ARM64"].lower()
        current = shutil.which("dbmate")
        have = self.tool_version(current) if current else None
        evidence("dbmate_pinned_version", wanted)
        evidence("dbmate_found", "%s (%s)" % (current, have or "version unknown") if current else "no")
        if have == wanted:
            evidence("dbmate", "already at the pinned version; nothing to install")
            return

        if platform.machine() not in DBMATE_MACHINES:
            raise HelperError("tools.conf pins only the linux-arm64 dbmate build but this machine "
                              "is %r" % platform.machine(), "platform.machine()")
        url = DBMATE_URL % wanted
        tmpdir = tempfile.mkdtemp(prefix="autodeploy-dbmate-")
        try:
            tmp = os.path.join(tmpdir, "dbmate")
            digest = hashlib.sha256()
            info("downloading %s" % url)
            try:
                with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT) as response, \
                        open(tmp, "wb") as handle:
                    for chunk in iter(lambda: response.read(1 << 20), b""):
                        digest.update(chunk)
                        handle.write(chunk)
            except (urllib.error.URLError, OSError) as exc:
                raise HelperError("cannot download dbmate: %s" % exc, url)
            evidence("dbmate_download_sha256", digest.hexdigest())
            if digest.hexdigest() != expected_sha:
                raise HelperError("downloaded dbmate does not match the sha256 pinned in tools.conf "
                                  "(expected %s)" % expected_sha, url)
            os.chmod(tmp, 0o755)
            downloaded = self.tool_version(tmp)
            if downloaded != wanted:
                raise HelperError("the downloaded dbmate reports version %s, expected %s"
                                  % (downloaded, wanted), tmp + " --version")

            result = run_cmd(self.privilege + ["install", "-m", "0755", tmp, DBMATE_INSTALL_PATH],
                             timeout=60)
            if not result.ok:
                raise HelperError("cannot install dbmate to %s: %s (the sudoers entry must allow "
                                  "`install`)" % (DBMATE_INSTALL_PATH, result.first_error_line()),
                                  result.command())
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        now = shutil.which("dbmate")
        now_version = self.tool_version(now) if now else None
        if now != DBMATE_INSTALL_PATH or now_version != wanted:
            raise HelperError("after installing, `dbmate` on PATH is %s (%s) instead of %s %s; "
                              "another copy earlier on PATH is shadowing it"
                              % (now, now_version, DBMATE_INSTALL_PATH, wanted), "PATH")
        evidence("dbmate", "installed %s at %s" % (now_version, now))

    # ----------------------------------------------------- 3. python venv
    def uv(self, args, **kwargs):
        return run_cmd([self.uv_path] + args, cwd=self.project_dir,
                       extra_env={"UV_PROJECT_ENVIRONMENT": self.venv_dir}, **kwargs)

    def list_packages(self, tolerant=False):
        """{normalised name: version} installed in the venv; {} when there is no venv."""
        if not os.path.exists(self.venv_python):
            return {}
        result = run_cmd([self.uv_path, "pip", "list", "--format", "json",
                          "--python", self.venv_python], timeout=120)
        try:
            if not result.ok:
                raise ValueError(result.first_error_line())
            return {norm(p["name"]): p["version"] for p in json.loads(result.stdout)}
        except (ValueError, KeyError, TypeError) as exc:
            if tolerant:
                warn("cannot list the existing venv's packages (%s); treating it as empty" % exc)
                return {}
            raise HelperError("cannot list the venv's packages: %s" % exc, result.command())

    def locked_names(self):
        """Package names uv.lock wants for the configured extras/groups (project excluded)."""
        groups = self.sync_args[self.sync_args.index(PYTHON) + 1:]
        result = self.uv(["export", "--locked", "--no-hashes", "--no-emit-project"] + groups,
                         timeout=120)
        if not result.ok:
            raise HelperError("uv export failed: %s" % result.first_error_line(), result.command())
        names = set()
        for raw in result.stdout.splitlines():
            line = raw.strip()
            if line and not line.startswith(("#", "-")):
                names.add(norm(re.split(r"[=<>!~;\s\[]", line, maxsplit=1)[0]))
        return names

    def stage_python(self):
        self.uv_path = shutil.which("uv")
        if self.uv_path is None:
            raise HelperError("uv is not on PATH", "PATH=%s" % os.environ.get("PATH", ""))
        version = run_cmd([self.uv_path, "--version"], timeout=30)
        evidence("uv_version", "%s (%s)" % (version.stdout.strip() or "unknown", self.uv_path))
        evidence("uv_lock_sha256", sha256_file(self.uv_lock))

        before = self.list_packages(tolerant=True)
        evidence("venv_before", "%d packages" % len(before) if before else "empty or did not exist")

        info("uv %s" % " ".join(self.sync_args))
        first = self.uv(self.sync_args, timeout=LONG_TIMEOUT, echo_prefix="    ", heartbeat=30)
        if not first.ok:
            raise HelperError("uv sync failed: %s" % reason_from_output(first),
                              "UV_PROJECT_ENVIRONMENT=%s %s" % (self.venv_dir, first.command()))

        after = self.list_packages()
        added = sorted(set(after) - set(before))
        removed = sorted(set(before) - set(after))
        changed = sorted(n for n in after if n in before and before[n] != after[n])
        evidence("packages_added", "%d %s" % (len(added), sample("%s %s" % (n, after[n]) for n in added)))
        evidence("packages_removed", "%d %s" % (len(removed), sample("%s %s" % (n, before[n]) for n in removed)))
        evidence("packages_changed", "%d %s" % (len(changed), sample(
            "%s %s -> %s" % (n, before[n], after[n]) for n in changed)))
        evidence("venv_after", "%d packages" % len(after))

        second = self.uv(self.sync_args, timeout=LONG_TIMEOUT)
        if not second.ok:
            raise HelperError("second uv sync failed: %s" % second.first_error_line(), second.command())
        again = self.list_packages()
        if again != after:
            drift = sorted(set(again) ^ set(after)) or sorted(n for n in after if after[n] != again.get(n))
            raise HelperError("a second uv sync changed the venv, so the first did not converge: %s"
                              % sample(drift), second.command())
        evidence("second_sync", "no changes (exit 0, package list identical)")

        check = run_cmd([self.uv_path, "pip", "check", "--python", self.venv_python], timeout=120)
        if not check.ok:
            for line in check.tail(15):
                info("    " + line)
            raise HelperError("uv pip check found problems: %s" % check.first_error_line(), check.command())
        evidence("pip_check", ([l.strip() for l in (check.stderr + "\n" + check.stdout).splitlines()
                                if l.strip()] or ["ok"])[-1])

        with open(self.pyproject, "rb") as handle:
            project = norm(tomllib.load(handle)["project"]["name"])
        extraneous = sorted(set(after) - self.locked_names() - {project})
        if extraneous:
            raise HelperError("packages in the venv that uv.lock does not call for: %s"
                              % sample(extraneous), "uv export vs uv pip list")
        evidence("extraneous_packages_after_sync", 0)

        python = run_cmd([self.venv_python, "--version"], timeout=30)
        evidence("venv_python", "%s (%s)" % (python.stdout.strip() or python.stderr.strip(),
                                              self.venv_python))

    # ------------------------------------------------------------ 4. node
    def stage_node(self):
        node, npm = shutil.which("node"), shutil.which("npm")
        if node is None or npm is None:
            raise HelperError("node and npm must both be on PATH", "PATH=%s" % os.environ.get("PATH", ""))
        evidence("node_version", run_cmd([node, "--version"], timeout=30).stdout.strip())
        evidence("npm_version", run_cmd([npm, "--version"], timeout=30).stdout.strip())
        evidence("package_lock_sha256", sha256_file(self.npm_lock))

        info("npm ci --no-audit --no-fund --include=dev  (in %s)" % self.frontend_dir)
        result = run_cmd([npm, "ci", "--no-audit", "--no-fund", "--include=dev"],
                         cwd=self.frontend_dir, timeout=LONG_TIMEOUT, echo_prefix="    ", heartbeat=30)
        if not result.ok:
            raise HelperError("npm ci failed: %s" % reason_from_output(result),
                              "cd %s && %s" % (self.frontend_dir, result.command()))
        if "EBADENGINE" in result.stdout:
            warn("npm reported EBADENGINE: this Node is older than a package requires "
                 "(see the output above); tests or the build may fail later")
        modules = os.path.join(self.frontend_dir, "node_modules")
        if not os.path.isdir(modules):
            raise HelperError("npm ci exited 0 but node_modules does not exist", modules)
        evidence("npm_ci", "ok, %s" % modules)

        info("npm run build  (in %s)" % self.frontend_dir)
        build_result = run_cmd([npm, "run", "build"],
                               cwd=self.frontend_dir, timeout=LONG_TIMEOUT, echo_prefix="    ", heartbeat=30)
        if not build_result.ok:
            raise HelperError("npm run build failed: %s" % reason_from_output(build_result),
                              "cd %s && %s" % (self.frontend_dir, build_result.command()))
        next_dir = os.path.join(self.frontend_dir, ".next")
        if not os.path.isdir(next_dir):
            raise HelperError("npm run build exited 0 but .next does not exist", next_dir)
        evidence("npm_build", "ok, %s" % next_dir)

    # ------------------------------------------------------- 5. playwright
    def stage_playwright(self):
        if not self.browsers:
            evidence("playwright_browsers", "none listed in tools.conf")
            return
        py = self.venv_python
        version = run_cmd([py, "-m", "playwright", "--version"], timeout=60)
        if not version.ok:
            raise HelperError("playwright is not usable in the venv: %s" % version.first_error_line(),
                              version.command())
        evidence("playwright_version", version.stdout.strip())

        info("playwright install %s" % " ".join(self.browsers))
        result = run_cmd([py, "-m", "playwright", "install"] + self.browsers,
                         timeout=LONG_TIMEOUT, echo_prefix="    ", heartbeat=30)
        if not result.ok:
            raise HelperError("playwright install failed: %s" % reason_from_output(result),
                              result.command())

        for name in self.browsers:
            launch = run_cmd([py, "-c", LAUNCH_CHECK, name], timeout=120)
            if not launch.ok:
                for line in launch.tail(12):
                    info("    " + line)
                raise HelperError("%s was installed but cannot be launched: %s (missing system "
                                  "libraries are the usual cause)" % (name, launch.first_error_line()),
                                  launch.command())
            evidence("browser_" + name, "launches, version %s" % launch.stdout.strip().splitlines()[-1])


def main():
    args = parse_args(sys.argv[1:])
    Dependencies(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
