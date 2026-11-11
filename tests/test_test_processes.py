"""Tests for quiescing and restoring production writers (D3, D17).

Covers test-up.sh: after the durable state file is written, stop the PRD
FastAPI unit, the agent and hardware-health timers, and production PM2
process pifitness-next; verify every writer is stopped before the test DB is
cloned; start the test API and test frontend only after PRD is quiescent.
Covers test-down.sh: restore exactly the writers recorded active before
test-up (FastAPI, both timers, PM2 pifitness-next); never start what was
inactive. No port-based killing or broad pkill anywhere in either script.

This module is also the single home of the shared test fixtures used by
test_test_up.py and test_test_deploy_scripts.py:

* STUB_BODIES / write_stubs: stubbed systemctl/pm2/sudo/pg_dump/psql/dropdb/
  createdb/pg_restore/curl/python3/npm/node. Every stub succeeds by default
  and supports opt-in failure injection through environment variables passed
  with run_test_up(..., states={...}):

      PG_DUMP_FAIL=1          pg_dump exits 1 (no dump file written)
      TEST_DB_EXISTS=1        psql reports that the TEST DB already exists
      TEST_DB_MARKER=<value>  marker returned by current_setting(...)
      PSQL_CREATE_FAIL=1      psql CREATE DATABASE ... TEMPLATE exits 1
      PG_RESTORE_FAIL=1       pg_restore (restore, not --list) exits 1
      SYSTEMCTL_START_FAIL=1  systemctl start exits 1
      PM2_START_FAIL=1        pm2 start exits 1
      PM2_RESTART_FAIL=1      pm2 restart exits 1
      PIP_FAIL=1 / NPM_FAIL=1 venv pip / npm exit 1
      CURL_FAIL=1             stub curl exits 1

* write_runtime_stubs / running_test_stack: stand-ins that make the Step 9
  verification of a fully started test stack pass (do not use them in
  failure tests; they have no failure injection).
* sandbox / seeded_sandbox / advance_test_ref / PROVISIONING_CHECKOUT_FILES:
  the temp origin/PRD pair. seeded_sandbox seeds test-ref with the files
  that checkout provisioning legitimately needs.

The Python stub creates the expected POSIX venv/bin/python and venv/bin/pip
executables so checkout provisioning is deterministic on Windows Git Bash.
Stop/start order and exact prior-state restoration are proved behaviorally
with mocked processes. Git drives a temp origin/PRD pair so test-up can fetch
a ref and resolve a full SHA; HOME is redirected to a temp dir so the state
file and test env file never leave the temp sandbox.
"""

import contextlib
import http.server
import os
import shutil
import subprocess
import threading

import pytest

# The test-down runner and state writer live in test_test_down.py (it is a
# strict superset of what this module used to duplicate); re-used here.
from tests.test_test_down import run_test_down, write_test_state

SCRIPT_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "deploy", "scripts")
)
TEST_UP = os.path.join(SCRIPT_DIR, "test-up.sh")
TEST_DOWN = os.path.join(SCRIPT_DIR, "test-down.sh")
BASH = "C:\\Program Files\\Git\\bin\\bash.exe"
GIT = shutil.which("git")

# test-down.sh, run as the recovery path from test-up.sh, probes this URL. It
# points at a closed loopback port so recovery never touches a real service.
UNREACHABLE_HEALTH_URL = "http://127.0.0.1:1/api/health"

pytestmark = pytest.mark.unit


def read_script(name):
    with open(os.path.join(SCRIPT_DIR, name), "r", encoding="utf-8") as fh:
        return fh.read()


def script_text(name):
    return read_script(name)


def bash_available():
    return os.path.isfile(BASH) and GIT is not None


requires_bash = pytest.mark.skipif(
    not bash_available(), reason="Git bash and git are required"
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

_STUB_PYTHON3 = r"""#!/usr/bin/env bash
echo "PYTHON3 $*" >> "$STUB_LOG"
if [ "$1" = "-m" ] && [ "$2" = "venv" ]; then
  # Resolve the already-created checkout through Git Bash itself. This avoids
  # mixing native C:/ paths and POSIX /c/ paths when creating venv/bin files.
  if [ -n "${TEST_DIR:-}" ] && [ -d "$TEST_DIR" ]; then
    test_dir_posix="$(cd "$TEST_DIR" && pwd -P)"
    venv_dir="$test_dir_posix/venv"
  else
    venv_arg="${!#}"
    if command -v cygpath >/dev/null 2>&1; then
      venv_dir="$(cygpath -u "$venv_arg")"
    else
      venv_dir="$venv_arg"
    fi
  fi
  mkdir -p "$venv_dir/bin"
  cat > "$venv_dir/bin/python" <<'PYTHON_EOF'
#!/usr/bin/env bash
echo "VENV_PYTHON $*" >> "$STUB_LOG"
echo "${TEST_DIR}/backend/pifitness/__init__.py"
exit 0
PYTHON_EOF
  chmod +x "$venv_dir/bin/python"
  cat > "$venv_dir/bin/pip" <<'PIP_EOF'
#!/usr/bin/env bash
echo "PIP $*" >> "$STUB_LOG"
if [ "${PIP_FAIL:-0}" = "1" ]; then exit 1; fi
exit 0
PIP_EOF
  chmod +x "$venv_dir/bin/pip"
fi
exit 0
"""

_STUB_NPM = r"""#!/usr/bin/env bash
echo "NPM $*" >> "$STUB_LOG"
if [ "${NPM_FAIL:-0}" = "1" ]; then exit 1; fi
exit 0
"""

_STUB_NODE = r"""#!/usr/bin/env bash
echo "NODE $*" >> "$STUB_LOG"
exit 0
"""

_STUB_SUDO = r"""#!/usr/bin/env bash
echo "SUDO $*" >> "$STUB_LOG"
"$@"
"""

_STUB_SYSTEMCTL = r"""#!/usr/bin/env bash
echo "SYSTEMCTL $*" >> "$STUB_LOG"
case "$1" in
  is-active)
    case "$2" in
      pifitness-fastapi.service) echo "${FASTAPI_STATE:-inactive}" ;;
      pifitness_agent.timer) echo "${AGENT_TIMER_STATE:-inactive}" ;;
      pifitness-hardware-health.timer) echo "${HARDWARE_HEALTH_TIMER_STATE:-inactive}" ;;
      *) echo inactive ;;
    esac ;;
  start)
    if [ "${SYSTEMCTL_START_FAIL:-0}" = "1" ]; then exit 1; fi ;;
  *) ;;
esac
exit 0
"""

_STUB_PM2 = r"""#!/usr/bin/env bash
echo "PM2 $*" >> "$STUB_LOG"
case "$1" in
  describe)
    if [ "$2" = "pifitness-next" ]; then
      if [ "${PM2_ONLINE:-0}" = "1" ]; then echo "status: online"; else echo "status: stopped"; fi
    fi
    exit 0 ;;
  start)
    if [ "${PM2_START_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0 ;;
  restart)
    if [ "${PM2_RESTART_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0 ;;
  *)
    exit 0 ;;
esac
"""

_STUB_PG_DUMP = r"""#!/usr/bin/env bash
echo "PG_DUMP $*" >> "$STUB_LOG"
dump_file=""
while [ $# -gt 0 ]; do
  case "$1" in
    -f) dump_file="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ "${PG_DUMP_FAIL:-0}" = "1" ]; then
  echo "pg_dump: simulated failure" >&2
  exit 1
fi
if [ -n "$dump_file" ]; then
  printf 'dump-data\n' > "$dump_file"
fi
exit 0
"""

_STUB_PSQL = r"""#!/usr/bin/env bash
echo "PSQL $*" >> "$STUB_LOG"
case "$*" in
  *pg_database*)
    # TEST DB existence probe. Absent by default.
    echo "${TEST_DB_EXISTS:-0}"
    ;;
  *current_setting*)
    echo "${TEST_DB_MARKER-test}"
    ;;
  *TEMPLATE*)
    if [ "${PSQL_CREATE_FAIL:-0}" = "1" ]; then
      echo "psql: simulated CREATE DATABASE failure" >&2
      exit 1
    fi
    ;;
esac
exit 0
"""

_STUB_PG_RESTORE = r"""#!/usr/bin/env bash
echo "PG_RESTORE $*" >> "$STUB_LOG"
case "$*" in
  *--list*)
    # Dump archive validation (test-up Step 2) always succeeds so test-up
    # reaches the DB restore. A restore failure is injected only into the
    # actual restore below.
    ;;
  *)
    if [ "${PG_RESTORE_FAIL:-0}" = "1" ]; then
      echo "pg_restore: simulated failure" >&2
      exit 1
    fi
    ;;
esac
exit 0
"""

_STUB_DROPDB = r"""#!/usr/bin/env bash
echo "DROP_DB $*" >> "$STUB_LOG"
exit 0
"""

_STUB_CREATEDB = r"""#!/usr/bin/env bash
echo "CREATE_DB $*" >> "$STUB_LOG"
exit 0
"""

_STUB_CURL = r"""#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
if [ "${CURL_FAIL:-0}" = "1" ]; then exit 1; fi
echo ok
exit 0
"""

_STUB_RM = r"""#!/usr/bin/env bash
echo "RM $*" >> "$STUB_LOG"
exit 0
"""

STUB_BODIES = {
    "python3": _STUB_PYTHON3,
    "npm": _STUB_NPM,
    "node": _STUB_NODE,
    "sudo": _STUB_SUDO,
    "systemctl": _STUB_SYSTEMCTL,
    "pm2": _STUB_PM2,
    "pg_dump": _STUB_PG_DUMP,
    "psql": _STUB_PSQL,
    "dropdb": _STUB_DROPDB,
    "createdb": _STUB_CREATEDB,
    "pg_restore": _STUB_PG_RESTORE,
    "curl": _STUB_CURL,
    "rm": _STUB_RM,
}


def _write_executable(path, body):
    # Force LF endings: a CRLF shebang or case pattern breaks bash.
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(body)
    os.chmod(path, 0o755)


def write_stubs(bin_dir):
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    for name, body in STUB_BODIES.items():
        _write_executable(os.path.join(bin_dir, name), body)


# Runtime stubs: make Step 9 (verify the complete test stack) pass. They have
# no failure injection, so never install them in a failure-injection test.
_RUNTIME_SYSTEMCTL_STUB = r"""#!/usr/bin/env bash
echo "SYSTEMCTL $*" >> "$STUB_LOG"
case "$1" in
  is-active)
    case "$2" in
      pifitness-fastapi-test.service) echo "active" ;;
      *) echo inactive ;;
    esac ;;
  start|stop|restart) ;;
  *) ;;
esac
exit 0
"""

_RUNTIME_PM2_STUB = r"""#!/usr/bin/env bash
echo "PM2 $*" >> "$STUB_LOG"
case "$1" in
  describe)
    if [ "$2" = "pifitness-next-test" ]; then echo "status: online"; fi
    exit 0 ;;
  start|stop|restart|delete) exit 0 ;;
  *) exit 0 ;;
esac
"""

_RUNTIME_CURL_STUB = r"""#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
echo '{"status":"ok","database":"ok"}'
exit 0
"""


def write_runtime_stubs(bin_dir):
    """Override systemctl/pm2/curl stubs so the test stack verification passes."""
    bin_dir = str(bin_dir)
    for name, body in (("systemctl", _RUNTIME_SYSTEMCTL_STUB),
                       ("pm2", _RUNTIME_PM2_STUB),
                       ("curl", _RUNTIME_CURL_STUB)):
        _write_executable(os.path.join(bin_dir, name), body)


class _OkHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP server standing in for the test stack.

    Returns a JSON body with a `status` field so test-up's FastAPI health
    probe (which greps for `"status"`) succeeds, and 200 for the Next.js and
    HTTPS probes. Git bash on Windows resolves real /usr/bin/curl.exe rather
    than any stub on PATH, so these real local servers let real curl succeed
    without touching the network.
    """

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok","database":"ok"}\n')

    def log_message(self, *args):
        pass


def start_local_servers(ports):
    """Start throwaway HTTP servers on the given loopback ports (use 0 for a
    free port).

    Returns (servers, urls) parallel to `ports`. Callers must shut servers
    down after use. `test-up.sh` hardcodes 127.0.0.1:8000 and 127.0.0.1:3000,
    so those ports are bound explicitly.
    """
    http.server.ThreadingHTTPServer.allow_reuse_address = True
    servers = []
    urls = []
    for port in ports:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", port), _OkHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append(server)
        urls.append("http://127.0.0.1:{}".format(server.server_address[1]))
    return servers, urls


@contextlib.contextmanager
def running_test_stack():
    """Serve the loopback endpoints Step 9 probes (8000, 3000 and a free port
    standing in for the HTTPS hostname). Yields the HTTPS stand-in URL; pass
    it to run_test_up as states={"HTTPS_HOSTNAME": url}."""
    servers, urls = start_local_servers([8000, 3000, 0])
    try:
        yield urls[2]
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()


# ---------------------------------------------------------------------------
# Git fixtures
# ---------------------------------------------------------------------------

# Minimum content a test-ref checkout needs for the current provisioning
# step of test-up.sh: backend/requirements.txt (pip install -r),
# backend/ (pip install -e) and frontend/pifitness/ with its lockfile
# (cd + npm ci). The directory frontend/pifitness exists because the
# lockfile lives there.
PROVISIONING_CHECKOUT_FILES = {
    "backend/requirements.txt": b"",
    "frontend/pifitness/package-lock.json": b"{}\n",
}


def make_repo(path):
    subprocess.run([GIT, "init", path], check=True, capture_output=True)
    subprocess.run(
        [GIT, "-C", path, "config", "user.email", "test@test"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [GIT, "-C", path, "config", "user.name", "Test"],
        check=True,
        capture_output=True,
    )


def make_bare_commit(bare_repo, name, content, message, ref):
    """Create a commit directly in a bare repo and point `ref` at it.

    Uses plumbing commands (hash-object, mktree, commit-tree, update-ref) so
    no working tree or scratch clone is needed. Avoids git push/fetch between
    clones, which behave inconsistently under the pytest subprocess
    environment on this host.
    """
    blob = subprocess.run(
        [GIT, "-C", bare_repo, "hash-object", "-w", "--stdin"],
        input=content.encode(), check=True, capture_output=True,
    ).stdout.decode().strip()
    tree_input = "100644 blob {0}\t{1}\n".format(blob, name).encode()
    tree = subprocess.run(
        [GIT, "-C", bare_repo, "mktree"],
        input=tree_input, check=True, capture_output=True,
    ).stdout.decode().strip()
    commit = subprocess.run(
        [GIT, "-C", bare_repo, "commit-tree", tree, "-m", message],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(
        [GIT, "-C", bare_repo, "update-ref", "refs/heads/" + ref, commit],
        check=True, capture_output=True,
    )


def advance_test_ref(origin, files, message):
    """Commit files onto the origin bare repo's test-ref branch using plumbing
    commands (bare repo has no worktree). test-up fetches that ref from origin,
    resolves its full SHA, and checks it out into the test checkout, so the
    files end up where checkout provisioning expects them."""
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@test",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@test",
        }
    )
    index_file = os.path.join(os.path.dirname(origin), "test-ref.index")
    if os.path.exists(index_file):
        os.remove(index_file)
    env["GIT_INDEX_FILE"] = index_file
    current_tree = subprocess.run(
        [GIT, "-C", origin, "rev-parse", "test-ref^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [GIT, "-C", origin, "read-tree", current_tree],
        check=True,
        capture_output=True,
        env=env,
    )
    parent = subprocess.run(
        [GIT, "-C", origin, "rev-parse", "test-ref"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    for rel, content in files.items():
        blob = subprocess.run(
            [GIT, "-C", origin, "hash-object", "-w", "--stdin"],
            input=content,
            check=True,
            capture_output=True,
        ).stdout.decode().strip()
        subprocess.run(
            [GIT, "-C", origin, "update-index", "--add", "--cacheinfo",
             "100644,%s,%s" % (blob, rel)],
            check=True,
            capture_output=True,
            env=env,
        )
    tree = subprocess.run(
        [GIT, "-C", origin, "write-tree"], check=True, capture_output=True, env=env
    ).stdout.decode().strip()
    commit = subprocess.run(
        [GIT, "-C", origin, "commit-tree", tree, "-p", parent, "-m", message],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()
    subprocess.run(
        [GIT, "-C", origin, "update-ref", "refs/heads/test-ref", commit],
        check=True,
        capture_output=True,
    )


def sandbox(tmp_path):
    """Isolated sandbox: bare origin with a remote-only test-ref branch, a
    cloned PRD repo, a temp HOME holding a valid test env file, and a stub
    bin dir placed first on PATH.

    test-ref holds only ref.txt here; use seeded_sandbox() for tests that run
    test-up past checkout provisioning."""
    origin = os.path.join(str(tmp_path), "origin.git")
    subprocess.run([GIT, "init", "--bare", origin], check=True, capture_output=True)

    # Seed origin directly with plumbing commands: a main commit and a
    # remote-only test-ref branch that test-up.sh fetches.
    make_bare_commit(origin, "README.md", "main\n", "initial on main", "main")
    make_bare_commit(origin, "ref.txt", "test-ref\n", "add ref branch file", "test-ref")
    subprocess.run(
        [GIT, "-C", origin, "symbolic-ref", "HEAD", "refs/heads/main"],
        check=True, capture_output=True,
    )

    prd = os.path.join(str(tmp_path), "PiFitness")
    subprocess.run([GIT, "clone", origin, prd], check=True, capture_output=True)
    test_dir = os.path.join(str(tmp_path), "PiFitness-test")
    home = os.path.join(str(tmp_path), "home")
    config_dir = os.path.join(home, ".config", "pifitness")
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, "test.env"), "w", encoding="utf-8") as fh:
        fh.write(
            "APP_ENV=test\n"
            "PG_DB=pifitness_test\n"
            "PG_HOST=localhost\n"
            "PG_PORT=15432\n"
            "PG_USER=dev\n"
            "PGSSLMODE=disable\n"
            "BOX=local\n"
            "SPOTIFY_ENABLED=false\n"
            "GARMIN_ENABLED=false\n"
            "PIRATE_ENABLED=false\n"
        )
    bin_dir = os.path.join(str(tmp_path), "bin")
    return prd, test_dir, home, bin_dir


def seeded_sandbox(tmp_path):
    """sandbox() plus a test-ref that carries the provisioning files, plus the
    default (success-capable, failure-injectable) stubs written to bin_dir."""
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(
        origin, PROVISIONING_CHECKOUT_FILES, "seed provisioning checkout on test-ref"
    )
    write_stubs(bin_dir)
    return prd, test_dir, home, bin_dir


def run_test_up(prd, test_dir, home, bin_dir, ref="test-ref", states=None):
    env = os.environ.copy()
    env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    env["HOME"] = home
    env["PRD_DIR"] = prd
    env["TEST_DIR"] = test_dir
    env["STUB_LOG"] = os.path.join(home, "stub.log")
    env["HEALTH_URL"] = UNREACHABLE_HEALTH_URL
    if states:
        env.update(states)
    stub_log_path = os.path.join(home, "stub.log")
    if os.path.exists(stub_log_path):
        os.remove(stub_log_path)
    proc = subprocess.run(
        [BASH, TEST_UP, ref],
        env=env,
        capture_output=True,
        text=True,
    )
    lines = []
    if os.path.exists(stub_log_path):
        with open(stub_log_path, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    return proc, lines


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_bash
def test_test_up_stops_writers_in_order_and_verifies_before_clone(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    # A successful run goes through Step 9, so the stack must look healthy.
    write_runtime_stubs(bin_dir)
    with running_test_stack() as https_url:
        proc, log = run_test_up(
            prd, test_dir, home, bin_dir, states={"HTTPS_HOSTNAME": https_url}
        )
    assert proc.returncode == 0, proc.stderr
    stops = [
        "SYSTEMCTL stop pifitness-fastapi.service",
        "SYSTEMCTL stop pifitness_agent.timer",
        "SYSTEMCTL stop pifitness-hardware-health.timer",
        "PM2 stop pifitness-next",
    ]
    stop_entries = [entry for entry in log if entry in stops]
    assert stop_entries == stops, log
    starts = [entry for entry in log if entry.startswith(("SYSTEMCTL start", "PM2 start"))]
    assert starts, "test API/frontend must start only after PRD is quiesced: %r" % log
    assert max(log.index(entry) for entry in stops) < min(log.index(entry) for entry in starts)
    probes = [
        entry
        for entry in log
        if entry.startswith("SYSTEMCTL is-active") or entry == "PM2 describe pifitness-next"
    ]
    assert probes, log
    last_stop = max(log.index(entry) for entry in stops)

    # list.index returns the first occurrence; probes appear both before
    # (state capture) and after (quiesce verification) the stops, so use the
    # last occurrence of each probe.
    def last_index(lst, value):
        return len(lst) - 1 - lst[::-1].index(value)

    assert any(last_index(log, entry) > last_stop for entry in probes), (
        "quiesce verification probe must run after all PRD writers are stopped: %r" % log
    )
    state = os.path.join(home, ".pifitness-test-state")
    assert os.path.isfile(state)
    with open(state, "r", encoding="utf-8") as fh:
        state_text = fh.read()
    assert "PM2_NEXTPROD_ACTIVE=" in state_text
    assert os.path.isdir(test_dir)


@requires_bash
def test_test_up_refuses_to_proceed_when_a_writer_is_still_active(tmp_path):
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"FASTAPI_STATE": "active"}
    )
    assert proc.returncode != 0
    assert "still active" in proc.stderr
    assert not os.path.isdir(test_dir), "test DB clone must not run while a PRD writer is active"
    starts = [entry for entry in log if entry.startswith(("SYSTEMCTL start", "PM2 start"))]
    assert not starts, "test units must not start before PRD is quiesced: %r" % log


@requires_bash
def test_test_down_restores_only_previously_active_writers(tmp_path):
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    os.makedirs(test_dir, exist_ok=True)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="inactive",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    proc, log = run_test_down(home, prd, test_dir, bin_dir)
    assert proc.returncode == 0, proc.stderr
    starts = [entry for entry in log if entry.startswith("SYSTEMCTL start")]
    assert "SYSTEMCTL start pifitness-fastapi.service" in starts
    assert "SYSTEMCTL start pifitness_agent.timer" not in log
    assert "SYSTEMCTL start pifitness-hardware-health.timer" in starts
    assert "PM2 restart pifitness-next" in log
    assert "Step 5: remove test checkout" in proc.stdout
    assert "Step 6: remove state file (cleanup complete)" in proc.stdout
    assert "test-down complete." in proc.stdout


@requires_bash
def test_test_down_leaves_inactive_writers_stopped(tmp_path):
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    os.makedirs(test_dir, exist_ok=True)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="inactive",
        AGENT_TIMER_ACTIVE="inactive",
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(home, prd, test_dir, bin_dir)
    assert proc.returncode == 0, proc.stderr
    assert not any(entry.startswith("SYSTEMCTL start") for entry in log)
    assert not any(entry.startswith("PM2 restart") for entry in log)
    assert "Step 5: remove test checkout" in proc.stdout
    assert "Step 6: remove state file (cleanup complete)" in proc.stdout
    assert "test-down complete." in proc.stdout


def test_test_up_stop_order_in_script():
    text = script_text("test-up.sh")
    fastapi = text.index("systemctl stop pifitness-fastapi.service")
    agent = text.index("systemctl stop pifitness_agent.timer")
    hardware = text.index("systemctl stop pifitness-hardware-health.timer")
    pm2 = text.index("pm2 stop pifitness-next")
    assert fastapi < agent < hardware < pm2
    assert pm2 < text.index("rm -rf \"$TEST_DIR\"")
    assert pm2 < text.index("Step 8: start test units")


def test_test_up_verifies_writers_quiesced_before_db_clone():
    text = script_text("test-up.sh")
    last_stop = max(
        text.index("systemctl stop pifitness-fastapi.service"),
        text.index("systemctl stop pifitness_agent.timer"),
        text.index("systemctl stop pifitness-hardware-health.timer"),
        text.index("pm2 stop pifitness-next"),
    )
    verify = text.index("verify PRD writers are quiesced before cloning the database")
    step4 = text.index("Step 4: create test DB")
    clone = text.index('git clone "$PRD_DIR" "$TEST_DIR"')
    assert last_stop < verify < step4 < clone
    block = text[verify:step4]
    assert "still active" in block
    assert "exit 1" in block
    assert "pm2_next_state" in block


def test_test_down_restore_gated_on_recorded_prior_state():
    text = script_text("test-down.sh")
    load = text.index('. "$STATE_FILE"')
    assert load < text.index("systemctl start pifitness-fastapi.service")
    assert load < text.index("systemctl start pifitness_agent.timer")
    assert load < text.index("systemctl start pifitness-hardware-health.timer")
    assert load < text.index("pm2 restart pifitness-next")
    assert "PM2_NEXTPROD_ACTIVE" in text
    assert "was not active before test-up; leaving stopped" in text
    assert "was not online before test-up; leaving stopped" in text


def test_integrations_disabled_and_timers_stopped_during_test_window():
    text = script_text("test-up.sh")
    for integration in ("SPOTIFY_ENABLED", "GARMIN_ENABLED", "PIRATE_ENABLED"):
        assert integration in text
    assert "systemctl stop pifitness_agent.timer" in text
    assert "systemctl stop pifitness-hardware-health.timer" in text


def test_scripts_pass_bash_syntax_check():
    if not os.path.isfile(BASH):
        pytest.skip("Git bash is required")
    for script in (TEST_UP, TEST_DOWN):
        proc = subprocess.run([BASH, "-n", script], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr


def test_no_pkill_or_port_based_process_killing():
    for name in ("test-up.sh", "test-down.sh"):
        text = script_text(name)
        assert "pkill" not in text
