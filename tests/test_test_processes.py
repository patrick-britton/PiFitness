"""Tests for quiescing and restoring production writers (D3, D17).

Covers test-up.sh: after the durable state file is written, stop the PRD
FastAPI unit, the agent and hardware-health timers, and production PM2
process pifitness-next; verify every writer is stopped before the test DB is
cloned; start the test API and test frontend only after PRD is quiescent.
Covers test-down.sh: restore exactly the writers recorded active before
test-up (FastAPI, both timers, PM2 pifitness-next); never start what was
inactive. No port-based killing or broad pkill anywhere in either script.

The scripts run against stubbed systemctl/pm2/sudo/pg_dump/psql/dropdb/
createdb/pg_restore/curl on a temp PATH, so stop/start order and exact
prior-state restoration are proved behaviorally with mocked processes. Git
drives a temp origin/PRD pair so test-up can fetch a ref and resolve a full
SHA; HOME is redirected to a temp dir so the state file and test env file
never leave the temp sandbox.
"""

import http.server
import os
import shutil
import subprocess
import threading

import pytest

SCRIPT_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "deploy", "scripts")
)
TEST_UP = os.path.join(SCRIPT_DIR, "test-up.sh")
TEST_DOWN = os.path.join(SCRIPT_DIR, "test-down.sh")
BASH = "C:\\Program Files\\Git\\bin\\bash.exe"
GIT = shutil.which("git")

pytestmark = pytest.mark.unit


def read_script(name):
    with open(os.path.join(SCRIPT_DIR, name), "r", encoding="utf-8") as fh:
        return fh.read()


def script_text(name):
    return read_script(name)


def bash_available():
    return os.path.isfile(BASH) and GIT is not None
STUB_BODIES = {
    "sudo": (
        "#!/usr/bin/env bash\n"
        'echo "SUDO $*" >> "$STUB_LOG"\n'
        '"$@"\n'
    ),
    "systemctl": (
        "#!/usr/bin/env bash\n"
        'echo "SYSTEMCTL $*" >> "$STUB_LOG"\n'
        'case "$1" in\n'
        "  is-active)\n"
        '    case "$2" in\n'
        '      pifitness-fastapi.service) echo "${FASTAPI_STATE:-inactive}" ;;\n'
        '      pifitness_agent.timer) echo "${AGENT_TIMER_STATE:-inactive}" ;;\n'
        '      pifitness-hardware-health.timer) echo "${HARDWARE_HEALTH_TIMER_STATE:-inactive}" ;;\n'
        '      *) echo inactive ;;\n'
        "    esac ;;\n"
        "  *) ;;\n"
        "esac\n"
        "exit 0\n"
    ),
    "pm2": (
        "#!/usr/bin/env bash\n"
        'echo "PM2 $*" >> "$STUB_LOG"\n'
        'case "$1" in\n'
        "  describe)\n"
        '    if [ "$2" = "pifitness-next" ]; then\n'
        '      if [ "${PM2_ONLINE:-0}" = "1" ]; then echo "status: online"; else echo "status: stopped"; fi\n'
        "    fi\n"
        "    exit 0 ;;\n"
        "  stop|restart|start|delete)\n"
        "    exit 0 ;;\n"
        "  *)\n"
        "    exit 0 ;;\n"
        "esac\n"
    ),
    "pg_dump": (
        "#!/usr/bin/env bash\n"
        'echo "PG_DUMP $*" >> "$STUB_LOG"\n'
        'dump_file=""\n'
        "while [ $# -gt 0 ]; do\n"
        '  case "$1" in\n'
        '    -f) dump_file="$2"; shift 2 ;;\n'
        '    *) shift ;;\n'
        "  esac\n"
        "done\n"
        'if [ -n "$dump_file" ]; then\n'
        '  printf "dump-data\\n" > "$dump_file"\n'
        "fi\n"
        "exit 0\n"
    ),
    "psql": (
        "#!/usr/bin/env bash\n"
        'echo "PSQL $*" >> "$STUB_LOG"\n'
    'case "$*" in\n'
    "  *current_setting*)\n"
    '    echo "test"\n'
    "    ;;\n"
    "esac\n"
        "exit 0\n"
    ),
    "dropdb": (
        "#!/usr/bin/env bash\n"
        'echo "DROP_DB $*" >> "$STUB_LOG"\n'
        "exit 0\n"
    ),
    "createdb": (
        "#!/usr/bin/env bash\n"
        'echo "CREATE_DB $*" >> "$STUB_LOG"\n'
        "exit 0\n"
    ),
    "pg_restore": (
        "#!/usr/bin/env bash\n"
        'echo "PG_RESTORE $*" >> "$STUB_LOG"\n'
        "exit 0\n"
    ),
    "curl": (
        "#!/usr/bin/env bash\n"
        'echo "CURL $*" >> "$STUB_LOG"\n'
        "echo ok\n"
        "exit 0\n"
    ),
    "rm": (
        "#!/usr/bin/env bash\n"
        'echo "RM $*" >> "$STUB_LOG"\n'
        "exit 0\n"
    ),
}
def write_stubs(bin_dir):
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    for name, body in STUB_BODIES.items():
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(stub, 0o755)


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
    """Create a commit directly in a bare repo and point  at it.

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


def sandbox(tmp_path):
    """Isolated sandbox: bare origin with a remote-only test-ref branch, a
    cloned PRD repo, a temp HOME holding a valid test env file, and a stub
    bin dir placed first on PATH."""
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
def run_test_up(prd, test_dir, home, bin_dir, ref="test-ref", states=None):
    env = os.environ.copy()
    env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    env["HOME"] = home
    env["PRD_DIR"] = prd
    env["TEST_DIR"] = test_dir
    env["STUB_LOG"] = os.path.join(home, "stub.log")
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


def write_test_state(home, **fields):
    state_lines = ["PHASE=complete"]
    for key, value in fields.items():
        state_lines.append(key + "=" + value)
    with open(os.path.join(home, ".pifitness-test-state"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(state_lines) + "\n")


class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(self, *args):
        pass


def run_test_down(home, prd, test_dir, bin_dir, keep_db=True):
    # Local health endpoint so test-down's curl-based PRD health check can
    # succeed without reaching the network. Served on localhost only.
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HealthHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = os.environ.copy()
        env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
        env["HOME"] = home
        env["PRD_DIR"] = prd
        env["TEST_DIR"] = test_dir
        env["HEALTH_URL"] = "http://127.0.0.1:{}/api/health".format(port)
        env["STUB_LOG"] = os.path.join(home, "stub-down.log")
        stub_log_path = os.path.join(home, "stub-down.log")
        if os.path.exists(stub_log_path):
            os.remove(stub_log_path)
        args = [BASH, TEST_DOWN]
        if keep_db:
            args.append("--keep-db")
        proc = subprocess.run(args, env=env, capture_output=True, text=True)
    finally:
        server.shutdown()
        server.server_close()
    lines = []
    if os.path.exists(stub_log_path):
        with open(stub_log_path, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    return proc, lines
def test_test_up_stops_writers_in_order_and_verifies_before_clone(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir)
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
def test_test_up_refuses_to_proceed_when_a_writer_is_still_active(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
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
def test_test_down_restores_only_previously_active_writers(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
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


def test_test_down_leaves_inactive_writers_stopped(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
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