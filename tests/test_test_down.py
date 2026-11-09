"""Failure-injection tests for test-down (D3).

Proves test-down is repeatable and never deletes the TEST DB, the test
checkout, or the state file prematurely: every injected failure stops with a
non-zero exit before cleanup, while the state file, checkout, and TEST DB
remain intact.

The script runs against stubbed sudo/systemctl/pm2/psql/dropdb/rm on a temp
PATH, with a localhost health server for curl, so each failure mode is proved
behaviorally (non-zero exit, no destructive side effect). HOME is redirected
to a temp dir so the state file never leaves the sandbox.
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
TEST_DOWN = os.path.join(SCRIPT_DIR, "test-down.sh")
BASH = r"C:\Program Files\Git\bin\bash.exe"
GIT = shutil.which("git")

pytestmark = pytest.mark.unit


def bash_available():
    return os.path.isfile(BASH) and GIT is not None


STUB_SUDO = """#!/usr/bin/env bash
echo "SUDO $*" >> "$STUB_LOG"
"$@"
"""

STUB_SYSTEMCTL = """#!/usr/bin/env bash
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
  *)
    exit 0 ;;
esac
exit 0
"""

STUB_PM2 = """#!/usr/bin/env bash
echo "PM2 $*" >> "$STUB_LOG"
case "$1" in
  describe)
    if [ "$2" = "pifitness-next" ]; then
      if [ "${PM2_ONLINE:-0}" = "1" ]; then echo "status: online"; else echo "status: stopped"; fi
    fi
    exit 0 ;;
  restart)
    if [ "${PM2_RESTART_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0 ;;
  stop|start|delete)
    exit 0 ;;
  *)
    exit 0 ;;
esac
"""

STUB_PSQL = """#!/usr/bin/env bash
echo "PSQL $*" >> "$STUB_LOG"
case "$*" in
  *pg_database*)
    echo "${TEST_DB_EXISTS:-1}"
    ;;
  *current_setting*)
    echo "${TEST_DB_MARKER-test}"
    ;;
  *)
    exit 0
    ;;
esac
exit 0
"""

STUB_DROP_DB = """#!/usr/bin/env bash
echo "DROP_DB $*" >> "$STUB_LOG"
exit 0
"""

STUB_CURL = """#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
if [ "${CURL_FAIL:-0}" = "1" ]; then exit 1; fi
echo ok
exit 0
"""

STUB_RM = """#!/usr/bin/env bash
echo "RM $*" >> "$STUB_LOG"
exit 0
"""


def write_stubs(bin_dir):
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    stubs = {
        "sudo": STUB_SUDO,
        "systemctl": STUB_SYSTEMCTL,
        "pm2": STUB_PM2,
        "psql": STUB_PSQL,
        "dropdb": STUB_DROP_DB,
        "curl": STUB_CURL,
        "rm": STUB_RM,
    }
    for name, body in stubs.items():
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(stub, 0o755)


def sandbox(tmp_path):
    prd = os.path.join(str(tmp_path), "PiFitness")
    test_dir = os.path.join(str(tmp_path), "PiFitness-test")
    home = os.path.join(str(tmp_path), "home")
    bin_dir = os.path.join(str(tmp_path), "bin")
    os.makedirs(prd, exist_ok=True)
    os.makedirs(test_dir, exist_ok=True)
    os.makedirs(home, exist_ok=True)
    return prd, test_dir, home, bin_dir


def write_test_state(home, **fields):
    state_lines = ["PHASE=complete"]
    for key, value in fields.items():
        state_lines.append(key + "=" + value)
    with open(os.path.join(home, ".pifitness-test-state"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(state_lines) + "\n")


class HealthHandler(http.server.BaseHTTPRequestHandler):
    fail = False

    def do_GET(self):
        self.send_response(500 if self.fail else 200)
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(self, *args):
        pass


def run_test_down(home, prd, test_dir, bin_dir, keep_db=True, mode=None, health_ok=True):
    # Local health endpoint so test-down's curl-based PRD health check is
    # proved against a real localhost server and never reaches the network.
    HealthHandler.fail = not health_ok
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
        env["HTTPS_HOSTNAME"] = env["HEALTH_URL"]
        env["STUB_LOG"] = os.path.join(home, "stub-down.log")
        if mode:
            env.update(mode)
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


def read_script(name):
    with open(os.path.join(SCRIPT_DIR, name), "r", encoding="utf-8") as fh:
        return fh.read()


def test_test_down_succeeds_and_removes_state(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(home, prd, test_dir, bin_dir)
    assert proc.returncode == 0, proc.stderr
    assert "SYSTEMCTL start pifitness-fastapi.service" in log, log
    assert "SYSTEMCTL start pifitness_agent.timer" in log, log
    assert "SYSTEMCTL start pifitness-hardware-health.timer" in log, log
    assert "PM2 restart pifitness-next" in log, log
    assert "test-down complete." in proc.stdout, proc.stdout
    assert not os.path.exists(state), state


def test_test_down_repeatable_after_full_success(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
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
        HARDWARE_HEALTH_TIMER_ACTIVE="inactive",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(home, prd, test_dir, bin_dir, keep_db=False)
    assert proc.returncode == 0, proc.stderr
    assert any(entry.startswith("DROP_DB") for entry in log), log
    assert "test-down complete." in proc.stdout, proc.stdout
    assert not os.path.exists(state), state
    # Second run with no state file: nothing to restore or drop, still safe.
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, keep_db=False, mode={"TEST_DB_EXISTS": "0"}
    )
    assert proc.returncode == 0, proc.stderr
    assert "test-down complete." in proc.stdout, proc.stdout
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not os.path.exists(state), state


def test_test_down_health_failure_retains_everything(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(home, prd, test_dir, bin_dir, health_ok=False)
    assert proc.returncode != 0, proc.stderr
    assert "PRD health check failed" in proc.stderr, proc.stderr
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert os.path.exists(state), state
    assert os.path.isdir(test_dir), test_dir


def test_test_down_pm2_restart_failure_retains_state(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, mode={"PM2_RESTART_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stderr
    assert "PM2 restart pifitness-next" in log, log
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert os.path.exists(state), state


def test_test_down_systemctl_start_failure_retains_state(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, mode={"SYSTEMCTL_START_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stderr
    assert "SYSTEMCTL start pifitness-fastapi.service" in log, log
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert os.path.exists(state), state


def test_test_down_unmarked_db_refused_and_nothing_deleted(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness_test",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    state = os.path.join(home, ".pifitness-test-state")
    proc, log = run_test_down(
        home,
        prd,
        test_dir,
        bin_dir,
        keep_db=False,
        mode={"TEST_DB_MARKER": ""},
    )
    assert proc.returncode != 0, proc.stderr
    assert "refusing to drop" in proc.stderr
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert os.path.exists(state), state
    assert os.path.isdir(test_dir), test_dir


def test_test_down_equal_db_names_stop_before_test_units(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB="pifitness",
        TEST_DB="pifitness",
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    proc, log = run_test_down(home, prd, test_dir, bin_dir)
    assert proc.returncode != 0, proc.stderr
    assert "PRD and TEST DB names must differ" in proc.stderr
    assert not any(entry.startswith("SYSTEMCTL") for entry in log), log
    assert not any(entry.startswith("PM2 restart") for entry in log), log


def test_test_down_critical_failures_are_not_suppressed():
    text = read_script("test-down.sh")
    assert "if ! sudo systemctl start pifitness-fastapi.service; then" in text
    assert "if ! pm2 restart pifitness-next; then" in text
    assert "pm2 restart pifitness-next 2>/dev/null || true" not in text
    assert "command -v curl" in text
    assert 'curl -fsS "$HEALTH_URL"' in text
    assert 'curl -fkS "$HTTPS_HOSTNAME"' in text
    drop = text.index("dropdb")
    state_rm = text.index('rm -f "$STATE_FILE"')
    assert drop < state_rm, "the TEST DB must be dropped before the state file is removed"
