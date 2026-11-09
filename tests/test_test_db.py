"""Fail-closed tests for the PRD dump and TEST database clone (D3, D8).

Covers test-up.sh: PRD_DB != TEST_DB and the configured host/port are checked
before any destructive command; an existing TEST_DB is dropped/recreated only
when its pifitness.env marker is test; a failed DROP is not suppressed; the
dump must exist, be non-empty, and pass pg_restore --list; the TimescaleDB
template-clone path is tried first with a dump/restore (pre/post-restore)
fallback; the pifitness.env=test marker is stamped only after successful
creation/restore and verified by connecting to the TEST database.
Covers test-down.sh: PRD_DB != TEST_DB and the configured host/port are
rechecked against the recorded state before the drop; an unmarked or
otherwise-marked TEST_DB is never dropped.

The scripts run against stubbed systemctl/pm2/sudo/pg_dump/psql/dropdb/
createdb/pg_restore/curl/rm on a temp PATH, so every failure mode is proved
behaviorally (non-zero exit, no destructive side effect). Git drives a temp
origin/PRD pair so test-up can fetch a ref and resolve a full SHA; HOME is
redirected to a temp dir so state and env files never leave the sandbox.
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
  *) ;;
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
  stop|restart|start|delete)
    exit 0 ;;
  *)
    exit 0 ;;
esac
"""

STUB_PG_DUMP = """#!/usr/bin/env bash
echo "PG_DUMP $*" >> "$STUB_LOG"
if [ "${DUMP_FAIL:-0}" = "1" ]; then exit 1; fi
dump_file=""
while [ $# -gt 0 ]; do
  case "$1" in
    -f) dump_file="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ -n "$dump_file" ]; then
  if [ "${DUMP_EMPTY:-0}" = "1" ]; then
    : > "$dump_file"
  else
    echo dump-data > "$dump_file"
  fi
fi
exit 0
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
  *"DROP DATABASE"*)
    if [ "${DROP_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0
    ;;
  *"CREATE DATABASE"*)
    if [ "${CLONE_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0
    ;;
  *"ALTER DATABASE"*)
    exit 0
    ;;
  *timescaledb_pre_restore*|*timescaledb_post_restore*)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"""

STUB_DROP_DB = """#!/usr/bin/env bash
echo "DROP_DB $*" >> "$STUB_LOG"
exit 0
"""

STUB_CREATE_DB = """#!/usr/bin/env bash
echo "CREATE_DB $*" >> "$STUB_LOG"
exit 0
"""

STUB_PG_RESTORE = """#!/usr/bin/env bash
echo "PG_RESTORE $*" >> "$STUB_LOG"
case "$*" in
  *--list*)
    if [ "${DUMP_INVALID:-0}" = "1" ]; then exit 1; fi
    echo "archive listing"
    exit 0
    ;;
  *)
    if [ "${RESTORE_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0
    ;;
esac
"""

STUB_CURL = """#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
echo ok
exit 0
"""

STUB_RM = """#!/usr/bin/env bash
echo "RM $*" >> "$STUB_LOG"
exit 0
"""

def read_script(name):
    with open(os.path.join(SCRIPT_DIR, name), "r", encoding="utf-8") as fh:
        return fh.read()


def write_stubs(bin_dir):
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    stubs = {
        "sudo": STUB_SUDO,
        "systemctl": STUB_SYSTEMCTL,
        "pm2": STUB_PM2,
        "pg_dump": STUB_PG_DUMP,
        "psql": STUB_PSQL,
        "dropdb": STUB_DROP_DB,
        "createdb": STUB_CREATE_DB,
        "pg_restore": STUB_PG_RESTORE,
        "curl": STUB_CURL,
        "rm": STUB_RM,
    }
    for name, body in stubs.items():
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(stub, 0o755)


def sandbox(tmp_path):
    """Isolated sandbox: bare origin with a remote-only test-ref branch, a
    cloned PRD repo, a temp HOME holding a valid test env file, and a stub
    bin dir placed first on PATH."""
    origin = os.path.join(str(tmp_path), "origin.git")
    subprocess.run([GIT, "init", "--bare", origin], check=True, capture_output=True)

    def make_bare_commit(bare_repo, name, content, message, ref):
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


def run_test_up(prd, test_dir, home, bin_dir, ref="test-ref", mode=None, states=None):
    env = os.environ.copy()
    env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    env["HOME"] = home
    env["PRD_DIR"] = prd
    env["TEST_DIR"] = test_dir
    env["STUB_LOG"] = os.path.join(home, "stub.log")
    if states:
        env.update(states)
    if mode:
        env.update(mode)
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


def run_test_down(home, prd, test_dir, bin_dir, keep_db=True, mode=None):
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


def test_test_up_refuses_equal_db_names(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, _ = run_test_up(
        prd, test_dir, home, bin_dir,
        states={"PRD_DB": "pifitness", "TEST_DB": "pifitness"},
    )
    assert proc.returncode != 0, proc.stderr
    assert "PRD and TEST DB names must differ" in proc.stderr


def test_test_up_refuses_wrong_host_port(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    env_file = os.path.join(home, ".config", "pifitness", "test.env")
    with open(env_file, "w", encoding="utf-8") as fh:
        fh.write(
            "APP_ENV=test\n"
            "PG_DB=pifitness_test\n"
            "PG_HOST=127.0.0.1\n"
            "PG_PORT=15432\n"
            "PG_USER=dev\n"
            "PGSSLMODE=disable\n"
            "BOX=local\n"
            "SPOTIFY_ENABLED=false\n"
            "GARMIN_ENABLED=false\n"
            "PIRATE_ENABLED=false\n"
        )
    proc, _ = run_test_up(prd, test_dir, home, bin_dir)
    assert proc.returncode != 0, proc.stderr
    assert "PG_HOST/PG_PORT must match" in proc.stderr


def test_test_up_refuses_unmarked_existing_test_db(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": ""},
    )
    assert proc.returncode != 0, proc.stderr
    assert "refusing to drop/recreate" in proc.stderr
    assert not any("DROP DATABASE" in entry for entry in log), log


def test_test_up_refuses_marked_otherwise_test_db(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": "production"},
    )
    assert proc.returncode != 0, proc.stderr
    assert "refusing to drop/recreate" in proc.stderr
    assert not any("DROP DATABASE" in entry for entry in log), log


def test_test_up_drop_failure_is_not_suppressed(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, _ = run_test_up(
        prd, test_dir, home, bin_dir,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": "test", "DROP_FAIL": "1"},
    )
    assert proc.returncode != 0, proc.stderr
    assert "failed to drop pifitness_test" in proc.stderr


def test_test_up_dump_failure_fail_closed(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, _ = run_test_up(prd, test_dir, home, bin_dir, mode={"DUMP_FAIL": "1"})
    assert proc.returncode != 0, proc.stderr
    assert "dump failed" in proc.stderr


def test_test_up_rejects_empty_dump(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, _ = run_test_up(prd, test_dir, home, bin_dir, mode={"DUMP_EMPTY": "1"})
    assert proc.returncode != 0, proc.stderr
    assert "dump is missing or empty" in proc.stderr


def test_test_up_rejects_invalid_dump_archive(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir, mode={"DUMP_INVALID": "1"})
    assert proc.returncode != 0, proc.stderr
    assert "dump archive is invalid" in proc.stderr
    assert any(entry.startswith("PG_RESTORE --list") for entry in log), log


def test_test_up_clone_then_restore_fail_closed(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        mode={"CLONE_FAIL": "1", "RESTORE_FAIL": "1", "TEST_DB_MARKER": "test"},
    )
    assert proc.returncode != 0, proc.stderr
    assert "pg_restore of" in proc.stderr and "failed" in proc.stderr
    assert any(entry.startswith("PG_RESTORE --list") for entry in log), log


def test_test_up_template_fallback_succeeds(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir, mode={"CLONE_FAIL": "1", "TEST_DB_MARKER": "test"})
    assert proc.returncode == 0, proc.stderr
    assert any(entry.startswith("PG_RESTORE --list") for entry in log), log
    assert any(
        entry.startswith("PG_RESTORE") and "--list" not in entry for entry in log
    ), log
    assert any("timescaledb_pre_restore" in entry for entry in log), log
    assert any("timescaledb_post_restore" in entry for entry in log), log


def test_test_up_marker_stamped_and_verified(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir, mode={"TEST_DB_EXISTS": "", "TEST_DB_MARKER": "test"})
    assert proc.returncode == 0, proc.stderr
    assert "test-up complete." in proc.stdout
    assert any(
        entry.startswith("PSQL")
        and "ALTER DATABASE" in entry
        and "pifitness.env" in entry
        for entry in log
    ), log
    assert any(
        entry.startswith("PSQL")
        and "-d pifitness_test" in entry
        and "current_setting" in entry
        for entry in log
    ), log


def test_test_up_dump_validation_and_unsuppressed_drop(tmp_path):
    """Regression: the dump is validated before the clone and no DROP is swallowed."""
    text = read_script("test-up.sh")
    assert "DROP DATABASE IF EXISTS" not in text
    assert "ERROR: failed to drop" in text
    assert text.index("pg_dump") < text.index("pg_restore --list")
    assert text.index("pg_restore --list") < text.index("Step 4: create test DB")


def test_test_down_refuses_equal_db_names(tmp_path):
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
        FASTAPI_ACTIVE="inactive",
        AGENT_TIMER_ACTIVE="inactive",
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(home, prd, test_dir, bin_dir)
    assert proc.returncode != 0, proc.stderr
    assert "PRD and TEST DB names must differ" in proc.stderr
    assert not any(entry.startswith("SYSTEMCTL") for entry in log), log


def test_test_down_host_port_mismatch_with_state(tmp_path):
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
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, mode={"DB_HOST": "127.0.0.1"}
    )
    assert proc.returncode != 0, proc.stderr
    assert "does not match configured" in proc.stderr
    assert not any(entry.startswith("SYSTEMCTL") for entry in log), log


def test_test_down_refuses_unmarked_test_db(tmp_path):
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
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, keep_db=False,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": ""},
    )
    assert proc.returncode != 0, proc.stderr
    assert "refusing to drop" in proc.stderr
    assert not any("DROP_DB" in entry for entry in log), log


def test_test_down_refuses_marked_otherwise_test_db(tmp_path):
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
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, keep_db=False,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": "production"},
    )
    assert proc.returncode != 0, proc.stderr
    assert "refusing to drop" in proc.stderr
    assert not any("DROP_DB" in entry for entry in log), log


def test_test_down_drops_only_marked_test_db(tmp_path):
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
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(home, prd, test_dir, bin_dir, keep_db=False, mode={"TEST_DB_MARKER": "test"})
    assert proc.returncode == 0, proc.stderr
    assert "test-down complete." in proc.stdout
    assert any("DROP_DB" in entry and "pifitness_test" in entry for entry in log), log


def test_test_down_keep_db_skips_drop(tmp_path):
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
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir, keep_db=True,
        mode={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": ""},
    )
    assert proc.returncode == 0, proc.stderr
    assert "test-down complete." in proc.stdout
    assert not any("DROP_DB" in entry for entry in log), log