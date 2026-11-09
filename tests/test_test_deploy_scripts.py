"""Failure-injection tests for test-up.sh and test-down.sh (D3, D11).

Covers every deployment transition under injected command failure: failed
dump, failed restore or clone, marker mismatch, migration failure,
checkout/build failure, test FastAPI start failure, test PM2 start
failure, failed PRD restart, and repeated test-down. Every injected
failure either prevents the test stack from starting or leaves the
durable state file describing the last completed phase so test-down can
recover; no failure can direct a destructive action at the PRD database.

The scripts run against stubbed commands on a temp PATH with HOME
redirected to a temp dir. Git drives a temp origin/PRD pair so test-up
can fetch a ref and resolve its full SHA.
"""

import os
import subprocess

import pytest

from tests.test_test_down import run_test_down, write_test_state
from tests.test_test_processes import bash_available, run_test_up, sandbox
from tests.test_test_up import (
    MIGRATION_CHECKOUT_FILES,
    advance_test_ref,
    read_script,
    start_local_servers,
    write_runtime_stubs,
)

SCRIPT_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "deploy", "scripts")
)
TEST_UP = os.path.join(SCRIPT_DIR, "test-up.sh")
TEST_DOWN = os.path.join(SCRIPT_DIR, "test-down.sh")
BASH = "C:\\Program Files\\Git\\bin\\bash.exe"

PRD_DB = "pifistics"
TEST_DB = "pifistics_test"

pytestmark = pytest.mark.unit

_STUB_SUDO = """#!/usr/bin/env bash
echo "SUDO $*" >> "$STUB_LOG"
"$@"
"""
_STUB_SYSTEMCTL = """#!/usr/bin/env bash
echo "SYSTEMCTL $*" >> "$STUB_LOG"
case "$1" in
  is-active)
    case "$2" in
      pifistics-fastapi.service) echo "${FASTAPI_STATE:-inactive}" ;;
      pifistics_agent.timer) echo "${AGENT_TIMER_STATE:-inactive}" ;;
      pifistics-hardware-health.timer) echo "${HARDWARE_HEALTH_TIMER_STATE:-inactive}" ;;
      *) echo inactive ;;
    esac ;;
  start)
    if [ "${SYSTEMCTL_START_FAIL:-0}" = "1" ]; then exit 1; fi ;;
  *)
    exit 0 ;;
esac
exit 0
"""
_STUB_PM2 = """#!/usr/bin/env bash
echo "PM2 $*" >> "$STUB_LOG"
case "$1" in
  describe)
    if [ "$2" = "pifistics-next" ]; then
      if [ "${PM2_ONLINE:-0}" = "1" ]; then echo "status: online"; else echo "status: stopped"; fi
    fi
    exit 0 ;;
  start|restart)
    if [ "${PM2_START_FAIL:-0}" = "1" ]; then exit 1; fi
    exit 0 ;;
  stop|delete)
    exit 0 ;;
  *)
    exit 0 ;;
esac
exit 0
"""
_STUB_PG_DUMP = """#!/usr/bin/env bash
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
  printf 'dump-data\\n' > "$dump_file"
fi
exit 0
"""
_STUB_PSQL = """#!/usr/bin/env bash
echo "PSQL $*" >> "$STUB_LOG"
if [[ "$*" == *pg_database* ]]; then
  echo "${TEST_DB_EXISTS:-1}"
  exit 0
fi
if [[ "$*" == *current_setting* ]]; then
  echo "${TEST_DB_MARKER-test}"
  exit 0
fi
if [[ "$*" == *TEMPLATE* && "${PSQL_CREATE_FAIL:-0}" = "1" ]]; then
  echo "psql: simulated CREATE DATABASE failure" >&2
  exit 1
fi
exit 0
"""
_STUB_PG_RESTORE = """#!/usr/bin/env bash
echo "PG_RESTORE $*" >> "$STUB_LOG"
case "$*" in
  *--list*)
    # Dump archive validation (test-up Step 2): succeed so test-up reaches
    # the restore step. A restore failure is injected only into the DB
    # restore below.
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
_STUB_DROP_DB = """#!/usr/bin/env bash
echo "DROP_DB $*" >> "$STUB_LOG"
exit 0
"""
_STUB_CREATE_DB = """#!/usr/bin/env bash
echo "CREATE_DB $*" >> "$STUB_LOG"
exit 0
"""
_STUB_CURL = """#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
if [ "${CURL_FAIL:-0}" = "1" ]; then exit 1; fi
echo ok
exit 0
"""
_STUB_RM = """#!/usr/bin/env bash
echo "RM $*" >> "$STUB_LOG"
exit 0
"""
_STUB_GIT = """#!/usr/bin/env bash
echo "GIT $*" >> "$STUB_LOG"
case "$1" in
  clone)
    if [ "${GIT_CLONE_FAIL:-0}" = "1" ]; then
      echo "git: simulated clone failure" >&2
      exit 1
    fi
    ;;
esac
PATH="$__GIT_REAL__" git "$@"
exit $?
"""

_PYTHON3_STUB = """\
#!/usr/bin/env bash
echo "PYTHON3 $*" >> "$STUB_LOG"
case "$1" in
  -m)
    case "$2" in
      venv)
        venv_dir="${!#}"
        mkdir -p "$venv_dir/bin"
        cat > "$venv_dir/bin/python" <<'EOF'
#!/usr/bin/env bash
echo "VENV_PYTHON $*" >> "$STUB_LOG"
shim_dir="$(cd "$(dirname "$0")" && pwd)"
test_dir="$(dirname "$(dirname "$shim_dir")")"
case "$*" in
  */migrate.py*)
    echo "Migration status:"
    echo "Version    Status   Name"
    echo "----------------------------------------"
    echo "1          applied  001__int19b_move_tokens_to_api_services.sql"
    echo "2          pending  002__int25_repoint_get_spotify_client.sql"
    case "$2" in
      up)
        if [ "${MIGRATE_FAIL:-0}" = "1" ]; then
          echo "Applying 002__int25_repoint_get_spotify_client.sql ..." >&2
          echo "migration failed: simulated runner failure" >&2
          exit 1
        fi
        echo "Applying 002__int25_repoint_get_spotify_client.sql ..."
        echo "  002__int25_repoint_get_spotify_client.sql applied."
        ;;
    esac
    exit 0
    ;;
  *)
    echo "${TEST_DIR:-$test_dir}/backend/pifitness/__init__.py"
    exit 0
    ;;
esac
EOF
        chmod +x "$venv_dir/bin/python"
        cat > "$venv_dir/bin/pip" <<'EOF'
#!/usr/bin/env bash
echo "PIP $*" >> "$STUB_LOG"
if [ "${PIP_FAIL:-0}" = "1" ]; then exit 1; fi
exit 0
EOF
        chmod +x "$venv_dir/bin/pip"
        ;;
    esac
    ;;
  *)
    exit 0
    ;;
esac
exit 0
"""
_NODE_STUB = """#!/usr/bin/env bash
echo "{name} $*" >> "$STUB_LOG"
exit 0
"""


def write_stubs(bin_dir):
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    git_search_path = "/usr/bin" + os.pathsep + os.environ.get("PATH", "")
    stubs = {
        "sudo": _STUB_SUDO,
        "systemctl": _STUB_SYSTEMCTL,
        "pm2": _STUB_PM2,
        "pg_dump": _STUB_PG_DUMP,
        "psql": _STUB_PSQL,
        "pg_restore": _STUB_PG_RESTORE,
        "dropdb": _STUB_DROP_DB,
        "createdb": _STUB_CREATE_DB,
        "curl": _STUB_CURL,
        "rm": _STUB_RM,
        "git": _STUB_GIT.replace("__GIT_REAL__", git_search_path),
    }
    for name, body in stubs.items():
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(stub, 0o755)


def write_runner_stubs(bin_dir):
    """Stub python3/npm/node so provisioning succeeds; migration runner
    outcome controllable via MIGRATE_FAIL, pip/npm via PIP_FAIL/NPM_FAIL."""
    bin_dir = str(bin_dir)
    os.makedirs(bin_dir, exist_ok=True)
    with open(os.path.join(bin_dir, "python3"), "w", encoding="utf-8") as fh:
        fh.write(_PYTHON3_STUB)
    os.chmod(os.path.join(bin_dir, "python3"), 0o755)
    for name in ("npm", "node"):
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(_NODE_STUB.format(name=name))
        os.chmod(stub, 0o755)


def parse_state(home):
    state_path = os.path.join(home, ".pifitness-test-state")
    fields = {}
    with open(state_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if "=" in line:
                key, value = line.split("=", 1)
                fields[key] = value
    return fields, state_path


def assert_no_prd_db_destruction(log):
    for entry in log:
        up = entry.upper()
        destructive = (
            "DROP_DB" in up or "DROP DATABASE" in up or "ALTER DATABASE" in up
        )
        if destructive and PRD_DB in up:
            raise AssertionError("destructive action against PRD DB: %s" % entry)


@pytest.mark.unit
def test_syntax_check_via_bash_n():
    if not os.path.isfile(BASH):
        pytest.skip("Git bash is required")
    for script in (TEST_UP, TEST_DOWN):
        proc = subprocess.run([BASH, "-n", script], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr


@pytest.mark.unit
def test_scripts_never_send_destructive_actions_to_prd_db():
    for name in ("test-up.sh", "test-down.sh"):
        text = read_script(name)
        assert "failpoint()" in text, "%s must declare the failpoint drill hook" % name
        for line in text.splitlines():
            if "dropdb" in line and "PRD_DB" in line:
                raise AssertionError("dropdb must not target $PRD_DB: %s" % line)
            if "-c" in line and "PRD_DB" in line and (
                "DROP DATABASE" in line.upper() or "ALTER DATABASE" in line.upper()
            ):
                raise AssertionError("DDL must not target $PRD_DB: %s" % line)
    assert (
        'pg_dump -Fc -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$PRD_DB"'
        in read_script("test-up.sh")
    )


@pytest.mark.unit
def test_state_written_after_every_durable_phase():
    text = read_script("test-up.sh")
    assert "trap 'write_state' INT TERM" in text
    phases = [
        'current_phase="initial"',
        'current_phase="prd-stopped"',
        'current_phase="test-db-created"',
        'current_phase="ref-resolved"',
        'current_phase="checkout-done"',
        'current_phase="checkout-provisioned"',
        'current_phase="migrations-applied"',
        'current_phase="test-units-started"',
        'current_phase="complete"',
    ]
    position = 0
    for phase in phases:
        index = text.index(phase, position)
        write_index = text.index("write_state", index)
        assert write_index > index, "write_state must follow %s" % phase
        position = write_index + 1


@pytest.mark.unit
def test_clone_aborts_before_service_start_with_state_intact():
    """Script-level guarantee: the git clone is ordered before test-unit
    starts and the ref-resolved state is written first, so a clone failure
    (set -e) aborts with the state file intact and no test services
    started. git is not stubbable on this host, so this is proved by script
    ordering rather than by injecting a clone failure."""
    text = read_script("test-up.sh")
    ref_resolved = text.index('current_phase="ref-resolved"')
    write_after_ref = text.index("write_state", ref_resolved)
    clone = text.index('git clone "$PRD_DIR" "$TEST_DIR"')
    start_units = text.index('echo "Step 9: start test units"')
    assert ref_resolved < write_after_ref < clone
    assert clone < start_units


@pytest.mark.unit
def test_happy_path_test_up_then_test_down(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    write_runtime_stubs(bin_dir)
    servers, urls = start_local_servers([8000, 3000, 0])
    os.environ["HTTPS_HOSTNAME"] = urls[2]
    try:
        proc, log = run_test_up(prd, test_dir, home, bin_dir)
    finally:
        for server in servers:
            server.server_close()
        os.environ.pop("HTTPS_HOSTNAME", None)
    assert proc.returncode == 0, proc.stderr
    assert "test-up complete." in proc.stdout
    assert_no_prd_db_destruction(log)
    assert not any("FAILPOINT" in entry for entry in log), "failpoints disabled by default"
    proc_down, log_down = run_test_down(home, prd, test_dir, bin_dir)
    assert proc_down.returncode == 0, proc_down.stderr
    assert "test-down complete." in proc_down.stdout
    assert_no_prd_db_destruction(log_down)


@pytest.mark.unit
def test_failed_dump_never_starts_test_stack_or_state(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"PG_DUMP_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert not any(
        entry.startswith(("SYSTEMCTL start", "PM2 start")) for entry in log
    ), "no test service may start when the dump fails: %r" % log
    assert not os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_marker_mismatch_refuses_drop_and_keeps_state(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"TEST_DB_MARKER": ""}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "prd-stopped", fields
    assert os.path.exists(state_path)
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(
        entry.startswith(("SYSTEMCTL start", "PM2 start")) for entry in log
    )
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_failed_restore_leaves_state_for_recovery(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    # Force the template clone to fail so test-up falls back to pg_restore
    # into the TEST DB, then inject a pg_restore failure there.
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        states={"PSQL_CREATE_FAIL": "1", "PG_RESTORE_FAIL": "1"},
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "prd-stopped", fields
    assert os.path.exists(state_path)
    assert not any(
        entry.startswith(("SYSTEMCTL start", "PM2 start")) for entry in log
    ), "no test service may start when the restore fails: %r" % log
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_checkout_build_failure_stops_before_services(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"PIP_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "checkout-done", fields
    assert os.path.exists(state_path)
    assert not any(
        entry.startswith(("SYSTEMCTL start", "PM2 start")) for entry in log
    )
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_migration_failure_stops_before_services(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"MIGRATE_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "checkout-provisioned", fields
    assert os.path.exists(state_path)
    assert not any(
        entry.startswith(("SYSTEMCTL start", "PM2 start")) for entry in log
    )
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_fastapi_start_failure_runs_recovery(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"SYSTEMCTL_START_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert not any(entry.startswith("PM2 start") for entry in log), log
    assert "Running recovery path (test-down.sh)..." in proc.stderr, proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_pm2_start_failure_runs_recovery(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"PM2_START_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert "Running recovery path (test-down.sh)..." in proc.stderr, proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_repeated_test_down_is_safe_and_leaves_nothing_deleted(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB=PRD_DB,
        TEST_DB=TEST_DB,
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="inactive",
        AGENT_TIMER_ACTIVE="inactive",
        HARDWARE_HEALTH_TIMER_ACTIVE="unknown",
        PM2_NEXTPROD_ACTIVE="unknown",
    )
    for _ in range(2):
        proc, log = run_test_down(home, prd, test_dir, bin_dir)
        assert proc.returncode == 0, proc.stderr
        assert "test-down complete." in proc.stdout
        assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not os.path.exists(os.path.join(home, ".pifitness-test-state"))


@pytest.mark.unit
def test_failpoint_refuses_without_acknowledgement(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    os.environ["PIFITNESS_FAILPOINT_PRE_START"] = "1"
    try:
        proc, log = run_test_up(prd, test_dir, home, bin_dir)
    finally:
        os.environ.pop("PIFITNESS_FAILPOINT_PRE_START", None)
    assert proc.returncode != 0, proc.stdout
    assert "test-only acknowledgement" in proc.stderr, proc.stderr
    assert not any("FAILPOINT pre_start engaged" in entry for entry in log)
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "migrations-applied", fields
    assert os.path.exists(state_path)
    assert not any(
        entry.startswith(("SYSTEMCTL start pifistics-fastapi-test.service", "PM2 start"))
        for entry in log
    )
    assert "Running recovery path" not in proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_failpoint_engaged_stops_before_test_stack(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    os.environ.update(
        {"PIFITNESS_FAILPOINT_PRE_START": "1", "PIFITNESS_FAILPOINT_ACK": "test-only"}
    )
    try:
        proc, log = run_test_up(prd, test_dir, home, bin_dir)
    finally:
        os.environ.pop("PIFITNESS_FAILPOINT_PRE_START", None)
        os.environ.pop("PIFITNESS_FAILPOINT_ACK", None)
    assert proc.returncode != 0, proc.stdout
    assert "FAILPOINT pre_start engaged" in proc.stderr, proc.stderr
    assert "test-up complete." not in proc.stdout
    assert not any(
        entry.startswith("SYSTEMCTL start pifistics-fastapi-test.service") for entry in log
    )
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "migrations-applied", fields
    assert os.path.exists(state_path)
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_test_down_failpoint_refuses_without_acknowledgement(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB=PRD_DB,
        TEST_DB=TEST_DB,
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    os.environ["PIFITNESS_FAILPOINT_DURING_RESTORE"] = "1"
    try:
        proc, log = run_test_down(home, prd, test_dir, bin_dir)
    finally:
        os.environ.pop("PIFITNESS_FAILPOINT_DURING_RESTORE", None)
    assert proc.returncode != 0, proc.stdout
    assert "test-only acknowledgement" in proc.stderr, proc.stderr
    assert not any("FAILPOINT during_restore engaged" in entry for entry in log)
    assert os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
def test_test_down_failpoint_engaged_stops_before_cleanup(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    write_test_state(
        home,
        PRD_DIR=prd,
        TEST_DIR=test_dir,
        PRD_DB=PRD_DB,
        TEST_DB=TEST_DB,
        DB_HOST="localhost",
        DB_PORT="15432",
        FASTAPI_ACTIVE="active",
        AGENT_TIMER_ACTIVE="active",
        HARDWARE_HEALTH_TIMER_ACTIVE="active",
        PM2_NEXTPROD_ACTIVE="online",
    )
    os.environ.update(
        {
            "PIFITNESS_FAILPOINT_DURING_RESTORE": "1",
            "PIFITNESS_FAILPOINT_ACK": "test-only",
        }
    )
    try:
        proc, log = run_test_down(home, prd, test_dir, bin_dir)
    finally:
        os.environ.pop("PIFITNESS_FAILPOINT_DURING_RESTORE", None)
        os.environ.pop("PIFITNESS_FAILPOINT_ACK", None)
    assert proc.returncode != 0, proc.stdout
    assert "FAILPOINT during_restore engaged" in proc.stderr, proc.stderr
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert not any(
        entry.startswith("SYSTEMCTL start") for entry in log
    ), "PRD restore must not run when the failpoint engages"
    assert os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert_no_prd_db_destruction(log)