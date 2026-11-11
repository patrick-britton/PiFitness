"""Failure-injection tests for test-up.sh and test-down.sh (D3, D11).

Covers every deployment transition under injected command failure: failed
dump, failed restore or clone, marker mismatch, checkout/build failure, test
FastAPI start failure, test PM2 start failure, failed PRD restart, and
repeated test-down. Every injected failure either prevents the test stack
from starting or leaves the durable state file describing the last completed
phase so test-down can recover; no failure can direct a destructive action at
the PRD database.

The scripts run against the shared stubs from test_test_processes.py on a temp
PATH with HOME redirected to a temp dir. Those stubs succeed by default and
fail on demand through environment variables (see that module's docstring).
Each failure test also asserts that its injection actually landed (the stub
log shows the failing command ran), so an ineffective injection cannot pass
silently. Git drives a temp origin/PRD pair so test-up can fetch a ref and
resolve its full SHA.
"""

import os
import re
import subprocess

import pytest

from tests.test_test_down import run_test_down, write_test_state
from tests.test_test_processes import (
    BASH,
    SCRIPT_DIR,
    TEST_DOWN,
    TEST_UP,
    read_script,
    requires_bash,
    run_test_up,
    running_test_stack,
    sandbox,  # noqa: F401  (kept importable for ad-hoc use)
    seeded_sandbox,
    write_runtime_stubs,
)

PRD_DB = "pifitness"
TEST_DB = "pifitness_test"

START_PREFIXES = ("SYSTEMCTL start", "PM2 start")

pytestmark = pytest.mark.unit


def entries(log, *prefixes):
    return [entry for entry in log if entry.startswith(prefixes)]


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


# Matches the exact PRD database name only: not pifitness_test and not the
# pifitness.env setting name used by the marker.
_PRD_DB_NAME = re.compile(r"(?<![\w.])" + re.escape(PRD_DB) + r"(?![\w.])", re.IGNORECASE)


def assert_no_prd_db_destruction(log):
    for entry in log:
        up = entry.upper()
        destructive = (
            "DROP_DB" in up or "DROP DATABASE" in up or "ALTER DATABASE" in up
        )
        if destructive and _PRD_DB_NAME.search(entry):
            raise AssertionError("destructive action against PRD DB: %s" % entry)


def test_prd_db_destruction_helper_flags_only_the_prd_database():
    # The helper must be able to fail, and must not flag TEST DB operations.
    for bad in (
        'PSQL -h localhost -d postgres -c DROP DATABASE "pifitness"',
        "DROP_DB -h localhost -p 15432 -U dev --if-exists pifitness",
        'PSQL -d postgres -c ALTER DATABASE "pifitness" SET x = 1',
    ):
        with pytest.raises(AssertionError):
            assert_no_prd_db_destruction([bad])
    assert_no_prd_db_destruction(
        [
            'PSQL -d postgres -c DROP DATABASE "pifitness_test"',
            "DROP_DB -h localhost -p 15432 -U dev --if-exists pifitness_test",
            "PSQL -d postgres -c ALTER DATABASE \"pifitness_test\" SET pifitness.env = 'test';",
            'PSQL -d postgres -c CREATE DATABASE "pifitness_test" TEMPLATE "pifitness";',
        ]
    )


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
    started. git is not stubbable on this host (Git bash resolves its own
    git ahead of any stub on PATH), so this is proved by script ordering
    rather than by injecting a clone failure."""
    text = read_script("test-up.sh")
    ref_resolved = text.index('current_phase="ref-resolved"')
    write_after_ref = text.index("write_state", ref_resolved)
    clone = text.index('git clone "$PRD_DIR" "$TEST_DIR"')
    start_units = text.index('echo "Step 8: start test units"')
    assert ref_resolved < write_after_ref < clone
    assert clone < start_units


@pytest.mark.unit
@requires_bash
def test_happy_path_test_up_then_test_down(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    write_runtime_stubs(bin_dir)
    with running_test_stack() as https_url:
        proc, log = run_test_up(
            prd, test_dir, home, bin_dir, states={"HTTPS_HOSTNAME": https_url}
        )
    assert proc.returncode == 0, proc.stderr
    assert "test-up complete." in proc.stdout
    assert_no_prd_db_destruction(log)
    assert not any("FAILPOINT" in entry for entry in log), "failpoints disabled by default"
    proc_down, log_down = run_test_down(home, prd, test_dir, bin_dir)
    assert proc_down.returncode == 0, proc_down.stderr
    assert "test-down complete." in proc_down.stdout
    assert_no_prd_db_destruction(log_down)


@pytest.mark.unit
@requires_bash
def test_existing_marked_test_db_is_dropped_and_recreated(tmp_path):
    """A pre-existing TEST DB carrying the 'test' marker is dropped and then
    recreated from the PRD template; only the TEST DB is ever dropped."""
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    write_runtime_stubs(bin_dir)
    with running_test_stack() as https_url:
        proc, log = run_test_up(
            prd, test_dir, home, bin_dir,
            states={"HTTPS_HOSTNAME": https_url, "TEST_DB_EXISTS": "1"},
        )
    assert proc.returncode == 0, proc.stderr
    drops = [i for i, e in enumerate(log) if e.startswith("PSQL") and "DROP DATABASE" in e]
    creates = [i for i, e in enumerate(log) if e.startswith("PSQL") and "CREATE DATABASE" in e]
    assert len(drops) == 1 and len(creates) == 1, log
    assert drops[0] < creates[0], log
    assert '"%s"' % TEST_DB in log[drops[0]], log[drops[0]]
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_failed_dump_never_starts_test_stack_or_state(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"PG_DUMP_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert "dump failed" in proc.stderr, proc.stderr
    # The injection landed: pg_dump ran, and nothing else did.
    assert entries(log, "PG_DUMP"), log
    assert [e for e in log if not e.startswith("PG_DUMP")] == [], log
    assert not entries(log, *START_PREFIXES), (
        "no test service may start when the dump fails: %r" % log
    )
    assert not os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert not os.path.exists(test_dir)
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
@pytest.mark.parametrize("marker", ["prod", ""], ids=["other-marker", "unmarked"])
def test_marker_mismatch_refuses_drop_and_keeps_state(tmp_path, marker):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        states={"TEST_DB_EXISTS": "1", "TEST_DB_MARKER": marker},
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert "refusing to drop/recreate" in proc.stderr, proc.stderr
    # The injection landed: the script found the DB and read its marker.
    assert any("pg_database" in e for e in entries(log, "PSQL")), log
    assert any("current_setting" in e for e in entries(log, "PSQL")), log
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "prd-stopped", fields
    assert os.path.exists(state_path)
    assert not any("DROP DATABASE" in e.upper() for e in log), log
    assert not any("CREATE DATABASE" in e.upper() for e in log), log
    assert not entries(log, "DROP_DB", "CREATE_DB"), log
    assert not entries(log, *START_PREFIXES)
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_failed_restore_leaves_state_for_recovery(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    # Force the template clone to fail so test-up falls back to pg_restore
    # into the TEST DB, then inject a pg_restore failure there.
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        states={"PSQL_CREATE_FAIL": "1", "PG_RESTORE_FAIL": "1"},
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert "pg_restore of" in proc.stderr, proc.stderr
    # Both injections landed: the template clone failed, the fallback ran,
    # and the failing restore (not just the --list validation) was invoked.
    assert any("TEMPLATE" in e for e in entries(log, "PSQL")), log
    assert entries(log, "CREATE_DB"), log
    restores = [e for e in entries(log, "PG_RESTORE") if "--list" not in e]
    assert restores, log
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "prd-stopped", fields
    assert os.path.exists(state_path)
    assert not entries(log, *START_PREFIXES), (
        "no test service may start when the restore fails: %r" % log
    )
    assert not os.path.exists(test_dir)
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
@pytest.mark.parametrize(
    "variable, expected_entry",
    [("PIP_FAIL", "PIP install -r"), ("NPM_FAIL", "NPM ci")],
    ids=["pip-install", "npm-ci"],
)
def test_checkout_build_failure_stops_before_services(tmp_path, variable, expected_entry):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(prd, test_dir, home, bin_dir, states={variable: "1"})
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    # The injection landed at the intended command.
    assert entries(log, expected_entry), log
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "checkout-done", fields
    assert os.path.exists(state_path)
    assert not entries(log, *START_PREFIXES)
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_fastapi_start_failure_runs_recovery(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"SYSTEMCTL_START_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    # The injection landed: the test FastAPI start was attempted and failed.
    assert "SYSTEMCTL start pifitness-fastapi-test.service" in log, log
    assert "failed to start test FastAPI unit" in proc.stderr, proc.stderr
    assert not entries(log, "PM2 start"), log
    assert "Running recovery path (test-down.sh)..." in proc.stderr, proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_pm2_start_failure_runs_recovery(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir, states={"PM2_START_FAIL": "1"}
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    # The injection landed: FastAPI started, then the PM2 start failed.
    assert "SYSTEMCTL start pifitness-fastapi-test.service" in log, log
    assert entries(log, "PM2 start"), log
    assert "failed to start PM2 test process" in proc.stderr, proc.stderr
    assert "Running recovery path (test-down.sh)..." in proc.stderr, proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_repeated_test_down_is_safe_and_leaves_nothing_deleted(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
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
    # PRD checkout is never removed.
    assert os.path.isdir(prd)


@pytest.mark.unit
@requires_bash
def test_failpoint_refuses_without_acknowledgement(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        states={"PIFITNESS_FAILPOINT_PRE_START": "1"},
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-only acknowledgement" in proc.stderr, proc.stderr
    assert not any("FAILPOINT pre_start engaged" in entry for entry in log)
    assert "FAILPOINT pre_start engaged" not in proc.stderr
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "checkout-provisioned", fields
    assert os.path.exists(state_path)
    assert not entries(log, *START_PREFIXES)
    assert "Running recovery path" not in proc.stderr
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_failpoint_engaged_stops_before_test_stack(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(
        prd, test_dir, home, bin_dir,
        states={
            "PIFITNESS_FAILPOINT_PRE_START": "1",
            "PIFITNESS_FAILPOINT_ACK": "test-only",
        },
    )
    assert proc.returncode != 0, proc.stdout
    assert "FAILPOINT pre_start engaged" in proc.stderr, proc.stderr
    assert "test-up complete." not in proc.stdout
    assert not entries(log, *START_PREFIXES)
    fields, state_path = parse_state(home)
    assert fields.get("PHASE") == "checkout-provisioned", fields
    assert os.path.exists(state_path)
    assert_no_prd_db_destruction(log)


def _write_active_state(home, prd, test_dir):
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


@pytest.mark.unit
@requires_bash
def test_test_down_failpoint_refuses_without_acknowledgement(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    os.makedirs(test_dir, exist_ok=True)
    _write_active_state(home, prd, test_dir)
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir,
        mode={"PIFITNESS_FAILPOINT_DURING_RESTORE": "1"},
    )
    assert proc.returncode != 0, proc.stdout
    assert "test-only acknowledgement" in proc.stderr, proc.stderr
    assert not any("FAILPOINT during_restore engaged" in entry for entry in log)
    assert os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert os.path.isdir(test_dir), "checkout must not be removed"
    assert not entries(log, "SYSTEMCTL start"), log
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert_no_prd_db_destruction(log)


@pytest.mark.unit
@requires_bash
def test_test_down_failpoint_engaged_stops_before_cleanup(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    os.makedirs(test_dir, exist_ok=True)
    _write_active_state(home, prd, test_dir)
    proc, log = run_test_down(
        home, prd, test_dir, bin_dir,
        mode={
            "PIFITNESS_FAILPOINT_DURING_RESTORE": "1",
            "PIFITNESS_FAILPOINT_ACK": "test-only",
        },
    )
    assert proc.returncode != 0, proc.stdout
    assert "FAILPOINT during_restore engaged" in proc.stderr, proc.stderr
    assert not any(entry.startswith("DROP_DB") for entry in log), log
    assert not any(entry.startswith("RM") for entry in log), log
    assert not entries(log, "SYSTEMCTL start"), (
        "PRD restore must not run when the failpoint engages"
    )
    assert os.path.exists(os.path.join(home, ".pifitness-test-state"))
    assert os.path.isdir(test_dir), "checkout must not be removed"
    assert_no_prd_db_destruction(log)
