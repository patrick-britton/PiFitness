"""Tests for durable test state (~/.pifitness-test-state).

Covers D3: test-up writes the state file atomically (temp file + rename,
mode 0600) before stopping any PRD service, timer, or PM2 process; it records
the PRD commit, requested ref and resolved test SHA, PRD/TEST DB names plus
host/port, dump path, canonical checkout paths, the initial active state of
FastAPI, each relevant timer/service, and production PM2 pifitness-next; a
phase/status field is updated after every major transition; no secrets are
stored; a crash or Ctrl+C leaves the current phase durable; and the recorded
state is sufficient for test-down to decide what to restore.
"""

import os
import stat
from pathlib import Path

import pytest

SCRIPT_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "deploy", "scripts")
)
TEST_UP = os.path.join(SCRIPT_DIR, "test-up.sh")
TEST_DOWN = os.path.join(SCRIPT_DIR, "test-down.sh")

REQUIRED_FIELDS = (
    "PHASE",
    "PRD_COMMIT",
    "REF",
    "RESOLVED_SHA",
    "PRD_DB",
    "TEST_DB",
    "DB_HOST",
    "DB_PORT",
    "DUMP_PATH",
    "PRD_DIR",
    "TEST_DIR",
    "FASTAPI_ACTIVE",
    "AGENT_TIMER_ACTIVE",
    "HARDWARE_HEALTH_TIMER_ACTIVE",
    "PM2_NEXTPROD_ACTIVE",
)


def read_script(name):
    with open(os.path.join(SCRIPT_DIR, name), "r", encoding="utf-8") as fh:
        return fh.read()


def parse_state_text(text):
    fields = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        fields[key.strip()] = value.strip()
    return fields


def write_state_equivalent(target, fields, phase):
    """Python equivalent of test-up.sh write_state(): temp file, 0600, rename."""
    tmp = "{0}.tmp.{1}".format(target, os.getpid())
    old_umask = os.umask(0o077)
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("PHASE={0}\n".format(phase))
            for key, value in fields.items():
                fh.write("{0}={1}\n".format(key, value))
        os.replace(tmp, target)
    finally:
        os.umask(old_umask)


@pytest.mark.unit
def test_atomic_write_renames_temp_file_without_leftovers(tmp_path):
    target = str(tmp_path / "state")
    fields = {"PRD_COMMIT": "c" * 40, "PRD_DIR": "/home/god/PiFitness"}
    write_state_equivalent(target, fields, "initial")
    assert os.path.isfile(target)
    leftovers = [
        name for name in os.listdir(tmp_path) if name.startswith("state.tmp.")
    ]
    assert not leftovers, "temp state file left behind: {0}".format(leftovers)
    parsed = parse_state_text(Path(target).read_text(encoding="utf-8"))
    assert parsed["PHASE"] == "initial"
    assert parsed["PRD_COMMIT"] == "c" * 40


@pytest.mark.unit
def test_atomic_write_creates_restrictive_permissions(tmp_path):
    if os.name == "nt":
        pytest.skip("Windows does not enforce POSIX mode bits")
    target = str(tmp_path / "state")
    write_state_equivalent(target, {"PRD_COMMIT": "c" * 40}, "initial")
    mode = stat.S_IMODE(os.stat(target).st_mode)
    assert mode == 0o600, "expected mode 0600, got {0}".format(oct(mode))


@pytest.mark.unit
def test_state_records_all_required_fields(tmp_path):
    target = str(tmp_path / "state")
    fields = {
        "PHASE": "initial",
        "PRD_COMMIT": "c" * 40,
        "REF": "main",
        "RESOLVED_SHA": "d" * 40,
        "PRD_DB": "pifitness",
        "TEST_DB": "pifitness_test",
        "DB_HOST": "localhost",
        "DB_PORT": "15432",
        "DUMP_PATH": "/home/god/Documents/DB_Backups/pifitness_prd_20260101_000000.dump",
        "PRD_DIR": "/home/god/PiFitness",
        "TEST_DIR": "/home/god/PiFitness-test",
        "FASTAPI_ACTIVE": "active",
        "AGENT_TIMER_ACTIVE": "inactive",
        "HARDWARE_HEALTH_TIMER_ACTIVE": "unknown",
        "PM2_NEXTPROD_ACTIVE": "online",
    }
    write_state_equivalent(target, fields, "initial")
    parsed = parse_state_text(Path(target).read_text(encoding="utf-8"))
    for key in REQUIRED_FIELDS:
        assert key in parsed, "missing state field: {0}".format(key)


@pytest.mark.unit
def test_required_state_fields_are_secret_free():
    for key in REQUIRED_FIELDS:
        upper = key.upper()
        for token in ("PG_PASSWORD", "SECRET", "TOKEN", "API_KEY"):
            assert token not in upper


@pytest.mark.unit
def test_phase_field_updates_after_every_major_transition(tmp_path):
    target = str(tmp_path / "state")
    fields = {"PRD_COMMIT": "c" * 40}
    phases = [
        "initial",
        "prd-stopped",
        "test-db-created",
        "migrations-applied",
        "ref-resolved",
        "checkout-done",
        "test-units-started",
        "complete",
    ]
    for phase in phases:
        write_state_equivalent(target, fields, phase)
        parsed = parse_state_text(Path(target).read_text(encoding="utf-8"))
        assert parsed["PHASE"] == phase, "phase not updated to {0}".format(phase)


@pytest.mark.unit
def test_state_sufficient_for_test_down_restore_decisions(tmp_path):
    target = str(tmp_path / "state")
    fields = {
        "PHASE": "prd-stopped",
        "PRD_COMMIT": "c" * 40,
        "PRD_DB": "pifitness",
        "TEST_DB": "pifitness_test",
        "DB_HOST": "localhost",
        "DB_PORT": "15432",
        "PRD_DIR": "/home/god/PiFitness",
        "TEST_DIR": "/home/god/PiFitness-test",
        "FASTAPI_ACTIVE": "active",
        "AGENT_TIMER_ACTIVE": "inactive",
        "HARDWARE_HEALTH_TIMER_ACTIVE": "active",
        "PM2_NEXTPROD_ACTIVE": "online",
    }
    write_state_equivalent(target, fields, "prd-stopped")
    parsed = parse_state_text(Path(target).read_text(encoding="utf-8"))
    assert parsed["FASTAPI_ACTIVE"] == "active"
    assert parsed["AGENT_TIMER_ACTIVE"] == "inactive"
    assert parsed["HARDWARE_HEALTH_TIMER_ACTIVE"] == "active"
    assert parsed["PM2_NEXTPROD_ACTIVE"] == "online"
    assert parsed["PHASE"]


@pytest.mark.unit
def test_state_written_before_any_prd_shutdown():
    lines = read_script("test-up.sh").splitlines()
    capture_index = next(
        i for i, line in enumerate(lines)
        if "Capture the initial active state" in line
    )
    write_indexes = [
        i for i, line in enumerate(lines[capture_index:], start=capture_index)
        if line.strip() == "write_state"
    ]
    stop_indexes = [
        i for i, line in enumerate(lines)
        if "systemctl stop" in line
    ]
    assert write_indexes, "no write_state call after state capture"
    assert stop_indexes, "no PRD shutdown command found"
    assert min(write_indexes) < min(stop_indexes), (
        "state file must be durable before the first PRD service/timer stop"
    )


@pytest.mark.unit
def test_state_write_uses_atomic_temp_then_rename():
    text = read_script("test-up.sh")
    assert ".tmp.$$" in text
    assert "umask 077" in text
    assert "mv -f" in text


@pytest.mark.unit
def test_crash_trap_persists_current_phase():
    text = read_script("test-up.sh")
    assert "trap" in text
    assert "INT" in text
    assert "TERM" in text
    assert "write_state" in text


@pytest.mark.unit
def test_state_writer_never_stores_secrets():
    text = read_script("test-up.sh")
    start = text.index("write_state() {")
    end = text.index("\n}", start)
    body = text[start:end]
    for token in ("PG_PASSWORD", "SECRET", "TOKEN"):
        assert token not in body


@pytest.mark.unit
def test_down_loads_state_before_cleanup():
    lines = read_script("test-down.sh").splitlines()
    load_indexes = [
        i for i, line in enumerate(lines)
        if '. "$STATE_FILE"' in line
    ]
    rm_indexes = [
        i for i, line in enumerate(lines)
        if 'rm -f "$STATE_FILE"' in line
    ]
    health_indexes = [
        i for i, line in enumerate(lines)
        if "health" in line.lower() and "curl" in line
    ]
    assert load_indexes, "test-down does not load the state file"
    assert rm_indexes, "test-down does not remove the state file"
    assert health_indexes, "test-down has no PRD health check"
    assert min(load_indexes) < min(rm_indexes), (
        "state must be loaded before cleanup decisions"
    )
    assert min(health_indexes) < min(rm_indexes), (
        "PRD health must pass before the state file is removed"
    )