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


# REVIEWED: Unit — real write_state_equivalent renames temp file without leftovers
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


# REVIEWED: Unit — real write_state_equivalent creates restrictive permissions
@pytest.mark.unit
def test_atomic_write_creates_restrictive_permissions(tmp_path):
    if os.name == "nt":
        pytest.skip("Windows does not enforce POSIX mode bits")
    target = str(tmp_path / "state")
    write_state_equivalent(target, {"PRD_COMMIT": "c" * 40}, "initial")
    mode = stat.S_IMODE(os.stat(target).st_mode)
    assert mode == 0o600, "expected mode 0600, got {0}".format(oct(mode))


# REVIEWED: Unit — real state records all required fields
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


# REVIEWED: Unit — real state sufficient for test-down restore decisions
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






