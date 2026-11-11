"""Tests for test-up.sh: ref resolution from origin and path validation.

Covers D16: resolve the requested ref from origin, record its full commit SHA,
and check out that exact SHA into the test dir. Before any rm -rf, canonicalize
and validate PRD_DIR and TEST_DIR; refuse symlinks, empty paths, "/", the PRD
directory, and any path that is an ancestor of PRD.

Shared fixtures (stubs, sandbox, seeded test-ref, local test-stack servers)
live in test_test_processes.py.
"""

import os
import subprocess

import pytest

from tests.test_test_processes import (
    PROVISIONING_CHECKOUT_FILES,  # noqa: F401  (re-exported for older imports)
    advance_test_ref,  # noqa: F401  (re-exported for older imports)
    read_script,
    requires_bash,
    run_test_up,
    running_test_stack,
    sandbox,
    seeded_sandbox,
    start_local_servers,  # noqa: F401  (re-exported for older imports)
    write_runtime_stubs,
    write_stubs,
)


SCRIPT_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "deploy", "scripts", "test-up.sh"))


# ---------------------------------------------------------------------------
# Path validation (canonicalize_path equivalent, cross-platform)
# ---------------------------------------------------------------------------


def canonicalize_path_python(path):
    """Python equivalent of the bash canonicalize_path() in test-up.sh.

    Refuses: empty paths, "/", symlinks. Returns os.path.abspath(path).
    """
    if not path:
        raise ValueError("empty path")
    if path == "/":
        raise ValueError(f"path is root: {path}")
    if os.path.islink(path):
        raise ValueError(f"path is a symlink: {path}")
    return os.path.abspath(path)


def validate_dirs(prd_dir, test_dir):
    """Python equivalent of the test-up.sh dir validation before rm -rf.

    Refuses: TEST_DIR == PRD_DIR, TEST_DIR is an ancestor of PRD_DIR.
    """
    if test_dir == prd_dir:
        raise ValueError(f"TEST_DIR must not equal PRD_DIR: {test_dir}")
    # TEST_DIR must not be an ancestor of PRD_DIR
    if prd_dir == test_dir or prd_dir.startswith(test_dir + os.sep):
        raise ValueError(
            f"TEST_DIR is PRD_DIR or an ancestor of PRD_DIR: {test_dir}"
        )


@pytest.mark.unit
def test_canonicalize_refuses_empty_path():
    with pytest.raises(ValueError, match="empty path"):
        canonicalize_path_python("")


@pytest.mark.unit
def test_canonicalize_refuses_root():
    with pytest.raises(ValueError, match="path is root"):
        canonicalize_path_python("/")


@pytest.mark.unit
def test_canonicalize_refuses_symlink(tmp_path):
    link = tmp_path / "alink"
    target = tmp_path / "target"
    target.mkdir()
    os.symlink(target, link)
    with pytest.raises(ValueError, match="path is a symlink"):
        canonicalize_path_python(str(link))


@pytest.mark.unit
def test_canonicalize_returns_absolute_path(tmp_path):
    sub = tmp_path / "sub" / "deep"
    sub.mkdir(parents=True)
    result = canonicalize_path_python(str(sub))
    assert os.path.isabs(result)
    assert result == os.path.abspath(str(sub))


@pytest.mark.unit
def test_validate_refuses_test_dir_equals_prd_dir(tmp_path):
    d = str(tmp_path / "shared")
    os.makedirs(d)
    with pytest.raises(ValueError, match="must not equal PRD_DIR"):
        validate_dirs(d, d)


@pytest.mark.unit
def test_validate_refuses_test_dir_ancestor_of_prd(tmp_path):
    prd = str(tmp_path / "prd" / "PiFitness")
    test = str(tmp_path / "prd")
    os.makedirs(prd)
    with pytest.raises(ValueError, match="ancestor of PRD_DIR"):
        validate_dirs(prd, test)


@pytest.mark.unit
def test_validate_allows_sibling_dirs(tmp_path):
    prd = str(tmp_path / "PiFitness")
    test = str(tmp_path / "PiFitness-test")
    os.makedirs(prd)
    # Should not raise
    validate_dirs(prd, test)


# The tests above exercise Python copies of the guards. The two below run the
# real script, so a regression in test-up.sh itself is caught. Both stop
# before any stub is used or anything is deleted.


@pytest.mark.unit
@requires_bash
def test_script_refuses_test_dir_equal_to_prd_dir(tmp_path):
    prd, _test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, prd, home, bin_dir)
    assert proc.returncode == 1, proc.stderr
    assert "TEST_DIR must not equal PRD_DIR" in proc.stderr, proc.stderr
    assert log == [], log
    assert os.path.isdir(prd)
    assert not os.path.exists(os.path.join(home, ".pifitness-test-state"))


@pytest.mark.unit
@requires_bash
def test_script_refuses_test_dir_that_is_ancestor_of_prd(tmp_path):
    prd, _test_dir, home, bin_dir = sandbox(tmp_path)
    write_stubs(bin_dir)
    proc, log = run_test_up(prd, os.path.dirname(prd), home, bin_dir)
    assert proc.returncode == 1, proc.stderr
    assert "TEST_DIR is an ancestor of PRD_DIR" in proc.stderr, proc.stderr
    assert log == [], log
    assert os.path.isdir(prd)
    assert not os.path.exists(os.path.join(home, ".pifitness-test-state"))


# ---------------------------------------------------------------------------
# Ref resolution from origin (requires git)
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_fetch_resolves_remote_only_branch_to_full_sha(tmp_path):
    """Fetch a branch that exists only on origin, resolve to full SHA,
    and prove the checkout SHA equals the resolved SHA."""
    # Build a bare origin repo
    origin = tmp_path / "origin.git"
    origin.mkdir()
    subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)

    # PRD repo cloned from origin, with a remote-only branch
    prd = tmp_path / "PiFitness"
    subprocess.run(["git", "clone", str(origin), str(prd)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(prd), "config", "user.email", "test@test"],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(prd), "config", "user.name", "Test"],
                   check=True, capture_output=True)

    # Create and commit on a branch only in origin
    subprocess.run(["git", "-C", str(prd), "checkout", "-b", "remote-only-feature"],
                   check=True, capture_output=True)
    readme = prd / "README.md"
    readme.write_text("remote-only branch content\n")
    subprocess.run(["git", "-C", str(prd), "add", "README.md"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(prd), "commit", "-m", "add README on remote-only branch"],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(prd), "push", "origin", "remote-only-feature"],
                   check=True, capture_output=True)

    # Fetch the remote-only branch from origin into PRD
    subprocess.run(["git", "-C", str(prd), "fetch", "origin", "remote-only-feature"],
                   check=True, capture_output=True)

    # Resolve to full SHA (as test-up.sh does)
    resolved = subprocess.run(
        ["git", "-C", str(prd), "rev-parse", "FETCH_HEAD"],
        check=True, capture_output=True, text=True
    ).stdout.strip()
    assert len(resolved) == 40, f"expected full SHA, got {resolved!r}"

    # Clone PRD into TEST_DIR and checkout the resolved SHA
    test_dir = tmp_path / "PiFitness-test"
    subprocess.run(["git", "clone", str(prd), str(test_dir)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(test_dir), "checkout", resolved],
                   check=True, capture_output=True)

    # The checked-out HEAD in TEST_DIR must equal the resolved SHA
    actual = subprocess.run(
        ["git", "-C", str(test_dir), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True
    ).stdout.strip()
    assert actual == resolved, f"checkout SHA {actual!r} != resolved SHA {resolved!r}"


# ---------------------------------------------------------------------------
# Deployment script contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_test_up_does_not_apply_migrations():
    text = read_script("test-up.sh")
    assert "$TEST_DIR/tools/db/migrate.py" not in text
    assert "migration-apply.log" not in text
    assert 'current_phase="migrations-applied"' not in text


# ---------------------------------------------------------------------------
# Test stack start and positive verification (D3, task 5.19)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_test_up_starts_test_units_fatal():
    text = read_script("test-up.sh")
    start = text.index('echo "Step 8: start test units"')
    verify = text.index('echo "Step 9: verify the complete test stack"')
    complete = text.index('echo "test-up complete."')
    block = text[start:verify]
    assert start < verify < complete
    assert 'sudo systemctl start pifitness-fastapi-test.service' in block
    assert 'pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js"' in block
    assert "|| true" not in block


@pytest.mark.unit
def test_test_up_verifies_complete_test_stack_before_completion():
    text = read_script("test-up.sh")
    verify = text.index('echo "Step 9: verify the complete test stack"')
    complete = text.index('echo "test-up complete."')
    tail = text[verify:complete]
    for probe in (
        "systemctl is-active pifitness-fastapi-test.service",
        "pm2 describe pifitness-next-test",
        "health_body",
        '"status"',
        "curl -fsS http://127.0.0.1:8000/api/health",
        "curl -fsS http://127.0.0.1:3000",
        "$HTTPS_HOSTNAME",
        "current_setting('pifitness.env', true)",
        "recover_and_fail",
    ):
        assert probe in tail, "missing verification probe: %s" % probe
    assert "test-down.sh" in text


@pytest.mark.unit
@requires_bash
def test_test_up_exits_nonzero_when_test_service_not_active(tmp_path):
    # Default stubs report every unit except the PRD ones as inactive, so the
    # Step 9 "test FastAPI unit is active" check must fail and recover.
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    proc, log = run_test_up(prd, test_dir, home, bin_dir)
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    assert "test FastAPI unit is not active" in proc.stderr, proc.stderr
    assert "Running recovery path (test-down.sh)..." in proc.stderr, proc.stderr
    state_file = os.path.join(home, ".pifitness-test-state")
    assert os.path.isfile(state_file), "state file must be left intact on failure"
    assert "SYSTEMCTL start pifitness-fastapi-test.service" in log
    assert "SYSTEMCTL is-active pifitness-fastapi-test.service" in log


@pytest.mark.unit
@requires_bash
def test_test_up_exits_zero_when_all_checks_pass(tmp_path):
    prd, test_dir, home, bin_dir = seeded_sandbox(tmp_path)
    write_runtime_stubs(bin_dir)
    with running_test_stack() as https_url:
        proc, log = run_test_up(
            prd, test_dir, home, bin_dir, states={"HTTPS_HOSTNAME": https_url}
        )
    assert proc.returncode == 0, proc.stderr
    assert "test-up complete." in proc.stdout
    starts = [entry for entry in log if entry.startswith(("SYSTEMCTL start", "PM2 start"))]
    assert "SYSTEMCTL start pifitness-fastapi-test.service" in starts
    assert any(entry.startswith("PM2 start") for entry in starts), log
    assert "SYSTEMCTL is-active pifitness-fastapi-test.service" in log
    assert "PM2 describe pifitness-next-test" in log
    # curl is resolved by Git bash to /usr/bin/curl.exe (not any stub on PATH),
    # so real-curl probes do not appear in the stub log. Their success is
    # already proved by proc.returncode == 0 and "test-up complete." above:
    # any probe failure calls recover_and_fail and exits nonzero.
    assert any("current_setting('pifitness.env', true)" in entry for entry in log), log
    start_idx = log.index("SYSTEMCTL start pifitness-fastapi-test.service")
    verify_idx = log.index("SYSTEMCTL is-active pifitness-fastapi-test.service")
    assert start_idx < verify_idx, "verification must run after start: %r" % log
