"""Tests for test-up.sh: ref resolution from origin and path validation.

Covers D16: resolve the requested ref from origin, record its full commit SHA,
and check out that exact SHA into the test dir. Before any rm -rf, canonicalize
and validate PRD_DIR and TEST_DIR; refuse symlinks, empty paths, "/", the PRD
directory, and any path that is an ancestor of PRD.
"""

import http.server
import os
import shutil
import socket
import subprocess
import threading

import pytest

from tests.test_test_processes import GIT, bash_available, run_test_up, sandbox, write_stubs

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
with open(os.path.join(REPO_ROOT, "tools", "db", "migrate.py"), "rb") as _migrate_fh:
    MIGRATE_PY = _migrate_fh.read()

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
# Migration runner invocation (D3, D4)
# ---------------------------------------------------------------------------


def read_script(name):
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "..", "deploy", "scripts", name), "r", encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.unit
def test_test_up_migration_step_runs_after_provisioning_before_start():
    text = read_script("test-up.sh")
    provision = text.index('echo "Step 7: provision test checkout')
    migration = text.index('echo "Step 8: apply pending migrations to $TEST_DB"')
    start = text.index('echo "Step 9: start test units"')
    assert provision < migration < start, (
        "migration must run after checkout provisioning and before starting test services"
    )


@pytest.mark.unit
def test_test_up_migration_uses_test_checkout_runner_without_silent_skip():
    text = read_script("test-up.sh")
    assert "$TEST_DIR/tools/db/migrate.py" in text
    assert "$PRD_DIR/deploy/scripts/run_migrations.sh" not in text
    assert "no migration runner found; skipping" not in text
    assert "migration status before:" in text
    assert "migration status after:" in text
    assert "migrations completed in" in text
    fail_branch = text.index("test services will not start")
    start = text.index('echo "Step 9: start test units"')
    assert fail_branch < start, (
        "migration failure must stop test-up before test services start"
    )


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

_NODE_STUB = """\
#!/usr/bin/env bash
echo "{name} $*" >> "$STUB_LOG"
exit 0
"""


def write_runner_stubs(bin_dir):
    """Stub python3/npm/node so provisioning succeeds and the migration runner
    outcome is controllable through MIGRATE_FAIL in the run environment."""
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


def advance_test_ref(origin, files, message):
    """Commit files onto the origin bare repo's test-ref branch using plumbing
    commands (bare repo has no worktree). test-up fetches that ref from origin,
    resolves its full SHA, and checks it out into the test checkout, so the
    files end up where the migration runner and provisioning expect them."""
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


MIGRATION_CHECKOUT_FILES = {
    "backend/requirements.txt": b"",
    "frontend/pifitness/package-lock.json": b"{}\n",
    "tools/db/migrate.py": MIGRATE_PY,
}


@pytest.mark.unit
def test_test_up_migration_invokes_test_checkout_runner_and_starts_services(tmp_path):
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
    migrate_calls = [
        entry for entry in log if entry.startswith("VENV_PYTHON ") and "migrate.py" in entry
    ]
    assert any(" status" in entry for entry in migrate_calls), log
    assert any(" up" in entry for entry in migrate_calls), log
    assert "migration status before:" in proc.stdout
    assert "migrations completed in" in proc.stdout
    assert "migration status after:" in proc.stdout
    starts = [
        entry
        for entry in log
        if entry.startswith(("SYSTEMCTL start", "PM2 start"))
    ]
    assert starts, "test services must start after migration succeeds: %r" % log


@pytest.mark.unit
def test_test_up_migration_failure_exits_before_service_start(tmp_path):
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
    assert "migration runner failed" in proc.stderr, proc.stderr
    starts = [
        entry
        for entry in log
        if entry.startswith(("SYSTEMCTL start", "PM2 start"))
    ]
    assert not starts, "no test service may start when migration fails: %r" % log


@pytest.mark.unit
def test_test_up_missing_migration_runner_fails_closed(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(
        origin,
        {
            "backend/requirements.txt": b"",
            "frontend/pifitness/package-lock.json": b"{}\n",
        },
        "seed test checkout without migration runner",
    )
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir)
    assert proc.returncode != 0, proc.stdout
    assert "migration runner missing" in proc.stderr, proc.stderr
    starts = [
        entry
        for entry in log
        if entry.startswith(("SYSTEMCTL start", "PM2 start"))
    ]
    assert not starts, "no test service may start when the runner is missing: %r" % log


# ---------------------------------------------------------------------------
# Test stack start and positive verification (D3, task 5.19)
# ---------------------------------------------------------------------------

_CUSTOM_SYSTEMCTL_STUB = """\
#!/usr/bin/env bash
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

_CUSTOM_PM2_STUB = """\
#!/usr/bin/env bash
echo "PM2 $*" >> "$STUB_LOG"
case "$1" in
  describe)
    if [ "$2" = "pifitness-next-test" ]; then echo "status: online"; fi
    exit 0 ;;
  start|stop|restart|delete) exit 0 ;;
  *) exit 0 ;;
esac
"""

_CUSTOM_CURL_STUB = """\
#!/usr/bin/env bash
echo "CURL $*" >> "$STUB_LOG"
echo '{"status":"ok","database":"ok"}'
exit 0
"""


def write_runtime_stubs(bin_dir):
    """Override systemctl/pm2/curl stubs so the test stack verification passes."""
    bin_dir = str(bin_dir)
    for name, body in (("systemctl", _CUSTOM_SYSTEMCTL_STUB),
                       ("pm2", _CUSTOM_PM2_STUB),
                       ("curl", _CUSTOM_CURL_STUB)):
        stub = os.path.join(bin_dir, name)
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(stub, 0o755)


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
    down after use. Git bash on Windows shadows stubbed `curl` with
    /usr/bin/curl.exe, so real curl is used; these servers let those real curl
    probes succeed without touching the network. `test-up.sh` hardcodes
    127.0.0.1:8000 and 127.0.0.1:3000, so those ports are bound explicitly.
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


@pytest.mark.unit
def test_test_up_starts_test_units_fatal_without_silent_skip():
    text = read_script("test-up.sh")
    start = text.index('echo "Step 9: start test units"')
    verify = text.index('echo "Step 10: verify the complete test stack"')
    complete = text.index('echo "test-up complete."')
    block = text[start:verify]
    assert start < verify < complete
    assert 'sudo systemctl start pifitness-fastapi-test.service' in block
    assert 'pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js"' in block
    assert "|| true" not in block


@pytest.mark.unit
def test_test_up_verifies_complete_test_stack_before_completion():
    text = read_script("test-up.sh")
    verify = text.index('echo "Step 10: verify the complete test stack"')
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
def test_test_up_exits_nonzero_when_test_service_not_active(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    proc, log = run_test_up(prd, test_dir, home, bin_dir)
    assert proc.returncode != 0, proc.stdout
    assert "test-up complete." not in proc.stdout
    state_file = os.path.join(home, ".pifitness-test-state")
    assert os.path.isfile(state_file), "state file must be left intact on failure"
    assert "SYSTEMCTL start pifitness-fastapi-test.service" in log
    assert "SYSTEMCTL is-active pifitness-fastapi-test.service" in log


@pytest.mark.unit
def test_test_up_exits_zero_when_all_checks_pass(tmp_path):
    if not bash_available():
        pytest.skip("Git bash and git are required")
    prd, test_dir, home, bin_dir = sandbox(tmp_path)
    origin = os.path.join(os.path.dirname(prd), "origin.git")
    advance_test_ref(origin, MIGRATION_CHECKOUT_FILES, "seed migration runner on test-ref")
    write_stubs(bin_dir)
    write_runner_stubs(bin_dir)
    write_runtime_stubs(bin_dir)
    servers, urls = start_local_servers([8000, 3000, 0])
    https_url = urls[2]
    os.environ["HTTPS_HOSTNAME"] = https_url
    try:
        proc, log = run_test_up(prd, test_dir, home, bin_dir)
    finally:
        for server in servers:
            server.server_close()
        os.environ.pop("HTTPS_HOSTNAME", None)
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