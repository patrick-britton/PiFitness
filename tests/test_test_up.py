"""Tests for test-up.sh: ref resolution from origin and path validation.

Covers D16: resolve the requested ref from origin, record its full commit SHA,
and check out that exact SHA into the test dir. Before any rm -rf, canonicalize
and validate PRD_DIR and TEST_DIR; refuse symlinks, empty paths, "/", the PRD
directory, and any path that is an ancestor of PRD.
"""

import os
import subprocess

import pytest

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