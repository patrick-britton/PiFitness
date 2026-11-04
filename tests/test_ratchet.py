#!/usr/bin/env python3
"""Tests for tools/ratchet.py."""
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path.cwd()
RATCHET = [sys.executable, "tools/ratchet.py"]


def run_ratchet(*args, input_text=None):
    return subprocess.run(
        [*RATCHET, *args],
        capture_output=True,
        text=True,
        input=input_text,
        cwd=REPO_ROOT,
    )


def write_baseline(path: Path, keys):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sorted(keys)) + ("\n" if keys else ""))


@pytest.mark.unit
def test_compare_no_new_violations_exits_0(tmp_path):
    """No new violations -> exit 0."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234", "check|b.py|rule2|efgh5678"])
    result = run_ratchet("compare", "mytest",
                         "check|a.py|rule1|abcd1234",
                         "check|b.py|rule2|efgh5678",
                         "--baseline", str(bl))
    assert result.returncode == 0
    assert result.stdout.strip() == ""


@pytest.mark.unit
def test_compare_planted_new_key_fails(tmp_path):
    """A planted new key (not in baseline) -> exit 1 and prints the key."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234"])
    result = run_ratchet("compare", "mytest",
                         "check|a.py|rule1|abcd1234",
                         "check|c.py|rule3|ijkl9012",
                         "--baseline", str(bl))
    assert result.returncode == 1
    assert "check|c.py|rule3|ijkl9012" in result.stdout


@pytest.mark.unit
def test_compare_removed_key_warns_prune(tmp_path):
    """A removed key (in baseline but not in current) -> PRUNE warning, exit 0."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234", "check|b.py|rule2|efgh5678"])
    result = run_ratchet("compare", "mytest",
                         "check|a.py|rule1|abcd1234",
                         "--baseline", str(bl))
    assert result.returncode == 0
    assert "PRUNE" in result.stdout
    assert "check|b.py|rule2|efgh5678" in result.stdout


@pytest.mark.unit
def test_compare_stdin_keys(tmp_path):
    """Keys read from stdin when no positional keys given."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234"])
    result = run_ratchet("compare", "mytest", "--baseline", str(bl),
                         input_text="check|a.py|rule1|abcd1234\ncheck|c.py|rule3|ijkl9012\n")
    assert result.returncode == 1
    assert "check|c.py|rule3|ijkl9012" in result.stdout


@pytest.mark.unit
def test_prune_removes_stale_keys(tmp_path):
    """prune removes keys no longer present."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234", "check|b.py|rule2|efgh5678"])
    result = run_ratchet("prune", "mytest", "--baseline", str(bl),
                         input_text="check|a.py|rule1|abcd1234\n")
    assert result.returncode == 0
    remaining = bl.read_text().strip().splitlines()
    assert remaining == ["check|a.py|rule1|abcd1234"]


@pytest.mark.unit
def test_update_baseline_removes_stale_only(tmp_path):
    """--update-baseline removes stale keys but does not add new ones without --allow-grow."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234", "check|b.py|rule2|efgh5678"])
    result = run_ratchet("compare", "mytest",
                         "check|a.py|rule1|abcd1234",
                         "check|c.py|rule3|ijkl9012",
                         "--update-baseline",
                         "--baseline", str(bl))
    assert result.returncode == 0
    remaining = bl.read_text().strip().splitlines()
    assert remaining == ["check|a.py|rule1|abcd1234"]
    assert "check|c.py|rule3|ijkl9012" not in remaining


@pytest.mark.unit
def test_update_baseline_with_allow_grow_adds_new_keys(tmp_path):
    """--update-baseline --allow-grow adds new keys."""
    bl = tmp_path / "mytest.txt"
    write_baseline(bl, ["check|a.py|rule1|abcd1234"])
    result = run_ratchet("compare", "mytest",
                         "check|a.py|rule1|abcd1234",
                         "check|c.py|rule3|ijkl9012",
                         "--update-baseline", "--allow-grow",
                         "--baseline", str(bl))
    assert result.returncode == 0
    remaining = bl.read_text().strip().splitlines()
    assert remaining == [
        "check|a.py|rule1|abcd1234",
        "check|c.py|rule3|ijkl9012",
    ]