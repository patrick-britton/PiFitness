"""Contract tests for the TEST bootstrap's read-only database decision gate."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "deploy" / "scripts" / "bootstrap.sh"


def read_script() -> str:
    return SCRIPT_PATH.read_text(encoding="utf-8")


def function_body(text: str, name: str, next_marker: str) -> str:
    start = text.index(name)
    end = text.index(next_marker, start)
    return text[start:end]


def test_schema_comparison_dependency_is_in_the_locked_deployment_group():
    text = read_script()
    uv_sync = text.index('"$UV_BIN" sync')
    version_check = text.index('[[ "$MIGRADIFF_VERSION" == "1.7.2" ]]')
    inspector = text.index('inspect_test_db.py')
    sync_end = text.index('\n)', uv_sync)
    sync_command = text[uv_sync:sync_end]

    assert uv_sync < version_check < inspector
    assert "--locked" in sync_command
    assert "--no-dev" in sync_command
    assert "--group deployment" in sync_command
    assert "--no-editable" in sync_command
    assert '"$UV_BIN" pip install' not in text
    assert "requirements-schema-compare.txt" not in text
    assert 'version("migradiff")' in text

    pyproject = (REPO_ROOT / "backend" / "pyproject.toml").read_text(encoding="utf-8")
    assert "[dependency-groups]" in pyproject
    assert "deployment = [" in pyproject
    assert '"migradiff[pg]==1.7.2"' in pyproject


def test_bootstrap_uses_the_two_existing_environment_files():
    text = read_script()
    assert 'ENV_FILE="/home/god/.config/pifitness/test.env"' in text
    assert 'PRD_ENV_FILE="$SOURCE_REPO/backend/.env"' in text
    assert '--test-env-file "$ENV_FILE"' in text
    assert '--prd-env-file "$PRD_ENV_FILE"' in text
    assert 'test-migrate.env' not in text
    assert 'prd-migrate.env' not in text


def test_inspector_is_invoked_read_only_with_prd_authorization_and_evidence():
    text = read_script()
    invoke = text.index('inspect_test_db.py')
    end = text.index('); then', invoke)
    invocation = text[invoke:end]

    assert '--allow-prd' in invocation
    assert '--diff-file "$SCHEMA_DIFF_FILE"' in invocation
    assert 'INSPECTION_REPORT=' in text
    assert 'chmod 0600 "$INSPECTION_REPORT"' in text
    assert 'No database or existing checkout was modified.' in text


def test_database_inspection_precedes_transactional_checkout_promotion():
    text = read_script()
    inspector = text.index('inspect_test_db.py')
    decision = text.index('case "$DB_ACTION" in', inspector)
    preserve_old = text.index('git -C "$SOURCE_REPO" worktree move "$APP_DIR" "$PREVIOUS_DIR"')
    promote_stage = text.index('git -C "$SOURCE_REPO" worktree move "$STAGE_DIR" "$APP_DIR"')
    restore_old = text.index('git -C "$SOURCE_REPO" worktree move "$PREVIOUS_DIR" "$APP_DIR"')
    cleanup_old = text.index('git -C "$SOURCE_REPO" worktree remove --force "$PREVIOUS_DIR"')

    assert inspector < decision < preserve_old < promote_stage < restore_old < cleanup_old
    assert 'Staged checkout promotion failed. Previous TEST checkout was restored.' in text
    assert 'manual recovery is required' in text


def test_policy_actions_are_all_handled_and_route_refreshes_to_recovery():
    text = read_script()
    decision_block = text[text.index('case "$DB_ACTION" in'):]

    for action in ("reuse_test", "create_from_prd|refresh_from_prd", "abort"):
        assert action in decision_block

    assert "unknown decision" in decision_block
    assert "latest PRD nightly backup" in decision_block
    assert 'restore_test_db.py' in decision_block
    assert 'rollback --state-file "$DB_RECOVERY_STATE_FILE"' not in text
    assert 'verified PRD-derived TEST baseline is retained' in text


def test_compatible_database_prompts_for_optional_freshness_refresh():
    text = read_script()
    assert '[[ "$DB_OFFER_REFRESH" == "true" ]]' in text
    assert '[[ -t 0 ]]' in text
    assert 'read -r -p "TEST schema is compatible with PRD.' in text
    assert '[y/N]' in text
    assert 'n|no|"")' in text


def test_failed_preflight_removes_only_the_staging_worktree():
    text = read_script()
    cleanup = function_body(text, "cleanup() {", "trap cleanup EXIT")
    assert 'git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR"' in cleanup
    assert 'git -C "$SOURCE_REPO" worktree move' not in cleanup
    assert 'trap cleanup EXIT' in text
    assert 'trap - EXIT' in text  # disarmed only after successful promotion


def test_old_prd_dump_requirement_and_automatic_migration_are_not_reintroduced():
    text = read_script()
    assert "pg_dump" not in text
    assert "pg_restore" not in text
    assert "dropdb" not in text
    assert "createdb" not in text
    assert "migrate.py up" not in text
    assert "--apply" not in text


def test_current_database_gate_does_not_start_or_stop_services():
    text = read_script()
    assert 'systemctl is-active --quiet pifitness-fastapi-test.service' in text
    assert 'systemctl stop ' not in text
    assert 'systemctl start ' not in text
    assert 'pm2 stop ' not in text
    assert 'pm2 start ' not in text


def test_pg_dump_is_not_a_preflight_executable_requirement():
    text = read_script()
    assert 'command -v pg_dump' not in text
    assert 'Required PostgreSQL executable pg_dump is missing' not in text


def test_bootstrap_renders_inspector_failure_as_human_diagnostic_and_keeps_json_evidence():
    text = read_script()
    failure = text[text.index('if ! (', text.index('INSPECTION_REPORT=')):text.index('chmod 0600 "$INSPECTION_REPORT"')]
    assert 'cat "$INSPECTION_REPORT"' not in failure
    assert 'cat "$INSPECTION_ERROR"' in failure
    assert 'Structured inspection report:' in failure
    assert 'No database or existing checkout was modified' in failure


def test_bootstrap_prints_inspection_summary_instead_of_raw_json():
    text = read_script()
    assert 'PRD: %s (marker %s)' in text
    assert 'TEST: %s (marker %s)' in text
    assert 'Schema comparison:' in text
    assert 'Database decision:' in text
    assert 'cat "$INSPECTION_REPORT"' not in text
