"""Contract tests for bootstrap's direct TEST database replacement integration."""

from pathlib import Path
import pytest

pytestmark = pytest.mark.unit
REPO_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = (REPO_ROOT / "deploy" / "scripts" / "bootstrap.sh").read_text(encoding="utf-8")


def test_recovery_helper_is_invoked_for_create_refresh_and_opt_in_freshness():
    assert 'create_from_prd|refresh_from_prd)' in BOOTSTRAP
    assert 'NEEDS_DB_RECOVERY=1' in BOOTSTRAP
    assert 'restore_test_db.py' in BOOTSTRAP
    assert 'restore \\\n            --backup-dir "$PRD_BACKUP_DIR"' in BOOTSTRAP
    assert '--backup-glob "$PRD_BACKUP_GLOB"' in BOOTSTRAP
    assert 'n|no|"")' in BOOTSTRAP


def test_recovery_uses_historical_backup_default_with_explicit_overrides():
    assert 'PIFITNESS_PRD_BACKUP_DIR:-$HOME/Documents/DB_Backups' in BOOTSTRAP
    assert 'PIFITNESS_PRD_BACKUP_GLOB:-pifitness_prd_*.dump' in BOOTSTRAP
    assert 'pg_dump' not in BOOTSTRAP


def test_post_recovery_inspection_requires_test_marker_and_valid_baseline_provenance():
    assert 'inspect_test_db.py' in BOOTSTRAP
    assert 'test.get("marker") != "test"' in BOOTSTRAP
    assert 'comparison.get("status") not in {"identical", "restored_baseline", "migration_managed"}' in BOOTSTRAP
    assert 'decision.get("action") != "reuse_test"' in BOOTSTRAP
    assert 'PRD schema comparison: identical' not in BOOTSTRAP


def test_failed_later_bootstrap_step_keeps_verified_prd_baseline():
    cleanup = BOOTSTRAP[BOOTSTRAP.index('cleanup() {'):BOOTSTRAP.index('\ntrap cleanup EXIT')]
    assert 'exit_status=$?' in cleanup
    assert 'verified PRD-derived TEST baseline is retained' in cleanup
    assert 'rollback --state-file' not in cleanup


def test_direct_restore_and_verification_run_before_staged_checkout_is_promoted():
    restore = BOOTSTRAP.index('restore_test_db.py')
    post_inspection = BOOTSTRAP.index('Verifying restored TEST database identity, permissions, and baseline provenance')
    preserve_checkout = BOOTSTRAP.index('git -C "$SOURCE_REPO" worktree move "$APP_DIR" "$PREVIOUS_DIR"')
    assert restore < post_inspection < preserve_checkout
    assert 'Previous TEST database discarded after successful replacement verification.' in BOOTSTRAP
    assert 'PRD-derived TEST baseline remains in place' in BOOTSTRAP
    assert '--max-backup-age-hours 24' in BOOTSTRAP


def test_no_migration_or_service_lifecycle_was_added_to_this_database_step():
    assert 'migrate.py up' not in BOOTSTRAP
    assert 'systemctl stop ' not in BOOTSTRAP
    assert 'systemctl start ' not in BOOTSTRAP
    assert 'pm2 stop ' not in BOOTSTRAP
    assert 'pm2 start ' not in BOOTSTRAP


def test_interrupted_db_swap_is_reconciled_before_initial_inspection():
    reconcile = BOOTSTRAP.index("recover-pending --state-dir")
    inspect = BOOTSTRAP.index('echo "Inspecting TEST database against PRD')
    assert reconcile < inspect
    assert 'test-db-recovery-reconcile-' in BOOTSTRAP


def test_inspector_receives_baseline_journal_directory_for_migration_retries():
    assert '--baseline-state-dir "$EVIDENCE_DIR"' in BOOTSTRAP
    assert 'migration-managed' in BOOTSTRAP
    assert 'checksums verified' in BOOTSTRAP


def test_recovery_failure_cleanup_does_not_claim_a_verified_baseline():
    cleanup = BOOTSTRAP[BOOTSTRAP.index('cleanup() {'):BOOTSTRAP.index('\ntrap cleanup EXIT')]
    assert 'recovery_status' in cleanup
    assert '[[ "$recovery_status" == "baseline_ready" ]]' in cleanup
    assert 'No verified baseline is being claimed.' in cleanup
