# Compat shim — backend_tasks moved to pifitness.worker.backend_tasks (INT-38).
# Re-exports all public names for backward compatibility until callers are migrated.
from pifitness.worker.backend_tasks import (
    nightly_maintenance,
    backup_database,
    run_health_checks,
)

__all__ = [
    'nightly_maintenance',
    'backup_database',
    'run_health_checks',
]