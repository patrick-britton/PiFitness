-- INT-38 rollback: revert the python_function repointing in tasks.task_config.
-- Restores pifitness.worker.json_extractors -> backend_functions.json_extractors.
-- backend_functions.backend_tasks -> pifitness.worker.backend_tasks (documented).
-- Only safe to run if INT38__repoint_worker_tasks.sql was applied.
-- After rollback, backend_functions.json_extractors.* module no longer exists
-- in current code (moved to pifitness.worker); this rollback exists for
-- paired-forward/rollback discipline only.

BEGIN;

UPDATE tasks.task_config
SET python_function = 'backend_functions.json_extractors'
WHERE python_function = 'pifitness.worker.json_extractors';

UPDATE tasks.task_config
SET python_function = 'backend_functions.backend_tasks.backup_database'
WHERE python_function = 'pifitness.worker.backend_tasks.backup_database';

UPDATE tasks.task_config
SET python_function = 'backend_functions.backend_tasks.nightly_maintenance'
WHERE python_function = 'pifitness.worker.backend_tasks.nightly_maintenance';

COMMIT;
