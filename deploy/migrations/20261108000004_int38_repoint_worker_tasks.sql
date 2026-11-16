-- INT-38 production migration: repoint python_function values in tasks.task_config
-- from backend_functions.<moved_module> to pifitness.worker.<moved_module>.
--
-- Four modules moved from backend/backend_functions/ to backend/pifitness/worker/:
--   backend_functions.backend_tasks      -> pifitness.worker.backend_tasks
--   backend_functions.task_execution     -> pifitness.worker.task_execution
--   backend_functions.ultimate_task_executioner_v2 -> pifitness.worker.ultimate_task_executioner_v2
--   backend_functions.json_extractors    -> pifitness.worker.json_extractors
--
-- json_extractors appears in tasks.task_config.python_function values.
-- task_execution.py reads python_function dynamically via importlib (lines 108, 146),
-- so updating the DB column auto-reflects at runtime. No UPDATE against the view
-- is required.
--
-- backend_tasks has 2 rows in dev DB (backup_database, nightly_maintenance).
-- Earlier analysis incorrectly claimed zero based on code grep; DB state is
-- authoritative and requires explicit UPDATE for each function.
--
-- Applied unmodified against production; never edit after apply.
--
-- Rollback: see -- migrate:down section below.
--
-- migrate:up

-- json_extractors (per INT-38 original -- rows existed when this migration was designed)
UPDATE tasks.task_config
SET python_function = REPLACE(python_function, 'backend_functions.json_extractors', 'pifitness.worker.json_extractors')
WHERE python_function LIKE '%backend_functions.json_extractors%';

-- backend_tasks.backup_database (1 row in dev DB as of INT-38b)
UPDATE tasks.task_config
SET python_function = 'pifitness.worker.backend_tasks.backup_database'
WHERE python_function = 'backend_functions.backend_tasks.backup_database';

-- backend_tasks.nightly_maintenance (1 row in dev DB as of INT-38b)
UPDATE tasks.task_config
SET python_function = 'pifitness.worker.backend_tasks.nightly_maintenance'
WHERE python_function = 'backend_functions.backend_tasks.nightly_maintenance';

-- migrate:down
-- INT-38 rollback: revert the python_function repointing in tasks.task_config.
-- Restores pifitness.worker.json_extractors -> backend_functions.json_extractors.
-- backend_functions.backend_tasks -> pifitness.worker.backend_tasks (documented).
-- Only safe to run if INT38__repoint_worker_tasks.sql was applied.
-- After rollback, backend_functions.json_extractors.* module no longer exists
-- in current code (moved to pifitness.worker); this rollback exists for
-- paired-forward/rollback discipline only.

UPDATE tasks.task_config
SET python_function = 'backend_functions.json_extractors'
WHERE python_function = 'pifitness.worker.json_extractors';

UPDATE tasks.task_config
SET python_function = 'backend_functions.backend_tasks.backup_database'
WHERE python_function = 'pifitness.worker.backend_tasks.backup_database';

UPDATE tasks.task_config
SET python_function = 'backend_functions.backend_tasks.nightly_maintenance'
WHERE python_function = 'pifitness.worker.backend_tasks.nightly_maintenance';
