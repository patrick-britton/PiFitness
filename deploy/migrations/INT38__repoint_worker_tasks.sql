-- INT-38 production migration: repoint python_function values in tasks.task_config
-- from backend_functions.<moved_module> to pifitness.worker.<moved_module>.
--
-- Four modules moved from backend/backend_functions/ to backend/pifitness/worker/:
--   backend_functions.backend_tasks      -> pifitness.worker.backend_tasks
--   backend_functions.task_execution     -> pifitness.worker.task_execution
--   backend_functions.ultimate_task_executioner_v2 -> pifitness.worker.ultimate_task_executioner_v2
--   backend_functions.json_extractors    -> pifitness.worker.json_extractors
--
-- Only json_extractors appears in tasks.task_config.python_function values.
-- task_execution.py reads python_function dynamically via importlib (lines 108, 146),
-- so updating the DB column auto-reflects at runtime. No UPDATE against the view
-- is required.
--
-- backend_tasks has no python_function entries (grep found zero references),
-- but the module path is documented here for completeness.
--
-- Applied unmodified against production; never edit after apply.
--
-- Rollback: see INT38__repoint_worker_tasks_rollback.sql

BEGIN;

UPDATE tasks.task_config
SET python_function = REPLACE(python_function, 'backend_functions.json_extractors', 'pifitness.worker.json_extractors')
WHERE python_function LIKE '%backend_functions.json_extractors%';

COMMIT;