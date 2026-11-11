-- INT-35 production migration: repoint pirate dispatcher references from the
-- deleted backend_functions.service_logins module to the canonical home in
-- pifitness.integrations.garmin.provider.
--
-- Two dispatch values need repointing:
--   * backend_functions.service_logins.pirate_garmin_login
--       -> pifitness.integrations.garmin.provider.garmin_login
--   * extract_pirate_universal -> extract_garmin_universal (in function_library,
--     which feeds the non-updatable tasks.vw_task_info view)
--
-- Why only api_service_list and function_library are updated:
--   tasks.vw_task_info is a complex (non-updatable) view over
--   api_services.api_service_list and api_services.function_library. It projects
--   sl.api_service_function AS python_login_function and
--   fl.python_extraction_function. Updating the underlying tables auto-reflects
--   through the view. No UPDATE against the view is required.
--
-- No code home for backend_functions.service_logins exists as of INT-28b.
-- Source module was deleted at INT-28b.
-- Applied unmodified against production; never edit after apply.
--
-- Rollback: see -- migrate:down section below.
--
-- migrate:up

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.garmin.provider.garmin_login'
WHERE api_service_function = 'backend_functions.service_logins.pirate_garmin_login';

UPDATE api_services.function_library
SET python_extraction_function = 'extract_garmin_universal'
WHERE python_extraction_function = 'extract_pirate_universal';

-- migrate:down
-- INT-35 rollback: restore pirate dispatcher references to the
-- deleted backend_functions.service_logins module.
--
-- Exact inverse of INT35__repoint_pirate_dispatchers.sql.

UPDATE api_services.api_service_list
SET api_service_function = 'backend_functions.service_logins.pirate_garmin_login'
WHERE api_service_function = 'pifitness.integrations.garmin.provider.garmin_login';

UPDATE api_services.function_library
SET python_extraction_function = 'extract_pirate_universal'
WHERE python_extraction_function = 'extract_garmin_universal';
