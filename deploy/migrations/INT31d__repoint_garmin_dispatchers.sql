-- INT-31d production migration: repoint garmin dispatcher references from the
-- deleted backend_functions.service_logins module to the canonical home in
-- pifitness.integrations.garmin.provider.
--
-- Two dispatch values need repointing:
--   * backend_functions.service_logins.get_garmin_client
--       -> pifitness.integrations.garmin.provider.get_garmin_client
--   * backend_functions.service_logins.pirate_garmin_login
--       -> pifitness.integrations.garmin.provider.garmin_login
--
-- Why only api_service_list is updated:
--   tasks.vw_task_info is a complex (non-updatable) view over
--   api_services.api_service_list and api_services.function_library. It projects
--   sl.api_service_function AS python_login_function. Updating api_service_list
--   auto-reflects through the view. No UPDATE against the view is required.
--
-- No code home for backend_functions.service_logins exists as of INT-28b.
-- Applied unmodified against production; never edit after apply.
--
-- Rollback: see INT31d__repoint_garmin_dispatchers_rollback.sql

BEGIN;

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.garmin.provider.get_garmin_client'
WHERE api_service_function = 'backend_functions.service_logins.get_garmin_client';

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.garmin.provider.garmin_login'
WHERE api_service_function = 'backend_functions.service_logins.pirate_garmin_login';

COMMIT;