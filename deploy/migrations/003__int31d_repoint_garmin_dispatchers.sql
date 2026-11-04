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
-- Rollback: see -- migrate:down section below.
--
-- migrate:up

BEGIN;

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.garmin.provider.get_garmin_client'
WHERE api_service_function = 'backend_functions.service_logins.get_garmin_client';

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.garmin.provider.garmin_login'
WHERE api_service_function = 'backend_functions.service_logins.pirate_garmin_login';

COMMIT;

-- migrate:down
-- INT-31d rollback: revert the garmin dispatcher repointing.
-- Restores the two api_service_list rows to their pre-INT-31d values.
-- Only safe to run if INT31d__repoint_garmin_dispatchers.sql was applied.
-- After rollback, backend_functions.service_logins.* rows exist again -- note
-- that module no longer exists in current code; this rollback exists for
-- paired-forward/rollback discipline only.

BEGIN;

UPDATE api_services.api_service_list
SET api_service_function = 'backend_functions.service_logins.get_garmin_client'
WHERE api_service_function = 'pifitness.integrations.garmin.provider.get_garmin_client';

UPDATE api_services.api_service_list
SET api_service_function = 'backend_functions.service_logins.pirate_garmin_login'
WHERE api_service_function = 'pifitness.integrations.garmin.provider.garmin_login';

COMMIT;
