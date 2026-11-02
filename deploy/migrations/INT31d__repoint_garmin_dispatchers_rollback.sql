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