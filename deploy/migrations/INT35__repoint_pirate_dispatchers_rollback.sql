-- INT-35 rollback: restore pirate dispatcher references to the
-- deleted backend_functions.service_logins module.
--
-- Exact inverse of INT35__repoint_pirate_dispatchers.sql.
--
BEGIN;

UPDATE api_services.api_service_list
SET api_service_function = 'backend_functions.service_logins.pirate_garmin_login'
WHERE api_service_function = 'pifitness.integrations.garmin.provider.garmin_login';

UPDATE api_services.function_library
SET python_extraction_function = 'extract_pirate_universal'
WHERE python_extraction_function = 'extract_garmin_universal';

COMMIT;