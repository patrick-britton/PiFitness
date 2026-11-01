-- INT-18-live migration: rename DB dispatch references from pirate_garmin_login to garmin_login.
-- Run ON THE PI (or any host that can reach Postgres at 192.168.86.104).
-- Run AFTER INT-18 commits; run BEFORE INT-18-final.
-- Confirms zero remaining rows reference the old name.

-- 0. Inspect the base table behind tasks.vw_task_info (table names below are
--    placeholders; adjust to match \d+ tasks.vw_task_info and
--    \d+ api_services.api_service_list before running):
--   \d+ tasks.vw_task_info
--   \d+ tasks.task_info
--   \d+ tasks.task_definition
--   \d+ api_services.api_service_list

-- 1. Update tasks base table:
UPDATE tasks.task_info
   SET python_login_function = 'backend_functions.service_logins.garmin_login'
 WHERE python_login_function = 'backend_functions.service_logins.pirate_garmin_login';

-- 2. Update api_services base table:
UPDATE api_services.api_service_list
   SET api_service_function = 'backend_functions.service_logins.garmin_login'
 WHERE api_service_function = 'backend_functions.service_logins.pirate_garmin_login';

-- 3. Verify zero remaining references:
SELECT 'tasks.task_info' AS source, count(*) AS remaining
  FROM tasks.task_info
 WHERE python_login_function = 'backend_functions.service_logins.pirate_garmin_login'
UNION ALL
SELECT 'api_services.api_service_list', count(*)
  FROM api_services.api_service_list
 WHERE api_service_function = 'backend_functions.service_logins.pirate_garmin_login';

-- Expected: both counts are 0.
