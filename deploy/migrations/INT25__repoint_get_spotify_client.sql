-- INT-25 production migration: repoint DB dispatch references from
--   backend_functions.service_logins.get_spotify_client
-- to
--   pifitness.integrations.spotify.provider.get_spotify_client.
-- Unexecuted in dev; run at production merge (INT-18-live precedent).
-- Dev DB state as of INT-31c: 1 api_service_list row (api_service_function column).
-- The 4 vw_task_info rows are a projection of the same underlying row through
-- the view -- updating api_service_list fixes both. See INT-31d for the same
-- pattern applied to garmin.

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.spotify.provider.get_spotify_client'
WHERE api_service_function = 'backend_functions.service_logins.get_spotify_client';

-- NOTE: tasks.vw_task_info is a complex (non-updatable) view over api_services.api_service_list
-- and api_services.function_library. Updating api_services.api_service_list.api_service_function
-- auto-reflects through the view. No UPDATE against the view is required.
