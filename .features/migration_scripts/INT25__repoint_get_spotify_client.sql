-- INT-25 production migration: repoint DB dispatch references from
--   backend_functions.service_logins.get_spotify_client
-- to
--   pifitness.integrations.spotify.provider.get_spotify_client.
-- Unexecuted in dev; run at production merge (INT-18-live precedent).
-- Dev DB state as of INT-25: 1 api_service_list row, 4 vw_task_info rows.

UPDATE api_services.api_service_list
SET api_service_function = 'pifitness.integrations.spotify.provider.get_spotify_client'
WHERE api_service_function = 'backend_functions.service_logins.get_spotify_client';

UPDATE tasks.vw_task_info
SET python_login_function = 'pifitness.integrations.spotify.provider.get_spotify_client'
WHERE python_login_function = 'backend_functions.service_logins.get_spotify_client';
