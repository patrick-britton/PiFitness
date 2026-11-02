-- INT-25 rollback: revert the Spotify get_spotify_client repointing.
-- Restores the api_service_list row to its pre-INT-25 value.
-- Only safe to run if INT25__repoint_get_spotify_client.sql was applied.
--
-- After rollback, backend_functions.service_logins.get_spotify_client exists
-- again in the DB, but that module no longer exists in current code (deleted
-- at INT-28b). This rollback restores the previous DB state regardless; it is
-- not a code-coherent state on its own.
--
-- No tasks.vw_task_info UPDATE: the view projects api_service_list.api_service_function
-- as python_login_function, so updating api_service_list auto-reflects.

BEGIN;

UPDATE api_services.api_service_list
SET api_service_function = 'backend_functions.service_logins.get_spotify_client'
WHERE api_service_function = 'pifitness.integrations.spotify.provider.get_spotify_client';

COMMIT;