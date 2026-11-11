-- Sentinel migration used to verify dev_reset.py replays pending migrations.
-- A deliberate test migration: survives dev_reset.py and is recorded in
-- schema_migrations so a subsequent reset shows it already applied.

-- migrate:up
CREATE TABLE dev_reset_replay_sentinel (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- migrate:down
DROP TABLE dev_reset_replay_sentinel;