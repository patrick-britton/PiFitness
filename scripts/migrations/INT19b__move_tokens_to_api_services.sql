-- INT-19c: move token tables from _migration to api_services on PRD.
-- Run at merge time by the operator. NOT executed by agents.
--
-- Mirrors the dev-DB migration done in INT-19b, with sequence rewire
-- included inline (LIKE INCLUDING ALL leaves id defaults pointing at
-- _migration.*_id_seq, which blocks the DROP SCHEMA at the end).

BEGIN;

-- Pre-check: _migration must contain exactly the two token tables.
DO $$
DECLARE
    n INT;
BEGIN
    SELECT count(*) INTO n FROM information_schema.tables
    WHERE table_schema = '_migration'
      AND table_name IN ('api_tokens', 'spotify_auth_metadata');
    IF n != 2 THEN
        RAISE EXCEPTION 'PRD pre-check failed: expected 2 _migration token tables, found %', n;
    END IF;
END $$;

-- Row counts before copy. Operator: record these.
SELECT '_migration.api_tokens' AS source, count(*) FROM _migration.api_tokens
UNION ALL
SELECT '_migration.spotify_auth_metadata', count(*) FROM _migration.spotify_auth_metadata;

-- 1. Create api_services tables, mirroring _migration structure.
CREATE TABLE api_services.api_tokens (LIKE _migration.api_tokens INCLUDING ALL);
INSERT INTO api_services.api_tokens SELECT * FROM _migration.api_tokens;
UPDATE api_services.api_tokens SET is_active = true WHERE is_active IS NULL;
ALTER TABLE api_services.api_tokens ALTER COLUMN is_active SET DEFAULT true;
ALTER TABLE api_services.api_tokens ALTER COLUMN is_active SET NOT NULL;

CREATE TABLE api_services.spotify_auth_metadata (LIKE _migration.spotify_auth_metadata INCLUDING ALL);
INSERT INTO api_services.spotify_auth_metadata SELECT * FROM _migration.spotify_auth_metadata;

-- 2. Rewire id sequences.
DO $$
DECLARE
    r RECORD;
    seq_name TEXT;
    max_id BIGINT;
BEGIN
    FOR r IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'api_services'
          AND table_name IN ('api_tokens', 'spotify_auth_metadata')
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'api_services'
              AND table_name = r.table_name
              AND column_name = 'id'
              AND column_default LIKE '%_migration%'
        ) THEN
            seq_name := format('api_services.%I_id_seq', r.table_name);
            EXECUTE format('CREATE SEQUENCE %s OWNED BY api_services.%I.id', seq_name, r.table_name);
            EXECUTE format('SELECT COALESCE(MAX(id),0)+1 FROM api_services.%I', r.table_name) INTO max_id;
            EXECUTE format('SELECT setval(%L, %s, false)', seq_name, max_id);
            EXECUTE format('ALTER TABLE api_services.%I ALTER COLUMN id SET DEFAULT nextval(%L)', r.table_name, seq_name);
        END IF;
    END LOOP;
END $$;

-- 3. Row counts after copy. Operator: verify match pre-copy.
SELECT 'api_services.api_tokens' AS source, count(*) FROM api_services.api_tokens
UNION ALL
SELECT 'api_services.spotify_auth_metadata', count(*) FROM api_services.spotify_auth_metadata;

-- 4. Drop _migration. Fails fast if any dependency remains.
DROP TABLE _migration.api_tokens;
DROP TABLE _migration.spotify_auth_metadata;
DROP SCHEMA _migration;

-- 5. Confirm _migration gone. Expected: 0.
SELECT count(*) AS _migration_present FROM information_schema.schemata WHERE schema_name = '_migration';

COMMIT;
