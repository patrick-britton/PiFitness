-- INT-19c: retire the _migration schema; its two token tables move to api_services.
-- Applied by autodeploy (dbmate) like every other migration.
--
-- NO ROLLBACK: destructive schema retirement. The rollback is the exit handler's restore
-- of the pinned prd_baseline_<ts>.dump (TEST: the clean baseline), never a down migration.
-- Recreating _migration would un-retire the schema this migration exists to retire.
--
-- Mirrors the dev-DB migration done in INT-19b, with the sequence rewire included inline
-- (LIKE INCLUDING ALL leaves id defaults pointing at _migration.*_id_seq, which blocks the
-- DROP SCHEMA at the end).
--
-- dbmate runs all of this as ONE transaction, and every guard below RAISEs, so a failure
-- leaves the database exactly as it was. Nothing here needs an operator: dbmate discards
-- SELECT output, so checks are assertions, not queries to read.
--
-- migrate:up

-- Pre-check: _migration holds exactly the two token tables, api_services exists and does
-- not already have them. (Fails here, at the top, instead of at CREATE TABLE / DROP SCHEMA.)
DO $$
DECLARE
    n_named INT;
    n_total INT;
BEGIN
    SELECT count(*) FILTER (WHERE table_name IN ('api_tokens', 'spotify_auth_metadata')),
           count(*)
      INTO n_named, n_total
      FROM information_schema.tables
     WHERE table_schema = '_migration';
    IF n_named <> 2 OR n_total <> 2 THEN
        RAISE EXCEPTION 'pre-check failed: _migration must contain exactly api_tokens and spotify_auth_metadata (named found: %, total found: %)', n_named, n_total;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'api_services') THEN
        RAISE EXCEPTION 'pre-check failed: schema api_services does not exist';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'api_services'
                  AND table_name IN ('api_tokens', 'spotify_auth_metadata')) THEN
        RAISE EXCEPTION 'pre-check failed: api_services already contains a token table';
    END IF;
END $$;

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

-- 3. Assert the copy is complete BEFORE anything is dropped.
DO $$
DECLARE
    tokens_src BIGINT;
    tokens_dst BIGINT;
    spotify_src BIGINT;
    spotify_dst BIGINT;
BEGIN
    SELECT count(*) INTO tokens_src  FROM _migration.api_tokens;
    SELECT count(*) INTO tokens_dst  FROM api_services.api_tokens;
    SELECT count(*) INTO spotify_src FROM _migration.spotify_auth_metadata;
    SELECT count(*) INTO spotify_dst FROM api_services.spotify_auth_metadata;
    IF tokens_src <> tokens_dst OR spotify_src <> spotify_dst THEN
        RAISE EXCEPTION 'row count mismatch after copy: api_tokens % -> %, spotify_auth_metadata % -> %',
            tokens_src, tokens_dst, spotify_src, spotify_dst;
    END IF;
END $$;

-- 4. Drop _migration. Fails (and rolls everything back) if any dependency remains.
DROP TABLE _migration.api_tokens;
DROP TABLE _migration.spotify_auth_metadata;
DROP SCHEMA _migration;

-- migrate:down
-- NO ROLLBACK: destructive schema retirement. Restore from the pinned baseline dump.
DO $$ BEGIN RAISE EXCEPTION 'NO ROLLBACK: 20261108000000 destructive migration; restore from the pinned baseline dump'; END $$;
