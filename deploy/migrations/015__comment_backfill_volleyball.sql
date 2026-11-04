-- migrate:up
COMMENT ON TABLE volleyball."game_events" IS 'Volleyball table.';
COMMENT ON COLUMN volleyball."game_events"."event_id" IS 'Identifier.';
COMMENT ON COLUMN volleyball."game_events"."game_id" IS 'Identifier.';
COMMENT ON COLUMN volleyball."game_events"."kind" IS 'Kind value.';
COMMENT ON COLUMN volleyball."game_events"."message" IS 'Message value.';
COMMENT ON COLUMN volleyball."game_events"."recorded_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."game_events"."created_at" IS 'Timestamp (UTC).';
COMMENT ON TABLE volleyball."games" IS 'Volleyball table.';
COMMENT ON COLUMN volleyball."games"."game_id" IS 'Identifier.';
COMMENT ON COLUMN volleyball."games"."team_b_name" IS 'Team b name value.';
COMMENT ON COLUMN volleyball."games"."status" IS 'Status value.';
COMMENT ON COLUMN volleyball."games"."started_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."games"."completed_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."games"."label" IS 'Label value.';
COMMENT ON COLUMN volleyball."games"."notes" IS 'Notes value.';
COMMENT ON COLUMN volleyball."games"."created_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."games"."partner_number" IS 'Partner number value.';
COMMENT ON COLUMN volleyball."games"."partner_name" IS 'Partner name value.';
COMMENT ON COLUMN volleyball."games"."first_entry_at" IS 'First entry at value.';
COMMENT ON TABLE volleyball."points" IS 'Volleyball table.';
COMMENT ON COLUMN volleyball."points"."point_id" IS 'Identifier.';
COMMENT ON COLUMN volleyball."points"."game_id" IS 'Identifier.';
COMMENT ON COLUMN volleyball."points"."scoring_team" IS 'Scoring team value.';
COMMENT ON COLUMN volleyball."points"."recorded_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."points"."created_at" IS 'Timestamp (UTC).';
COMMENT ON COLUMN volleyball."points"."event_type" IS 'Event type value.';

-- migrate:down
COMMENT ON TABLE volleyball."game_events" IS NULL;
COMMENT ON TABLE volleyball."games" IS NULL;
COMMENT ON TABLE volleyball."points" IS NULL;
