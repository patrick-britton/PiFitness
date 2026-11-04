-- migrate:up
COMMENT ON TABLE volleyball."game_events" IS 'Table game_events.';
COMMENT ON COLUMN volleyball."game_events"."event_id" IS 'Column event_id.';
COMMENT ON COLUMN volleyball."game_events"."game_id" IS 'Column game_id.';
COMMENT ON COLUMN volleyball."game_events"."kind" IS 'Column kind.';
COMMENT ON COLUMN volleyball."game_events"."message" IS 'Column message.';
COMMENT ON COLUMN volleyball."game_events"."recorded_at" IS 'Column recorded_at.';
COMMENT ON COLUMN volleyball."game_events"."created_at" IS 'Column created_at.';
COMMENT ON TABLE volleyball."games" IS 'Table games.';
COMMENT ON COLUMN volleyball."games"."game_id" IS 'Column game_id.';
COMMENT ON COLUMN volleyball."games"."team_b_name" IS 'Column team_b_name.';
COMMENT ON COLUMN volleyball."games"."status" IS 'Column status.';
COMMENT ON COLUMN volleyball."games"."started_at" IS 'Column started_at.';
COMMENT ON COLUMN volleyball."games"."completed_at" IS 'Column completed_at.';
COMMENT ON COLUMN volleyball."games"."label" IS 'Column label.';
COMMENT ON COLUMN volleyball."games"."notes" IS 'Column notes.';
COMMENT ON COLUMN volleyball."games"."created_at" IS 'Column created_at.';
COMMENT ON COLUMN volleyball."games"."partner_number" IS 'Column partner_number.';
COMMENT ON COLUMN volleyball."games"."partner_name" IS 'Column partner_name.';
COMMENT ON COLUMN volleyball."games"."first_entry_at" IS 'Column first_entry_at.';
COMMENT ON TABLE volleyball."points" IS 'Table points.';
COMMENT ON COLUMN volleyball."points"."point_id" IS 'Column point_id.';
COMMENT ON COLUMN volleyball."points"."game_id" IS 'Column game_id.';
COMMENT ON COLUMN volleyball."points"."scoring_team" IS 'Column scoring_team.';
COMMENT ON COLUMN volleyball."points"."recorded_at" IS 'Column recorded_at.';
COMMENT ON COLUMN volleyball."points"."created_at" IS 'Column created_at.';
COMMENT ON COLUMN volleyball."points"."event_type" IS 'Column event_type.';

-- migrate:down
COMMENT ON TABLE volleyball."game_events" IS NULL;
COMMENT ON TABLE volleyball."games" IS NULL;
COMMENT ON TABLE volleyball."points" IS NULL;
