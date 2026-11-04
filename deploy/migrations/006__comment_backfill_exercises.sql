-- migrate:up
COMMENT ON TABLE exercises.exercise_attempts IS 'One attempt at an exercise timer session.';
COMMENT ON COLUMN exercises.exercise_attempts.attempt_id IS 'Primary key.';
COMMENT ON COLUMN exercises.exercise_attempts.exercise_id IS 'FK to exercise_timers.exercise_id.';
COMMENT ON COLUMN exercises.exercise_attempts.interval_seconds_used IS 'Interval seconds configured for this attempt.';
COMMENT ON COLUMN exercises.exercise_attempts.started_at IS 'Attempt start timestamp.';
COMMENT ON COLUMN exercises.exercise_attempts.ended_at IS 'Attempt end timestamp.';
COMMENT ON COLUMN exercises.exercise_attempts.paced_count IS 'Count of paced intervals completed.';
COMMENT ON COLUMN exercises.exercise_attempts.total_count IS 'Total intervals completed.';
COMMENT ON COLUMN exercises.exercise_attempts.notes IS 'Free-text notes for the attempt.';
COMMENT ON COLUMN exercises.exercise_attempts.created_at IS 'Row creation timestamp.';

COMMENT ON TABLE exercises.exercise_timers IS 'Configured exercise interval timers.';
COMMENT ON COLUMN exercises.exercise_timers.exercise_id IS 'Primary key.';
COMMENT ON COLUMN exercises.exercise_timers.name IS 'Timer display name.';
COMMENT ON COLUMN exercises.exercise_timers.interval_seconds IS 'Interval length in seconds.';
COMMENT ON COLUMN exercises.exercise_timers.notes IS 'Free-text notes for the timer.';
COMMENT ON COLUMN exercises.exercise_timers.created_at IS 'Row creation timestamp.';
COMMENT ON COLUMN exercises.exercise_timers.updated_at IS 'Row last-update timestamp.';

-- migrate:down
COMMENT ON TABLE exercises.exercise_attempts IS NULL;
COMMENT ON TABLE exercises.exercise_timers IS NULL;
