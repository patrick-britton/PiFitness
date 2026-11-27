-- Exclude newest qualifying run/trail run from auto-sync view to reserve it for manual processing.
--
-- migrate:up

CREATE OR REPLACE VIEW activities.vw_activity_ids_to_sync AS
 SELECT activity_id,
    round(duration_elapsed_s, 0) * 2::numeric AS max_points
   FROM activities.activities
  WHERE NOT is_downloaded
    AND NOT activity_type_name ~~ '%treadmill%'::text
    AND distance_m > 1000::numeric
    AND activity_id IS DISTINCT FROM (
        SELECT max(activity_id)
        FROM activities.activities
        WHERE (activity_type_name ~~ '%running%'::text OR activity_type_name ~~ '%trail%'::text)
          AND distance_m > 1000::numeric
    )
  ORDER BY (activity_type_name ~~ '%running%'::text) DESC, start_time_utc DESC
 LIMIT 15;

-- migrate:down

CREATE OR REPLACE VIEW activities.vw_activity_ids_to_sync AS
 SELECT activity_id,
    round(duration_elapsed_s, 0) * 2::numeric AS max_points
   FROM activities.activities
  WHERE NOT is_downloaded
    AND NOT activity_type_name ~~ '%treadmill%'::text
    AND distance_m > 1000::numeric
  ORDER BY (activity_type_name ~~ '%running%'::text) DESC, start_time_utc DESC
 LIMIT 15;
