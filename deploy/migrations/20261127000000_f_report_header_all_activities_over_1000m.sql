-- Include all activities over 1000m in Recent Activity Report navigation,
-- whether processed or not, so they can be viewed and reprocessed in-place.
--
-- migrate:up

CREATE OR REPLACE FUNCTION activities.f_report_header(p_activity_id bigint DEFAULT NULL::bigint)
 RETURNS TABLE(activity_id bigint, activity_type_name text, activity_name text, start_time_utc timestamp with time zone, distance_m double precision, distance_mi double precision, distance_display text, duration_elapsed_s double precision, total_time_display text, pace_display text, elevation_m_gain double precision, hr_median double precision, hr_maximum double precision, prev_activity_id bigint, next_activity_id bigint)
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$
WITH qualifying AS (
    SELECT a.activity_id,
           a.activity_type_name,
           a.activity_name,
           a.start_time_utc,
           a.distance_m,
           a.duration_elapsed_s,
           a.elevation_m_gain,
           -- pos 1 = newest; prev = pos+1 (older), next = pos-1 (newer)
           row_number() OVER (ORDER BY a.activity_id DESC) AS pos
    FROM activities.activities a
    WHERE a.distance_m > 1000
),
     target AS (
         SELECT q.*
         FROM qualifying q
         WHERE q.activity_id = COALESCE(
                 p_activity_id,
                 (SELECT max(q2.activity_id) FROM qualifying q2)
                               )
     )
SELECT
    t.activity_id,
    t.activity_type_name,
    t.activity_name,
    t.start_time_utc,
    t.distance_m,
    m2mi(t.distance_m) AS distance_mi,
    m2mi(t.distance_m)::text || ' mi' AS distance_display,
    t.duration_elapsed_s,
    CASE
        WHEN t.duration_elapsed_s IS NULL THEN NULL
        WHEN round(t.duration_elapsed_s::numeric, 1) >= 3600 THEN
            (floor(round(t.duration_elapsed_s::numeric, 1) / 3600))::text || ':' ||
            lpad((floor((round(t.duration_elapsed_s::numeric, 1) % 3600) / 60))::text, 2, '0') || ':' ||
            to_char(round(t.duration_elapsed_s::numeric, 1) % 60, 'FM00.0')
        ELSE
            (floor(round(t.duration_elapsed_s::numeric, 1) / 60))::text || ':' ||
            to_char(round(t.duration_elapsed_s::numeric, 1) % 60, 'FM00.0')
        END::text AS total_time_display,
    CASE
        WHEN t.duration_elapsed_s IS NULL OR m2mi(t.distance_m) = 0 THEN NULL
        ELSE (
                 CASE
                     WHEN round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) >= 3600 THEN
                         (floor(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 3600))::text || ':' ||
                         lpad((floor((round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 3600) / 60))::text, 2, '0') || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     WHEN round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) >= 600 THEN
                         lpad((floor((round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 60))::text), 2, '0') || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     ELSE
                         (floor(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 60))::text || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     END
                 )::text || '/mi'
        END AS pace_display,
    t.elevation_m_gain,
    hr.hr_median,
    hr.hr_maximum,
    (SELECT q3.activity_id FROM qualifying q3 WHERE q3.pos = t.pos + 1) AS prev_activity_id,
    (SELECT q4.activity_id FROM qualifying q4 WHERE q4.pos = t.pos - 1) AS next_activity_id
FROM target t
         CROSS JOIN LATERAL (
    SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY ad.heartrate_bpm)::double precision AS hr_median,
           max(ad.heartrate_bpm)::double precision AS hr_maximum
    FROM activities.activity_details ad
    WHERE ad.activity_id = t.activity_id
      AND ad.heartrate_bpm IS NOT NULL
    ) hr;
$function$;

-- migrate:down

CREATE OR REPLACE FUNCTION activities.f_report_header(p_activity_id bigint DEFAULT NULL::bigint)
 RETURNS TABLE(activity_id bigint, activity_type_name text, activity_name text, start_time_utc timestamp with time zone, distance_m double precision, distance_mi double precision, distance_display text, duration_elapsed_s double precision, total_time_display text, pace_display text, elevation_m_gain double precision, hr_median double precision, hr_maximum double precision, prev_activity_id bigint, next_activity_id bigint)
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$
WITH qualifying AS (
    SELECT a.activity_id,
           a.activity_type_name,
           a.activity_name,
           a.start_time_utc,
           a.distance_m,
           a.duration_elapsed_s,
           a.elevation_m_gain,
           -- pos 1 = newest; prev = pos+1 (older), next = pos-1 (newer)
           row_number() OVER (ORDER BY a.activity_id DESC) AS pos
    FROM activities.activities a
    WHERE (lower(a.activity_type_name) LIKE '%run%'
        OR lower(a.activity_type_name) LIKE '%walk%'
        OR lower(a.activity_type_name) LIKE '%hike%')
      AND a.distance_m > 1000
      AND a.activity_path IS NOT NULL
),
     target AS (
         SELECT q.*
         FROM qualifying q
         WHERE q.activity_id = COALESCE(
                 p_activity_id,
                 (SELECT max(q2.activity_id) FROM qualifying q2)
                               )
     )
SELECT
    t.activity_id,
    t.activity_type_name,
    t.activity_name,
    t.start_time_utc,
    t.distance_m,
    m2mi(t.distance_m) AS distance_mi,
    m2mi(t.distance_m)::text || ' mi' AS distance_display,
    t.duration_elapsed_s,
    CASE
        WHEN t.duration_elapsed_s IS NULL THEN NULL
        WHEN round(t.duration_elapsed_s::numeric, 1) >= 3600 THEN
            (floor(round(t.duration_elapsed_s::numeric, 1) / 3600))::text || ':' ||
            lpad((floor((round(t.duration_elapsed_s::numeric, 1) % 3600) / 60))::text, 2, '0') || ':' ||
            to_char(round(t.duration_elapsed_s::numeric, 1) % 60, 'FM00.0')
        ELSE
            (floor(round(t.duration_elapsed_s::numeric, 1) / 60))::text || ':' ||
            to_char(round(t.duration_elapsed_s::numeric, 1) % 60, 'FM00.0')
        END::text AS total_time_display,
    CASE
        WHEN t.duration_elapsed_s IS NULL OR m2mi(t.distance_m) = 0 THEN NULL
        ELSE (
                 CASE
                     WHEN round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) >= 3600 THEN
                         (floor(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 3600))::text || ':' ||
                         lpad((floor((round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 3600) / 60))::text, 2, '0') || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     WHEN round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) >= 600 THEN
                         lpad((floor((round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 60))::text), 2, '0') || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     ELSE
                         (floor(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) / 60))::text || ':' ||
                         to_char(round((t.duration_elapsed_s / m2mi(t.distance_m))::numeric, 1) % 60, 'FM00.0')
                     END
                 )::text || '/mi'
        END AS pace_display,
    t.elevation_m_gain,
    hr.hr_median,
    hr.hr_maximum,
    (SELECT q3.activity_id FROM qualifying q3 WHERE q3.pos = t.pos + 1) AS prev_activity_id,
    (SELECT q4.activity_id FROM qualifying q4 WHERE q4.pos = t.pos - 1) AS next_activity_id
FROM target t
         CROSS JOIN LATERAL (
    SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY ad.heartrate_bpm)::double precision AS hr_median,
           max(ad.heartrate_bpm)::double precision AS hr_maximum
    FROM activities.activity_details ad
    WHERE ad.activity_id = t.activity_id
      AND ad.heartrate_bpm IS NOT NULL
    ) hr;
$function$;
