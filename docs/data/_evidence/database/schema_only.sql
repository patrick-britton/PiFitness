--
-- PostgreSQL database dump
--

\restrict XSiVRBwh1qBezwGFiMIIybVuaBZClxbUeUtnlxGeofW81alcgWzLZwBIE5rGc5P

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: activities; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA activities;


--
-- Name: api_services; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA api_services;


--
-- Name: exercises; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA exercises;


--
-- Name: food; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA food;


--
-- Name: health; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA health;


--
-- Name: logging; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA logging;


--
-- Name: metrics; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA metrics;


--
-- Name: music; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA music;


--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA staging;


--
-- Name: tasks; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA tasks;


--
-- Name: volleyball; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA volleyball;


--
-- Name: amcheck; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS amcheck WITH SCHEMA public;


--
-- Name: EXTENSION amcheck; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION amcheck IS 'functions for verifying relation integrity';


--
-- Name: btree_gist; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;


--
-- Name: EXTENSION btree_gist; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gist IS 'support for indexing common datatypes in GiST';


--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


--
-- Name: postgis_raster; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis_raster WITH SCHEMA public;


--
-- Name: EXTENSION postgis_raster; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis_raster IS 'PostGIS raster types and functions';


--
-- Name: assign_elevation_reference_distance(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.assign_elevation_reference_distance(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Step 1: Calculate the neighborhood average and trust score in memory
    UPDATE activities.activity_details_distance
    SET elevation_reference = COALESCE(elevation_hammered, avg_elevation)
    FROM activities.elevation_reference
    WHERE activity_id = p_activity_id and activity_details_distance.grid_5m_00_id = elevation_reference.grid_5m_00_id;

END;
$$;


--
-- Name: assign_elevation_reference_time(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.assign_elevation_reference_time(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Step 1: Calculate the neighborhood average and trust score in memory
    UPDATE activities.activity_details
    SET elevation_reference = COALESCE(elevation_hammered, avg_elevation)
    FROM activities.elevation_reference
    WHERE activity_id = p_activity_id and activity_details.grid_5m_00_id = elevation_reference.grid_5m_00_id;

END;
$$;


--
-- Name: calculate_pr_potential(numeric, numeric, numeric, numeric); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.calculate_pr_potential(weight_delta numeric, vo2_delta numeric, load_readiness_delta numeric, load_ratio numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    engine_impact NUMERIC;
    multiplier NUMERIC;
    total_coefficient NUMERIC;
BEGIN
    -- 1. Calculate the "Engine" Foundation (Base 1.0)
    -- COALESCE(val, 0) ensures NULLs are excluded/ignored.
    -- weight_delta is multiplied by -1.0 because a negative delta (weight loss) increases speed.
    engine_impact := 1.0
        + COALESCE(weight_delta * -1.0, 0)          -- Weight
        + COALESCE(vo2_delta * 0.85, 0)             -- VO2 Max
        + COALESCE(load_readiness_delta * 0.15, 0); -- Readiness

    -- 2. Determine the Capacity Multiplier based on Load Ratio
    -- If load_ratio is NULL, it falls back to the Neutral 1.0 multiplier
    multiplier := CASE
                      WHEN load_ratio BETWEEN 0.85 AND 0.98 THEN 1.03 -- Primed (+3%)
                      WHEN load_ratio > 1.15                THEN 0.85 -- Overreached (-15%)
                      WHEN load_ratio < 0.60                THEN 0.92 -- Detrained (-8%)
                      ELSE 1.0                                        -- Neutral / NULL
        END;

    -- 3. Calculate total coefficient (e.g., 1.0525)
    total_coefficient := engine_impact * multiplier;

    -- 4. Convert to the literal percentage format for the UI (e.g., 5.25 for 5.25%)
    RETURN ROUND((total_coefficient - 1.0), 3);
END;
$$;


--
-- Name: f_report_header(bigint); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.f_report_header(p_activity_id bigint DEFAULT NULL::bigint) RETURNS TABLE(activity_id bigint, activity_type_name text, activity_name text, start_time_utc timestamp with time zone, distance_m double precision, distance_mi double precision, distance_display text, duration_elapsed_s double precision, total_time_display text, pace_display text, elevation_m_gain double precision, hr_median double precision, hr_maximum double precision, prev_activity_id bigint, next_activity_id bigint)
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
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
    -- lower()/LIKE is non-sargable but activities is ~2.5k rows: seq scan
    -- measured trivial; do NOT add an expression index for this.
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
$$;


--
-- Name: hammer_elevation_reference(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.hammer_elevation_reference()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Step 1: Calculate the neighborhood average and trust score in memory
    WITH neighborhood AS (
        SELECT
            e1.grid_5m_00_id,
            avg(e2.avg_elevation) AS neighbor_avg
        FROM activities.elevation_reference e1
                 JOIN activities.elevation_reference e2
            -- 7.1m radius captures the 8 immediately adjacent 5m grid cells
                      ON ST_DWithin(e1.grid_5m_00_id, e2.grid_5m_00_id, 0.000071)
        WHERE e1.grid_5m_00_id != e2.grid_5m_00_id
        GROUP BY e1.grid_5m_00_id
    ),
         trust_calc AS (
             SELECT
                 r.grid_5m_00_id,
                 r.avg_elevation,
                 n.neighbor_avg,
                 -- Base trust on observations (e.g., 10+ obs = 100% trust)
                 LEAST(r.obs_count / 10.0, 1.0)
                     * -- Penalty for high variance (if stddev > 2m, trust drops sharply)
                 LEAST(1.0, COALESCE(2.0 / NULLIF(r.elev_stddev, 0), 1.0)) AS trust_score
             FROM activities.elevation_reference r
                      JOIN neighborhood n ON r.grid_5m_00_id = n.grid_5m_00_id
         )
    -- Step 2: Apply the Bayesian Shrinkage formula to the target column
    UPDATE activities.elevation_reference target
    SET elevation_hammered =
            (tc.trust_score * tc.avg_elevation) +
            ((1.0 - tc.trust_score) * tc.neighbor_avg)
    FROM trust_calc tc
    WHERE target.grid_5m_00_id = tc.grid_5m_00_id
      -- Optimization: Only update rows that actually need hammering (trust < 1.0)
      AND tc.trust_score < 1.0;

    -- For cells with perfect trust (or no neighbors), hammered equals avg
    UPDATE activities.elevation_reference
    SET elevation_hammered = avg_elevation
    WHERE elevation_hammered IS NULL;

END;
$$;


--
-- Name: pr_potential_v1(double precision, double precision, double precision, double precision, double precision, double precision); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.pr_potential_v1(weight_delta_pct double precision, vo2_max_delta_pct double precision, acute_load_delta double precision, distance_miles double precision, elevation_gain_m double precision, reference_time_s double precision) RETURNS double precision
    LANGUAGE plpgsql
    AS $$
DECLARE
    steepness_m_per_mile double precision;
    predicted_time_delta_pct double precision;
BEGIN
    -- 1. Calculate course steepness
    steepness_m_per_mile := elevation_gain_m / NULLIF(distance_miles, 0);

    -- 2. Apply terrain-specific coefficients derived from data analysis
    IF steepness_m_per_mile > 30 THEN
        -- Hilly Course Coefficients
        predicted_time_delta_pct :=
                (0.48 * COALESCE(weight_delta_pct,0)) +
                (-0.71 * COALESCE(vo2_max_delta_pct,0)) +
                (0.03 * COALESCE(acute_load_delta,0));
    ELSE
        -- Flat Course Coefficients
        predicted_time_delta_pct :=
                (0.32 * COALESCE(weight_delta_pct,0)) +
                (-0.83 * COALESCE(vo2_max_delta_pct,0)) +
                (-0.01 * COALESCE(acute_load_delta,0));
    END IF;

    -- 3. Return the predicted time adjusted by the delta
    -- (e.g., if delta is -0.05, you are 5% faster than reference)
    RETURN ROUND((reference_time_s * (1 + predicted_time_delta_pct))::NUMERIC,2);
END;
$$;


--
-- Name: pr_potential_v2(numeric, numeric, numeric, numeric, numeric, numeric, numeric); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.pr_potential_v2(p_weight_delta numeric, p_vo2_max_delta numeric, p_acute_load_delta numeric, p_max_load_delta numeric, p_distance_mi numeric, p_elevation_gain_m numeric, p_reference_time_s numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_elev_ratio   NUMERIC;
    v_pr_score     NUMERIC;
BEGIN
    -- Guard: avoid division by zero on degenerate course data
    IF p_distance_mi IS NULL OR p_distance_mi <= 0 THEN
        RAISE EXCEPTION 'p_distance_mi must be a positive non-null value';
    END IF;

    IF p_reference_time_s IS NULL OR p_reference_time_s <= 0 THEN
        RAISE EXCEPTION 'p_reference_time_s must be a positive non-null value';
    END IF;

    -- Elevation ratio determines which regime applies
    -- Threshold of 6 m/mi derived from median elev ratio in calibration dataset
    v_elev_ratio := COALESCE(p_elevation_gain_m, 0) / p_distance_mi;

    IF v_elev_ratio < 6 THEN
        -- Flat / low-gradient regime
        -- VO2 and max_load are excluded: both were statistically insignificant
        -- on flat courses in the calibration data (VO2 p=0.16, max_load p=0.04
        -- but with a counterintuitive sign consistent with collinearity artifact)
        v_pr_score :=
                0.80 * COALESCE(p_weight_delta, 0)
                    + 0.021 * COALESCE(p_acute_load_delta, 0);
    ELSE
        -- Hilly / steep regime
        -- Acute load excluded: not significant on steep courses (p=0.42)
        v_pr_score :=
                0.47  * COALESCE(p_weight_delta, 0)
                    - 0.83  * COALESCE(p_vo2_max_delta, 0)
                    - 0.015 * COALESCE(p_max_load_delta, 0);
    END IF;

    -- PR_Score is the predicted fractional time delta vs reference.
    -- Negative score → faster than PR; positive → slower than PR.
    RETURN ROUND((p_reference_time_s * (1.0 + v_pr_score))::NUMERIC, 2);
END;
$$;


--
-- Name: pr_potential_v3(double precision, double precision, double precision, double precision, double precision, double precision); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.pr_potential_v3(weight_delta_pct double precision, vo2_delta_pct double precision, acute_load_delta double precision, distance_mi double precision, elevation_gain_m double precision, reference_time_s double precision) RETURNS double precision
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Core model coefficients (from regression)
    v_weight_coeff CONSTANT DOUBLE PRECISION := 0.54;
    v_vo2_coeff    CONSTANT DOUBLE PRECISION := -1.04;
    v_acute_coeff  CONSTANT DOUBLE PRECISION := 0.01;

    -- Derived values
    v_base_delta DOUBLE PRECISION;
    v_grade_factor DOUBLE PRECISION;
    v_adjusted_delta DOUBLE PRECISION;
BEGIN
    /*
        Base predicted % delta vs reference

        Interpretation:
        - Positive = slower than reference
        - Negative = faster than reference
    */
    v_base_delta :=
          (v_weight_coeff * COALESCE(weight_delta_pct,0))
        + (v_vo2_coeff    * COALESCE(vo2_delta_pct,0))
        + (v_acute_coeff  * COALESCE(acute_load_delta,0));

    /*
        Optional course adjustment (kept intentionally small and stable)

        Rationale:
        - Elevation impact scales roughly with grade (elevation per mile)
        - Weight matters slightly more on steeper courses
        - VO2 matters slightly more on longer efforts

        This is a *light-touch modifier*, not a re-fit model.
    */
    IF distance_mi > 0 THEN
        v_grade_factor := elevation_gain_m / distance_mi;
    ELSE
        v_grade_factor := 0;
    END IF;

    /*
        Apply small interaction adjustments:
        - Increase weight penalty slightly for steeper courses
        - Increase VO2 benefit slightly for longer courses
    */
    v_adjusted_delta :=
        v_base_delta
        * (
            1
            + (0.0005 * v_grade_factor)   -- hill sensitivity
            + (0.02   * LN(GREATEST(distance_mi, 1)))
          );

    /*
        Final predicted time
    */
    RETURN round((reference_time_s * (1 + v_adjusted_delta))::NUMERIC,2);

END;
$$;


--
-- Name: process_elevation_backlog(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.process_elevation_backlog()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_updated_count INT;
BEGIN
    WITH updates AS (
        SELECT
            ad.activity_id,
            ad.elapsed_duration_s,
            -- Transform activity point to match the Raster SRID (usually 4269)
            -- and extract the value
            ST_Value(r.rast, ST_Transform(ad.loc_point::geometry, ST_SRID(r.rast))) as verified_elevation
        FROM activities.activity_details ad
                 JOIN activities.elevation_rasters r
                      ON ST_Intersects(r.rast, ST_Transform(ad.loc_point::geometry, ST_SRID(r.rast)))
        WHERE ad.elevation_reference IS NULL
    )
    UPDATE activities.activity_details ad
    SET elevation_reference = up.verified_elevation
    FROM updates up
    WHERE ad.activity_id = up.activity_id
      AND ad.elapsed_duration_s = up.elapsed_duration_s;

    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    RAISE NOTICE 'Updated % points with verified 1m elevation.', v_updated_count;
END;
$$;


--
-- Name: rebuild_elevation_reference(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.rebuild_elevation_reference()
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE activities.elevation_reference;
    ANALYZE activities.activity_details_distance;

    INSERT INTO activities.elevation_reference (grid_5m_00_id, avg_elevation, obs_count, elev_stddev)
    SELECT
        grid_5m_00_id,
        percentile_disc(0.5) WITHIN GROUP (
            ORDER BY elevation_m_smooth
        ) as avg_elevation,
        count(*) as obs_count,
        stddev(elevation_m_smooth) as elev_stddev
    FROM activities.activity_details_distance
    GROUP BY grid_5m_00_id;

    UPDATE activities.elevation_reference
    SET elevation_hammered = avg_elevation;

END;
$$;


--
-- Name: refresh_overlaps(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.refresh_overlaps()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO activities.overlap_queue (segment_id_a, segment_id_b, match_confidence)
    WITH cp1 AS (
        -- STAGE 1: Cheapest Filters Only
        -- Uses Indexes (&&) and avoids heavy math.
        SELECT 
            s1.segment_id AS id_a, 
            s2.segment_id AS id_b,
            s1.segment_path AS path_a,
            s2.segment_path AS path_b
        FROM activities.segments s1
        JOIN activities.segments s2 
          ON s1.segment_id < s2.segment_id 
          AND s1.segment_path && s2.segment_path -- Index Bounding Box
        LEFT JOIN activities.overlap_queue q 
          ON q.segment_id_a = s1.segment_id AND q.segment_id_b = s2.segment_id
        LEFT JOIN activities.distinct_segments ds 
          ON (ds.segment_id = s1.segment_id AND ds.match_seg_id = s2.segment_id)
          OR (ds.segment_id = s2.segment_id AND ds.match_seg_id = s1.segment_id)
        WHERE q.queue_id IS NULL 
          AND ds.segment_id IS NULL
 			 -- Stage 2: Cheap Proximity
    ),

	cp2 as (
	SELECT * FROM cp1
	WHERE ST_DWithin(path_a, path_b, 0.0002)
	),
	
    scored_matches AS (
        -- STAGE 2: Apply match_confidence ONCE per survivor
        -- This only runs for the few pairs that are physically close.
        SELECT 
            id_a, 
            id_b, 
            match_confidence(path_a, path_b) as confidence
        FROM cp2
    )
    -- STAGE 3: Final Filter and Insert
    SELECT id_a, id_b, confidence 
    FROM scored_matches 
    WHERE confidence > -1000
    ON CONFLICT (segment_id_a, segment_id_b) DO NOTHING;

    -- Cleanup
    DELETE FROM activities.overlap_queue q
    WHERE NOT EXISTS (SELECT 1 FROM activities.segments s WHERE s.segment_id = q.segment_id_a)
       OR NOT EXISTS (SELECT 1 FROM activities.segments s WHERE s.segment_id = q.segment_id_b);
END;
$$;


--
-- Name: repath_activities(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.repath_activities()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE activities.activities
    SET activity_path = sub.path
    FROM (
             SELECT
                 ap.activity_id,
                 ST_SetSRID(
                         ST_MakeLine(
                                 ST_MakePoint(longitude, latitude, COALESCE(elevation_reference, elevation_m_smooth), elapsed_duration_s)
                                 ORDER BY elapsed_duration_s ASC
                         ),
                         4326
                 ) AS path
             FROM activities.activity_details rd
                      INNER JOIN activities.activities ap on ap.activity_id = rd.activity_id
             GROUP BY ap.activity_id
         ) AS sub
    WHERE activities.activity_id = sub.activity_id;

END;
$$;


--
-- Name: repath_segment_matches(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.repath_segment_matches()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE activities.segment_matches
    SET activity_path = sub.path
    FROM (
             SELECT
                 ap.activity_id,
                 ST_SetSRID(
                         ST_MakeLine(
                                 ST_MakePoint(longitude, latitude, COALESCE(elevation_reference, elevation_m_smooth), elapsed_duration_s)
                                 ORDER BY elapsed_duration_s ASC
                         ),
                         4326
                 ) AS path
             FROM activities.activity_details rd
                      INNER JOIN activities.segment_matches ap on ap.activity_id = rd.activity_id
                 AND rd.elapsed_duration_s >= ap.activity_start_point
                 AND rd.elapsed_duration_s <= ap.activity_end_point
             GROUP BY ap.activity_id
         ) AS sub
    WHERE segment_matches.activity_id = sub.activity_id;

END;
$$;


--
-- Name: repath_segments(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.repath_segments()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        UPDATE activities.segments
        SET segment_path = sub.path
        FROM (
                 SELECT
                     ap.segment_id,
                     ST_SetSRID(
                             ST_MakeLine(
                                     ST_MakePoint(longitude, latitude, COALESCE(elevation_reference, elevation_m_smooth), elapsed_duration_s)
                                     ORDER BY elapsed_duration_s ASC
                             ),
                             4326
                     ) AS path
                 FROM activities.activity_details rd
                          INNER JOIN activities.segments ap on ap.activity_reference_id = rd.activity_id
                                                                   AND rd.elapsed_duration_s >= ap.reference_start_point
                                                                   AND rd.elapsed_duration_s <= ap.reference_end_point
                 GROUP BY ap.segment_id
             ) AS sub
        WHERE segments.segment_id = sub.segment_id;

END;
$$;


--
-- Name: resample_activity_1m(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.resample_activity_1m(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM activities.activity_resampled_1m
    WHERE activity_id = p_activity_id;

    WITH base AS (
        SELECT DISTINCT ON (distance_mm)
            activity_id,
            distance_mm::double precision AS distance_mm,
            latitude::double precision     AS latitude,
            longitude::double precision    AS longitude,
            elevation_m::double precision  AS elevation_m,
            elapsed_duration_s::double precision AS elapsed_duration_s
        FROM activities.activity_details
        WHERE activity_id = p_activity_id
        ORDER BY distance_mm
    ),

         segments AS (
             SELECT
                 *,
                 LEAD(distance_mm)        OVER w AS next_distance_mm,
                 LEAD(latitude)           OVER w AS next_latitude,
                 LEAD(longitude)          OVER w AS next_longitude,
                 LEAD(elevation_m)        OVER w AS next_elevation_m,
                 LEAD(elapsed_duration_s) OVER w AS next_elapsed_duration_s
             FROM base
             WINDOW w AS (ORDER BY distance_mm)
         ),

         meter_grid AS (
             SELECT generate_series(
                            0,
                            floor((SELECT max(distance_mm) FROM base) / 1000)::int
                    ) AS distance_m
         ),

         assigned AS (
             SELECT
                 m.distance_m,
                 s.*,
                 (m.distance_m * 1000 - s.distance_mm)
                     / (s.next_distance_mm - s.distance_mm) AS frac
             FROM meter_grid m
                      JOIN segments s
                           ON m.distance_m * 1000 >= s.distance_mm
                               AND m.distance_m * 1000 <  s.next_distance_mm
             WHERE s.next_distance_mm > s.distance_mm
         )

    INSERT INTO activities.activity_resampled_1m (
        activity_id,
        distance_m,
        latitude,
        longitude,
        elevation_m,
        elapsed_duration_s
    )
    SELECT
        p_activity_id,
        distance_m,

        latitude
            + (next_latitude - latitude) * frac,

        longitude
            + (next_longitude - longitude) * frac,

        elevation_m
            + (next_elevation_m - elevation_m) * frac,

        elapsed_duration_s
            + (next_elapsed_duration_s - elapsed_duration_s) * frac

    FROM assigned
    ORDER BY distance_m;
END;
$$;


--
-- Name: resample_activity_to_distance(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.resample_activity_to_distance(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- 1. Cleanup existing records for this activity
    DELETE FROM activities.activity_details_distance
    WHERE activity_id = p_activity_id;

    INSERT INTO activities.activity_details_distance (
        activity_type_id,
        activity_id,
        distance_m,
        elapsed_duration_ms,
        latitude,
        longitude,
        heartrate_bpm,
        cadence_spm,
        air_temp_c,
        elevation_m,
        speed_mmps,
        vertical_speed_mps,
        vert_oscillation_cm,
        stride_length_cm,
        vert_ratio,
        ground_contact_time_ms,
        ground_contact_balance_left,
        performance_condition,
        loc_point
    )
    WITH base AS (
        SELECT
            activity_type_id,
            activity_id,
            distance_mm,
            elapsed_duration_s,
            latitude,
            longitude,
            elevation_m_smooth AS elevation_m,
            heartrate_bpm,
            cadence_spm,
            air_temp_c,
            speed_mmps,
            vertical_speed_mps,
            vert_oscillation_cm,
            stride_length_cm,
            vert_ratio,
            ground_contact_time_ms,
            ground_contact_balance_left,
            performance_condition
        FROM activities.activity_details
        WHERE activity_id = p_activity_id
    ),
         normalized_base AS (
             SELECT
                 activity_type_id,
                 activity_id,
                 distance_mm - (SELECT MIN(distance_mm) FROM base) AS distance_mm,
                 elapsed_duration_s,
                 latitude,
                 longitude,
                 elevation_m,
                 heartrate_bpm,
                 cadence_spm,
                 air_temp_c,
                 speed_mmps,
                 vertical_speed_mps,
                 vert_oscillation_cm,
                 stride_length_cm,
                 vert_ratio,
                 ground_contact_time_ms,
                 ground_contact_balance_left,
                 performance_condition
             FROM base
         ),
         -- 004-005 T05 monotonic-distance frontier (see header).
         monotonic_base AS (
             SELECT *
             FROM (
                      SELECT
                          nb.*,
                          MAX(nb.distance_mm) OVER (
                              ORDER BY nb.elapsed_duration_s, nb.distance_mm
                              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                              ) AS prev_max_distance_mm
                      FROM normalized_base nb
                  ) frontier
             WHERE prev_max_distance_mm IS NULL
                OR distance_mm > prev_max_distance_mm
         ),
         segment_bounds AS (
             -- Create pairs of points (current and next) for all metrics
             SELECT
                 activity_type_id,
                 activity_id,
                 (distance_mm / 1000.0) AS d1,
                 LEAD(distance_mm / 1000.0) OVER (ORDER BY elapsed_duration_s) AS d2,
                 (elapsed_duration_s * 1000) AS t1,
                 LEAD(elapsed_duration_s * 1000) OVER (ORDER BY elapsed_duration_s) AS t2,

                 -- Spatial
                 latitude AS lat1,
                 LEAD(latitude) OVER (ORDER BY elapsed_duration_s) AS lat2,
                 longitude AS lon1,
                 LEAD(longitude) OVER (ORDER BY elapsed_duration_s) AS lon2,

                 -- Core Metrics
                 elevation_m::float AS ele1,
                 LEAD(elevation_m::float) OVER (ORDER BY elapsed_duration_s) AS ele2,
                 heartrate_bpm::float AS hr1,
                 LEAD(heartrate_bpm::float) OVER (ORDER BY elapsed_duration_s) AS hr2,
                 cadence_spm::float AS cad1,
                 LEAD(cadence_spm::float) OVER (ORDER BY elapsed_duration_s) AS cad2,
                 air_temp_c::float AS tmp1,
                 LEAD(air_temp_c::float) OVER (ORDER BY elapsed_duration_s) AS tmp2,
                 speed_mmps::float AS spd1,
                 LEAD(speed_mmps::float) OVER (ORDER BY elapsed_duration_s) AS spd2,

                 -- Advanced Metrics
                 vertical_speed_mps::float AS v_spd1,
                 LEAD(vertical_speed_mps::float) OVER (ORDER BY elapsed_duration_s) AS v_spd2,
                 vert_oscillation_cm::float AS v_osc1,
                 LEAD(vert_oscillation_cm::float) OVER (ORDER BY elapsed_duration_s) AS v_osc2,
                 stride_length_cm::float AS strd1,
                 LEAD(stride_length_cm::float) OVER (ORDER BY elapsed_duration_s) AS strd2,
                 vert_ratio::float AS v_rat1,
                 LEAD(vert_ratio::float) OVER (ORDER BY elapsed_duration_s) AS v_rat2,
                 ground_contact_time_ms::float AS gct1,
                 LEAD(ground_contact_time_ms::float) OVER (ORDER BY elapsed_duration_s) AS gct2,
                 ground_contact_balance_left::float AS gcb1,
                 LEAD(ground_contact_balance_left::float) OVER (ORDER BY elapsed_duration_s) AS gcb2,
                 performance_condition::float AS perf1,
                 LEAD(performance_condition::float) OVER (ORDER BY elapsed_duration_s) AS perf2
             FROM monotonic_base
         ),
         distance_grid AS (
             -- Generate one row for every meter from 0 to total distance
             SELECT m AS dist_m
             FROM generate_series(
                          0,
                          (SELECT (MAX(distance_mm) / 1000)::int FROM monotonic_base)
                  ) m
         ),
         interpolated AS (
             -- Match each meter-mark to the corresponding original segment
             SELECT
                 s.*,
                 g.dist_m AS target_dist,
                 CASE
                     WHEN s.d2 IS NULL THEN 0
                     WHEN s.d2 = s.d1 THEN 0
                     ELSE (g.dist_m - s.d1) / (s.d2 - s.d1)
                     END AS f
             FROM distance_grid g
                      JOIN segment_bounds s
                           ON g.dist_m >= s.d1
                               AND (g.dist_m < s.d2 OR (s.d2 IS NULL AND g.dist_m = s.d1))
         )
    SELECT
        activity_type_id,
        activity_id,
        target_dist,
        round(t1 + (COALESCE(t2, t1) - t1) * f)::integer AS elapsed_duration_ms,
        (lat1 + (COALESCE(lat2, lat1) - lat1) * f)::numeric(9,6) AS latitude,
        (lon1 + (COALESCE(lon2, lon1) - lon1) * f)::numeric(9,6) AS longitude,
        round(hr1 + (COALESCE(hr2, hr1) - hr1) * f)::smallint AS heartrate_bpm,
        round(cad1 + (COALESCE(cad2, cad1) - cad1) * f)::smallint AS cadence_spm,
        round(tmp1 + (COALESCE(tmp2, tmp1) - tmp1) * f)::smallint AS air_temp_c,
        round(ele1 + (COALESCE(ele2, ele1) - ele1) * f)::numeric(7,2) AS elevation_m,
        round(spd1 + (COALESCE(spd2, spd1) - spd1) * f)::bigint AS speed_mmps,
        round(v_spd1 + (COALESCE(v_spd2, v_spd1) - v_spd1) * f)::numeric(5,1) AS vertical_speed_mps,
        round(v_osc1 + (COALESCE(v_osc2, v_osc1) - v_osc1) * f)::numeric(5,2) AS vert_oscillation_cm,
        round(strd1 + (COALESCE(strd2, strd1) - strd1) * f)::numeric(4,1) AS stride_length_cm,
        round(v_rat1 + (COALESCE(v_rat2, v_rat1) - v_rat1) * f)::numeric(4,2) AS vert_ratio,
        round(gct1 + (COALESCE(gct2, gct1) - gct1) * f)::smallint AS ground_contact_time_ms,
        round(gcb1 + (COALESCE(gcb2, gcb1) - gcb1) * f)::numeric(4,2) AS ground_contact_balance_left,
        round(perf1 + (COALESCE(perf2, perf1) - perf1) * f)::smallint AS performance_condition,
        st_makepoint(
                (lon1 + (COALESCE(lon2, lon1) - lon1) * f)::numeric(9,6),
                (lat1 + (COALESCE(lat2, lat1) - lat1) * f)::numeric(9,6)
        )
    FROM interpolated
    ON CONFLICT (activity_id, distance_m) DO UPDATE
        SET elevation_m = EXCLUDED.elevation_m;

    UPDATE activities.activity_details_distance
    SET elevation_m_smooth = elevation_m
    WHERE elevation_m_smooth IS NULL
      AND activity_id = p_activity_id;

    CALL activities.assign_elevation_reference_distance(p_activity_id);
END;
$$;


--
-- Name: safe_delta(numeric, numeric); Type: FUNCTION; Schema: activities; Owner: -
--

CREATE FUNCTION activities.safe_delta(x numeric, y numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN CASE
               WHEN y IS NULL THEN NULL
               WHEN y = 0    THEN 1.0
               ELSE (x - y) / y
        END;
END;
$$;


--
-- Name: segment_matches_all_freschet(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matches_all_freschet()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_activity_id bigint;
BEGIN
    FOR v_activity_id IN
        SELECT DISTINCT activity_id
        FROM activities.temp_segment_matches_downselect
        WHERE activity_id IS NOT NULL
        LOOP
            CALL activities.update_segment_matches_freschet(v_activity_id);
        END LOOP;
END;
$$;


--
-- Name: segment_matches_all_hausdorff(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matches_all_hausdorff()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_activity_id bigint;
BEGIN
    FOR v_activity_id IN
        SELECT DISTINCT activity_id
        FROM activities.temp_segment_matches_downselect
        WHERE activity_id IS NOT NULL
        LOOP
            CALL activities.update_segment_matches_hausdorff(v_activity_id);
        END LOOP;
END;
$$;


--
-- Name: segment_matches_all_polygon(); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matches_all_polygon()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_activity_id bigint;
BEGIN
    FOR v_activity_id IN
        SELECT DISTINCT activity_id
        FROM activities.temp_segment_matches_downselect
        WHERE activity_id IS NOT NULL
        LOOP
            CALL activities.update_segment_matches_polygon(v_activity_id);
        END LOOP;
END;
$$;


--
-- Name: segment_matching_activity_pair_generation(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_activity_pair_generation(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE activities.temp_segment_matches_downselect;

    WITH start_passes AS (
        -- 1. Find ALL trackpoints inside the Start Gate (~25 meters)
        SELECT
            d.activity_id,
            d.distance_m as dist_s,
            s.distance_m as seg_dist,
            s.segment_id,
            s.utm_srid,
--             s.segment_path,
            ST_distance(d.loc_point, s.start_point) as start_apart
        FROM
            activities.segments s
            INNER JOIN activities.temp_segment_matching_segments t on s.segment_id = t.segment_id
            INNER JOIN activities.activity_details_distance d ON d.activity_id =  p_activity_id--22127682969

        WHERE
            ST_DWithin(d.loc_point::geography, s.start_point, 25)
    ),
        end_passes AS (
            -- 2. Find ALL trackpoints inside the End Gate (~25 meters)
            SELECT
                d.activity_id,
                d.distance_m dist_e,
                ST_distance(d.loc_point, s.end_point) as end_apart
            FROM
                activities.segments s
                    INNER JOIN activities.temp_segment_matching_segments t on s.segment_id = t.segment_id
                    INNER JOIN activities.activity_details_distance d ON d.activity_id = p_activity_id -- 22127682969
--                  activities.segments s ON s.segment_id = 2
            WHERE
                --                 d.distance_m > s.distance_m-50 and
                ST_DWithin(d.loc_point::geography, s.end_point, 25)
        )
       ,potential_pairs AS (
        -- 3. Pair them up: For every start point, find the VERY NEXT end point
        SELECT
            sp.activity_id,
            sp.segment_id,
            sp.utm_srid,
            sp.dist_s AS start_dist_candidate,
            ep.dist_e AS end_dist_candidate,
            sp.seg_dist,
            ep.dist_e- sp.dist_s as act_dist,
            sp.start_apart,
            ep.end_apart,
            ep.dist_e,
            sp.dist_s,
            ROW_NUMBER() OVER (partition by sp.segment_id, ep.dist_e ORDER BY (sp.start_apart, abs(ep.dist_e- sp.dist_s))) as start_rank
        FROM
            start_passes sp
                JOIN
            end_passes ep
            ON sp.activity_id = ep.activity_id  and
               ep.dist_e > sp.dist_s -- The end must happen AFTER the start
        where abs(sp.seg_dist - (ep.dist_e-sp.dist_s)) < 50 --length must be within 50 meters of the fingerprint
    )

       ,unique_starts as (
        SELECT
            activity_id,
            segment_id,
            sp.utm_srid,
            start_dist_candidate,
            end_dist_candidate,
            seg_dist,
            act_dist,
            start_apart,
            end_apart,
            ROW_NUMBER() OVER (partition by segment_id, start_dist_candidate ORDER BY end_apart, act_dist) as end_rank
        FROM potential_pairs sp
        WHERE start_rank = 1
    )

       , valid_pairs as (
        SELECT * FROM unique_starts
        where end_rank = 1)

       , best_pairings as (
        SELECT
            *,
            row_number() OVER (PARTITION BY segment_id, round(start_dist_candidate/300.0,0) ORDER BY (start_apart+end_apart, act_dist)) as match_rank
        FROM valid_pairs
    )
       -- 4. Deduplicate: Handle "lingering" in the gates
       , final_pairings as (
        SELECT
            vp.segment_id,
            vp.utm_srid,
            vp.activity_id,
            st_setsrid(st_makeline(st_makepoint(ad.longitude::double precision, ad.latitude::double precision,
                                                ad.elevation_m::double precision,
                                                ad.distance_m::double precision)
                                   ORDER BY ad.distance_m), 4326) as full_path,
            vp.start_dist_candidate AS activity_start_point,
            vp.end_dist_candidate AS activity_end_point

        FROM
            best_pairings vp
                INNER JOIN activities.activity_details_distance ad on vp.activity_id = ad.activity_id
                and ad.distance_m between vp.start_dist_candidate and vp.end_dist_candidate
        WHERE match_rank=1
        GROUP BY vp.segment_id, vp.activity_id, vp.start_dist_candidate, vp.end_dist_candidate, vp.utm_srid)

    INSERT INTO activities.temp_segment_matches_downselect
    (activity_id, segment_id, best_start_dist, best_end_dist, full_path, full_path_transformed, utm_srid)
    SELECT
        activity_id,
        segment_id,
        activity_start_point as best_start_dist,
        activity_end_point as best_end_dist,
        full_path,
        st_transform(full_path, utm_srid) as full_path_transformed,
        utm_srid
    FROM final_pairings fp;


    UPDATE activities.temp_segment_matches_downselect
    SET dist_deviation = s.dist_deviation,
        ruggedness = s.ruggedness
    FROM activities.vw_segment_matching_downselect_scores s
    WHERE temp_segment_matches_downselect.activity_id = s.activity_id
      and s.best_start_dist=  temp_segment_matches_downselect.best_start_dist
      and s.best_end_dist =  temp_segment_matches_downselect.best_end_dist;
END;
$$;


--
-- Name: segment_matching_finalize_match(boolean, bigint, integer, integer, integer, numeric); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_finalize_match(IN p_approved boolean, IN p_activity_id bigint, IN p_segment_id integer, IN p_start integer, IN p_end integer, IN p_confidence numeric)
    LANGUAGE plpgsql
    AS $$

BEGIN
    WITH
        seg as (
            SELECT
                activity_id,
                activity_type_id,
                distance_m,
                COALESCE(elevation_m_smooth
                             - LEAD(elevation_m_smooth) OVER (ORDER BY distance_m),0) as elevation_delta,
                latitude,
                longitude,
                elevation_m_smooth as elevation_m,
                row_number() OVER (ORDER BY distance_m) as row_asc,
                row_number() OVER (ORDER BY distance_m DESC) as row_desc
            FROM activities.activity_details_distance
            WHERE activity_id = p_activity_id and distance_m between p_start and p_end
            ORDER BY distance_m
        )
        ,
        seg_prep as (
            SELECT
                activity_id,
                activity_type_id,
                min(distance_m)::integer                                                 AS reference_start_point,
                max(distance_m)::integer                                                 AS reference_end_point,
                st_setsrid(st_makeline(st_makepoint(longitude::double precision, latitude::double precision,
                                                    elevation_m::double precision, distance_m::double precision)
                                       ORDER BY distance_m), 4326)               AS segment_path,
                max(distance_m) - min(distance_m)                                        AS distance_m,
                sum(
                        CASE
                            WHEN elevation_delta > 0::numeric THEN elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_gain,
                sum(
                        CASE
                            WHEN elevation_delta < 0::numeric THEN - elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_loss,
                st_makepoint(max(
                                     CASE
                                         WHEN row_asc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_asc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS start_point,
                st_makepoint(max(
                                     CASE
                                         WHEN row_desc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_desc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS end_point
            FROM seg
            GROUP BY activity_id, activity_type_id
            )
    INSERT INTO activities.segment_matches (segment_id, activity_id, activity_start_point,
                                            activity_end_point, activity_path, match_confirmed, match_confidence)
    SELECT
        p_segment_id as segment_id,
        activity_id,
        reference_start_point as activity_start_point,
        reference_end_point as activity_end_point,
        segment_path as activity_path,
        p_approved as match_confirmed,
        p_confidence as match_confidence
    FROM seg_prep
    ON CONFLICT (segment_id, activity_id) DO NOTHING;

    DELETE FROM activities.temp_segment_matches_downselect
           WHERE activity_id = p_activity_id
             and segment_id = p_segment_id
             and best_start_dist = p_start
             and best_end_dist = p_end;

END;
$$;


--
-- Name: segment_matching_mass_confirmation(integer); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_mass_confirmation(IN p_approval_phase integer)
    LANGUAGE plpgsql
    AS $$

BEGIN
    WITH
        seg as (
            SELECT
                scaa.segment_id,
                ad.activity_id,
                ad.activity_type_id,
                ad.distance_m,
                COALESCE(elevation_m_smooth
                             - LEAD(elevation_m_smooth) OVER (ORDER BY ad.distance_m),0) as elevation_delta,
                latitude,
                longitude,
                elevation_m_smooth as elevation_m,
                scaa.confidence,
                row_number() OVER (PARTITION BY segment_id, ad.activity_id ORDER BY ad.distance_m) as row_asc,
                row_number() OVER (PARTITION BY segment_id, ad.activity_id ORDER BY ad.distance_m DESC) as row_desc
            FROM activities.activity_details_distance ad
                INNER JOIN activities.vw_segment_create_auto_approve scaa ON
                    ad.activity_id = scaa.activity_id
                        and ad.distance_m between scaa.best_start_dist and scaa.best_end_dist
                        AND approval_phase <= p_approval_phase
            ORDER BY scaa.segment_id, ad.activity_id, ad.distance_m
        )

        ,seg_prep as (
            SELECT
                segment_id,
                activity_id,
                activity_type_id,
                confidence,
                min(distance_m)::integer                                                 AS reference_start_point,
                max(distance_m)::integer                                                 AS reference_end_point,
                st_setsrid(st_makeline(st_makepoint(longitude::double precision, latitude::double precision,
                                                    elevation_m::double precision, distance_m::double precision)
                                       ORDER BY distance_m), 4326)               AS segment_path,
                max(distance_m) - min(distance_m)                                        AS distance_m,
                sum(
                        CASE
                            WHEN elevation_delta > 0::numeric THEN elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_gain,
                sum(
                        CASE
                            WHEN elevation_delta < 0::numeric THEN - elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_loss,
                st_makepoint(max(
                                     CASE
                                         WHEN row_asc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_asc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS start_point,
                st_makepoint(max(
                                     CASE
                                         WHEN row_desc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_desc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS end_point
            FROM seg
            GROUP BY segment_id, activity_id, activity_type_id, confidence
            )
    INSERT INTO activities.segment_matches (segment_id, activity_id, activity_start_point,
                                            activity_end_point, activity_path, match_confirmed, match_confidence)
    SELECT
        segment_id as segment_id,
        activity_id,
        reference_start_point as activity_start_point,
        reference_end_point as activity_end_point,
        segment_path as activity_path,
        True as match_confirmed,
        confidence as match_confidence
    FROM seg_prep
    ON CONFLICT (segment_id, activity_id) DO NOTHING;

    DELETE FROM activities.temp_segment_matches_downselect tsm
           using activities.vw_segment_create_auto_approve vscaa
        where tsm.activity_id = vscaa.activity_id and vscaa.segment_id = tsm.segment_id
    and vscaa.approval_phase <= p_approval_phase;

END;
$$;


--
-- Name: segment_matching_match_activities(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_match_activities(IN p_segment_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN

    TRUNCATE activities.temp_segment_matching_activities;

    INSERT INTO activities.temp_segment_matching_activities (activity_id)
    SELECT
        a.activity_id
    FROM activities.activities a
-- Rule 1: Match the activity_type_id of the target segment
             JOIN activities.segments target_s ON a.activity_type_id = target_s.activity_type_id
    JOIN activities.segments s ON p_segment_id = s.segment_id and s.activity_reference_id != a.activity_id
    WHERE target_s.segment_id = p_segment_id -- This is p_segment_id

      -- Rule 2: The activity is not already matched to THIS specific segment
      AND NOT EXISTS (
        SELECT 1
        FROM activities.segment_matches sm
        WHERE sm.activity_id = a.activity_id
          AND sm.segment_id = target_s.segment_id
    )

      -- Rule 3: If target is a course, exclude if activity is linked to ANY other course
      AND (
        target_s.is_course = FALSE
            OR NOT EXISTS (
            SELECT 1
            FROM activities.segment_matches sm_other
                     JOIN activities.segments s_other ON sm_other.segment_id = s_other.segment_id
            WHERE sm_other.activity_id = a.activity_id
              AND s_other.is_course = TRUE
        )
        )
      AND  a.activity_path && s.segment_path
      AND ST_DWithin(a.activity_path::geography, s.start_point, 50)
      AND ST_DWithin(a.activity_path::geography, s.end_point, 50);

END;
$$;


--
-- Name: segment_matching_match_segments(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_match_segments(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN

    TRUNCATE activities.temp_segment_matching_segments;

    INSERT INTO activities.temp_segment_matching_segments (segment_id)
    SELECT
        s.segment_id
    FROM activities.segments s

            INNER JOIN activities.activities a ON a.activity_id = p_activity_id
                                                                      and a.activity_type_id = s.activity_type_id
    WHERE    a.activity_path && s.segment_path
      AND ST_DWithin(a.activity_path::geography, s.start_point, 50)
      AND ST_DWithin(a.activity_path::geography, s.end_point, 50);

END;
$$;


--
-- Name: segment_matching_pair_generation(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_pair_generation(IN p_segment_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE activities.temp_segment_matches_downselect;

    WITH start_passes AS (
        -- 1. Find ALL trackpoints inside the Start Gate (~25 meters)
        SELECT
            d.activity_id,
            d.distance_m as dist_s,
            s.distance_m as seg_dist,
            s.segment_id,
            s.utm_srid,
--             s.segment_path,
            ST_distance(d.loc_point, s.start_point) as start_apart
        FROM
            activities.activity_details_distance d
                JOIN
            activities.temp_segment_matching_activities t ON d.activity_id = t.activity_id
                JOIN
            activities.segments s ON s.segment_id = p_segment_id
--                 activities.segments s ON s.segment_id = 13
        WHERE
            ST_DWithin(d.loc_point::geography, s.start_point, 25)
    ),
        end_passes AS (
            -- 2. Find ALL trackpoints inside the End Gate (~25 meters)
            SELECT
                d.activity_id,
                d.distance_m dist_e,
                ST_distance(d.loc_point, s.end_point) as end_apart
            FROM
                activities.activity_details_distance d
                    JOIN
                activities.temp_segment_matching_activities t ON d.activity_id = t.activity_id
                    JOIN
                activities.segments s ON s.segment_id = p_segment_id
--                  activities.segments s ON s.segment_id = 13
            WHERE
            --                 d.distance_m > s.distance_m-50 and
            ST_DWithin(d.loc_point::geography, s.end_point, 25)
        )
       ,potential_pairs AS (
        -- 3. Pair them up: For every start point, find the VERY NEXT end point
        SELECT
            sp.activity_id,
            sp.segment_id,
            sp.utm_srid,
            sp.dist_s AS start_dist_candidate,
            ep.dist_e AS end_dist_candidate,
            sp.seg_dist,
            ep.dist_e- sp.dist_s as act_dist,
            sp.start_apart,
            ep.end_apart,
            ep.dist_e,
            sp.dist_s,
            ROW_NUMBER() OVER (partition by sp.activity_id, ep.dist_e ORDER BY (sp.start_apart, abs(ep.dist_e- sp.dist_s))) as start_rank
        FROM
            start_passes sp
                JOIN
            end_passes ep
            ON sp.activity_id = ep.activity_id  and
               ep.dist_e > sp.dist_s -- The end must happen AFTER the start
        where abs(sp.seg_dist - (ep.dist_e-sp.dist_s)) < 150 --length must be within 50 meters of the fingerprint
    )

       ,unique_starts as (
        SELECT
            activity_id,
            segment_id,
            sp.utm_srid,
            start_dist_candidate,
            end_dist_candidate,
            seg_dist,
            act_dist,
            start_apart,
            end_apart,
            ROW_NUMBER() OVER (partition by activity_id, start_dist_candidate ORDER BY end_apart, act_dist) as end_rank
        FROM potential_pairs sp
        WHERE start_rank = 1
    )

       , valid_pairs as (
        SELECT * FROM unique_starts
        where end_rank = 1)

       , best_pairings as (
        SELECT
            *,
            row_number() OVER (PARTITION BY activity_id, round(start_dist_candidate/300.0,0) ORDER BY (start_apart+end_apart, act_dist)) as match_rank
        FROM valid_pairs
    )
       -- 4. Deduplicate: Handle "lingering" in the gates
       , final_pairings as (
        SELECT
            vp.segment_id,
            vp.utm_srid,
            vp.activity_id,
            st_setsrid(st_makeline(st_makepoint(ad.longitude::double precision, ad.latitude::double precision,
                                                ad.elevation_m::double precision,
                                                ad.distance_m::double precision)
                                   ORDER BY ad.distance_m), 4326) as full_path,
            vp.start_dist_candidate AS activity_start_point,
            vp.end_dist_candidate AS activity_end_point

        FROM
            best_pairings vp
                INNER JOIN activities.activity_details_distance ad on vp.activity_id = ad.activity_id
                and ad.distance_m between vp.start_dist_candidate and vp.end_dist_candidate
        WHERE match_rank=1
        GROUP BY vp.segment_id, vp.activity_id, vp.start_dist_candidate, vp.end_dist_candidate, vp.utm_srid)

    INSERT INTO activities.temp_segment_matches_downselect
    (activity_id, segment_id, best_start_dist, best_end_dist, full_path, full_path_transformed, utm_srid)
    SELECT
        activity_id,
        segment_id,
        activity_start_point as best_start_dist,
        activity_end_point as best_end_dist,
        full_path,
        st_transform(full_path, utm_srid) as full_path_transformed,
        utm_srid
    FROM final_pairings fp;


    UPDATE activities.temp_segment_matches_downselect
    SET dist_deviation = s.dist_deviation,
        ruggedness = s.ruggedness
    FROM activities.vw_segment_matching_downselect_scores s
    WHERE temp_segment_matches_downselect.activity_id = s.activity_id
      and s.best_start_dist=  temp_segment_matches_downselect.best_start_dist
    and s.best_end_dist =  temp_segment_matches_downselect.best_end_dist;
END;
$$;


--
-- Name: segment_matching_segment_creation(text, bigint, integer, integer, boolean); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.segment_matching_segment_creation(IN p_segment_name text, IN p_activity_id bigint, IN p_start integer, IN p_end integer, IN p_is_course boolean)
    LANGUAGE plpgsql
    AS $$

BEGIN
    WITH
       seg as (
            SELECT
                ad.activity_id,
                ad.activity_type_id,
                ad.distance_m,
                COALESCE(elevation_m_smooth
                             - LEAD(elevation_m_smooth) OVER (ORDER BY ad.distance_m),0) as elevation_delta,
                latitude,
                longitude,
                elevation_m_smooth as elevation_m,
                row_number() OVER (ORDER BY ad.distance_m) as row_asc,
                row_number() OVER (ORDER BY ad.distance_m DESC) as row_desc
            FROM activities.activity_details_distance ad
            where activity_id = p_activity_id and distance_m between p_start and p_end
            ORDER BY distance_m
        )
        ,
        seg_prep as (
            SELECT
                activity_id,
                activity_type_id,
                min(distance_m)::integer                                                 AS reference_start_point,
                max(distance_m)::integer                                                 AS reference_end_point,
                st_setsrid(st_makeline(st_makepoint(longitude::double precision, latitude::double precision,
                                                    elevation_m::double precision, distance_m::double precision)
                                       ORDER BY distance_m), 4326)               AS segment_path,
                max(distance_m) - min(distance_m)                                        AS distance_m,
                sum(
                        CASE
                            WHEN elevation_delta > 0::numeric THEN elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_gain,
                sum(
                        CASE
                            WHEN elevation_delta < 0::numeric THEN - elevation_delta
                            ELSE 0::numeric
                            END)                                                         AS elevation_loss,
                st_makepoint(max(
                                     CASE
                                         WHEN row_asc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_asc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS start_point,
                st_makepoint(max(
                                     CASE
                                         WHEN row_desc = 1 THEN longitude
                                         ELSE NULL::numeric
                                         END)::double precision, max(
                                     CASE
                                         WHEN row_desc = 1 THEN latitude
                                         ELSE NULL::numeric
                                         END)::double precision)::geography(Point, 4326) AS end_point
            FROM seg
            GROUP BY activity_id, activity_type_id)
    INSERT INTO activities.segments (
        activity_reference_id,
        reference_start_point,
        reference_end_point,
        is_course,
        segment_name,
        segment_path,
        distance_m,
        elevation_gain,
        elevation_loss,
        start_point,
        end_point,
        activity_type_id
    )
    SELECT
        activity_id as activity_reference_id,
        reference_start_point,
        reference_end_point,
        p_is_course as is_course,
        p_segment_name as segment_name,
        segment_path,
        distance_m,
        elevation_gain,
        elevation_loss,
        start_point,
        end_point,
        activity_type_id
    FROM seg_prep;


END;
$$;


--
-- Name: smooth_elevation_spikes_by_distance(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.smooth_elevation_spikes_by_distance(IN p_activity_id bigint DEFAULT NULL::bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_is_loop BOOLEAN;
    v_start_elev FLOAT;
    v_start_elev_reference FLOAT;
    v_end_elev FLOAT;
    v_end_elev_reference FLOAT;
    v_drift_per_meter FLOAT;
    v_drift_per_meter_reference FLOAT;
    v_total_distance FLOAT;
BEGIN
    -- 1. Create a temporary work table for this activity to keep I/O sequential
    DROP TABLE if exists current_track;

    CREATE TEMP TABLE current_track AS
    SELECT
        activity_id as id,
        distance_m,
        elevation_m,
        elevation_reference,
        loc_point as geom,
        -- STEP A: Rolling Median (Window of 10 meters)
        -- We use a subquery/array approach because percentile_cont isn't a native window function
        (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sub.elevation_m)
         FROM (
                  SELECT t2.elevation_m
                  FROM activities.activity_details_distance t2
                  WHERE t2.activity_id = p_activity_id
                    AND t2.distance_m BETWEEN t1.distance_m - 10 AND t1.distance_m + 10
              ) sub) as smoothed_elev,
        (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sub.elevation_reference)
         FROM (
                  SELECT t2.elevation_reference
                  FROM activities.activity_details_distance t2
                  WHERE t2.activity_id = p_activity_id
                    AND t2.distance_m BETWEEN t1.distance_m - 10 AND t1.distance_m + 10
              ) sub) as smoothed_elev_reference
    FROM activities.activity_details_distance t1
    WHERE activity_id = p_activity_id
    ORDER BY distance_m;

    -- 2. Detect if it's a loop (Start/End within 20m of each other)
    SELECT
        ST_Distance(
                (SELECT geom FROM current_track ORDER BY distance_m ASC LIMIT 1),
                (SELECT geom FROM current_track ORDER BY distance_m DESC LIMIT 1)
        ) < 100 INTO v_is_loop;

    -- 3. If it's a loop, calculate the drift correction
    IF v_is_loop THEN
        SELECT smoothed_elev INTO v_start_elev FROM current_track ORDER BY distance_m ASC LIMIT 1;
        SELECT smoothed_elev INTO v_end_elev FROM current_track ORDER BY distance_m DESC LIMIT 1;
        SELECT smoothed_elev_reference INTO v_start_elev_reference FROM current_track ORDER BY distance_m ASC LIMIT 1;
        SELECT smoothed_elev_reference INTO v_end_elev_reference FROM current_track ORDER BY distance_m DESC LIMIT 1;
        SELECT MAX(distance_m) INTO v_total_distance FROM current_track;

        -- Total drift divided by time
        v_drift_per_meter := (v_end_elev - v_start_elev) / NULLIF(v_total_distance, 0);
        v_drift_per_meter_reference := (v_end_elev_reference - v_start_elev_reference) / NULLIF(v_total_distance, 0);
        --         SELECT
--             ((SELECT smoothed_elev FROM current_track ORDER BY elapsed_duration_s DESC LIMIT 1)
--                 - (SELECT smoothed_elev FROM current_track ORDER BY elapsed_duration_s ASC LIMIT 1))
--             / (SELECT MAX(elapsed_duration_s) FROM current_track)
    ELSE
        v_drift_per_meter := 0;
        v_drift_per_meter_reference := 0;
    END IF;

    -- 4. Update the main table with the Noise-Removed + Drift-Corrected values
    UPDATE activities.activity_details_distance t
    SET elevation_m_smooth = ROUND((ct.smoothed_elev - (ct.distance_m * v_drift_per_meter
        ))::numeric, 2),
        elevation_reference = ROUND((ct.smoothed_elev_reference - (ct.distance_m * v_drift_per_meter_reference
            ))::numeric, 2)
    FROM current_track ct
    WHERE t.activity_id = ct.id and ct.distance_m = t.distance_m;

    DROP TABLE current_track;
END;
$$;


--
-- Name: smooth_elevation_spikes_by_time(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.smooth_elevation_spikes_by_time(IN p_activity_id bigint DEFAULT NULL::bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_is_loop BOOLEAN;
    v_start_elev FLOAT;
    v_end_elev FLOAT;
    v_drift_per_s FLOAT;
    v_total_time FLOAT;
BEGIN
    -- 1. Create a temporary work table for this activity to keep I/O sequential
    DROP TABLE if exists current_track;

    CREATE TEMP TABLE current_track AS
    SELECT
        activity_id as id,
        elapsed_duration_s,
        elevation_m,
        loc_point as geom,
        -- STEP A: Rolling Median (Window of 10 meters)
        -- We use a subquery/array approach because percentile_cont isn't a native window function
        (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sub.elevation_m)
         FROM (
                  SELECT t2.elevation_m
                  FROM activities.activity_details t2
                  WHERE t2.activity_id = p_activity_id
                    AND t2.elapsed_duration_s BETWEEN t1.elapsed_duration_s - 10 AND t1.elapsed_duration_s + 10
              ) sub) as smoothed_elev
    FROM activities.activity_details t1
    WHERE activity_id = p_activity_id
    ORDER BY elapsed_duration_s;

    -- 2. Detect if it's a loop (Start/End within 20m of each other)
    SELECT
        ST_Distance(
                (SELECT geom FROM current_track ORDER BY elapsed_duration_s ASC LIMIT 1),
                (SELECT geom FROM current_track ORDER BY elapsed_duration_s DESC LIMIT 1)
        ) < 100 INTO v_is_loop;

    -- 3. If it's a loop, calculate the drift correction
    IF v_is_loop THEN
        SELECT smoothed_elev INTO v_start_elev FROM current_track ORDER BY elapsed_duration_s ASC LIMIT 1;
        SELECT smoothed_elev INTO v_end_elev FROM current_track ORDER BY elapsed_duration_s DESC LIMIT 1;
        SELECT MAX(elapsed_duration_s) INTO v_total_time FROM current_track;

        -- Total drift divided by time
        v_drift_per_s := (v_end_elev - v_start_elev) / NULLIF(v_total_time, 0);
--         SELECT
--             ((SELECT smoothed_elev FROM current_track ORDER BY elapsed_duration_s DESC LIMIT 1)
--                 - (SELECT smoothed_elev FROM current_track ORDER BY elapsed_duration_s ASC LIMIT 1))
--             / (SELECT MAX(elapsed_duration_s) FROM current_track)
    ELSE
        v_drift_per_s:= 0;
    END IF;

    -- 4. Update the main table with the Noise-Removed + Drift-Corrected values
    UPDATE activities.activity_details t
    SET elevation_m_smooth = ROUND((ct.smoothed_elev - (ct.elapsed_duration_s * v_drift_per_s
     ))::numeric, 2)
    FROM current_track ct
    WHERE t.activity_id = ct.id and ct.elapsed_duration_s = t.elapsed_duration_s;

    DROP TABLE current_track;
END;
$$;


--
-- Name: update_elevation_reference(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_elevation_reference(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- We use a CTE pipeline to calculate the two-pass adjustment in memory
    -- before executing a single UPDATE write to the disk.
    WITH run_data AS (
        -- Step 1: Aggregate the new run's data into grid cells
        SELECT
            grid_5m_00_id,
            avg(elevation_m_smooth) AS m_smooth,
            avg(elevation_reference) AS m_ref
        FROM activities.activity_details
        WHERE activity_id = p_activity_id
          AND grid_5m_00_id IS NOT NULL
        GROUP BY grid_5m_00_id
    ),
         pass_1 AS (
             -- Step 2: Calculate the first adjustment using m_smooth
             SELECT
                 r.grid_5m_00_id,
                 r.avg_elevation AS old_elev,
                 r.obs_count,
                 r.elev_stddev,
                 rd.m_smooth,
                 rd.m_ref,
                 -- Gain 1: Normal running average, penalized if outside 2 standard deviations
                 CASE
                     WHEN r.elev_stddev > 0 AND abs(rd.m_smooth - r.avg_elevation) > (2 * r.elev_stddev)
                         THEN (1.0 / (r.obs_count + 1.0)) * 0.1
                     ELSE (1.0 / (r.obs_count + 1.0))
                     END AS k1
             FROM activities.elevation_reference r
                      JOIN run_data rd ON r.grid_5m_00_id = rd.grid_5m_00_id
         ),
         pass_2 AS (
             -- Step 3: Calculate the second adjustment using m_ref ON TOP of pass 1
             SELECT
                 grid_5m_00_id,
                 obs_count,
                 m_ref,
                 -- Calculate the intermediate elevation after Pass 1
                 old_elev + k1 * (m_smooth - old_elev) AS elev_after_1,

                 -- Gain 2: Calculated against the NEW observation count (+2) and the intermediate elevation
                 CASE
                     WHEN elev_stddev > 0 AND abs(m_ref - (old_elev + k1 * (m_smooth - old_elev))) > (2 * elev_stddev)
                         THEN (1.0 / (obs_count + 2.0)) * 0.1
                     ELSE (1.0 / (obs_count + 2.0))
                     END AS k2
             FROM pass_1
         )
    -- Step 4: Execute the single disk write
    UPDATE activities.elevation_reference r
    SET
        -- Apply Pass 2 math to the Pass 1 result
        avg_elevation = p2.elev_after_1 + p2.k2 * (p2.m_ref - p2.elev_after_1),
        -- Increment observations to reflect the two passes
        obs_count = r.obs_count + 2,
        last_updated = current_timestamp
    FROM pass_2 p2
    WHERE r.grid_5m_00_id = p2.grid_5m_00_id;

    -- Note: stddev is left unchanged here for performance.
    -- It acts as our historical "anchor" of trust.

END;
$$;


--
-- Name: update_elevation_reference_by_distance(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_elevation_reference_by_distance(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- We use a CTE pipeline to calculate the two-pass adjustment in memory,
    -- bridging both existing rows and completely new grid cells,
    -- before executing a single UPSERT to the disk.
    WITH run_data AS (
        -- Step 1: Aggregate the new run's data into grid cells
        SELECT
            grid_5m_00_id,
            avg(elevation_m_smooth) AS m_smooth,
            avg(elevation_reference) AS m_ref,
            -- Calculate standard deviation for new inserts; default to 0 if only 1 data point hits the cell
            COALESCE(stddev(elevation_m_smooth), 0) AS run_stddev
        FROM activities.activity_details_distance
        WHERE activity_id = p_activity_id
          AND grid_5m_00_id IS NOT NULL
        GROUP BY grid_5m_00_id
    ),
         merged_state AS (
             -- Step 2: Combine incoming run data with existing reference data (if any)
             SELECT
                 rd.grid_5m_00_id,
                 rd.m_smooth,
                 rd.m_ref,
                 rd.run_stddev,
                 r.avg_elevation AS old_elev,
                 COALESCE(r.obs_count, 0) AS obs_count,
                 COALESCE(r.elev_stddev, 0) AS elev_stddev,

                 -- Gain 1: Normal running average, penalized if outside 2 standard deviations.
                 -- (Uses COALESCE on old_elev to gracefully handle brand new rows)
                 CASE
                     WHEN r.elev_stddev > 0 AND abs(rd.m_smooth - r.avg_elevation) > (2 * r.elev_stddev)
                         THEN (1.0 / (COALESCE(r.obs_count, 0) + 1.0)) * 0.1
                     ELSE (1.0 / (COALESCE(r.obs_count, 0) + 1.0))
                     END AS k1
             FROM run_data rd
                      LEFT JOIN activities.elevation_reference r
                                ON rd.grid_5m_00_id = r.grid_5m_00_id
         ),
         pass_2 AS (
             -- Step 3: Calculate the intermediate elevation and the second gain (k2)
             SELECT
                 grid_5m_00_id,
                 run_stddev,
                 obs_count,
                 elev_stddev,
                 m_ref,

                 -- Intermediate elevation after Pass 1. If row is new, old_elev is null,
                 -- and k1 is 1.0, meaning elev_after_1 naturally becomes m_smooth.
                 COALESCE(old_elev, m_smooth) + k1 * (m_smooth - COALESCE(old_elev, m_smooth)) AS elev_after_1,

                 -- Gain 2: Calculated against the NEW observation count (+2)
                 CASE
                     WHEN elev_stddev > 0 AND abs(m_ref - (COALESCE(old_elev, m_smooth) + k1 * (m_smooth - COALESCE(old_elev, m_smooth)))) > (2 * elev_stddev)
                         THEN (1.0 / (obs_count + 2.0)) * 0.1
                     ELSE (1.0 / (obs_count + 2.0))
                     END AS k2
             FROM merged_state
         ),
         final_calc AS (
             -- Step 4: Finalize the math for the upsert
             SELECT
                 grid_5m_00_id,
                 -- Apply Pass 2 math to the Pass 1 result
                 elev_after_1 + k2 * (m_ref - elev_after_1) AS final_avg_elevation,
                 obs_count + 2 AS final_obs_count,

                 -- If this is a brand new cell (historical obs_count = 0), seed it with the run's stddev
                 CASE
                     WHEN obs_count = 0 THEN run_stddev
                     ELSE elev_stddev
                     END AS final_stddev
             FROM pass_2
         )
    -- Step 5: Execute the single disk write (UPSERT)
    -- Requires grid_5m_00_id to be a UNIQUE constraint or PRIMARY KEY
    INSERT INTO activities.elevation_reference (
        grid_5m_00_id,
        avg_elevation,
        obs_count,
        elev_stddev,
        last_updated
    )
    SELECT
        grid_5m_00_id,
        final_avg_elevation,
        final_obs_count,
        final_stddev,
        current_timestamp
    FROM final_calc
    ON CONFLICT (grid_5m_00_id) DO UPDATE SET
                                              avg_elevation = EXCLUDED.avg_elevation,
                                              obs_count = EXCLUDED.obs_count,
                                              last_updated = EXCLUDED.last_updated;

    -- Note: stddev is left unchanged on UPDATE per your original design,
    -- serving as the historical "anchor" of trust. It is only set on INSERT.

END;
$$;


--
-- Name: update_elevation_reference_by_time(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_elevation_reference_by_time(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- We use a CTE pipeline to calculate the two-pass adjustment in memory,
    -- bridging both existing rows and completely new grid cells,
    -- before executing a single UPSERT to the disk.
    WITH run_data AS (
        -- Step 1: Aggregate the new run's data into grid cells
        SELECT
            grid_5m_00_id,
            avg(elevation_m_smooth) AS m_smooth,
            avg(COALESCE(elevation_reference,elevation_m_smooth)) AS m_ref,
            -- Calculate standard deviation for new inserts; default to 0 if only 1 data point hits the cell
            COALESCE(stddev(elevation_m_smooth), 0) AS run_stddev
        FROM activities.activity_details
        WHERE activity_id = p_activity_id
          AND grid_5m_00_id IS NOT NULL
        GROUP BY grid_5m_00_id
    ),
         merged_state AS (
             -- Step 2: Combine incoming run data with existing reference data (if any)
             SELECT
                 rd.grid_5m_00_id,
                 rd.m_smooth,
                 rd.m_ref,
                 rd.run_stddev,
                 r.avg_elevation AS old_elev,
                 COALESCE(r.obs_count, 0) AS obs_count,
                 COALESCE(r.elev_stddev, 0) AS elev_stddev,

                 -- Gain 1: Normal running average, penalized if outside 2 standard deviations.
                 -- (Uses COALESCE on old_elev to gracefully handle brand new rows)
                 CASE
                     WHEN r.elev_stddev > 0 AND abs(rd.m_smooth - r.avg_elevation) > (2 * r.elev_stddev)
                         THEN (1.0 / (COALESCE(r.obs_count, 0) + 1.0)) * 0.1
                     ELSE (1.0 / (COALESCE(r.obs_count, 0) + 1.0))
                     END AS k1
             FROM run_data rd
                      LEFT JOIN activities.elevation_reference r
                                ON rd.grid_5m_00_id = r.grid_5m_00_id
         ),
         pass_2 AS (
             -- Step 3: Calculate the intermediate elevation and the second gain (k2)
             SELECT
                 grid_5m_00_id,
                 run_stddev,
                 obs_count,
                 elev_stddev,
                 m_ref,

                 -- Intermediate elevation after Pass 1. If row is new, old_elev is null,
                 -- and k1 is 1.0, meaning elev_after_1 naturally becomes m_smooth.
                 COALESCE(old_elev, m_smooth) + k1 * (m_smooth - COALESCE(old_elev, m_smooth)) AS elev_after_1,

                 -- Gain 2: Calculated against the NEW observation count (+2)
                 CASE
                     WHEN elev_stddev > 0 AND abs(m_ref - (COALESCE(old_elev, m_smooth) + k1 * (m_smooth - COALESCE(old_elev, m_smooth)))) > (2 * elev_stddev)
                         THEN (1.0 / (obs_count + 2.0)) * 0.1
                     ELSE (1.0 / (obs_count + 2.0))
                     END AS k2
             FROM merged_state
         ),
         final_calc AS (
             -- Step 4: Finalize the math for the upsert
             SELECT
                 grid_5m_00_id,
                 -- Apply Pass 2 math to the Pass 1 result
                 elev_after_1 + k2 * (m_ref - elev_after_1) AS final_avg_elevation,
                 obs_count + 2 AS final_obs_count,

                 -- If this is a brand new cell (historical obs_count = 0), seed it with the run's stddev
                 CASE
                     WHEN obs_count = 0 THEN run_stddev
                     ELSE elev_stddev
                     END AS final_stddev
             FROM pass_2
         )
    -- Step 5: Execute the single disk write (UPSERT)
    -- Requires grid_5m_00_id to be a UNIQUE constraint or PRIMARY KEY
    INSERT INTO activities.elevation_reference (
        grid_5m_00_id,
        avg_elevation,
        obs_count,
        elev_stddev,
        last_updated
    )
    SELECT
        grid_5m_00_id,
        final_avg_elevation,
        final_obs_count,
        final_stddev,
        current_timestamp
    FROM final_calc
    ON CONFLICT (grid_5m_00_id) DO UPDATE SET
                                              avg_elevation = EXCLUDED.avg_elevation,
                                              obs_count = EXCLUDED.obs_count,
                                              last_updated = EXCLUDED.last_updated;

    -- Note: stddev is left unchanged on UPDATE per your original design,
    -- serving as the historical "anchor" of trust. It is only set on INSERT.

END;
$$;


--
-- Name: update_elevations_for_activity(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_elevations_for_activity(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_rows_updated INT;
BEGIN
    RAISE NOTICE 'Updating elevations for activity %...', p_activity_id;
    
    UPDATE activities.activity_details ad
    SET elevation_tiles = ST_Value(
        et.rast, 
        ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326)
    )
    FROM activities.elevation_tiles et
    WHERE ad.activity_id = p_activity_id
      AND et.tile_id = activities.get_tile_id(ad.latitude, ad.longitude)
      AND ad.elevation_tiles IS NULL
      AND ST_Intersects(et.rast, ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326));
    
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    
    RAISE NOTICE 'Updated % points for activity %', v_rows_updated, p_activity_id;
END;
$$;


--
-- Name: update_segment_matches(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_segment_matches(IN p_segment_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE activities.temp_segment_matches_downselect;

    WITH start_passes AS (
        -- 1. Find ALL trackpoints inside the Start Gate (~25 meters)
        SELECT
            d.activity_id,
            d.distance_m as dist_s,
            s.distance_m as seg_dist,
            s.segment_id,
--             s.segment_path,
            ST_distance(d.loc_point, s.start_point) as start_apart
        FROM
            activities.activity_details_distance d
                JOIN
            activities.temp_segment_matching_activities t ON d.activity_id = t.activity_id
                JOIN
            activities.segments s ON s.segment_id = p_segment_id
--                 activities.segments s ON s.segment_id = 2
        WHERE
            ST_DWithin(d.loc_point::geography, s.start_point, 25)
    ),
        end_passes AS (
            -- 2. Find ALL trackpoints inside the End Gate (~25 meters)
            SELECT
                d.activity_id,
                d.distance_m dist_e,
                ST_distance(d.loc_point, s.end_point) as end_apart
            FROM
                activities.activity_details_distance d
                    JOIN
                activities.temp_segment_matching_activities t ON d.activity_id = t.activity_id
                    JOIN
                activities.segments s ON s.segment_id = p_segment_id
--                  activities.segments s ON s.segment_id = 2
            WHERE
--                 d.distance_m > s.distance_m-50 and
                ST_DWithin(d.loc_point::geography, s.end_point, 25)
        )
       ,potential_pairs AS (
        -- 3. Pair them up: For every start point, find the VERY NEXT end point
        SELECT
            sp.activity_id,
            sp.segment_id,
--                  sp.segment_path,
            sp.dist_s AS start_dist_candidate,
            ep.dist_e AS end_dist_candidate,
            sp.seg_dist,
            ep.dist_e- sp.dist_s as act_dist,
            sp.start_apart,
            ep.end_apart,
            ep.dist_e,
            sp.dist_s,
            ROW_NUMBER() OVER (partition by sp.activity_id, ep.dist_e ORDER BY (sp.start_apart, abs(ep.dist_e- sp.dist_s))) as start_rank
        FROM
            start_passes sp
                JOIN
            end_passes ep
            ON sp.activity_id = ep.activity_id  and
               ep.dist_e > sp.dist_s -- The end must happen AFTER the start
        where abs(sp.seg_dist - (ep.dist_e-sp.dist_s)) < 50 --length must be within 50 meters of the fingerprint
    )

       ,unique_starts as (
        SELECT
            activity_id,
            segment_id,
--                  segment_path,
            start_dist_candidate,
            end_dist_candidate,
            seg_dist,
            act_dist,
            start_apart,
            end_apart,
            ROW_NUMBER() OVER (partition by activity_id, start_dist_candidate ORDER BY end_apart, act_dist) as end_rank
        FROM potential_pairs sp
        WHERE start_rank = 1
    )

       , valid_pairs as (
        SELECT * FROM unique_starts
        where end_rank = 1)

       , best_pairings as (
        SELECT
            *,
            row_number() OVER (PARTITION BY activity_id, round(start_dist_candidate/300.0,0) ORDER BY (start_apart+end_apart, act_dist)) as match_rank
        FROM valid_pairs
    )
       -- 4. Deduplicate: Handle "lingering" in the gates
       , final_pairings as (
        SELECT
            vp.segment_id,
            vp.activity_id,
            st_setsrid(st_makeline(st_makepoint(ad.longitude::double precision, ad.latitude::double precision,
                                                ad.elevation_m::double precision,
                                                ad.distance_m::double precision)
                                   ORDER BY ad.distance_m), 4326) as activity_path,
            vp.start_dist_candidate AS activity_start_point,
            vp.end_dist_candidate AS activity_end_point

        FROM
            best_pairings vp
                INNER JOIN activities.activity_details_distance ad on vp.activity_id = ad.activity_id
                and ad.distance_m between vp.start_dist_candidate and vp.end_dist_candidate
        WHERE match_rank=1
        GROUP BY vp.segment_id, vp.activity_id, vp.start_dist_candidate, vp.end_dist_candidate)



    INSERT INTO activities.temp_segment_matches_downselect
    (activity_id, best_start_dist, best_end_dist, segment_path)
    SELECT
        activity_id,
        activity_start_point as best_start_dist,
        activity_end_point as best_end_dist,
        activity_path as segment_path
    FROM final_pairings fp;

    WITH segment_data AS (
        SELECT
            segment_id,
            segment_path,
            -- Dynamically calculate the global UTM SRID based on the start point's Lat/Lon
            CASE
                WHEN ST_Y(start_point::geometry) >= 0 THEN 32600
                ELSE 32700
                END + floor((ST_X(start_point::geometry) + 180) / 6)::int + 1 AS utm_srid
        FROM activities.segments
        WHERE segment_id = p_segment_id
    ),
        path_metrics AS (
        SELECT
            t.activity_id,
            t.best_start_dist,
            t.best_end_dist,
            -- 1. Calculate Spatial Deviation using Frechet Distance
            -- We normalize this: 0 deviation is perfect (1.0 score)
            ST_FrechetDistance(
                    ST_Transform(t.segment_path, sd.utm_srid),
                    ST_Transform(sd.segment_path, sd.utm_srid)
            ) AS max_deviation_m,
            ST_Area(
                    ST_MakePolygon(
                            ST_AddPoint(
                                    ST_MakeLine(
                                            ST_Transform(t.segment_path, sd.utm_srid),
                                            ST_Reverse(ST_Transform(sd.segment_path, sd.utm_srid))
                                    ),
                                    ST_StartPoint(ST_Transform(t.segment_path, sd.utm_srid))
                            )
                    )
            ) / NULLIF(ST_Length(ST_Transform(sd.segment_path, sd.utm_srid)), 0) AS avg_deviation_m,
            abs(ST_ZMax(t.segment_path) - ST_ZMin(t.segment_path)) as match_range,
            abs(ST_ZMax(sd.segment_path) - ST_ZMin(sd.segment_path)) as source_range
        FROM
            activities.temp_segment_matches_downselect t
                JOIN
            segment_data sd ON sd.segment_id = p_segment_id
    ),
         path_calcs as (SELECT
                            activity_id,
                            best_start_dist,
                            best_end_dist,
                            -- 1. Deviation Score (Normalized: 1.0 is perfect)
                            -- We assume a 50m deviation is a 'total' mismatch (0.0)
                            max_deviation_m AS max_deviation,
                            avg_deviation_m AS avg_deviation,

                            -- 2. Ruggedness Score
                            -- Compares the vertical scale of the activity vs the segment
                            1 - LEAST(1, abs(match_range - source_range) / NULLIF(source_range, 0)) AS ruggedness_score,

                            -- 3. Overall Confidence (Weighted Average)
                            -- Spatial geometry is usually more 'truthful' than GPS elevation data
                            -- Weight: 70% Path, 30% Elevation
                            (GREATEST(0, 1 - (avg_deviation_m / 30.0)) * 0.7) +
                            (GREATEST(0, 1 - (max_deviation_m / 100.0)) * 0.3) AS spatial_score
                        FROM
                            path_metrics)
    UPDATE activities.temp_segment_matches_downselect
    set confidence = round(((spatial_score * 0.8) + (ruggedness_score * 0.2))::NUMERIC, 3),
        max_deviation = round(pc.max_deviation::NUMERIC,1),
        avg_deviation = round(pc.avg_deviation::NUMERIC,1),
        ruggedness = round(pc.ruggedness_score::NUMERIC,3)
    FROM path_calcs pc
    WHERE temp_segment_matches_downselect.activity_id = pc.activity_id;


END;
$$;


--
-- Name: update_segment_matches_freschet(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_segment_matches_freschet(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    rec record;
BEGIN
    -- Process rows one by one to avoid a single bad geometry
    -- from crashing the whole batch and to allow detailed logging.
    FOR rec IN
        SELECT
            t.activity_id,
            t.best_start_dist,
            t.best_end_dist,
            t.full_path_transformed,
            s.segment_path_transformed
        FROM activities.temp_segment_matches_downselect t
                 INNER JOIN activities.segments s ON s.segment_id = t.segment_id
        WHERE t.activity_id = p_activity_id
        LOOP
            BEGIN
                -- Skip NULL or empty geometries (Fréchet not defined).
                IF rec.full_path_transformed IS NULL
                    OR rec.segment_path_transformed IS NULL
                    OR ST_IsEmpty(rec.full_path_transformed)
                    OR ST_IsEmpty(rec.segment_path_transformed) THEN
                    RAISE NOTICE 'Skipping (activity_id=%, dists %/%) due to NULL or empty geometry.',
                        rec.activity_id, rec.best_start_dist, rec.best_end_dist;
                    CONTINUE;
                END IF;

                -- Ensure valid geometries. ST_FrechetDistance may crash
                -- on invalid inputs; make them valid first.
                IF NOT ST_IsValid(rec.full_path_transformed) THEN
                    RAISE NOTICE 'Fixing invalid full_path_transformed for activity_id=%.', rec.activity_id;
                    rec.full_path_transformed := ST_MakeValid(rec.full_path_transformed);
                END IF;

                IF NOT ST_IsValid(rec.segment_path_transformed) THEN
                    RAISE NOTICE 'Fixing invalid segment_path_transformed for activity_id=%.', rec.activity_id;
                    rec.segment_path_transformed := ST_MakeValid(rec.segment_path_transformed);
                END IF;

                -- If geometry is still NULL after making valid (e.g., ST_MakeValid
                -- could return NULL on completely degenerate input), skip.
                IF rec.full_path_transformed IS NULL OR rec.segment_path_transformed IS NULL THEN
                    RAISE NOTICE 'Skipping (activity_id=%) because geometry became NULL after validation.',
                        rec.activity_id;
                    CONTINUE;
                END IF;

                -- Reduce complexity: Fréchet distance is O(n*m); extremely large
                -- geometries can cause memory exhaustion or internal errors.
                -- Use a tolerance suitable for your data (adjust as needed).
                rec.full_path_transformed := ST_Simplify(rec.full_path_transformed, 0.5);
                rec.segment_path_transformed := ST_Simplify(rec.segment_path_transformed, 0.5);

                -- Now compute the Fréchet distance. The UPDATE is done row‑by‑row.
                UPDATE activities.temp_segment_matches_downselect
                SET freschet_deviation_m = ROUND(
                        ST_FrechetDistance(
                                rec.full_path_transformed,
                                rec.segment_path_transformed
                        )::numeric, 1)
                WHERE activity_id = rec.activity_id
                  AND best_start_dist = rec.best_start_dist
                  AND best_end_dist = rec.best_end_dist;

                RAISE NOTICE 'Successfully updated Fréchet distance for activity_id=% dists %/%',
                    rec.activity_id, rec.best_start_dist, rec.best_end_dist;

            EXCEPTION WHEN OTHERS THEN
                -- Catch any normal SQL/PlpgSQL error (not segfaults, but still
                -- useful if, for example, ST_FrechetDistance raises a regular error).
                RAISE WARNING 'Failed Fréchet distance for activity_id=% (dists %/%): %',
                    rec.activity_id, rec.best_start_dist, rec.best_end_dist, SQLERRM;
            -- Continue with next row; the failing row will keep its previous
            -- (likely NULL) freschet_deviation_m.
            END;
        END LOOP;
END;
$$;


--
-- Name: update_segment_matches_hausdorff(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_segment_matches_hausdorff(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN

    with calcs as (
        SELECT
            activity_id,
            best_start_dist,
            best_end_dist,
--             2. Average Deviation in Meters
--             All components of the Polygon calculation must be in UTM
            ST_HausdorffDistance(
                    t.full_path_transformed,
                    segment_path_transformed
            ) AS hausdorff_deviation_m

        FROM activities.temp_segment_matches_downselect t
                 INNER JOIN activities.segments s ON s.segment_id = t.segment_id
        WHERE activity_id = p_activity_id)

       , final_sql as (SELECT activity_id,
                              best_start_dist,
                              best_end_dist,
                              hausdorff_deviation_m
                       from calcs)
    UPDATE activities.temp_segment_matches_downselect
    set hausdorff_deviation_m = s.hausdorff_deviation_m
    FROM final_sql s
    WHERE temp_segment_matches_downselect.activity_id = s.activity_id
      and s.best_start_dist=  temp_segment_matches_downselect.best_start_dist
      and s.best_end_dist =  temp_segment_matches_downselect.best_end_dist;

END;
$$;


--
-- Name: update_segment_matches_polygon(bigint); Type: PROCEDURE; Schema: activities; Owner: -
--

CREATE PROCEDURE activities.update_segment_matches_polygon(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN

    with calcs as (
        SELECT
            activity_id,
            best_start_dist,
            best_end_dist,
            DEGREES(ST_Azimuth(ST_PointN(full_path_transformed, 1),
                               ST_PointN(full_path_transformed, 11))) as act_start_hdg,
            DEGREES(ST_Azimuth(ST_PointN(segment_path_transformed, 1),
                               ST_PointN(segment_path_transformed, 11))) as seg_start_hdg,

            -- Final Heading (Point N-10 to Point N)
            DEGREES(ST_Azimuth(ST_PointN(full_path_transformed,
                                         ST_NumPoints(full_path_transformed) - 10),
                               ST_EndPoint(full_path_transformed))) as act_end_hdg,
            DEGREES(ST_Azimuth(ST_PointN(segment_path_transformed,
                                         ST_NumPoints(segment_path_transformed) - 10),
                               ST_EndPoint(segment_path_transformed))) as seg_end_hdg,
--             2. Average Deviation in Meters
--             All components of the Polygon calculation must be in UTM
            ROUND((ST_Area(
                           ST_MakePolygon(
                                   ST_AddPoint(
                                           ST_MakeLine(
                                                   full_path_transformed,
                                                   ST_Reverse(segment_path_transformed)
                                           ),
                                           ST_StartPoint(full_path_transformed)
                                   )
                           )
                   ) / NULLIF(ST_Length(segment_path_transformed), 0))::NUMERIC,2) AS polygon_deviation_m

        FROM activities.temp_segment_matches_downselect t
                 INNER JOIN activities.segments s ON s.segment_id = t.segment_id
        WHERE activity_id = p_activity_id)

       , final_sql as (SELECT activity_id,
                              best_start_dist,
                              best_end_dist,
                              polygon_deviation_m,
                              ((180 - ABS(ABS(act_start_hdg - seg_start_hdg) - 180)) -- start heading
                                  + (180 - ABS(ABS(act_end_hdg - seg_end_hdg) - 180)) -- end heading
                              )/2 as heading_deviation
                       from calcs)
    UPDATE activities.temp_segment_matches_downselect
    set polygon_deviation_m = s.polygon_deviation_m,
        heading_deviation = s.heading_deviation
    FROM final_sql s
    WHERE temp_segment_matches_downselect.activity_id = s.activity_id
      and s.best_start_dist=  temp_segment_matches_downselect.best_start_dist
      and s.best_end_dist =  temp_segment_matches_downselect.best_end_dist;

END;
$$;


--
-- Name: fn_diary_stats(date); Type: FUNCTION; Schema: food; Owner: -
--

CREATE FUNCTION food.fn_diary_stats(day date) RETURNS TABLE(today_kcal double precision, last_24h_kcal double precision, avg_7d_kcal double precision, scale_max double precision)
    LANGUAGE sql STABLE
    AS $$
WITH day_bounds AS (
    SELECT (day::TIMESTAMPTZ) AS d0,
           (day::TIMESTAMPTZ + INTERVAL '1 day') AS d1
),
     today AS (
         SELECT sum(e.kcal)::DOUBLE PRECISION AS k
         FROM food.diary_entries e, day_bounds b
         WHERE e.logged_at >= b.d0 AND e.logged_at < b.d1
     ),
     last_24h AS (
         SELECT sum(e.kcal)::DOUBLE PRECISION AS k
         FROM food.diary_entries e
         WHERE e.logged_at >= (now() - INTERVAL '24 hours')
           AND e.logged_at <= now()
     ),
     week AS (
         SELECT avg(d.k)::DOUBLE PRECISION AS k
         FROM (
                  SELECT sum(e.kcal) AS k
                  FROM food.diary_entries e, day_bounds b
                  WHERE e.logged_at >= (b.d0 - INTERVAL '7 days')
                    AND e.logged_at < b.d0
                  GROUP BY (e.logged_at::DATE)
              ) d
     )
SELECT t.k, r.k, w.k,
       greatest(coalesce(t.k, 0), coalesce(r.k, 0), coalesce(w.k, 0))
FROM today t, last_24h r, week w;
$$;


--
-- Name: fn_food_completeness(integer); Type: FUNCTION; Schema: food; Owner: -
--

CREATE FUNCTION food.fn_food_completeness(present_count integer) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT CASE WHEN coalesce(present_count, 0) >= 8 THEN 'High'
            WHEN coalesce(present_count, 0) >= 4 THEN 'Medium'
            ELSE 'Low' END;
$$;


--
-- Name: fn_food_plausibility(double precision, double precision, double precision, double precision, double precision, double precision, double precision); Type: FUNCTION; Schema: food; Owner: -
--

CREATE FUNCTION food.fn_food_plausibility(kcal double precision, protein_g double precision, fat_g double precision, carbs_g double precision, sugars_g double precision, satfat_g double precision, fiber_g double precision) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT
    (CASE WHEN kcal IS NOT NULL AND protein_g IS NOT NULL
        AND fat_g IS NOT NULL AND carbs_g IS NOT NULL
        AND (4*protein_g + 9*fat_g + 4*carbs_g) > 0
              THEN (CASE WHEN kcal / (4*protein_g + 9*fat_g + 4*carbs_g)
            BETWEEN 0.7 AND 1.3 THEN 0 ELSE 1 END)
          ELSE 0 END)
        + (CASE WHEN protein_g IS NOT NULL AND fat_g IS NOT NULL
        AND carbs_g IS NOT NULL
                    THEN (CASE WHEN (protein_g + fat_g + carbs_g) <= 102
                                   THEN 0 ELSE 1 END)
                ELSE 0 END)
        + (CASE WHEN sugars_g IS NOT NULL AND carbs_g IS NOT NULL
                    THEN (CASE WHEN sugars_g <= carbs_g THEN 0 ELSE 1 END)
                ELSE 0 END)
        + (CASE WHEN satfat_g IS NOT NULL AND fat_g IS NOT NULL
                    THEN (CASE WHEN satfat_g <= fat_g THEN 0 ELSE 1 END)
                ELSE 0 END)
        + (CASE WHEN fiber_g IS NOT NULL AND carbs_g IS NOT NULL
                    THEN (CASE WHEN fiber_g <= carbs_g THEN 0 ELSE 1 END)
                ELSE 0 END)
        + (CASE WHEN kcal IS NOT NULL
                    THEN (CASE WHEN kcal <= 902 THEN 0 ELSE 1 END)
                ELSE 0 END);
$$;


--
-- Name: isrc_duplicate_finder(); Type: PROCEDURE; Schema: music; Owner: -
--

CREATE PROCEDURE music.isrc_duplicate_finder()
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

	TRUNCATE staging.isrc_dupe_seeds;

	INSERT INTO staging.isrc_dupe_seeds
	(isrc, track_name_search,
	artist_name_search, album_name_search, artist_id, album_id, duration_ms)
	SELECT DISTINCT
		at1.track_isrc,
		at1.track_name_search,
		aar.artist_name_search,
		aal.album_name_search,
		at1.artist_id,
		at1.album_id,
		at1.duration_ms
	FROM music.all_tracks at1
		LEFT JOIN music.isrc_swaps s on s.valid_mapping and s.original_isrc = at1.track_isrc
		INNER JOIN music.vw_best_track_id bt on bt.best_track_id = at1.track_id
		INNER JOIN music.all_artists aar on aar.artist_id = at1.artist_id
		INNER JOIN music.all_albums aal on aal.album_id = at1.album_id
	WHERE 
		(s.mapped_isrc IS NULL 
		OR at1.track_isrc IS NOT DISTINCT FROM s.mapped_isrc)
	and	artist_name_search not like '%phish%';

	ANALYZE staging.isrc_dupe_seeds;
	CLUSTER staging.isrc_dupe_seeds USING idx_isrc_seeds_duration;

	TRUNCATE staging.isrc_dupe_pairs;

	INSERT INTO staging.isrc_dupe_pairs 
	SELECT 
	dp.*
	FROM (
	
	SELECT
		s1.isrc as isrc1,
		s2.isrc as isrc2,
		s1.track_name_search as track1,
		s2.track_name_search as track2,
		s1.artist_name_search as artist1,
		s2.artist_name_search as artist2,
		s1.artist_id as artist_id1,
		s2.artist_id as artist_id2,
		s1.album_name_search as album1,
		s2.album_name_search as album2,
		s1.album_id as album_id1,
		s2.album_id as album_id2,
		s1.duration_ms as duration1,
		s2.duration_ms as duration2
		FROM staging.isrc_dupe_seeds s1
			INNER JOIN staging.isrc_dupe_seeds s2 on s1.isrc != s2.isrc
			and s1.duration_ms - s2.duration_ms < 10000
			and s2.duration_ms - s1.duration_ms < 10000
			and SIMILARITY(s1.track_name_search, s2.track_name_search) >0.6
		) dp
		LEFT JOIN music.isrc_non_swaps s on (isrc1 = original_isrc and isrc2 = mapped_isrc)
			OR (isrc2 = original_isrc and isrc1 = mapped_isrc)
		WHERE s.original_isrc is NULL;

	

	TRUNCATE music.isrc_possible_dupes;
	
	INSERT INTO music.isrc_possible_dupes
	SELECT
		isrc1,
		isrc2,
		((track_score * 4 + artist_score + album_score) * (LEAST(duration1, duration2)::NUMERIC / greatest(duration1, duration2)::NUMERIC))::NUMERIC(3,2) as match_score,
		track_score::NUMERIC(3,2) as track_score,
		artist_score::NUMERIC(3,2) as artist_score,
		album_score::NUMERIC(3,2) as album_score
	FROM (
		SELECT
		isrc1,
		isrc2,
		track1,
		track2,
		similarity(track1, track2) as track_score,
		artist1,
		artist2,
		CASE WHEN artist_id1 = artist_id2 THEN 2
			ELSE similarity(artist1, artist2)
			END as artist_score,
		album1,
		album2,
		CASE WHEN album_id1 = album_id2 THEN 2
			ELSE similarity(album1, album2)
			END as album_score,
		duration1,
		duration2
		FROM staging.isrc_dupe_pairs
		) SUB;

	INSERT INTO music.isrc_swaps
	(original_isrc,
	mapped_isrc,
	mapping_time_utc)
	SELECT 
	isrc1 as original_isrc,
	preferred_isrc as mapped_isrc,
	CURRENT_TIMESTAMP as mapping_time_utc
	FROM music.vw_isrc_dupe_review
	WHERE track_score >=1
		ANd artist_score >=0.95
		and album_score >=0.95
		and duration_score >= 0.95
	ON CONFLICT (original_isrc)
	DO UPDATE SET
	mapped_isrc = EXCLUDED.mapped_isrc,
	mapping_time_utc = CURRENT_TIMESTAMP;

	INSERT INTO music.isrc_non_swaps
	(original_isrc,
	mapped_isrc,
	mapping_time_utc)
	SELECT DISTINCT
	isrc1 as original_isrc,
	preferred_isrc as mapped_isrc,
	CURRENT_TIMESTAMP as mapping_time_utc
	FROM music.vw_isrc_dupe_review
	WHERE duration_score <0.99
		ANd artist_score <0.25
		and album_score <0.25
	ORDER BY original_isrc

	ON CONFLICT (original_isrc, mapped_isrc)
	DO UPDATE SET
	mapped_isrc = EXCLUDED.mapped_isrc,
	mapping_time_utc = CURRENT_TIMESTAMP;

	INSERT INTO logging.music_db_info
	(track_id_count,
	isrc_id_count,
	mapped_isrc_id_count,
	artist_id_count,
	album_id_count)
	SELECT 
	COUNT(DISTINCT track_id) as track_id_count,
	COUNT(DISTINCT track_isrc) as isrc_id_count,
	COUNT(DISTINCT COALESCE(mapped_isrc, track_isrc)) as preferred_isrc_count,
	COUNT(DISTINCT artist_id) as artist_id_count,
	COUNT(DISTINCT album_id) as album_id_count
	FROM music.all_tracks at1
		LEFT JOIN music.isrc_swaps s on at1.track_isrc = s.original_isrc;

END;
$$;


--
-- Name: c2f(numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.c2f(p_celsius numeric) RETURNS numeric
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
SELECT ROUND((p_celsius * 9.0 / 5.0) + 32.0,1);
$$;


--
-- Name: calculate_next_execution_utc(timestamp with time zone, text, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.calculate_next_execution_utc(p_last_success timestamp with time zone, p_task_frequency text, p_task_interval numeric) RETURNS timestamp with time zone
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_next_execution TIMESTAMPTZ;
	v_next_execution_timing TIMESTAMPTZ;
    v_now_utc TIMESTAMPTZ := NOW();
BEGIN

    CASE p_task_frequency
        WHEN 'Hourly' THEN
            -- Calculate when it should execute based on last success
            v_next_execution := p_last_success + (p_task_interval*60 || ' minutes')::INTERVAL;
            
            -- If that time is in the past, it's a missed execution - return it anyway
            -- Don't advance to the next interval
           
        WHEN 'Daily' THEN
            -- Calculate 24 hours from the prior success
            v_next_execution_timing := p_last_success + (24 || ' hours')::INTERVAL;
			-- Calculate the next scheduled occurrence based on p_task_interval as hour of day
			v_next_execution := DATE_TRUNC('day', now()) + (p_task_interval*60 || ' minutes')::INTERVAL;

			IF p_last_success > v_next_execution THEN
				v_next_execution := v_next_execution + (1 || ' day')::INTERVAL;
			END IF;
            
            -- Execute whichever one is first.
            IF v_next_execution_timing <= v_next_execution THEN
                v_next_execution := v_next_execution_timing;
            END IF;
            
            -- Return this time even if it's in the past (missed execution)
            
        WHEN 'Weekly' THEN
            -- p_task_interval represents day of week (0=Sunday, 1=Monday, ..., 6=Saturday) in UTC
            BEGIN
                -- Calculate 7 days from last success
				v_next_execution_timing := p_last_success + (7 || ' days')::INTERVAL;
				-- If today is the specified day of week, set the execution at 2 am of today
				IF p_task_interval = EXTRACT(DOW FROM now()) THEN
					v_next_execution = DATE_TRUNC('day', now()) + (2 || ' hours')::INTERVAL;
				ELSE
				--Otherwise set the next execution at 7 days from today
					v_next_execution = v_next_execution_timing;
				END IF;

				IF p_last_success > v_next_execution THEN
					v_next_execution := v_next_execution + (7 || ' days')::INTERVAL;
				END IF;
                
                -- Return whichever value is first.
                IF v_next_execution_timing < v_next_execution THEN
                    v_next_execution = v_next_execution_timing;
                END IF;
                

            END;
            
            -- Return this time even if it's in the past
            
        WHEN 'Monthly' THEN
			-- Calculate 30 days from last success
			v_next_execution_timing := p_last_success + (30 || ' days')::INTERVAL;
            -- p_task_interval represents day of month (1-31) at 2am UTC
            -- Calcualte 2 am of that day for this month.
            v_next_execution := DATE_TRUNC('month', now()) + (p_task_interval || ' days')::INTERVAL + INTERVAL '2 hours';

			IF p_last_success > v_next_execution THEN
				v_next_execution := v_next_execution + (1 || ' month')::INTERVAL;
			END IF;
            
            -- Set the time to the minimum schedule
            IF v_next_execution_timing <= v_next_execution THEN
                v_next_execution := v_next_execution_timing;
            END IF;
                        
        ELSE
            RETURN NULL;
    END CASE;
    RETURN v_next_execution;
END;
$$;


--
-- Name: clean_album_display(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_album_display(album_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
DECLARE
    album text;
    original_album text;
    suffixes text[] := ARRAY['Radio Edit', 'Studio Rerecord', 'Studio Rerecording', 'Re-recording', 'Radio Mix', 'UK Radio Version', 'Single Version', 'Edit', '7" Edit', 'Original Version', 'Bonus Track', 'Excerpt'];
    suf text;
    escaped_suf text;
    prefix_match text[];
BEGIN
    original_album := album_name;
    album := trim(album_name);

    -- 1. REMASTERED (Improved to catch wrapping parentheses)
    -- This pattern specifically targets "(Remastered)", "[Remastered]", or "- Remastered"
    album := regexp_replace(album, '[\(\[\{]\s*((19|20)[0-9]{2}\s*)?Remaster(ed)?(\s*(19|20)[0-9]{2})?\s*[\)\]\}]', '', 'gi');
    
    -- This handles the standalone or trailing "- Remastered" versions
    album := regexp_replace(album, '[\s–—-]*(?:(?:19|20)[0-9]{2}\s*)?Remaster(ed)?(?:\s*(?:19|20)[0-9]{2})?[\s]*$', '', 'gi');
    
    album := trim(album);

    -- 2. SUFFIXES (Improved to catch wrapping parentheses)
    FOREACH suf IN ARRAY suffixes LOOP
        escaped_suf := regexp_replace(suf, '([^a-zA-Z0-9\s])', '\\\1', 'g');
        
        -- If suffix is inside parens like (Radio Edit), remove the whole thing
        IF album ~* ('[\(\[\{]\s*' || escaped_suf || '\s*[\)\]\}]') THEN
             album := regexp_replace(album, '[\(\[\{]\s*' || escaped_suf || '\s*[\)\]\}]', '', 'gi');
        -- Otherwise handle the dash/trailing version
        ELSIF album ~* ('[–—\s-]?\(?' || escaped_suf || '\)?\s*$') THEN
            prefix_match := regexp_match(album, '[–—-]\s*([A-Za-z0-9 ]+)\s+' || escaped_suf, 'i');
            IF prefix_match IS NOT NULL AND array_length(regexp_split_to_array(trim(prefix_match[1]), '\s+'), 1) > 1 THEN
                CONTINUE;
            END IF;
            album := regexp_replace(album, '[–—\s-]?\(?' || escaped_suf || '\)?\s*$', '', 'i');
        END IF;
        album := trim(album);
    END LOOP;

    -- 3. STANDARDIZE MIX/EDIT
    IF album ~* '\s*[–—-]\s*([A-Za-z0-9 ,&''\.]+?(?:Mix|Remix|Edit))$' THEN
        album := regexp_replace(album, '\s*[–—-]\s*([A-Za-z0-9 ,&''\.]+?(?:Mix|Remix|Edit))$', ' (\1)', 'i');
    END IF;

    -- 4. CLEANUP (Aggressive dangling punctuation removal)
    album := regexp_replace(album, '[–—-]+\s*$', '', 'g');       -- Trailing dashes
    album := regexp_replace(album, '\(\s*\)|\[\s*\]|\{\s*\}', '', 'g'); -- Empty parens/brackets
    album := regexp_replace(album, '\s*-\s*\)', ')', 'g');      -- Dash before closing paren
    album := regexp_replace(album, '[\(\[\{]\s*$', '', 'g');     -- Dangling open paren at end
    album := regexp_replace(album, '[\/;]+\s*$', '', 'g');       -- Slashes at end
    album := regexp_replace(album, '\s{2,}', ' ', 'g');          -- Double spaces
    album := trim(album);

    -- 5. RECOVERY
    RETURN CASE WHEN length(album) <= 3 THEN trim(original_album) ELSE album END;
END;
$_$;


--
-- Name: clean_album_match(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_album_match(album_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN regexp_replace(lower(clean_album_display(album_name)), '[\W_]+', '', 'g');
END;
$$;


--
-- Name: clean_artist_display(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_artist_display(track_name text, artist_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    track text := trim(track_name);
    artist text := trim(artist_name);
    feat_match text[];
    featured text;
BEGIN
    -- Capture featured artist from the track string
    feat_match := regexp_match(track, '[\(\[\{]?\s*(feat\.|featuring|ft\.)\s*([^\)\]\}]+)[\)\]\}]?', 'i');
    
    IF feat_match IS NOT NULL THEN
        featured := trim(feat_match[2]);
        -- Prevent "Artist ft. Artist"
        IF position(lower(featured) in lower(artist)) = 0 THEN
            artist := artist || ' ft. ' || featured;
        END IF;
    END IF;

    RETURN artist;
END;
$$;


--
-- Name: clean_artist_match(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_artist_match(artist_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN regexp_replace(lower(trim(artist_name)), '[\W_]+', '', 'g');
END;
$$;


--
-- Name: clean_track_display(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_track_display(track_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
DECLARE
    track text;
    original_track text;
    -- Suffixes to remove
    suffixes text[] := ARRAY[
        'Radio Edit', 'Studio Rerecord', 'Studio Rerecording', 
        'Re-recording', 'Radio Mix', 'UK Radio Version', 'Single Version', 
        'Edit', '7" Edit', 'Original Version', 'Bonus Track', 'Excerpt'
    ];
    suf text;
    escaped_suf text;
    prefix_match text[];
BEGIN
    original_track := track_name;
    track := trim(track_name);

    -- 1. FEATURED ARTISTS
    track := regexp_replace(track, '[\(\[\{]?\s*(feat\.|featuring|ft\.)\s*([^\)\]\}]+)[\)\]\}]?', '', 'gi');
    -- Note: In trim(), the characters are a set, not a regex range, so order doesn't matter here.
    track := trim(track, ' -–—');

    -- 2. LIVE
    -- Regex: (?:^|\s) ensures we match start of string or a space, avoiding partial word matches
    IF track ~* '(?:^|\s)(?:-|\()\s*live\s*(at .*)?\)?$' THEN
        track := regexp_replace(track, '(?:^|\s)(?:-|\()\s*live\s*(at .*)?\)?$', '', 'gi');
        track := trim(track) || ' (Live)';
    END IF;

    -- 3. REMASTERED
    -- FIX: Moved the standard dash '-' to the END of the brackets: [\s–—-]
    track := regexp_replace(track, '[\s–—-]*(?:(?:19|20)[0-9]{2}\s*)?Remaster(ed)?(?:\s*(?:19|20)[0-9]{2})?[\s\)]*$', '', 'gi');
    track := regexp_replace(track, '[\s–—-]*(?:(?:19|20)[0-9]{2}\s*)?Remaster(ed)?(?:\s*(?:19|20)[0-9]{2})?', '', 'gi');
    track := trim(track);

    -- 4. REMOVE UNWANTED SUFFIXES
    FOREACH suf IN ARRAY suffixes LOOP
        -- Escape special regex characters in the suffix
        escaped_suf := regexp_replace(suf, '([^a-zA-Z0-9\s])', '\\\1', 'g');
        
        -- Check if suffix exists at end
        -- FIX: Moved dash to end in pattern construction
        IF track ~* ('[–—\s-]?\(?' || escaped_suf || '\)?\s*$') THEN
            
            -- Detect preceding word count (The "Fatboy Slim" check)
            prefix_match := regexp_match(track, '[–—-]\s*([A-Za-z0-9 ]+)\s+' || escaped_suf, 'i');
            
            -- If match found AND word count > 1, SKIP removal
            IF prefix_match IS NOT NULL AND array_length(regexp_split_to_array(trim(prefix_match[1]), '\s+'), 1) > 1 THEN
                CONTINUE;
            END IF;

            -- Otherwise, Remove
            track := regexp_replace(track, '[–—\s-]?\(?' || escaped_suf || '\)?\s*$', '', 'i');
            track := trim(track);
        END IF;
    END LOOP;

    -- 5. STANDARDIZE MIX/EDIT
    -- FIX: Dash moved to end in character class [–—-]
    IF track ~* '\s*[–—-]\s*([A-Za-z0-9 ,&''\.]+?(?:Mix|Remix|Edit))$' THEN
        track := regexp_replace(track, '\s*[–—-]\s*([A-Za-z0-9 ,&''\.]+?(?:Mix|Remix|Edit))$', ' (\1)', 'i');
    END IF;

    -- 6. CLEANUP RESIDUAL PUNCTUATION
    -- FIX: Dash moved to end
    track := regexp_replace(track, '[–—-]+\s*$', '', 'g'); -- trailing dashes
    track := regexp_replace(track, '\(\s*\)', '', 'g');     -- empty parens
    track := regexp_replace(track, '\s*-\s*\)', ')', 'g');  -- dash before closing paren
    track := regexp_replace(track, '[\(\[\{]\s*$', '', 'g'); -- open bracket at end
    track := regexp_replace(track, '[\/;]+\s*$', '', 'g');   -- slashes at end
    track := regexp_replace(track, '\s{2,}', ' ', 'g');      -- double spaces
    track := regexp_replace(track, '\[\s*\]', '', 'g');      -- empty brackets
    track := trim(track);

    -- 7. EMPTY / TOO SHORT RECOVERY
    IF length(track) <= 2 THEN
        track := trim(original_track);
    END IF;

    RETURN track;
END;
$_$;


--
-- Name: clean_track_match(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.clean_track_match(track_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    -- Call Function 1, then strip all non-alphanumeric
    RETURN regexp_replace(lower(clean_track_display(track_name)), '[\W_]+', '', 'g');
END;
$$;


--
-- Name: flatten_activity_details(integer, text); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.flatten_activity_details(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    rec RECORD;
    v_count INT :=0;
    v_start_point INT :=0;
    v_end_point INT;
BEGIN

    CALL staging.flatten_activity_metrics(v_task_id, v_friendly_name);
    -- CALL staging.flatten_activity_metrics(19, 'Activity Details');

    INSERT INTO activities.activity_details (
        activity_id,
        activity_type_id,
        ts_utc,
        moving_duration_s,
        elapsed_duration_s,
        duration_s,
        distance_mm,
        latitude,
        longitude,
        heartrate_bpm,
        cadence_spm,
        air_temp_c,
        elevation_m,
        speed_mmps,
        vertical_speed_mps,
        vert_oscillation_cm,
        stride_length_cm,
        vert_ratio,
        ground_contact_time_ms,
        ground_contact_balance_left,
        performance_condition,
        loc_point,
        elevation_m_smooth)
    SELECT
        activity_id,
        activity_type_id,
        ts_utc,
        moving_duration_s,
        elapsed_duration_s,
        duration_s,
        distance_mm,
        latitude,
        longitude,
        heartrate_bpm,
        cadence_spm,
        air_temp_c,
        elevation_m,
        speed_mmps,
        vertical_speed_mps,
        vert_oscillation_cm,
        stride_length_cm,
        vert_ratio,
        ground_contact_time_ms,
        ground_contact_balance_left,
        performance_condition,
        loc_point,
        elevation_smooth_m
    FROM staging.vw_activity_flatten
    WHERE latitude IS NOT NULL and longitude IS NOT NULL
    ON CONFLICT(ts_utc)
        DO UPDATE SET
                      activity_type_id = EXCLUDED.activity_type_id,
                      moving_duration_s = EXCLUDED.moving_duration_s,
                      elapsed_duration_s = EXCLUDED.elapsed_duration_s,
                      -- ts_utc = EXCLUDED.ts_utc,
                      duration_s = EXCLUDED.duration_s,
                      distance_mm = EXCLUDED.distance_mm,
                      latitude = EXCLUDED.latitude,
                      longitude = EXCLUDED.longitude,
                      heartrate_bpm = EXCLUDED.heartrate_bpm,
                      cadence_spm = EXCLUDED.cadence_spm,
                      air_temp_c = EXCLUDED.air_temp_c,
                      elevation_m = EXCLUDED.elevation_m,
                      speed_mmps = EXCLUDED.speed_mmps,
                      vertical_speed_mps = EXCLUDED.vertical_speed_mps,
                      vert_oscillation_cm = EXCLUDED.vert_oscillation_cm,
                      stride_length_cm = EXCLUDED.stride_length_cm,
                      vert_ratio = EXCLUDED.vert_ratio,
                      ground_contact_time_ms = EXCLUDED.ground_contact_time_ms,
                      ground_contact_balance_left = EXCLUDED.ground_contact_balance_left,
                      performance_condition = EXCLUDED.performance_condition;


    UPDATE activities.activities
    SET activity_path = sub.path, is_downloaded=TRUE
    FROM (
             SELECT
                 rd.activity_id,
                 ST_SetSRID(
                         ST_MakeLine(
                                 ST_MakePoint(longitude, latitude, elevation_m_smooth, elapsed_duration_s)
                                 ORDER BY elapsed_duration_s ASC
                         ),
                         4326
                 ) AS path
             FROM activities.activity_details rd
                      INNER JOIN staging.activity_payload ap on ap.activity_id = rd.activity_id
             GROUP BY rd.activity_id
         ) AS sub
    WHERE activities.activity_id = sub.activity_id;

    FOR rec IN
        SELECT DISTINCT activity_id
        FROM staging.activity_payload
        LOOP
            -- Call your existing matching function for the current ID
            CALL activities.smooth_elevation_spikes(rec.activity_id);
            SELECT MAX(elapsed_duration_s) FROM activities.activity_details where activity_id = rec.activity_id into v_end_point;
            CALL staging.match_activity_to_segment(rec.activity_id, 0, v_end_point, TRUE);

            INSERT INTO health.heartrate_raw
            (ts_utc,
             heartrate_bpm)
            SELECT ts_utc, heartrate_bpm from activities.activity_details WHERE activity_id = rec.activity_id
            ON CONFLICT(ts_utc) DO UPDATE SET
                heartrate_bpm = EXCLUDED.heartrate_bpm
            where health.heartrate_raw.heartrate_bpm is DISTINCT FROM EXCLUDED.heartrate_bpm;
            v_count := v_count + 1;
        END LOOP;

    CALL activities.refresh_overlaps();
    CALL activities.repath_segment_matches();
    CALL activities.repath_segments();

    UPDATE api_services.function_library
    SET value_recency = CURRENT_TIMESTAMP  --############UPDATE TABLE NAME
    WHERE friendly_name = v_friendly_name;

    CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports where task_id = v_task_id;

END;
$$;


--
-- Name: g2lb(numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.g2lb(m numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$

BEGIN
   RETURN ROUND(m*0.00220462,1);
END;
$$;


--
-- Name: m2mi(numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.m2mi(m numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$

BEGIN
   RETURN ROUND(m*0.000621371,1);
END;
$$;


--
-- Name: match_activity_to_course(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.match_activity_to_course(p_activity_id bigint) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_activity_geom GEOMETRY;
    v_start_pt GEOMETRY;
    v_end_pt GEOMETRY;
    v_length FLOAT;
    v_matched_course_id INT;
    v_new_course_id INT;
    
    -- Tuning Knobs
    DIST_THRESHOLD FLOAT := 20.0;     -- Meters (Start/End proximity)
    LENGTH_TOLERANCE FLOAT := 20;   -- 10% difference allowed
    MATCH_THRESHOLD FLOAT := 20.0;    -- Max Frechet Distance (meters)
BEGIN
    -- 1. Extract activity geometry and metadata
    SELECT 
        ST_Force2D(trajectory), 
        ST_StartPoint(ST_Force2D(trajectory)),
        ST_EndPoint(ST_Force2D(trajectory)),
        ST_Length(trajectory, true) 
    INTO v_activity_geom, v_start_pt, v_end_pt, v_length
    FROM activities.activities 
    WHERE activity_id = p_activity_id;

    -- 2. Find the best match among existing courses
    SELECT course_id INTO v_matched_course_id
    FROM activities.courses
    WHERE 
        -- Coarse spatial filter (Uses GiST Indexes)
        ST_DWithin(ST_StartPoint(course_path), v_start_pt, DIST_THRESHOLD)
        AND ST_DWithin(ST_EndPoint(course_path), v_end_pt, DIST_THRESHOLD)
        -- Length filter
        AND ABS(ST_Length(course_path, true) - v_length) < LENGTH_TOLERANCE
        -- Final Shape check
        AND ST_FrechetDistance(course_path, v_activity_geom) < MATCH_THRESHOLD
    ORDER BY 
        ST_FrechetDistance(course_path, v_activity_geom) ASC
    LIMIT 1;

    -- 3. Decision Branch
    IF v_matched_course_id IS NOT NULL THEN
        -- OPTION 2b: Assign best match found
        UPDATE activities.activities 
        SET course_id = v_matched_course_id 
        WHERE activity_id = p_activity_id;
        
        RAISE NOTICE 'Activity % matched to existing Course %', p_activity_id, v_matched_course_id;
        
    ELSE
        -- OPTION 2a: Create and assign a new course
        INSERT INTO activities.courses (course_name, course_path)
        VALUES (
            'Course Created ' || to_char(now(), 'YYYY-MM-DD HH24:MI'), 
            v_activity_geom
        )
        RETURNING course_id INTO v_new_course_id;
        
        UPDATE activities.activities 
        SET course_id = v_new_course_id 
        WHERE activity_id = p_activity_id;
        
        RAISE NOTICE 'No match found. Activity % assigned to new Course %', p_activity_id, v_new_course_id;
    END IF;
END;
$$;


--
-- Name: match_confidence(public.geometry, public.geometry); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.match_confidence(p_path_a public.geometry, p_path_b public.geometry) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_penalty NUMERIC := 0;
    v_len_a NUMERIC;
    v_len_b NUMERIC;
    v_diff NUMERIC;
BEGIN
    -- 1. Perfect Match Shortcut
    IF p_path_a = p_path_b THEN RETURN 100.0; END IF;
    
    -- 2. Length Divergence (Fastest Check)
    v_len_a := ST_Length(p_path_a, true);
    v_len_b := ST_Length(p_path_b, true);
    
    -- Gate: > 20% length difference is an auto-fail
    -- (Using percentage is safer than raw meters for short vs long segments)
    IF ABS(v_len_a - v_len_b) > (v_len_b * 0.20) THEN RETURN -9999.0; END IF;

    -- Penalty: 1 point for every 1% difference
    v_penalty := v_penalty + (ABS(v_len_a - v_len_b) / NULLIF(v_len_b, 1) * 100);

    -- 3. Start Point Divergence
    v_diff := ST_Distance(ST_StartPoint(p_path_a), ST_StartPoint(p_path_b), true);
    IF v_diff > 100 THEN RETURN -9999.0; END IF;
    -- Penalty: 1 point per 10 meters off
    v_penalty := v_penalty + (v_diff / 10.0);

    -- 4. End Point Divergence
    v_diff := ST_Distance(ST_EndPoint(p_path_a), ST_EndPoint(p_path_b), true);
    IF v_diff > 100 THEN RETURN -9999.0; END IF;
    -- Penalty: 1 point per 10 meters off
    v_penalty := v_penalty + (v_diff / 10.0);

    -- 5. Ruggedness (Vertical Component)
    -- Using NULLIF to prevent zero errors, though unlikely here
    v_diff := ABS(
        (ST_3DLength(p_path_a) - ST_Length(p_path_a)) - 
        (ST_3DLength(p_path_b) - ST_Length(p_path_b))
    );
    
    -- Penalty: 1 point per 5 meters of vertical mismatch
    -- This scales it down so it doesn't dominate the score
    v_penalty := v_penalty + (v_diff / 50.0);

    -- 7. Frechet Distance (Most Expensive)
    -- Transform to 3857 for meters
    v_diff := ST_FrechetDistance(ST_Transform(p_path_a, 3857), ST_Transform(p_path_b, 3857));

    IF v_diff > 500 THEN RETURN -9999.0; END IF; -- Strict shape matching
    
    -- Penalty: 1 point per meter of shape deviation
    v_penalty := v_penalty + v_diff;

	-- IF v_penalty >= 1000 THEN RETURN -9999.0; END IF;
		

    -- Final Score Calculation
    RETURN ROUND(100.0 - v_penalty,2)::NUMERIC(6,2);
END;
$$;


--
-- Name: median_discrete(anyelement); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.median_discrete(anyelement) RETURNS double precision
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY $1)
$_$;


--
-- Name: pace_as_text(numeric, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pace_as_text(p_distance_miles numeric, p_time_seconds numeric) RETURNS text
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$

DECLARE
    pace_seconds NUMERIC;
    minutes_part integer;
    seconds_part NUMERIC;
BEGIN
    IF p_distance_miles IS NULL OR p_distance_miles = 0 THEN
        RETURN NULL;
    END IF;

    pace_seconds := p_time_seconds / p_distance_miles;

	if pace_seconds < 0 THEN
		minutes_part := ceiling(pace_seconds / 60);
		seconds_part := abs(pace_seconds) - (abs(minutes_part) * 60);
	ELSE
    	minutes_part := floor(pace_seconds / 60);
		seconds_part := pace_seconds - (minutes_part * 60);
	END IF;
    

    RETURN minutes_part::text || ':' ||
           lpad(to_char(seconds_part, 'FM00.00'), 5, '0');
END;
$$;


--
-- Name: requires_catchup(timestamp with time zone, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.requires_catchup(p_current_through timestamp with time zone, p_task_frequency text, p_loop_type text) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
SELECT 
    p_loop_type in ('Day', 'Range')
	AND p_task_frequency != 'Retired'
	AND EXTRACT(EPOCH FROM (utc_now() - p_current_through)) / 3600 > 24
$$;


--
-- Name: s2str(integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.s2str(p_seconds integer) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_negative  BOOLEAN;
    v_abs       INTEGER;
    v_hours     INTEGER;
    v_minutes   INTEGER;
    v_secs      INTEGER;
BEGIN
    v_negative := p_seconds < 0;
    v_abs      := ABS(p_seconds);
    v_hours    := v_abs / 3600;
    v_minutes  := (v_abs % 3600) / 60;
    v_secs     := v_abs % 60;

    RETURN CASE WHEN v_negative THEN '-' ELSE '' END
        || v_hours::TEXT
        || ':'
        || LPAD(v_minutes::TEXT, 2, '0')
        || ':'
        || LPAD(v_secs::TEXT, 2, '0');
END;
$$;


--
-- Name: salvage_table(text, text, boolean); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.salvage_table(IN p_schema text, IN p_table text, IN p_reload boolean DEFAULT false)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_src     text := format('%I.%I', p_schema, p_table);
    v_dst     text := format('%I.%I', p_schema, p_table || '_salvage');
    v_cols    text;
    v_fks     text;
    v_salv    bigint;
    v_back    bigint;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'Requires a superuser connection (zero_damaged_pages)';
    END IF;
    IF to_regclass(v_src) IS NULL THEN
        RAISE EXCEPTION 'Source table % does not exist', v_src;
    END IF;

    -- Non-generated columns only (generated columns can't be inserted into)
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
    INTO v_cols
    FROM pg_attribute
    WHERE attrelid = v_src::regclass AND attnum > 0
      AND NOT attisdropped AND attgenerated = '';

    IF NOT p_reload THEN
        -- PHASE 1: copy readable rows into <table>_salvage.
        IF to_regclass(v_dst) IS NOT NULL THEN
            RAISE EXCEPTION '% already exists; drop or rename it first', v_dst;
        END IF;

        EXECUTE format('CREATE TABLE %s AS SELECT %s FROM %s WITH NO DATA',
                       v_dst, v_cols, v_src);

        PERFORM set_config('zero_damaged_pages', 'on', true);
        PERFORM set_config('max_parallel_workers_per_gather', '0', true);

        EXECUTE format('INSERT INTO %s SELECT %s FROM %s', v_dst, v_cols, v_src);
        GET DIAGNOSTICS v_salv = ROW_COUNT;

        RAISE NOTICE 'Phase 1 done: % rows salvaged into %. Inspect it, then CALL with p_reload => true.',
            v_salv, v_dst;
    ELSE
        -- PHASE 2: TRUNCATE original and reload from the salvage table.
        IF to_regclass(v_dst) IS NULL THEN
            RAISE EXCEPTION 'Run phase 1 first: % does not exist', v_dst;
        END IF;

        SELECT string_agg(conrelid::regclass::text || ' (' || conname || ')', ', ')
        INTO v_fks
        FROM pg_constraint
        WHERE contype = 'f' AND confrelid = v_src::regclass;
        IF v_fks IS NOT NULL THEN
            RAISE EXCEPTION 'Foreign keys reference %: %. Drop/recreate them manually first.',
                v_src, v_fks;
        END IF;

        EXECUTE format('SELECT count(*) FROM %s', v_dst) INTO v_salv;

        EXECUTE format('TRUNCATE TABLE %s', v_src);
        EXECUTE format('INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM %s',
                       v_src, v_cols, v_cols, v_dst);

        EXECUTE format('SELECT count(*) FROM %s', v_src) INTO v_back;
        IF v_back <> v_salv THEN
            RAISE EXCEPTION 'Row count mismatch (salvage %, reloaded %); rolling back',
                v_salv, v_back;
        END IF;

        RAISE NOTICE 'Phase 2 done: % reloaded with % rows. % kept until you drop it.',
            v_src, v_back, v_dst;
    END IF;
END;
$$;


--
-- Name: time_ago(timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.time_ago(p_timestamp timestamp with time zone) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_total_seconds BIGINT;
    v_minutes INTEGER;
    v_hours INTEGER;
    v_days INTEGER;
    v_months INTEGER;
BEGIN
    -- Handle NULL
    IF p_timestamp IS NULL THEN
        RETURN '';
    END IF;
    
    -- Calculate total seconds difference
    v_total_seconds := EXTRACT(EPOCH FROM (v_now - p_timestamp))::BIGINT;
    
    -- Handle future dates (if negative)
    IF v_total_seconds < 0 THEN
        v_total_seconds := ABS(v_total_seconds);
    END IF;
    
    -- Return appropriate format based on time difference
    IF v_total_seconds < 60 THEN
        -- Less than 60 seconds
        RETURN v_total_seconds || 's';
        
    ELSIF v_total_seconds < (120 * 60) THEN
        -- Less than 120 minutes
        v_minutes := v_total_seconds / 60;
        RETURN v_minutes || 'm';
        
    ELSIF v_total_seconds < (48 * 3600) THEN
        -- Less than 48 hours
        v_hours := v_total_seconds / 3600;
        RETURN v_hours || 'h';
        
    ELSIF v_total_seconds < (30 * 86400) THEN
        -- Less than 30 days
        v_days := v_total_seconds / 86400;
        RETURN v_days || 'd';
        
    ELSE
        -- 30+ days
        v_months := v_total_seconds / (30 * 86400);
        RETURN v_months || 'mo';
    END IF;
END;
$$;


--
-- Name: utc_now(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.utc_now() RETURNS timestamp with time zone
    LANGUAGE sql STABLE
    AS $$
    SELECT (NOW() AT TIME ZONE 'UTC')
$$;


--
-- Name: activities_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.activities_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO activities.activities (  --############UPDATE TABLE NAME
        activity_id,
        activity_name,
        activity_type_id,
        activity_type_name,
        distance_m,
        duration_s,
        duration_elapsed_s,
        duration_moving_s,
        calories,
        calories_bmr,
        steps,
        stride_length_cm_avg,
        cadence_double_spm_max,
        cadence_running_spm_max,
        cadence_running_spm_avg,
        speed_mps_max,
        speed_mps_avg,
        heartrate_max,
        heartrate_avg,
        start_timestamp_utc,
        start_time_utc,
        end_time_utc,
        latitude_start,
        latitude_end,
        longitude_start,
        longitude_end,
        elevation_m_min,
        elevation_m_max,
        elevation_m_gain,
        elevation_m_loss,
        temperature_c_min,
        temperature_c_max,
        water_ml_estimated,
        training_load,
        training_effect_aerobic,
        training_effect_anerobic,
        intensity_minutes_moderate,
        intensity_minutes_vigorous
    )
    SELECT
        activity_id,
        activity_name,
        activity_type_id,
        activity_type_name,
        distance_m,
        duration_s,
        duration_elapsed_s,
        duration_moving_s,
        calories,
        calories_bmr,
        steps,
        stride_length_cm_avg,
        cadence_double_spm_max,
        cadence_running_spm_max,
        cadence_running_spm_avg,
        speed_mps_max,
        speed_mps_avg,
        heartrate_max,
        heartrate_avg,
        start_timestamp_utc,
        start_time_utc,
        end_time_utc,
        latitude_start,
        latitude_end,
        longitude_start,
        longitude_end,
        elevation_m_min,
        elevation_m_max,
        elevation_m_gain,
        elevation_m_loss,
        temperature_c_min,
        temperature_c_max,
        water_ml_estimated,
        training_load,
        training_effect_aerobic,
        training_effect_anerobic,
        intensity_minutes_moderate,
        intensity_minutes_vigorous
    FROM (
             SELECT
                 (payload->>'activityId')::BIGINT AS activity_id,
                 (payload->>'activityName') AS activity_name,
                 (payload->'activityType'->>'typeId')::NUMERIC::SMALLINT AS activity_type_id,
                 (payload->'activityType'->>'typeKey') AS activity_type_name,
                 (payload->>'distance')::NUMERIC(8,2) AS distance_m,
                 (payload->>'duration')::NUMERIC(8,2) AS duration_s,
                 (payload->>'elapsedDuration')::NUMERIC(8,2) AS duration_elapsed_s,
                 (payload->>'movingDuration')::NUMERIC::INTEGER AS duration_moving_s,
                 (payload->>'calories')::NUMERIC::INTEGER AS calories,
                 (payload->>'bmrCalories')::NUMERIC::INTEGER AS calories_bmr,
                 (payload->>'steps')::NUMERIC::INTEGER AS steps,
                 (payload->>'avgStrideLength')::NUMERIC::INTEGER AS stride_length_cm_avg,
                 (payload->>'maxDoubleCadence')::NUMERIC::INTEGER AS cadence_double_spm_max,
                 (payload->>'maxRunningCadenceInStepsPerMinute')::NUMERIC::INTEGER AS cadence_running_spm_max,
                 (payload->>'averageRunningCadenceInStepsPerMinute')::NUMERIC(4,1) AS cadence_running_spm_avg,
                 (payload->>'maxSpeed')::NUMERIC(5,2) AS speed_mps_max,
                 (payload->>'averageSpeed')::NUMERIC(5,2) AS speed_mps_avg,
                 (payload->>'maxHR')::NUMERIC::INTEGER AS heartrate_max,
                 (payload->>'averageHR')::NUMERIC::INTEGER AS heartrate_avg,
                 (payload->>'beginTimestamp')::BIGINT AS start_timestamp_utc,
                 ((payload->>'startTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ AS start_time_utc,
                 ((payload->>'endTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ AS end_time_utc,
                 (payload->>'startLatitude')::NUMERIC(18,15) AS latitude_start,
                 (payload->>'endLatitude')::NUMERIC(18,15) AS latitude_end,
                 (payload->>'startLongitude')::NUMERIC(18,15) AS longitude_start,
                 (payload->>'endLongitude')::NUMERIC(18,15) AS longitude_end,
                 (payload->>'minElevation')::NUMERIC(10,2) AS elevation_m_min,
                 (payload->>'maxElevation')::NUMERIC(10,2) AS elevation_m_max,
                 (payload->>'elevationGain')::NUMERIC(10,2) AS elevation_m_gain,
                 (payload->>'elevationLoss')::NUMERIC(10,2) AS elevation_m_loss,
                 (payload->>'minTemperature')::NUMERIC::SMALLINT AS temperature_c_min,
                 (payload->>'maxTemperature')::NUMERIC::SMALLINT AS temperature_c_max,
                 (payload->>'waterEstimated')::NUMERIC::INTEGER AS water_ml_estimated,
                 (payload->>'activityTrainingLoad')::NUMERIC(10,2) AS training_load,
                 (payload->>'aerobicTrainingEffect')::NUMERIC(10,2) AS training_effect_aerobic,
                 (payload->>'anaerobicTrainingEffect')::NUMERIC(10,2) AS training_effect_anerobic,
                 (payload->>'moderateIntensityMinutes')::NUMERIC::INTEGER AS intensity_minutes_moderate,
                 (payload->>'vigorousIntensityMinutes')::NUMERIC::INTEGER AS intensity_minutes_vigorous,
                 ROW_NUMBER() OVER (
                     PARTITION BY (payload->>'activityId')::BIGINT
                     ORDER BY ((payload->>'endTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ DESC
                     ) as row_num
             FROM staging.api_imports
             WHERE task_id = v_task_id
               AND (payload->>'activityId')::BIGINT IS NOT NULL
         ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (activity_id)
        DO NOTHING;
    --    DO UPDATE SET
    --        body_battery_value = EXCLUDED.body_battery_value
    -- WHERE health.body_battery_raw.body_battery_value IS DISTINCT FROM EXCLUDED.body_battery_value;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library
    SET value_recency = (SELECT MAX(end_time_utc) FROM activities.activities)  --############UPDATE TABLE NAME
    -- WHERE friendly_name = 'Activities'
    WHERE friendly_name = v_friendly_name;

    UPDATE health.heartrate_raw SET activity_label = 'running'
    FROM activities.activities a
    WHERE health.heartrate_raw.ts_utc between a.start_time_utc and a.end_time_utc and health.heartrate_raw.activity_label != 'running'
      and a.activity_type_name like '%run%';

    UPDATE health.heartrate_raw SET activity_label = 'walking'
    FROM activities.activities a
    WHERE health.heartrate_raw.ts_utc between a.start_time_utc and a.end_time_utc and health.heartrate_raw.activity_label != 'walking'
      and a.activity_type_name like '%walk%';

    UPDATE health.heartrate_raw SET activity_label = 'active'
    FROM activities.activities a
    WHERE health.heartrate_raw.ts_utc between a.start_time_utc and a.end_time_utc
      and a.activity_type_name not like '%run%' and a.activity_type_name not like '%walk%'
      and health.heartrate_raw.activity_label NOT IN ('running','walking', 'active');

    CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: body_battery_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.body_battery_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.body_battery_raw (  --############UPDATE TABLE NAME
        ts_utc,
        body_battery_value
    )
    SELECT
        ts_utc,
        body_battery_value
    FROM (
        SELECT 
            to_timestamp(round((array_element->>0)::bigint/1000.0)) AS ts_utc,
            (array_element->>1)::integer AS body_battery_value,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((array_element->>0)::bigint/1000.0))
                ORDER BY (array_element->>0)::bigint ASC
            ) as row_num
        FROM staging.api_imports,
             jsonb_array_elements(payload->'bodyBatteryValuesArray') AS array_element
        WHERE task_id = v_task_id 
          AND (array_element->>1)::integer IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
    DO UPDATE SET
        body_battery_value = EXCLUDED.body_battery_value
	WHERE health.body_battery_raw.body_battery_value IS DISTINCT FROM EXCLUDED.body_battery_value;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.body_battery_raw)  --############UPDATE TABLE NAME
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: body_composition_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.body_composition_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.body_composition_raw (  --############UPDATE TABLE NAME
        ts_utc,
			weight_total_g,
			weight_bone_g,
			weight_muscle_g,
			fat_pct,
			water_pct,
			body_mass_index
    )
    SELECT
        ts_utc,
		weight_total_g,
		weight_bone_g,
		weight_muscle_g,
		fat_pct,
		water_pct,
		body_mass_index
    FROM (
        SELECT
            to_timestamp(round(GREATEST((elem ->> 'samplePk')::BIGINT, (elem ->> 'date')::BIGINT,(elem ->> 'timestampGMT')::BIGINT)/1000)) as ts_utc,
			(elem ->> 'weight')::NUMERIC::INTEGER AS weight_total_g,
			(elem ->> 'boneMass')::NUMERIC::INTEGER AS weight_bone_g,
			(elem ->> 'muscleMass')::NUMERIC::INTEGER AS weight_muscle_g,
			ROUND((elem ->> 'bodyFat')::NUMERIC/100, 3)::NUMERIC(4,3) AS fat_pct,
			ROUND((elem ->> 'bodyWater')::NUMERIC/100, 3)::NUMERIC(4,3) AS water_pct,
			ROUND((elem ->> 'bmi')::NUMERIC, 2)::NUMERIC(4,2) AS body_mass_index,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round(GREATEST((elem ->> 'samplePk')::BIGINT, (elem ->> 'date')::BIGINT,(elem ->> 'timestampGMT')::BIGINT)/1000))
                ORDER BY ((payload->>'endTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ DESC
            ) as row_num
        FROM staging.api_imports
		CROSS JOIN LATERAL jsonb_array_elements(payload -> 'dateWeightList') AS elem
--         WHERE task_id = v_task_id
        WHERE task_id = 8
          AND (elem ->> 'weight')::NUMERIC::INTEGER IS NOT NULL
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        weight_total_g = EXCLUDED.weight_total_g,
		weight_bone_g = EXCLUDED.weight_bone_g,
		weight_muscle_g = EXCLUDED.weight_muscle_g,
		fat_pct = EXCLUDED.fat_pct,
		water_pct = EXCLUDED.water_pct,
		body_mass_index = EXCLUDED.body_mass_index
	WHERE health.body_composition_raw.weight_total_g IS DISTINCT FROM EXCLUDED.weight_total_g OR
	health.body_composition_raw.weight_bone_g IS DISTINCT FROM EXCLUDED.weight_bone_g OR
	health.body_composition_raw.weight_muscle_g IS DISTINCT FROM EXCLUDED.weight_muscle_g OR
	health.body_composition_raw.fat_pct IS DISTINCT FROM EXCLUDED.fat_pct OR
	health.body_composition_raw.water_pct IS DISTINCT FROM EXCLUDED.water_pct OR
	health.body_composition_raw.body_mass_index IS DISTINCT FROM EXCLUDED.body_mass_index;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library
    SET value_recency = (SELECT MAX(ts_utc) FROM health.body_composition_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Body Comp'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: build_anchors(); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.build_anchors()
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE staging.interpolation_anchors;

    WITH forward_pass AS (
        SELECT
            ts_utc,
            load_value,
            max(ts_utc) FILTER (WHERE load_value IS NOT NULL)
                OVER (ORDER BY ts_utc
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_ts
        FROM staging.interpolation_load_values
    ),
    backward_pass AS (
        SELECT
            ts_utc,
            min(ts_utc) FILTER (WHERE load_value IS NOT NULL)
                OVER (ORDER BY ts_utc
                      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_ts
        FROM staging.interpolation_load_values
    )
    INSERT INTO staging.interpolation_anchors (
        ts_utc,
        load_value,
        prev_value,
        prev_ts,
        next_value,
        next_ts
    )
    SELECT DISTINCT
        b.ts_utc,
        b.load_value,
        pv.load_value AS prev_value,
        f.prev_ts,
        nv.load_value AS next_value,
        bwd.next_ts
    FROM staging.interpolation_load_values b
    LEFT JOIN forward_pass f
        ON f.ts_utc = b.ts_utc
    LEFT JOIN backward_pass bwd
        ON bwd.ts_utc = b.ts_utc
    LEFT JOIN staging.interpolation_load_values pv
        ON pv.ts_utc = f.prev_ts
    LEFT JOIN staging.interpolation_load_values nv
        ON nv.ts_utc = bwd.next_ts;
END;
$$;


--
-- Name: create_segment_from_activity(bigint, integer, integer, boolean, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.create_segment_from_activity(IN v_activity_id bigint, IN v_start_point integer, IN v_end_point integer, IN v_is_course boolean, IN v_segment_name text DEFAULT ('New:'::text || (CURRENT_TIMESTAMP)::date))
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN
	INSERT INTO activities.segments
	(
	activity_reference_id,
	activity_type_id,
	reference_start_point,
	reference_end_point,
	is_course,
	segment_name,
	segment_path,
	distance_m,
	elevation_gain,
	elevation_loss,
	start_point,
	end_point
	)
	SELECT 
		activity_reference_id,
		activity_type_id,
		reference_start_point,
		reference_end_point,
		is_course,
		segment_name,
		segment_path,
		ROUND(ST_Length(segment_path, true)::NUMERIC, 2)::NUMERIC(8,2) as distance_m,
		ROUND(COALESCE(ascent_m,0),2) as ascent_m,
		ROUND(COALESCE(descent_m,0),2) as descent_m,
		ST_StartPoint(segment_path) as start_point,
		ST_EndPoint(segment_path) as end_point
	FROM (
		SELECT 
			a.activity_id as activity_reference_id,
			a.activity_type_id,
			v_start_point as reference_start_point,
			v_end_point as reference_end_point,
			v_is_course as is_course,
			v_segment_name as segment_name,
			ST_SetSRID(
					ST_MakeLine(
						ST_MakePoint(rd.longitude, rd.latitude, rd.elevation_m_smooth, rd.elapsed_duration_s) 
						ORDER BY elapsed_duration_s ASC
					), 
					4326
				) as segment_path
			
			FROM activities.activities a 
				INNER JOIN activities.activity_details rd on rd.activity_id = a.activity_id
					and elapsed_duration_s >= v_start_point and elapsed_duration_s <= v_end_point
			WHERE a.activity_id = v_activity_id
			GROUP BY a.activity_id, a.activity_type_id
		) traj
	INNER JOIN (SELECT activity_id,
			SUM(CASE WHEN elevation_delta>0 then elevation_delta else 0 END) as ascent_m,
			SUM(CASE WHEN elevation_delta<0 then elevation_delta else 0 END) as descent_m
			FROM activities.vw_activity_elevation_deltas
			WHERE elevation_delta !=0 and activity_id = v_activity_id
			AND elapsed_duration_s >= v_start_point and elapsed_duration_s <= v_end_point
			GROUP BY activity_id) deltas on traj.activity_reference_id = deltas.activity_id		
			;

END;
$$;


--
-- Name: derive_activity_elevations(bigint); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.derive_activity_elevations(IN v_actvity_id bigint)
    LANGUAGE plpgsql
    AS $$

BEGIN
    TRUNCATE staging.temp_activity_locations;

    INSERT INTO staging.temp_activity_locations
    (latitude, longitude, loc_point, elevation_m_smooth, grid_5m_00, grid_5m_25_00, grid_5m_00_25, grid_5m_25_25)
    SELECT
        latitude, longitude, loc_point, elevation_m_smooth, grid_5m_00, grid_5m_25_00, grid_5m_00_25, grid_5m_25_25
    FROM activities.activity_details
    WHERE activity_id = v_actvity_id;

    ANALYZE staging.temp_activity_locations;

    TRUNCATE staging.temp_activity_matched_locations;
    INSERT INTO staging.temp_activity_matched_locations
    (latitude, longitude, loc_point, loc_point_matched, elevation_m_smooth)
    SELECT
        s1.latitude,
        s1.longitude,
        s1.loc_point,
        s2.loc_point as loc_point_matched,
        s2.elevation_m_smooth
    FROM staging.temp_activity_locations s1
        INNER JOIN activities.activity_details s2 on s1.grid_5m_00 = s2.grid_5m_00
                            OR s1.grid_5m_25_00 = s2.grid_5m_25_00
                            OR s1.grid_5m_00_25 = s2.grid_5m_00_25
                            OR s1.grid_5m_25_25 = s2.grid_5m_25_25
    ;

    ANALYZE staging.temp_activity_matched_locations;

    TRUNCATE staging.temp_elevation_medians;
    INSERT INTO staging.temp_elevation_medians (loc_point, median_elev)
    SELECT loc_point, percentile_disc(0.5) within group ( order by elevation_m_smooth) as median_elev
    FROM staging.temp_activity_matched_locations
    GROUP BY loc_point;

    analyze staging.temp_elevation_medians;

    UPDATE activities.activity_details
    SET elevation_reference = median_elev
    FROM staging.temp_elevation_medians
    WHERE activities.activity_details.loc_point::geometry = staging.temp_elevation_medians.loc_point::GEOMETRY;

END;
$$;


--
-- Name: fitness_age_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.fitness_age_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.fitness_age_raw (  --############UPDATE TABLE NAME
        ts_utc,
			human_age,
	        fitness_age_measured,
			fitness_age_potential
    )
    SELECT
        ts_utc,
			human_age,
	        fitness_age_measured,
			fitness_age_potential
    FROM (
        SELECT 
            date_trunc('day', (payload->>'lastUpdated')::TIMESTAMPTZ) AS ts_utc,
			ROUND(
				    EXTRACT(YEAR FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) +
				    EXTRACT(MONTH FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) / 12.0 +
				    EXTRACT(DAY FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) / 365.25,
				    2
				)  AS human_age,
	        ROUND((payload->>'fitnessAge')::NUMERIC,2)::NUMERIC(4,2) AS fitness_age_measured,
	        ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) AS fitness_age_potential,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('day', (payload->>'lastUpdated')::TIMESTAMPTZ)
                ORDER BY ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) asc
            ) as row_num
        FROM staging.api_imports
        WHERE task_id = v_task_id 
          AND ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        human_age = EXCLUDED.human_age,
		fitness_age_measured = EXCLUDED.fitness_age_measured,
		fitness_age_potential = EXCLUDED.fitness_age_potential
	WHERE health.fitness_age_raw.human_age IS DISTINCT FROM EXCLUDED.human_age OR
	health.fitness_age_raw.fitness_age_measured IS DISTINCT FROM EXCLUDED.fitness_age_measured OR
	health.fitness_age_raw.fitness_age_potential IS DISTINCT FROM EXCLUDED.fitness_age_potential;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.fitness_age_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Fitness Age'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: flatten_activity_details(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.flatten_activity_details(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$

BEGIN

    CALL staging.flatten_activity_metrics(v_task_id, v_friendly_name);
    -- CALL staging.flatten_activity_metrics(19, 'Activity Details');

    INSERT INTO activities.activity_details (
        activity_id,
        activity_type_id,
        ts_utc,
        moving_duration_s,
        elapsed_duration_s,
        duration_s,
        distance_mm,
        latitude,
        longitude,
        heartrate_bpm,
        cadence_spm,
        air_temp_c,
        elevation_m,
        speed_mmps,
        vertical_speed_mps,
        vert_oscillation_cm,
        stride_length_cm,
        vert_ratio,
        ground_contact_time_ms,
        ground_contact_balance_left,
        performance_condition,
        loc_point,
        elevation_m_smooth)
    SELECT
        activity_id,
        activity_type_id,
        ts_utc,
        moving_duration_s,
        elapsed_duration_s,
        duration_s,
        distance_mm,
        latitude,
        longitude,
        heartrate_bpm,
        cadence_spm,
        air_temp_c,
        elevation_m,
        speed_mmps,
        vertical_speed_mps,
        vert_oscillation_cm,
        stride_length_cm,
        vert_ratio,
        ground_contact_time_ms,
        ground_contact_balance_left,
        performance_condition,
        loc_point,
        elevation_smooth_m
    FROM staging.vw_activity_flatten
    WHERE latitude IS NOT NULL and longitude IS NOT NULL
    ON CONFLICT (activity_id, ts_utc)   -- T26: was ON CONFLICT(ts_utc); PK is (activity_id, ts_utc)
        DO UPDATE SET
                      activity_type_id = EXCLUDED.activity_type_id,
                      moving_duration_s = EXCLUDED.moving_duration_s,
                      elapsed_duration_s = EXCLUDED.elapsed_duration_s,
                      -- ts_utc = EXCLUDED.ts_utc,
                      duration_s = EXCLUDED.duration_s,
                      distance_mm = EXCLUDED.distance_mm,
                      latitude = EXCLUDED.latitude,
                      longitude = EXCLUDED.longitude,
                      heartrate_bpm = EXCLUDED.heartrate_bpm,
                      cadence_spm = EXCLUDED.cadence_spm,
                      air_temp_c = EXCLUDED.air_temp_c,
                      elevation_m = EXCLUDED.elevation_m,
                      speed_mmps = EXCLUDED.speed_mmps,
                      vertical_speed_mps = EXCLUDED.vertical_speed_mps,
                      vert_oscillation_cm = EXCLUDED.vert_oscillation_cm,
                      stride_length_cm = EXCLUDED.stride_length_cm,
                      vert_ratio = EXCLUDED.vert_ratio,
                      ground_contact_time_ms = EXCLUDED.ground_contact_time_ms,
                      ground_contact_balance_left = EXCLUDED.ground_contact_balance_left,
                      performance_condition = EXCLUDED.performance_condition;


    INSERT INTO activities.activity_processing_queue (activity_id)
    SELECT DISTINCT activity_id FROM staging.vw_activity_flatten
    ON CONFLICT (activity_id) DO UPDATE SET created_at_utc = CURRENT_TIMESTAMP;

    UPDATE api_services.function_library
    SET value_recency = (SELECT MAX(ts_utc) FROM activities.activity_details)
    WHERE friendly_name = v_friendly_name;

    CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports where task_id = v_task_id;

END;
$$;


--
-- Name: flatten_activity_metrics(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.flatten_activity_metrics(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
BEGIN
	TRUNCATE staging.activity_payload CASCADE;

	INSERT INTO staging.activity_payload
	(activity_id, payload)
	SELECT activity_id, payload
	FROM (
	SELECT --DISTINCT			
		(payload->>'activityId')::BIGINT as activity_id,
        payload,
		ROW_NUMBER() OVER (PARTITION BY (payload->>'activityId')::BIGINT ORDER BY event_time_utc desc) as rn
    FROM staging.api_imports wh
    WHERE task_id=v_task_id
	) sub
	where rn = 1;
	
    TRUNCATE staging.activity_metric_descriptors CASCADE;

	INSERT INTO staging.activity_metric_descriptors
	(
	activity_id,
	metric_idx,
	metric_name,
	metric_unit
	)
	SELECT 
	r.activity_id,
    (md_ord)::SMALLINT AS metric_idx,
    (md_elem->>'key')::TEXT AS metric_name,
    (md_elem->'unit'->>'key')::TEXT AS metric_unit
	FROM staging.activity_payload r
	CROSS JOIN LATERAL jsonb_array_elements(
	    r.payload->'metricDescriptors'
	) WITH ORDINALITY AS md(md_elem, md_ord);

	TRUNCATE staging.activity_metrics_flat CASCADE;

	INSERT INTO staging.activity_metrics_flat (
	activity_id, activity_detail_index, metric_index, metric_value
	)
	SELECT 
		ap.activity_id,
		(activity.activity_ord - 1)::INTEGER AS activity_detail_index,
	    (elem.ordinality)::INTEGER AS metric_index,
	    elem.value::text AS metric_value
	FROM staging.activity_payload ap
		CROSS JOIN LATERAL jsonb_array_elements(
    	ap.payload->'activityDetailMetrics'
		) WITH ORDINALITY AS activity(obj, activity_ord)
		CROSS JOIN LATERAL jsonb_array_elements(
		    activity.obj->'metrics'
		) WITH ORDINALITY AS elem(value, ordinality)
	WHERE elem.value::text != 'null';

	TRUNCATE staging.activity_metrics_matched CASCADE;

	INSERT INTO staging.activity_metrics_matched
	(activity_id, activity_type_id, activity_detail_index, metric_name, metric_value)
	SELECT d.activity_id, a.activity_type_id, f.activity_detail_index, d.metric_name, f.metric_value 
	FROM staging.activity_metric_descriptors d
		INNER JOIN activities.activities a on a.activity_id = d.activity_id
	INNER JOIN staging.activity_metrics_flat f on
		f.activity_id = d.activity_id 
		and f.metric_index = d.metric_idx;

END;
$$;


--
-- Name: flatten_now_playing(); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.flatten_now_playing()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_api_function_name TEXT := 'now_playing';  -- Hardcoded function name
	v_error_message TEXT;
BEGIN
    BEGIN 
        INSERT INTO staging.stg_listen_history (
            track_id,
            track_name,
            album_id,
            album_name,
            artist_id,
            artist_name,
            popularity,
            duration_ms,
            incoming_isrc,
            available_market_count,
            available_in_usa,
            context_type,
            context_uri,
            played_at_utc,
			track_release,
			track_release_precision
        )
        SELECT
		    src.payload->'item'->>'id' AS track_id,
		    src.payload->'item'->>'name' AS track_name,
		    src.payload->'item'->'album'->>'id' AS album_id,
		    src.payload->'item'->'album'->>'name' AS album_name,
		    src.payload->'item'->'album'->'artists'->0->>'id' AS artist_id,
		    src.payload->'item'->'album'->'artists'->0->>'name' AS artist_name,
		    (src.payload->'item'->>'popularity')::int AS popularity,
		    (src.payload->'item'->>'duration_ms')::int AS duration_ms,
		    src.payload->'item'->'external_ids'->>'isrc' AS incoming_isrc,
		    jsonb_array_length(src.payload->'item'->'album'->'available_markets') AS available_market_count,
		    (
		        SELECT 'US' = ANY(
		            ARRAY(
		                SELECT jsonb_array_elements_text(src.payload->'item'->'album'->'available_markets')
		            )
		        )
		    ) AS available_in_usa,
		    src.payload->'context'->>'type' AS context_type,
		    src.payload->'context'->>'uri' AS context_uri,
		    to_timestamp((src.payload->>'timestamp')::bigint / 1000.0) AS played_at_utc,
		    src.payload->'item'->'album'->>'release_date' AS track_release,
		    src.payload->'item'->'album'->>'release_date_precision' AS track_release_precision
		FROM staging.api_imports src
         WHERE src.api_function_name = v_api_function_name AND (src.payload->'item'->>'id') IS NOT NULL

        ON CONFLICT (track_id, played_at_utc)
        DO UPDATE SET
            track_name = EXCLUDED.track_name,
            album_id = EXCLUDED.album_id,
            album_name = EXCLUDED.album_name,
            artist_id = EXCLUDED.artist_id,
            artist_name = EXCLUDED.artist_name,
            popularity = EXCLUDED.popularity,
            duration_ms = EXCLUDED.duration_ms,
            incoming_isrc = EXCLUDED.incoming_isrc,
            available_market_count = EXCLUDED.available_market_count,
            available_in_usa = EXCLUDED.available_in_usa,
            context_type = EXCLUDED.context_type,
            context_uri = EXCLUDED.context_uri;

		INSERT INTO music.all_tracks (
		track_id,
		track_name,
		track_name_clean,
		track_name_search,
		track_isrc,
		artist_id,
		artist_display_name,
		album_id,
		release_date,
		available_in_us,
		total_markets,
		popularity,
		duration_ms,
		updated_at_utc
		)
		SELECT DISTINCT
			track_id,
			track_name,
			track_name_clean,
			track_name_search,
			track_isrc,
			artist_id,
			artist_display_name,
			album_id,
			release_date,
			available_in_us,
			total_markets,
			popularity,
			duration_ms,
			updated_at_utc
			FROM staging.vw_track_inserts_from_listen_history d
		ON CONFLICT (track_id)  -- assuming epoch_ts is your primary key
	    DO UPDATE SET
	        track_name_clean = EXCLUDED.track_name_clean,
			track_name_search = EXCLUDED.track_name_search,
			release_date = EXCLUDED.release_date,
			available_in_us = EXCLUDED.available_in_us,
			total_markets = EXCLUDED.total_markets,
			popularity = EXCLUDED.popularity,
			duration_ms = EXCLUDED.duration_ms,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE ((music.all_tracks.track_name_clean IS DISTINCT FROM EXCLUDED.track_name_clean OR
			music.all_tracks.track_name_search IS DISTINCT FROM EXCLUDED.track_name_search OR
			music.all_tracks.release_date IS DISTINCT FROM EXCLUDED.release_date OR
			music.all_tracks.available_in_us IS DISTINCT FROM EXCLUDED.available_in_us OR
			music.all_tracks.total_markets IS DISTINCT FROM EXCLUDED.total_markets OR
			music.all_tracks.popularity IS DISTINCT FROM EXCLUDED.popularity OR
			music.all_tracks.duration_ms IS DISTINCT FROM EXCLUDED.duration_ms) AND 
			music.all_tracks.updated_at_utc < EXCLUDED.updated_at_utc);

		INSERT INTO music.all_artists (
				artist_id,
				artist_name,
				artist_name_search,
				updated_at_utc
		)
		SELECT DISTINCT
			artist_id,
			artist_name,
			artist_name_search,
			updated_at_utc
		FROM staging.vw_artist_inserts
		ON CONFLICT (artist_id)
		DO UPDATE SET 
			artist_name = EXCLUDED.artist_name,
			artist_name_search = EXCLUDED.artist_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_artists.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_artists.artist_name IS DISTINCT FROM EXCLUDED.artist_name OR
			music.all_artists.artist_name_search IS DISTINCT FROM EXCLUDED.artist_name_search));

		INSERT INTO music.all_albums (
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		)
		SELECT DISTINCT
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		FROM staging.vw_album_inserts
		ON CONFLICT (album_id)
		DO UPDATE SET 
			album_name = EXCLUDED.album_name,
			album_name_clean = EXCLUDED.album_name_clean,
			album_name_search = EXCLUDED.album_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_albums.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_albums.album_name IS DISTINCT FROM EXCLUDED.album_name OR
			music.all_albums.album_name_clean IS DISTINCT FROM EXCLUDED.album_name_clean OR 
			music.all_albums.album_name_search IS DISTINCT FROM EXCLUDED.album_name_search));

		DELETE FROM staging.api_imports WHERE api_function_name = v_api_function_name;

		DELETE FROM staging.stg_now_playing;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
            INSERT INTO logging.application_events (event_category, event_description, error_text)
            VALUES ('SPROC ERROR', v_api_function_name, v_error_message);
    END;
	
END;
$$;


--
-- Name: flatten_playlist_details(text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.flatten_playlist_details(IN list_type text DEFAULT 'auto'::text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_api_function_name TEXT := 'playlist_details';
    v_error_message TEXT;
    v_bad_ids INTEGER;
BEGIN
    BEGIN
        SELECT count(*) INTO v_bad_ids
        FROM staging.api_imports r
        WHERE api_function_name = v_api_function_name
          AND left(split_part(split_part((r.payload->>'href'), '/playlists/', 2), '/tracks', 1), 22)
              !~ '^[A-Za-z0-9]{22}$';
        IF v_bad_ids > 0 THEN
            RAISE EXCEPTION 'flatten_playlist_details: % staged payload(s) with non-22-char id — aborting, no writes (000-001 AC-8)', v_bad_ids;
        END IF;

        INSERT INTO staging.stg_playlist_details (
            playlist_id, track_id, track_name, track_order, popularity,
            duration_ms, album_id, album_name, track_release,
            track_release_precision, artist_id, artist_name, incoming_isrc,
            track_markets, fetched_at_utc
        )
        SELECT
            left(split_part(split_part((r.payload->>'href'), '/playlists/', 2), '/tracks', 1), 22) AS playlist_id,
            t.track->>'id' AS track_id,
            t.track->>'name' AS track_name,
            COALESCE(
                NULLIF(split_part(split_part((r.payload->>'href'), 'offset=', 2), '&limit', 1), ''),
                '0'
            )::INTEGER + i.item_pos AS track_order,
            (t.track->>'popularity')::INT AS popularity,
            (t.track->>'duration_ms')::INT AS duration_ms,
            t.track->'album'->>'id' AS album_id,
            t.track->'album'->>'name' AS album_name,
            (t.track->'album'->>'release_date') AS track_release,
            t.track->'album'->>'release_date_precision' AS track_release_precision,
            t.track->'artists'->0->>'id' AS artist_id,
            t.track->'artists'->0->>'name' AS artist_name,
            t.track->'external_ids'->>'isrc' AS incoming_isrc,
            ARRAY(
                SELECT jsonb_array_elements_text(t.track->'available_markets')
            ) AS track_markets,
            now() as fetched_at_utc
        FROM staging.api_imports r
        CROSS JOIN LATERAL jsonb_array_elements(r.payload->'items')
            WITH ORDINALITY AS i(item, item_pos)
        CROSS JOIN LATERAL (
            SELECT i.item->'track' AS track
        ) t
        WHERE api_function_name = v_api_function_name
        and t.track IS NOT NULL;
                -- REMAINDER FROM LIVE BODY (verbatim from PRD 2026-09-14; see
        -- 000-001_rollback_flatten_playlist_details.sql). These upserts +
        -- playlist_isrcs refresh + staging cleanup follow the corrected INSERT
        -- above unchanged — only the playlist_id derivation + fail-loud gate
        -- above are the T05 changes.
        INSERT INTO music.all_tracks (
            track_id,
            track_name,
            track_name_clean,
            track_name_search,
            track_isrc,
            artist_id,
            artist_display_name,
            album_id,
            release_date,
            available_in_us,
            total_markets,
            popularity,
            duration_ms,
            updated_at_utc
        )
        SELECT DISTINCT
            track_id,
            track_name,
            track_name_clean,
            track_name_search,
            track_isrc,
            artist_id,
            artist_display_name,
            album_id,
            release_date,
            available_in_us,
            total_markets,
            popularity,
            duration_ms,
            updated_at_utc
            FROM staging.vw_track_inserts d
        ON CONFLICT (track_id)  -- assuming epoch_ts is your primary key
        DO UPDATE SET
            track_name_clean = EXCLUDED.track_name_clean,
            track_name_search = EXCLUDED.track_name_search,
            release_date = EXCLUDED.release_date,
            available_in_us = EXCLUDED.available_in_us,
            total_markets = EXCLUDED.total_markets,
            popularity = EXCLUDED.popularity,
            duration_ms = EXCLUDED.duration_ms,
            updated_at_utc = EXCLUDED.updated_at_utc
        WHERE ((music.all_tracks.track_name_clean IS DISTINCT FROM EXCLUDED.track_name_clean OR
            music.all_tracks.track_name_search IS DISTINCT FROM EXCLUDED.track_name_search OR
            music.all_tracks.release_date IS DISTINCT FROM EXCLUDED.release_date OR
            music.all_tracks.available_in_us IS DISTINCT FROM EXCLUDED.available_in_us OR
            music.all_tracks.total_markets IS DISTINCT FROM EXCLUDED.total_markets OR
            music.all_tracks.popularity IS DISTINCT FROM EXCLUDED.popularity OR
            music.all_tracks.duration_ms IS DISTINCT FROM EXCLUDED.duration_ms) AND
            music.all_tracks.updated_at_utc < EXCLUDED.updated_at_utc);

        INSERT INTO music.all_artists (
            artist_id,
            artist_name,
            artist_name_search,
            updated_at_utc
        )
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_name_search,
            updated_at_utc
        FROM staging.vw_artist_inserts
        ON CONFLICT (artist_id)
        DO UPDATE SET
            artist_name = EXCLUDED.artist_name,
            artist_name_search = EXCLUDED.artist_name_search,
            updated_at_utc = EXCLUDED.updated_at_utc
        WHERE (music.all_artists.updated_at_utc < EXCLUDED.updated_at_utc AND
            (music.all_artists.artist_name IS DISTINCT FROM EXCLUDED.artist_name OR
            music.all_artists.artist_name_search IS DISTINCT FROM EXCLUDED.artist_name_search));

        INSERT INTO music.all_albums (
            album_id,
            album_name,
            album_name_clean,
            album_name_search,
            updated_at_utc
        )
        SELECT DISTINCT
            album_id,
            album_name,
            album_name_clean,
            album_name_search,
            updated_at_utc
        FROM staging.vw_album_inserts
        ON CONFLICT (album_id)
        DO UPDATE SET
            album_name = EXCLUDED.album_name,
            album_name_clean = EXCLUDED.album_name_clean,
            album_name_search = EXCLUDED.album_name_search,
            updated_at_utc = EXCLUDED.updated_at_utc
        WHERE (music.all_albums.updated_at_utc < EXCLUDED.updated_at_utc AND
            (music.all_albums.album_name IS DISTINCT FROM EXCLUDED.album_name OR
            music.all_albums.album_name_clean IS DISTINCT FROM EXCLUDED.album_name_clean OR
            music.all_albums.album_name_search IS DISTINCT FROM EXCLUDED.album_name_search));

        DELETE FROM music.playlist_isrcs WHERE playlist_id in (SELECT DISTINCT playlist_id from staging.stg_playlist_details);

        IF COALESCE(list_type, '') NOT IN ('once', 'seeds') THEN
            INSERT INTO music.playlist_isrcs (
                playlist_id,
                isrc,
                track_order
            )
            SELECT
                playlist_id,
                isrc,
                track_order
            FROM staging.vw_playlist_inserts;
        END IF;

        DELETE FROM staging.api_imports WHERE api_function_name = v_api_function_name;

        DELETE FROM staging.stg_playlist_details;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
            INSERT INTO logging.application_events (event_category, event_description, error_text)
            VALUES ('SPROC ERROR', v_api_function_name, v_error_message);
            RAISE;
    END;
END;
$_$;


--
-- Name: flatten_playlists(); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.flatten_playlists()
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_api_function_name TEXT := 'current_user_playlists';  -- Hardcoded function name
	v_error_message TEXT;
BEGIN
	BEGIN

		UPDATE music.playlist_config set is_active=False where is_active;

	    INSERT INTO music.playlist_config (
	        playlist_id,
			playlist_name,
	        playlist_description,
			track_count
	        -- add other columns as needed like date, charged, drained, etc.
	    )
	    SELECT
			(item->>'id') AS playlist_id,
	        (item->>'name') as playlist_name,
			(item->>'description') as playlist_description,
			(item->'tracks'->>'total')::NUMERIC(4,0) as track_count
	    FROM staging.api_imports src
			CROSS JOIN LATERAL jsonb_array_elements(
				    CASE
				        WHEN jsonb_typeof(src.payload->'items') = 'array'
				        THEN src.payload->'items'
				        ELSE '[]'::jsonb
				    END
				) AS item
	    WHERE api_function_name = v_api_function_name
	    ON CONFLICT (playlist_id)  -- assuming epoch_ts is your primary key
	    DO UPDATE SET
	        playlist_name = EXCLUDED.playlist_name,
			playlist_description = EXCLUDED.playlist_description,
			track_count = EXCLUDED.track_count,
			is_active = TRUE
		WHERE (music.playlist_config.playlist_name IS DISTINCT FROM EXCLUDED.playlist_name OR
		music.playlist_config.playlist_description IS DISTINCT FROM EXCLUDED.playlist_description OR
		music.playlist_config.track_count IS DISTINCT FROM EXCLUDED.track_count
		or NOT music.playlist_config.is_active);

-- 		DELETE FROM music.playlist_relationships pr
-- 		WHERE pr.child_playlist_id in (SELECT DISTINCT playlist_id FROM music.playlist_config WHERE not is_active);

	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
			INSERT INTO logging.application_events (event_category, event_description, error_text)
			VALUES ('SPROC ERROR', v_api_function_name, v_error_message);
	END;

    DELETE FROM staging.api_imports WHERE api_function_name = v_api_function_name;
END;
$$;


--
-- Name: generate_series_5min(timestamp with time zone, timestamp with time zone); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.generate_series_5min(IN lower_ts timestamp with time zone, IN upper_ts timestamp with time zone)
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE staging.interpolation_load_values;

    INSERT INTO staging.interpolation_load_values (ts_utc)
    SELECT generate_series(
        date_trunc('minute', lower_ts)
            - (extract(minute from lower_ts)::int % 5) * interval '1 minute',
        date_trunc('minute', upper_ts)
            - (extract(minute from upper_ts)::int % 5) * interval '1 minute',
        interval '5 minutes'
    );
END;
$$;


--
-- Name: get_elevation_deltas(bigint, integer, integer); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.get_elevation_deltas(IN v_activity_id bigint, IN v_start_point integer, IN v_end_point integer)
    LANGUAGE plpgsql
    AS $$

BEGIN

	TRUNCATE staging.temp_elevation_smoothed;

	INSERT INTO staging.temp_elevation_smoothed
	(activity_id, track_point, elevation_m)
	SELECT 
		a.activity_id,
		a.elapsed_duration_s as track_point,
		round(avg(e.elevation_avg) OVER 
			(PARTITION BY a.activity_id
			ORDER BY a.elapsed_duration_s 
			ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 1) AS elevation_m
	FROM activities.running_details a
		JOIN activities.vw_avg_elevation e ON e.latitude = a.latitude AND e.longitude = a.longitude
	WHERE activity_id = v_activity_id and elapsed_duration_s >= v_start_point and elapsed_duration_s <= v_end_point;
	
	TRUNCATE staging.temp_elevation_deltas;
	
	INSERT INTO staging.temp_elevation_deltas
	(activity_id,
	ascent_m,
	descent_m)
	SELECT 
		activity_id,
		ROUND(sum(CASE WHEN elevation_delta > 0 then elevation_delta else 0 END),2) as ascent_m,
		ROUND(sum(CASE WHEN elevation_delta < 0 then elevation_delta else 0 END),2) as descent_m
	FROM (
		SELECT 
		activity_id,
		elevation_m - LAG(elevation_m) OVER (PARTITION BY activity_id ORDER BY track_point asc) as elevation_delta
		FROM staging.temp_elevation_smoothed
		) subquery
	GROUP BY activity_id;

END;
$$;


--
-- Name: heartrate_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.heartrate_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.heartrate_raw (  --############UPDATE TABLE NAME
        ts_utc,
		heartrate_bpm
    )
    SELECT
        ts_utc,
		heartrate_bpm
    FROM (
        SELECT 
            to_timestamp(round((array_element->>0)::bigint/1000)) AS ts_utc,
			(array_element->>1)::SMALLINT AS heartrate_bpm,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((array_element->>0)::bigint/1000))
                ORDER BY event_time_utc asc
            ) as row_num
        FROM staging.api_imports
	         CROSS JOIN LATERAL jsonb_array_elements(payload->'heartRateValues') AS array_element
        WHERE task_id = v_task_id
          AND jsonb_typeof(payload->'heartRateValues') = 'array' -- IS NOT NULL
		  AND (array_element->>1) IS NOT NULL
          AND (array_element->>1)::INTEGER > 15
		  ) subquery
		  WHERE row_num =1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        heartrate_bpm = EXCLUDED.heartrate_bpm
	WHERE health.heartrate_raw.heartrate_bpm IS DISTINCT FROM EXCLUDED.heartrate_bpm;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.heartrate_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Heart Rate'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
	
END;
$$;


--
-- Name: hrv_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.hrv_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.hrv_raw (  --############UPDATE TABLE NAME
        ts_utc,
		hrv_value
    )
    SELECT
        ts_utc,
		hrv_value
    FROM (
        SELECT 
            DATE_TRUNC('second', ((elem ->> 'readingTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ) AS ts_utc,
			(elem ->> 'hrvValue')::SMALLINT AS hrv_value,
            ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC('second', ((elem ->> 'readingTimeGMT')::TIMESTAMP at TIME ZONE 'UTC')::TIMESTAMPTZ)
                ORDER BY (elem ->> 'hrvValue')::SMALLINT asc
            ) as row_num
        FROM staging.api_imports
	         CROSS JOIN LATERAL jsonb_array_elements(payload -> 'hrvReadings') AS elem
        WHERE task_id = v_task_id 
          AND COALESCE((elem ->> 'hrvValue')::SMALLINT,0) > 10 -- IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        hrv_value = EXCLUDED.hrv_value
	WHERE health.hrv_raw.hrv_value IS DISTINCT FROM EXCLUDED.hrv_value;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.hrv_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'HRV'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: json_sampling(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.json_sampling(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
BEGIN
	INSERT INTO staging.json_examples
	(raw_table_name,
	json_sample,
	updated_at_utc
	)
	SELECT
	v_friendly_name as raw_table_name,
	payload as json_sample,
	CURRENT_TIMESTAMP as updated_at_utc
	FROM staging.api_imports WHERE task_id = v_task_id
    ORDER BY event_time_utc desc
	LIMIT 1
	ON CONFLICT(raw_table_name)
	DO UPDATE
	SET
		json_sample = EXCLUDED.json_sample,
		updated_at_utc = EXCLUDED.updated_at_utc;

END;
$$;


--
-- Name: listen_history_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.listen_history_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN
	--------------------------------------------------------------------
    -- HRV
    --------------------------------------------------------------------
	
    INSERT INTO staging.stg_listen_history (  --############UPDATE TABLE NAME
        track_id,
		track_name,
		album_id,
		album_name,
		artist_id,
		artist_name,
		popularity,
		duration_ms,
		incoming_isrc,
		available_market_count,
		available_in_usa,
		context_type,
		context_uri,
		played_at_utc,
		track_release,
		track_release_precision
    )
    SELECT
        track_id,
		track_name,
		album_id,
		album_name,
		artist_id,
		artist_name,
		popularity,
		duration_ms,
		incoming_isrc,
		available_market_count,
		available_in_usa,
		context_type,
		context_uri,
		played_at_utc,
		track_release,
		track_release_precision
    FROM (
        SELECT 
            item->'track'->>'id' AS track_id,
            item->'track'->>'name' AS track_name,
            item->'track'->'album'->>'id' AS album_id,
            item->'track'->'album'->>'name' AS album_name,
            item->'track'->'album'->'artists'->0->>'id' AS artist_id,
            item->'track'->'album'->'artists'->0->>'name' AS artist_name,
            (item->'track'->>'popularity')::int AS popularity,
            (item->'track'->>'duration_ms')::int AS duration_ms,
            item->'track'->'external_ids'->>'isrc' AS incoming_isrc,
            jsonb_array_length(item->'track'->'album'->'available_markets') AS available_market_count,
            (
                SELECT 'US' = ANY(
                    ARRAY(
                        SELECT jsonb_array_elements_text(item->'track'->'album'->'available_markets')
                    )
                )
            ) AS available_in_usa,
            item->'context'->>'type' AS context_type,
            item->'context'->>'uri' AS context_uri,
            (item->>'played_at')::timestamptz AS played_at_utc,
			(item->'track'->'album'->>'release_date')    AS track_release,
		    (item->'track'->'album'->>'release_date_precision')  AS track_release_precision,
            ROW_NUMBER() OVER (
                PARTITION BY item->'track'->>'id', COALESCE(item->'context'->>'uri',''), (item->>'played_at')::timestamptz
                ORDER BY (item->'track'->>'popularity')::int desc
            ) as row_num
        FROM staging.api_imports src
             CROSS JOIN LATERAL jsonb_array_elements(
				    CASE
				        WHEN jsonb_typeof(src.payload->'items') = 'array'
				        THEN src.payload->'items'
				        ELSE '[]'::jsonb
				    END
				) AS item
        WHERE task_id = v_task_id 
          AND item->'track'->>'id' IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (track_id, played_at_utc)
        DO UPDATE SET
            track_name = EXCLUDED.track_name,
            album_id = EXCLUDED.album_id,
            album_name = EXCLUDED.album_name,
            artist_id = EXCLUDED.artist_id,
            artist_name = EXCLUDED.artist_name,
            popularity = EXCLUDED.popularity,
            duration_ms = EXCLUDED.duration_ms,
            incoming_isrc = EXCLUDED.incoming_isrc,
            available_market_count = EXCLUDED.available_market_count,
            available_in_usa = EXCLUDED.available_in_usa,
            context_type = EXCLUDED.context_type,
            context_uri = EXCLUDED.context_uri;  --############UPDATE TABLE NAME

	INSERT INTO music.listening_history (
		played_at_utc,
		isrc,
		playlist_id
		)
		SELECT 
		played_at_utc,
		isrc,
		playlist_id
		FROM staging.vw_listening_history_inserts
		ON CONFLICT(played_at_utc, isrc)
		DO NOTHING;

		INSERT INTO music.all_tracks (
		track_id,
		track_name,
		track_name_clean,
		track_name_search,
		track_isrc,
		artist_id,
		artist_display_name,
		album_id,
		release_date,
		available_in_us,
		total_markets,
		popularity,
		duration_ms,
		updated_at_utc
		)
		SELECT DISTINCT
			track_id,
			track_name,
			track_name_clean,
			track_name_search,
			track_isrc,
			artist_id,
			artist_display_name,
			album_id,
			release_date,
			available_in_us,
			total_markets,
			popularity,
			duration_ms,
			updated_at_utc
			FROM staging.vw_track_inserts_from_listen_history d
		ON CONFLICT (track_id)  -- assuming epoch_ts is your primary key
	    DO UPDATE SET
	        track_name_clean = EXCLUDED.track_name_clean,
			track_name_search = EXCLUDED.track_name_search,
			release_date = EXCLUDED.release_date,
			available_in_us = EXCLUDED.available_in_us,
			total_markets = EXCLUDED.total_markets,
			popularity = EXCLUDED.popularity,
			duration_ms = EXCLUDED.duration_ms,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE ((music.all_tracks.track_name_clean IS DISTINCT FROM EXCLUDED.track_name_clean OR
			music.all_tracks.track_name_search IS DISTINCT FROM EXCLUDED.track_name_search OR
			music.all_tracks.release_date IS DISTINCT FROM EXCLUDED.release_date OR
			music.all_tracks.available_in_us IS DISTINCT FROM EXCLUDED.available_in_us OR
			music.all_tracks.total_markets IS DISTINCT FROM EXCLUDED.total_markets OR
			music.all_tracks.popularity IS DISTINCT FROM EXCLUDED.popularity OR
			music.all_tracks.duration_ms IS DISTINCT FROM EXCLUDED.duration_ms) AND 
			music.all_tracks.updated_at_utc < EXCLUDED.updated_at_utc);

		INSERT INTO music.all_artists (
				artist_id,
				artist_name,
				artist_name_search,
				updated_at_utc
		)
		SELECT DISTINCT
			artist_id,
			artist_name,
			artist_name_search,
			updated_at_utc
		FROM staging.vw_artist_inserts
		ON CONFLICT (artist_id)
		DO UPDATE SET 
			artist_name = EXCLUDED.artist_name,
			artist_name_search = EXCLUDED.artist_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_artists.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_artists.artist_name IS DISTINCT FROM EXCLUDED.artist_name OR
			music.all_artists.artist_name_search IS DISTINCT FROM EXCLUDED.artist_name_search));

		INSERT INTO music.all_albums (
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		)
		SELECT DISTINCT
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		FROM staging.vw_album_inserts
		ON CONFLICT (album_id)
		DO UPDATE SET 
			album_name = EXCLUDED.album_name,
			album_name_clean = EXCLUDED.album_name_clean,
			album_name_search = EXCLUDED.album_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_albums.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_albums.album_name IS DISTINCT FROM EXCLUDED.album_name OR
			music.all_albums.album_name_clean IS DISTINCT FROM EXCLUDED.album_name_clean OR 
			music.all_albums.album_name_search IS DISTINCT FROM EXCLUDED.album_name_search));

	
	--------------------------------------------------------------------
    -- Recency Update
    --------------------------------------------------------------------	
    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(played_at_utc) FROM staging.stg_listen_history)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Recent Plays'
    WHERE friendly_name = v_friendly_name;
	
	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;

	TRUNCATE staging.stg_listen_history;
END;
$$;


--
-- Name: playlist_detail_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.playlist_detail_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN
	TRUNCATE staging.stg_playlist_details;

    INSERT INTO staging.stg_playlist_details (  --############UPDATE TABLE NAME
			playlist_id,
			track_id,
	        track_name,
			track_order,
			popularity,
			duration_ms,
			album_id,
			album_name,
			track_release,
			track_release_precision,
			artist_id,
			artist_name,
			incoming_isrc,
			track_markets,
			fetched_at_utc
    )
    SELECT
			playlist_id,
			track_id,
	        track_name,
			track_order,
			popularity,
			duration_ms,
			album_id,
			album_name,
			track_release,
			track_release_precision,
			artist_id,
			artist_name,
			incoming_isrc,
			track_markets,
			fetched_at_utc

    FROM (
        SELECT
            left(split_part(split_part((r.payload->>'href'), '/playlists/', 2), '/tracks', 1),22) AS playlist_id,

		    t.track->>'id'                         AS track_id,
		    t.track->>'name'                       AS track_name,
			COALESCE(
                NULLIF(split_part(split_part((r.payload->>'href'), 'offset=', 2), '&limit', 1), ''),
                '0'
            )::INTEGER + i.item_pos AS track_order,
		    (t.track->>'popularity')::INT          AS popularity,
		    (t.track->>'duration_ms')::INT         AS duration_ms,

		    t.track->'album'->>'id'                AS album_id,
		    t.track->'album'->>'name'              AS album_name,

		    (t.track->'album'->>'release_date')    AS track_release,
		    t.track->'album'->>'release_date_precision'  AS track_release_precision,

		    t.track->'artists'->0->>'id'            AS artist_id,
		    t.track->'artists'->0->>'name'          AS artist_name,

			t.track->'external_ids'->>'isrc'		AS incoming_isrc,

		    ARRAY(
		        SELECT jsonb_array_elements_text(t.track->'available_markets')
		    )                                       AS track_markets,

		    CURRENT_TIMESTAMP as fetched_at_utc,
            ROW_NUMBER() OVER (
                PARTITION BY t.track->>'id', split_part(split_part((r.payload->>'href'), '/playlists/', 2), '/tracks', 1)
                ORDER BY event_time_utc desc
            ) as row_num
        FROM staging.api_imports r
		CROSS JOIN LATERAL jsonb_array_elements(r.payload->'items')
			WITH ORDINALITY AS i(item, item_pos)
		CROSS JOIN LATERAL (
		    SELECT i.item->'track' AS track
		) t
        WHERE task_id = v_task_id
          AND t.track->>'id'  IS NOT NULL
    ) AS source_data
    WHERE row_num = 1;

	INSERT INTO music.all_tracks (
		track_id,
		track_name,
		track_name_clean,
		track_name_search,
		track_isrc,
		artist_id,
		artist_display_name,
		album_id,
		release_date,
		available_in_us,
		total_markets,
		popularity,
		duration_ms,
		updated_at_utc
		)
		SELECT DISTINCT
			track_id,
			track_name,
			track_name_clean,
			track_name_search,
			track_isrc,
			artist_id,
			artist_display_name,
			album_id,
			release_date,
			available_in_us,
			total_markets,
			popularity,
			duration_ms,
			updated_at_utc
			FROM staging.vw_track_inserts d
		ON CONFLICT (track_id)
	    DO UPDATE SET
	        track_name_clean = EXCLUDED.track_name_clean,
			track_name_search = EXCLUDED.track_name_search,
			release_date = EXCLUDED.release_date,
			available_in_us = EXCLUDED.available_in_us,
			total_markets = EXCLUDED.total_markets,
			popularity = EXCLUDED.popularity,
			duration_ms = EXCLUDED.duration_ms,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE ((music.all_tracks.track_name_clean IS DISTINCT FROM EXCLUDED.track_name_clean OR
			music.all_tracks.track_name_search IS DISTINCT FROM EXCLUDED.track_name_search OR
			music.all_tracks.release_date IS DISTINCT FROM EXCLUDED.release_date OR
			music.all_tracks.available_in_us IS DISTINCT FROM EXCLUDED.available_in_us OR
			music.all_tracks.total_markets IS DISTINCT FROM EXCLUDED.total_markets OR
			music.all_tracks.popularity IS DISTINCT FROM EXCLUDED.popularity OR
			music.all_tracks.duration_ms IS DISTINCT FROM EXCLUDED.duration_ms) AND
			music.all_tracks.updated_at_utc < EXCLUDED.updated_at_utc);

		INSERT INTO music.all_artists (
				artist_id,
				artist_name,
				artist_name_search,
				updated_at_utc
		)
		SELECT DISTINCT
			artist_id,
			artist_name,
			artist_name_search,
			updated_at_utc
		FROM staging.vw_artist_inserts
		ON CONFLICT (artist_id)
		DO UPDATE SET
			artist_name = EXCLUDED.artist_name,
			artist_name_search = EXCLUDED.artist_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_artists.updated_at_utc < EXCLUDED.updated_at_utc AND
			(music.all_artists.artist_name IS DISTINCT FROM EXCLUDED.artist_name OR
			music.all_artists.artist_name_search IS DISTINCT FROM EXCLUDED.artist_name_search));

		INSERT INTO music.all_albums (
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		)
		SELECT DISTINCT
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		FROM staging.vw_album_inserts
		ON CONFLICT (album_id)
		DO UPDATE SET
			album_name = EXCLUDED.album_name,
			album_name_clean = EXCLUDED.album_name_clean,
			album_name_search = EXCLUDED.album_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_albums.updated_at_utc < EXCLUDED.updated_at_utc AND
			(music.all_albums.album_name IS DISTINCT FROM EXCLUDED.album_name OR
			music.all_albums.album_name_clean IS DISTINCT FROM EXCLUDED.album_name_clean OR
			music.all_albums.album_name_search IS DISTINCT FROM EXCLUDED.album_name_search));

		DELETE FROM music.playlist_isrcs WHERE playlist_id in (SELECT DISTINCT playlist_id from staging.stg_playlist_details);

		UPDATE music.playlist_config SET last_synced_utc = CURRENT_TIMESTAMP where playlist_id in (SELECT DISTINCT playlist_id from staging.stg_playlist_details);

		INSERT INTO music.playlist_isrcs (
				playlist_id,
				isrc,
				track_order
			)
			SELECT
				playlist_id,
				isrc,
				track_order
			FROM staging.vw_playlist_inserts;

	--------------------------------------------------------------------
    -- Recency Update
    --------------------------------------------------------------------
    UPDATE api_services.function_library
    SET value_recency = CURRENT_TIMESTAMP  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Playlist Details'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;

	DELETE FROM staging.stg_playlist_details;

END;
$$;


--
-- Name: playlist_header_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.playlist_header_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_staged_rows INTEGER;
    v_bad_ids INTEGER;
    v_missing_ids INTEGER;
BEGIN
    UPDATE music.playlist_config SET prior_track_count = track_count;
   SELECT count(*) INTO v_staged_rows
    FROM staging.api_imports src
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(src.payload->'items') = 'array'
        THEN src.payload->'items' ELSE '[]'::jsonb END
    ) AS item
    WHERE task_id = v_task_id AND (item->>'id') IS NOT NULL;

    SELECT count(*) INTO v_bad_ids
    FROM staging.api_imports src
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(src.payload->'items') = 'array'
        THEN src.payload->'items' ELSE '[]'::jsonb END
    ) AS item
    WHERE task_id = v_task_id AND (item->>'id') IS NOT NULL
      AND (item->>'id') !~ '^[A-Za-z0-9]{22}$';

    INSERT INTO music.playlist_config (
        playlist_id, playlist_name, playlist_description, track_count,
        last_verified_utc, is_active
    )
    SELECT playlist_id, playlist_name, playlist_description, track_count,
        last_verified_utc, is_active
    FROM (
        SELECT (item->>'id') AS playlist_id,
            (item->>'name') as playlist_name,
            (item->>'description') as playlist_description,
            (item->'tracks'->>'total')::NUMERIC(4,0) as track_count,
            CURRENT_TIMESTAMP as last_verified_utc,
            TRUE as is_active,
            ROW_NUMBER() OVER (
                PARTITION BY (item->>'id') ORDER BY event_time_utc desc
            ) as row_num
        FROM staging.api_imports src
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(src.payload->'items') = 'array'
            THEN src.payload->'items' ELSE '[]'::jsonb END
        ) AS item
        WHERE task_id = v_task_id AND (item->>'id') IS NOT NULL
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (playlist_id)
    DO UPDATE SET playlist_name = EXCLUDED.playlist_name,
        playlist_description = EXCLUDED.playlist_description,
        track_count = EXCLUDED.track_count,
        last_verified_utc = EXCLUDED.last_verified_utc,
        is_active = TRUE
    WHERE (music.playlist_config.playlist_name IS DISTINCT FROM EXCLUDED.playlist_name OR
    music.playlist_config.playlist_description IS DISTINCT FROM EXCLUDED.playlist_description OR
    music.playlist_config.track_count IS DISTINCT FROM EXCLUDED.track_count
    or NOT music.playlist_config.is_active);

    SELECT count(*) INTO v_missing_ids
    FROM music.playlist_config pc
    WHERE pc.is_active AND NOT EXISTS (
        SELECT 1 FROM staging.api_imports src
                          CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(src.payload->'items') = 'array'
                         THEN src.payload->'items' ELSE '[]'::jsonb END
                                             ) AS item
        WHERE src.task_id = v_task_id AND (item->>'id') = pc.playlist_id
    );

    IF v_staged_rows = 0 OR v_bad_ids > 0 OR v_missing_ids > 0 THEN
        INSERT INTO logging.application_events (event_category, event_description, error_text)
        VALUES ('SPROC GUARD', v_friendly_name,
                format('playlist_header_flatten aborted: staged=%s bad=%s missing=%s (000-001 AC-5/AC-8)',
                       v_staged_rows, v_bad_ids, v_missing_ids));
        RAISE EXCEPTION 'playlist_header_flatten: incomplete payload (staged=%, bad=%, missing=%) — no deletes (000-001)', v_staged_rows, v_bad_ids, v_missing_ids;
    END IF;

    UPDATE music.playlist_config set is_active = FALSE
    WHERE is_active AND playlist_id NOT IN (
        SELECT DISTINCT (item->>'id') FROM staging.api_imports src
                                               CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(src.payload->'items') = 'array'
                         THEN src.payload->'items' ELSE '[]'::jsonb END
                                                                  ) AS item
        WHERE src.task_id = v_task_id AND (item->>'id') IS NOT NULL
    );

    DELETE FROM music.playlist_relationships pr
    WHERE pr.child_playlist_id in (SELECT DISTINCT playlist_id FROM music.playlist_config WHERE not is_active);

    UPDATE api_services.function_library
    SET value_recency = CURRENT_TIMESTAMP
    WHERE friendly_name = v_friendly_name;

    CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;

END;
$_$;


--
-- Name: populate_load_values(text, text, text, text, boolean, integer); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.populate_load_values(IN src_schema text, IN src_table text, IN src_col text, IN src_ts_col text, IN infer_values boolean, IN target_scale integer)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    -- We build the aggregation expressions dynamically based on scale
    expr_d30  text;
    expr_d90  text;
    expr_dall text;
BEGIN
    -- Pre-calculate the SQL fragments for rounding. 
    -- We avoid complex nested format() calls inside the main query.
    IF target_scale IS NOT NULL THEN
        expr_d30  := format('round(avg(val) FILTER (WHERE ts_utc > now() - interval ''30 days''), %s)', target_scale);
        expr_d90  := format('round(avg(val) FILTER (WHERE ts_utc > now() - interval ''90 days''), %s)', target_scale);
        expr_dall := format('round(avg(val), %s)', target_scale);
    ELSE
        expr_d30  := 'avg(val) FILTER (WHERE ts_utc > now() - interval ''30 days'')';
        expr_d90  := 'avg(val) FILTER (WHERE ts_utc > now() - interval ''90 days'')';
        expr_dall := 'avg(val)';
    END IF;

    IF infer_values THEN
        EXECUTE format('
            WITH src_5m AS (
                SELECT
                    date_bin(
                        interval ''5 minutes'',
                        %1$I,
                        timestamptz ''1970-01-01''
                    ) AS ts_utc,
                    avg(%2$I) AS val
                FROM %3$I.%4$I
                WHERE %2$I IS NOT NULL
                GROUP BY 1
            ),
            stats AS (
                SELECT
                    extract(hour from ts_utc)::int * 12
                      + extract(minute from ts_utc)::int / 5 AS bucket,
                    %5$s AS d30,
                    %6$s AS d90,
                    %7$s AS d_all
                FROM src_5m
                GROUP BY 1
            )
            UPDATE staging.interpolation_load_values b
            SET load_value = COALESCE(
                s.val,
                st.d30,
                st.d90,
                st.d_all
            )
            FROM src_5m s
            LEFT JOIN stats st
              ON st.bucket =
                   extract(hour from s.ts_utc)::int * 12
                 + extract(minute from s.ts_utc)::int / 5
            WHERE s.ts_utc = b.ts_utc',
            src_ts_col,   -- %1$I
            src_col,      -- %2$I
            src_schema,   -- %3$I
            src_table,    -- %4$I
            expr_d30,     -- %5$s (pre-built string)
            expr_d90,     -- %6$s
            expr_dall     -- %7$s
        );
    ELSE
        EXECUTE format('
            WITH src_5m AS (
                SELECT
                    date_bin(
                        interval ''5 minutes'',
                        %1$I,
                        timestamptz ''1970-01-01''
                    ) AS ts_utc,
                    avg(%2$I) AS val
                FROM %3$I.%4$I
                WHERE %2$I IS NOT NULL
                GROUP BY 1
            )
            UPDATE staging.interpolation_load_values b
            SET load_value = s.val
            FROM src_5m s
            WHERE s.ts_utc = b.ts_utc',
            src_ts_col,
            src_col,
            src_schema,
            src_table
        );
    END IF;
END;
$_$;


--
-- Name: race_predictions_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.race_predictions_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO activities.race_predictions_raw (  --############UPDATE TABLE NAME
        ts_utc,
		prediction_5k_s,
		prediction_10k_s,
		prediction_half_marathon_s, 
		prediction_marathon_s
    )
    SELECT
        ts_utc,
		prediction_5k_s,
		prediction_10k_s,
		prediction_half_marathon_s, 
		prediction_marathon_s
    FROM (
        SELECT 
            (payload->>'calendarDate')::TIMESTAMPTZ AS ts_utc,
			(payload->>'time5K')::INTEGER AS prediction_5k_s, 
			(payload->>'time10K')::INTEGER AS prediction_10k_s, 
			(payload->>'timeHalfMarathon')::INTEGER AS prediction_half_marathon_s,
			(payload->>'timeMarathon')::INTEGER AS prediction_marathon_s,
            ROW_NUMBER() OVER (
                PARTITION BY (payload->>'calendarDate')::TIMESTAMPTZ
                ORDER BY (payload->>'time5K')::integer asc
            ) as row_num
        FROM staging.api_imports
        WHERE task_id = v_task_id 
          AND (payload->>'time5K')::integer IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        prediction_5k_s = EXCLUDED.prediction_5k_s,
		prediction_10k_s = EXCLUDED.prediction_10k_s,
		prediction_half_marathon_s = EXCLUDED.prediction_half_marathon_s,
		prediction_marathon_s = EXCLUDED.prediction_marathon_s
	WHERE activities.race_predictions_raw.prediction_5k_s IS DISTINCT FROM EXCLUDED.prediction_5k_s OR
	activities.race_predictions_raw.prediction_10k_s IS DISTINCT FROM EXCLUDED.prediction_10k_s OR
	activities.race_predictions_raw.prediction_half_marathon_s IS DISTINCT FROM EXCLUDED.prediction_half_marathon_s OR
	activities.race_predictions_raw.prediction_marathon_s IS DISTINCT FROM EXCLUDED.prediction_marathon_s;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM activities.race_predictions_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Race Predictions'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: respiration_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.respiration_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.respiration_raw (  --############UPDATE TABLE NAME
        ts_utc,
		breaths_per_minute
    )
    SELECT
        ts_utc,
		breaths_per_minute
    FROM (
        SELECT 
            to_timestamp(round((array_element->>0)::bigint/1000)) AS ts_utc,
	        (ROUND((array_element->>1)::NUMERIC, 0))::SMALLINT AS breaths_per_minute,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((array_element->>0)::bigint/1000))
                ORDER BY (ROUND((array_element->>1)::NUMERIC, 0))::SMALLINT desc
            ) as row_num
        FROM staging.api_imports src
	         CROSS JOIN LATERAL jsonb_array_elements(
				    CASE
				        WHEN jsonb_typeof(src.payload->'respirationValuesArray') = 'array'
				        THEN src.payload->'respirationValuesArray'
				        ELSE '[]'::jsonb
				    END
				) AS array_element
        WHERE task_id = v_task_id 
          AND COALESCE((ROUND((array_element->>1)::NUMERIC, 0))::SMALLINT,0) > 5 -- IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        breaths_per_minute = EXCLUDED.breaths_per_minute
	WHERE health.respiration_raw.breaths_per_minute IS DISTINCT FROM EXCLUDED.breaths_per_minute;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.respiration_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Respiration'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: single_activity_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.single_activity_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

 --    INSERT INTO health.fitness_age_raw (  --############UPDATE TABLE NAME
 --        ts_utc,
	-- 		human_age,
	--         fitness_age_measured,
	-- 		fitness_age_potential
 --    )
 --    SELECT
 --        ts_utc,
	-- 		human_age,
	--         fitness_age_measured,
	-- 		fitness_age_potential
 --    FROM (
 --        SELECT 
 --            date_trunc('day', (payload->>'lastUpdated')::TIMESTAMPTZ) AS ts_utc,
	-- 		ROUND(
	-- 			    EXTRACT(YEAR FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) +
	-- 			    EXTRACT(MONTH FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) / 12.0 +
	-- 			    EXTRACT(DAY FROM AGE((payload->>'lastUpdated')::date, '1980-07-18'::DATE)) / 365.25,
	-- 			    2
	-- 			)  AS human_age,
	--         ROUND((payload->>'fitnessAge')::NUMERIC,2)::NUMERIC(4,2) AS fitness_age_measured,
	--         ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) AS fitness_age_potential,
 --            ROW_NUMBER() OVER (
 --                PARTITION BY date_trunc('day', (payload->>'lastUpdated')::TIMESTAMPTZ)
 --                ORDER BY ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) asc
 --            ) as row_num
 --        FROM staging.api_imports
 --        WHERE task_id = v_task_id 
 --          AND ROUND((payload->>'achievableFitnessAge')::NUMERIC,2)::NUMERIC(4,2) IS NOT NULL
 --    ) AS source_data 
 --    WHERE row_num = 1
 --    ON CONFLICT (ts_utc)  
	-- -- DO NOTHING;
 --    DO UPDATE SET
 --        human_age = EXCLUDED.human_age,
	-- 	fitness_age_measured = EXCLUDED.fitness_age_measured,
	-- 	fitness_age_potential = EXCLUDED.fitness_age_potential
	-- WHERE health.fitness_age_raw.human_age IS DISTINCT FROM EXCLUDED.human_age OR
	-- health.fitness_age_raw.fitness_age_measured IS DISTINCT FROM EXCLUDED.fitness_age_measured OR
	-- health.fitness_age_raw.fitness_age_potential IS DISTINCT FROM EXCLUDED.fitness_age_potential;  --############UPDATE TABLE NAME

 --    UPDATE api_services.function_library 
 --    SET value_recency = (SELECT MAX(ts_utc) FROM health.fitness_age_raw)  --############UPDATE TABLE NAME
	-- -- WHERE friendly_name = 'Fitness Age'
 --    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    -- DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: sleep_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.sleep_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN
    --------------------------------------------------------------------
    -- Sleep Metas
    --------------------------------------------------------------------
    INSERT INTO health.sleep_totals
    (sleep_end_date,
     sleep_start_utc,
     sleep_end_utc,
     sleep_score,
     heartrate_bpm,
     spo2,
     breaths_per_min,
     hrv_value,
     sleep_duration_s,
     rem_sleep_s,
     light_sleep_s,
     awake_sleep_s,
     deep_sleep_s,
     score_label
     )
    SELECT
        sleep_end_date,
        sleep_start_utc,
        sleep_end_utc,
        sleep_score,
        heartrate_bpm,
        spo2,
        breaths_per_min,
        hrv_value,
        sleep_duration_s,
        rem_sleep_s,
        light_sleep_s,
        awake_sleep_s,
        deep_sleep_s,
        score_label
    FROM (
             SELECT
                 (ai.payload->'dailySleepDTO'->>'calendarDate')::DATE as sleep_end_date,
                 date_trunc('second',to_timestamp((ai.payload->'dailySleepDTO' ->> 'sleepStartTimestampGMT')::bigint / 1000.0)::timestamptz) as sleep_start_utc,
                 date_trunc('second',to_timestamp((ai.payload->'dailySleepDTO' ->> 'sleepEndTimestampGMT')::bigint / 1000.0)::timestamptz) as sleep_end_utc,
                 ROUND((ai.payload->'dailySleepDTO'->'sleepScores'->'overall'->>'value')::NUMERIC,0)::INTEGER as sleep_score,
                 ROUND((ai.payload->'dailySleepDTO'->>'averageSpO2HRSleep')::NUMERIC,1) as heartrate_bpm,
                 ROUND((ai.payload->'dailySleepDTO'->>'averageSpO2Value')::NUMERIC,1) as spo2,
                 ROUND((ai.payload->'dailySleepDTO'->>'averageRespirationValue')::NUMERIC,1) as breaths_per_min,
                 ROUND((ai.payload->>'avgOvernightHrv')::NUMERIC,1) as hrv_value,
                 ROUND((ai.payload->'dailySleepDTO'->>'sleepTimeSeconds')::NUMERIC,0)::INTEGER as sleep_duration_s,
                 ROUND((ai.payload->'dailySleepDTO'->>'remSleepSeconds')::NUMERIC,0)::INTEGER as rem_sleep_s,
                 ROUND((ai.payload->'dailySleepDTO'->>'lightSleepSeconds')::NUMERIC,0)::INTEGER as light_sleep_s,
                 ROUND((ai.payload->'dailySleepDTO'->>'awakeSleepSeconds')::NUMERIC,0)::INTEGER as awake_sleep_s,
                 ROUND((ai.payload->'dailySleepDTO'->>'deepSleepSeconds')::NUMERIC,0)::INTEGER as deep_sleep_s,
                 (ai.payload->'dailySleepDTO'->'sleepScores'->'overall'->>'qualifierKey')::TEXT as score_label,
                 ROW_NUMBER() OVER
                     (PARTITION BY (ai.payload->'dailySleepDTO'->>'calendarDate')::DATE
                     ORDER BY ai.event_time_utc desc) as row_num
             FROM staging.api_imports ai
             WHERE task_id = 6
               AND ai.payload->'dailySleepDTO' IS NOT NULL
               AND (ai.payload->'dailySleepDTO'->>'calendarDate') IS NOT NULL
             AND date_trunc('second',to_timestamp((ai.payload->'dailySleepDTO' ->> 'sleepStartTimestampGMT')::bigint / 1000.0)::timestamptz) is NOT NULL
         ) AS source_data
    WHERE row_num = 1
        ON CONFLICT (sleep_start_utc)
    -- DO NOTHING;
    DO UPDATE SET
                  sleep_end_date = EXCLUDED.sleep_end_date,
                  sleep_end_utc = EXCLUDED.sleep_end_utc,
                  sleep_score = EXCLUDED.sleep_score,
                  heartrate_bpm = EXCLUDED.heartrate_bpm,
                  spo2 = EXCLUDED.spo2,
                  breaths_per_min = EXCLUDED.breaths_per_min,
                  hrv_value = EXCLUDED.hrv_value,
                  sleep_duration_s = EXCLUDED.sleep_duration_s,
                  rem_sleep_s = EXCLUDED.rem_sleep_s,
                  light_sleep_s = EXCLUDED.light_sleep_s,
                  awake_sleep_s = EXCLUDED.awake_sleep_s,
                  deep_sleep_s = EXCLUDED.deep_sleep_s,
                  score_label = EXCLUDED.score_label
       WHERE health.sleep_totals.sleep_end_utc IS DISTINCT FROM EXCLUDED.sleep_end_utc;

	--------------------------------------------------------------------
    -- HRV
    --------------------------------------------------------------------

    INSERT INTO health.hrv_raw (  --############UPDATE TABLE NAME
        ts_utc,
		hrv_value
    )
    SELECT
        ts_utc,
		hrv_value
    FROM (
        SELECT
            date_trunc('second',to_timestamp((elem ->> 'startGMT')::bigint / 1000.0)::timestamptz) as ts_utc,
			round((elem ->> 'value')::numeric, 0)::smallint as hrv_value,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('second',to_timestamp((elem ->> 'startGMT')::bigint / 1000.0)::timestamptz)
                ORDER BY round((elem ->> 'value')::numeric, 0)::smallint desc
            ) as row_num
        FROM staging.api_imports ai
		CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'hrvData') AS elem
        WHERE task_id = v_task_id
          AND COALESCE(round((elem ->> 'value')::numeric, 0)::smallint,0) >10
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        hrv_value = EXCLUDED.hrv_value
	WHERE health.hrv_raw.hrv_value IS DISTINCT FROM EXCLUDED.hrv_value;  --############UPDATE TABLE NAME


	--------------------------------------------------------------------
    -- HEART RATE
    --------------------------------------------------------------------
    INSERT INTO health.heartrate_raw (  --############UPDATE TABLE NAME
        ts_utc,
		heartrate_bpm
    )
    SELECT
        ts_utc,
		heartrate_bpm
    FROM (
        SELECT
            to_timestamp(round((elem ->> 'startGMT')::bigint/1000)) as ts_utc,
			round((elem ->> 'value')::numeric, 0)::smallint as heartrate_bpm,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((elem ->> 'startGMT')::bigint/1000))
                ORDER BY round((elem ->> 'value')::numeric, 0)::smallint desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'sleepHeartRate') AS elem
        WHERE task_id = v_task_id
          AND COALESCE(round((elem ->> 'value')::numeric, 0)::smallint,0) >10
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        heartrate_bpm = EXCLUDED.heartrate_bpm
	WHERE health.heartrate_raw.heartrate_bpm IS DISTINCT FROM EXCLUDED.heartrate_bpm;  --############UPDATE TABLE NAME

    --------------------------------------------------------------------
    -- STRESS
    --------------------------------------------------------------------
	INSERT INTO health.stress_raw (  --############UPDATE TABLE NAME
        ts_utc,
		stress_value
    )
    SELECT
        ts_utc,
		stress_value
    FROM (
        SELECT
            to_timestamp(round((elem ->> 'startGMT')::bigint/1000)) as ts_utc,
			((elem ->> 'value')::numeric)::smallint as stress_value,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((elem ->> 'startGMT')::bigint/1000))
                ORDER BY ((elem ->> 'value')::numeric)::smallint desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'sleepStress') AS elem
        WHERE task_id = v_task_id
          AND COALESCE(((elem ->> 'value')::numeric)::smallint,0) >5
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        stress_value = EXCLUDED.stress_value
	WHERE health.stress_raw.stress_value IS DISTINCT FROM EXCLUDED.stress_value;  --############UPDATE TABLE NAME

    --------------------------------------------------------------------
    -- MOVEMENT
    --------------------------------------------------------------------
	INSERT INTO health.sleep_movement_raw (  --############UPDATE TABLE NAME
        ts_utc,
		sleep_movement
    )
    SELECT
        ts_utc,
		sleep_movement
    FROM (
        SELECT
            date_trunc('second', ((elem ->> 'startGMT')::TIMESTAMP at TIME ZONE 'UTC')::timestamptz) as ts_utc,
			(elem ->> 'activityLevel')::numeric(5,2) as sleep_movement,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('second', ((elem ->> 'startGMT')::TIMESTAMP at TIME ZONE 'UTC')::timestamptz)
                ORDER BY (elem ->> 'activityLevel')::numeric(5,2) desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'sleepStress') AS elem
        WHERE task_id = v_task_id
          AND (elem ->> 'activityLevel')::numeric(5,2) IS NOT NULL
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        sleep_movement = EXCLUDED.sleep_movement
	WHERE health.sleep_movement_raw.sleep_movement IS DISTINCT FROM EXCLUDED.sleep_movement;

	--------------------------------------------------------------------
    -- BODY BATTERY
    --------------------------------------------------------------------
	INSERT INTO health.body_battery_raw (  --############UPDATE TABLE NAME
        ts_utc,
		body_battery_value
    )
    SELECT
        ts_utc,
		body_battery_value
    FROM (
        SELECT
            to_timestamp(round((elem ->> 'startGMT')::bigint/1000)) as ts_utc,
			(elem ->> 'value')::smallint as body_battery_value,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((elem ->> 'startGMT')::bigint/1000))
                ORDER BY (elem ->> 'value')::smallint desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'sleepBodyBattery') AS elem
        WHERE task_id = v_task_id
          AND COALESCE((elem ->> 'value')::smallint,0) > 0
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        body_battery_value = EXCLUDED.body_battery_value
	WHERE health.body_battery_raw.body_battery_value IS DISTINCT FROM EXCLUDED.body_battery_value;


    --------------------------------------------------------------------
    -- RESPIRATION
    --------------------------------------------------------------------
	INSERT INTO health.respiration_raw (  --############UPDATE TABLE NAME
        ts_utc,
		breaths_per_minute
    )
    SELECT
        ts_utc,
		breaths_per_minute
    FROM (
        SELECT
            to_timestamp(round((elem ->> 'startTimeGMT')::bigint / 1000.0))as ts_utc,
			round((elem ->> 'respirationValue')::numeric, 0)::smallint as breaths_per_minute,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((elem ->> 'startTimeGMT')::bigint / 1000.0))
                ORDER BY round((elem ->> 'respirationValue')::numeric, 0)::smallint desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'wellnessEpochRespirationDataDTOList') AS elem
        WHERE task_id = v_task_id
          AND COALESCE(round((elem ->> 'respirationValue')::numeric, 0)::smallint,0) > 5
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        breaths_per_minute = EXCLUDED.breaths_per_minute
	WHERE health.respiration_raw.breaths_per_minute IS DISTINCT FROM EXCLUDED.breaths_per_minute;


	--------------------------------------------------------------------
    -- SPO2
    --------------------------------------------------------------------
	INSERT INTO health.spo2_raw (  --############UPDATE TABLE NAME
        ts_utc,
		spo2_reading,
		spo2_confidence
    )
    SELECT
        ts_utc,
		spo2_reading,
		spo2_confidence
    FROM (
        SELECT
            date_trunc('second', ((elem ->> 'epochTimestamp')::TIMESTAMP at TIME ZONE 'UTC')::timestamptz) as ts_utc,
            (elem ->> 'spo2Reading')::SMALLINT as spo2_reading,
            round((elem ->> 'readingConfidence')::numeric, 0)::smallint as spo2_confidence,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('second', ((elem ->> 'epochTimestamp')::TIMESTAMP at TIME ZONE 'UTC')::timestamptz)
                ORDER BY (elem ->> 'spo2Reading')::SMALLINT desc
            ) as row_num
        FROM staging.api_imports ai
        CROSS JOIN LATERAL jsonb_array_elements(ai.payload -> 'wellnessEpochSPO2DataDTOList') AS elem
        WHERE task_id = v_task_id
          AND COALESCE((elem ->> 'spo2Reading')::SMALLINT, 0) > 50
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        spo2_reading = EXCLUDED.spo2_reading,
		spo2_confidence = EXCLUDED.spo2_confidence
	WHERE health.spo2_raw.spo2_reading IS DISTINCT FROM EXCLUDED.spo2_reading OR
		health.spo2_raw.spo2_confidence IS DISTINCT FROM EXCLUDED.spo2_confidence;

	--------------------------------------------------------------------
    -- Recency Update
    --------------------------------------------------------------------
    UPDATE api_services.function_library
    SET value_recency = (SELECT MAX(sleep_end_utc) FROM health.sleep_totals)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Sleep'
    WHERE friendly_name = v_friendly_name;

    UPDATE health.heartrate_raw SET activity_label = 'asleep'
    FROM health.sleep_totals st
    WHERE health.heartrate_raw.ts_utc between st.sleep_start_utc and st.sleep_end_utc and health.heartrate_raw.activity_label != 'asleep';


    CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: steps_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.steps_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO activities.steps_raw (  --############UPDATE TABLE NAME
        ts_utc,
		step_goal,
		total_steps,
		total_distance_m
    )
    SELECT
        ts_utc,
		step_goal,
		total_steps,
		total_distance_m
    FROM (
        SELECT 
            (payload->>'calendarDate')::TIMESTAMPTZ AS ts_utc,
	        (payload->>'stepGoal')::integer AS step_goal,
	        (payload->>'totalSteps')::integer AS total_steps,
	        (payload->>'totalDistance')::integer AS total_distance_m,
            ROW_NUMBER() OVER (
                PARTITION BY (payload->>'calendarDate')::TIMESTAMPTZ
                ORDER BY (payload->>'totalSteps')::integer DESC
            ) as row_num
        FROM staging.api_imports
        WHERE task_id = v_task_id 
          AND (payload->>'totalSteps')::integer IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        step_goal = EXCLUDED.step_goal,
		total_steps = EXCLUDED.total_steps,
		total_distance_m = EXCLUDED.total_distance_m 	
	WHERE activities.steps_raw.step_goal IS DISTINCT FROM EXCLUDED.step_goal OR
	activities.steps_raw.total_steps IS DISTINCT FROM EXCLUDED.total_steps OR
	activities.steps_raw.total_distance_m IS DISTINCT FROM EXCLUDED.total_distance_m;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM activities.steps_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Steps'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: stress_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.stress_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.stress_raw (  --############UPDATE TABLE NAME
        ts_utc,
		stress_value
    )
    SELECT
        ts_utc,
		stress_value
    FROM (
        SELECT 
            to_timestamp(round((array_element->>0)::bigint/1000)) AS ts_utc,
	        (array_element->>1)::integer AS stress_value,
            ROW_NUMBER() OVER (
                PARTITION BY to_timestamp(round((array_element->>0)::bigint/1000))
                ORDER BY (array_element->>1)::integer desc
            ) as row_num
        FROM staging.api_imports,
	         jsonb_array_elements(payload->'stressValuesArray') AS array_element
        WHERE task_id = v_task_id 
          AND COALESCE((array_element->>1)::integer,0) > 10 -- IS NOT NULL
    ) AS source_data 
    WHERE row_num = 1
    ON CONFLICT (ts_utc)  
	-- DO NOTHING;
    DO UPDATE SET
        stress_value = EXCLUDED.stress_value
	WHERE health.stress_raw.stress_value IS DISTINCT FROM EXCLUDED.stress_value;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library 
    SET value_recency = (SELECT MAX(ts_utc) FROM health.stress_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Stress'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: track_id_search_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.track_id_search_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN
	--------------------------------------------------------------------
    -- HRV
    --------------------------------------------------------------------
	
    INSERT INTO staging.stg_listen_history (  --############UPDATE TABLE NAME
        track_id,
		track_name,
		album_id,
		album_name,
		artist_id,
		artist_name,
		popularity,
		duration_ms,
		incoming_isrc,
		available_market_count,
		available_in_usa,
		context_type,
		context_uri,
		played_at_utc,
		track_release,
		track_release_precision
    )
    SELECT
        track_id,
		track_name,
		album_id,
		album_name,
		artist_id,
		artist_name,
		popularity,
		duration_ms,
		incoming_isrc,
		available_market_count,
		available_in_usa,
		context_type,
		context_uri,
		played_at_utc,
		track_release,
		track_release_precision
    FROM (
		SELECT
		    item->>'id'                                      AS track_id,
		    item->>'name'                                    AS track_name,		
		    item->'album'->>'id'                              AS album_id,
		    item->'album'->>'name'                            AS album_name,		
		    item->'album'->'artists'->0->>'id'                AS artist_id,
		    item->'album'->'artists'->0->>'name'              AS artist_name,		
		    (item->>'popularity')::int                        AS popularity,
		    (item->>'duration_ms')::int                       AS duration_ms,		
		    item->'external_ids'->>'isrc'                     AS incoming_isrc,		
		    jsonb_array_length(item->'album'->'available_markets')
		                                                       AS available_market_count,
		
		    'US' = ANY (
		        ARRAY (
		            SELECT jsonb_array_elements_text(
		                item->'album'->'available_markets'
		            )
		        )
		    )                                                 AS available_in_usa,
			NULL as context_type,
			NULL as context_uri,
			'1980-07-18'::TIMESTAMPTZ as played_at_utc,
		    item->'album'->>'release_date'                    AS track_release,
		    item->'album'->>'release_date_precision'          AS track_release_precision,
		
		    ROW_NUMBER() OVER (
		        PARTITION BY
		            item->>'id',
		            item->'external_ids'->>'isrc'
		        ORDER BY
		            event_time_utc DESC
		    ) AS row_num
		
		FROM staging.api_imports src
		CROSS JOIN LATERAL jsonb_array_elements(
		    CASE
		        WHEN jsonb_typeof(src.payload) = 'array'
		        THEN src.payload
		        ELSE '[]'::jsonb
		    END
		) AS item
		WHERE task_id = v_task_id
		) subquery
	where row_num = 1
    ON CONFLICT (track_id, played_at_utc)
        DO UPDATE SET
            track_name = EXCLUDED.track_name,
            album_id = EXCLUDED.album_id,
            album_name = EXCLUDED.album_name,
            artist_id = EXCLUDED.artist_id,
            artist_name = EXCLUDED.artist_name,
            popularity = EXCLUDED.popularity,
            duration_ms = EXCLUDED.duration_ms,
            incoming_isrc = EXCLUDED.incoming_isrc,
            available_market_count = EXCLUDED.available_market_count,
            available_in_usa = EXCLUDED.available_in_usa,
            context_type = EXCLUDED.context_type,
            context_uri = EXCLUDED.context_uri;  --############UPDATE TABLE NAME


		INSERT INTO music.all_tracks (
		track_id,
		track_name,
		track_name_clean,
		track_name_search,
		track_isrc,
		artist_id,
		artist_display_name,
		album_id,
		release_date,
		available_in_us,
		total_markets,
		popularity,
		duration_ms,
		updated_at_utc
		)
		SELECT DISTINCT
			track_id,
			track_name,
			track_name_clean,
			track_name_search,
			track_isrc,
			artist_id,
			artist_display_name,
			album_id,
			release_date,
			available_in_us,
			total_markets,
			popularity,
			duration_ms,
			updated_at_utc
			FROM staging.vw_track_inserts_from_listen_history d
		ON CONFLICT (track_id)  -- assuming epoch_ts is your primary key
	    DO UPDATE SET
	        track_name_clean = EXCLUDED.track_name_clean,
			track_name_search = EXCLUDED.track_name_search,
			release_date = EXCLUDED.release_date,
			available_in_us = EXCLUDED.available_in_us,
			total_markets = EXCLUDED.total_markets,
			popularity = EXCLUDED.popularity,
			duration_ms = EXCLUDED.duration_ms,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE ((music.all_tracks.track_name_clean IS DISTINCT FROM EXCLUDED.track_name_clean OR
			music.all_tracks.track_name_search IS DISTINCT FROM EXCLUDED.track_name_search OR
			music.all_tracks.release_date IS DISTINCT FROM EXCLUDED.release_date OR
			music.all_tracks.available_in_us IS DISTINCT FROM EXCLUDED.available_in_us OR
			music.all_tracks.total_markets IS DISTINCT FROM EXCLUDED.total_markets OR
			music.all_tracks.popularity IS DISTINCT FROM EXCLUDED.popularity OR
			music.all_tracks.duration_ms IS DISTINCT FROM EXCLUDED.duration_ms) AND 
			music.all_tracks.updated_at_utc < EXCLUDED.updated_at_utc);

		INSERT INTO music.all_artists (
				artist_id,
				artist_name,
				artist_name_search,
				updated_at_utc
		)
		SELECT DISTINCT
			artist_id,
			artist_name,
			artist_name_search,
			updated_at_utc
		FROM staging.vw_artist_inserts
		ON CONFLICT (artist_id)
		DO UPDATE SET 
			artist_name = EXCLUDED.artist_name,
			artist_name_search = EXCLUDED.artist_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_artists.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_artists.artist_name IS DISTINCT FROM EXCLUDED.artist_name OR
			music.all_artists.artist_name_search IS DISTINCT FROM EXCLUDED.artist_name_search));

		INSERT INTO music.all_albums (
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		)
		SELECT DISTINCT
			album_id,
			album_name,
			album_name_clean,
			album_name_search,
			updated_at_utc
		FROM staging.vw_album_inserts
		ON CONFLICT (album_id)
		DO UPDATE SET 
			album_name = EXCLUDED.album_name,
			album_name_clean = EXCLUDED.album_name_clean,
			album_name_search = EXCLUDED.album_name_search,
			updated_at_utc = EXCLUDED.updated_at_utc
		WHERE (music.all_albums.updated_at_utc < EXCLUDED.updated_at_utc AND 
			(music.all_albums.album_name IS DISTINCT FROM EXCLUDED.album_name OR
			music.all_albums.album_name_clean IS DISTINCT FROM EXCLUDED.album_name_clean OR 
			music.all_albums.album_name_search IS DISTINCT FROM EXCLUDED.album_name_search));

	
	--------------------------------------------------------------------
    -- Recency Update
    --------------------------------------------------------------------	
    UPDATE api_services.function_library 
    SET value_recency = CURRENT_TIMESTAMP --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Recent Plays'
    WHERE friendly_name = v_friendly_name;
	
	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;

	TRUNCATE staging.stg_listen_history;
END;
$$;


--
-- Name: training_status_flatten(integer, text); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.training_status_flatten(IN v_task_id integer, IN v_friendly_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE

BEGIN

    INSERT INTO health.training_status_raw (  --############UPDATE TABLE NAME
        ts_utc,
		vo2_max_value,
		altitude_current_m,
		altitude_acclimation_m,
		altitude_acclimation_pct,
		heat_acclimation_pct,
		training_load_acute,
		training_load_min,
		training_load_max
    )
    SELECT
        ts_utc,
		vo2_max_value,
		altitude_current_m,
		altitude_acclimation_m,
		altitude_acclimation_pct,
		heat_acclimation_pct,
		training_load_acute,
		training_load_min,
		training_load_max
    FROM (
        SELECT
            (payload->'mostRecentTrainingStatus'->>'lastPrimarySyncDate')::DATE::TIMESTAMPTZ AS ts_utc,
			ROUND((payload->'mostRecentVO2Max'->'generic'->>'vo2MaxPreciseValue')::NUMERIC, 1)::NUMERIC(4,1) as vo2_max_value,
			ROUND((payload->'mostRecentVO2Max'->'heatAltitudeAcclimation'->>'currentAltitude')::NUMERIC ,0)::INTEGER as altitude_current_m,
			ROUND((payload->'mostRecentVO2Max'->'heatAltitudeAcclimation'->>'altitudeAcclimation')::NUMERIC ,0)::INTEGER as altitude_acclimation_m,
			ROUND((payload->'mostRecentVO2Max'->'heatAltitudeAcclimation'->>'acclimationPercentage')::NUMERIC/100, 0) as altitude_acclimation_pct,
			ROUND((payload->'mostRecentVO2Max'->'heatAltitudeAcclimation'->>'heatAcclimationPercentage')::NUMERIC/100, 0) as heat_acclimation_pct,
			ROUND(
				(payload->'mostRecentTrainingStatus'->'latestTrainingStatusData'->
				(payload->'mostRecentTrainingStatus'->'recordedDevices'->0->> 'deviceId')
				->'acuteTrainingLoadDTO'->>'dailyTrainingLoadAcute')::NUMERIC,1)::NUMERIC(5,1) as training_load_acute,
			ROUND(
				(payload->'mostRecentTrainingStatus'->'latestTrainingStatusData'->
				(payload->'mostRecentTrainingStatus'->'recordedDevices'->0->> 'deviceId')
				->'acuteTrainingLoadDTO'->>'minTrainingLoadChronic')::NUMERIC,1)::NUMERIC(5,1) as training_load_min,
			ROUND(
				(payload->'mostRecentTrainingStatus'->'latestTrainingStatusData'->
				(payload->'mostRecentTrainingStatus'->'recordedDevices'->0->> 'deviceId')
				->'acuteTrainingLoadDTO'->>'maxTrainingLoadChronic')::NUMERIC,1)::NUMERIC(5,1) as training_load_max,
            ROW_NUMBER() OVER (
                PARTITION BY (payload->'mostRecentTrainingStatus'->>'lastPrimarySyncDate')::TIMESTAMPTZ
                ORDER BY ROUND((payload->'mostRecentVO2Max'->'generic'->>'vo2MaxValue')::NUMERIC, 1)::NUMERIC(4,1) desc
            ) as row_num
        FROM staging.api_imports
        WHERE task_id = v_task_id
          AND ROUND(
				(payload->'mostRecentTrainingStatus'->'latestTrainingStatusData'->
				(payload->'mostRecentTrainingStatus'->'recordedDevices'->0->> 'deviceId')
				->'acuteTrainingLoadDTO'->>'dailyTrainingLoadAcute')::NUMERIC,1)::NUMERIC(5,1) IS NOT NULL
        AND (payload->'mostRecentTrainingStatus'->>'lastPrimarySyncDate')::DATE::TIMESTAMPTZ IS NOT NULL
    ) AS source_data
    WHERE row_num = 1
    ON CONFLICT (ts_utc)
	-- DO NOTHING;
    DO UPDATE SET
        ts_utc = EXCLUDED.ts_utc,
		vo2_max_value = EXCLUDED.vo2_max_value,
		altitude_current_m = EXCLUDED.altitude_current_m,
		altitude_acclimation_m = EXCLUDED.altitude_acclimation_m,
		altitude_acclimation_pct = EXCLUDED.altitude_acclimation_pct,
		heat_acclimation_pct = EXCLUDED.heat_acclimation_pct,
		training_load_acute = EXCLUDED.training_load_acute,
		training_load_min = EXCLUDED.training_load_min,
		training_load_max = EXCLUDED.training_load_max
	WHERE health.training_status_raw.vo2_max_value IS DISTINCT FROM EXCLUDED.vo2_max_value OR
	health.training_status_raw.altitude_current_m IS DISTINCT FROM EXCLUDED.altitude_current_m OR
	health.training_status_raw.altitude_acclimation_m IS DISTINCT FROM EXCLUDED.altitude_acclimation_m OR
	health.training_status_raw.altitude_acclimation_pct IS DISTINCT FROM EXCLUDED.altitude_acclimation_pct OR
	health.training_status_raw.heat_acclimation_pct IS DISTINCT FROM EXCLUDED.heat_acclimation_pct OR
	health.training_status_raw.training_load_acute IS DISTINCT FROM EXCLUDED.training_load_acute OR
	health.training_status_raw.training_load_min IS DISTINCT FROM EXCLUDED.training_load_min OR
	health.training_status_raw.training_load_max IS DISTINCT FROM EXCLUDED.training_load_max;  --############UPDATE TABLE NAME

    UPDATE api_services.function_library
    SET value_recency = (SELECT MAX(ts_utc) FROM health.training_status_raw)  --############UPDATE TABLE NAME
	-- WHERE friendly_name = 'Training Status'
    WHERE friendly_name = v_friendly_name;

	CALL staging.json_sampling(v_task_id, v_friendly_name);

    DELETE FROM staging.api_imports WHERE task_id = v_task_id;
END;
$$;


--
-- Name: update_segment_details(bigint); Type: PROCEDURE; Schema: staging; Owner: -
--

CREATE PROCEDURE staging.update_segment_details(IN p_activity_id bigint)
    LANGUAGE plpgsql
    AS $$

BEGIN
    INSERT INTO activities.segments_details
        (segment_id,
         activity_id,
         activity_start_point,
         activity_end_point,
         start_time_utc,
         distance_m,
         elapsed_duration_s,
         max_hr,
         avg_hr,
         avg_cadence,
         avg_temp,
         avg_vert_osc,
         avg_vert_ratio,
         avg_gct,
         avg_perf,
         avg_balance,
         avg_stride_length,
         segment_path)

    SELECT
        sm.segment_id,
        add.activity_id,
        sm.activity_start_point,
        sm.activity_end_point,
        a.start_time_utc +
        min((add.elapsed_duration_ms / 1000) || ' seconds'::text)::interval AS start_time_utc,
        max(add.distance_m) - min(add.distance_m) AS distance_m,
        round(((max(add.elapsed_duration_ms) - min(add.elapsed_duration_ms)) / 1000.0)::numeric,
        2)                                                                                  AS elapsed_duration_s,
        max(add.heartrate_bpm)                                                                    AS max_hr,
        round(avg(add.heartrate_bpm),0) AS avg_hr,
        round(avg(add.cadence_spm),0)                                                                      AS avg_cadence,
        round(avg(c2f(add.air_temp_c::numeric)) ,1)                                                        AS avg_temp,
        round(avg(add.vert_oscillation_cm),1)                                                      AS avg_vert_osc,
        round(avg(add.vert_ratio),1)                                                            AS avg_vert_ratio,
        round(avg(add.ground_contact_time_ms),0)                                                           AS avg_gct,
        round(avg(add.performance_condition),1)                                                            AS avg_perf,
        50-round(avg(add.ground_contact_balance_left),1) as avg_balance,
        round(avg(add.stride_length_cm),1)                                                                 AS avg_stride_length,
        ST_SetSRID(
                ST_MakeLine(
                        ST_MakePoint(longitude, latitude, elevation_m_smooth, add.distance_m)
                        ORDER BY add.distance_m ASC
                ),
                4326
        ) as segment_path
    FROM activities.segment_matches sm
    JOIN activities.segments s ON sm.segment_id = s.segment_id
    JOIN activities.activity_details_distance add ON sm.activity_id = add.activity_id
    JOIN activities.activities a
                             ON a.activity_id = add.activity_id AND add.distance_m >= sm.activity_start_point AND
    add.distance_m <= sm.activity_end_point
--     WHERE a.activity_id=22127682969
    WHERE p_activity_id IS NULL or p_activity_id = add.activity_id
    GROUP BY sm.segment_id, s.is_course, add.activity_id, a.start_time_utc, s.segment_name, sm.activity_start_point,
    sm.activity_end_point
    ON CONFLICT (segment_id, activity_id, activity_start_point, activity_end_point) DO UPDATE SET
        start_time_utc = excluded.start_time_utc,
        distance_m = EXCLUDED.distance_m,
        elapsed_duration_s = EXCLUDED.elapsed_duration_s,
        max_hr = EXCLUDED.max_hr,
        avg_hr = EXCLUDED.avg_hr,
        avg_cadence = EXCLUDED.avg_cadence,
        avg_temp = EXCLUDED.avg_temp,
        avg_vert_osc = EXCLUDED.avg_vert_osc,
        avg_vert_ratio = EXCLUDED.avg_vert_ratio,
        avg_gct = EXCLUDED.avg_gct,
        avg_perf = EXCLUDED.avg_perf,
        avg_balance = EXCLUDED.avg_balance,
        avg_stride_length = EXCLUDED.avg_stride_length,
        segment_path = EXCLUDED.segment_path;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: activities; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.activities (
    activity_id bigint NOT NULL,
    activity_name text,
    activity_type_id smallint,
    activity_type_name text,
    distance_m numeric(8,2),
    duration_s numeric(8,2),
    duration_elapsed_s numeric(8,2),
    duration_moving_s integer,
    calories integer,
    calories_bmr integer,
    steps integer,
    stride_length_cm_avg integer,
    cadence_double_spm_max integer,
    cadence_running_spm_max integer,
    cadence_running_spm_avg numeric(4,1),
    speed_mps_max numeric(5,2),
    speed_mps_avg numeric(5,2),
    heartrate_max integer,
    heartrate_avg integer,
    start_timestamp_utc bigint,
    start_time_utc timestamp with time zone,
    end_time_utc timestamp with time zone,
    latitude_start numeric(18,15),
    latitude_end numeric(18,15),
    longitude_start numeric(18,15),
    longitude_end numeric(18,15),
    elevation_m_min numeric(10,2),
    elevation_m_max numeric(10,2),
    elevation_m_gain numeric(10,2),
    elevation_m_loss numeric(10,2),
    temperature_c_min smallint,
    temperature_c_max smallint,
    water_ml_estimated integer,
    training_load numeric(10,2),
    training_effect_aerobic numeric(10,2),
    training_effect_anerobic numeric(10,2),
    intensity_minutes_moderate integer,
    intensity_minutes_vigorous integer,
    is_downloaded boolean DEFAULT false,
    activity_path public.geometry(LineStringZM,4326)
);


--
-- Name: TABLE activities; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.activities IS 'Table activities.';


--
-- Name: COLUMN activities.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activities.activity_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.activity_name IS 'Column activity_name.';


--
-- Name: COLUMN activities.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN activities.activity_type_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.activity_type_name IS 'Column activity_type_name.';


--
-- Name: COLUMN activities.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN activities.duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.duration_s IS 'Column duration_s.';


--
-- Name: COLUMN activities.duration_elapsed_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.duration_elapsed_s IS 'Column duration_elapsed_s.';


--
-- Name: COLUMN activities.duration_moving_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.duration_moving_s IS 'Column duration_moving_s.';


--
-- Name: COLUMN activities.calories; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.calories IS 'Column calories.';


--
-- Name: COLUMN activities.calories_bmr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.calories_bmr IS 'Column calories_bmr.';


--
-- Name: COLUMN activities.steps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.steps IS 'Column steps.';


--
-- Name: COLUMN activities.stride_length_cm_avg; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.stride_length_cm_avg IS 'Column stride_length_cm_avg.';


--
-- Name: COLUMN activities.cadence_double_spm_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.cadence_double_spm_max IS 'Column cadence_double_spm_max.';


--
-- Name: COLUMN activities.cadence_running_spm_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.cadence_running_spm_max IS 'Column cadence_running_spm_max.';


--
-- Name: COLUMN activities.cadence_running_spm_avg; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.cadence_running_spm_avg IS 'Column cadence_running_spm_avg.';


--
-- Name: COLUMN activities.speed_mps_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.speed_mps_max IS 'Column speed_mps_max.';


--
-- Name: COLUMN activities.speed_mps_avg; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.speed_mps_avg IS 'Column speed_mps_avg.';


--
-- Name: COLUMN activities.heartrate_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.heartrate_max IS 'Column heartrate_max.';


--
-- Name: COLUMN activities.heartrate_avg; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.heartrate_avg IS 'Column heartrate_avg.';


--
-- Name: COLUMN activities.start_timestamp_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.start_timestamp_utc IS 'Column start_timestamp_utc.';


--
-- Name: COLUMN activities.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN activities.end_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.end_time_utc IS 'Column end_time_utc.';


--
-- Name: COLUMN activities.latitude_start; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.latitude_start IS 'Column latitude_start.';


--
-- Name: COLUMN activities.latitude_end; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.latitude_end IS 'Column latitude_end.';


--
-- Name: COLUMN activities.longitude_start; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.longitude_start IS 'Column longitude_start.';


--
-- Name: COLUMN activities.longitude_end; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.longitude_end IS 'Column longitude_end.';


--
-- Name: COLUMN activities.elevation_m_min; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.elevation_m_min IS 'Column elevation_m_min.';


--
-- Name: COLUMN activities.elevation_m_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.elevation_m_max IS 'Column elevation_m_max.';


--
-- Name: COLUMN activities.elevation_m_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.elevation_m_gain IS 'Column elevation_m_gain.';


--
-- Name: COLUMN activities.elevation_m_loss; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.elevation_m_loss IS 'Column elevation_m_loss.';


--
-- Name: COLUMN activities.temperature_c_min; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.temperature_c_min IS 'Column temperature_c_min.';


--
-- Name: COLUMN activities.temperature_c_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.temperature_c_max IS 'Column temperature_c_max.';


--
-- Name: COLUMN activities.water_ml_estimated; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.water_ml_estimated IS 'Column water_ml_estimated.';


--
-- Name: COLUMN activities.training_load; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.training_load IS 'Column training_load.';


--
-- Name: COLUMN activities.training_effect_aerobic; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.training_effect_aerobic IS 'Column training_effect_aerobic.';


--
-- Name: COLUMN activities.training_effect_anerobic; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.training_effect_anerobic IS 'Column training_effect_anerobic.';


--
-- Name: COLUMN activities.intensity_minutes_moderate; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.intensity_minutes_moderate IS 'Column intensity_minutes_moderate.';


--
-- Name: COLUMN activities.intensity_minutes_vigorous; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.intensity_minutes_vigorous IS 'Column intensity_minutes_vigorous.';


--
-- Name: COLUMN activities.is_downloaded; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.is_downloaded IS 'Column is_downloaded.';


--
-- Name: COLUMN activities.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activities.activity_path IS 'Column activity_path.';


--
-- Name: activity_details; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.activity_details (
    activity_id bigint CONSTRAINT activity_details_activity_id_not_null1 NOT NULL,
    ts_utc timestamp with time zone NOT NULL,
    moving_duration_s integer,
    elapsed_duration_s integer,
    duration_s integer,
    distance_mm bigint,
    latitude numeric(9,6),
    longitude numeric(9,6),
    heartrate_bpm smallint,
    cadence_spm smallint,
    air_temp_c smallint,
    elevation_m numeric(7,1),
    speed_mmps bigint,
    vertical_speed_mps numeric(5,1),
    vert_oscillation_cm numeric(5,2),
    stride_length_cm numeric(4,1),
    vert_ratio numeric(4,2),
    ground_contact_time_ms smallint,
    ground_contact_balance_left numeric(4,2),
    performance_condition smallint,
    activity_type_id smallint,
    elevation_m_smooth numeric(10,2),
    elevation_reference numeric(10,2),
    loc_point public.geography(Point,4326),
    grid_5m_00_id public.geometry(Point,4326) GENERATED ALWAYS AS (public.st_snaptogrid((loc_point)::public.geometry, (0.000045)::double precision)) STORED
);


--
-- Name: TABLE activity_details; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.activity_details IS 'Table activity_details.';


--
-- Name: COLUMN activity_details.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_details.ts_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN activity_details.moving_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.moving_duration_s IS 'Column moving_duration_s.';


--
-- Name: COLUMN activity_details.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN activity_details.duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.duration_s IS 'Column duration_s.';


--
-- Name: COLUMN activity_details.distance_mm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.distance_mm IS 'Column distance_mm.';


--
-- Name: COLUMN activity_details.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.latitude IS 'Column latitude.';


--
-- Name: COLUMN activity_details.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.longitude IS 'Column longitude.';


--
-- Name: COLUMN activity_details.heartrate_bpm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN activity_details.cadence_spm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.cadence_spm IS 'Column cadence_spm.';


--
-- Name: COLUMN activity_details.air_temp_c; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.air_temp_c IS 'Column air_temp_c.';


--
-- Name: COLUMN activity_details.elevation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN activity_details.speed_mmps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.speed_mmps IS 'Column speed_mmps.';


--
-- Name: COLUMN activity_details.vertical_speed_mps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.vertical_speed_mps IS 'Column vertical_speed_mps.';


--
-- Name: COLUMN activity_details.vert_oscillation_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.vert_oscillation_cm IS 'Column vert_oscillation_cm.';


--
-- Name: COLUMN activity_details.stride_length_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.stride_length_cm IS 'Column stride_length_cm.';


--
-- Name: COLUMN activity_details.vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.vert_ratio IS 'Column vert_ratio.';


--
-- Name: COLUMN activity_details.ground_contact_time_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.ground_contact_time_ms IS 'Column ground_contact_time_ms.';


--
-- Name: COLUMN activity_details.ground_contact_balance_left; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.ground_contact_balance_left IS 'Column ground_contact_balance_left.';


--
-- Name: COLUMN activity_details.performance_condition; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.performance_condition IS 'Column performance_condition.';


--
-- Name: COLUMN activity_details.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN activity_details.elevation_m_smooth; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: COLUMN activity_details.elevation_reference; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.elevation_reference IS 'Column elevation_reference.';


--
-- Name: COLUMN activity_details.loc_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN activity_details.grid_5m_00_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details.grid_5m_00_id IS 'Column grid_5m_00_id.';


--
-- Name: activity_details_distance; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.activity_details_distance (
    activity_type_id smallint,
    activity_id bigint NOT NULL,
    distance_m integer NOT NULL,
    elapsed_duration_ms integer,
    latitude numeric(9,6),
    longitude numeric(9,6),
    heartrate_bpm smallint,
    cadence_spm smallint,
    air_temp_c smallint,
    elevation_m numeric(7,2),
    speed_mmps bigint,
    vertical_speed_mps numeric(5,1),
    vert_oscillation_cm numeric(5,2),
    stride_length_cm numeric(4,1),
    vert_ratio numeric(4,2),
    ground_contact_time_ms smallint,
    ground_contact_balance_left numeric(4,2),
    performance_condition smallint,
    elevation_m_smooth numeric(10,2),
    elevation_reference numeric(10,2),
    loc_point public.geography(Point,4326),
    grid_5m_00_id public.geometry(Point,4326) GENERATED ALWAYS AS (public.st_snaptogrid((loc_point)::public.geometry, (0.000045)::double precision)) STORED
);


--
-- Name: TABLE activity_details_distance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.activity_details_distance IS 'Table activity_details_distance.';


--
-- Name: COLUMN activity_details_distance.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN activity_details_distance.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_details_distance.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN activity_details_distance.elapsed_duration_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.elapsed_duration_ms IS 'Column elapsed_duration_ms.';


--
-- Name: COLUMN activity_details_distance.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.latitude IS 'Column latitude.';


--
-- Name: COLUMN activity_details_distance.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.longitude IS 'Column longitude.';


--
-- Name: COLUMN activity_details_distance.heartrate_bpm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN activity_details_distance.cadence_spm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.cadence_spm IS 'Column cadence_spm.';


--
-- Name: COLUMN activity_details_distance.air_temp_c; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.air_temp_c IS 'Column air_temp_c.';


--
-- Name: COLUMN activity_details_distance.elevation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN activity_details_distance.speed_mmps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.speed_mmps IS 'Column speed_mmps.';


--
-- Name: COLUMN activity_details_distance.vertical_speed_mps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.vertical_speed_mps IS 'Column vertical_speed_mps.';


--
-- Name: COLUMN activity_details_distance.vert_oscillation_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.vert_oscillation_cm IS 'Column vert_oscillation_cm.';


--
-- Name: COLUMN activity_details_distance.stride_length_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.stride_length_cm IS 'Column stride_length_cm.';


--
-- Name: COLUMN activity_details_distance.vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.vert_ratio IS 'Column vert_ratio.';


--
-- Name: COLUMN activity_details_distance.ground_contact_time_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.ground_contact_time_ms IS 'Column ground_contact_time_ms.';


--
-- Name: COLUMN activity_details_distance.ground_contact_balance_left; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.ground_contact_balance_left IS 'Column ground_contact_balance_left.';


--
-- Name: COLUMN activity_details_distance.performance_condition; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.performance_condition IS 'Column performance_condition.';


--
-- Name: COLUMN activity_details_distance.elevation_m_smooth; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: COLUMN activity_details_distance.elevation_reference; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.elevation_reference IS 'Column elevation_reference.';


--
-- Name: COLUMN activity_details_distance.loc_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN activity_details_distance.grid_5m_00_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_details_distance.grid_5m_00_id IS 'Column grid_5m_00_id.';


--
-- Name: activity_processing_queue; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.activity_processing_queue (
    activity_id bigint NOT NULL,
    created_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE activity_processing_queue; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.activity_processing_queue IS 'Table activity_processing_queue.';


--
-- Name: COLUMN activity_processing_queue.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_processing_queue.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_processing_queue.created_at_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_processing_queue.created_at_utc IS 'Column created_at_utc.';


--
-- Name: activity_resampled_1m; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.activity_resampled_1m (
    activity_id bigint,
    distance_m integer,
    latitude double precision,
    longitude double precision,
    elevation_m double precision,
    elapsed_duration_s double precision
);


--
-- Name: TABLE activity_resampled_1m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.activity_resampled_1m IS 'Table activity_resampled_1m.';


--
-- Name: COLUMN activity_resampled_1m.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_resampled_1m.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN activity_resampled_1m.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.latitude IS 'Column latitude.';


--
-- Name: COLUMN activity_resampled_1m.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.longitude IS 'Column longitude.';


--
-- Name: COLUMN activity_resampled_1m.elevation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN activity_resampled_1m.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.activity_resampled_1m.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: courses; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.courses (
    course_id integer NOT NULL,
    course_name text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    course_path public.geometry(LineString,4326),
    course_distance_m numeric(8,2),
    course_elevation_gain_m numeric(10,2)
);


--
-- Name: TABLE courses; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.courses IS 'Table courses.';


--
-- Name: COLUMN courses.course_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.course_id IS 'Column course_id.';


--
-- Name: COLUMN courses.course_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.course_name IS 'Column course_name.';


--
-- Name: COLUMN courses.created_at; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.created_at IS 'Column created_at.';


--
-- Name: COLUMN courses.course_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.course_path IS 'Column course_path.';


--
-- Name: COLUMN courses.course_distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.course_distance_m IS 'Column course_distance_m.';


--
-- Name: COLUMN courses.course_elevation_gain_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.courses.course_elevation_gain_m IS 'Column course_elevation_gain_m.';


--
-- Name: courses_course_id_seq; Type: SEQUENCE; Schema: activities; Owner: -
--

CREATE SEQUENCE activities.courses_course_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: courses_course_id_seq; Type: SEQUENCE OWNED BY; Schema: activities; Owner: -
--

ALTER SEQUENCE activities.courses_course_id_seq OWNED BY activities.courses.course_id;


--
-- Name: distinct_segments; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.distinct_segments (
    segment_id integer NOT NULL,
    match_seg_id integer NOT NULL
);


--
-- Name: TABLE distinct_segments; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.distinct_segments IS 'Table distinct_segments.';


--
-- Name: COLUMN distinct_segments.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.distinct_segments.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN distinct_segments.match_seg_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.distinct_segments.match_seg_id IS 'Column match_seg_id.';


--
-- Name: elevation_reference; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.elevation_reference (
    grid_5m_00_id public.geometry(Point,4326) NOT NULL,
    avg_elevation double precision NOT NULL,
    obs_count integer DEFAULT 1 NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    elev_stddev numeric,
    elevation_hammered numeric,
    elevation_sanded numeric
);


--
-- Name: TABLE elevation_reference; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.elevation_reference IS 'Table elevation_reference.';


--
-- Name: COLUMN elevation_reference.grid_5m_00_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.grid_5m_00_id IS 'Column grid_5m_00_id.';


--
-- Name: COLUMN elevation_reference.avg_elevation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.avg_elevation IS 'Column avg_elevation.';


--
-- Name: COLUMN elevation_reference.obs_count; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.obs_count IS 'Column obs_count.';


--
-- Name: COLUMN elevation_reference.last_updated; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.last_updated IS 'Column last_updated.';


--
-- Name: COLUMN elevation_reference.elev_stddev; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.elev_stddev IS 'Column elev_stddev.';


--
-- Name: COLUMN elevation_reference.elevation_hammered; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.elevation_hammered IS 'Column elevation_hammered.';


--
-- Name: COLUMN elevation_reference.elevation_sanded; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.elevation_reference.elevation_sanded IS 'Column elevation_sanded.';


--
-- Name: metric_list; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.metric_list (
    activity_type_id smallint NOT NULL,
    metric_name text NOT NULL,
    metric_unit text,
    last_seen_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE metric_list; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.metric_list IS 'Table metric_list.';


--
-- Name: COLUMN metric_list.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.metric_list.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN metric_list.metric_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.metric_list.metric_name IS 'Column metric_name.';


--
-- Name: COLUMN metric_list.metric_unit; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.metric_list.metric_unit IS 'Column metric_unit.';


--
-- Name: COLUMN metric_list.last_seen_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.metric_list.last_seen_utc IS 'Column last_seen_utc.';


--
-- Name: overlap_queue; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.overlap_queue (
    queue_id integer NOT NULL,
    segment_id_a integer NOT NULL,
    segment_id_b integer NOT NULL,
    match_confidence numeric,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: TABLE overlap_queue; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.overlap_queue IS 'Table overlap_queue.';


--
-- Name: COLUMN overlap_queue.queue_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.overlap_queue.queue_id IS 'Column queue_id.';


--
-- Name: COLUMN overlap_queue.segment_id_a; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.overlap_queue.segment_id_a IS 'Column segment_id_a.';


--
-- Name: COLUMN overlap_queue.segment_id_b; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.overlap_queue.segment_id_b IS 'Column segment_id_b.';


--
-- Name: COLUMN overlap_queue.match_confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.overlap_queue.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN overlap_queue.created_at; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.overlap_queue.created_at IS 'Column created_at.';


--
-- Name: overlap_queue_queue_id_seq; Type: SEQUENCE; Schema: activities; Owner: -
--

CREATE SEQUENCE activities.overlap_queue_queue_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: overlap_queue_queue_id_seq; Type: SEQUENCE OWNED BY; Schema: activities; Owner: -
--

ALTER SEQUENCE activities.overlap_queue_queue_id_seq OWNED BY activities.overlap_queue.queue_id;


--
-- Name: race_predictions_raw; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.race_predictions_raw (
    ts_utc timestamp with time zone NOT NULL,
    prediction_5k_s integer,
    prediction_10k_s integer,
    prediction_half_marathon_s integer,
    prediction_marathon_s integer
);


--
-- Name: TABLE race_predictions_raw; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.race_predictions_raw IS 'Table race_predictions_raw.';


--
-- Name: COLUMN race_predictions_raw.ts_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.race_predictions_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN race_predictions_raw.prediction_5k_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.race_predictions_raw.prediction_5k_s IS 'Column prediction_5k_s.';


--
-- Name: COLUMN race_predictions_raw.prediction_10k_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.race_predictions_raw.prediction_10k_s IS 'Column prediction_10k_s.';


--
-- Name: COLUMN race_predictions_raw.prediction_half_marathon_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.race_predictions_raw.prediction_half_marathon_s IS 'Column prediction_half_marathon_s.';


--
-- Name: COLUMN race_predictions_raw.prediction_marathon_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.race_predictions_raw.prediction_marathon_s IS 'Column prediction_marathon_s.';


--
-- Name: segment_match_exclusions; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.segment_match_exclusions (
    segment_id integer,
    activity_id bigint
);


--
-- Name: TABLE segment_match_exclusions; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.segment_match_exclusions IS 'Table segment_match_exclusions.';


--
-- Name: COLUMN segment_match_exclusions.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_match_exclusions.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN segment_match_exclusions.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_match_exclusions.activity_id IS 'Column activity_id.';


--
-- Name: segment_matches; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.segment_matches (
    segment_id integer NOT NULL,
    activity_id bigint NOT NULL,
    activity_start_point integer,
    activity_end_point integer,
    activity_path public.geometry(LineStringZM,4326),
    match_confirmed boolean,
    match_confidence numeric(6,2),
    match_ts_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE segment_matches; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.segment_matches IS 'Table segment_matches.';


--
-- Name: COLUMN segment_matches.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN segment_matches.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN segment_matches.activity_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN segment_matches.activity_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN segment_matches.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN segment_matches.match_confirmed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.match_confirmed IS 'Column match_confirmed.';


--
-- Name: COLUMN segment_matches.match_confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN segment_matches.match_ts_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segment_matches.match_ts_utc IS 'Column match_ts_utc.';


--
-- Name: segment_matches_segment_id_seq; Type: SEQUENCE; Schema: activities; Owner: -
--

CREATE SEQUENCE activities.segment_matches_segment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: segment_matches_segment_id_seq; Type: SEQUENCE OWNED BY; Schema: activities; Owner: -
--

ALTER SEQUENCE activities.segment_matches_segment_id_seq OWNED BY activities.segment_matches.segment_id;


--
-- Name: segments; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.segments (
    segment_id integer NOT NULL,
    activity_reference_id bigint,
    reference_start_point integer,
    reference_end_point integer,
    is_course boolean DEFAULT false,
    segment_name text,
    segment_path public.geometry(LineStringZM,4326),
    distance_m numeric(8,2),
    elevation_gain numeric(10,2),
    elevation_loss numeric(10,2),
    start_point public.geography(Point,4326),
    end_point public.geography(Point,4326),
    activity_type_id smallint,
    utm_srid integer GENERATED ALWAYS AS (((
CASE
    WHEN (public.st_y((start_point)::public.geometry) >= (0)::double precision) THEN 32600
    ELSE 32700
END + (floor(((public.st_x((start_point)::public.geometry) + (180)::double precision) / (6)::double precision)))::integer) + 1)) STORED,
    segment_path_transformed public.geometry GENERATED ALWAYS AS (public.st_transform(segment_path, ((
CASE
    WHEN (public.st_y((start_point)::public.geometry) >= (0)::double precision) THEN 32600
    ELSE 32700
END + (floor(((public.st_x((start_point)::public.geometry) + (180)::double precision) / (6)::double precision)))::integer) + 1))) STORED
);


--
-- Name: TABLE segments; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.segments IS 'Table segments.';


--
-- Name: COLUMN segments.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN segments.activity_reference_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.activity_reference_id IS 'Column activity_reference_id.';


--
-- Name: COLUMN segments.reference_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.reference_start_point IS 'Column reference_start_point.';


--
-- Name: COLUMN segments.reference_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.reference_end_point IS 'Column reference_end_point.';


--
-- Name: COLUMN segments.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.is_course IS 'Column is_course.';


--
-- Name: COLUMN segments.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN segments.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.segment_path IS 'Column segment_path.';


--
-- Name: COLUMN segments.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN segments.elevation_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.elevation_gain IS 'Column elevation_gain.';


--
-- Name: COLUMN segments.elevation_loss; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.elevation_loss IS 'Column elevation_loss.';


--
-- Name: COLUMN segments.start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.start_point IS 'Column start_point.';


--
-- Name: COLUMN segments.end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.end_point IS 'Column end_point.';


--
-- Name: COLUMN segments.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN segments.utm_srid; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.utm_srid IS 'Column utm_srid.';


--
-- Name: COLUMN segments.segment_path_transformed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments.segment_path_transformed IS 'Column segment_path_transformed.';


--
-- Name: segments_details; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.segments_details (
    segment_id integer NOT NULL,
    activity_id bigint NOT NULL,
    activity_start_point integer NOT NULL,
    activity_end_point integer NOT NULL,
    start_time_utc timestamp with time zone,
    distance_m integer,
    elapsed_duration_s numeric,
    max_hr numeric,
    avg_hr numeric,
    avg_cadence numeric,
    avg_temp numeric,
    avg_vert_osc numeric,
    avg_vert_ratio numeric,
    avg_gct numeric,
    avg_perf numeric,
    avg_balance numeric,
    avg_stride_length numeric,
    segment_path public.geometry(LineStringZM,4326)
);


--
-- Name: TABLE segments_details; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.segments_details IS 'Table segments_details.';


--
-- Name: COLUMN segments_details.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN segments_details.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN segments_details.activity_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN segments_details.activity_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN segments_details.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN segments_details.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN segments_details.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN segments_details.max_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.max_hr IS 'Column max_hr.';


--
-- Name: COLUMN segments_details.avg_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_hr IS 'Column avg_hr.';


--
-- Name: COLUMN segments_details.avg_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_cadence IS 'Column avg_cadence.';


--
-- Name: COLUMN segments_details.avg_temp; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_temp IS 'Column avg_temp.';


--
-- Name: COLUMN segments_details.avg_vert_osc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_vert_osc IS 'Column avg_vert_osc.';


--
-- Name: COLUMN segments_details.avg_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_vert_ratio IS 'Column avg_vert_ratio.';


--
-- Name: COLUMN segments_details.avg_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_gct IS 'Column avg_gct.';


--
-- Name: COLUMN segments_details.avg_perf; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_perf IS 'Column avg_perf.';


--
-- Name: COLUMN segments_details.avg_balance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_balance IS 'Column avg_balance.';


--
-- Name: COLUMN segments_details.avg_stride_length; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.avg_stride_length IS 'Column avg_stride_length.';


--
-- Name: COLUMN segments_details.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.segments_details.segment_path IS 'Column segment_path.';


--
-- Name: segments_segment_id_seq; Type: SEQUENCE; Schema: activities; Owner: -
--

CREATE SEQUENCE activities.segments_segment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: segments_segment_id_seq; Type: SEQUENCE OWNED BY; Schema: activities; Owner: -
--

ALTER SEQUENCE activities.segments_segment_id_seq OWNED BY activities.segments.segment_id;


--
-- Name: steps_raw; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.steps_raw (
    ts_utc timestamp with time zone NOT NULL,
    step_goal integer,
    total_steps integer,
    total_distance_m integer
);


--
-- Name: TABLE steps_raw; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.steps_raw IS 'Table steps_raw.';


--
-- Name: COLUMN steps_raw.ts_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.steps_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN steps_raw.step_goal; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.steps_raw.step_goal IS 'Column step_goal.';


--
-- Name: COLUMN steps_raw.total_steps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.steps_raw.total_steps IS 'Column total_steps.';


--
-- Name: COLUMN steps_raw.total_distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.steps_raw.total_distance_m IS 'Column total_distance_m.';


--
-- Name: temp_activity_segment_metas; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_activity_segment_metas (
    activity_id bigint,
    start_point integer,
    end_point integer
);


--
-- Name: TABLE temp_activity_segment_metas; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_activity_segment_metas IS 'Table temp_activity_segment_metas.';


--
-- Name: COLUMN temp_activity_segment_metas.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_activity_segment_metas.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_activity_segment_metas.start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_activity_segment_metas.start_point IS 'Column start_point.';


--
-- Name: COLUMN temp_activity_segment_metas.end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_activity_segment_metas.end_point IS 'Column end_point.';


--
-- Name: temp_segment_details; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segment_details (
    activity_id bigint,
    activity_type_id smallint,
    is_course boolean,
    segment_name text,
    row_asc integer,
    row_desc integer,
    elapsed_duration_s integer,
    latitude numeric(9,6),
    longitude numeric(9,6),
    elevation_m numeric(10,2),
    distance_m numeric(10,2),
    elevation_delta numeric(10,2)
);


--
-- Name: TABLE temp_segment_details; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segment_details IS 'Table temp_segment_details.';


--
-- Name: COLUMN temp_segment_details.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_segment_details.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN temp_segment_details.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.is_course IS 'Column is_course.';


--
-- Name: COLUMN temp_segment_details.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN temp_segment_details.row_asc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.row_asc IS 'Column row_asc.';


--
-- Name: COLUMN temp_segment_details.row_desc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.row_desc IS 'Column row_desc.';


--
-- Name: COLUMN temp_segment_details.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN temp_segment_details.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.latitude IS 'Column latitude.';


--
-- Name: COLUMN temp_segment_details.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.longitude IS 'Column longitude.';


--
-- Name: COLUMN temp_segment_details.elevation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN temp_segment_details.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN temp_segment_details.elevation_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_details.elevation_delta IS 'Column elevation_delta.';


--
-- Name: temp_segment_matches_downselect; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segment_matches_downselect (
    activity_id bigint,
    best_start_dist integer,
    best_end_dist integer,
    full_path public.geometry(LineStringZM,4326),
    confidence numeric(5,2),
    ruggedness numeric(5,2),
    utm_srid integer,
    short_path public.geometry(LineStringZM,4326),
    segment_id integer,
    dist_deviation numeric,
    full_path_transformed public.geometry,
    freschet_deviation_m numeric,
    polygon_deviation_m numeric,
    hausdorff_deviation_m numeric,
    heading_deviation numeric
);


--
-- Name: TABLE temp_segment_matches_downselect; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segment_matches_downselect IS 'Table temp_segment_matches_downselect.';


--
-- Name: COLUMN temp_segment_matches_downselect.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_segment_matches_downselect.best_start_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.best_start_dist IS 'Column best_start_dist.';


--
-- Name: COLUMN temp_segment_matches_downselect.best_end_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.best_end_dist IS 'Column best_end_dist.';


--
-- Name: COLUMN temp_segment_matches_downselect.full_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.full_path IS 'Column full_path.';


--
-- Name: COLUMN temp_segment_matches_downselect.confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.confidence IS 'Column confidence.';


--
-- Name: COLUMN temp_segment_matches_downselect.ruggedness; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.ruggedness IS 'Column ruggedness.';


--
-- Name: COLUMN temp_segment_matches_downselect.utm_srid; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.utm_srid IS 'Column utm_srid.';


--
-- Name: COLUMN temp_segment_matches_downselect.short_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.short_path IS 'Column short_path.';


--
-- Name: COLUMN temp_segment_matches_downselect.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN temp_segment_matches_downselect.dist_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.dist_deviation IS 'Column dist_deviation.';


--
-- Name: COLUMN temp_segment_matches_downselect.full_path_transformed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.full_path_transformed IS 'Column full_path_transformed.';


--
-- Name: COLUMN temp_segment_matches_downselect.freschet_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.freschet_deviation_m IS 'Column freschet_deviation_m.';


--
-- Name: COLUMN temp_segment_matches_downselect.polygon_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.polygon_deviation_m IS 'Column polygon_deviation_m.';


--
-- Name: COLUMN temp_segment_matches_downselect.hausdorff_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.hausdorff_deviation_m IS 'Column hausdorff_deviation_m.';


--
-- Name: COLUMN temp_segment_matches_downselect.heading_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_downselect.heading_deviation IS 'Column heading_deviation.';


--
-- Name: temp_segment_matches_v1; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segment_matches_v1 (
    segment_id integer,
    activity_id bigint,
    activity_start_point integer,
    activity_end_point integer,
    activity_path public.geometry(LineStringZM,4326),
    match_confirmed boolean,
    match_confidence numeric(6,2),
    match_ts_utc timestamp with time zone
);


--
-- Name: TABLE temp_segment_matches_v1; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segment_matches_v1 IS 'Table temp_segment_matches_v1.';


--
-- Name: COLUMN temp_segment_matches_v1.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN temp_segment_matches_v1.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_segment_matches_v1.activity_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN temp_segment_matches_v1.activity_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN temp_segment_matches_v1.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN temp_segment_matches_v1.match_confirmed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.match_confirmed IS 'Column match_confirmed.';


--
-- Name: COLUMN temp_segment_matches_v1.match_confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN temp_segment_matches_v1.match_ts_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matches_v1.match_ts_utc IS 'Column match_ts_utc.';


--
-- Name: temp_segment_matching_activities; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segment_matching_activities (
    activity_id bigint NOT NULL
);


--
-- Name: TABLE temp_segment_matching_activities; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segment_matching_activities IS 'Table temp_segment_matching_activities.';


--
-- Name: COLUMN temp_segment_matching_activities.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matching_activities.activity_id IS 'Column activity_id.';


--
-- Name: temp_segment_matching_segments; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segment_matching_segments (
    segment_id integer
);


--
-- Name: TABLE temp_segment_matching_segments; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segment_matching_segments IS 'Table temp_segment_matching_segments.';


--
-- Name: COLUMN temp_segment_matching_segments.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segment_matching_segments.segment_id IS 'Column segment_id.';


--
-- Name: temp_segments_v1; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.temp_segments_v1 (
    segment_id integer,
    activity_reference_id bigint,
    reference_start_point integer,
    reference_end_point integer,
    is_course boolean,
    segment_name text,
    segment_path public.geometry(LineStringZM,4326),
    distance_m numeric(8,2),
    elevation_gain numeric(10,2),
    elevation_loss numeric(10,2),
    start_point public.geometry,
    end_point public.geometry,
    activity_type_id smallint
);


--
-- Name: TABLE temp_segments_v1; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.temp_segments_v1 IS 'Table temp_segments_v1.';


--
-- Name: COLUMN temp_segments_v1.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN temp_segments_v1.activity_reference_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.activity_reference_id IS 'Column activity_reference_id.';


--
-- Name: COLUMN temp_segments_v1.reference_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.reference_start_point IS 'Column reference_start_point.';


--
-- Name: COLUMN temp_segments_v1.reference_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.reference_end_point IS 'Column reference_end_point.';


--
-- Name: COLUMN temp_segments_v1.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.is_course IS 'Column is_course.';


--
-- Name: COLUMN temp_segments_v1.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN temp_segments_v1.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.segment_path IS 'Column segment_path.';


--
-- Name: COLUMN temp_segments_v1.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN temp_segments_v1.elevation_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.elevation_gain IS 'Column elevation_gain.';


--
-- Name: COLUMN temp_segments_v1.elevation_loss; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.elevation_loss IS 'Column elevation_loss.';


--
-- Name: COLUMN temp_segments_v1.start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.start_point IS 'Column start_point.';


--
-- Name: COLUMN temp_segments_v1.end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.end_point IS 'Column end_point.';


--
-- Name: COLUMN temp_segments_v1.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.temp_segments_v1.activity_type_id IS 'Column activity_type_id.';


--
-- Name: training_cycle_dates; Type: TABLE; Schema: activities; Owner: -
--

CREATE TABLE activities.training_cycle_dates (
    cycle_start_utc timestamp with time zone NOT NULL,
    cycle_name text
);


--
-- Name: TABLE training_cycle_dates; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON TABLE activities.training_cycle_dates IS 'Table training_cycle_dates.';


--
-- Name: COLUMN training_cycle_dates.cycle_start_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.training_cycle_dates.cycle_start_utc IS 'Column cycle_start_utc.';


--
-- Name: COLUMN training_cycle_dates.cycle_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.training_cycle_dates.cycle_name IS 'Column cycle_name.';


--
-- Name: vw_activity_elevation_deltas; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_elevation_deltas AS
 SELECT activity_id,
    elapsed_duration_s,
    round(((((elevation_m_smooth + COALESCE(lag(elevation_m_smooth) OVER (PARTITION BY activity_id ORDER BY elapsed_duration_s), elevation_m_smooth)) + COALESCE(lead(elevation_m_smooth) OVER (PARTITION BY activity_id ORDER BY elapsed_duration_s), elevation_m_smooth)) / (3)::numeric) - COALESCE(lag(elevation_m_smooth) OVER (PARTITION BY activity_id ORDER BY elapsed_duration_s), elevation_m_smooth)), 1) AS elevation_delta
   FROM activities.activity_details;


--
-- Name: COLUMN vw_activity_elevation_deltas.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_elevation_deltas.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_elevation_deltas.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_elevation_deltas.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_activity_elevation_deltas.elevation_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_elevation_deltas.elevation_delta IS 'Column elevation_delta.';


--
-- Name: vw_activity_header_stats; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_header_stats AS
 WITH most_recent AS (
         SELECT max(activities.activity_id) AS activity_id,
                CASE
                    WHEN ((lower(activities.activity_type_name) ~~ '%run%'::text) AND (activities.distance_m > (1000)::numeric)) THEN 'run'::text
                    WHEN (((lower(activities.activity_type_name) ~~ '%walk%'::text) OR (lower(activities.activity_type_name) ~~ 'hik%'::text)) AND (activities.distance_m > (1000)::numeric)) THEN 'walk'::text
                    ELSE 'other'::text
                END AS activity_type_basic
           FROM activities.activities
          WHERE (((lower(activities.activity_type_name) ~~ '%walk%'::text) OR (lower(activities.activity_type_name) ~~ 'hik%'::text) OR (lower(activities.activity_type_name) ~~ '%run%'::text)) AND (activities.distance_m > (1000)::numeric) AND (activities.activity_path IS NOT NULL))
          GROUP BY
                CASE
                    WHEN ((lower(activities.activity_type_name) ~~ '%run%'::text) AND (activities.distance_m > (1000)::numeric)) THEN 'run'::text
                    WHEN (((lower(activities.activity_type_name) ~~ '%walk%'::text) OR (lower(activities.activity_type_name) ~~ 'hik%'::text)) AND (activities.distance_m > (1000)::numeric)) THEN 'walk'::text
                    ELSE 'other'::text
                END
        ), hr_values AS (
         SELECT ad.activity_id,
            mr.activity_type_basic,
            max(ad.heartrate_bpm) AS hr_maximum,
            percentile_disc((0.5)::double precision) WITHIN GROUP (ORDER BY ad.heartrate_bpm) AS hr_median
           FROM (most_recent mr
             JOIN activities.activity_details ad ON ((ad.activity_id = mr.activity_id)))
          GROUP BY ad.activity_id, mr.activity_type_basic
        ), final AS (
         SELECT mr.activity_id,
            mr.activity_type_basic,
            ((public.m2mi(a.distance_m))::text || ' mi'::text) AS distance_mi,
            (a.duration_elapsed_s / public.m2mi(a.distance_m)) AS pace_spm,
                CASE
                    WHEN (a.duration_elapsed_s IS NULL) THEN NULL::text
                    WHEN (round((a.duration_elapsed_s)::numeric, 1) >= (3600)::numeric) THEN (((((floor((round((a.duration_elapsed_s)::numeric, 1) / (3600)::numeric)))::text || ':'::text) || lpad((floor(((round((a.duration_elapsed_s)::numeric, 1) % (3600)::numeric) / (60)::numeric)))::text, 2, '0'::text)) || ':'::text) || to_char((round((a.duration_elapsed_s)::numeric, 1) % (60)::numeric), 'FM00.0'::text))
                    WHEN (round((a.duration_elapsed_s)::numeric, 1) >= (600)::numeric) THEN ((lpad((floor((round((a.duration_elapsed_s)::numeric, 1) / (60)::numeric)))::text, 2, '0'::text) || ':'::text) || to_char((round((a.duration_elapsed_s)::numeric, 1) % (60)::numeric), 'FM00.0'::text))
                    ELSE (((floor((round((a.duration_elapsed_s)::numeric, 1) / (60)::numeric)))::text || ':'::text) || to_char((round((a.duration_elapsed_s)::numeric, 1) % (60)::numeric), 'FM00.0'::text))
                END AS duration_display,
            a.elevation_m_gain,
            a.start_time_utc,
            a.activity_path,
            mr.hr_median,
            mr.hr_maximum
           FROM (hr_values mr
             JOIN activities.activities a ON ((mr.activity_id = a.activity_id)))
        )
 SELECT activity_id,
    activity_type_basic,
    distance_mi,
    duration_display,
    (
        CASE
            WHEN (pace_spm IS NULL) THEN NULL::text
            WHEN (round(pace_spm, 1) >= (3600)::numeric) THEN (((((floor((round(pace_spm, 1) / (3600)::numeric)))::text || ':'::text) || lpad((floor(((round(pace_spm, 1) % (3600)::numeric) / (60)::numeric)))::text, 2, '0'::text)) || ':'::text) || to_char((round(pace_spm, 1) % (60)::numeric), 'FM00.0'::text))
            WHEN (round(pace_spm, 1) >= (600)::numeric) THEN ((lpad((floor((round(pace_spm, 1) / (60)::numeric)))::text, 2, '0'::text) || ':'::text) || to_char((round(pace_spm, 1) % (60)::numeric), 'FM00.0'::text))
            ELSE (((floor((round(pace_spm, 1) / (60)::numeric)))::text || ':'::text) || to_char((round(pace_spm, 1) % (60)::numeric), 'FM00.0'::text))
        END || '/mi'::text) AS pace_display,
    start_time_utc,
    activity_path,
    hr_median,
    hr_maximum
   FROM final;


--
-- Name: COLUMN vw_activity_header_stats.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_header_stats.activity_type_basic; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.activity_type_basic IS 'Column activity_type_basic.';


--
-- Name: COLUMN vw_activity_header_stats.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_activity_header_stats.duration_display; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.duration_display IS 'Column duration_display.';


--
-- Name: COLUMN vw_activity_header_stats.pace_display; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.pace_display IS 'Column pace_display.';


--
-- Name: COLUMN vw_activity_header_stats.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_activity_header_stats.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN vw_activity_header_stats.hr_median; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.hr_median IS 'Column hr_median.';


--
-- Name: COLUMN vw_activity_header_stats.hr_maximum; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_header_stats.hr_maximum IS 'Column hr_maximum.';


--
-- Name: vw_activity_ids_to_sync; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_ids_to_sync AS
 SELECT activity_id,
    (round(duration_elapsed_s, 0) * (2)::numeric) AS max_points
   FROM activities.activities
  WHERE ((NOT is_downloaded) AND (NOT (activity_type_name ~~ '%treadmill%'::text)) AND (distance_m > (1000)::numeric))
  ORDER BY (activity_type_name ~~ '%running%'::text) DESC, start_time_utc DESC
 LIMIT 15;


--
-- Name: COLUMN vw_activity_ids_to_sync.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_ids_to_sync.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_ids_to_sync.max_points; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_ids_to_sync.max_points IS 'Column max_points.';


--
-- Name: vw_activity_path_updates; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_path_updates AS
 SELECT a.activity_id,
    public.st_setsrid(public.st_makeline(public.st_makepoint((ad.longitude)::double precision, (ad.latitude)::double precision, (ad.elevation_m_smooth)::double precision, (ad.elapsed_duration_s)::double precision) ORDER BY ad.elapsed_duration_s), 4326) AS activity_path
   FROM (activities.activities a
     JOIN activities.activity_details ad ON ((a.activity_id = ad.activity_id)))
  GROUP BY a.activity_id;


--
-- Name: COLUMN vw_activity_path_updates.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_path_updates.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_path_updates.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_path_updates.activity_path IS 'Column activity_path.';


--
-- Name: vw_segment_comps; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_comps AS
 WITH tm AS (
         SELECT am.activity_id,
            ((a.start_time_utc)::date)::text AS act_date,
            am.start_point,
            am.end_point,
            row_number() OVER (PARTITION BY am.activity_id ORDER BY a.start_time_utc, am.start_point) AS act_rank,
            row_number() OVER (ORDER BY a.start_time_utc DESC, am.start_point DESC) AS meta_rank,
            sm.distance_m
           FROM ((activities.temp_activity_segment_metas am
             JOIN activities.activities a ON ((am.activity_id = a.activity_id)))
             JOIN activities.segments_details sm ON (((am.activity_id = sm.activity_id) AND (am.start_point = sm.activity_start_point) AND (am.end_point = sm.activity_end_point))))
        ), ms AS (
         SELECT tm.activity_id,
            (tm.act_date ||
                CASE
                    WHEN (tm.act_rank > 1) THEN ((' ('::text || (tm.act_rank)::text) || ')'::text)
                    ELSE ''::text
                END) AS id_val,
            tm.start_point,
            tm.end_point,
            tm.meta_rank,
            ((max(
                CASE
                    WHEN (tm.meta_rank = 1) THEN tm.distance_m
                    ELSE NULL::integer
                END) OVER ())::numeric / (tm.distance_m)::numeric) AS distance_multiplier
           FROM tm
        ), canon AS (
         SELECT ms.id_val,
            (ad.elapsed_duration_ms - min(ad.elapsed_duration_ms) OVER ()) AS elapsed_ms,
            (ad.distance_m - ms.start_point) AS distance_m,
            ad.elevation_m_smooth,
            ad.heartrate_bpm
           FROM (ms
             JOIN activities.activity_details_distance ad ON (((ms.activity_id = ad.activity_id) AND (ms.start_point <= ad.distance_m) AND (ms.end_point >= ad.distance_m))))
          WHERE (ms.meta_rank = 1)
        ), d_adj AS (
         SELECT DISTINCT ON (ms.id_val, (round((((ad.distance_m - ms.start_point))::numeric * ms.distance_multiplier), 0))) ms.id_val,
            ms.meta_rank,
            (ad.elapsed_duration_ms - min(ad.elapsed_duration_ms) OVER (PARTITION BY ad.activity_id)) AS elapsed_ms,
            round((((ad.distance_m - ms.start_point))::numeric * ms.distance_multiplier), 0) AS distance_m,
            (ad.distance_m - ms.start_point) AS distance_raw,
            ad.longitude,
            ad.latitude,
            ad.heartrate_bpm,
            ad.cadence_spm,
            ad.elevation_m_smooth,
            ad.ground_contact_balance_left,
            ad.performance_condition,
                CASE
                    WHEN (ad.vert_ratio < (0)::numeric) THEN NULL::numeric
                    ELSE ad.vert_ratio
                END AS vert_ratio,
            ad.vert_oscillation_cm,
            ad.air_temp_c,
            ad.stride_length_cm,
            ad.ground_contact_time_ms,
            ad.speed_mmps
           FROM (ms
             JOIN activities.activity_details_distance ad ON (((ms.activity_id = ad.activity_id) AND (ms.start_point <= ad.distance_m) AND (ms.end_point >= ad.distance_m))))
          ORDER BY ms.id_val, (round((((ad.distance_m - ms.start_point))::numeric * ms.distance_multiplier), 0)), (ad.elapsed_duration_ms - min(ad.elapsed_duration_ms) OVER (PARTITION BY ad.activity_id))
        )
 SELECT nc.id_val,
    nc.meta_rank,
    c.elevation_m_smooth,
    c.distance_m,
    nc.elapsed_ms,
    round(((nc.elapsed_ms - c.elapsed_ms))::numeric, 2) AS elapsed_ms_delta,
    nc.longitude,
    nc.latitude,
    nc.heartrate_bpm,
    round(((c.heartrate_bpm - nc.heartrate_bpm))::numeric, 0) AS heartrate_bpm_delta,
    nc.cadence_spm,
    nc.ground_contact_balance_left,
    nc.performance_condition,
    nc.vert_ratio,
    nc.vert_oscillation_cm,
    nc.air_temp_c,
    nc.stride_length_cm,
    nc.ground_contact_time_ms,
    nc.speed_mmps
   FROM (canon c
     LEFT JOIN d_adj nc ON (((c.distance_m)::numeric = nc.distance_m)))
  ORDER BY nc.meta_rank, nc.id_val, c.distance_m;


--
-- Name: COLUMN vw_segment_comps.id_val; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.id_val IS 'Column id_val.';


--
-- Name: COLUMN vw_segment_comps.meta_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.meta_rank IS 'Column meta_rank.';


--
-- Name: COLUMN vw_segment_comps.elevation_m_smooth; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: COLUMN vw_segment_comps.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN vw_segment_comps.elapsed_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.elapsed_ms IS 'Column elapsed_ms.';


--
-- Name: COLUMN vw_segment_comps.elapsed_ms_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.elapsed_ms_delta IS 'Column elapsed_ms_delta.';


--
-- Name: COLUMN vw_segment_comps.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.longitude IS 'Column longitude.';


--
-- Name: COLUMN vw_segment_comps.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.latitude IS 'Column latitude.';


--
-- Name: COLUMN vw_segment_comps.heartrate_bpm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN vw_segment_comps.heartrate_bpm_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.heartrate_bpm_delta IS 'Column heartrate_bpm_delta.';


--
-- Name: COLUMN vw_segment_comps.cadence_spm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.cadence_spm IS 'Column cadence_spm.';


--
-- Name: COLUMN vw_segment_comps.ground_contact_balance_left; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.ground_contact_balance_left IS 'Column ground_contact_balance_left.';


--
-- Name: COLUMN vw_segment_comps.performance_condition; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.performance_condition IS 'Column performance_condition.';


--
-- Name: COLUMN vw_segment_comps.vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.vert_ratio IS 'Column vert_ratio.';


--
-- Name: COLUMN vw_segment_comps.vert_oscillation_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.vert_oscillation_cm IS 'Column vert_oscillation_cm.';


--
-- Name: COLUMN vw_segment_comps.air_temp_c; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.air_temp_c IS 'Column air_temp_c.';


--
-- Name: COLUMN vw_segment_comps.stride_length_cm; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.stride_length_cm IS 'Column stride_length_cm.';


--
-- Name: COLUMN vw_segment_comps.ground_contact_time_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.ground_contact_time_ms IS 'Column ground_contact_time_ms.';


--
-- Name: COLUMN vw_segment_comps.speed_mmps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_comps.speed_mmps IS 'Column speed_mmps.';


--
-- Name: vw_activity_segments_aggregated; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_segments_aggregated AS
 WITH telemetry AS (
         SELECT am.id_val,
            am.meta_rank,
            am.longitude,
            am.latitude,
            am.elevation_m_smooth,
            am.distance_m,
            (floor(((am.elapsed_ms / 1000))::double precision))::integer AS elapsed_duration_s,
            am.heartrate_bpm,
            am.heartrate_bpm_delta,
            am.cadence_spm,
            am.elapsed_ms,
            am.elapsed_ms_delta,
            am.ground_contact_balance_left,
            am.performance_condition,
            am.vert_ratio,
            am.vert_oscillation_cm,
            am.air_temp_c,
            am.stride_length_cm,
            am.ground_contact_time_ms,
            am.speed_mmps
           FROM activities.vw_segment_comps am
        ), downsampled AS (
         SELECT DISTINCT ON (telemetry.meta_rank, telemetry.id_val, telemetry.elapsed_duration_s) telemetry.id_val,
            telemetry.meta_rank,
            telemetry.elapsed_duration_s,
            telemetry.distance_m,
            telemetry.elapsed_ms,
            telemetry.elapsed_ms_delta,
            telemetry.longitude,
            telemetry.latitude,
            telemetry.heartrate_bpm,
            telemetry.heartrate_bpm_delta,
            telemetry.cadence_spm,
            telemetry.elevation_m_smooth,
            telemetry.ground_contact_balance_left,
            telemetry.performance_condition,
            telemetry.vert_ratio,
            telemetry.vert_oscillation_cm,
            telemetry.air_temp_c,
            telemetry.stride_length_cm,
            telemetry.ground_contact_time_ms,
            telemetry.speed_mmps
           FROM telemetry
          ORDER BY telemetry.meta_rank, telemetry.id_val, telemetry.elapsed_duration_s, telemetry.elapsed_ms
        )
 SELECT id_val,
    meta_rank,
    json_agg(json_build_array(longitude, latitude) ORDER BY elapsed_duration_s) AS path_coords,
    json_agg(elapsed_duration_s ORDER BY elapsed_duration_s) AS map_timestamps,
    array_agg(elapsed_duration_s ORDER BY elapsed_duration_s) AS x_time,
    array_agg(distance_m ORDER BY elapsed_duration_s) AS x_distance,
    array_agg(round(((elapsed_ms)::numeric / 1000.0), 2) ORDER BY elapsed_duration_s) AS y_time,
    array_agg(round((elapsed_ms_delta / 1000.0), 2) ORDER BY elapsed_duration_s) AS y_time_delta,
    array_agg(heartrate_bpm ORDER BY elapsed_duration_s) AS y_hr,
    array_agg(heartrate_bpm_delta ORDER BY elapsed_duration_s) AS y_hr_delta,
    array_agg(elevation_m_smooth ORDER BY elapsed_duration_s) AS y_elevation,
    array_agg(cadence_spm ORDER BY elapsed_duration_s) AS y_cadence,
    array_agg(ground_contact_balance_left ORDER BY elapsed_duration_s) AS y_balance,
    array_agg(performance_condition ORDER BY elapsed_duration_s) AS y_performance,
    array_agg(vert_ratio ORDER BY elapsed_duration_s) AS y_vert_ratio,
    array_agg(vert_oscillation_cm ORDER BY elapsed_duration_s) AS y_osc,
    array_agg(public.c2f((air_temp_c)::numeric) ORDER BY elapsed_duration_s) AS y_temp,
    array_agg(stride_length_cm ORDER BY elapsed_duration_s) AS y_stride,
    array_agg(ground_contact_time_ms ORDER BY elapsed_duration_s) AS y_gct,
    array_agg(speed_mmps ORDER BY elapsed_duration_s) AS y_speed
   FROM downsampled
  GROUP BY id_val, meta_rank
  ORDER BY meta_rank, id_val;


--
-- Name: COLUMN vw_activity_segments_aggregated.id_val; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.id_val IS 'Column id_val.';


--
-- Name: COLUMN vw_activity_segments_aggregated.meta_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.meta_rank IS 'Column meta_rank.';


--
-- Name: COLUMN vw_activity_segments_aggregated.path_coords; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.path_coords IS 'Column path_coords.';


--
-- Name: COLUMN vw_activity_segments_aggregated.map_timestamps; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.map_timestamps IS 'Column map_timestamps.';


--
-- Name: COLUMN vw_activity_segments_aggregated.x_time; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.x_time IS 'Column x_time.';


--
-- Name: COLUMN vw_activity_segments_aggregated.x_distance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.x_distance IS 'Column x_distance.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_time; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_time IS 'Column y_time.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_time_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_time_delta IS 'Column y_time_delta.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_hr IS 'Column y_hr.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_hr_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_hr_delta IS 'Column y_hr_delta.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_elevation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_elevation IS 'Column y_elevation.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_cadence IS 'Column y_cadence.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_balance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_balance IS 'Column y_balance.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_performance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_performance IS 'Column y_performance.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_vert_ratio IS 'Column y_vert_ratio.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_osc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_osc IS 'Column y_osc.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_temp; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_temp IS 'Column y_temp.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_stride; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_stride IS 'Column y_stride.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_gct IS 'Column y_gct.';


--
-- Name: COLUMN vw_activity_segments_aggregated.y_speed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_segments_aggregated.y_speed IS 'Column y_speed.';


--
-- Name: sleep_totals; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.sleep_totals (
    sleep_end_date date,
    sleep_start_utc timestamp with time zone NOT NULL,
    sleep_end_utc timestamp with time zone,
    sleep_score integer,
    heartrate_bpm numeric(5,2),
    spo2 numeric(5,2),
    breaths_per_min numeric(5,2),
    hrv_value smallint,
    sleep_duration_s integer,
    rem_sleep_s integer,
    light_sleep_s integer,
    awake_sleep_s integer,
    deep_sleep_s integer,
    score_label text
);


--
-- Name: TABLE sleep_totals; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.sleep_totals IS 'Table sleep_totals.';


--
-- Name: COLUMN sleep_totals.sleep_end_date; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.sleep_end_date IS 'Column sleep_end_date.';


--
-- Name: COLUMN sleep_totals.sleep_start_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.sleep_start_utc IS 'Column sleep_start_utc.';


--
-- Name: COLUMN sleep_totals.sleep_end_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.sleep_end_utc IS 'Column sleep_end_utc.';


--
-- Name: COLUMN sleep_totals.sleep_score; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.sleep_score IS 'Column sleep_score.';


--
-- Name: COLUMN sleep_totals.heartrate_bpm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN sleep_totals.spo2; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.spo2 IS 'Column spo2.';


--
-- Name: COLUMN sleep_totals.breaths_per_min; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.breaths_per_min IS 'Column breaths_per_min.';


--
-- Name: COLUMN sleep_totals.hrv_value; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.hrv_value IS 'Column hrv_value.';


--
-- Name: COLUMN sleep_totals.sleep_duration_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.sleep_duration_s IS 'Column sleep_duration_s.';


--
-- Name: COLUMN sleep_totals.rem_sleep_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.rem_sleep_s IS 'Column rem_sleep_s.';


--
-- Name: COLUMN sleep_totals.light_sleep_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.light_sleep_s IS 'Column light_sleep_s.';


--
-- Name: COLUMN sleep_totals.awake_sleep_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.awake_sleep_s IS 'Column awake_sleep_s.';


--
-- Name: COLUMN sleep_totals.deep_sleep_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.deep_sleep_s IS 'Column deep_sleep_s.';


--
-- Name: COLUMN sleep_totals.score_label; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_totals.score_label IS 'Column score_label.';


--
-- Name: vw_activity_sleep; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_sleep AS
 WITH a AS (
         SELECT activities.activity_id,
            (activities.start_time_utc)::date AS activity_date,
            activities.start_time_utc
           FROM activities.activities
        ), s AS (
         SELECT s1.sleep_end_date,
            s1.sleep_score,
            s1.sleep_end_utc,
            round(((s1.sleep_duration_s)::numeric / 3600.0), 1) AS one_day_sleep_hours,
            round(avg(((s2.sleep_duration_s)::numeric / 3600.0)), 1) AS avg_sleep_hours
           FROM (health.sleep_totals s1
             JOIN health.sleep_totals s2 ON (((s1.sleep_end_date >= (s2.sleep_end_date - '3 days'::interval)) AND (s1.sleep_end_date <= s2.sleep_end_date))))
          GROUP BY s1.sleep_end_date, s1.sleep_score, s1.sleep_end_utc, s1.sleep_duration_s
        )
 SELECT a.activity_id,
    round((floor(EXTRACT(epoch FROM (a.start_time_utc - s.sleep_end_utc))) / 3600.0), 1) AS awake_hours_prior,
    s.sleep_score,
    s.avg_sleep_hours,
    s.one_day_sleep_hours
   FROM (a
     LEFT JOIN s ON ((s.sleep_end_date = a.activity_date)));


--
-- Name: COLUMN vw_activity_sleep.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_sleep.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_sleep.awake_hours_prior; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_sleep.awake_hours_prior IS 'Column awake_hours_prior.';


--
-- Name: COLUMN vw_activity_sleep.sleep_score; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_sleep.sleep_score IS 'Column sleep_score.';


--
-- Name: COLUMN vw_activity_sleep.avg_sleep_hours; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_sleep.avg_sleep_hours IS 'Column avg_sleep_hours.';


--
-- Name: COLUMN vw_activity_sleep.one_day_sleep_hours; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_sleep.one_day_sleep_hours IS 'Column one_day_sleep_hours.';


--
-- Name: heartrate_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.heartrate_raw (
    ts_utc timestamp with time zone NOT NULL,
    heartrate_bpm smallint,
    activity_label text DEFAULT 'awake'::text,
    hr_date date GENERATED ALWAYS AS (((ts_utc AT TIME ZONE 'UTC'::text))::date) STORED
);


--
-- Name: TABLE heartrate_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.heartrate_raw IS 'Table heartrate_raw.';


--
-- Name: COLUMN heartrate_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.heartrate_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN heartrate_raw.heartrate_bpm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.heartrate_raw.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN heartrate_raw.activity_label; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.heartrate_raw.activity_label IS 'Column activity_label.';


--
-- Name: COLUMN heartrate_raw.hr_date; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.heartrate_raw.hr_date IS 'Column hr_date.';


--
-- Name: training_status_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.training_status_raw (
    ts_utc timestamp with time zone NOT NULL,
    vo2_max_value numeric(4,1),
    altitude_current_m integer,
    altitude_acclimation_m integer,
    altitude_acclimation_pct numeric(4,2),
    heat_acclimation_pct numeric(4,2),
    training_load_acute numeric(5,1),
    training_load_min numeric(5,1),
    training_load_max numeric(5,1)
);


--
-- Name: TABLE training_status_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.training_status_raw IS 'Table training_status_raw.';


--
-- Name: COLUMN training_status_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN training_status_raw.vo2_max_value; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.vo2_max_value IS 'Column vo2_max_value.';


--
-- Name: COLUMN training_status_raw.altitude_current_m; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.altitude_current_m IS 'Column altitude_current_m.';


--
-- Name: COLUMN training_status_raw.altitude_acclimation_m; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.altitude_acclimation_m IS 'Column altitude_acclimation_m.';


--
-- Name: COLUMN training_status_raw.altitude_acclimation_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.altitude_acclimation_pct IS 'Column altitude_acclimation_pct.';


--
-- Name: COLUMN training_status_raw.heat_acclimation_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.heat_acclimation_pct IS 'Column heat_acclimation_pct.';


--
-- Name: COLUMN training_status_raw.training_load_acute; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.training_load_acute IS 'Column training_load_acute.';


--
-- Name: COLUMN training_status_raw.training_load_min; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.training_load_min IS 'Column training_load_min.';


--
-- Name: COLUMN training_status_raw.training_load_max; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.training_status_raw.training_load_max IS 'Column training_load_max.';


--
-- Name: vw_heart_rates; Type: VIEW; Schema: health; Owner: -
--

CREATE VIEW health.vw_heart_rates AS
 SELECT hr_date,
    percentile_cont((0.1)::double precision) WITHIN GROUP (ORDER BY ((heartrate_bpm)::double precision)) FILTER (WHERE (activity_label = 'asleep'::text)) AS resting_hr_asleep,
    percentile_cont((0.95)::double precision) WITHIN GROUP (ORDER BY ((heartrate_bpm)::double precision)) AS max_hr,
    percentile_cont((0.1)::double precision) WITHIN GROUP (ORDER BY ((heartrate_bpm)::double precision)) FILTER (WHERE (activity_label = 'awake'::text)) AS resting_hr_awake
   FROM ( SELECT d.hr_date,
            r.heartrate_bpm,
            r.activity_label
           FROM (( SELECT DISTINCT heartrate_raw.hr_date
                   FROM health.heartrate_raw
                  WHERE (heartrate_raw.hr_date > '2025-07-01'::date)) d
             JOIN health.heartrate_raw r ON (((r.hr_date > (d.hr_date - '7 days'::interval)) AND (r.hr_date <= d.hr_date))))) windowed
  GROUP BY hr_date;


--
-- Name: COLUMN vw_heart_rates.hr_date; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_heart_rates.hr_date IS 'Column hr_date.';


--
-- Name: COLUMN vw_heart_rates.resting_hr_asleep; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_heart_rates.resting_hr_asleep IS 'Column resting_hr_asleep.';


--
-- Name: COLUMN vw_heart_rates.max_hr; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_heart_rates.max_hr IS 'Column max_hr.';


--
-- Name: COLUMN vw_heart_rates.resting_hr_awake; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_heart_rates.resting_hr_awake IS 'Column resting_hr_awake.';


--
-- Name: vw_activity_training_status; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_training_status AS
 SELECT a.activity_id,
    ts.vo2_max_value,
    ts.altitude_acclimation_m,
    ts.heat_acclimation_pct,
    ts.training_load_acute,
    ts.training_load_pct,
    ts.training_readiness,
    ts.training_load_mid,
    ts.training_load_max,
    ts.training_load_min,
    hr.resting_hr_asleep,
    hr.resting_hr_awake,
    st.sleep_duration_s,
    st.sleep_score,
    st.light_sleep_s,
    st.deep_sleep_s,
    st.awake_sleep_s,
    st.rem_sleep_s,
    EXTRACT(epoch FROM (a.start_time_utc - st.sleep_end_utc)) AS awake_s_before_activity,
    a.start_time_utc,
    st.sleep_end_utc,
    st.sleep_end_date,
    (a.start_time_utc)::date AS activity_start_date
   FROM (((activities.activities a
     LEFT JOIN health.vw_heart_rates hr ON (((a.start_time_utc)::date = (hr.hr_date - '1 day'::interval))))
     LEFT JOIN LATERAL ( SELECT sleep_totals.sleep_end_date,
            sleep_totals.sleep_start_utc,
            sleep_totals.sleep_end_utc,
            sleep_totals.sleep_score,
            sleep_totals.heartrate_bpm,
            sleep_totals.spo2,
            sleep_totals.breaths_per_min,
            sleep_totals.hrv_value,
            sleep_totals.sleep_duration_s,
            sleep_totals.rem_sleep_s,
            sleep_totals.light_sleep_s,
            sleep_totals.awake_sleep_s,
            sleep_totals.deep_sleep_s,
            sleep_totals.score_label
           FROM health.sleep_totals
          WHERE (sleep_totals.sleep_end_utc < a.start_time_utc)
          ORDER BY sleep_totals.sleep_end_utc DESC
         LIMIT 1) st ON (true))
     LEFT JOIN LATERAL ( SELECT training_status_raw.ts_utc,
            training_status_raw.vo2_max_value,
            training_status_raw.altitude_acclimation_m,
            training_status_raw.heat_acclimation_pct,
            training_status_raw.training_load_acute,
            training_status_raw.training_load_min,
            training_status_raw.training_load_max,
            ((training_status_raw.training_load_max + training_status_raw.training_load_min) / 2.0) AS training_load_mid,
            round(((training_status_raw.training_load_min - training_status_raw.training_load_acute) / training_status_raw.training_load_min), 3) AS training_readiness,
            round((training_status_raw.training_load_acute / training_status_raw.training_load_max), 3) AS training_load_pct
           FROM health.training_status_raw
          WHERE (training_status_raw.ts_utc < a.start_time_utc)
          ORDER BY training_status_raw.ts_utc DESC
         LIMIT 1) ts ON (true));


--
-- Name: COLUMN vw_activity_training_status.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_training_status.vo2_max_value; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.vo2_max_value IS 'Column vo2_max_value.';


--
-- Name: COLUMN vw_activity_training_status.altitude_acclimation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.altitude_acclimation_m IS 'Column altitude_acclimation_m.';


--
-- Name: COLUMN vw_activity_training_status.heat_acclimation_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.heat_acclimation_pct IS 'Column heat_acclimation_pct.';


--
-- Name: COLUMN vw_activity_training_status.training_load_acute; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_load_acute IS 'Column training_load_acute.';


--
-- Name: COLUMN vw_activity_training_status.training_load_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_load_pct IS 'Column training_load_pct.';


--
-- Name: COLUMN vw_activity_training_status.training_readiness; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_readiness IS 'Column training_readiness.';


--
-- Name: COLUMN vw_activity_training_status.training_load_mid; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_load_mid IS 'Column training_load_mid.';


--
-- Name: COLUMN vw_activity_training_status.training_load_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_load_max IS 'Column training_load_max.';


--
-- Name: COLUMN vw_activity_training_status.training_load_min; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.training_load_min IS 'Column training_load_min.';


--
-- Name: COLUMN vw_activity_training_status.resting_hr_asleep; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.resting_hr_asleep IS 'Column resting_hr_asleep.';


--
-- Name: COLUMN vw_activity_training_status.resting_hr_awake; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.resting_hr_awake IS 'Column resting_hr_awake.';


--
-- Name: COLUMN vw_activity_training_status.sleep_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.sleep_duration_s IS 'Column sleep_duration_s.';


--
-- Name: COLUMN vw_activity_training_status.sleep_score; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.sleep_score IS 'Column sleep_score.';


--
-- Name: COLUMN vw_activity_training_status.light_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.light_sleep_s IS 'Column light_sleep_s.';


--
-- Name: COLUMN vw_activity_training_status.deep_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.deep_sleep_s IS 'Column deep_sleep_s.';


--
-- Name: COLUMN vw_activity_training_status.awake_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.awake_sleep_s IS 'Column awake_sleep_s.';


--
-- Name: COLUMN vw_activity_training_status.rem_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.rem_sleep_s IS 'Column rem_sleep_s.';


--
-- Name: COLUMN vw_activity_training_status.awake_s_before_activity; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.awake_s_before_activity IS 'Column awake_s_before_activity.';


--
-- Name: COLUMN vw_activity_training_status.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_activity_training_status.sleep_end_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.sleep_end_utc IS 'Column sleep_end_utc.';


--
-- Name: COLUMN vw_activity_training_status.sleep_end_date; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.sleep_end_date IS 'Column sleep_end_date.';


--
-- Name: COLUMN vw_activity_training_status.activity_start_date; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_training_status.activity_start_date IS 'Column activity_start_date.';


--
-- Name: body_composition_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.body_composition_raw (
    ts_utc timestamp with time zone NOT NULL,
    weight_total_g integer,
    weight_bone_g integer,
    weight_muscle_g integer,
    fat_pct numeric(4,3),
    water_pct numeric(4,3),
    body_mass_index numeric(4,2)
);


--
-- Name: TABLE body_composition_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.body_composition_raw IS 'Table body_composition_raw.';


--
-- Name: COLUMN body_composition_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN body_composition_raw.weight_total_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.weight_total_g IS 'Column weight_total_g.';


--
-- Name: COLUMN body_composition_raw.weight_bone_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.weight_bone_g IS 'Column weight_bone_g.';


--
-- Name: COLUMN body_composition_raw.weight_muscle_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.weight_muscle_g IS 'Column weight_muscle_g.';


--
-- Name: COLUMN body_composition_raw.fat_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.fat_pct IS 'Column fat_pct.';


--
-- Name: COLUMN body_composition_raw.water_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.water_pct IS 'Column water_pct.';


--
-- Name: COLUMN body_composition_raw.body_mass_index; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_composition_raw.body_mass_index IS 'Column body_mass_index.';


--
-- Name: vw_activity_weights; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_weights AS
 WITH interpolations AS (
         SELECT a.activity_id,
            a.start_time_utc,
            prev.ts_utc AS prior_weigh_in_ts,
            prev.weight_total_g AS prior_weight_g,
            nxt.ts_utc AS next_weigh_in_ts,
            nxt.weight_total_g AS next_weight_g,
            (COALESCE(
                CASE
                    WHEN ((prev.ts_utc IS NOT NULL) AND (nxt.ts_utc IS NOT NULL)) THEN round(((prev.weight_total_g)::numeric + (((nxt.weight_total_g - prev.weight_total_g))::numeric * (EXTRACT(epoch FROM (a.start_time_utc - prev.ts_utc)) / NULLIF(EXTRACT(epoch FROM (nxt.ts_utc - prev.ts_utc)), (0)::numeric)))))
                    WHEN (prev.ts_utc IS NOT NULL) THEN (prev.weight_total_g)::numeric
                    WHEN (nxt.ts_utc IS NOT NULL) THEN (nxt.weight_total_g)::numeric
                    ELSE NULL::numeric
                END, NULL::numeric))::integer AS interpolated_weight_g,
            COALESCE(
                CASE
                    WHEN ((prev.ts_utc IS NOT NULL) AND (nxt.ts_utc IS NOT NULL)) THEN round(((prev.fat_pct)::numeric + ((nxt.fat_pct - prev.fat_pct) * (EXTRACT(epoch FROM (a.start_time_utc - prev.ts_utc)) / NULLIF(EXTRACT(epoch FROM (nxt.ts_utc - prev.ts_utc)), (0)::numeric)))), 3)
                    WHEN (prev.ts_utc IS NOT NULL) THEN (prev.fat_pct)::numeric
                    WHEN (nxt.ts_utc IS NOT NULL) THEN (nxt.fat_pct)::numeric
                    ELSE NULL::numeric
                END, NULL::numeric) AS interpolated_fat_pct,
            (COALESCE(
                CASE
                    WHEN ((prev.ts_utc IS NOT NULL) AND (nxt.ts_utc IS NOT NULL)) THEN round(((prev.weight_muscle_g)::numeric + (((nxt.weight_muscle_g - prev.weight_muscle_g))::numeric * (EXTRACT(epoch FROM (a.start_time_utc - prev.ts_utc)) / NULLIF(EXTRACT(epoch FROM (nxt.ts_utc - prev.ts_utc)), (0)::numeric)))))
                    WHEN (prev.ts_utc IS NOT NULL) THEN (prev.weight_muscle_g)::numeric
                    WHEN (nxt.ts_utc IS NOT NULL) THEN (nxt.weight_muscle_g)::numeric
                    ELSE NULL::numeric
                END, NULL::numeric))::integer AS interpolated_muscle_g
           FROM ((activities.activities a
             LEFT JOIN LATERAL ( SELECT body_composition_raw.ts_utc,
                    body_composition_raw.weight_total_g,
                    body_composition_raw.weight_muscle_g,
                    body_composition_raw.fat_pct
                   FROM health.body_composition_raw
                  WHERE (body_composition_raw.ts_utc <= a.start_time_utc)
                  ORDER BY body_composition_raw.ts_utc DESC
                 LIMIT 1) prev ON (true))
             LEFT JOIN LATERAL ( SELECT body_composition_raw.ts_utc,
                    body_composition_raw.weight_total_g,
                    body_composition_raw.weight_muscle_g,
                    body_composition_raw.fat_pct
                   FROM health.body_composition_raw
                  WHERE (body_composition_raw.ts_utc > a.start_time_utc)
                  ORDER BY body_composition_raw.ts_utc
                 LIMIT 1) nxt ON (true))
        )
 SELECT activity_id,
    public.g2lb((interpolated_weight_g)::numeric) AS weight_lb,
    public.g2lb((interpolated_muscle_g)::numeric) AS muscle_lb,
    public.g2lb(((interpolated_weight_g)::numeric * interpolated_fat_pct)) AS fat_lb
   FROM interpolations;


--
-- Name: COLUMN vw_activity_weights.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_weights.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_weights.weight_lb; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_weights.weight_lb IS 'Column weight_lb.';


--
-- Name: COLUMN vw_activity_weights.muscle_lb; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_weights.muscle_lb IS 'Column muscle_lb.';


--
-- Name: COLUMN vw_activity_weights.fat_lb; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_weights.fat_lb IS 'Column fat_lb.';


--
-- Name: vw_segments_details_ranked; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segments_details_ranked AS
 WITH most_recent_training_cycle AS (
         SELECT max(training_cycle_dates.cycle_start_utc) AS mrtc
           FROM activities.training_cycle_dates
        )
 SELECT segment_id,
    activity_id,
        CASE
            WHEN (start_time_utc > ( SELECT most_recent_training_cycle.mrtc
               FROM most_recent_training_cycle)) THEN 'Current Cycle'::text
            WHEN (start_time_utc > (CURRENT_TIMESTAMP - '365 days'::interval)) THEN 'Last 365'::text
            ELSE 'All Time'::text
        END AS cycle_name,
    activity_start_point,
    activity_end_point,
    row_number() OVER (PARTITION BY segment_id ORDER BY start_time_utc DESC) AS recency_rank,
    row_number() OVER (PARTITION BY segment_id ORDER BY elapsed_duration_s) AS all_time_rank,
    row_number() OVER (PARTITION BY segment_id ORDER BY (start_time_utc > (CURRENT_TIMESTAMP - '365 days'::interval)) DESC, elapsed_duration_s) AS last_365_rank,
    row_number() OVER (PARTITION BY segment_id ORDER BY (start_time_utc > ( SELECT most_recent_training_cycle.mrtc
           FROM most_recent_training_cycle)) DESC, elapsed_duration_s) AS current_cycle_rank,
    start_time_utc,
    public.m2mi((distance_m)::numeric) AS distance_mi,
    elapsed_duration_s,
    max_hr,
    avg_hr,
    avg_cadence,
    avg_temp,
    avg_vert_osc,
        CASE
            WHEN (avg_vert_ratio < (0)::numeric) THEN NULL::numeric
            ELSE avg_vert_ratio
        END AS avg_vert_ratio,
    avg_gct,
    avg_perf,
    avg_balance,
    avg_stride_length,
    segment_path
   FROM activities.segments_details;


--
-- Name: COLUMN vw_segments_details_ranked.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segments_details_ranked.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segments_details_ranked.cycle_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.cycle_name IS 'Column cycle_name.';


--
-- Name: COLUMN vw_segments_details_ranked.activity_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN vw_segments_details_ranked.activity_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN vw_segments_details_ranked.recency_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.recency_rank IS 'Column recency_rank.';


--
-- Name: COLUMN vw_segments_details_ranked.all_time_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.all_time_rank IS 'Column all_time_rank.';


--
-- Name: COLUMN vw_segments_details_ranked.last_365_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.last_365_rank IS 'Column last_365_rank.';


--
-- Name: COLUMN vw_segments_details_ranked.current_cycle_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.current_cycle_rank IS 'Column current_cycle_rank.';


--
-- Name: COLUMN vw_segments_details_ranked.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_segments_details_ranked.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_segments_details_ranked.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_segments_details_ranked.max_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.max_hr IS 'Column max_hr.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_hr IS 'Column avg_hr.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_cadence IS 'Column avg_cadence.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_temp; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_temp IS 'Column avg_temp.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_vert_osc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_vert_osc IS 'Column avg_vert_osc.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_vert_ratio IS 'Column avg_vert_ratio.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_gct IS 'Column avg_gct.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_perf; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_perf IS 'Column avg_perf.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_balance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_balance IS 'Column avg_balance.';


--
-- Name: COLUMN vw_segments_details_ranked.avg_stride_length; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.avg_stride_length IS 'Column avg_stride_length.';


--
-- Name: COLUMN vw_segments_details_ranked.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_details_ranked.segment_path IS 'Column segment_path.';


--
-- Name: vw_segment_leaderboard; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_leaderboard AS
 WITH labels AS (
         SELECT r.segment_id,
            s.is_course,
            s.segment_name,
            r.activity_id,
            r.activity_start_point,
            r.activity_end_point,
            r.cycle_name,
            w.weight_lb,
            w.muscle_lb,
            w.fat_lb,
            r.distance_mi,
            r.elapsed_duration_s,
            r.start_time_utc,
            r.all_time_rank,
            r.last_365_rank,
            r.current_cycle_rank,
            r.recency_rank,
            ts.vo2_max_value,
            ts.altitude_acclimation_m,
            ts.heat_acclimation_pct,
            ts.training_load_acute,
            ts.training_readiness,
            ts.training_load_pct,
            ts.training_load_mid,
            ts.training_load_max,
            ts.training_load_min,
            ts.resting_hr_asleep,
            ts.resting_hr_awake,
            ts.sleep_duration_s,
            ts.sleep_score,
            ts.light_sleep_s,
            ts.deep_sleep_s,
            ts.awake_sleep_s,
            ts.rem_sleep_s,
            ts.awake_s_before_activity,
            r.max_hr,
            r.avg_hr,
            r.avg_cadence,
            r.avg_temp,
            r.avg_vert_osc,
            r.avg_vert_ratio,
            r.avg_gct,
            r.avg_perf,
            r.avg_balance,
            r.avg_stride_length
           FROM (((activities.vw_segments_details_ranked r
             JOIN activities.segments s ON ((r.segment_id = s.segment_id)))
             LEFT JOIN activities.vw_activity_weights w ON ((w.activity_id = r.activity_id)))
             LEFT JOIN activities.vw_activity_training_status ts ON ((r.activity_id = ts.activity_id)))
        ), final_sql AS (
         SELECT l.segment_id,
            l.is_course,
            l.segment_name,
            l.activity_id,
            l.activity_start_point,
            l.activity_end_point,
            l.cycle_name,
            l.weight_lb,
            l.muscle_lb,
            l.fat_lb,
            l.distance_mi,
            l.elapsed_duration_s,
            public.pace_as_text(l.distance_mi, l.elapsed_duration_s) AS pace_str,
            l.start_time_utc,
            l.all_time_rank,
            l.last_365_rank,
            l.current_cycle_rank,
            l.recency_rank,
            l.vo2_max_value,
            l.altitude_acclimation_m,
            l.heat_acclimation_pct,
            l.training_load_acute,
            l.training_readiness,
            l.training_load_pct,
            l.training_load_mid,
            l.training_load_max,
            l.training_load_min,
            l.resting_hr_asleep,
            l.resting_hr_awake,
            l.sleep_duration_s,
            l.sleep_score,
            l.light_sleep_s,
            l.deep_sleep_s,
            l.awake_sleep_s,
            l.rem_sleep_s,
            l.awake_s_before_activity,
            l.max_hr,
            l.avg_hr,
            l.avg_cadence,
            l.avg_temp,
            l.avg_vert_osc,
            l.avg_vert_ratio,
            l.avg_gct,
            l.avg_perf,
            l.avg_balance,
            l.avg_stride_length
           FROM labels l
        )
 SELECT segment_id,
    is_course,
    segment_name,
    cycle_name,
    activity_id,
    activity_start_point,
    activity_end_point,
    distance_mi,
    start_time_utc,
    all_time_rank,
    last_365_rank,
    current_cycle_rank,
    recency_rank,
    elapsed_duration_s,
    pace_str,
    weight_lb,
        CASE
            WHEN (COALESCE(weight_lb, (0)::numeric) > (0)::numeric) THEN round((muscle_lb / weight_lb), 3)
            ELSE NULL::numeric
        END AS muscle_pct,
        CASE
            WHEN (COALESCE(weight_lb, (0)::numeric) > (0)::numeric) THEN round((fat_lb / weight_lb), 3)
            ELSE NULL::numeric
        END AS fat_pct,
    vo2_max_value,
    altitude_acclimation_m,
    heat_acclimation_pct,
    training_load_acute,
    training_load_pct,
    training_readiness,
    training_load_mid,
    training_load_max,
    training_load_min,
    resting_hr_asleep,
    resting_hr_awake,
    sleep_duration_s,
    round(((sleep_duration_s)::numeric / 3600.0), 2) AS sleep_hours,
    sleep_score,
    light_sleep_s,
    deep_sleep_s,
    awake_sleep_s,
    rem_sleep_s,
    round((awake_s_before_activity / 3600.0), 2) AS awake_hours,
    max_hr,
    avg_hr,
    avg_cadence,
    avg_temp,
    avg_vert_osc,
    avg_vert_ratio,
    avg_gct,
    avg_perf,
    avg_balance,
    avg_stride_length
   FROM final_sql f;


--
-- Name: COLUMN vw_segment_leaderboard.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segment_leaderboard.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_segment_leaderboard.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_segment_leaderboard.cycle_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.cycle_name IS 'Column cycle_name.';


--
-- Name: COLUMN vw_segment_leaderboard.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segment_leaderboard.activity_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN vw_segment_leaderboard.activity_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN vw_segment_leaderboard.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_segment_leaderboard.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_segment_leaderboard.all_time_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.all_time_rank IS 'Column all_time_rank.';


--
-- Name: COLUMN vw_segment_leaderboard.last_365_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.last_365_rank IS 'Column last_365_rank.';


--
-- Name: COLUMN vw_segment_leaderboard.current_cycle_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.current_cycle_rank IS 'Column current_cycle_rank.';


--
-- Name: COLUMN vw_segment_leaderboard.recency_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.recency_rank IS 'Column recency_rank.';


--
-- Name: COLUMN vw_segment_leaderboard.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_segment_leaderboard.pace_str; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.pace_str IS 'Column pace_str.';


--
-- Name: COLUMN vw_segment_leaderboard.weight_lb; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.weight_lb IS 'Column weight_lb.';


--
-- Name: COLUMN vw_segment_leaderboard.muscle_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.muscle_pct IS 'Column muscle_pct.';


--
-- Name: COLUMN vw_segment_leaderboard.fat_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.fat_pct IS 'Column fat_pct.';


--
-- Name: COLUMN vw_segment_leaderboard.vo2_max_value; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.vo2_max_value IS 'Column vo2_max_value.';


--
-- Name: COLUMN vw_segment_leaderboard.altitude_acclimation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.altitude_acclimation_m IS 'Column altitude_acclimation_m.';


--
-- Name: COLUMN vw_segment_leaderboard.heat_acclimation_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.heat_acclimation_pct IS 'Column heat_acclimation_pct.';


--
-- Name: COLUMN vw_segment_leaderboard.training_load_acute; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_load_acute IS 'Column training_load_acute.';


--
-- Name: COLUMN vw_segment_leaderboard.training_load_pct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_load_pct IS 'Column training_load_pct.';


--
-- Name: COLUMN vw_segment_leaderboard.training_readiness; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_readiness IS 'Column training_readiness.';


--
-- Name: COLUMN vw_segment_leaderboard.training_load_mid; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_load_mid IS 'Column training_load_mid.';


--
-- Name: COLUMN vw_segment_leaderboard.training_load_max; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_load_max IS 'Column training_load_max.';


--
-- Name: COLUMN vw_segment_leaderboard.training_load_min; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.training_load_min IS 'Column training_load_min.';


--
-- Name: COLUMN vw_segment_leaderboard.resting_hr_asleep; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.resting_hr_asleep IS 'Column resting_hr_asleep.';


--
-- Name: COLUMN vw_segment_leaderboard.resting_hr_awake; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.resting_hr_awake IS 'Column resting_hr_awake.';


--
-- Name: COLUMN vw_segment_leaderboard.sleep_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.sleep_duration_s IS 'Column sleep_duration_s.';


--
-- Name: COLUMN vw_segment_leaderboard.sleep_hours; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.sleep_hours IS 'Column sleep_hours.';


--
-- Name: COLUMN vw_segment_leaderboard.sleep_score; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.sleep_score IS 'Column sleep_score.';


--
-- Name: COLUMN vw_segment_leaderboard.light_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.light_sleep_s IS 'Column light_sleep_s.';


--
-- Name: COLUMN vw_segment_leaderboard.deep_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.deep_sleep_s IS 'Column deep_sleep_s.';


--
-- Name: COLUMN vw_segment_leaderboard.awake_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.awake_sleep_s IS 'Column awake_sleep_s.';


--
-- Name: COLUMN vw_segment_leaderboard.rem_sleep_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.rem_sleep_s IS 'Column rem_sleep_s.';


--
-- Name: COLUMN vw_segment_leaderboard.awake_hours; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.awake_hours IS 'Column awake_hours.';


--
-- Name: COLUMN vw_segment_leaderboard.max_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.max_hr IS 'Column max_hr.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_hr; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_hr IS 'Column avg_hr.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_cadence IS 'Column avg_cadence.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_temp; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_temp IS 'Column avg_temp.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_vert_osc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_vert_osc IS 'Column avg_vert_osc.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_vert_ratio IS 'Column avg_vert_ratio.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_gct IS 'Column avg_gct.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_perf; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_perf IS 'Column avg_perf.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_balance; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_balance IS 'Column avg_balance.';


--
-- Name: COLUMN vw_segment_leaderboard.avg_stride_length; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_leaderboard.avg_stride_length IS 'Column avg_stride_length.';


--
-- Name: vw_activity_summary; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_summary AS
 SELECT a.activity_id,
    a.start_time_utc AS activity_start_utc,
    public.m2mi(a.distance_m) AS distance_mi,
    public.pace_as_text(public.m2mi(a.distance_m), a.duration_elapsed_s) AS activity_pace,
    lrd.segment_name,
    lrd.segment_id,
    lrd.is_course,
    lrd.elapsed_duration_s AS activity_time_s,
    lrd.distance_mi AS segment_distance_mi,
    lrd.all_time_rank,
    best.elapsed_duration_s AS best_time_s,
    prior.elapsed_duration_s AS prior_time_s,
    (best.elapsed_duration_s - lrd.elapsed_duration_s) AS best_delta_s,
    (prior.elapsed_duration_s - lrd.elapsed_duration_s) AS prior_delta_s,
    public.pace_as_text(lrd.distance_mi, lrd.elapsed_duration_s) AS segment_pace
   FROM (((activities.activities a
     LEFT JOIN activities.vw_segment_leaderboard lrd ON ((a.activity_id = lrd.activity_id)))
     LEFT JOIN activities.vw_segment_leaderboard best ON (((best.segment_id = lrd.segment_id) AND (best.all_time_rank = 1))))
     LEFT JOIN activities.vw_segment_leaderboard prior ON (((prior.segment_id = lrd.segment_id) AND (prior.recency_rank = (lrd.recency_rank + 1)))))
  ORDER BY lrd.is_course DESC, lrd.start_time_utc;


--
-- Name: COLUMN vw_activity_summary.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_summary.activity_start_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.activity_start_utc IS 'Column activity_start_utc.';


--
-- Name: COLUMN vw_activity_summary.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_activity_summary.activity_pace; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.activity_pace IS 'Column activity_pace.';


--
-- Name: COLUMN vw_activity_summary.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_activity_summary.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_activity_summary.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_activity_summary.activity_time_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.activity_time_s IS 'Column activity_time_s.';


--
-- Name: COLUMN vw_activity_summary.segment_distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.segment_distance_mi IS 'Column segment_distance_mi.';


--
-- Name: COLUMN vw_activity_summary.all_time_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.all_time_rank IS 'Column all_time_rank.';


--
-- Name: COLUMN vw_activity_summary.best_time_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.best_time_s IS 'Column best_time_s.';


--
-- Name: COLUMN vw_activity_summary.prior_time_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.prior_time_s IS 'Column prior_time_s.';


--
-- Name: COLUMN vw_activity_summary.best_delta_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.best_delta_s IS 'Column best_delta_s.';


--
-- Name: COLUMN vw_activity_summary.prior_delta_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.prior_delta_s IS 'Column prior_delta_s.';


--
-- Name: COLUMN vw_activity_summary.segment_pace; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary.segment_pace IS 'Column segment_pace.';


--
-- Name: vw_activity_summary_segment_creation; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_activity_summary_segment_creation AS
SELECT
    NULL::bigint AS activity_id,
    NULL::text AS activity_type_name,
    NULL::timestamp with time zone AS start_time_utc,
    NULL::numeric AS distance_mi,
    NULL::numeric(8,2) AS distance_m,
    NULL::text AS child_courses,
    NULL::bigint AS child_segments,
    NULL::bigint AS matched_courses,
    NULL::bigint AS matched_segments;


--
-- Name: COLUMN vw_activity_summary_segment_creation.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.activity_type_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.activity_type_name IS 'Column activity_type_name.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.child_courses; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.child_courses IS 'Column child_courses.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.child_segments; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.child_segments IS 'Column child_segments.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.matched_courses; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.matched_courses IS 'Column matched_courses.';


--
-- Name: COLUMN vw_activity_summary_segment_creation.matched_segments; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_activity_summary_segment_creation.matched_segments IS 'Column matched_segments.';


--
-- Name: vw_course_review; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_course_review AS
 SELECT s.segment_id,
    s.segment_name,
    s.activity_reference_id,
    s.reference_start_point,
    s.reference_end_point,
        CASE
            WHEN s.is_course THEN 'Course'::text
            ELSE 'Segment'::text
        END AS effort_label,
    a.activity_type_name,
    s.segment_path AS activity_path,
    round((s.distance_m * 0.000621371), 1) AS distance_mi,
    s.elevation_gain,
    s.elevation_loss,
    COALESCE(h.activity_count, (0)::bigint) AS matched_activities,
    h.last_event_utc,
    row_number() OVER (ORDER BY (s.segment_name ~~ 'New%'::text) DESC, COALESCE((h.last_event_utc)::timestamp without time zone, '1980-07-18 00:00:00'::timestamp without time zone) DESC, COALESCE(h.activity_count, (0)::bigint) DESC) AS course_idx,
    count(*) OVER () AS max_course_idx,
    sum(
        CASE
            WHEN (s.segment_name ~~ 'New%'::text) THEN 1
            ELSE 0
        END) OVER (PARTITION BY s.is_course) AS new_count
   FROM ((activities.segments s
     LEFT JOIN ( SELECT sm.segment_id,
            count(DISTINCT sm.activity_id) AS activity_count,
            (max(a_1.start_time_utc))::date AS last_event_utc
           FROM (activities.segment_matches sm
             JOIN activities.activities a_1 ON ((a_1.activity_id = sm.activity_id)))
          WHERE sm.match_confirmed
          GROUP BY sm.segment_id) h ON ((h.segment_id = s.segment_id)))
     LEFT JOIN activities.activities a ON ((a.activity_id = s.activity_reference_id)))
  ORDER BY s.is_course DESC, h.last_event_utc DESC;


--
-- Name: COLUMN vw_course_review.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_course_review.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_course_review.activity_reference_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.activity_reference_id IS 'Column activity_reference_id.';


--
-- Name: COLUMN vw_course_review.reference_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.reference_start_point IS 'Column reference_start_point.';


--
-- Name: COLUMN vw_course_review.reference_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.reference_end_point IS 'Column reference_end_point.';


--
-- Name: COLUMN vw_course_review.effort_label; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.effort_label IS 'Column effort_label.';


--
-- Name: COLUMN vw_course_review.activity_type_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.activity_type_name IS 'Column activity_type_name.';


--
-- Name: COLUMN vw_course_review.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN vw_course_review.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_course_review.elevation_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.elevation_gain IS 'Column elevation_gain.';


--
-- Name: COLUMN vw_course_review.elevation_loss; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.elevation_loss IS 'Column elevation_loss.';


--
-- Name: COLUMN vw_course_review.matched_activities; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.matched_activities IS 'Column matched_activities.';


--
-- Name: COLUMN vw_course_review.last_event_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.last_event_utc IS 'Column last_event_utc.';


--
-- Name: COLUMN vw_course_review.course_idx; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.course_idx IS 'Column course_idx.';


--
-- Name: COLUMN vw_course_review.max_course_idx; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.max_course_idx IS 'Column max_course_idx.';


--
-- Name: COLUMN vw_course_review.new_count; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_course_review.new_count IS 'Column new_count.';


--
-- Name: vw_elevation_smooth; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_elevation_smooth AS
 WITH pass1 AS (
         SELECT activity_details.activity_id,
            activity_details.latitude,
            activity_details.longitude,
            activity_details.elapsed_duration_s,
            activity_details.elevation_m AS elevation_raw,
            round(avg(activity_details.elevation_m) OVER (PARTITION BY activity_details.activity_id ORDER BY activity_details.elapsed_duration_s ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 1) AS elevation_m
           FROM activities.activity_details
        )
 SELECT activity_id,
    latitude,
    longitude,
    round(latitude, 4) AS lat5,
    round(longitude, 4) AS long5,
    elevation_raw,
    elevation_m,
    (public.st_setsrid(public.st_makepoint((longitude)::double precision, (latitude)::double precision), 4326))::public.geography AS geog
   FROM pass1;


--
-- Name: COLUMN vw_elevation_smooth.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_elevation_smooth.latitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.latitude IS 'Column latitude.';


--
-- Name: COLUMN vw_elevation_smooth.longitude; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.longitude IS 'Column longitude.';


--
-- Name: COLUMN vw_elevation_smooth.lat5; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.lat5 IS 'Column lat5.';


--
-- Name: COLUMN vw_elevation_smooth.long5; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.long5 IS 'Column long5.';


--
-- Name: COLUMN vw_elevation_smooth.elevation_raw; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.elevation_raw IS 'Column elevation_raw.';


--
-- Name: COLUMN vw_elevation_smooth.elevation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN vw_elevation_smooth.geog; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_elevation_smooth.geog IS 'Column geog.';


--
-- Name: vw_last_activity_id_by_type; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_last_activity_id_by_type AS
 SELECT max(activities.activity_id) AS activity_id,
    'Run'::text AS activity_type
   FROM activities.activities
  WHERE ((activities.activity_type_name ~~ '%running%'::text) AND (activities.distance_m > (1000)::numeric))
UNION ALL
 SELECT max(activities.activity_id) AS activity_id,
    'Walk'::text AS activity_type
   FROM activities.activities
  WHERE (((activities.activity_type_name ~~ '%walk%'::text) OR (activities.activity_type_name ~~ 'hik%'::text)) AND (activities.distance_m > (1500)::numeric));


--
-- Name: COLUMN vw_last_activity_id_by_type.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_last_activity_id_by_type.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_last_activity_id_by_type.activity_type; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_last_activity_id_by_type.activity_type IS 'Column activity_type.';


--
-- Name: vw_matched_segment_path_updates; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_matched_segment_path_updates AS
 SELECT s.segment_id,
    s.activity_id,
    public.st_setsrid(public.st_makeline(public.st_makepoint((ad.longitude)::double precision, (ad.latitude)::double precision, (ad.elevation_m_smooth)::double precision, (ad.elapsed_duration_s)::double precision) ORDER BY ad.elapsed_duration_s), 4326) AS activity_path
   FROM (activities.segment_matches s
     JOIN activities.activity_details ad ON (((s.activity_id = ad.activity_id) AND (s.activity_start_point <= ad.elapsed_duration_s) AND (s.activity_end_point >= ad.elapsed_duration_s))))
  GROUP BY s.segment_id, s.activity_id;


--
-- Name: COLUMN vw_matched_segment_path_updates.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_matched_segment_path_updates.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_matched_segment_path_updates.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_matched_segment_path_updates.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_matched_segment_path_updates.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_matched_segment_path_updates.activity_path IS 'Column activity_path.';


--
-- Name: vw_overlapped_segments; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_overlapped_segments AS
 WITH top_pair AS (
         SELECT q.segment_id_a,
            q.segment_id_b,
            q.match_confidence
           FROM activities.overlap_queue q
          ORDER BY q.segment_id_a, q.match_confidence DESC
         LIMIT 1
        ), pair_stats AS (
         SELECT sm.segment_id,
            count(*) FILTER (WHERE sm.match_confirmed) AS confirmed_cnt,
            count(*) FILTER (WHERE (NOT sm.match_confirmed)) AS unconfirmed_cnt,
            array_agg(sm.activity_id) AS activity_list
           FROM (activities.segment_matches sm
             JOIN top_pair tp_1 ON (((sm.segment_id = tp_1.segment_id_a) OR (sm.segment_id = tp_1.segment_id_b))))
          GROUP BY sm.segment_id
        )
 SELECT 1 AS match_rank,
    tp.match_confidence,
    s.segment_id,
    s.segment_name,
        CASE
            WHEN (s.segment_id < s_other.segment_id) THEN 'primary_segment'::text
            ELSE 'pretender_segment'::text
        END AS effort_label,
    s.segment_path AS activity_path,
    st.activity_list AS matched_activities
   FROM (((((top_pair tp
     CROSS JOIN LATERAL ( VALUES (tp.segment_id_a,tp.segment_id_b), (tp.segment_id_b,tp.segment_id_a)) pairs(my_id, other_id))
     JOIN activities.segments s ON ((s.segment_id = pairs.my_id)))
     JOIN activities.segments s_other ON ((s_other.segment_id = pairs.other_id)))
     LEFT JOIN pair_stats st ON ((st.segment_id = s.segment_id)))
     LEFT JOIN pair_stats st_other ON ((st_other.segment_id = s_other.segment_id)))
  ORDER BY s.segment_id;


--
-- Name: COLUMN vw_overlapped_segments.match_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.match_rank IS 'Column match_rank.';


--
-- Name: COLUMN vw_overlapped_segments.match_confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN vw_overlapped_segments.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_overlapped_segments.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_overlapped_segments.effort_label; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.effort_label IS 'Column effort_label.';


--
-- Name: COLUMN vw_overlapped_segments.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN vw_overlapped_segments.matched_activities; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_overlapped_segments.matched_activities IS 'Column matched_activities.';


--
-- Name: vw_possible_segment_matches; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_possible_segment_matches AS
 WITH sm AS (
         SELECT sm.activity_id,
            sm.activity_path,
            (a.start_time_utc)::date AS activity_date,
            sm.segment_id,
            s.segment_path,
            s.segment_name,
            sm.match_confidence,
            s.is_course,
            row_number() OVER (ORDER BY sm.match_confidence DESC) AS match_rank
           FROM ((activities.segment_matches sm
             JOIN activities.segments s ON ((s.segment_id = sm.segment_id)))
             JOIN activities.activities a ON ((sm.activity_id = a.activity_id)))
          WHERE (NOT sm.match_confirmed)
          ORDER BY sm.activity_id DESC, sm.match_confidence
        )
 SELECT segment_id,
    segment_name,
    activity_id,
    activity_date,
    match_confidence,
    match_rank,
    is_course,
    activity_path,
    effort_label
   FROM ( SELECT sm.segment_id,
            sm.segment_name,
            sm.activity_id,
            sm.activity_date,
            sm.match_confidence,
            sm.match_rank,
            sm.is_course,
            sm.segment_path AS activity_path,
            'segment'::text AS effort_label
           FROM sm
        UNION ALL
         SELECT sm.segment_id,
            sm.segment_name,
            sm.activity_id,
            sm.activity_date,
            sm.match_confidence,
            sm.match_rank,
            sm.is_course,
            sm.activity_path,
            'activity'::text AS effort_label
           FROM sm) subquery
  ORDER BY match_rank, effort_label DESC;


--
-- Name: COLUMN vw_possible_segment_matches.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_possible_segment_matches.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_possible_segment_matches.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_possible_segment_matches.activity_date; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.activity_date IS 'Column activity_date.';


--
-- Name: COLUMN vw_possible_segment_matches.match_confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN vw_possible_segment_matches.match_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.match_rank IS 'Column match_rank.';


--
-- Name: COLUMN vw_possible_segment_matches.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_possible_segment_matches.activity_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN vw_possible_segment_matches.effort_label; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_possible_segment_matches.effort_label IS 'Column effort_label.';


--
-- Name: vw_pr_hunter; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_pr_hunter AS
 WITH lbc AS (
         SELECT body_composition_raw.weight_total_g
           FROM health.body_composition_raw
          ORDER BY body_composition_raw.ts_utc DESC
         LIMIT 1
        ), lts AS (
         SELECT r.vo2_max_value AS curr_vo2,
            (r.training_load_acute)::numeric AS curr_load,
            (r.training_load_max)::numeric AS curr_load_max
           FROM health.training_status_raw r
          ORDER BY r.ts_utc DESC
         LIMIT 1
        ), curr AS (
         SELECT lts.curr_vo2,
            lts.curr_load,
            lts.curr_load_max,
            ( SELECT public.g2lb((lbc.weight_total_g)::numeric) AS g2lb
                   FROM lbc) AS current_weight
           FROM lts
        ), courses AS (
         SELECT s.segment_id,
            s.segment_name,
            (max(s.start_time_utc))::date AS last_effort,
            count(DISTINCT s.activity_id) AS effort_count,
            max(
                CASE
                    WHEN (s.recency_rank = 1) THEN s.elapsed_duration_s
                    ELSE NULL::numeric
                END) AS last_effort_s,
            max(
                CASE
                    WHEN (s.recency_rank = 1) THEN s.distance_mi
                    ELSE NULL::numeric
                END) AS last_effort_distance,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.elapsed_duration_s
                    ELSE NULL::numeric
                END) AS all_time_s,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.elapsed_duration_s
                    ELSE NULL::numeric
                END) AS last_365_s,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.elapsed_duration_s
                    ELSE NULL::numeric
                END) AS cycle_s,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.distance_mi
                    ELSE NULL::numeric
                END) AS all_time_distance_mi,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.distance_mi
                    ELSE NULL::numeric
                END) AS last_365_distance_mi,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.distance_mi
                    ELSE NULL::numeric
                END) AS cycle_distance_mi,
            (max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.start_time_utc
                    ELSE NULL::timestamp with time zone
                END))::date AS all_time_effort,
            (max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.start_time_utc
                    ELSE NULL::timestamp with time zone
                END))::date AS last_365_effort,
            (max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.start_time_utc
                    ELSE NULL::timestamp with time zone
                END))::date AS cycle_effort,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.weight_lb
                    ELSE NULL::numeric
                END) AS all_time_weight,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.weight_lb
                    ELSE NULL::numeric
                END) AS last_365_weight,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.weight_lb
                    ELSE NULL::numeric
                END) AS cycle_weight,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.vo2_max_value
                    ELSE NULL::numeric
                END) AS all_time_vo2,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.vo2_max_value
                    ELSE NULL::numeric
                END) AS last_365_vo2,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.vo2_max_value
                    ELSE NULL::numeric
                END) AS cycle_vo2,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.training_load_acute
                    ELSE NULL::numeric
                END) AS all_time_acute_load,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.training_load_acute
                    ELSE NULL::numeric
                END) AS last_365_acute_load,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.training_load_acute
                    ELSE NULL::numeric
                END) AS cycle_acute_load,
            max(
                CASE
                    WHEN (s.all_time_rank = 1) THEN s.training_load_max
                    ELSE NULL::numeric
                END) AS all_time_load_max,
            max(
                CASE
                    WHEN (s.last_365_rank = 1) THEN s.training_load_max
                    ELSE NULL::numeric
                END) AS last_365_load_max,
            max(
                CASE
                    WHEN (s.current_cycle_rank = 1) THEN s.training_load_max
                    ELSE NULL::numeric
                END) AS cycle_load_max
           FROM (activities.vw_segment_leaderboard s
             JOIN activities.activities a ON (((s.activity_id = a.activity_id) AND (a.activity_type_name ~~ '%run%'::text))))
          WHERE s.is_course
          GROUP BY s.segment_id, s.segment_name
        ), deltas AS (
         SELECT courses.segment_id,
            courses.segment_name,
            public.m2mi(segments.distance_m) AS segment_distance_mi,
            segments.elevation_gain AS segment_elevation_gain,
            courses.all_time_effort,
            courses.last_365_effort,
            courses.last_effort,
            courses.last_effort_s,
            courses.last_effort_distance,
            courses.cycle_effort,
            courses.all_time_s,
            courses.last_365_s,
            courses.cycle_s,
            courses.all_time_distance_mi,
            courses.last_365_distance_mi,
            courses.cycle_distance_mi,
            courses.effort_count,
            activities.safe_delta(curr.current_weight, courses.all_time_weight) AS wd_at,
            activities.safe_delta(curr.current_weight, courses.last_365_weight) AS wd_365,
            activities.safe_delta(curr.current_weight, courses.cycle_weight) AS wd_cycle,
            activities.safe_delta(curr.curr_vo2, courses.all_time_vo2) AS vo2_at,
            activities.safe_delta(curr.curr_vo2, courses.last_365_vo2) AS vo2_365,
            activities.safe_delta(curr.curr_vo2, courses.cycle_vo2) AS vo2_cycle,
            activities.safe_delta(curr.curr_load, courses.all_time_acute_load) AS acute_at,
            activities.safe_delta(curr.curr_load, courses.last_365_acute_load) AS acute_365,
            activities.safe_delta(curr.curr_load, courses.cycle_acute_load) AS acute_cycle,
            activities.safe_delta(curr.curr_load_max, courses.all_time_load_max) AS load_max_at,
            activities.safe_delta(curr.curr_load_max, courses.last_365_load_max) AS load_max_365,
            activities.safe_delta(curr.curr_load_max, courses.cycle_load_max) AS load_max_cycle
           FROM ((courses
             CROSS JOIN curr)
             JOIN activities.segments ON ((segments.segment_id = courses.segment_id)))
        ), comps AS (
         SELECT deltas.segment_id,
            deltas.segment_name,
            deltas.segment_distance_mi,
            deltas.segment_elevation_gain,
            deltas.last_effort,
            deltas.last_effort_s,
            deltas.last_effort_distance,
            deltas.effort_count,
            deltas.all_time_effort,
            deltas.last_365_effort,
            deltas.cycle_effort,
            deltas.all_time_s,
            deltas.last_365_s,
            deltas.cycle_s,
            deltas.all_time_distance_mi,
            deltas.last_365_distance_mi,
            deltas.cycle_distance_mi,
            activities.pr_potential_v1((deltas.wd_at)::double precision, (deltas.vo2_at)::double precision, (deltas.acute_at)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.all_time_s)::double precision) AS at_score_v1,
            activities.pr_potential_v1((deltas.wd_365)::double precision, (deltas.vo2_365)::double precision, (deltas.acute_365)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.last_365_s)::double precision) AS year_score_v1,
            activities.pr_potential_v1((deltas.wd_cycle)::double precision, (deltas.vo2_cycle)::double precision, (deltas.acute_cycle)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.cycle_s)::double precision) AS cycle_score_v1,
            activities.pr_potential_v2(deltas.wd_at, deltas.vo2_at, deltas.acute_at, deltas.load_max_at, deltas.segment_distance_mi, deltas.segment_elevation_gain, deltas.all_time_s) AS at_score_v2,
            activities.pr_potential_v2(deltas.wd_365, deltas.vo2_365, deltas.acute_365, deltas.load_max_365, deltas.segment_distance_mi, deltas.segment_elevation_gain, deltas.last_365_s) AS year_score_v2,
            activities.pr_potential_v2(deltas.wd_cycle, deltas.vo2_cycle, deltas.acute_cycle, deltas.load_max_cycle, deltas.segment_distance_mi, deltas.segment_elevation_gain, deltas.cycle_s) AS cycle_score_v2,
            activities.pr_potential_v3((deltas.wd_at)::double precision, (deltas.vo2_at)::double precision, (deltas.acute_at)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.all_time_s)::double precision) AS at_score_v3,
            activities.pr_potential_v3((deltas.wd_365)::double precision, (deltas.vo2_365)::double precision, (deltas.acute_365)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.last_365_s)::double precision) AS year_score_v3,
            activities.pr_potential_v3((deltas.wd_cycle)::double precision, (deltas.vo2_cycle)::double precision, (deltas.acute_cycle)::double precision, (deltas.segment_distance_mi)::double precision, (deltas.segment_elevation_gain)::double precision, (deltas.cycle_s)::double precision) AS cycle_score_v3,
            deltas.wd_at,
            deltas.wd_365,
            deltas.wd_cycle,
            deltas.vo2_at,
            deltas.vo2_365,
            deltas.vo2_cycle,
            deltas.acute_at,
            deltas.acute_365,
            deltas.acute_cycle,
            deltas.load_max_at,
            deltas.load_max_365,
            deltas.load_max_cycle
           FROM deltas
        ), precomps AS (
         SELECT comps.segment_id,
            comps.segment_name,
            comps.segment_distance_mi,
            comps.segment_elevation_gain,
            comps.effort_count AS total_attempts,
            (((comps.at_score_v1 + (comps.at_score_v2)::double precision) + comps.at_score_v3) / (3)::double precision) AS at_score_agg,
            comps.all_time_s,
            (((comps.year_score_v1 + (comps.year_score_v2)::double precision) + comps.year_score_v3) / (3)::double precision) AS year_score_agg,
            comps.last_365_s,
            (((comps.cycle_score_v1 + (comps.cycle_score_v2)::double precision) + comps.cycle_score_v3) / (3)::double precision) AS cycle_score_agg,
            comps.cycle_s,
            comps.vo2_at,
            comps.vo2_365,
            comps.vo2_cycle,
            comps.acute_at,
            comps.acute_365,
            comps.acute_cycle,
            comps.load_max_at,
            comps.load_max_365,
            comps.load_max_cycle,
            comps.wd_365,
            comps.wd_at,
            comps.wd_cycle,
            comps.all_time_effort,
            comps.last_365_effort,
            comps.cycle_effort,
            comps.last_effort
           FROM comps
        ), matches AS (
         SELECT precomps.segment_id,
            precomps.segment_name,
            precomps.segment_distance_mi,
            precomps.segment_elevation_gain,
            precomps.total_attempts,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN 'All-Time'::text
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN 'Year'::text
                    WHEN (precomps.cycle_score_agg < (precomps.cycle_s)::double precision) THEN 'Cycle'::text
                    ELSE 'Best effort'::text
                END AS attempt_label,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.all_time_effort
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.last_365_effort
                    WHEN (precomps.cycle_score_agg < (precomps.cycle_s)::double precision) THEN precomps.cycle_effort
                    ELSE precomps.last_effort
                END AS vs_date,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.all_time_s
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.last_365_s
                    ELSE precomps.cycle_s
                END AS vs_seconds,
            round((
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.at_score_agg
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.year_score_agg
                    ELSE precomps.cycle_score_agg
                END)::numeric, 3) AS predicted_time_s,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.wd_at
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.wd_365
                    ELSE precomps.wd_cycle
                END AS weight_delta,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.vo2_at
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.vo2_365
                    ELSE precomps.vo2_cycle
                END AS vo2_delta,
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.acute_at
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.acute_365
                    ELSE precomps.acute_cycle
                END AS acute_load_delta,
            round(
                CASE
                    WHEN (precomps.at_score_agg < (precomps.all_time_s)::double precision) THEN precomps.load_max_at
                    WHEN (precomps.year_score_agg < (precomps.last_365_s)::double precision) THEN precomps.load_max_365
                    ELSE precomps.load_max_cycle
                END, 3) AS load_max_delta
           FROM precomps
        )
 SELECT segment_id,
    segment_name,
    vs_date,
    attempt_label,
    total_attempts,
    predicted_time_s,
    public.pace_as_text(segment_distance_mi, vs_seconds) AS vs_pace,
    public.pace_as_text(segment_distance_mi, predicted_time_s) AS estimated_pace,
    (((vs_seconds - predicted_time_s) *
        CASE
            WHEN (attempt_label = 'All-Time'::text) THEN 1.15
            WHEN (attempt_label = 'Year'::text) THEN 1.05
            WHEN (attempt_label = 'Cycle'::text) THEN (1)::numeric
            ELSE 0.75
        END) * (((total_attempts % (5)::bigint) + 1))::numeric) AS prestige_score,
    round(weight_delta, 3) AS weight_delta,
    round(vo2_delta, 3) AS vo2_delta,
    round(acute_load_delta, 3) AS acute_load_delta,
    round(load_max_delta, 3) AS load_max_delta
   FROM matches;


--
-- Name: COLUMN vw_pr_hunter.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_pr_hunter.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_pr_hunter.vs_date; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.vs_date IS 'Column vs_date.';


--
-- Name: COLUMN vw_pr_hunter.attempt_label; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.attempt_label IS 'Column attempt_label.';


--
-- Name: COLUMN vw_pr_hunter.total_attempts; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.total_attempts IS 'Column total_attempts.';


--
-- Name: COLUMN vw_pr_hunter.predicted_time_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.predicted_time_s IS 'Column predicted_time_s.';


--
-- Name: COLUMN vw_pr_hunter.vs_pace; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.vs_pace IS 'Column vs_pace.';


--
-- Name: COLUMN vw_pr_hunter.estimated_pace; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.estimated_pace IS 'Column estimated_pace.';


--
-- Name: COLUMN vw_pr_hunter.prestige_score; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.prestige_score IS 'Column prestige_score.';


--
-- Name: COLUMN vw_pr_hunter.weight_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.weight_delta IS 'Column weight_delta.';


--
-- Name: COLUMN vw_pr_hunter.vo2_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.vo2_delta IS 'Column vo2_delta.';


--
-- Name: COLUMN vw_pr_hunter.acute_load_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.acute_load_delta IS 'Column acute_load_delta.';


--
-- Name: COLUMN vw_pr_hunter.load_max_delta; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_pr_hunter.load_max_delta IS 'Column load_max_delta.';


--
-- Name: vw_report_segment_efforts; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_report_segment_efforts AS
 SELECT sd.segment_id,
    s.segment_name,
    s.is_course,
    sd.activity_id,
    sd.start_time_utc,
    sd.elapsed_duration_s,
    NULLIF(sd.avg_cadence, '-99.0'::numeric) AS avg_cadence,
    NULLIF(sd.avg_stride_length, '-99.0'::numeric) AS avg_stride_length,
    NULLIF(sd.avg_gct, '-99.0'::numeric) AS avg_gct,
    NULLIF(sd.avg_vert_ratio, '-99.0'::numeric) AS avg_vert_ratio,
    ( SELECT (1 + count(*))
           FROM activities.segments_details f
          WHERE ((f.segment_id = sd.segment_id) AND (f.elapsed_duration_s < sd.elapsed_duration_s))) AS all_time_rank,
    ( SELECT count(*) AS count
           FROM activities.segments_details c
          WHERE (c.segment_id = sd.segment_id)) AS total_attempts,
    prior.elapsed_duration_s AS prior_time_s,
    best.elapsed_duration_s AS best_time_excl_s,
    (prior.elapsed_duration_s - sd.elapsed_duration_s) AS prior_delta_s,
    (best.elapsed_duration_s - sd.elapsed_duration_s) AS best_delta_s,
    p10.avg_cadence AS prior10_avg_cadence,
    p10.avg_stride_length AS prior10_avg_stride_length,
    p10.avg_gct AS prior10_avg_gct,
    p10.avg_vert_ratio AS prior10_avg_vert_ratio
   FROM ((((activities.segments_details sd
     JOIN activities.segments s ON ((s.segment_id = sd.segment_id)))
     LEFT JOIN LATERAL ( SELECT p.elapsed_duration_s
           FROM activities.segments_details p
          WHERE ((p.segment_id = sd.segment_id) AND (p.start_time_utc < sd.start_time_utc))
          ORDER BY p.start_time_utc DESC
         LIMIT 1) prior ON (true))
     CROSS JOIN LATERAL ( SELECT min(b.elapsed_duration_s) AS elapsed_duration_s
           FROM activities.segments_details b
          WHERE ((b.segment_id = sd.segment_id) AND (b.activity_id <> sd.activity_id))) best)
     CROSS JOIN LATERAL ( SELECT avg(x.avg_cadence) AS avg_cadence,
            avg(x.avg_stride_length) AS avg_stride_length,
            avg(x.avg_gct) AS avg_gct,
            avg(x.avg_vert_ratio) AS avg_vert_ratio
           FROM ( SELECT NULLIF(p2.avg_cadence, '-99.0'::numeric) AS avg_cadence,
                    NULLIF(p2.avg_stride_length, '-99.0'::numeric) AS avg_stride_length,
                    NULLIF(p2.avg_gct, '-99.0'::numeric) AS avg_gct,
                    NULLIF(p2.avg_vert_ratio, '-99.0'::numeric) AS avg_vert_ratio
                   FROM activities.segments_details p2
                  WHERE ((p2.segment_id = sd.segment_id) AND (p2.start_time_utc < sd.start_time_utc))
                  ORDER BY p2.start_time_utc DESC
                 LIMIT 10) x) p10);


--
-- Name: COLUMN vw_report_segment_efforts.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_report_segment_efforts.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_report_segment_efforts.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_report_segment_efforts.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_report_segment_efforts.start_time_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.start_time_utc IS 'Column start_time_utc.';


--
-- Name: COLUMN vw_report_segment_efforts.elapsed_duration_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_report_segment_efforts.avg_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.avg_cadence IS 'Column avg_cadence.';


--
-- Name: COLUMN vw_report_segment_efforts.avg_stride_length; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.avg_stride_length IS 'Column avg_stride_length.';


--
-- Name: COLUMN vw_report_segment_efforts.avg_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.avg_gct IS 'Column avg_gct.';


--
-- Name: COLUMN vw_report_segment_efforts.avg_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.avg_vert_ratio IS 'Column avg_vert_ratio.';


--
-- Name: COLUMN vw_report_segment_efforts.all_time_rank; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.all_time_rank IS 'Column all_time_rank.';


--
-- Name: COLUMN vw_report_segment_efforts.total_attempts; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.total_attempts IS 'Column total_attempts.';


--
-- Name: COLUMN vw_report_segment_efforts.prior_time_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior_time_s IS 'Column prior_time_s.';


--
-- Name: COLUMN vw_report_segment_efforts.best_time_excl_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.best_time_excl_s IS 'Column best_time_excl_s.';


--
-- Name: COLUMN vw_report_segment_efforts.prior_delta_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior_delta_s IS 'Column prior_delta_s.';


--
-- Name: COLUMN vw_report_segment_efforts.best_delta_s; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.best_delta_s IS 'Column best_delta_s.';


--
-- Name: COLUMN vw_report_segment_efforts.prior10_avg_cadence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior10_avg_cadence IS 'Column prior10_avg_cadence.';


--
-- Name: COLUMN vw_report_segment_efforts.prior10_avg_stride_length; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior10_avg_stride_length IS 'Column prior10_avg_stride_length.';


--
-- Name: COLUMN vw_report_segment_efforts.prior10_avg_gct; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior10_avg_gct IS 'Column prior10_avg_gct.';


--
-- Name: COLUMN vw_report_segment_efforts.prior10_avg_vert_ratio; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_report_segment_efforts.prior10_avg_vert_ratio IS 'Column prior10_avg_vert_ratio.';


--
-- Name: vw_run_timing; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_run_timing AS
 WITH last_run AS (
         SELECT a.activity_id,
            public.time_ago(a.start_time_utc) AS run_age,
            a.start_time_utc AS run_time_start,
            a.end_time_utc AS run_time_end
           FROM activities.activities a
          WHERE (a.activity_type_name ~~ '%running%'::text)
          ORDER BY a.activity_id DESC
         LIMIT 1
        ), possible_warmups AS (
         SELECT a.activity_id,
            a.start_time_utc AS warmup_start
           FROM (activities.activities a
             CROSS JOIN last_run l)
          WHERE ((a.activity_type_name = 'walking'::text) AND (a.end_time_utc > (l.run_time_start - '00:05:00'::interval)) AND (a.start_time_utc < l.run_time_start))
          ORDER BY a.start_time_utc DESC
         LIMIT 1
        ), possible_cooldowns AS (
         SELECT a.activity_id,
            a.end_time_utc AS cooldown_end
           FROM (activities.activities a
             CROSS JOIN last_run l)
          WHERE ((a.activity_type_name = 'walking'::text) AND (a.start_time_utc < (l.run_time_end + '00:05:00'::interval)) AND (a.start_time_utc > l.run_time_start))
          ORDER BY a.start_time_utc
         LIMIT 1
        )
 SELECT activity_id,
    run_age,
    music_start,
    music_end,
    (floor(EXTRACT(epoch FROM (music_end - music_start))) * (1000)::numeric) AS music_duration_ms
   FROM ( SELECT a.activity_id,
            a.run_age,
            a.run_time_start,
            a.run_time_end,
            COALESCE(w.warmup_start, a.run_time_start) AS music_start,
            COALESCE(c.cooldown_end, a.run_time_end) AS music_end
           FROM ((last_run a
             LEFT JOIN possible_warmups w ON ((1 = 1)))
             LEFT JOIN possible_cooldowns c ON ((1 = 1)))) sub;


--
-- Name: COLUMN vw_run_timing.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_run_timing.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_run_timing.run_age; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_run_timing.run_age IS 'Column run_age.';


--
-- Name: COLUMN vw_run_timing.music_start; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_run_timing.music_start IS 'Column music_start.';


--
-- Name: COLUMN vw_run_timing.music_end; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_run_timing.music_end IS 'Column music_end.';


--
-- Name: COLUMN vw_run_timing.music_duration_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_run_timing.music_duration_ms IS 'Column music_duration_ms.';


--
-- Name: vw_temp_segment_matches_downselect; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_temp_segment_matches_downselect AS
 SELECT tsm.activity_id,
    tsm.segment_id,
    (a.start_time_utc)::date AS effort_date,
    tsm.best_start_dist,
    tsm.best_end_dist,
    tsm.ruggedness,
    tsm.dist_deviation,
    round(tsm.polygon_deviation_m, 1) AS polygon_deviation_m,
    round(tsm.hausdorff_deviation_m, 1) AS hausdorff_deviation_m,
    round(tsm.freschet_deviation_m, 1) AS freschet_deviation_m,
    round(((((tsm.dist_deviation * (5)::numeric) + ((1)::numeric - tsm.ruggedness)) + (COALESCE(tsm.polygon_deviation_m, (10)::numeric) / (10)::numeric)) + ((COALESCE(tsm.hausdorff_deviation_m, (35)::numeric) / (35)::numeric) * (COALESCE(tsm.freschet_deviation_m, (50)::numeric) / (50)::numeric))), 2) AS confidence
   FROM (activities.temp_segment_matches_downselect tsm
     JOIN activities.activities a ON ((a.activity_id = tsm.activity_id)))
  ORDER BY (round(((((tsm.dist_deviation * (5)::numeric) + ((1)::numeric - tsm.ruggedness)) + (COALESCE(tsm.polygon_deviation_m, (10)::numeric) / (10)::numeric)) + ((COALESCE(tsm.hausdorff_deviation_m, (35)::numeric) / (35)::numeric) * (COALESCE(tsm.freschet_deviation_m, (50)::numeric) / (50)::numeric))), 2)) DESC;


--
-- Name: COLUMN vw_temp_segment_matches_downselect.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.effort_date; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.effort_date IS 'Column effort_date.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.best_start_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.best_start_dist IS 'Column best_start_dist.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.best_end_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.best_end_dist IS 'Column best_end_dist.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.ruggedness; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.ruggedness IS 'Column ruggedness.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.dist_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.dist_deviation IS 'Column dist_deviation.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.polygon_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.polygon_deviation_m IS 'Column polygon_deviation_m.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.hausdorff_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.hausdorff_deviation_m IS 'Column hausdorff_deviation_m.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.freschet_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.freschet_deviation_m IS 'Column freschet_deviation_m.';


--
-- Name: COLUMN vw_temp_segment_matches_downselect.confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_temp_segment_matches_downselect.confidence IS 'Column confidence.';


--
-- Name: vw_segment_create_auto_approve; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_create_auto_approve AS
 SELECT tsm.activity_id,
    tsm.segment_id,
    tsm.best_start_dist,
    tsm.best_end_dist,
    tsm.dist_deviation,
    tsm.polygon_deviation_m,
    tsm.hausdorff_deviation_m,
    tsm.freschet_deviation_m,
    tsm.confidence,
        CASE
            WHEN ((tsm.dist_deviation < 0.002) AND (tsm.polygon_deviation_m < (2)::numeric)) THEN 1
            WHEN (COALESCE(tsm.hausdorff_deviation_m, (999)::numeric) < (15)::numeric) THEN 2
            WHEN (COALESCE(tsm.freschet_deviation_m, (999)::numeric) < (15)::numeric) THEN 3
            ELSE 4
        END AS approval_phase
   FROM (activities.vw_temp_segment_matches_downselect tsm
     JOIN activities.segments s ON (((s.activity_reference_id <> tsm.activity_id) AND (s.segment_id = tsm.segment_id))));


--
-- Name: COLUMN vw_segment_create_auto_approve.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segment_create_auto_approve.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segment_create_auto_approve.best_start_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.best_start_dist IS 'Column best_start_dist.';


--
-- Name: COLUMN vw_segment_create_auto_approve.best_end_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.best_end_dist IS 'Column best_end_dist.';


--
-- Name: COLUMN vw_segment_create_auto_approve.dist_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.dist_deviation IS 'Column dist_deviation.';


--
-- Name: COLUMN vw_segment_create_auto_approve.polygon_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.polygon_deviation_m IS 'Column polygon_deviation_m.';


--
-- Name: COLUMN vw_segment_create_auto_approve.hausdorff_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.hausdorff_deviation_m IS 'Column hausdorff_deviation_m.';


--
-- Name: COLUMN vw_segment_create_auto_approve.freschet_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.freschet_deviation_m IS 'Column freschet_deviation_m.';


--
-- Name: COLUMN vw_segment_create_auto_approve.confidence; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.confidence IS 'Column confidence.';


--
-- Name: COLUMN vw_segment_create_auto_approve.approval_phase; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_auto_approve.approval_phase IS 'Column approval_phase.';


--
-- Name: vw_segment_create_from_details; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_create_from_details AS
 SELECT activity_id AS activity_reference_id,
    (min(distance_m))::integer AS reference_start_point,
    (max(distance_m))::integer AS reference_end_point,
    is_course,
    segment_name,
    public.st_setsrid(public.st_makeline(public.st_makepoint((longitude)::double precision, (latitude)::double precision, (elevation_m)::double precision, (distance_m)::double precision) ORDER BY elapsed_duration_s), 4326) AS segment_path,
    (max(distance_m) - min(distance_m)) AS distance_m,
    sum(
        CASE
            WHEN (elevation_delta > (0)::numeric) THEN elevation_delta
            ELSE (0)::numeric
        END) AS elevation_gain,
    sum(
        CASE
            WHEN (elevation_delta < (0)::numeric) THEN (- elevation_delta)
            ELSE (0)::numeric
        END) AS elevation_loss,
    (public.st_makepoint((max(
        CASE
            WHEN (row_asc = 1) THEN longitude
            ELSE NULL::numeric
        END))::double precision, (max(
        CASE
            WHEN (row_asc = 1) THEN latitude
            ELSE NULL::numeric
        END))::double precision))::public.geography(Point,4326) AS start_point,
    (public.st_makepoint((max(
        CASE
            WHEN (row_desc = 1) THEN longitude
            ELSE NULL::numeric
        END))::double precision, (max(
        CASE
            WHEN (row_desc = 1) THEN latitude
            ELSE NULL::numeric
        END))::double precision))::public.geography(Point,4326) AS end_point,
    activity_type_id
   FROM activities.temp_segment_details
  GROUP BY activity_id, activity_type_id, is_course, segment_name;


--
-- Name: COLUMN vw_segment_create_from_details.activity_reference_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.activity_reference_id IS 'Column activity_reference_id.';


--
-- Name: COLUMN vw_segment_create_from_details.reference_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.reference_start_point IS 'Column reference_start_point.';


--
-- Name: COLUMN vw_segment_create_from_details.reference_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.reference_end_point IS 'Column reference_end_point.';


--
-- Name: COLUMN vw_segment_create_from_details.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_segment_create_from_details.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_segment_create_from_details.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.segment_path IS 'Column segment_path.';


--
-- Name: COLUMN vw_segment_create_from_details.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN vw_segment_create_from_details.elevation_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.elevation_gain IS 'Column elevation_gain.';


--
-- Name: COLUMN vw_segment_create_from_details.elevation_loss; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.elevation_loss IS 'Column elevation_loss.';


--
-- Name: COLUMN vw_segment_create_from_details.start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.start_point IS 'Column start_point.';


--
-- Name: COLUMN vw_segment_create_from_details.end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.end_point IS 'Column end_point.';


--
-- Name: COLUMN vw_segment_create_from_details.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_create_from_details.activity_type_id IS 'Column activity_type_id.';


--
-- Name: vw_segment_matching_downselect_confidence; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_matching_downselect_confidence AS
 WITH calcs AS (
         SELECT temp_segment_matches_downselect.activity_id,
            temp_segment_matches_downselect.best_start_dist,
            temp_segment_matches_downselect.best_end_dist,
            temp_segment_matches_downselect.dist_deviation,
            temp_segment_matches_downselect.polygon_deviation_m,
            temp_segment_matches_downselect.freschet_deviation_m,
            temp_segment_matches_downselect.hausdorff_deviation_m,
            ((((4 +
                CASE
                    WHEN (temp_segment_matches_downselect.dist_deviation < 0.003) THEN 1
                    WHEN (temp_segment_matches_downselect.dist_deviation > 0.005) THEN '-1'::integer
                    ELSE 0
                END) +
                CASE
                    WHEN (temp_segment_matches_downselect.polygon_deviation_m IS NULL) THEN 0
                    WHEN (temp_segment_matches_downselect.polygon_deviation_m <= (2)::numeric) THEN 1
                    WHEN (temp_segment_matches_downselect.polygon_deviation_m >= (7)::numeric) THEN '-1'::integer
                    ELSE 0
                END) +
                CASE
                    WHEN (temp_segment_matches_downselect.hausdorff_deviation_m IS NULL) THEN 0
                    WHEN (temp_segment_matches_downselect.hausdorff_deviation_m <= (5)::numeric) THEN 1
                    WHEN (temp_segment_matches_downselect.hausdorff_deviation_m >= (50)::numeric) THEN '-2'::integer
                    WHEN (temp_segment_matches_downselect.hausdorff_deviation_m >= (20)::numeric) THEN '-1'::integer
                    ELSE 0
                END) +
                CASE
                    WHEN (temp_segment_matches_downselect.freschet_deviation_m IS NULL) THEN 0
                    WHEN (temp_segment_matches_downselect.freschet_deviation_m <= (5)::numeric) THEN 1
                    WHEN (temp_segment_matches_downselect.freschet_deviation_m >= (50)::numeric) THEN '-2'::integer
                    WHEN (temp_segment_matches_downselect.freschet_deviation_m >= (20)::numeric) THEN '-1'::integer
                    ELSE 0
                END) AS confidence_score
           FROM activities.temp_segment_matches_downselect
        )
 SELECT (confidence_score >= 6) AS confirmed,
    activity_id,
    best_start_dist,
    best_end_dist,
    dist_deviation,
    polygon_deviation_m,
    freschet_deviation_m,
    hausdorff_deviation_m,
    confidence_score
   FROM calcs
  ORDER BY confidence_score DESC;


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.confirmed; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.confirmed IS 'Column confirmed.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.best_start_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.best_start_dist IS 'Column best_start_dist.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.best_end_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.best_end_dist IS 'Column best_end_dist.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.dist_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.dist_deviation IS 'Column dist_deviation.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.polygon_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.polygon_deviation_m IS 'Column polygon_deviation_m.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.freschet_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.freschet_deviation_m IS 'Column freschet_deviation_m.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.hausdorff_deviation_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.hausdorff_deviation_m IS 'Column hausdorff_deviation_m.';


--
-- Name: COLUMN vw_segment_matching_downselect_confidence.confidence_score; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_confidence.confidence_score IS 'Column confidence_score.';


--
-- Name: vw_segment_matching_downselect_scores; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_matching_downselect_scores AS
 WITH calcs AS (
         SELECT t.activity_id,
            t.best_start_dist,
            t.best_end_dist,
            round((abs(((s.distance_m)::numeric - ((t.best_end_dist - t.best_start_dist))::numeric)) / (s.distance_m)::numeric), 3) AS dist_deviation,
            abs((public.st_zmax((t.full_path)::public.box3d) - public.st_zmin((t.full_path)::public.box3d))) AS match_range,
            abs((public.st_zmax((s.segment_path)::public.box3d) - public.st_zmin((s.segment_path)::public.box3d))) AS source_range
           FROM (activities.temp_segment_matches_downselect t
             JOIN activities.segments s ON ((s.segment_id = t.segment_id)))
        )
 SELECT activity_id,
    best_start_dist,
    best_end_dist,
    dist_deviation,
    ((1)::double precision - LEAST((1)::double precision, (abs((match_range - source_range)) / NULLIF(source_range, (0)::double precision)))) AS ruggedness
   FROM calcs;


--
-- Name: COLUMN vw_segment_matching_downselect_scores.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_scores.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segment_matching_downselect_scores.best_start_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_scores.best_start_dist IS 'Column best_start_dist.';


--
-- Name: COLUMN vw_segment_matching_downselect_scores.best_end_dist; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_scores.best_end_dist IS 'Column best_end_dist.';


--
-- Name: COLUMN vw_segment_matching_downselect_scores.dist_deviation; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_scores.dist_deviation IS 'Column dist_deviation.';


--
-- Name: COLUMN vw_segment_matching_downselect_scores.ruggedness; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_matching_downselect_scores.ruggedness IS 'Column ruggedness.';


--
-- Name: vw_segment_path_updates; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_path_updates AS
 SELECT traj.segment_id,
    traj.activity_reference_id,
    traj.activity_type_id,
    traj.reference_start_point,
    traj.reference_end_point,
    traj.segment_path,
    (round((public.st_length((traj.segment_path)::public.geography, true))::numeric, 2))::numeric(8,2) AS distance_m,
    round(COALESCE(deltas.ascent_m, (0)::numeric), 2) AS ascent_m,
    round(COALESCE(deltas.descent_m, (0)::numeric), 2) AS descent_m,
    public.st_startpoint(traj.segment_path) AS start_point,
    public.st_endpoint(traj.segment_path) AS end_point
   FROM (( SELECT s.segment_id,
            a.activity_id AS activity_reference_id,
            a.activity_type_id,
            s.reference_start_point,
            s.reference_end_point,
            public.st_setsrid(public.st_makeline(public.st_makepoint((rd.longitude)::double precision, (rd.latitude)::double precision, (rd.elevation_m_smooth)::double precision, (rd.elapsed_duration_s)::double precision) ORDER BY rd.elapsed_duration_s), 4326) AS segment_path
           FROM ((activities.activities a
             JOIN activities.segments s ON ((a.activity_id = s.activity_reference_id)))
             JOIN activities.activity_details rd ON (((rd.activity_id = a.activity_id) AND (rd.elapsed_duration_s >= s.reference_start_point) AND (rd.elapsed_duration_s <= s.reference_end_point))))
          GROUP BY s.segment_id, a.activity_id, a.activity_type_id, s.reference_start_point, s.reference_end_point) traj
     JOIN ( SELECT aed.activity_id,
            sum(
                CASE
                    WHEN (aed.elevation_delta > (0)::numeric) THEN aed.elevation_delta
                    ELSE (0)::numeric
                END) AS ascent_m,
            sum(
                CASE
                    WHEN (aed.elevation_delta < (0)::numeric) THEN aed.elevation_delta
                    ELSE (0)::numeric
                END) AS descent_m
           FROM (activities.vw_activity_elevation_deltas aed
             JOIN activities.segments s ON ((aed.activity_id = s.activity_reference_id)))
          WHERE ((aed.elevation_delta <> (0)::numeric) AND (aed.elapsed_duration_s >= s.reference_start_point) AND (aed.elapsed_duration_s <= s.reference_end_point))
          GROUP BY aed.activity_id) deltas ON ((traj.activity_reference_id = deltas.activity_id)));


--
-- Name: COLUMN vw_segment_path_updates.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segment_path_updates.activity_reference_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.activity_reference_id IS 'Column activity_reference_id.';


--
-- Name: COLUMN vw_segment_path_updates.activity_type_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN vw_segment_path_updates.reference_start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.reference_start_point IS 'Column reference_start_point.';


--
-- Name: COLUMN vw_segment_path_updates.reference_end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.reference_end_point IS 'Column reference_end_point.';


--
-- Name: COLUMN vw_segment_path_updates.segment_path; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.segment_path IS 'Column segment_path.';


--
-- Name: COLUMN vw_segment_path_updates.distance_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.distance_m IS 'Column distance_m.';


--
-- Name: COLUMN vw_segment_path_updates.ascent_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.ascent_m IS 'Column ascent_m.';


--
-- Name: COLUMN vw_segment_path_updates.descent_m; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.descent_m IS 'Column descent_m.';


--
-- Name: COLUMN vw_segment_path_updates.start_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.start_point IS 'Column start_point.';


--
-- Name: COLUMN vw_segment_path_updates.end_point; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_path_updates.end_point IS 'Column end_point.';


--
-- Name: vw_segment_racing; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segment_racing AS
 SELECT s.segment_id,
    s.segment_name,
    public.m2mi(s.distance_m) AS distance_mi,
    a.effort_count,
    a.last_effort,
    a.last_id AS activity_id,
    s.is_course
   FROM (activities.segments s
     JOIN ( SELECT sm.segment_id,
            count(DISTINCT sm.activity_id) AS effort_count,
            max(a_1.start_time_utc) AS last_effort,
            max(sm.activity_id) AS last_id
           FROM (activities.segment_matches sm
             JOIN activities.activities a_1 ON ((a_1.activity_id = sm.activity_id)))
          WHERE sm.match_confirmed
          GROUP BY sm.segment_id) a ON (((a.segment_id = s.segment_id) AND (a.effort_count > 1))))
  ORDER BY a.last_effort DESC, a.effort_count DESC;


--
-- Name: COLUMN vw_segment_racing.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segment_racing.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_segment_racing.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_segment_racing.effort_count; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.effort_count IS 'Column effort_count.';


--
-- Name: COLUMN vw_segment_racing.last_effort; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.last_effort IS 'Column last_effort.';


--
-- Name: COLUMN vw_segment_racing.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_segment_racing.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segment_racing.is_course IS 'Column is_course.';


--
-- Name: vw_segments_effort_stats; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_segments_effort_stats AS
SELECT
    NULL::integer AS segment_id,
    NULL::text AS segment_name,
    NULL::boolean AS is_course,
    NULL::text AS activity_type_name,
    NULL::numeric AS distance_mi,
    NULL::numeric(10,2) AS elevation_gain,
    NULL::date AS last_effort,
    NULL::bigint AS matched_activity_count;


--
-- Name: COLUMN vw_segments_effort_stats.segment_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN vw_segments_effort_stats.segment_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.segment_name IS 'Column segment_name.';


--
-- Name: COLUMN vw_segments_effort_stats.is_course; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.is_course IS 'Column is_course.';


--
-- Name: COLUMN vw_segments_effort_stats.activity_type_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.activity_type_name IS 'Column activity_type_name.';


--
-- Name: COLUMN vw_segments_effort_stats.distance_mi; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.distance_mi IS 'Column distance_mi.';


--
-- Name: COLUMN vw_segments_effort_stats.elevation_gain; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.elevation_gain IS 'Column elevation_gain.';


--
-- Name: COLUMN vw_segments_effort_stats.last_effort; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.last_effort IS 'Column last_effort.';


--
-- Name: COLUMN vw_segments_effort_stats.matched_activity_count; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_segments_effort_stats.matched_activity_count IS 'Column matched_activity_count.';


--
-- Name: vw_unmatched_activitites; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_unmatched_activitites AS
 SELECT DISTINCT a.activity_id
   FROM (activities.activities a
     LEFT JOIN activities.segment_matches sm ON ((sm.activity_id = a.activity_id)))
  WHERE (a.is_downloaded AND (sm.activity_id IS NULL))
  ORDER BY a.activity_id DESC;


--
-- Name: COLUMN vw_unmatched_activitites.activity_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_unmatched_activitites.activity_id IS 'Column activity_id.';


--
-- Name: all_tracks; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.all_tracks (
    track_id text NOT NULL,
    track_name text,
    track_name_clean text,
    track_name_search text,
    track_isrc text,
    duration_ms integer,
    artist_id text,
    artist_display_name text,
    album_id text,
    release_date date,
    available_in_us boolean,
    total_markets integer,
    popularity integer,
    updated_at_utc timestamp with time zone,
    id_synced_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE all_tracks; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.all_tracks IS 'Table all_tracks.';


--
-- Name: COLUMN all_tracks.track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.track_id IS 'Column track_id.';


--
-- Name: COLUMN all_tracks.track_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.track_name IS 'Column track_name.';


--
-- Name: COLUMN all_tracks.track_name_clean; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN all_tracks.track_name_search; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.track_name_search IS 'Column track_name_search.';


--
-- Name: COLUMN all_tracks.track_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.track_isrc IS 'Column track_isrc.';


--
-- Name: COLUMN all_tracks.duration_ms; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN all_tracks.artist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN all_tracks.artist_display_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.artist_display_name IS 'Column artist_display_name.';


--
-- Name: COLUMN all_tracks.album_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.album_id IS 'Column album_id.';


--
-- Name: COLUMN all_tracks.release_date; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.release_date IS 'Column release_date.';


--
-- Name: COLUMN all_tracks.available_in_us; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.available_in_us IS 'Column available_in_us.';


--
-- Name: COLUMN all_tracks.total_markets; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.total_markets IS 'Column total_markets.';


--
-- Name: COLUMN all_tracks.popularity; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.popularity IS 'Column popularity.';


--
-- Name: COLUMN all_tracks.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: COLUMN all_tracks.id_synced_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_tracks.id_synced_at_utc IS 'Column id_synced_at_utc.';


--
-- Name: isrc_swaps; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.isrc_swaps (
    original_isrc text NOT NULL,
    mapped_isrc text,
    mapping_time_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    valid_mapping boolean DEFAULT true
);


--
-- Name: TABLE isrc_swaps; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.isrc_swaps IS 'Table isrc_swaps.';


--
-- Name: COLUMN isrc_swaps.original_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_swaps.original_isrc IS 'Column original_isrc.';


--
-- Name: COLUMN isrc_swaps.mapped_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_swaps.mapped_isrc IS 'Column mapped_isrc.';


--
-- Name: COLUMN isrc_swaps.mapping_time_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_swaps.mapping_time_utc IS 'Column mapping_time_utc.';


--
-- Name: COLUMN isrc_swaps.valid_mapping; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_swaps.valid_mapping IS 'Column valid_mapping.';


--
-- Name: playlist_config; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.playlist_config (
    playlist_id text NOT NULL,
    playlist_name text,
    playlist_description text,
    track_count integer,
    auto_shuffle boolean DEFAULT false,
    seeds_only boolean DEFAULT false,
    make_recs boolean DEFAULT false,
    ratings_weight numeric(4,1) DEFAULT 1.0,
    recency_weight numeric(4,1) DEFAULT 1.0,
    randomness_weight numeric(4,1) DEFAULT 1.0,
    is_active boolean DEFAULT true,
    minutes_to_sync integer DEFAULT 9999,
    manual_shuffle boolean DEFAULT false,
    last_synced_utc timestamp with time zone,
    last_auto_shuffled_utc timestamp with time zone,
    last_verified_utc timestamp with time zone,
    prior_track_count integer DEFAULT 0
);


--
-- Name: TABLE playlist_config; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.playlist_config IS 'Table playlist_config.';


--
-- Name: COLUMN playlist_config.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN playlist_config.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN playlist_config.playlist_description; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.playlist_description IS 'Column playlist_description.';


--
-- Name: COLUMN playlist_config.track_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.track_count IS 'Column track_count.';


--
-- Name: COLUMN playlist_config.auto_shuffle; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.auto_shuffle IS 'Column auto_shuffle.';


--
-- Name: COLUMN playlist_config.seeds_only; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.seeds_only IS 'Column seeds_only.';


--
-- Name: COLUMN playlist_config.make_recs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.make_recs IS 'Column make_recs.';


--
-- Name: COLUMN playlist_config.ratings_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.ratings_weight IS 'Column ratings_weight.';


--
-- Name: COLUMN playlist_config.recency_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.recency_weight IS 'Column recency_weight.';


--
-- Name: COLUMN playlist_config.randomness_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.randomness_weight IS 'Column randomness_weight.';


--
-- Name: COLUMN playlist_config.is_active; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.is_active IS 'Column is_active.';


--
-- Name: COLUMN playlist_config.minutes_to_sync; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.minutes_to_sync IS 'Column minutes_to_sync.';


--
-- Name: COLUMN playlist_config.manual_shuffle; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.manual_shuffle IS 'Column manual_shuffle.';


--
-- Name: COLUMN playlist_config.last_synced_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.last_synced_utc IS 'Column last_synced_utc.';


--
-- Name: COLUMN playlist_config.last_auto_shuffled_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.last_auto_shuffled_utc IS 'Column last_auto_shuffled_utc.';


--
-- Name: COLUMN playlist_config.last_verified_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.last_verified_utc IS 'Column last_verified_utc.';


--
-- Name: COLUMN playlist_config.prior_track_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_config.prior_track_count IS 'Column prior_track_count.';


--
-- Name: playlist_isrcs; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.playlist_isrcs (
    playlist_id text,
    isrc text,
    track_order integer,
    updated_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE playlist_isrcs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.playlist_isrcs IS 'Table playlist_isrcs.';


--
-- Name: COLUMN playlist_isrcs.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_isrcs.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN playlist_isrcs.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_isrcs.isrc IS 'Column isrc.';


--
-- Name: COLUMN playlist_isrcs.track_order; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_isrcs.track_order IS 'Column track_order.';


--
-- Name: COLUMN playlist_isrcs.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_isrcs.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: playlist_relationships; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.playlist_relationships (
    parent_playlist_id text NOT NULL,
    child_playlist_id text NOT NULL,
    child_playlist_type text
);


--
-- Name: TABLE playlist_relationships; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.playlist_relationships IS 'Table playlist_relationships.';


--
-- Name: COLUMN playlist_relationships.parent_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_relationships.parent_playlist_id IS 'Column parent_playlist_id.';


--
-- Name: COLUMN playlist_relationships.child_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_relationships.child_playlist_id IS 'Column child_playlist_id.';


--
-- Name: COLUMN playlist_relationships.child_playlist_type; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_relationships.child_playlist_type IS 'Column child_playlist_type.';


--
-- Name: vw_best_track_id; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_best_track_id AS
 SELECT isrc,
    best_track_id,
    isrc_preference
   FROM ( SELECT COALESCE(s.mapped_isrc, t.track_isrc) AS isrc,
            t.track_id AS best_track_id,
            row_number() OVER (PARTITION BY COALESCE(s.mapped_isrc, t.track_isrc) ORDER BY t.available_in_us DESC, t.release_date, t.total_markets DESC, t.popularity DESC) AS isrc_preference
           FROM (music.all_tracks t
             LEFT JOIN music.isrc_swaps s ON (((s.original_isrc = t.track_isrc) AND s.valid_mapping)))) unnamed_subquery
  WHERE (isrc_preference = 1);


--
-- Name: COLUMN vw_best_track_id.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_best_track_id.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_best_track_id.best_track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_best_track_id.best_track_id IS 'Column best_track_id.';


--
-- Name: COLUMN vw_best_track_id.isrc_preference; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_best_track_id.isrc_preference IS 'Column isrc_preference.';


--
-- Name: vw_watch_music_heard; Type: VIEW; Schema: activities; Owner: -
--

CREATE VIEW activities.vw_watch_music_heard AS
 SELECT playlist_id,
    playlist_name,
    track_id,
    isrc,
    track_order,
    track_name_clean,
    artist_display_name,
    duration_ms,
    music_start,
    start_cumulative_ms,
    end_cumulative_ms,
    pct_listened,
    total_listen_ms,
    played_at_utc
   FROM ( SELECT unnamed_subquery.playlist_id,
            unnamed_subquery.playlist_name,
            unnamed_subquery.track_id,
            unnamed_subquery.isrc,
            unnamed_subquery.track_order,
            unnamed_subquery.track_name_clean,
            unnamed_subquery.artist_display_name,
            unnamed_subquery.duration_ms,
            unnamed_subquery.music_start,
            unnamed_subquery.start_cumulative_ms,
            unnamed_subquery.end_cumulative_ms,
                CASE
                    WHEN (unnamed_subquery.music_ms > (unnamed_subquery.end_cumulative_ms)::numeric) THEN 1.00
                    WHEN (unnamed_subquery.music_ms < (unnamed_subquery.start_cumulative_ms)::numeric) THEN 0.00
                    ELSE round(((1)::numeric - (((unnamed_subquery.end_cumulative_ms)::numeric - unnamed_subquery.music_ms) / (unnamed_subquery.duration_ms)::numeric)), 2)
                END AS pct_listened,
            unnamed_subquery.music_ms AS total_listen_ms,
            (unnamed_subquery.music_start + (((unnamed_subquery.end_cumulative_ms - unnamed_subquery.duration_ms))::double precision * '00:00:00.001'::interval)) AS played_at_utc
           FROM ( SELECT pc.playlist_id,
                    pc.playlist_name,
                    tr.track_id,
                    bt.isrc,
                    pi.track_order,
                    tr.track_name_clean,
                    tr.artist_display_name,
                    tr.duration_ms,
                    rt.music_start,
                    rt.music_duration_ms AS music_ms,
                    (sum(tr.duration_ms) OVER (PARTITION BY pi.playlist_id ORDER BY pi.track_order) - tr.duration_ms) AS start_cumulative_ms,
                    sum(tr.duration_ms) OVER (PARTITION BY pi.playlist_id ORDER BY pi.track_order) AS end_cumulative_ms
                   FROM (((((music.playlist_config pc
                     JOIN music.playlist_relationships pr ON (((pr.parent_playlist_id = pc.playlist_id) AND (pr.child_playlist_type = 'manual'::text))))
                     JOIN music.playlist_isrcs pi ON ((pi.playlist_id = pr.child_playlist_id)))
                     JOIN music.vw_best_track_id bt ON ((bt.isrc = pi.isrc)))
                     JOIN music.all_tracks tr ON ((bt.best_track_id = tr.track_id)))
                     CROSS JOIN activities.vw_run_timing rt)
                  WHERE pc.manual_shuffle) unnamed_subquery) subquery_two
  WHERE (pct_listened > 0.20);


--
-- Name: COLUMN vw_watch_music_heard.playlist_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_watch_music_heard.playlist_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN vw_watch_music_heard.track_id; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_watch_music_heard.isrc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_watch_music_heard.track_order; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.track_order IS 'Column track_order.';


--
-- Name: COLUMN vw_watch_music_heard.track_name_clean; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN vw_watch_music_heard.artist_display_name; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.artist_display_name IS 'Column artist_display_name.';


--
-- Name: COLUMN vw_watch_music_heard.duration_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN vw_watch_music_heard.music_start; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.music_start IS 'Column music_start.';


--
-- Name: COLUMN vw_watch_music_heard.start_cumulative_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.start_cumulative_ms IS 'Column start_cumulative_ms.';


--
-- Name: COLUMN vw_watch_music_heard.end_cumulative_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.end_cumulative_ms IS 'Column end_cumulative_ms.';


--
-- Name: COLUMN vw_watch_music_heard.pct_listened; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.pct_listened IS 'Column pct_listened.';


--
-- Name: COLUMN vw_watch_music_heard.total_listen_ms; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.total_listen_ms IS 'Column total_listen_ms.';


--
-- Name: COLUMN vw_watch_music_heard.played_at_utc; Type: COMMENT; Schema: activities; Owner: -
--

COMMENT ON COLUMN activities.vw_watch_music_heard.played_at_utc IS 'Column played_at_utc.';


--
-- Name: api_service_list; Type: TABLE; Schema: api_services; Owner: -
--

CREATE TABLE api_services.api_service_list (
    api_service_name text NOT NULL,
    api_service_function text,
    api_credential_requirements text,
    rate_limit_detected_utc timestamp with time zone,
    rate_limit_cleared_utc timestamp with time zone
);


--
-- Name: TABLE api_service_list; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON TABLE api_services.api_service_list IS 'Table api_service_list.';


--
-- Name: COLUMN api_service_list.api_service_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_service_list.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN api_service_list.api_service_function; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_service_list.api_service_function IS 'Column api_service_function.';


--
-- Name: COLUMN api_service_list.api_credential_requirements; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_service_list.api_credential_requirements IS 'Column api_credential_requirements.';


--
-- Name: COLUMN api_service_list.rate_limit_detected_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_service_list.rate_limit_detected_utc IS 'Column rate_limit_detected_utc.';


--
-- Name: COLUMN api_service_list.rate_limit_cleared_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_service_list.rate_limit_cleared_utc IS 'Column rate_limit_cleared_utc.';


--
-- Name: api_tokens; Type: TABLE; Schema: api_services; Owner: -
--

CREATE TABLE api_services.api_tokens (
    id integer NOT NULL,
    service_name text NOT NULL,
    token_data text NOT NULL,
    created_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean DEFAULT true NOT NULL,
    refresh_token_expires_at_utc timestamp with time zone
);


--
-- Name: TABLE api_tokens; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON TABLE api_services.api_tokens IS 'Table api_tokens.';


--
-- Name: COLUMN api_tokens.id; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.id IS 'Column id.';


--
-- Name: COLUMN api_tokens.service_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.service_name IS 'Column service_name.';


--
-- Name: COLUMN api_tokens.token_data; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.token_data IS 'Column token_data.';


--
-- Name: COLUMN api_tokens.created_at_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.created_at_utc IS 'Column created_at_utc.';


--
-- Name: COLUMN api_tokens.updated_at_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: COLUMN api_tokens.is_active; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.is_active IS 'Column is_active.';


--
-- Name: COLUMN api_tokens.refresh_token_expires_at_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.api_tokens.refresh_token_expires_at_utc IS 'Column refresh_token_expires_at_utc.';


--
-- Name: api_tokens_id_seq; Type: SEQUENCE; Schema: api_services; Owner: -
--

CREATE SEQUENCE api_services.api_tokens_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: api_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: api_services; Owner: -
--

ALTER SEQUENCE api_services.api_tokens_id_seq OWNED BY api_services.api_tokens.id;


--
-- Name: credentials; Type: TABLE; Schema: api_services; Owner: -
--

CREATE TABLE api_services.credentials (
    api_service_name text NOT NULL,
    api_credentials text
);


--
-- Name: TABLE credentials; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON TABLE api_services.credentials IS 'Table credentials.';


--
-- Name: COLUMN credentials.api_service_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.credentials.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN credentials.api_credentials; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.credentials.api_credentials IS 'Column api_credentials.';


--
-- Name: function_library; Type: TABLE; Schema: api_services; Owner: -
--

CREATE TABLE api_services.function_library (
    friendly_name text NOT NULL,
    api_service_name text,
    api_function_name text,
    python_extraction_function text,
    flatten_sproc text,
    value_recency timestamp with time zone,
    loop_strategy text,
    iter_path_keys text,
    iter_query_keys text,
    static_path_params text,
    static_query_params text
);


--
-- Name: TABLE function_library; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON TABLE api_services.function_library IS 'Table function_library.';


--
-- Name: COLUMN function_library.friendly_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.friendly_name IS 'Column friendly_name.';


--
-- Name: COLUMN function_library.api_service_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN function_library.api_function_name; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.api_function_name IS 'Column api_function_name.';


--
-- Name: COLUMN function_library.python_extraction_function; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.python_extraction_function IS 'Column python_extraction_function.';


--
-- Name: COLUMN function_library.flatten_sproc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.flatten_sproc IS 'Column flatten_sproc.';


--
-- Name: COLUMN function_library.value_recency; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.value_recency IS 'Column value_recency.';


--
-- Name: COLUMN function_library.loop_strategy; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.loop_strategy IS 'Column loop_strategy.';


--
-- Name: COLUMN function_library.iter_path_keys; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.iter_path_keys IS 'Column iter_path_keys.';


--
-- Name: COLUMN function_library.iter_query_keys; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.iter_query_keys IS 'Column iter_query_keys.';


--
-- Name: COLUMN function_library.static_path_params; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.static_path_params IS 'Column static_path_params.';


--
-- Name: COLUMN function_library.static_query_params; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.function_library.static_query_params IS 'Column static_query_params.';


--
-- Name: spotify_auth_metadata; Type: TABLE; Schema: api_services; Owner: -
--

CREATE TABLE api_services.spotify_auth_metadata (
    id integer NOT NULL,
    authorized_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    refresh_token_hash text,
    expires_at_utc timestamp with time zone NOT NULL,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE spotify_auth_metadata; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON TABLE api_services.spotify_auth_metadata IS 'Table spotify_auth_metadata.';


--
-- Name: COLUMN spotify_auth_metadata.id; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.id IS 'Column id.';


--
-- Name: COLUMN spotify_auth_metadata.authorized_at_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.authorized_at_utc IS 'Column authorized_at_utc.';


--
-- Name: COLUMN spotify_auth_metadata.refresh_token_hash; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.refresh_token_hash IS 'Column refresh_token_hash.';


--
-- Name: COLUMN spotify_auth_metadata.expires_at_utc; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.expires_at_utc IS 'Column expires_at_utc.';


--
-- Name: COLUMN spotify_auth_metadata.is_active; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.is_active IS 'Column is_active.';


--
-- Name: COLUMN spotify_auth_metadata.created_at; Type: COMMENT; Schema: api_services; Owner: -
--

COMMENT ON COLUMN api_services.spotify_auth_metadata.created_at IS 'Column created_at.';


--
-- Name: spotify_auth_metadata_id_seq; Type: SEQUENCE; Schema: api_services; Owner: -
--

CREATE SEQUENCE api_services.spotify_auth_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: spotify_auth_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: api_services; Owner: -
--

ALTER SEQUENCE api_services.spotify_auth_metadata_id_seq OWNED BY api_services.spotify_auth_metadata.id;


--
-- Name: exercise_attempts; Type: TABLE; Schema: exercises; Owner: -
--

CREATE TABLE exercises.exercise_attempts (
    attempt_id integer NOT NULL,
    exercise_id integer NOT NULL,
    interval_seconds_used numeric(5,2) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone NOT NULL,
    paced_count integer NOT NULL,
    total_count integer NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT exercise_attempts_check CHECK ((total_count >= paced_count)),
    CONSTRAINT exercise_attempts_paced_count_check CHECK ((paced_count >= 0))
);


--
-- Name: TABLE exercise_attempts; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON TABLE exercises.exercise_attempts IS 'One attempt at an exercise timer session.';


--
-- Name: COLUMN exercise_attempts.attempt_id; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.attempt_id IS 'Primary key.';


--
-- Name: COLUMN exercise_attempts.exercise_id; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.exercise_id IS 'FK to exercise_timers.exercise_id.';


--
-- Name: COLUMN exercise_attempts.interval_seconds_used; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.interval_seconds_used IS 'Interval seconds configured for this attempt.';


--
-- Name: COLUMN exercise_attempts.started_at; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.started_at IS 'Attempt start timestamp.';


--
-- Name: COLUMN exercise_attempts.ended_at; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.ended_at IS 'Attempt end timestamp.';


--
-- Name: COLUMN exercise_attempts.paced_count; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.paced_count IS 'Count of paced intervals completed.';


--
-- Name: COLUMN exercise_attempts.total_count; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.total_count IS 'Total intervals completed.';


--
-- Name: COLUMN exercise_attempts.notes; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.notes IS 'Free-text notes for the attempt.';


--
-- Name: COLUMN exercise_attempts.created_at; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_attempts.created_at IS 'Row creation timestamp.';


--
-- Name: exercise_attempts_attempt_id_seq; Type: SEQUENCE; Schema: exercises; Owner: -
--

CREATE SEQUENCE exercises.exercise_attempts_attempt_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exercise_attempts_attempt_id_seq; Type: SEQUENCE OWNED BY; Schema: exercises; Owner: -
--

ALTER SEQUENCE exercises.exercise_attempts_attempt_id_seq OWNED BY exercises.exercise_attempts.attempt_id;


--
-- Name: exercise_timers; Type: TABLE; Schema: exercises; Owner: -
--

CREATE TABLE exercises.exercise_timers (
    exercise_id integer NOT NULL,
    name text NOT NULL,
    interval_seconds numeric(5,2) NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT exercise_timers_interval_seconds_check CHECK ((interval_seconds > (0)::numeric))
);


--
-- Name: TABLE exercise_timers; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON TABLE exercises.exercise_timers IS 'Configured exercise interval timers.';


--
-- Name: COLUMN exercise_timers.exercise_id; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.exercise_id IS 'Primary key.';


--
-- Name: COLUMN exercise_timers.name; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.name IS 'Timer display name.';


--
-- Name: COLUMN exercise_timers.interval_seconds; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.interval_seconds IS 'Interval length in seconds.';


--
-- Name: COLUMN exercise_timers.notes; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.notes IS 'Free-text notes for the timer.';


--
-- Name: COLUMN exercise_timers.created_at; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.created_at IS 'Row creation timestamp.';


--
-- Name: COLUMN exercise_timers.updated_at; Type: COMMENT; Schema: exercises; Owner: -
--

COMMENT ON COLUMN exercises.exercise_timers.updated_at IS 'Row last-update timestamp.';


--
-- Name: exercise_timers_exercise_id_seq; Type: SEQUENCE; Schema: exercises; Owner: -
--

CREATE SEQUENCE exercises.exercise_timers_exercise_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exercise_timers_exercise_id_seq; Type: SEQUENCE OWNED BY; Schema: exercises; Owner: -
--

ALTER SEQUENCE exercises.exercise_timers_exercise_id_seq OWNED BY exercises.exercise_timers.exercise_id;


--
-- Name: diary_entries; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.diary_entries (
    entry_id integer NOT NULL,
    food_id integer,
    name_snapshot text NOT NULL,
    kcal_per_100g double precision,
    protein_g double precision,
    fat_g double precision,
    carbs_g double precision,
    fiber_g double precision,
    sugars_g double precision,
    sodium_mg double precision,
    caffeine_mg double precision,
    is_partial boolean DEFAULT false NOT NULL,
    qty double precision NOT NULL,
    unit text NOT NULL,
    kcal double precision,
    logged_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT diary_entries_caffeine_mg_check CHECK (((caffeine_mg IS NULL) OR (caffeine_mg >= (0)::double precision))),
    CONSTRAINT diary_entries_carbs_g_check CHECK (((carbs_g IS NULL) OR (carbs_g >= (0)::double precision))),
    CONSTRAINT diary_entries_fat_g_check CHECK (((fat_g IS NULL) OR (fat_g >= (0)::double precision))),
    CONSTRAINT diary_entries_fiber_g_check CHECK (((fiber_g IS NULL) OR (fiber_g >= (0)::double precision))),
    CONSTRAINT diary_entries_kcal_check CHECK (((kcal IS NULL) OR (kcal >= (0)::double precision))),
    CONSTRAINT diary_entries_kcal_per_100g_check CHECK (((kcal_per_100g IS NULL) OR (kcal_per_100g >= (0)::double precision))),
    CONSTRAINT diary_entries_protein_g_check CHECK (((protein_g IS NULL) OR (protein_g >= (0)::double precision))),
    CONSTRAINT diary_entries_qty_check CHECK ((qty > (0)::double precision)),
    CONSTRAINT diary_entries_sodium_mg_check CHECK (((sodium_mg IS NULL) OR (sodium_mg >= (0)::double precision))),
    CONSTRAINT diary_entries_sugars_g_check CHECK (((sugars_g IS NULL) OR (sugars_g >= (0)::double precision)))
);


--
-- Name: TABLE diary_entries; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.diary_entries IS 'Table diary_entries.';


--
-- Name: COLUMN diary_entries.entry_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.entry_id IS 'Column entry_id.';


--
-- Name: COLUMN diary_entries.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.food_id IS 'Column food_id.';


--
-- Name: COLUMN diary_entries.name_snapshot; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.name_snapshot IS 'Column name_snapshot.';


--
-- Name: COLUMN diary_entries.kcal_per_100g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.kcal_per_100g IS 'Column kcal_per_100g.';


--
-- Name: COLUMN diary_entries.protein_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.protein_g IS 'Column protein_g.';


--
-- Name: COLUMN diary_entries.fat_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.fat_g IS 'Column fat_g.';


--
-- Name: COLUMN diary_entries.carbs_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.carbs_g IS 'Column carbs_g.';


--
-- Name: COLUMN diary_entries.fiber_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.fiber_g IS 'Column fiber_g.';


--
-- Name: COLUMN diary_entries.sugars_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.sugars_g IS 'Column sugars_g.';


--
-- Name: COLUMN diary_entries.sodium_mg; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.sodium_mg IS 'Column sodium_mg.';


--
-- Name: COLUMN diary_entries.caffeine_mg; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.caffeine_mg IS 'Column caffeine_mg.';


--
-- Name: COLUMN diary_entries.is_partial; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.is_partial IS 'Column is_partial.';


--
-- Name: COLUMN diary_entries.qty; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.qty IS 'Column qty.';


--
-- Name: COLUMN diary_entries.unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.unit IS 'Column unit.';


--
-- Name: COLUMN diary_entries.kcal; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.kcal IS 'Column kcal.';


--
-- Name: COLUMN diary_entries.logged_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.logged_at IS 'Column logged_at.';


--
-- Name: COLUMN diary_entries.created_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.diary_entries.created_at IS 'Column created_at.';


--
-- Name: diary_entries_entry_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.diary_entries_entry_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: diary_entries_entry_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.diary_entries_entry_id_seq OWNED BY food.diary_entries.entry_id;


--
-- Name: foods; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.foods (
    food_id integer NOT NULL,
    name text NOT NULL,
    source text DEFAULT 'db'::text NOT NULL,
    fdc_id integer,
    barcode text,
    kcal_per_100g double precision,
    protein_g double precision,
    fat_g double precision,
    carbs_g double precision,
    fiber_g double precision,
    sugars_g double precision,
    sodium_mg double precision,
    caffeine_mg double precision,
    serving_size double precision,
    serving_unit text,
    serving_text text,
    nutrients_raw jsonb,
    normalized_from text,
    needs_review boolean DEFAULT false NOT NULL,
    review_reason text,
    quality_score double precision DEFAULT 0 NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    deleted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT foods_caffeine_mg_check CHECK (((caffeine_mg IS NULL) OR (caffeine_mg >= (0)::double precision))),
    CONSTRAINT foods_carbs_g_check CHECK (((carbs_g IS NULL) OR (carbs_g >= (0)::double precision))),
    CONSTRAINT foods_fat_g_check CHECK (((fat_g IS NULL) OR (fat_g >= (0)::double precision))),
    CONSTRAINT foods_fiber_g_check CHECK (((fiber_g IS NULL) OR (fiber_g >= (0)::double precision))),
    CONSTRAINT foods_kcal_per_100g_check CHECK (((kcal_per_100g IS NULL) OR (kcal_per_100g >= (0)::double precision))),
    CONSTRAINT foods_normalized_from_check CHECK (((normalized_from IS NULL) OR (normalized_from = ANY (ARRAY['usda_branded'::text, 'usda_survey'::text, 'usda_legacy'::text, 'off'::text, 'manual'::text, 'recipe'::text])))),
    CONSTRAINT foods_protein_g_check CHECK (((protein_g IS NULL) OR (protein_g >= (0)::double precision))),
    CONSTRAINT foods_quality_score_check CHECK ((quality_score >= (0)::double precision)),
    CONSTRAINT foods_serving_size_check CHECK (((serving_size IS NULL) OR (serving_size > (0)::double precision))),
    CONSTRAINT foods_sodium_mg_check CHECK (((sodium_mg IS NULL) OR (sodium_mg >= (0)::double precision))),
    CONSTRAINT foods_source_check CHECK ((source = ANY (ARRAY['api'::text, 'db'::text, 'recipe'::text]))),
    CONSTRAINT foods_sugars_g_check CHECK (((sugars_g IS NULL) OR (sugars_g >= (0)::double precision)))
);


--
-- Name: TABLE foods; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.foods IS 'Table foods.';


--
-- Name: COLUMN foods.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.food_id IS 'Column food_id.';


--
-- Name: COLUMN foods.name; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.name IS 'Column name.';


--
-- Name: COLUMN foods.source; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.source IS 'Column source.';


--
-- Name: COLUMN foods.fdc_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.fdc_id IS 'Column fdc_id.';


--
-- Name: COLUMN foods.barcode; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.barcode IS 'Column barcode.';


--
-- Name: COLUMN foods.kcal_per_100g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.kcal_per_100g IS 'Column kcal_per_100g.';


--
-- Name: COLUMN foods.protein_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.protein_g IS 'Column protein_g.';


--
-- Name: COLUMN foods.fat_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.fat_g IS 'Column fat_g.';


--
-- Name: COLUMN foods.carbs_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.carbs_g IS 'Column carbs_g.';


--
-- Name: COLUMN foods.fiber_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.fiber_g IS 'Column fiber_g.';


--
-- Name: COLUMN foods.sugars_g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.sugars_g IS 'Column sugars_g.';


--
-- Name: COLUMN foods.sodium_mg; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.sodium_mg IS 'Column sodium_mg.';


--
-- Name: COLUMN foods.caffeine_mg; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.caffeine_mg IS 'Column caffeine_mg.';


--
-- Name: COLUMN foods.serving_size; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.serving_size IS 'Column serving_size.';


--
-- Name: COLUMN foods.serving_unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.serving_unit IS 'Column serving_unit.';


--
-- Name: COLUMN foods.serving_text; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.serving_text IS 'Column serving_text.';


--
-- Name: COLUMN foods.nutrients_raw; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.nutrients_raw IS 'Column nutrients_raw.';


--
-- Name: COLUMN foods.normalized_from; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.normalized_from IS 'Column normalized_from.';


--
-- Name: COLUMN foods.needs_review; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.needs_review IS 'Column needs_review.';


--
-- Name: COLUMN foods.review_reason; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.review_reason IS 'Column review_reason.';


--
-- Name: COLUMN foods.quality_score; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.quality_score IS 'Column quality_score.';


--
-- Name: COLUMN foods.is_deleted; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.is_deleted IS 'Column is_deleted.';


--
-- Name: COLUMN foods.deleted_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.deleted_at IS 'Column deleted_at.';


--
-- Name: COLUMN foods.created_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.foods.created_at IS 'Column created_at.';


--
-- Name: foods_food_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.foods_food_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: foods_food_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.foods_food_id_seq OWNED BY food.foods.food_id;


--
-- Name: recipe_cook_states; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.recipe_cook_states (
    recipe_id integer NOT NULL,
    checked_steps jsonb DEFAULT '[]'::jsonb NOT NULL,
    checked_foods jsonb DEFAULT '[]'::jsonb NOT NULL,
    timers jsonb DEFAULT '[]'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recipe_cook_states_check CHECK (((jsonb_typeof(checked_steps) = 'array'::text) AND (jsonb_typeof(checked_foods) = 'array'::text) AND (jsonb_typeof(timers) = 'array'::text)))
);


--
-- Name: TABLE recipe_cook_states; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.recipe_cook_states IS 'Table recipe_cook_states.';


--
-- Name: COLUMN recipe_cook_states.recipe_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_cook_states.recipe_id IS 'Column recipe_id.';


--
-- Name: COLUMN recipe_cook_states.checked_steps; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_cook_states.checked_steps IS 'Column checked_steps.';


--
-- Name: COLUMN recipe_cook_states.checked_foods; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_cook_states.checked_foods IS 'Column checked_foods.';


--
-- Name: COLUMN recipe_cook_states.timers; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_cook_states.timers IS 'Column timers.';


--
-- Name: COLUMN recipe_cook_states.updated_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_cook_states.updated_at IS 'Column updated_at.';


--
-- Name: recipe_ingredients; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.recipe_ingredients (
    recipe_id integer NOT NULL,
    ingredient_id integer NOT NULL,
    food_id integer NOT NULL,
    qty double precision NOT NULL,
    unit text NOT NULL,
    CONSTRAINT recipe_ingredients_qty_check CHECK ((qty > (0)::double precision)),
    CONSTRAINT recipe_ingredients_unit_check CHECK ((btrim(unit) <> ''::text))
);


--
-- Name: TABLE recipe_ingredients; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.recipe_ingredients IS 'Table recipe_ingredients.';


--
-- Name: COLUMN recipe_ingredients.recipe_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_ingredients.recipe_id IS 'Column recipe_id.';


--
-- Name: COLUMN recipe_ingredients.ingredient_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_ingredients.ingredient_id IS 'Column ingredient_id.';


--
-- Name: COLUMN recipe_ingredients.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_ingredients.food_id IS 'Column food_id.';


--
-- Name: COLUMN recipe_ingredients.qty; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_ingredients.qty IS 'Column qty.';


--
-- Name: COLUMN recipe_ingredients.unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_ingredients.unit IS 'Column unit.';


--
-- Name: recipe_ingredients_ingredient_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.recipe_ingredients_ingredient_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recipe_ingredients_ingredient_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.recipe_ingredients_ingredient_id_seq OWNED BY food.recipe_ingredients.ingredient_id;


--
-- Name: recipe_step_foods; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.recipe_step_foods (
    recipe_id integer NOT NULL,
    step_id integer NOT NULL,
    food_id integer NOT NULL,
    qty double precision NOT NULL,
    unit text NOT NULL,
    CONSTRAINT recipe_step_foods_qty_check CHECK ((qty > (0)::double precision))
);


--
-- Name: TABLE recipe_step_foods; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.recipe_step_foods IS 'Table recipe_step_foods.';


--
-- Name: COLUMN recipe_step_foods.recipe_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_step_foods.recipe_id IS 'Column recipe_id.';


--
-- Name: COLUMN recipe_step_foods.step_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_step_foods.step_id IS 'Column step_id.';


--
-- Name: COLUMN recipe_step_foods.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_step_foods.food_id IS 'Column food_id.';


--
-- Name: COLUMN recipe_step_foods.qty; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_step_foods.qty IS 'Column qty.';


--
-- Name: COLUMN recipe_step_foods.unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_step_foods.unit IS 'Column unit.';


--
-- Name: recipe_steps; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.recipe_steps (
    step_id integer NOT NULL,
    recipe_id integer NOT NULL,
    step_no integer NOT NULL,
    instruction text DEFAULT ''::text NOT NULL,
    timer_seconds integer,
    CONSTRAINT recipe_steps_step_no_check CHECK ((step_no >= 1)),
    CONSTRAINT recipe_steps_timer_seconds_check CHECK (((timer_seconds IS NULL) OR (timer_seconds >= 0)))
);


--
-- Name: TABLE recipe_steps; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.recipe_steps IS 'Table recipe_steps.';


--
-- Name: COLUMN recipe_steps.step_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_steps.step_id IS 'Column step_id.';


--
-- Name: COLUMN recipe_steps.recipe_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_steps.recipe_id IS 'Column recipe_id.';


--
-- Name: COLUMN recipe_steps.step_no; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_steps.step_no IS 'Column step_no.';


--
-- Name: COLUMN recipe_steps.instruction; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_steps.instruction IS 'Column instruction.';


--
-- Name: COLUMN recipe_steps.timer_seconds; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipe_steps.timer_seconds IS 'Column timer_seconds.';


--
-- Name: recipe_steps_step_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.recipe_steps_step_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recipe_steps_step_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.recipe_steps_step_id_seq OWNED BY food.recipe_steps.step_id;


--
-- Name: recipes; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.recipes (
    recipe_id integer NOT NULL,
    title text NOT NULL,
    category text DEFAULT 'Dinner'::text NOT NULL,
    times_prepared integer DEFAULT 0 NOT NULL,
    food_id integer,
    is_deleted boolean DEFAULT false NOT NULL,
    deleted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    servings_count integer,
    serving_unit text,
    CONSTRAINT recipes_category_check CHECK ((category = ANY (ARRAY['Breakfast'::text, 'Lunch'::text, 'Dinner'::text, 'Side'::text, 'Dessert'::text, 'Drink'::text]))),
    CONSTRAINT recipes_serving_unit_check CHECK (((serving_unit IS NULL) OR (btrim(serving_unit) <> ''::text))),
    CONSTRAINT recipes_servings_count_check CHECK (((servings_count IS NULL) OR (servings_count > 0))),
    CONSTRAINT recipes_times_prepared_check CHECK ((times_prepared >= 0))
);


--
-- Name: TABLE recipes; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.recipes IS 'Table recipes.';


--
-- Name: COLUMN recipes.recipe_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.recipe_id IS 'Column recipe_id.';


--
-- Name: COLUMN recipes.title; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.title IS 'Column title.';


--
-- Name: COLUMN recipes.category; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.category IS 'Column category.';


--
-- Name: COLUMN recipes.times_prepared; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.times_prepared IS 'Column times_prepared.';


--
-- Name: COLUMN recipes.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.food_id IS 'Column food_id.';


--
-- Name: COLUMN recipes.is_deleted; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.is_deleted IS 'Column is_deleted.';


--
-- Name: COLUMN recipes.deleted_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.deleted_at IS 'Column deleted_at.';


--
-- Name: COLUMN recipes.created_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.created_at IS 'Column created_at.';


--
-- Name: COLUMN recipes.servings_count; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.servings_count IS 'Column servings_count.';


--
-- Name: COLUMN recipes.serving_unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.recipes.serving_unit IS 'Column serving_unit.';


--
-- Name: recipes_recipe_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.recipes_recipe_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recipes_recipe_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.recipes_recipe_id_seq OWNED BY food.recipes.recipe_id;


--
-- Name: tri_tip_events; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.tri_tip_events (
    tri_tip_id integer NOT NULL,
    weight_lbs numeric(4,2) NOT NULL,
    shape text NOT NULL,
    target_internal_temp_f numeric(5,2) DEFAULT 125.0 NOT NULL,
    status text DEFAULT 'initiated'::text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    label text,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tri_tip_events_weight_lbs_check CHECK ((weight_lbs > (0)::numeric))
);


--
-- Name: TABLE tri_tip_events; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.tri_tip_events IS 'Table tri_tip_events.';


--
-- Name: COLUMN tri_tip_events.tri_tip_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.tri_tip_id IS 'Column tri_tip_id.';


--
-- Name: COLUMN tri_tip_events.weight_lbs; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.weight_lbs IS 'Column weight_lbs.';


--
-- Name: COLUMN tri_tip_events.shape; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.shape IS 'Column shape.';


--
-- Name: COLUMN tri_tip_events.target_internal_temp_f; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.target_internal_temp_f IS 'Column target_internal_temp_f.';


--
-- Name: COLUMN tri_tip_events.status; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.status IS 'Column status.';


--
-- Name: COLUMN tri_tip_events.started_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.started_at IS 'Column started_at.';


--
-- Name: COLUMN tri_tip_events.completed_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.completed_at IS 'Column completed_at.';


--
-- Name: COLUMN tri_tip_events.label; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.label IS 'Column label.';


--
-- Name: COLUMN tri_tip_events.notes; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.notes IS 'Column notes.';


--
-- Name: COLUMN tri_tip_events.created_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_events.created_at IS 'Column created_at.';


--
-- Name: tri_tip_events_tri_tip_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.tri_tip_events_tri_tip_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tri_tip_events_tri_tip_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.tri_tip_events_tri_tip_id_seq OWNED BY food.tri_tip_events.tri_tip_id;


--
-- Name: tri_tip_readings; Type: TABLE; Schema: food; Owner: -
--

CREATE TABLE food.tri_tip_readings (
    reading_id integer NOT NULL,
    tri_tip_id integer NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    grill_temp_f numeric(5,2) NOT NULL,
    internal_temp_f numeric(5,2) NOT NULL,
    note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE tri_tip_readings; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON TABLE food.tri_tip_readings IS 'Table tri_tip_readings.';


--
-- Name: COLUMN tri_tip_readings.reading_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.reading_id IS 'Column reading_id.';


--
-- Name: COLUMN tri_tip_readings.tri_tip_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.tri_tip_id IS 'Column tri_tip_id.';


--
-- Name: COLUMN tri_tip_readings.recorded_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.recorded_at IS 'Column recorded_at.';


--
-- Name: COLUMN tri_tip_readings.grill_temp_f; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.grill_temp_f IS 'Column grill_temp_f.';


--
-- Name: COLUMN tri_tip_readings.internal_temp_f; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.internal_temp_f IS 'Column internal_temp_f.';


--
-- Name: COLUMN tri_tip_readings.note; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.note IS 'Column note.';


--
-- Name: COLUMN tri_tip_readings.created_at; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.tri_tip_readings.created_at IS 'Column created_at.';


--
-- Name: tri_tip_readings_reading_id_seq; Type: SEQUENCE; Schema: food; Owner: -
--

CREATE SEQUENCE food.tri_tip_readings_reading_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tri_tip_readings_reading_id_seq; Type: SEQUENCE OWNED BY; Schema: food; Owner: -
--

ALTER SEQUENCE food.tri_tip_readings_reading_id_seq OWNED BY food.tri_tip_readings.reading_id;


--
-- Name: vw_food_search; Type: VIEW; Schema: food; Owner: -
--

CREATE VIEW food.vw_food_search AS
 SELECT food_id,
    name,
    source,
    kcal_per_100g,
    caffeine_mg,
    quality_score,
    needs_review,
    (public.similarity(name, ''::text) + (quality_score / (100.0)::double precision)) AS rank_score
   FROM food.foods
  WHERE (NOT is_deleted);


--
-- Name: COLUMN vw_food_search.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.food_id IS 'Column food_id.';


--
-- Name: COLUMN vw_food_search.name; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.name IS 'Column name.';


--
-- Name: COLUMN vw_food_search.source; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.source IS 'Column source.';


--
-- Name: COLUMN vw_food_search.kcal_per_100g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.kcal_per_100g IS 'Column kcal_per_100g.';


--
-- Name: COLUMN vw_food_search.caffeine_mg; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.caffeine_mg IS 'Column caffeine_mg.';


--
-- Name: COLUMN vw_food_search.quality_score; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.quality_score IS 'Column quality_score.';


--
-- Name: COLUMN vw_food_search.needs_review; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.needs_review IS 'Column needs_review.';


--
-- Name: COLUMN vw_food_search.rank_score; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_food_search.rank_score IS 'Column rank_score.';


--
-- Name: vw_usual_foods; Type: VIEW; Schema: food; Owner: -
--

CREATE VIEW food.vw_usual_foods AS
 SELECT f.food_id,
    f.name,
    f.source,
    f.kcal_per_100g,
    f.quality_score,
    (count(e.entry_id))::integer AS times_eaten,
    (avg(exp(((- ((EXTRACT(hour FROM e.logged_at) - EXTRACT(hour FROM now())) ^ (2)::numeric)) / 18.0))))::double precision AS hour_weight,
    m.qty AS mode_qty,
    m.unit AS mode_unit
   FROM ((food.foods f
     JOIN food.diary_entries e ON ((e.food_id = f.food_id)))
     LEFT JOIN LATERAL ( SELECT e2.qty,
            e2.unit
           FROM food.diary_entries e2
          WHERE (e2.food_id = f.food_id)
          GROUP BY e2.qty, e2.unit
          ORDER BY (count(*)) DESC, (max(e2.logged_at)) DESC
         LIMIT 1) m ON (true))
  WHERE ((NOT f.is_deleted) AND (NOT f.needs_review) AND (f.kcal_per_100g IS NOT NULL))
  GROUP BY f.food_id, f.name, f.source, f.kcal_per_100g, f.quality_score, m.qty, m.unit;


--
-- Name: COLUMN vw_usual_foods.food_id; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.food_id IS 'Column food_id.';


--
-- Name: COLUMN vw_usual_foods.name; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.name IS 'Column name.';


--
-- Name: COLUMN vw_usual_foods.source; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.source IS 'Column source.';


--
-- Name: COLUMN vw_usual_foods.kcal_per_100g; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.kcal_per_100g IS 'Column kcal_per_100g.';


--
-- Name: COLUMN vw_usual_foods.quality_score; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.quality_score IS 'Column quality_score.';


--
-- Name: COLUMN vw_usual_foods.times_eaten; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.times_eaten IS 'Column times_eaten.';


--
-- Name: COLUMN vw_usual_foods.hour_weight; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.hour_weight IS 'Column hour_weight.';


--
-- Name: COLUMN vw_usual_foods.mode_qty; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.mode_qty IS 'Column mode_qty.';


--
-- Name: COLUMN vw_usual_foods.mode_unit; Type: COMMENT; Schema: food; Owner: -
--

COMMENT ON COLUMN food.vw_usual_foods.mode_unit IS 'Column mode_unit.';


--
-- Name: body_battery_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.body_battery_raw (
    ts_utc timestamp with time zone NOT NULL,
    body_battery_value integer
);


--
-- Name: TABLE body_battery_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.body_battery_raw IS 'Table body_battery_raw.';


--
-- Name: COLUMN body_battery_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_battery_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN body_battery_raw.body_battery_value; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_battery_raw.body_battery_value IS 'Column body_battery_value.';


--
-- Name: body_dimensions; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.body_dimensions (
    captured_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    butt_cm numeric(4,1),
    waist_cm numeric(4,1),
    stomach_cm numeric(4,1),
    chest_cm numeric(4,1),
    neck_cm numeric(4,1)
);


--
-- Name: TABLE body_dimensions; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.body_dimensions IS 'Table body_dimensions.';


--
-- Name: COLUMN body_dimensions.captured_at_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.captured_at_utc IS 'Column captured_at_utc.';


--
-- Name: COLUMN body_dimensions.butt_cm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.butt_cm IS 'Column butt_cm.';


--
-- Name: COLUMN body_dimensions.waist_cm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.waist_cm IS 'Column waist_cm.';


--
-- Name: COLUMN body_dimensions.stomach_cm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.stomach_cm IS 'Column stomach_cm.';


--
-- Name: COLUMN body_dimensions.chest_cm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.chest_cm IS 'Column chest_cm.';


--
-- Name: COLUMN body_dimensions.neck_cm; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.body_dimensions.neck_cm IS 'Column neck_cm.';


--
-- Name: fitness_age_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.fitness_age_raw (
    ts_utc timestamp with time zone NOT NULL,
    human_age numeric(4,2),
    fitness_age_measured numeric(4,2),
    fitness_age_potential numeric(4,2)
);


--
-- Name: TABLE fitness_age_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.fitness_age_raw IS 'Table fitness_age_raw.';


--
-- Name: COLUMN fitness_age_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.fitness_age_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN fitness_age_raw.human_age; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.fitness_age_raw.human_age IS 'Column human_age.';


--
-- Name: COLUMN fitness_age_raw.fitness_age_measured; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.fitness_age_raw.fitness_age_measured IS 'Column fitness_age_measured.';


--
-- Name: COLUMN fitness_age_raw.fitness_age_potential; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.fitness_age_raw.fitness_age_potential IS 'Column fitness_age_potential.';


--
-- Name: hrv_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.hrv_raw (
    ts_utc timestamp with time zone NOT NULL,
    hrv_value smallint
);


--
-- Name: TABLE hrv_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.hrv_raw IS 'Table hrv_raw.';


--
-- Name: COLUMN hrv_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.hrv_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN hrv_raw.hrv_value; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.hrv_raw.hrv_value IS 'Column hrv_value.';


--
-- Name: photo_metadata; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.photo_metadata (
    captured_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    photo_type text,
    file_name text
);


--
-- Name: TABLE photo_metadata; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.photo_metadata IS 'Table photo_metadata.';


--
-- Name: COLUMN photo_metadata.captured_at_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.photo_metadata.captured_at_utc IS 'Column captured_at_utc.';


--
-- Name: COLUMN photo_metadata.photo_type; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.photo_metadata.photo_type IS 'Column photo_type.';


--
-- Name: COLUMN photo_metadata.file_name; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.photo_metadata.file_name IS 'Column file_name.';


--
-- Name: respiration_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.respiration_raw (
    ts_utc timestamp with time zone NOT NULL,
    breaths_per_minute smallint
);


--
-- Name: TABLE respiration_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.respiration_raw IS 'Table respiration_raw.';


--
-- Name: COLUMN respiration_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.respiration_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN respiration_raw.breaths_per_minute; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.respiration_raw.breaths_per_minute IS 'Column breaths_per_minute.';


--
-- Name: sleep_movement_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.sleep_movement_raw (
    ts_utc timestamp with time zone NOT NULL,
    sleep_movement numeric(5,2)
);


--
-- Name: TABLE sleep_movement_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.sleep_movement_raw IS 'Table sleep_movement_raw.';


--
-- Name: COLUMN sleep_movement_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_movement_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN sleep_movement_raw.sleep_movement; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.sleep_movement_raw.sleep_movement IS 'Column sleep_movement.';


--
-- Name: spo2_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.spo2_raw (
    ts_utc timestamp with time zone NOT NULL,
    spo2_reading smallint,
    spo2_confidence smallint
);


--
-- Name: TABLE spo2_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.spo2_raw IS 'Table spo2_raw.';


--
-- Name: COLUMN spo2_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.spo2_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN spo2_raw.spo2_reading; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.spo2_raw.spo2_reading IS 'Column spo2_reading.';


--
-- Name: COLUMN spo2_raw.spo2_confidence; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.spo2_raw.spo2_confidence IS 'Column spo2_confidence.';


--
-- Name: stress_raw; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.stress_raw (
    ts_utc timestamp with time zone NOT NULL,
    stress_value smallint
);


--
-- Name: TABLE stress_raw; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.stress_raw IS 'Table stress_raw.';


--
-- Name: COLUMN stress_raw.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.stress_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN stress_raw.stress_value; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.stress_raw.stress_value IS 'Column stress_value.';


--
-- Name: vw_body_comp_interpolation_precalc; Type: VIEW; Schema: health; Owner: -
--

CREATE VIEW health.vw_body_comp_interpolation_precalc AS
 WITH precalc AS (
         SELECT bc.ts_utc,
            COALESCE(lead(bc.ts_utc) OVER (ORDER BY bc.ts_utc), CURRENT_TIMESTAMP) AS next_ts_utc,
            (floor(EXTRACT(epoch FROM (COALESCE(lead(bc.ts_utc) OVER (ORDER BY bc.ts_utc), CURRENT_TIMESTAMP) - bc.ts_utc))))::integer AS delta_s,
            bc.weight_total_g,
            (COALESCE(lead(bc.weight_total_g) OVER (ORDER BY bc.ts_utc), bc.weight_total_g) - bc.weight_total_g) AS weight_delta_g,
            bc.weight_bone_g,
            (COALESCE(lead(bc.weight_bone_g) OVER (ORDER BY bc.ts_utc), bc.weight_bone_g) - bc.weight_bone_g) AS weight_bone_delta_g,
            bc.weight_muscle_g,
            (COALESCE(lead(bc.weight_muscle_g) OVER (ORDER BY bc.ts_utc), bc.weight_muscle_g) - bc.weight_muscle_g) AS weight_muscle_delta_g,
            bc.fat_pct,
            (COALESCE(lead(bc.fat_pct) OVER (ORDER BY bc.ts_utc), bc.fat_pct) - bc.fat_pct) AS delta_fat_pct,
            bc.water_pct,
            (COALESCE(lead(bc.water_pct) OVER (ORDER BY bc.ts_utc), bc.water_pct) - bc.water_pct) AS delta_water_pct
           FROM health.body_composition_raw bc
        )
 SELECT ts_utc,
    next_ts_utc,
    weight_total_g,
    delta_s,
    ((weight_delta_g)::numeric / (delta_s)::numeric) AS weight_delta_per_s,
    weight_bone_g,
    ((weight_bone_delta_g)::numeric / (delta_s)::numeric) AS bone_delta_per_s,
    weight_muscle_g,
    ((weight_muscle_delta_g)::numeric / (delta_s)::numeric) AS muscle_delta_per_s,
    fat_pct,
    (delta_fat_pct / (delta_s)::numeric) AS fat_delta_per_s,
    water_pct,
    (delta_water_pct / (delta_s)::numeric) AS water_delta_per_s
   FROM precalc;


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.next_ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.next_ts_utc IS 'Column next_ts_utc.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.weight_total_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.weight_total_g IS 'Column weight_total_g.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.delta_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.delta_s IS 'Column delta_s.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.weight_delta_per_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.weight_delta_per_s IS 'Column weight_delta_per_s.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.weight_bone_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.weight_bone_g IS 'Column weight_bone_g.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.bone_delta_per_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.bone_delta_per_s IS 'Column bone_delta_per_s.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.weight_muscle_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.weight_muscle_g IS 'Column weight_muscle_g.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.muscle_delta_per_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.muscle_delta_per_s IS 'Column muscle_delta_per_s.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.fat_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.fat_pct IS 'Column fat_pct.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.fat_delta_per_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.fat_delta_per_s IS 'Column fat_delta_per_s.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.water_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.water_pct IS 'Column water_pct.';


--
-- Name: COLUMN vw_body_comp_interpolation_precalc.water_delta_per_s; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_body_comp_interpolation_precalc.water_delta_per_s IS 'Column water_delta_per_s.';


--
-- Name: vw_activity_weights; Type: VIEW; Schema: health; Owner: -
--

CREATE VIEW health.vw_activity_weights AS
 WITH a AS (
         SELECT activities.activity_id,
            activities.start_time_utc
           FROM activities.activities
        ), bcj AS (
         SELECT a.activity_id,
            a.start_time_utc AS activity_start,
            floor(EXTRACT(epoch FROM (a.start_time_utc - bc.ts_utc))) AS s_since_weigh_in,
            bc.weight_total_g,
            bc.weight_delta_per_s,
            bc.weight_bone_g,
            bc.bone_delta_per_s,
            bc.weight_muscle_g,
            bc.muscle_delta_per_s,
            bc.fat_pct,
            bc.fat_delta_per_s,
            bc.water_pct,
            bc.water_delta_per_s
           FROM (a
             LEFT JOIN health.vw_body_comp_interpolation_precalc bc ON (((a.start_time_utc >= bc.ts_utc) AND (a.start_time_utc < bc.next_ts_utc))))
        ), calcs AS (
         SELECT bcj.activity_id,
            bcj.activity_start,
            round(((bcj.weight_total_g)::numeric + (bcj.weight_delta_per_s * bcj.s_since_weigh_in)), 0) AS weight_total_g,
            round(((bcj.weight_bone_g)::numeric + (bcj.bone_delta_per_s * bcj.s_since_weigh_in)), 0) AS weight_bone_g,
            round(((bcj.weight_muscle_g)::numeric + (bcj.muscle_delta_per_s * bcj.s_since_weigh_in)), 0) AS weight_muscle_g,
            round((bcj.fat_pct + (bcj.fat_delta_per_s * bcj.s_since_weigh_in)), 4) AS fat_pct,
            round((bcj.water_pct + (bcj.water_delta_per_s * bcj.s_since_weigh_in)), 3) AS water_pct
           FROM bcj
        )
 SELECT activity_id,
    weight_total_g,
    weight_bone_g,
    weight_muscle_g,
    round((weight_total_g * fat_pct), 0) AS weight_fat_g,
    fat_pct,
    water_pct
   FROM calcs;


--
-- Name: COLUMN vw_activity_weights.activity_id; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_weights.weight_total_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.weight_total_g IS 'Column weight_total_g.';


--
-- Name: COLUMN vw_activity_weights.weight_bone_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.weight_bone_g IS 'Column weight_bone_g.';


--
-- Name: COLUMN vw_activity_weights.weight_muscle_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.weight_muscle_g IS 'Column weight_muscle_g.';


--
-- Name: COLUMN vw_activity_weights.weight_fat_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.weight_fat_g IS 'Column weight_fat_g.';


--
-- Name: COLUMN vw_activity_weights.fat_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.fat_pct IS 'Column fat_pct.';


--
-- Name: COLUMN vw_activity_weights.water_pct; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.vw_activity_weights.water_pct IS 'Column water_pct.';


--
-- Name: weight_target; Type: TABLE; Schema: health; Owner: -
--

CREATE TABLE health.weight_target (
    ts_utc timestamp with time zone NOT NULL,
    weight_total_g integer
);


--
-- Name: TABLE weight_target; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON TABLE health.weight_target IS 'Table weight_target.';


--
-- Name: COLUMN weight_target.ts_utc; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.weight_target.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN weight_target.weight_total_g; Type: COMMENT; Schema: health; Owner: -
--

COMMENT ON COLUMN health.weight_target.weight_total_g IS 'Column weight_total_g.';


--
-- Name: api_logins; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.api_logins (
    event_time_utc timestamp with time zone DEFAULT now() NOT NULL,
    api_service_name text,
    event_name text,
    token_age_s smallint,
    error_text text
);


--
-- Name: TABLE api_logins; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.api_logins IS 'Table api_logins.';


--
-- Name: COLUMN api_logins.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.api_logins.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN api_logins.api_service_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.api_logins.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN api_logins.event_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.api_logins.event_name IS 'Column event_name.';


--
-- Name: COLUMN api_logins.token_age_s; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.api_logins.token_age_s IS 'Column token_age_s.';


--
-- Name: COLUMN api_logins.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.api_logins.error_text IS 'Column error_text.';


--
-- Name: application_events; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.application_events (
    event_time_utc timestamp with time zone DEFAULT now() NOT NULL,
    event_category text,
    event_description text,
    execution_time_ms integer,
    error_text text,
    task_id integer,
    data_event text
);


--
-- Name: TABLE application_events; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.application_events IS 'Table application_events.';


--
-- Name: COLUMN application_events.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN application_events.event_category; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.event_category IS 'Column event_category.';


--
-- Name: COLUMN application_events.event_description; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.event_description IS 'Column event_description.';


--
-- Name: COLUMN application_events.execution_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.execution_time_ms IS 'Column execution_time_ms.';


--
-- Name: COLUMN application_events.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.error_text IS 'Column error_text.';


--
-- Name: COLUMN application_events.task_id; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.task_id IS 'Column task_id.';


--
-- Name: COLUMN application_events.data_event; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.application_events.data_event IS 'Column data_event.';


--
-- Name: db_size_log; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.db_size_log (
    event_time_utc timestamp with time zone DEFAULT now(),
    table_name text,
    total_size_mb numeric(5,1),
    table_size_mb numeric(5,1),
    index_size_mb numeric(5,1)
);


--
-- Name: TABLE db_size_log; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.db_size_log IS 'Table db_size_log.';


--
-- Name: COLUMN db_size_log.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_size_log.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN db_size_log.table_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_size_log.table_name IS 'Column table_name.';


--
-- Name: COLUMN db_size_log.total_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_size_log.total_size_mb IS 'Column total_size_mb.';


--
-- Name: COLUMN db_size_log.table_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_size_log.table_size_mb IS 'Column table_size_mb.';


--
-- Name: COLUMN db_size_log.index_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_size_log.index_size_mb IS 'Column index_size_mb.';


--
-- Name: db_stats; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.db_stats (
    event_time_utc timestamp with time zone DEFAULT now(),
    size_before_mb numeric(5,1),
    size_after_mb numeric(5,1),
    maintenance_time_ms integer,
    total_time_ms integer,
    maintenance_type text
);


--
-- Name: TABLE db_stats; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.db_stats IS 'Table db_stats.';


--
-- Name: COLUMN db_stats.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN db_stats.size_before_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.size_before_mb IS 'Column size_before_mb.';


--
-- Name: COLUMN db_stats.size_after_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.size_after_mb IS 'Column size_after_mb.';


--
-- Name: COLUMN db_stats.maintenance_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.maintenance_time_ms IS 'Column maintenance_time_ms.';


--
-- Name: COLUMN db_stats.total_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.total_time_ms IS 'Column total_time_ms.';


--
-- Name: COLUMN db_stats.maintenance_type; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.db_stats.maintenance_type IS 'Column maintenance_type.';


--
-- Name: health_checks; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.health_checks (
    check_id bigint NOT NULL,
    check_time_utc timestamp with time zone DEFAULT now() NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL,
    value numeric,
    message text,
    CONSTRAINT health_checks_status_check CHECK ((status = ANY (ARRAY['ok'::text, 'warn'::text, 'critical'::text])))
);


--
-- Name: TABLE health_checks; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.health_checks IS 'Table health_checks.';


--
-- Name: COLUMN health_checks.check_id; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.check_id IS 'Column check_id.';


--
-- Name: COLUMN health_checks.check_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.check_time_utc IS 'Column check_time_utc.';


--
-- Name: COLUMN health_checks.check_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.check_name IS 'Column check_name.';


--
-- Name: COLUMN health_checks.status; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.status IS 'Column status.';


--
-- Name: COLUMN health_checks.value; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.value IS 'Column value.';


--
-- Name: COLUMN health_checks.message; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.health_checks.message IS 'Column message.';


--
-- Name: health_checks_check_id_seq; Type: SEQUENCE; Schema: logging; Owner: -
--

CREATE SEQUENCE logging.health_checks_check_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: health_checks_check_id_seq; Type: SEQUENCE OWNED BY; Schema: logging; Owner: -
--

ALTER SEQUENCE logging.health_checks_check_id_seq OWNED BY logging.health_checks.check_id;


--
-- Name: music_db_info; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.music_db_info (
    ts_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    track_id_count integer,
    isrc_id_count integer,
    mapped_isrc_id_count integer,
    artist_id_count integer,
    album_id_count integer
);


--
-- Name: TABLE music_db_info; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.music_db_info IS 'Table music_db_info.';


--
-- Name: COLUMN music_db_info.ts_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN music_db_info.track_id_count; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.track_id_count IS 'Column track_id_count.';


--
-- Name: COLUMN music_db_info.isrc_id_count; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.isrc_id_count IS 'Column isrc_id_count.';


--
-- Name: COLUMN music_db_info.mapped_isrc_id_count; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.mapped_isrc_id_count IS 'Column mapped_isrc_id_count.';


--
-- Name: COLUMN music_db_info.artist_id_count; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.artist_id_count IS 'Column artist_id_count.';


--
-- Name: COLUMN music_db_info.album_id_count; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.music_db_info.album_id_count IS 'Column album_id_count.';


--
-- Name: task_executions; Type: TABLE; Schema: logging; Owner: -
--

CREATE TABLE logging.task_executions (
    record_id bigint NOT NULL,
    event_time_utc timestamp with time zone DEFAULT now(),
    task_name text,
    extract_time_ms integer,
    load_time_ms integer,
    transform_time_ms integer,
    failure_type text,
    error_text text,
    task_id integer,
    interpolation_time_ms integer,
    forecast_time_ms integer
);


--
-- Name: TABLE task_executions; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON TABLE logging.task_executions IS 'Table task_executions.';


--
-- Name: COLUMN task_executions.record_id; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.record_id IS 'Column record_id.';


--
-- Name: COLUMN task_executions.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN task_executions.task_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.task_name IS 'Column task_name.';


--
-- Name: COLUMN task_executions.extract_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.extract_time_ms IS 'Column extract_time_ms.';


--
-- Name: COLUMN task_executions.load_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.load_time_ms IS 'Column load_time_ms.';


--
-- Name: COLUMN task_executions.transform_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.transform_time_ms IS 'Column transform_time_ms.';


--
-- Name: COLUMN task_executions.failure_type; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.failure_type IS 'Column failure_type.';


--
-- Name: COLUMN task_executions.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.error_text IS 'Column error_text.';


--
-- Name: COLUMN task_executions.task_id; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.task_id IS 'Column task_id.';


--
-- Name: COLUMN task_executions.interpolation_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.interpolation_time_ms IS 'Column interpolation_time_ms.';


--
-- Name: COLUMN task_executions.forecast_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.task_executions.forecast_time_ms IS 'Column forecast_time_ms.';


--
-- Name: task_executions_record_id_seq; Type: SEQUENCE; Schema: logging; Owner: -
--

CREATE SEQUENCE logging.task_executions_record_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_executions_record_id_seq; Type: SEQUENCE OWNED BY; Schema: logging; Owner: -
--

ALTER SEQUENCE logging.task_executions_record_id_seq OWNED BY logging.task_executions.record_id;


--
-- Name: vw_all_event_history; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_all_event_history AS
 WITH logins AS (
         SELECT api_logins.event_time_utc,
            'Login'::text AS event_type,
            (((COALESCE(((api_logins.api_service_name || ' '::text) || api_logins.event_name), 'Unknown'::text) || '('::text) || (COALESCE((api_logins.token_age_s)::integer, 0))::text) || ')'::text) AS description,
            api_logins.error_text
           FROM logging.api_logins
          WHERE (api_logins.event_name IS NOT NULL)
        ), app_events AS (
         SELECT application_events.event_time_utc,
            'App Event'::text AS event_type,
            (((((application_events.event_category || ' '::text) || application_events.event_description) || '('::text) || ((COALESCE(application_events.execution_time_ms, 0) / 1000))::text) || ')'::text) AS description,
            application_events.error_text
           FROM logging.application_events
        ), task_events AS (
         SELECT task_executions.event_time_utc,
            'Task Execution'::text AS event_type,
            task_executions.task_name AS description,
            ((task_executions.error_text || ' @ '::text) || task_executions.failure_type) AS error_text
           FROM logging.task_executions
        ), combo AS (
         SELECT logins.event_time_utc,
            logins.event_type,
            logins.description,
            logins.error_text
           FROM logins
        UNION ALL
         SELECT app_events.event_time_utc,
            app_events.event_type,
            app_events.description,
            app_events.error_text
           FROM app_events
        UNION ALL
         SELECT task_events.event_time_utc,
            task_events.event_type,
            task_events.description,
            task_events.error_text
           FROM task_events
        ), qfinal AS (
         SELECT combo.event_time_utc,
            combo.event_type,
            combo.description,
            combo.error_text,
            (COALESCE(combo.error_text, 'None'::text) <> 'None'::text) AS is_error
           FROM combo
        )
 SELECT event_time_utc,
    event_type,
    description,
    error_text,
    is_error,
    (description !~~ '%skipped%'::text) AS not_skip_row
   FROM qfinal
  ORDER BY event_time_utc DESC;


--
-- Name: COLUMN vw_all_event_history.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN vw_all_event_history.event_type; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.event_type IS 'Column event_type.';


--
-- Name: COLUMN vw_all_event_history.description; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.description IS 'Column description.';


--
-- Name: COLUMN vw_all_event_history.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.error_text IS 'Column error_text.';


--
-- Name: COLUMN vw_all_event_history.is_error; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.is_error IS 'Column is_error.';


--
-- Name: COLUMN vw_all_event_history.not_skip_row; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_all_event_history.not_skip_row IS 'Column not_skip_row.';


--
-- Name: dim_date; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_date (
    date_key integer NOT NULL,
    date_utc date NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    day_of_month smallint NOT NULL,
    day_of_week smallint NOT NULL,
    day_name text NOT NULL,
    month_name text NOT NULL,
    week_of_year smallint NOT NULL,
    day_of_year smallint
);


--
-- Name: vw_date; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vw_date AS
 WITH anchor AS (
         SELECT CURRENT_DATE AS today
        )
 SELECT d.date_key,
    d.date_utc,
    d.year,
    d.quarter,
    d.month,
    d.day_of_month,
    d.day_of_week,
    d.day_name,
    d.month_name,
    d.week_of_year,
    (d.date_utc - a.today) AS relative_day,
    (((date_trunc('week'::text, (d.date_utc)::timestamp with time zone))::date - (date_trunc('week'::text, (a.today)::timestamp with time zone))::date) / 7) AS relative_week,
    ((((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)) * (12)::numeric) + (EXTRACT(month FROM d.date_utc) - EXTRACT(month FROM a.today))))::integer AS relative_month,
    ((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)))::integer AS relative_year,
    d.day_of_year,
    floor((((d.date_utc - a.today) / 30))::double precision) AS relative_30,
    floor((((d.date_utc - a.today) / 90))::double precision) AS relative_90,
    (((d.date_utc - a.today))::double precision - (floor((((d.date_utc - a.today) / 30))::double precision) * (30)::double precision)) AS dm30,
    (((d.date_utc - a.today))::double precision - (floor((((d.date_utc - a.today) / 90))::double precision) * (90)::double precision)) AS dm90
   FROM (public.dim_date d
     CROSS JOIN anchor a)
  WHERE (d.date_utc <= a.today)
  ORDER BY d.date_utc DESC;


--
-- Name: vw_db_biggest_table_chart; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_db_biggest_table_chart AS
 WITH tt AS (
         SELECT unnamed_subquery.db_table
           FROM ( SELECT unnamed_subquery_1.db_table,
                    row_number() OVER (ORDER BY unnamed_subquery_1.biggest_table DESC) AS rval
                   FROM ( SELECT l.table_name AS db_table,
                            max(l.total_size_mb) AS biggest_table
                           FROM logging.db_size_log l
                          GROUP BY l.table_name) unnamed_subquery_1) unnamed_subquery
          WHERE (unnamed_subquery.rval < 4)
        ), td AS (
         SELECT unnamed_subquery.table_name AS db_table,
            d.date_key,
            d.date_utc
           FROM (( SELECT DISTINCT db_size_log.table_name
                   FROM logging.db_size_log) unnamed_subquery
             CROSS JOIN public.vw_date d)
          WHERE (d.date_utc > '2025-11-16'::date)
        )
 SELECT td.date_utc,
    td.db_table,
    max(sl.total_size_mb) AS total_size_mb
   FROM ((td
     JOIN tt ON ((tt.db_table = td.db_table)))
     LEFT JOIN logging.db_size_log sl ON (((sl.table_name = td.db_table) AND (td.date_key = (to_char(sl.event_time_utc, 'YYYYMMDD'::text))::integer))))
  GROUP BY td.date_utc, td.db_table
  ORDER BY td.date_utc;


--
-- Name: COLUMN vw_db_biggest_table_chart.date_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_biggest_table_chart.date_utc IS 'Column date_utc.';


--
-- Name: COLUMN vw_db_biggest_table_chart.db_table; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_biggest_table_chart.db_table IS 'Column db_table.';


--
-- Name: COLUMN vw_db_biggest_table_chart.total_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_biggest_table_chart.total_size_mb IS 'Column total_size_mb.';


--
-- Name: vw_db_size; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_db_size AS
 WITH bq AS (
         SELECT (((pg_stat_user_tables.schemaname)::text || '.'::text) || (pg_stat_user_tables.relname)::text) AS table_name,
            (((pg_total_relation_size((pg_stat_user_tables.relid)::regclass) / 1024) / 1024))::numeric(10,1) AS total_size_mb,
            (((pg_relation_size((pg_stat_user_tables.relid)::regclass) / 1024) / 1024))::numeric(10,1) AS table_size_mb,
            (((pg_indexes_size((pg_stat_user_tables.relid)::regclass) / 1024) / 1024))::numeric(10,1) AS index_size_mb
           FROM pg_stat_user_tables
        )
 SELECT table_name,
    total_size_mb,
    table_size_mb,
    index_size_mb,
    (total_size_mb - table_size_mb) AS other_size_mb
   FROM bq
  WHERE (total_size_mb > (0)::numeric)
  ORDER BY total_size_mb DESC;


--
-- Name: COLUMN vw_db_size.table_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size.table_name IS 'Column table_name.';


--
-- Name: COLUMN vw_db_size.total_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size.total_size_mb IS 'Column total_size_mb.';


--
-- Name: COLUMN vw_db_size.table_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size.table_size_mb IS 'Column table_size_mb.';


--
-- Name: COLUMN vw_db_size.index_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size.index_size_mb IS 'Column index_size_mb.';


--
-- Name: COLUMN vw_db_size.other_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size.other_size_mb IS 'Column other_size_mb.';


--
-- Name: vw_db_size_chart; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_db_size_chart AS
 WITH table_list AS (
         SELECT DISTINCT unnamed_subquery.table_name,
            d.date_key,
            d.date_utc
           FROM (( SELECT DISTINCT db_size_log.table_name
                   FROM logging.db_size_log) unnamed_subquery
             CROSS JOIN public.vw_date d)
          WHERE (d.date_utc > '2025-11-16'::date)
        ), max_records AS (
         SELECT vals.table_name,
            vals.date_utc,
            vals.table_size_mb,
            vals.index_size_mb,
            vals.other_size_mb
           FROM ( SELECT t.table_name,
                    t.date_key,
                    t.date_utc,
                    sl.table_size_mb,
                    sl.index_size_mb,
                    (sl.total_size_mb - (sl.table_size_mb + sl.index_size_mb)) AS other_size_mb,
                    row_number() OVER (PARTITION BY t.table_name, t.date_key ORDER BY sl.event_time_utc DESC) AS rval
                   FROM (table_list t
                     LEFT JOIN logging.db_size_log sl ON (((t.table_name = sl.table_name) AND (t.date_key = (to_char(sl.event_time_utc, 'YYYYMMDD'::text))::integer))))) vals
          WHERE (vals.rval = 1)
        )
 SELECT date_utc,
    sum(table_size_mb) AS table_size_mb,
    sum(index_size_mb) AS index_size_mb,
    sum(other_size_mb) AS other_size_mb
   FROM max_records
  GROUP BY date_utc
  ORDER BY date_utc;


--
-- Name: COLUMN vw_db_size_chart.date_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size_chart.date_utc IS 'Column date_utc.';


--
-- Name: COLUMN vw_db_size_chart.table_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size_chart.table_size_mb IS 'Column table_size_mb.';


--
-- Name: COLUMN vw_db_size_chart.index_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size_chart.index_size_mb IS 'Column index_size_mb.';


--
-- Name: COLUMN vw_db_size_chart.other_size_mb; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_db_size_chart.other_size_mb IS 'Column other_size_mb.';


--
-- Name: vw_event_analysis; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_event_analysis AS
 SELECT public.time_ago(event_time_utc) AS task_age,
    event_category,
    event_description,
    error_text,
    execution_time_ms,
    event_time_utc,
    (error_text IS NOT NULL) AS is_error
   FROM logging.application_events
  ORDER BY event_time_utc DESC;


--
-- Name: COLUMN vw_event_analysis.task_age; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.task_age IS 'Column task_age.';


--
-- Name: COLUMN vw_event_analysis.event_category; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.event_category IS 'Column event_category.';


--
-- Name: COLUMN vw_event_analysis.event_description; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.event_description IS 'Column event_description.';


--
-- Name: COLUMN vw_event_analysis.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.error_text IS 'Column error_text.';


--
-- Name: COLUMN vw_event_analysis.execution_time_ms; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.execution_time_ms IS 'Column execution_time_ms.';


--
-- Name: COLUMN vw_event_analysis.event_time_utc; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN vw_event_analysis.is_error; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_event_analysis.is_error IS 'Column is_error.';


--
-- Name: vw_last_login; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_last_login AS
 SELECT DISTINCT ON (api_service_name) api_service_name,
    (round(EXTRACT(epoch FROM (public.utc_now() - event_time_utc)), 0))::integer AS seconds_since_last_event
   FROM logging.api_logins
  WHERE (event_name ~~ 'login%'::text)
  ORDER BY api_service_name, event_time_utc DESC;


--
-- Name: COLUMN vw_last_login.api_service_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_last_login.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN vw_last_login.seconds_since_last_event; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_last_login.seconds_since_last_event IS 'Column seconds_since_last_event.';


--
-- Name: vw_task_analysis; Type: VIEW; Schema: logging; Owner: -
--

CREATE VIEW logging.vw_task_analysis AS
 SELECT public.time_ago(event_time_utc) AS task_age,
    task_name,
    (failure_type IS NOT NULL) AS is_failure,
    error_text
   FROM logging.task_executions
  ORDER BY event_time_utc DESC;


--
-- Name: COLUMN vw_task_analysis.task_age; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_task_analysis.task_age IS 'Column task_age.';


--
-- Name: COLUMN vw_task_analysis.task_name; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_task_analysis.task_name IS 'Column task_name.';


--
-- Name: COLUMN vw_task_analysis.is_failure; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_task_analysis.is_failure IS 'Column is_failure.';


--
-- Name: COLUMN vw_task_analysis.error_text; Type: COMMENT; Schema: logging; Owner: -
--

COMMENT ON COLUMN logging.vw_task_analysis.error_text IS 'Column error_text.';


--
-- Name: all_albums; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.all_albums (
    album_id text NOT NULL,
    album_name text,
    album_name_clean text,
    album_name_search text,
    updated_at_utc timestamp with time zone
);


--
-- Name: TABLE all_albums; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.all_albums IS 'Table all_albums.';


--
-- Name: COLUMN all_albums.album_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_albums.album_id IS 'Column album_id.';


--
-- Name: COLUMN all_albums.album_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_albums.album_name IS 'Column album_name.';


--
-- Name: COLUMN all_albums.album_name_clean; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_albums.album_name_clean IS 'Column album_name_clean.';


--
-- Name: COLUMN all_albums.album_name_search; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_albums.album_name_search IS 'Column album_name_search.';


--
-- Name: COLUMN all_albums.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_albums.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: all_artists; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.all_artists (
    artist_id text NOT NULL,
    artist_name text,
    artist_name_search text,
    updated_at_utc timestamp with time zone
);


--
-- Name: TABLE all_artists; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.all_artists IS 'Table all_artists.';


--
-- Name: COLUMN all_artists.artist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_artists.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN all_artists.artist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_artists.artist_name IS 'Column artist_name.';


--
-- Name: COLUMN all_artists.artist_name_search; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_artists.artist_name_search IS 'Column artist_name_search.';


--
-- Name: COLUMN all_artists.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.all_artists.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: isrc_non_swaps; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.isrc_non_swaps (
    original_isrc text,
    mapped_isrc text,
    mapping_time_utc timestamp with time zone
);


--
-- Name: TABLE isrc_non_swaps; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.isrc_non_swaps IS 'Table isrc_non_swaps.';


--
-- Name: COLUMN isrc_non_swaps.original_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_non_swaps.original_isrc IS 'Column original_isrc.';


--
-- Name: COLUMN isrc_non_swaps.mapped_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_non_swaps.mapped_isrc IS 'Column mapped_isrc.';


--
-- Name: COLUMN isrc_non_swaps.mapping_time_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_non_swaps.mapping_time_utc IS 'Column mapping_time_utc.';


--
-- Name: isrc_possible_dupes; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.isrc_possible_dupes (
    isrc1 text,
    isrc2 text,
    match_score numeric(3,2),
    track_score numeric(3,2),
    artist_score numeric(3,2),
    album_score numeric(3,2)
);


--
-- Name: TABLE isrc_possible_dupes; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.isrc_possible_dupes IS 'Table isrc_possible_dupes.';


--
-- Name: COLUMN isrc_possible_dupes.isrc1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.isrc1 IS 'Column isrc1.';


--
-- Name: COLUMN isrc_possible_dupes.isrc2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.isrc2 IS 'Column isrc2.';


--
-- Name: COLUMN isrc_possible_dupes.match_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.match_score IS 'Column match_score.';


--
-- Name: COLUMN isrc_possible_dupes.track_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.track_score IS 'Column track_score.';


--
-- Name: COLUMN isrc_possible_dupes.artist_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.artist_score IS 'Column artist_score.';


--
-- Name: COLUMN isrc_possible_dupes.album_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.isrc_possible_dupes.album_score IS 'Column album_score.';


--
-- Name: listening_history; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.listening_history (
    played_at_utc timestamp with time zone NOT NULL,
    isrc text NOT NULL,
    playlist_id text
);


--
-- Name: TABLE listening_history; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.listening_history IS 'Table listening_history.';


--
-- Name: COLUMN listening_history.played_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.listening_history.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN listening_history.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.listening_history.isrc IS 'Column isrc.';


--
-- Name: COLUMN listening_history.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.listening_history.playlist_id IS 'Column playlist_id.';


--
-- Name: missing_relationships; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.missing_relationships AS
 SELECT playlist_id,
    playlist_name,
    needs_auto,
    needs_manual,
    needs_recs
   FROM ( SELECT pc.playlist_id,
            pc.playlist_name,
            (pc.auto_shuffle AND (a.child_playlist_id IS NULL)) AS needs_auto,
            (pc.manual_shuffle AND (m.child_playlist_id IS NULL)) AS needs_manual,
            (pc.make_recs AND (r.child_playlist_id IS NULL)) AS needs_recs
           FROM (((music.playlist_config pc
             LEFT JOIN music.playlist_relationships m ON (((m.parent_playlist_id = pc.playlist_id) AND (m.child_playlist_type = 'manual'::text))))
             LEFT JOIN music.playlist_relationships a ON (((a.parent_playlist_id = pc.playlist_id) AND (a.child_playlist_type = 'auto'::text))))
             LEFT JOIN music.playlist_relationships r ON (((r.parent_playlist_id = pc.playlist_id) AND (r.child_playlist_type = 'recommendation'::text))))) unnamed_subquery
  WHERE (needs_auto OR needs_manual OR needs_recs);


--
-- Name: COLUMN missing_relationships.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.missing_relationships.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN missing_relationships.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.missing_relationships.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN missing_relationships.needs_auto; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.missing_relationships.needs_auto IS 'Column needs_auto.';


--
-- Name: COLUMN missing_relationships.needs_manual; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.missing_relationships.needs_manual IS 'Column needs_manual.';


--
-- Name: COLUMN missing_relationships.needs_recs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.missing_relationships.needs_recs IS 'Column needs_recs.';


--
-- Name: playlist_recommendation_exclusions; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.playlist_recommendation_exclusions (
    playlist_id text,
    isrc text,
    elo_track_predicted text,
    decision_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE playlist_recommendation_exclusions; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.playlist_recommendation_exclusions IS 'Table playlist_recommendation_exclusions.';


--
-- Name: COLUMN playlist_recommendation_exclusions.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_recommendation_exclusions.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN playlist_recommendation_exclusions.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_recommendation_exclusions.isrc IS 'Column isrc.';


--
-- Name: COLUMN playlist_recommendation_exclusions.elo_track_predicted; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_recommendation_exclusions.elo_track_predicted IS 'Column elo_track_predicted.';


--
-- Name: COLUMN playlist_recommendation_exclusions.decision_timestamp; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.playlist_recommendation_exclusions.decision_timestamp IS 'Column decision_timestamp.';


--
-- Name: ratings; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.ratings (
    playlist_id text NOT NULL,
    isrc text NOT NULL,
    elo_rating integer,
    rating_count integer,
    rating_wins integer,
    rating_losses integer,
    last_rated_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE ratings; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.ratings IS 'Table ratings.';


--
-- Name: COLUMN ratings.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN ratings.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.isrc IS 'Column isrc.';


--
-- Name: COLUMN ratings.elo_rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.elo_rating IS 'Column elo_rating.';


--
-- Name: COLUMN ratings.rating_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.rating_count IS 'Column rating_count.';


--
-- Name: COLUMN ratings.rating_wins; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.rating_wins IS 'Column rating_wins.';


--
-- Name: COLUMN ratings.rating_losses; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.rating_losses IS 'Column rating_losses.';


--
-- Name: COLUMN ratings.last_rated_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings.last_rated_utc IS 'Column last_rated_utc.';


--
-- Name: ratings_history; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.ratings_history (
    rating_time_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    playlist_id text NOT NULL,
    isrc text NOT NULL,
    isrc_vs text,
    elo_old integer,
    elo_new integer,
    rating_result smallint
);


--
-- Name: TABLE ratings_history; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.ratings_history IS 'Table ratings_history.';


--
-- Name: COLUMN ratings_history.rating_time_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.rating_time_utc IS 'Column rating_time_utc.';


--
-- Name: COLUMN ratings_history.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN ratings_history.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.isrc IS 'Column isrc.';


--
-- Name: COLUMN ratings_history.isrc_vs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.isrc_vs IS 'Column isrc_vs.';


--
-- Name: COLUMN ratings_history.elo_old; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.elo_old IS 'Column elo_old.';


--
-- Name: COLUMN ratings_history.elo_new; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.elo_new IS 'Column elo_new.';


--
-- Name: COLUMN ratings_history.rating_result; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.ratings_history.rating_result IS 'Column rating_result.';


--
-- Name: temp_listening_history; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.temp_listening_history (
    played_at_utc timestamp with time zone,
    isrc text,
    playlist_id text
);


--
-- Name: TABLE temp_listening_history; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.temp_listening_history IS 'Table temp_listening_history.';


--
-- Name: COLUMN temp_listening_history.played_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.temp_listening_history.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN temp_listening_history.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.temp_listening_history.isrc IS 'Column isrc.';


--
-- Name: COLUMN temp_listening_history.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.temp_listening_history.playlist_id IS 'Column playlist_id.';


--
-- Name: track_recommendation_decisions; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.track_recommendation_decisions (
    playlist_id text,
    isrc text,
    elo_track_linear numeric,
    elo_track_rf numeric,
    elo_track_neural_net numeric,
    elo_track_pairwise numeric,
    elo_track_predicted numeric,
    elo_artist_snap numeric,
    elo_genre_snap numeric,
    popularity_snap numeric,
    was_promoted boolean,
    decision_time_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    model_version text,
    notes text
);


--
-- Name: TABLE track_recommendation_decisions; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.track_recommendation_decisions IS 'Table track_recommendation_decisions.';


--
-- Name: COLUMN track_recommendation_decisions.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN track_recommendation_decisions.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.isrc IS 'Column isrc.';


--
-- Name: COLUMN track_recommendation_decisions.elo_track_linear; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_track_linear IS 'Column elo_track_linear.';


--
-- Name: COLUMN track_recommendation_decisions.elo_track_rf; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_track_rf IS 'Column elo_track_rf.';


--
-- Name: COLUMN track_recommendation_decisions.elo_track_neural_net; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_track_neural_net IS 'Column elo_track_neural_net.';


--
-- Name: COLUMN track_recommendation_decisions.elo_track_pairwise; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_track_pairwise IS 'Column elo_track_pairwise.';


--
-- Name: COLUMN track_recommendation_decisions.elo_track_predicted; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_track_predicted IS 'Column elo_track_predicted.';


--
-- Name: COLUMN track_recommendation_decisions.elo_artist_snap; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_artist_snap IS 'Column elo_artist_snap.';


--
-- Name: COLUMN track_recommendation_decisions.elo_genre_snap; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.elo_genre_snap IS 'Column elo_genre_snap.';


--
-- Name: COLUMN track_recommendation_decisions.popularity_snap; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.popularity_snap IS 'Column popularity_snap.';


--
-- Name: COLUMN track_recommendation_decisions.was_promoted; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.was_promoted IS 'Column was_promoted.';


--
-- Name: COLUMN track_recommendation_decisions.decision_time_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.decision_time_utc IS 'Column decision_time_utc.';


--
-- Name: COLUMN track_recommendation_decisions.model_version; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.model_version IS 'Column model_version.';


--
-- Name: COLUMN track_recommendation_decisions.notes; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendation_decisions.notes IS 'Column notes.';


--
-- Name: track_recommendations; Type: TABLE; Schema: music; Owner: -
--

CREATE TABLE music.track_recommendations (
    playlist_id text,
    isrc text,
    artist_id text,
    genre text,
    possible_genres text,
    artist_elo numeric,
    genre_elo numeric,
    playlist_genre_rank integer,
    is_best_genre boolean,
    track_name_clean text,
    popularity integer,
    m_acousticness numeric,
    m_energy numeric,
    m_instrumentalness numeric,
    m_key numeric,
    m_liveness numeric,
    m_mode numeric,
    m_speechiness numeric,
    m_tempo numeric,
    m_valence numeric,
    m_danceability numeric,
    elo_artist_norm numeric,
    elo_genre_norm numeric,
    elo_track_linear numeric,
    elo_track_random_forest numeric,
    elo_track_neural_net numeric,
    elo_track_pairwise numeric,
    elo_track_predicted numeric
);


--
-- Name: TABLE track_recommendations; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON TABLE music.track_recommendations IS 'Table track_recommendations.';


--
-- Name: COLUMN track_recommendations.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN track_recommendations.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.isrc IS 'Column isrc.';


--
-- Name: COLUMN track_recommendations.artist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN track_recommendations.genre; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.genre IS 'Column genre.';


--
-- Name: COLUMN track_recommendations.possible_genres; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.possible_genres IS 'Column possible_genres.';


--
-- Name: COLUMN track_recommendations.artist_elo; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.artist_elo IS 'Column artist_elo.';


--
-- Name: COLUMN track_recommendations.genre_elo; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.genre_elo IS 'Column genre_elo.';


--
-- Name: COLUMN track_recommendations.playlist_genre_rank; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.playlist_genre_rank IS 'Column playlist_genre_rank.';


--
-- Name: COLUMN track_recommendations.is_best_genre; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.is_best_genre IS 'Column is_best_genre.';


--
-- Name: COLUMN track_recommendations.track_name_clean; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN track_recommendations.popularity; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.popularity IS 'Column popularity.';


--
-- Name: COLUMN track_recommendations.m_acousticness; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_acousticness IS 'Column m_acousticness.';


--
-- Name: COLUMN track_recommendations.m_energy; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_energy IS 'Column m_energy.';


--
-- Name: COLUMN track_recommendations.m_instrumentalness; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_instrumentalness IS 'Column m_instrumentalness.';


--
-- Name: COLUMN track_recommendations.m_key; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_key IS 'Column m_key.';


--
-- Name: COLUMN track_recommendations.m_liveness; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_liveness IS 'Column m_liveness.';


--
-- Name: COLUMN track_recommendations.m_mode; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_mode IS 'Column m_mode.';


--
-- Name: COLUMN track_recommendations.m_speechiness; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_speechiness IS 'Column m_speechiness.';


--
-- Name: COLUMN track_recommendations.m_tempo; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_tempo IS 'Column m_tempo.';


--
-- Name: COLUMN track_recommendations.m_valence; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_valence IS 'Column m_valence.';


--
-- Name: COLUMN track_recommendations.m_danceability; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.m_danceability IS 'Column m_danceability.';


--
-- Name: COLUMN track_recommendations.elo_artist_norm; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_artist_norm IS 'Column elo_artist_norm.';


--
-- Name: COLUMN track_recommendations.elo_genre_norm; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_genre_norm IS 'Column elo_genre_norm.';


--
-- Name: COLUMN track_recommendations.elo_track_linear; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_track_linear IS 'Column elo_track_linear.';


--
-- Name: COLUMN track_recommendations.elo_track_random_forest; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_track_random_forest IS 'Column elo_track_random_forest.';


--
-- Name: COLUMN track_recommendations.elo_track_neural_net; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_track_neural_net IS 'Column elo_track_neural_net.';


--
-- Name: COLUMN track_recommendations.elo_track_pairwise; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_track_pairwise IS 'Column elo_track_pairwise.';


--
-- Name: COLUMN track_recommendations.elo_track_predicted; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.track_recommendations.elo_track_predicted IS 'Column elo_track_predicted.';


--
-- Name: vw_all_tracks; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_all_tracks AS
 SELECT at1.track_id,
    at1.track_name,
    at1.track_name_clean,
    at1.track_name_search,
    at1.track_isrc,
    at1.duration_ms,
    at1.artist_id,
    at1.artist_display_name,
    at1.album_id,
    at1.release_date,
    at1.available_in_us,
    at1.total_markets,
    at1.popularity,
    at1.updated_at_utc,
    ala.album_name_clean AS album_name,
    bt.best_track_id,
    (at1.track_id = bt.best_track_id) AS is_best_track_id,
    COALESCE(s.mapped_isrc, at1.track_isrc) AS best_isrc,
    (COALESCE(s.mapped_isrc, at1.track_isrc) = at1.track_isrc) AS is_best_isrc
   FROM (((music.all_tracks at1
     JOIN music.all_albums ala ON ((ala.album_id = at1.album_id)))
     LEFT JOIN music.isrc_swaps s ON (((s.original_isrc = at1.track_isrc) AND s.valid_mapping)))
     LEFT JOIN music.vw_best_track_id bt ON ((COALESCE(s.mapped_isrc, at1.track_isrc) = bt.isrc)));


--
-- Name: COLUMN vw_all_tracks.track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_all_tracks.track_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.track_name IS 'Column track_name.';


--
-- Name: COLUMN vw_all_tracks.track_name_clean; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN vw_all_tracks.track_name_search; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.track_name_search IS 'Column track_name_search.';


--
-- Name: COLUMN vw_all_tracks.track_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.track_isrc IS 'Column track_isrc.';


--
-- Name: COLUMN vw_all_tracks.duration_ms; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN vw_all_tracks.artist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN vw_all_tracks.artist_display_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.artist_display_name IS 'Column artist_display_name.';


--
-- Name: COLUMN vw_all_tracks.album_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.album_id IS 'Column album_id.';


--
-- Name: COLUMN vw_all_tracks.release_date; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.release_date IS 'Column release_date.';


--
-- Name: COLUMN vw_all_tracks.available_in_us; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.available_in_us IS 'Column available_in_us.';


--
-- Name: COLUMN vw_all_tracks.total_markets; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.total_markets IS 'Column total_markets.';


--
-- Name: COLUMN vw_all_tracks.popularity; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.popularity IS 'Column popularity.';


--
-- Name: COLUMN vw_all_tracks.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: COLUMN vw_all_tracks.album_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.album_name IS 'Column album_name.';


--
-- Name: COLUMN vw_all_tracks.best_track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.best_track_id IS 'Column best_track_id.';


--
-- Name: COLUMN vw_all_tracks.is_best_track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.is_best_track_id IS 'Column is_best_track_id.';


--
-- Name: COLUMN vw_all_tracks.best_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.best_isrc IS 'Column best_isrc.';


--
-- Name: COLUMN vw_all_tracks.is_best_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_all_tracks.is_best_isrc IS 'Column is_best_isrc.';


--
-- Name: vw_isrc_dupe_review; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_isrc_dupe_review AS
 SELECT ipd.isrc1,
    bt1.isrc_preference AS pref1,
    at1.track_name AS track_name1,
    at1.artist_display_name AS artist_name1,
    aa1.album_name AS album_name1,
    (round(((at1.duration_ms / 60))::numeric, 0))::integer AS duration_1,
    ipd.isrc2,
    bt2.isrc_preference AS pref2,
    at2.track_name AS track_name2,
    at2.artist_display_name AS artist_name2,
    aa2.album_name AS album_name2,
    (round(((at2.duration_ms / 60))::numeric, 0))::integer AS duration_2,
    ipd.match_score,
    ipd.track_score,
    ipd.artist_score,
    ipd.album_score,
    round(((LEAST(at1.duration_ms, at2.duration_ms))::numeric / (GREATEST(at1.duration_ms, at2.duration_ms))::numeric), 2) AS duration_score,
        CASE
            WHEN (at1.available_in_us AND (NOT at2.available_in_us)) THEN ipd.isrc1
            WHEN ((NOT at1.available_in_us) AND at2.available_in_us) THEN ipd.isrc2
            WHEN (at1.release_date < at2.release_date) THEN ipd.isrc1
            WHEN (at1.release_date > at2.release_date) THEN ipd.isrc2
            WHEN (at1.total_markets > at2.total_markets) THEN ipd.isrc1
            WHEN (at1.total_markets < at2.total_markets) THEN ipd.isrc2
            WHEN (at1.popularity > at2.popularity) THEN ipd.isrc1
            WHEN (at1.popularity < at2.popularity) THEN ipd.isrc2
            WHEN (at1.duration_ms < at2.duration_ms) THEN ipd.isrc1
            WHEN (at1.duration_ms > at2.duration_ms) THEN ipd.isrc2
            ELSE ipd.isrc1
        END AS preferred_isrc,
        CASE
            WHEN (at1.available_in_us <> at2.available_in_us) THEN 'avail'::text
            WHEN (at1.release_date <> at2.release_date) THEN 'release'::text
            WHEN (at1.total_markets <> at2.total_markets) THEN 'markets'::text
            WHEN (at1.popularity <> at2.popularity) THEN 'popularity'::text
            WHEN (at1.duration_ms <> at2.duration_ms) THEN 'duration'::text
            ELSE 'No Preference'::text
        END AS preferred_logic
   FROM ((((((music.isrc_possible_dupes ipd
     JOIN music.vw_best_track_id bt1 ON ((bt1.isrc = ipd.isrc1)))
     JOIN music.all_tracks at1 ON ((at1.track_id = bt1.best_track_id)))
     JOIN music.vw_best_track_id bt2 ON ((bt2.isrc = ipd.isrc2)))
     JOIN music.all_tracks at2 ON ((at2.track_id = bt2.best_track_id)))
     JOIN music.all_albums aa1 ON ((aa1.album_id = at1.album_id)))
     JOIN music.all_albums aa2 ON ((aa2.album_id = at2.album_id)))
  ORDER BY ipd.match_score DESC, ipd.track_score DESC, ipd.album_score DESC, ipd.artist_score DESC, at1.track_name;


--
-- Name: COLUMN vw_isrc_dupe_review.isrc1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.isrc1 IS 'Column isrc1.';


--
-- Name: COLUMN vw_isrc_dupe_review.pref1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.pref1 IS 'Column pref1.';


--
-- Name: COLUMN vw_isrc_dupe_review.track_name1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.track_name1 IS 'Column track_name1.';


--
-- Name: COLUMN vw_isrc_dupe_review.artist_name1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.artist_name1 IS 'Column artist_name1.';


--
-- Name: COLUMN vw_isrc_dupe_review.album_name1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.album_name1 IS 'Column album_name1.';


--
-- Name: COLUMN vw_isrc_dupe_review.duration_1; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.duration_1 IS 'Column duration_1.';


--
-- Name: COLUMN vw_isrc_dupe_review.isrc2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.isrc2 IS 'Column isrc2.';


--
-- Name: COLUMN vw_isrc_dupe_review.pref2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.pref2 IS 'Column pref2.';


--
-- Name: COLUMN vw_isrc_dupe_review.track_name2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.track_name2 IS 'Column track_name2.';


--
-- Name: COLUMN vw_isrc_dupe_review.artist_name2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.artist_name2 IS 'Column artist_name2.';


--
-- Name: COLUMN vw_isrc_dupe_review.album_name2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.album_name2 IS 'Column album_name2.';


--
-- Name: COLUMN vw_isrc_dupe_review.duration_2; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.duration_2 IS 'Column duration_2.';


--
-- Name: COLUMN vw_isrc_dupe_review.match_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.match_score IS 'Column match_score.';


--
-- Name: COLUMN vw_isrc_dupe_review.track_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.track_score IS 'Column track_score.';


--
-- Name: COLUMN vw_isrc_dupe_review.artist_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.artist_score IS 'Column artist_score.';


--
-- Name: COLUMN vw_isrc_dupe_review.album_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.album_score IS 'Column album_score.';


--
-- Name: COLUMN vw_isrc_dupe_review.duration_score; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.duration_score IS 'Column duration_score.';


--
-- Name: COLUMN vw_isrc_dupe_review.preferred_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.preferred_isrc IS 'Column preferred_isrc.';


--
-- Name: COLUMN vw_isrc_dupe_review.preferred_logic; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_isrc_dupe_review.preferred_logic IS 'Column preferred_logic.';


--
-- Name: vw_listening_history; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_listening_history AS
 SELECT lh.played_at_utc,
    COALESCE(s.mapped_isrc, lh.isrc) AS isrc,
    COALESCE(pr.parent_playlist_id, lh.playlist_id) AS playlist_id
   FROM ((music.listening_history lh
     LEFT JOIN music.isrc_swaps s ON ((s.valid_mapping AND (s.original_isrc = lh.isrc))))
     LEFT JOIN music.playlist_relationships pr ON ((pr.child_playlist_id = lh.playlist_id)));


--
-- Name: COLUMN vw_listening_history.played_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_listening_history.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN vw_listening_history.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_listening_history.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_listening_history.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_listening_history.playlist_id IS 'Column playlist_id.';


--
-- Name: vw_playlist_last_heard; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_last_heard AS
 SELECT max(lh.played_at_utc) AS last_played_utc,
    COALESCE(pr.parent_playlist_id, lh.playlist_id) AS playlist_id
   FROM (music.listening_history lh
     LEFT JOIN music.playlist_relationships pr ON ((pr.child_playlist_id = lh.playlist_id)))
  WHERE (COALESCE(pr.parent_playlist_id, lh.playlist_id) IS NOT NULL)
  GROUP BY COALESCE(pr.parent_playlist_id, lh.playlist_id);


--
-- Name: COLUMN vw_playlist_last_heard.last_played_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_last_heard.last_played_utc IS 'Column last_played_utc.';


--
-- Name: COLUMN vw_playlist_last_heard.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_last_heard.playlist_id IS 'Column playlist_id.';


--
-- Name: vw_playlist_config; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_config AS
 SELECT c.playlist_id,
    c.playlist_name,
    c.playlist_description,
    c.track_count,
    c.auto_shuffle,
    c.manual_shuffle,
    c.seeds_only,
    c.make_recs,
    c.ratings_weight,
    c.recency_weight,
    c.randomness_weight,
    c.minutes_to_sync,
        CASE
            WHEN (p.parent_playlist_id IS NOT NULL) THEN 'Parents'::text
            WHEN c.seeds_only THEN 'Seeds'::text
            ELSE 'Other'::text
        END AS playlist_type,
    lh.last_played_utc
   FROM (((music.playlist_config c
     LEFT JOIN music.playlist_relationships r ON ((r.child_playlist_id = c.playlist_id)))
     LEFT JOIN music.vw_playlist_last_heard lh ON ((lh.playlist_id = c.playlist_id)))
     LEFT JOIN ( SELECT DISTINCT playlist_relationships.parent_playlist_id
           FROM music.playlist_relationships) p ON ((p.parent_playlist_id = c.playlist_id)))
  WHERE (r.child_playlist_id IS NULL)
  ORDER BY (lh.last_played_utc IS NULL), lh.last_played_utc DESC, c.is_active DESC, c.seeds_only, c.auto_shuffle DESC, c.make_recs DESC, c.track_count DESC;


--
-- Name: COLUMN vw_playlist_config.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_config.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN vw_playlist_config.playlist_description; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.playlist_description IS 'Column playlist_description.';


--
-- Name: COLUMN vw_playlist_config.track_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.track_count IS 'Column track_count.';


--
-- Name: COLUMN vw_playlist_config.auto_shuffle; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.auto_shuffle IS 'Column auto_shuffle.';


--
-- Name: COLUMN vw_playlist_config.manual_shuffle; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.manual_shuffle IS 'Column manual_shuffle.';


--
-- Name: COLUMN vw_playlist_config.seeds_only; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.seeds_only IS 'Column seeds_only.';


--
-- Name: COLUMN vw_playlist_config.make_recs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.make_recs IS 'Column make_recs.';


--
-- Name: COLUMN vw_playlist_config.ratings_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.ratings_weight IS 'Column ratings_weight.';


--
-- Name: COLUMN vw_playlist_config.recency_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.recency_weight IS 'Column recency_weight.';


--
-- Name: COLUMN vw_playlist_config.randomness_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.randomness_weight IS 'Column randomness_weight.';


--
-- Name: COLUMN vw_playlist_config.minutes_to_sync; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.minutes_to_sync IS 'Column minutes_to_sync.';


--
-- Name: COLUMN vw_playlist_config.playlist_type; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.playlist_type IS 'Column playlist_type.';


--
-- Name: COLUMN vw_playlist_config.last_played_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_config.last_played_utc IS 'Column last_played_utc.';


--
-- Name: vw_playlist_with_mismatched_isrc; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_with_mismatched_isrc AS
 SELECT DISTINCT pd.playlist_id
   FROM (music.playlist_isrcs pd
     LEFT JOIN music.isrc_swaps s ON (((s.original_isrc = pd.isrc) AND s.valid_mapping)))
  WHERE (pd.isrc <> COALESCE(s.mapped_isrc, pd.isrc));


--
-- Name: COLUMN vw_playlist_with_mismatched_isrc.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_with_mismatched_isrc.playlist_id IS 'Column playlist_id.';


--
-- Name: vw_playlist_detail_sync_logic; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_detail_sync_logic AS
 WITH cp AS (
         SELECT pr.child_playlist_id AS playlist_id,
            pr.child_playlist_type AS list_type,
            pc.playlist_name,
            pc.track_count,
            COALESCE(pc.last_synced_utc, '1970-01-01 08:00:00+00'::timestamp with time zone) AS last_synced
           FROM (music.playlist_relationships pr
             JOIN music.playlist_config pc ON ((pc.playlist_id = pr.child_playlist_id)))
          WHERE (pr.child_playlist_type = 'manual'::text)
        ), pp AS (
         SELECT DISTINCT COALESCE(pr.parent_playlist_id, pc.playlist_id) AS playlist_id,
                CASE
                    WHEN (pc.track_count = 0) THEN 'never'::text
                    WHEN ((pc.track_count <> pc.prior_track_count) AND (pc.track_count > 0)) THEN 'updated'::text
                    WHEN ((pc.seeds_only OR pc.auto_shuffle) AND (pc.track_count > 0)) THEN 'seeds'::text
                    WHEN ((NOT pc.seeds_only) AND (NOT pc.auto_shuffle) AND (NOT pc.make_recs) AND pc.is_active) THEN 'rare seeds'::text
                    WHEN (pc.manual_shuffle OR pc.make_recs) THEN 'masters'::text
                    ELSE 'No Type'::text
                END AS list_type,
            pc.playlist_name,
            pc.track_count,
            COALESCE(pc.last_synced_utc, '1970-01-01 08:00:00+00'::timestamp with time zone) AS last_synced
           FROM (music.playlist_config pc
             LEFT JOIN music.playlist_relationships pr ON ((pr.parent_playlist_id = pc.playlist_id)))
          WHERE ((COALESCE(pr.child_playlist_type, ''::text) <> 'manual'::text) AND pc.is_active)
        ), combo AS (
         SELECT cp.playlist_id,
            cp.list_type,
            cp.playlist_name,
            cp.track_count,
            cp.last_synced
           FROM cp
        UNION
         SELECT pp.playlist_id,
            pp.list_type,
            pp.playlist_name,
            pp.track_count,
            pp.last_synced
           FROM pp
        ), ns AS (
         SELECT combo.playlist_id,
            combo.playlist_name,
            combo.list_type,
                CASE
                    WHEN (mm.* IS NOT NULL) THEN (CURRENT_TIMESTAMP - '1 day'::interval)
                    WHEN (combo.list_type = 'updated'::text) THEN CURRENT_TIMESTAMP
                    WHEN (combo.list_type = 'masters'::text) THEN (combo.last_synced + '3 days'::interval)
                    WHEN (combo.list_type = 'seeds'::text) THEN (combo.last_synced + '7 days'::interval)
                    WHEN (combo.list_type = 'rare seeds'::text) THEN (combo.last_synced + '30 days'::interval)
                    ELSE NULL::timestamp with time zone
                END AS next_valid_sync
           FROM (combo
             LEFT JOIN music.vw_playlist_with_mismatched_isrc mm ON ((combo.playlist_id = mm.playlist_id)))
          WHERE (combo.list_type = ANY (ARRAY['manual'::text, 'seeds'::text, 'rare seeds'::text, 'updated'::text, 'masters'::text]))
        ), vp AS (
         SELECT ns.playlist_id,
            ns.playlist_name,
            ns.list_type,
            ns.next_valid_sync,
            lh.last_played_utc,
                CASE
                    WHEN (ns.list_type = 'manual'::text) THEN (0)::bigint
                    ELSE row_number() OVER (PARTITION BY (ns.list_type = 'manual'::text) ORDER BY ns.next_valid_sync, COALESCE(lh.last_played_utc, '1970-01-01 08:00:00+00'::timestamp with time zone) DESC)
                END AS rval
           FROM (ns
             LEFT JOIN music.vw_playlist_last_heard lh ON ((lh.playlist_id = ns.playlist_id)))
        )
 SELECT playlist_id,
    playlist_name,
    next_valid_sync,
    last_played_utc,
    rval
   FROM vp
  WHERE ((rval < 6) AND (rval > 0));


--
-- Name: COLUMN vw_playlist_detail_sync_logic.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_detail_sync_logic.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_detail_sync_logic.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_detail_sync_logic.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN vw_playlist_detail_sync_logic.next_valid_sync; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_detail_sync_logic.next_valid_sync IS 'Column next_valid_sync.';


--
-- Name: COLUMN vw_playlist_detail_sync_logic.last_played_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_detail_sync_logic.last_played_utc IS 'Column last_played_utc.';


--
-- Name: COLUMN vw_playlist_detail_sync_logic.rval; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_detail_sync_logic.rval IS 'Column rval.';


--
-- Name: vw_playlist_families; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_families AS
 SELECT DISTINCT playlist_config.playlist_id,
    playlist_config.playlist_id AS child_playlist_id,
    'self'::text AS playlist_type
   FROM music.playlist_config
UNION
 SELECT p1.child_playlist_id AS playlist_id,
        CASE
            WHEN (p2.child_playlist_id = p1.child_playlist_id) THEN p1.parent_playlist_id
            ELSE p2.child_playlist_id
        END AS child_playlist_id,
        CASE
            WHEN (p2.child_playlist_id = p1.child_playlist_id) THEN 'parent'::text
            ELSE 'sibling'::text
        END AS playlist_type
   FROM (music.playlist_relationships p1
     JOIN music.playlist_relationships p2 ON ((p1.parent_playlist_id = p2.parent_playlist_id)));


--
-- Name: COLUMN vw_playlist_families.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_families.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_families.child_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_families.child_playlist_id IS 'Column child_playlist_id.';


--
-- Name: COLUMN vw_playlist_families.playlist_type; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_families.playlist_type IS 'Column playlist_type.';


--
-- Name: vw_playlist_isrcs; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_isrcs AS
 SELECT pi.playlist_id,
    COALESCE(s.mapped_isrc, pi.isrc) AS isrc,
    pi.track_order,
    pi.updated_at_utc
   FROM (music.playlist_isrcs pi
     LEFT JOIN music.isrc_swaps s ON ((s.original_isrc = pi.isrc)));


--
-- Name: COLUMN vw_playlist_isrcs.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrcs.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_isrcs.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrcs.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_playlist_isrcs.track_order; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrcs.track_order IS 'Column track_order.';


--
-- Name: COLUMN vw_playlist_isrcs.updated_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrcs.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: vw_playlist_shuffle_targets; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_shuffle_targets AS
 SELECT parent_playlist_id,
    child_playlist_id AS target_playlist_id,
    child_playlist_type AS target_playlist_type
   FROM music.playlist_relationships pr
  WHERE (child_playlist_type = ANY (ARRAY['auto'::text, 'manual'::text]));


--
-- Name: COLUMN vw_playlist_shuffle_targets.parent_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_targets.parent_playlist_id IS 'Column parent_playlist_id.';


--
-- Name: COLUMN vw_playlist_shuffle_targets.target_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_targets.target_playlist_id IS 'Column target_playlist_id.';


--
-- Name: COLUMN vw_playlist_shuffle_targets.target_playlist_type; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_targets.target_playlist_type IS 'Column target_playlist_type.';


--
-- Name: vw_ratings; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_ratings AS
 SELECT DISTINCT r.playlist_id,
    COALESCE(s.mapped_isrc, r.isrc) AS isrc,
    max(r.elo_rating) AS elo_rating,
    max(r.rating_count) AS rating_count,
    max(r.rating_wins) AS rating_wins,
    max(r.rating_losses) AS rating_losses,
    max(r.last_rated_utc) AS last_rated_utc
   FROM (music.ratings r
     LEFT JOIN music.isrc_swaps s ON (((s.original_isrc = r.isrc) AND s.valid_mapping)))
  GROUP BY r.playlist_id, COALESCE(s.mapped_isrc, r.isrc);


--
-- Name: COLUMN vw_ratings.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_ratings.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_ratings.elo_rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.elo_rating IS 'Column elo_rating.';


--
-- Name: COLUMN vw_ratings.rating_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.rating_count IS 'Column rating_count.';


--
-- Name: COLUMN vw_ratings.rating_wins; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.rating_wins IS 'Column rating_wins.';


--
-- Name: COLUMN vw_ratings.rating_losses; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.rating_losses IS 'Column rating_losses.';


--
-- Name: COLUMN vw_ratings.last_rated_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings.last_rated_utc IS 'Column last_rated_utc.';


--
-- Name: vw_playlist_isrc_stats; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_isrc_stats AS
 WITH catalog AS (
         SELECT st.parent_playlist_id,
            st.target_playlist_id,
            pi.isrc
           FROM (music.vw_playlist_shuffle_targets st
             JOIN music.vw_playlist_isrcs pi ON ((pi.playlist_id = st.parent_playlist_id)))
        ), isrc_last_played_anywhere AS (
         SELECT lh.isrc,
            max(lh.played_at_utc) AS last_played_at
           FROM music.vw_listening_history lh
          GROUP BY lh.isrc
        ), isrc_last_played_on_playlist AS (
         SELECT lh.playlist_id AS parent_playlist_id,
            lh.isrc,
            max(lh.played_at_utc) AS last_played_at,
            count(*) AS play_count
           FROM (music.vw_listening_history lh
             JOIN music.vw_playlist_shuffle_targets st ON ((st.parent_playlist_id = lh.playlist_id)))
          GROUP BY lh.playlist_id, lh.isrc
        ), playlist_last_played_any_track AS (
         SELECT lh.playlist_id AS parent_playlist_id,
            max(lh.played_at_utc) AS last_played_at
           FROM (music.vw_listening_history lh
             JOIN music.vw_playlist_shuffle_targets st ON ((st.parent_playlist_id = lh.playlist_id)))
          GROUP BY lh.playlist_id
        ), elo AS (
         SELECT st.parent_playlist_id,
            r_1.isrc,
            max(r_1.elo_rating) AS elo_rating
           FROM (music.vw_playlist_shuffle_targets st
             JOIN music.vw_ratings r_1 ON ((r_1.playlist_id = st.parent_playlist_id)))
          GROUP BY st.parent_playlist_id, r_1.isrc
        ), config AS (
         SELECT pc.playlist_id AS parent_playlist_id,
            COALESCE(pc.recency_weight, (1)::numeric) AS recency_weight,
            COALESCE(pc.ratings_weight, (1)::numeric) AS ratings_weight,
            COALESCE(pc.randomness_weight, (1)::numeric) AS randomness_weight,
            COALESCE(pc.minutes_to_sync, 9999) AS minutes_to_sync
           FROM (music.playlist_config pc
             JOIN music.vw_playlist_shuffle_targets st ON ((st.parent_playlist_id = pc.playlist_id)))
        ), recency_inputs AS (
         SELECT c.parent_playlist_id,
            c.target_playlist_id,
            c.isrc,
            COALESCE(pp.play_count, (0)::bigint) AS play_count_on_playlist,
            (((CURRENT_DATE - (COALESCE(gp.last_played_at, (CURRENT_TIMESTAMP - '730 days'::interval)))::date))::double precision ^ (2)::double precision) AS days_since_played_anywhere_sq,
            (((CURRENT_DATE - (COALESCE(pp.last_played_at, (CURRENT_TIMESTAMP - '730 days'::interval)))::date))::double precision ^ (2)::double precision) AS days_since_played_on_playlist_sq
           FROM ((catalog c
             LEFT JOIN isrc_last_played_anywhere gp ON ((gp.isrc = c.isrc)))
             LEFT JOIN isrc_last_played_on_playlist pp ON (((pp.parent_playlist_id = c.parent_playlist_id) AND (pp.isrc = c.isrc))))
        ), pressures AS (
         SELECT ri.parent_playlist_id,
            ri.target_playlist_id,
            ri.isrc,
            ri.play_count_on_playlist,
            COALESCE(e.elo_rating, 1500) AS elo_rating,
            percent_rank() OVER (PARTITION BY ri.parent_playlist_id ORDER BY ((ri.days_since_played_on_playlist_sq * (30)::double precision) + ri.days_since_played_anywhere_sq)) AS recency_pct,
            percent_rank() OVER (PARTITION BY ri.parent_playlist_id ORDER BY COALESCE(e.elo_rating, 1500)) AS ratings_pct,
            percent_rank() OVER (PARTITION BY ri.parent_playlist_id ORDER BY (random())) AS random_pct
           FROM (recency_inputs ri
             LEFT JOIN elo e ON (((e.parent_playlist_id = ri.parent_playlist_id) AND (e.isrc = ri.isrc))))
        ), ranked AS (
         SELECT p.parent_playlist_id,
            p.target_playlist_id,
            p.isrc,
            p.play_count_on_playlist,
            p.elo_rating,
            p.recency_pct,
            p.ratings_pct,
            p.random_pct,
            cfg.recency_weight,
            cfg.ratings_weight,
            cfg.randomness_weight,
            cfg.minutes_to_sync,
            (p.recency_pct * (cfg.recency_weight)::double precision) AS recency_pressure,
            (p.ratings_pct * (cfg.ratings_weight)::double precision) AS ratings_pressure,
            ((p.random_pct * (cfg.randomness_weight)::double precision) / (10.0)::double precision) AS randomness_pressure,
            (((p.recency_pct * (cfg.recency_weight)::double precision) + (p.ratings_pct * (cfg.ratings_weight)::double precision)) + ((p.random_pct * (cfg.randomness_weight)::double precision) / (10.0)::double precision)) AS total_weight,
            row_number() OVER (PARTITION BY p.parent_playlist_id ORDER BY (((p.recency_pct * (cfg.recency_weight)::double precision) + (p.ratings_pct * (cfg.ratings_weight)::double precision)) + ((p.random_pct * (cfg.randomness_weight)::double precision) / (10.0)::double precision)) DESC) AS default_new_order
           FROM (pressures p
             JOIN config cfg ON ((cfg.parent_playlist_id = p.parent_playlist_id)))
        )
 SELECT r.parent_playlist_id AS playlist_id,
    r.target_playlist_id,
    r.isrc,
    bt.best_track_id AS track_id,
    ((tr.track_name_clean || ' by '::text) || tr.artist_display_name) AS track_artist,
    al.album_name_clean AS album_name,
    (tr.duration_ms / 1000) AS duration_s,
    r.recency_pct,
    r.ratings_pct,
    r.random_pct,
    r.recency_weight,
    r.ratings_weight,
    r.randomness_weight,
    r.elo_rating,
    r.play_count_on_playlist,
    r.recency_pressure,
    r.ratings_pressure,
    r.randomness_pressure,
    r.total_weight,
    r.minutes_to_sync,
    r.default_new_order,
    sum(((tr.duration_ms)::numeric / 60000.0)) OVER (PARTITION BY r.parent_playlist_id ORDER BY r.default_new_order ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_playlist_minutes
   FROM (((ranked r
     JOIN music.vw_best_track_id bt ON ((bt.isrc = r.isrc)))
     JOIN music.all_tracks tr ON ((tr.track_id = bt.best_track_id)))
     JOIN music.all_albums al ON ((al.album_id = tr.album_id)));


--
-- Name: COLUMN vw_playlist_isrc_stats.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_isrc_stats.target_playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.target_playlist_id IS 'Column target_playlist_id.';


--
-- Name: COLUMN vw_playlist_isrc_stats.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_playlist_isrc_stats.track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_playlist_isrc_stats.track_artist; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.track_artist IS 'Column track_artist.';


--
-- Name: COLUMN vw_playlist_isrc_stats.album_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.album_name IS 'Column album_name.';


--
-- Name: COLUMN vw_playlist_isrc_stats.duration_s; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.duration_s IS 'Column duration_s.';


--
-- Name: COLUMN vw_playlist_isrc_stats.recency_pct; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.recency_pct IS 'Column recency_pct.';


--
-- Name: COLUMN vw_playlist_isrc_stats.ratings_pct; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.ratings_pct IS 'Column ratings_pct.';


--
-- Name: COLUMN vw_playlist_isrc_stats.random_pct; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.random_pct IS 'Column random_pct.';


--
-- Name: COLUMN vw_playlist_isrc_stats.recency_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.recency_weight IS 'Column recency_weight.';


--
-- Name: COLUMN vw_playlist_isrc_stats.ratings_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.ratings_weight IS 'Column ratings_weight.';


--
-- Name: COLUMN vw_playlist_isrc_stats.randomness_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.randomness_weight IS 'Column randomness_weight.';


--
-- Name: COLUMN vw_playlist_isrc_stats.elo_rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.elo_rating IS 'Column elo_rating.';


--
-- Name: COLUMN vw_playlist_isrc_stats.play_count_on_playlist; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.play_count_on_playlist IS 'Column play_count_on_playlist.';


--
-- Name: COLUMN vw_playlist_isrc_stats.recency_pressure; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.recency_pressure IS 'Column recency_pressure.';


--
-- Name: COLUMN vw_playlist_isrc_stats.ratings_pressure; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.ratings_pressure IS 'Column ratings_pressure.';


--
-- Name: COLUMN vw_playlist_isrc_stats.randomness_pressure; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.randomness_pressure IS 'Column randomness_pressure.';


--
-- Name: COLUMN vw_playlist_isrc_stats.total_weight; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.total_weight IS 'Column total_weight.';


--
-- Name: COLUMN vw_playlist_isrc_stats.minutes_to_sync; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.minutes_to_sync IS 'Column minutes_to_sync.';


--
-- Name: COLUMN vw_playlist_isrc_stats.default_new_order; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.default_new_order IS 'Column default_new_order.';


--
-- Name: COLUMN vw_playlist_isrc_stats.cumulative_playlist_minutes; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_isrc_stats.cumulative_playlist_minutes IS 'Column cumulative_playlist_minutes.';


--
-- Name: vw_playlist_shuffle_eligibility; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_playlist_shuffle_eligibility AS
 WITH e AS (
         SELECT DISTINCT playlist_config.playlist_id,
            playlist_config.playlist_name,
            COALESCE(playlist_config.last_auto_shuffled_utc, '2025-12-01 08:00:00+00'::timestamp with time zone) AS last_auto_shuffled_utc
           FROM (music.playlist_config
             LEFT JOIN music.vw_playlist_with_mismatched_isrc mm ON ((mm.playlist_id = playlist_config.playlist_id)))
          WHERE (playlist_config.is_active AND (playlist_config.auto_shuffle OR (mm.playlist_id IS NOT NULL)))
        ), lp AS (
         SELECT max(lh.played_at_utc) AS last_played_utc,
            COALESCE(pr.parent_playlist_id, lh.playlist_id) AS playlist_id
           FROM (music.listening_history lh
             LEFT JOIN music.playlist_relationships pr ON ((pr.child_playlist_id = lh.playlist_id)))
          WHERE (lh.playlist_id IS NOT NULL)
          GROUP BY COALESCE(pr.parent_playlist_id, lh.playlist_id)
        )
 SELECT e.playlist_id,
    e.playlist_name,
    lp.last_played_utc,
    e.last_auto_shuffled_utc
   FROM (e
     LEFT JOIN lp ON ((lp.playlist_id = e.playlist_id)))
  WHERE (COALESCE(lp.last_played_utc, '2025-12-01 08:00:00+00'::timestamp with time zone) > e.last_auto_shuffled_utc);


--
-- Name: COLUMN vw_playlist_shuffle_eligibility.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_eligibility.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_shuffle_eligibility.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_eligibility.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN vw_playlist_shuffle_eligibility.last_played_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_eligibility.last_played_utc IS 'Column last_played_utc.';


--
-- Name: COLUMN vw_playlist_shuffle_eligibility.last_auto_shuffled_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_playlist_shuffle_eligibility.last_auto_shuffled_utc IS 'Column last_auto_shuffled_utc.';


--
-- Name: vw_rating_eligible; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_rating_eligible AS
 WITH lp AS (
         SELECT COALESCE(s.mapped_isrc, lh.isrc) AS isrc,
            COALESCE(pr.parent_playlist_id, lh.playlist_id) AS playlist_id,
            max(lh.played_at_utc) AS last_played_utc
           FROM ((((music.listening_history lh
             LEFT JOIN music.isrc_swaps s ON (((s.original_isrc = lh.isrc) AND s.valid_mapping)))
             LEFT JOIN music.playlist_relationships pr ON ((pr.child_playlist_id = lh.playlist_id)))
             JOIN music.vw_playlist_isrcs pi ON (((pi.isrc = COALESCE(s.mapped_isrc, lh.isrc)) AND (pi.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id)))))
             JOIN music.playlist_config pc ON ((pc.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id))))
          WHERE ((lh.playlist_id IS NOT NULL) AND (pc.auto_shuffle OR pc.make_recs OR pc.manual_shuffle))
          GROUP BY COALESCE(s.mapped_isrc, lh.isrc), COALESCE(pr.parent_playlist_id, lh.playlist_id)
        )
 SELECT DISTINCT lp.isrc,
    lp.playlist_id,
    pc1.playlist_name,
    lp.last_played_utc,
    ir.last_rated_utc,
    (COALESCE(ir.last_rated_utc, ('1980-07-18 00:00:00'::timestamp without time zone)::timestamp with time zone) < lp.last_played_utc) AS needs_rating,
    (lp.last_played_utc > (CURRENT_TIMESTAMP - '24:00:00'::interval)) AS played_in_last_24,
    (ir.last_rated_utc IS NULL) AS never_rated
   FROM ((lp
     JOIN music.playlist_config pc1 ON ((pc1.playlist_id = lp.playlist_id)))
     LEFT JOIN music.vw_ratings ir ON (((ir.isrc = lp.isrc) AND (ir.playlist_id = lp.playlist_id))))
  WHERE ((COALESCE(ir.last_rated_utc, ('1980-07-18 00:00:00'::timestamp without time zone)::timestamp with time zone) < lp.last_played_utc) AND ((lp.last_played_utc > (CURRENT_TIMESTAMP - '24:00:00'::interval)) OR (COALESCE(ir.rating_count, 0) = 0)))
  ORDER BY lp.last_played_utc DESC, ir.last_rated_utc DESC;


--
-- Name: COLUMN vw_rating_eligible.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_rating_eligible.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_rating_eligible.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.playlist_name IS 'Column playlist_name.';


--
-- Name: COLUMN vw_rating_eligible.last_played_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.last_played_utc IS 'Column last_played_utc.';


--
-- Name: COLUMN vw_rating_eligible.last_rated_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.last_rated_utc IS 'Column last_rated_utc.';


--
-- Name: COLUMN vw_rating_eligible.needs_rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.needs_rating IS 'Column needs_rating.';


--
-- Name: COLUMN vw_rating_eligible.played_in_last_24; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.played_in_last_24 IS 'Column played_in_last_24.';


--
-- Name: COLUMN vw_rating_eligible.never_rated; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible.never_rated IS 'Column never_rated.';


--
-- Name: vw_rating_eligible_playlists; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_rating_eligible_playlists AS
 SELECT playlist_id,
    (((playlist_name || ' ('::text) || (count(DISTINCT isrc))::text) || ')'::text) AS playlist_name
   FROM music.vw_rating_eligible
  GROUP BY playlist_id, playlist_name;


--
-- Name: COLUMN vw_rating_eligible_playlists.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible_playlists.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_rating_eligible_playlists.playlist_name; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_eligible_playlists.playlist_name IS 'Column playlist_name.';


--
-- Name: vw_ratings_history; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_ratings_history AS
 SELECT rating_time_utc,
    playlist_id,
    isrc,
    isrc_vs,
    elo_old,
    elo_new,
    rating_result,
    row_num
   FROM ( SELECT h.rating_time_utc,
            h.playlist_id,
            COALESCE(s1.mapped_isrc, h.isrc) AS isrc,
            COALESCE(s2.mapped_isrc, h.isrc_vs) AS isrc_vs,
            h.elo_old,
            h.elo_new,
            h.rating_result,
            row_number() OVER (PARTITION BY h.playlist_id, COALESCE(s1.mapped_isrc, h.isrc), COALESCE(s2.mapped_isrc, h.isrc_vs) ORDER BY h.rating_time_utc DESC) AS row_num
           FROM ((music.ratings_history h
             LEFT JOIN music.isrc_swaps s1 ON (((s1.original_isrc = h.isrc) AND s1.valid_mapping)))
             LEFT JOIN music.isrc_swaps s2 ON (((s2.original_isrc = h.isrc_vs) AND s2.valid_mapping)))) subquery
  WHERE (row_num = 1);


--
-- Name: COLUMN vw_ratings_history.rating_time_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.rating_time_utc IS 'Column rating_time_utc.';


--
-- Name: COLUMN vw_ratings_history.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_ratings_history.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_ratings_history.isrc_vs; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.isrc_vs IS 'Column isrc_vs.';


--
-- Name: COLUMN vw_ratings_history.elo_old; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.elo_old IS 'Column elo_old.';


--
-- Name: COLUMN vw_ratings_history.elo_new; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.elo_new IS 'Column elo_new.';


--
-- Name: COLUMN vw_ratings_history.rating_result; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.rating_result IS 'Column rating_result.';


--
-- Name: COLUMN vw_ratings_history.row_num; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_history.row_num IS 'Column row_num.';


--
-- Name: vw_rating_update; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_rating_update AS
 SELECT playlist_id,
    isrc,
    elo_rating,
    rating_count,
    rating_wins,
    rating_losses,
    last_rated_utc
   FROM ( SELECT ratings_history.playlist_id,
            ratings_history.isrc,
            ratings_history.elo_new AS elo_rating,
            count(ratings_history.rating_time_utc) OVER (PARTITION BY ratings_history.playlist_id, ratings_history.isrc) AS rating_count,
            count(
                CASE
                    WHEN (ratings_history.rating_result > 0) THEN ratings_history.rating_time_utc
                    ELSE NULL::timestamp with time zone
                END) OVER (PARTITION BY ratings_history.playlist_id, ratings_history.isrc) AS rating_wins,
            count(
                CASE
                    WHEN (ratings_history.rating_result < 0) THEN ratings_history.rating_time_utc
                    ELSE NULL::timestamp with time zone
                END) OVER (PARTITION BY ratings_history.playlist_id, ratings_history.isrc) AS rating_losses,
            ratings_history.rating_time_utc AS last_rated_utc,
            row_number() OVER (PARTITION BY ratings_history.playlist_id, ratings_history.isrc ORDER BY ratings_history.rating_time_utc DESC) AS row_num
           FROM music.vw_ratings_history ratings_history) sub
  WHERE (row_num = 1);


--
-- Name: COLUMN vw_rating_update.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_rating_update.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_rating_update.elo_rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.elo_rating IS 'Column elo_rating.';


--
-- Name: COLUMN vw_rating_update.rating_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.rating_count IS 'Column rating_count.';


--
-- Name: COLUMN vw_rating_update.rating_wins; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.rating_wins IS 'Column rating_wins.';


--
-- Name: COLUMN vw_rating_update.rating_losses; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.rating_losses IS 'Column rating_losses.';


--
-- Name: COLUMN vw_rating_update.last_rated_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_rating_update.last_rated_utc IS 'Column last_rated_utc.';


--
-- Name: vw_ratings_update; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_ratings_update AS
 WITH lr AS (
         SELECT ratings_history.playlist_id,
            ratings_history.isrc,
            max(ratings_history.rating_time_utc) AS last_rated
           FROM music.vw_ratings_history ratings_history
          GROUP BY ratings_history.playlist_id, ratings_history.isrc
        )
 SELECT trh.playlist_id,
    trh.isrc,
    max(
        CASE
            WHEN (trh.rating_time_utc = lr.last_rated) THEN trh.elo_new
            ELSE NULL::integer
        END) AS elo_current,
    count(DISTINCT trh.rating_time_utc) AS rating_count,
    count(DISTINCT
        CASE
            WHEN (trh.rating_result > 0) THEN trh.rating_result
            ELSE NULL::smallint
        END) AS wins,
    count(DISTINCT
        CASE
            WHEN (trh.rating_result < 0) THEN trh.rating_result
            ELSE NULL::smallint
        END) AS losses,
    lr.last_rated AS last_rated_utc
   FROM (music.vw_ratings_history trh
     LEFT JOIN lr ON (((lr.playlist_id = trh.playlist_id) AND (lr.isrc = trh.isrc))))
  GROUP BY trh.playlist_id, trh.isrc, lr.last_rated;


--
-- Name: COLUMN vw_ratings_update.playlist_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_ratings_update.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_ratings_update.elo_current; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.elo_current IS 'Column elo_current.';


--
-- Name: COLUMN vw_ratings_update.rating_count; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.rating_count IS 'Column rating_count.';


--
-- Name: COLUMN vw_ratings_update.wins; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.wins IS 'Column wins.';


--
-- Name: COLUMN vw_ratings_update.losses; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.losses IS 'Column losses.';


--
-- Name: COLUMN vw_ratings_update.last_rated_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_ratings_update.last_rated_utc IS 'Column last_rated_utc.';


--
-- Name: vw_recent_plays; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_recent_plays AS
 WITH play_stats AS (
         SELECT listening_history.isrc,
            max(listening_history.played_at_utc) AS last_played_at_utc,
            count(DISTINCT
                CASE
                    WHEN (listening_history.played_at_utc > (CURRENT_TIMESTAMP - '30 days'::interval)) THEN listening_history.played_at_utc
                    ELSE NULL::timestamp with time zone
                END) AS playcount_last_30,
            count(DISTINCT listening_history.played_at_utc) AS playcount_total
           FROM music.vw_listening_history listening_history
          GROUP BY listening_history.isrc
        )
 SELECT ps.last_played_at_utc,
    ps.playcount_last_30,
    ps.playcount_total,
    (((allt.track_name_clean || ' by '::text) || allt.artist_display_name) || COALESCE((' from '::text || pc.playlist_name), ''::text)) AS track_artist,
    COALESCE(r.elo_rating, 1500) AS rating,
    max(ps.playcount_last_30) OVER () AS max_playcount_last_30,
    max(ps.playcount_total) OVER () AS max_playcount_total,
    max(COALESCE(r.elo_rating, 1500)) OVER () AS max_elo,
    min(COALESCE(r.elo_rating, 1500)) OVER () AS min_elo
   FROM ((((((play_stats ps
     JOIN music.vw_listening_history lh ON (((lh.isrc = ps.isrc) AND (lh.played_at_utc = ps.last_played_at_utc))))
     JOIN music.vw_best_track_id bt ON ((ps.isrc = bt.isrc)))
     JOIN music.all_tracks allt ON ((allt.track_id = bt.best_track_id)))
     LEFT JOIN music.playlist_relationships pr ON ((lh.playlist_id = pr.child_playlist_id)))
     LEFT JOIN music.vw_ratings r ON (((r.isrc = lh.isrc) AND (r.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id)))))
     LEFT JOIN music.playlist_config pc ON ((pc.playlist_id = COALESCE(pr.parent_playlist_id, lh.playlist_id))))
  ORDER BY ps.last_played_at_utc DESC;


--
-- Name: COLUMN vw_recent_plays.last_played_at_utc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.last_played_at_utc IS 'Column last_played_at_utc.';


--
-- Name: COLUMN vw_recent_plays.playcount_last_30; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.playcount_last_30 IS 'Column playcount_last_30.';


--
-- Name: COLUMN vw_recent_plays.playcount_total; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.playcount_total IS 'Column playcount_total.';


--
-- Name: COLUMN vw_recent_plays.track_artist; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.track_artist IS 'Column track_artist.';


--
-- Name: COLUMN vw_recent_plays.rating; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.rating IS 'Column rating.';


--
-- Name: COLUMN vw_recent_plays.max_playcount_last_30; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.max_playcount_last_30 IS 'Column max_playcount_last_30.';


--
-- Name: COLUMN vw_recent_plays.max_playcount_total; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.max_playcount_total IS 'Column max_playcount_total.';


--
-- Name: COLUMN vw_recent_plays.max_elo; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.max_elo IS 'Column max_elo.';


--
-- Name: COLUMN vw_recent_plays.min_elo; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_recent_plays.min_elo IS 'Column min_elo.';


--
-- Name: vw_track_id_finder; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_track_id_finder AS
 SELECT track_isrc
   FROM ( SELECT DISTINCT all_tracks.track_isrc,
            all_tracks.id_synced_at_utc
           FROM music.all_tracks) subquery
  ORDER BY id_synced_at_utc;


--
-- Name: COLUMN vw_track_id_finder.track_isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_track_id_finder.track_isrc IS 'Column track_isrc.';


--
-- Name: vw_track_id_to_isrc; Type: VIEW; Schema: music; Owner: -
--

CREATE VIEW music.vw_track_id_to_isrc AS
 SELECT t.track_id,
    COALESCE(s.mapped_isrc, t.track_isrc) AS isrc
   FROM (music.all_tracks t
     LEFT JOIN music.isrc_swaps s ON ((s.original_isrc = t.track_isrc)));


--
-- Name: COLUMN vw_track_id_to_isrc.track_id; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_track_id_to_isrc.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_track_id_to_isrc.isrc; Type: COMMENT; Schema: music; Owner: -
--

COMMENT ON COLUMN music.vw_track_id_to_isrc.isrc IS 'Column isrc.';


--
-- Name: dim_hour; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_hour (
    hour_key integer NOT NULL,
    ts_utc timestamp without time zone NOT NULL,
    date_key integer NOT NULL,
    hour smallint NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version bigint NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: staging.temp_bc_import; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."staging.temp_bc_import" (
    "ROW_ID" bigint,
    ts_utc text,
    "Weight" double precision,
    "BMI" double precision,
    "Body Fat" double precision,
    "Skeletal Muscle Mass" double precision,
    "Bone Mass" double precision,
    "Body Water" double precision
);


--
-- Name: v_end_elev; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.v_end_elev (
    smoothed_elev double precision
);


--
-- Name: v_is_loop; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.v_is_loop (
    "?column?" boolean
);


--
-- Name: v_start_elev; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.v_start_elev (
    smoothed_elev double precision
);


--
-- Name: v_total_distance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.v_total_distance (
    max integer
);


--
-- Name: vw_date_all; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vw_date_all AS
 WITH anchor AS (
         SELECT CURRENT_DATE AS today
        )
 SELECT d.date_key,
    d.date_utc,
    d.year,
    d.quarter,
    d.month,
    d.day_of_month,
    d.day_of_week,
    d.day_name,
    d.month_name,
    d.week_of_year,
    (d.date_utc - a.today) AS relative_day,
    (((date_trunc('week'::text, (d.date_utc)::timestamp with time zone))::date - (date_trunc('week'::text, (a.today)::timestamp with time zone))::date) / 7) AS relative_week,
    ((((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)) * (12)::numeric) + (EXTRACT(month FROM d.date_utc) - EXTRACT(month FROM a.today))))::integer AS relative_month,
    ((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)))::integer AS relative_year
   FROM (public.dim_date d
     CROSS JOIN anchor a)
  ORDER BY d.date_utc DESC;


--
-- Name: vw_hour; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vw_hour AS
 WITH anchor AS (
         SELECT CURRENT_DATE AS today
        )
 SELECT d.date_key,
    d.date_utc,
    h.ts_utc AS date_hour_utc,
    d.year,
    d.quarter,
    d.month,
    d.day_of_month,
    d.day_of_week,
    d.day_name,
    d.month_name,
    d.week_of_year,
    (d.date_utc - a.today) AS relative_day,
    (((date_trunc('week'::text, (d.date_utc)::timestamp with time zone))::date - (date_trunc('week'::text, (a.today)::timestamp with time zone))::date) / 7) AS relative_week,
    ((((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)) * (12)::numeric) + (EXTRACT(month FROM d.date_utc) - EXTRACT(month FROM a.today))))::integer AS relative_month,
    ((EXTRACT(year FROM d.date_utc) - EXTRACT(year FROM a.today)))::integer AS relative_year
   FROM ((public.dim_date d
     JOIN public.dim_hour h ON ((h.date_key = d.date_key)))
     CROSS JOIN anchor a)
  WHERE (d.date_utc <= a.today)
  ORDER BY d.date_utc DESC;


--
-- Name: activity_metric_descriptors; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.activity_metric_descriptors (
    activity_id bigint NOT NULL,
    metric_idx smallint NOT NULL,
    metric_name text,
    metric_unit text,
    activity_type_id smallint
);


--
-- Name: TABLE activity_metric_descriptors; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.activity_metric_descriptors IS 'Table activity_metric_descriptors.';


--
-- Name: COLUMN activity_metric_descriptors.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metric_descriptors.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_metric_descriptors.metric_idx; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metric_descriptors.metric_idx IS 'Column metric_idx.';


--
-- Name: COLUMN activity_metric_descriptors.metric_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metric_descriptors.metric_name IS 'Column metric_name.';


--
-- Name: COLUMN activity_metric_descriptors.metric_unit; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metric_descriptors.metric_unit IS 'Column metric_unit.';


--
-- Name: COLUMN activity_metric_descriptors.activity_type_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metric_descriptors.activity_type_id IS 'Column activity_type_id.';


--
-- Name: activity_metrics_flat; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.activity_metrics_flat (
    activity_id bigint NOT NULL,
    activity_detail_index integer NOT NULL,
    metric_index integer NOT NULL,
    metric_value text
);


--
-- Name: TABLE activity_metrics_flat; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.activity_metrics_flat IS 'Table activity_metrics_flat.';


--
-- Name: COLUMN activity_metrics_flat.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_flat.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_metrics_flat.activity_detail_index; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_flat.activity_detail_index IS 'Column activity_detail_index.';


--
-- Name: COLUMN activity_metrics_flat.metric_index; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_flat.metric_index IS 'Column metric_index.';


--
-- Name: COLUMN activity_metrics_flat.metric_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_flat.metric_value IS 'Column metric_value.';


--
-- Name: activity_metrics_matched; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.activity_metrics_matched (
    activity_id bigint,
    activity_detail_index integer,
    metric_name text,
    metric_value text,
    activity_type_id smallint
);


--
-- Name: TABLE activity_metrics_matched; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.activity_metrics_matched IS 'Table activity_metrics_matched.';


--
-- Name: COLUMN activity_metrics_matched.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_matched.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_metrics_matched.activity_detail_index; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_matched.activity_detail_index IS 'Column activity_detail_index.';


--
-- Name: COLUMN activity_metrics_matched.metric_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_matched.metric_name IS 'Column metric_name.';


--
-- Name: COLUMN activity_metrics_matched.metric_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_matched.metric_value IS 'Column metric_value.';


--
-- Name: COLUMN activity_metrics_matched.activity_type_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_metrics_matched.activity_type_id IS 'Column activity_type_id.';


--
-- Name: activity_payload; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.activity_payload (
    activity_id bigint NOT NULL,
    payload jsonb
);


--
-- Name: TABLE activity_payload; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.activity_payload IS 'Table activity_payload.';


--
-- Name: COLUMN activity_payload.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_payload.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN activity_payload.payload; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.activity_payload.payload IS 'Column payload.';


--
-- Name: api_imports; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.api_imports (
    load_id bigint NOT NULL,
    event_time_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    api_function_name text,
    payload jsonb NOT NULL,
    task_id integer
);


--
-- Name: TABLE api_imports; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.api_imports IS 'Table api_imports.';


--
-- Name: COLUMN api_imports.load_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.api_imports.load_id IS 'Column load_id.';


--
-- Name: COLUMN api_imports.event_time_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.api_imports.event_time_utc IS 'Column event_time_utc.';


--
-- Name: COLUMN api_imports.api_function_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.api_imports.api_function_name IS 'Column api_function_name.';


--
-- Name: COLUMN api_imports.payload; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.api_imports.payload IS 'Column payload.';


--
-- Name: COLUMN api_imports.task_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.api_imports.task_id IS 'Column task_id.';


--
-- Name: api_imports_load_id_seq; Type: SEQUENCE; Schema: staging; Owner: -
--

CREATE SEQUENCE staging.api_imports_load_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: api_imports_load_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: -
--

ALTER SEQUENCE staging.api_imports_load_id_seq OWNED BY staging.api_imports.load_id;


--
-- Name: bounds; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.bounds (
    id boolean DEFAULT true NOT NULL,
    min_ts timestamp with time zone,
    max_ts timestamp with time zone
);


--
-- Name: TABLE bounds; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.bounds IS 'Table bounds.';


--
-- Name: COLUMN bounds.id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.bounds.id IS 'Column id.';


--
-- Name: COLUMN bounds.min_ts; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.bounds.min_ts IS 'Column min_ts.';


--
-- Name: COLUMN bounds.max_ts; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.bounds.max_ts IS 'Column max_ts.';


--
-- Name: interpolation_anchors; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.interpolation_anchors (
    ts_utc timestamp with time zone NOT NULL,
    load_value numeric,
    prev_value numeric,
    prev_ts timestamp with time zone,
    next_value numeric,
    next_ts timestamp with time zone
);


--
-- Name: TABLE interpolation_anchors; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.interpolation_anchors IS 'Table interpolation_anchors.';


--
-- Name: COLUMN interpolation_anchors.ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN interpolation_anchors.load_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.load_value IS 'Column load_value.';


--
-- Name: COLUMN interpolation_anchors.prev_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.prev_value IS 'Column prev_value.';


--
-- Name: COLUMN interpolation_anchors.prev_ts; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.prev_ts IS 'Column prev_ts.';


--
-- Name: COLUMN interpolation_anchors.next_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.next_value IS 'Column next_value.';


--
-- Name: COLUMN interpolation_anchors.next_ts; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_anchors.next_ts IS 'Column next_ts.';


--
-- Name: interpolation_load_values; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.interpolation_load_values (
    ts_utc timestamp with time zone NOT NULL,
    load_value numeric
);


--
-- Name: TABLE interpolation_load_values; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.interpolation_load_values IS 'Table interpolation_load_values.';


--
-- Name: COLUMN interpolation_load_values.ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_load_values.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN interpolation_load_values.load_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.interpolation_load_values.load_value IS 'Column load_value.';


--
-- Name: isrc_dupe_pairs; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.isrc_dupe_pairs (
    isrc1 text,
    isrc2 text,
    track1 text,
    track2 text,
    artist1 text,
    artist2 text,
    artist_id1 text,
    artist_id2 text,
    album1 text,
    album2 text,
    album_id1 text,
    album_id2 text,
    duration1 integer,
    duration2 integer
);


--
-- Name: TABLE isrc_dupe_pairs; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.isrc_dupe_pairs IS 'Table isrc_dupe_pairs.';


--
-- Name: COLUMN isrc_dupe_pairs.isrc1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.isrc1 IS 'Column isrc1.';


--
-- Name: COLUMN isrc_dupe_pairs.isrc2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.isrc2 IS 'Column isrc2.';


--
-- Name: COLUMN isrc_dupe_pairs.track1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.track1 IS 'Column track1.';


--
-- Name: COLUMN isrc_dupe_pairs.track2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.track2 IS 'Column track2.';


--
-- Name: COLUMN isrc_dupe_pairs.artist1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.artist1 IS 'Column artist1.';


--
-- Name: COLUMN isrc_dupe_pairs.artist2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.artist2 IS 'Column artist2.';


--
-- Name: COLUMN isrc_dupe_pairs.artist_id1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.artist_id1 IS 'Column artist_id1.';


--
-- Name: COLUMN isrc_dupe_pairs.artist_id2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.artist_id2 IS 'Column artist_id2.';


--
-- Name: COLUMN isrc_dupe_pairs.album1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.album1 IS 'Column album1.';


--
-- Name: COLUMN isrc_dupe_pairs.album2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.album2 IS 'Column album2.';


--
-- Name: COLUMN isrc_dupe_pairs.album_id1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.album_id1 IS 'Column album_id1.';


--
-- Name: COLUMN isrc_dupe_pairs.album_id2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.album_id2 IS 'Column album_id2.';


--
-- Name: COLUMN isrc_dupe_pairs.duration1; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.duration1 IS 'Column duration1.';


--
-- Name: COLUMN isrc_dupe_pairs.duration2; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_pairs.duration2 IS 'Column duration2.';


--
-- Name: isrc_dupe_seeds; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.isrc_dupe_seeds (
    isrc text NOT NULL,
    track_name_search text,
    artist_name_search text,
    album_name_search text,
    artist_id text,
    album_id text,
    duration_ms integer,
    prior_comparison text
);


--
-- Name: TABLE isrc_dupe_seeds; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.isrc_dupe_seeds IS 'Table isrc_dupe_seeds.';


--
-- Name: COLUMN isrc_dupe_seeds.isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.isrc IS 'Column isrc.';


--
-- Name: COLUMN isrc_dupe_seeds.track_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.track_name_search IS 'Column track_name_search.';


--
-- Name: COLUMN isrc_dupe_seeds.artist_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.artist_name_search IS 'Column artist_name_search.';


--
-- Name: COLUMN isrc_dupe_seeds.album_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.album_name_search IS 'Column album_name_search.';


--
-- Name: COLUMN isrc_dupe_seeds.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN isrc_dupe_seeds.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.album_id IS 'Column album_id.';


--
-- Name: COLUMN isrc_dupe_seeds.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN isrc_dupe_seeds.prior_comparison; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.isrc_dupe_seeds.prior_comparison IS 'Column prior_comparison.';


--
-- Name: json_examples; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.json_examples (
    raw_table_name text NOT NULL,
    json_sample jsonb,
    updated_at_utc timestamp with time zone
);


--
-- Name: TABLE json_examples; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.json_examples IS 'Table json_examples.';


--
-- Name: COLUMN json_examples.raw_table_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.json_examples.raw_table_name IS 'Column raw_table_name.';


--
-- Name: COLUMN json_examples.json_sample; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.json_examples.json_sample IS 'Column json_sample.';


--
-- Name: COLUMN json_examples.updated_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.json_examples.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: stg_listen_history; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.stg_listen_history (
    track_id text NOT NULL,
    track_name text,
    album_id text,
    album_name text,
    artist_id text,
    artist_name text,
    popularity integer,
    duration_ms integer,
    incoming_isrc text,
    available_market_count integer,
    available_in_usa boolean,
    context_type text,
    context_uri text,
    played_at_utc timestamp with time zone NOT NULL,
    track_release text,
    track_release_precision text
);


--
-- Name: TABLE stg_listen_history; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.stg_listen_history IS 'Table stg_listen_history.';


--
-- Name: COLUMN stg_listen_history.track_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.track_id IS 'Column track_id.';


--
-- Name: COLUMN stg_listen_history.track_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.track_name IS 'Column track_name.';


--
-- Name: COLUMN stg_listen_history.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.album_id IS 'Column album_id.';


--
-- Name: COLUMN stg_listen_history.album_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.album_name IS 'Column album_name.';


--
-- Name: COLUMN stg_listen_history.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN stg_listen_history.artist_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.artist_name IS 'Column artist_name.';


--
-- Name: COLUMN stg_listen_history.popularity; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.popularity IS 'Column popularity.';


--
-- Name: COLUMN stg_listen_history.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN stg_listen_history.incoming_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.incoming_isrc IS 'Column incoming_isrc.';


--
-- Name: COLUMN stg_listen_history.available_market_count; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.available_market_count IS 'Column available_market_count.';


--
-- Name: COLUMN stg_listen_history.available_in_usa; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.available_in_usa IS 'Column available_in_usa.';


--
-- Name: COLUMN stg_listen_history.context_type; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.context_type IS 'Column context_type.';


--
-- Name: COLUMN stg_listen_history.context_uri; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.context_uri IS 'Column context_uri.';


--
-- Name: COLUMN stg_listen_history.played_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN stg_listen_history.track_release; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.track_release IS 'Column track_release.';


--
-- Name: COLUMN stg_listen_history.track_release_precision; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_listen_history.track_release_precision IS 'Column track_release_precision.';


--
-- Name: stg_now_playing; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.stg_now_playing (
    track_id text NOT NULL,
    track_name text,
    album_id text,
    album_name text,
    artist_id text,
    artist_name text,
    popularity integer,
    duration_ms integer,
    incoming_isrc text,
    available_market_count integer,
    available_in_usa boolean,
    context_type text,
    context_uri text,
    played_at_utc timestamp with time zone NOT NULL,
    track_release text,
    track_release_precision text
);


--
-- Name: TABLE stg_now_playing; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.stg_now_playing IS 'Table stg_now_playing.';


--
-- Name: COLUMN stg_now_playing.track_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.track_id IS 'Column track_id.';


--
-- Name: COLUMN stg_now_playing.track_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.track_name IS 'Column track_name.';


--
-- Name: COLUMN stg_now_playing.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.album_id IS 'Column album_id.';


--
-- Name: COLUMN stg_now_playing.album_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.album_name IS 'Column album_name.';


--
-- Name: COLUMN stg_now_playing.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN stg_now_playing.artist_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.artist_name IS 'Column artist_name.';


--
-- Name: COLUMN stg_now_playing.popularity; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.popularity IS 'Column popularity.';


--
-- Name: COLUMN stg_now_playing.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN stg_now_playing.incoming_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.incoming_isrc IS 'Column incoming_isrc.';


--
-- Name: COLUMN stg_now_playing.available_market_count; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.available_market_count IS 'Column available_market_count.';


--
-- Name: COLUMN stg_now_playing.available_in_usa; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.available_in_usa IS 'Column available_in_usa.';


--
-- Name: COLUMN stg_now_playing.context_type; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.context_type IS 'Column context_type.';


--
-- Name: COLUMN stg_now_playing.context_uri; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.context_uri IS 'Column context_uri.';


--
-- Name: COLUMN stg_now_playing.played_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN stg_now_playing.track_release; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.track_release IS 'Column track_release.';


--
-- Name: COLUMN stg_now_playing.track_release_precision; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_now_playing.track_release_precision IS 'Column track_release_precision.';


--
-- Name: stg_playlist_details; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.stg_playlist_details (
    playlist_id text,
    track_id text,
    track_name text,
    track_order integer,
    album_id text,
    album_name text,
    artist_id text,
    artist_name text,
    track_release text,
    track_release_precision text,
    track_markets text,
    popularity integer,
    incoming_isrc text,
    duration_ms integer,
    fetched_at_utc timestamp with time zone
);


--
-- Name: TABLE stg_playlist_details; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.stg_playlist_details IS 'Table stg_playlist_details.';


--
-- Name: COLUMN stg_playlist_details.playlist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN stg_playlist_details.track_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_id IS 'Column track_id.';


--
-- Name: COLUMN stg_playlist_details.track_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_name IS 'Column track_name.';


--
-- Name: COLUMN stg_playlist_details.track_order; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_order IS 'Column track_order.';


--
-- Name: COLUMN stg_playlist_details.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.album_id IS 'Column album_id.';


--
-- Name: COLUMN stg_playlist_details.album_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.album_name IS 'Column album_name.';


--
-- Name: COLUMN stg_playlist_details.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN stg_playlist_details.artist_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.artist_name IS 'Column artist_name.';


--
-- Name: COLUMN stg_playlist_details.track_release; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_release IS 'Column track_release.';


--
-- Name: COLUMN stg_playlist_details.track_release_precision; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_release_precision IS 'Column track_release_precision.';


--
-- Name: COLUMN stg_playlist_details.track_markets; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.track_markets IS 'Column track_markets.';


--
-- Name: COLUMN stg_playlist_details.popularity; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.popularity IS 'Column popularity.';


--
-- Name: COLUMN stg_playlist_details.incoming_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.incoming_isrc IS 'Column incoming_isrc.';


--
-- Name: COLUMN stg_playlist_details.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN stg_playlist_details.fetched_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.stg_playlist_details.fetched_at_utc IS 'Column fetched_at_utc.';


--
-- Name: temp_activity_locations; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.temp_activity_locations (
    latitude numeric(9,6),
    longitude numeric(9,6),
    loc_point public.geography(Point,4326),
    elevation_m_smooth numeric(10,2),
    grid_5m_00 public.geometry(Point,3857),
    grid_5m_25_00 public.geometry(Point,3857),
    grid_5m_00_25 public.geometry(Point,3857),
    grid_5m_25_25 public.geometry(Point,3857)
);


--
-- Name: TABLE temp_activity_locations; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_activity_locations IS 'Table temp_activity_locations.';


--
-- Name: COLUMN temp_activity_locations.latitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.latitude IS 'Column latitude.';


--
-- Name: COLUMN temp_activity_locations.longitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.longitude IS 'Column longitude.';


--
-- Name: COLUMN temp_activity_locations.loc_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN temp_activity_locations.elevation_m_smooth; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: COLUMN temp_activity_locations.grid_5m_00; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.grid_5m_00 IS 'Column grid_5m_00.';


--
-- Name: COLUMN temp_activity_locations.grid_5m_25_00; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.grid_5m_25_00 IS 'Column grid_5m_25_00.';


--
-- Name: COLUMN temp_activity_locations.grid_5m_00_25; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.grid_5m_00_25 IS 'Column grid_5m_00_25.';


--
-- Name: COLUMN temp_activity_locations.grid_5m_25_25; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_locations.grid_5m_25_25 IS 'Column grid_5m_25_25.';


--
-- Name: temp_activity_matched_locations; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.temp_activity_matched_locations (
    latitude numeric(9,6),
    longitude numeric(9,6),
    loc_point public.geography(Point,4326),
    loc_point_matched public.geography(Point,4326),
    elevation_m_smooth numeric(10,2)
);


--
-- Name: TABLE temp_activity_matched_locations; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_activity_matched_locations IS 'Table temp_activity_matched_locations.';


--
-- Name: COLUMN temp_activity_matched_locations.latitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_matched_locations.latitude IS 'Column latitude.';


--
-- Name: COLUMN temp_activity_matched_locations.longitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_matched_locations.longitude IS 'Column longitude.';


--
-- Name: COLUMN temp_activity_matched_locations.loc_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_matched_locations.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN temp_activity_matched_locations.loc_point_matched; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_matched_locations.loc_point_matched IS 'Column loc_point_matched.';


--
-- Name: COLUMN temp_activity_matched_locations.elevation_m_smooth; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_matched_locations.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: temp_activity_metas; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_activity_metas (
    activity_id bigint NOT NULL,
    activity_type_id smallint,
    activity_path public.geometry(LineStringZM,4326)
);


--
-- Name: TABLE temp_activity_metas; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_activity_metas IS 'Table temp_activity_metas.';


--
-- Name: COLUMN temp_activity_metas.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_metas.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_activity_metas.activity_type_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_metas.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN temp_activity_metas.activity_path; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_metas.activity_path IS 'Column activity_path.';


--
-- Name: temp_activity_trackpoints; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_activity_trackpoints (
    activity_id bigint,
    track_point integer,
    longitude numeric(18,15),
    latitude numeric(18,15),
    elevation_m numeric(10,2),
    activity_type_id smallint
);


--
-- Name: TABLE temp_activity_trackpoints; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_activity_trackpoints IS 'Table temp_activity_trackpoints.';


--
-- Name: COLUMN temp_activity_trackpoints.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_activity_trackpoints.track_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.track_point IS 'Column track_point.';


--
-- Name: COLUMN temp_activity_trackpoints.longitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.longitude IS 'Column longitude.';


--
-- Name: COLUMN temp_activity_trackpoints.latitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.latitude IS 'Column latitude.';


--
-- Name: COLUMN temp_activity_trackpoints.elevation_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN temp_activity_trackpoints.activity_type_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_activity_trackpoints.activity_type_id IS 'Column activity_type_id.';


--
-- Name: temp_bc_import; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_bc_import (
    "ROW_ID" bigint,
    ts_utc text,
    "Weight" double precision,
    "BMI" double precision,
    "Body Fat" double precision,
    "Skeletal Muscle Mass" double precision,
    "Bone Mass" double precision,
    "Body Water" double precision
);


--
-- Name: TABLE temp_bc_import; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_bc_import IS 'Table temp_bc_import.';


--
-- Name: COLUMN temp_bc_import."ROW_ID"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."ROW_ID" IS 'Column ROW_ID.';


--
-- Name: COLUMN temp_bc_import.ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN temp_bc_import."Weight"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."Weight" IS 'Column Weight.';


--
-- Name: COLUMN temp_bc_import."BMI"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."BMI" IS 'Column BMI.';


--
-- Name: COLUMN temp_bc_import."Body Fat"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."Body Fat" IS 'Column Body Fat.';


--
-- Name: COLUMN temp_bc_import."Skeletal Muscle Mass"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."Skeletal Muscle Mass" IS 'Column Skeletal Muscle Mass.';


--
-- Name: COLUMN temp_bc_import."Bone Mass"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."Bone Mass" IS 'Column Bone Mass.';


--
-- Name: COLUMN temp_bc_import."Body Water"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_bc_import."Body Water" IS 'Column Body Water.';


--
-- Name: temp_csv_import; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_csv_import (
    "Sleep Score 4 Weeks" text,
    "Score" text,
    "Resting Heart Rate" text,
    "Body Battery" text,
    "Pulse Ox" text,
    "Respiration" text,
    "HRV Status" text,
    "Quality" text,
    "Duration" text,
    "Bedtime" text,
    "Wake Time" text
);


--
-- Name: TABLE temp_csv_import; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_csv_import IS 'Table temp_csv_import.';


--
-- Name: COLUMN temp_csv_import."Sleep Score 4 Weeks"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Sleep Score 4 Weeks" IS 'Column Sleep Score 4 Weeks.';


--
-- Name: COLUMN temp_csv_import."Score"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Score" IS 'Column Score.';


--
-- Name: COLUMN temp_csv_import."Resting Heart Rate"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Resting Heart Rate" IS 'Column Resting Heart Rate.';


--
-- Name: COLUMN temp_csv_import."Body Battery"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Body Battery" IS 'Column Body Battery.';


--
-- Name: COLUMN temp_csv_import."Pulse Ox"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Pulse Ox" IS 'Column Pulse Ox.';


--
-- Name: COLUMN temp_csv_import."Respiration"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Respiration" IS 'Column Respiration.';


--
-- Name: COLUMN temp_csv_import."HRV Status"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."HRV Status" IS 'Column HRV Status.';


--
-- Name: COLUMN temp_csv_import."Quality"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Quality" IS 'Column Quality.';


--
-- Name: COLUMN temp_csv_import."Duration"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Duration" IS 'Column Duration.';


--
-- Name: COLUMN temp_csv_import."Bedtime"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Bedtime" IS 'Column Bedtime.';


--
-- Name: COLUMN temp_csv_import."Wake Time"; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_csv_import."Wake Time" IS 'Column Wake Time.';


--
-- Name: temp_elevation_deltas; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_elevation_deltas (
    activity_id bigint NOT NULL,
    ascent_m numeric(10,2),
    descent_m numeric(10,2)
);


--
-- Name: TABLE temp_elevation_deltas; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_elevation_deltas IS 'Table temp_elevation_deltas.';


--
-- Name: COLUMN temp_elevation_deltas.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_deltas.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_elevation_deltas.ascent_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_deltas.ascent_m IS 'Column ascent_m.';


--
-- Name: COLUMN temp_elevation_deltas.descent_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_deltas.descent_m IS 'Column descent_m.';


--
-- Name: temp_elevation_medians; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_elevation_medians (
    loc_point public.geography(Point,4326) NOT NULL,
    median_elev numeric(10,2)
);


--
-- Name: TABLE temp_elevation_medians; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_elevation_medians IS 'Table temp_elevation_medians.';


--
-- Name: COLUMN temp_elevation_medians.loc_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_medians.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN temp_elevation_medians.median_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_medians.median_elev IS 'Column median_elev.';


--
-- Name: temp_elevation_smoothed; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_elevation_smoothed (
    activity_id bigint,
    track_point integer,
    elevation_m numeric(10,2)
);


--
-- Name: TABLE temp_elevation_smoothed; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_elevation_smoothed IS 'Table temp_elevation_smoothed.';


--
-- Name: COLUMN temp_elevation_smoothed.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothed.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_elevation_smoothed.track_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothed.track_point IS 'Column track_point.';


--
-- Name: COLUMN temp_elevation_smoothed.elevation_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothed.elevation_m IS 'Column elevation_m.';


--
-- Name: temp_elevation_smoothing; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.temp_elevation_smoothing (
    elapsed_duration_s numeric NOT NULL,
    elevation_m_raw numeric,
    distance_m numeric
);


--
-- Name: TABLE temp_elevation_smoothing; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_elevation_smoothing IS 'Table temp_elevation_smoothing.';


--
-- Name: COLUMN temp_elevation_smoothing.elapsed_duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN temp_elevation_smoothing.elevation_m_raw; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing.elevation_m_raw IS 'Column elevation_m_raw.';


--
-- Name: COLUMN temp_elevation_smoothing.distance_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing.distance_m IS 'Column distance_m.';


--
-- Name: temp_elevation_smoothing_m; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.temp_elevation_smoothing_m (
    meter integer NOT NULL,
    elapsed_duration_s numeric,
    elevation_m_raw numeric,
    elevation_m_smooth numeric
);


--
-- Name: TABLE temp_elevation_smoothing_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_elevation_smoothing_m IS 'Table temp_elevation_smoothing_m.';


--
-- Name: COLUMN temp_elevation_smoothing_m.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing_m.meter IS 'Column meter.';


--
-- Name: COLUMN temp_elevation_smoothing_m.elapsed_duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing_m.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN temp_elevation_smoothing_m.elevation_m_raw; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing_m.elevation_m_raw IS 'Column elevation_m_raw.';


--
-- Name: COLUMN temp_elevation_smoothing_m.elevation_m_smooth; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_elevation_smoothing_m.elevation_m_smooth IS 'Column elevation_m_smooth.';


--
-- Name: temp_isrc_swaps; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_isrc_swaps (
    original_isrc text,
    mapped_isrc text,
    mapping_time_utc timestamp with time zone,
    valid_mapping boolean
);


--
-- Name: TABLE temp_isrc_swaps; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_isrc_swaps IS 'Table temp_isrc_swaps.';


--
-- Name: COLUMN temp_isrc_swaps.original_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_isrc_swaps.original_isrc IS 'Column original_isrc.';


--
-- Name: COLUMN temp_isrc_swaps.mapped_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_isrc_swaps.mapped_isrc IS 'Column mapped_isrc.';


--
-- Name: COLUMN temp_isrc_swaps.mapping_time_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_isrc_swaps.mapping_time_utc IS 'Column mapping_time_utc.';


--
-- Name: COLUMN temp_isrc_swaps.valid_mapping; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_isrc_swaps.valid_mapping IS 'Column valid_mapping.';


--
-- Name: temp_performance_profiling; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_performance_profiling (
    ts timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    segment text,
    code text,
    time_ms integer
);


--
-- Name: TABLE temp_performance_profiling; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_performance_profiling IS 'Table temp_performance_profiling.';


--
-- Name: COLUMN temp_performance_profiling.ts; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_performance_profiling.ts IS 'Column ts.';


--
-- Name: COLUMN temp_performance_profiling.segment; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_performance_profiling.segment IS 'Column segment.';


--
-- Name: COLUMN temp_performance_profiling.code; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_performance_profiling.code IS 'Column code.';


--
-- Name: COLUMN temp_performance_profiling.time_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_performance_profiling.time_ms IS 'Column time_ms.';


--
-- Name: temp_segment_matches; Type: TABLE; Schema: staging; Owner: -
--

CREATE UNLOGGED TABLE staging.temp_segment_matches (
    segment_id integer,
    activity_id bigint,
    activity_start_point integer,
    activity_end_point integer,
    activity_path public.geometry(LineStringZM,4326),
    match_confirmed boolean,
    match_confidence numeric(6,2),
    match_ts_utc timestamp with time zone
);


--
-- Name: TABLE temp_segment_matches; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_segment_matches IS 'Table temp_segment_matches.';


--
-- Name: COLUMN temp_segment_matches.segment_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.segment_id IS 'Column segment_id.';


--
-- Name: COLUMN temp_segment_matches.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN temp_segment_matches.activity_start_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.activity_start_point IS 'Column activity_start_point.';


--
-- Name: COLUMN temp_segment_matches.activity_end_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.activity_end_point IS 'Column activity_end_point.';


--
-- Name: COLUMN temp_segment_matches.activity_path; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.activity_path IS 'Column activity_path.';


--
-- Name: COLUMN temp_segment_matches.match_confirmed; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.match_confirmed IS 'Column match_confirmed.';


--
-- Name: COLUMN temp_segment_matches.match_confidence; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.match_confidence IS 'Column match_confidence.';


--
-- Name: COLUMN temp_segment_matches.match_ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_segment_matches.match_ts_utc IS 'Column match_ts_utc.';


--
-- Name: temp_training_status_raw; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.temp_training_status_raw (
    ts_utc timestamp with time zone,
    vo2_max_value numeric(4,1),
    altitude_current_m integer,
    altitude_acclimation_m integer,
    altitude_acclimation_pct numeric,
    heat_acclimation_pct numeric,
    training_load_acute numeric(5,1),
    training_load_min numeric(5,1),
    training_load_max numeric(5,1)
);


--
-- Name: TABLE temp_training_status_raw; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON TABLE staging.temp_training_status_raw IS 'Table temp_training_status_raw.';


--
-- Name: COLUMN temp_training_status_raw.ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN temp_training_status_raw.vo2_max_value; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.vo2_max_value IS 'Column vo2_max_value.';


--
-- Name: COLUMN temp_training_status_raw.altitude_current_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.altitude_current_m IS 'Column altitude_current_m.';


--
-- Name: COLUMN temp_training_status_raw.altitude_acclimation_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.altitude_acclimation_m IS 'Column altitude_acclimation_m.';


--
-- Name: COLUMN temp_training_status_raw.altitude_acclimation_pct; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.altitude_acclimation_pct IS 'Column altitude_acclimation_pct.';


--
-- Name: COLUMN temp_training_status_raw.heat_acclimation_pct; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.heat_acclimation_pct IS 'Column heat_acclimation_pct.';


--
-- Name: COLUMN temp_training_status_raw.training_load_acute; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.training_load_acute IS 'Column training_load_acute.';


--
-- Name: COLUMN temp_training_status_raw.training_load_min; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.training_load_min IS 'Column training_load_min.';


--
-- Name: COLUMN temp_training_status_raw.training_load_max; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.temp_training_status_raw.training_load_max IS 'Column training_load_max.';


--
-- Name: vw_activity_by_distance; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_activity_by_distance AS
 WITH bounds AS (
         SELECT temp_elevation_smoothing.elapsed_duration_s,
            temp_elevation_smoothing.distance_m,
            temp_elevation_smoothing.elevation_m_raw,
            COALESCE(lag(temp_elevation_smoothing.elapsed_duration_s) OVER (ORDER BY temp_elevation_smoothing.elapsed_duration_s), (0)::numeric) AS prev_t,
            COALESCE(lag(temp_elevation_smoothing.elevation_m_raw) OVER (ORDER BY temp_elevation_smoothing.elevation_m_raw), (0)::numeric) AS prev_elev_raw,
            COALESCE(lag(temp_elevation_smoothing.distance_m) OVER (ORDER BY temp_elevation_smoothing.elapsed_duration_s), (0)::numeric) AS prev_d
           FROM staging.temp_elevation_smoothing
        ), extrp AS (
         SELECT m_1.meter,
            (bounds.prev_t + ((bounds.elapsed_duration_s - bounds.prev_t) * ((m_1.meter - bounds.prev_d) / (bounds.distance_m - bounds.prev_d)))) AS elapsed_duration_s,
            (bounds.prev_elev_raw + ((bounds.elevation_m_raw - bounds.prev_elev_raw) * ((m_1.meter - bounds.prev_d) / (bounds.distance_m - bounds.prev_d)))) AS elevation_raw
           FROM (generate_series((0)::numeric, ( SELECT ceil(max(temp_elevation_smoothing.distance_m)) AS ceil
                   FROM staging.temp_elevation_smoothing)) m_1(meter)
             JOIN bounds ON (((m_1.meter >= bounds.prev_d) AND (m_1.meter <= bounds.distance_m))))
          WHERE (bounds.distance_m <> bounds.prev_d)
        )
 SELECT (meter)::integer AS meter,
    avg(elapsed_duration_s) AS elapsed_duration_s,
    avg(elevation_raw) AS elev_raw
   FROM extrp m
  GROUP BY ((meter)::integer)
  ORDER BY ((meter)::integer);


--
-- Name: COLUMN vw_activity_by_distance.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_by_distance.meter IS 'Column meter.';


--
-- Name: COLUMN vw_activity_by_distance.elapsed_duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_by_distance.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_activity_by_distance.elev_raw; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_by_distance.elev_raw IS 'Column elev_raw.';


--
-- Name: vw_activity_flatten; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_activity_flatten AS
 SELECT activity_id,
    activity_type_id,
    activity_detail_index,
    ts_utc,
    moving_duration_s,
    elapsed_duration_s,
    duration_s,
    distance_mm,
    latitude,
    longitude,
    heartrate_bpm,
    cadence_spm,
    air_temp_c,
    elevation_m,
    speed_mmps,
    vertical_speed_mps,
    vert_oscillation_cm,
    stride_length_cm,
    vert_ratio,
    ground_contact_time_ms,
    ground_contact_balance_left,
    performance_condition,
    (public.st_setsrid(public.st_makepoint((longitude)::double precision, (latitude)::double precision), 4326))::public.geography AS loc_point,
    elevation_m AS elevation_smooth_m
   FROM ( SELECT activity_metrics_matched.activity_id,
            activity_metrics_matched.activity_type_id,
            activity_metrics_matched.activity_detail_index,
            max(to_timestamp((round((
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directTimestamp'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END / (1000)::numeric), 4))::double precision)) AS ts_utc,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'sumMovingDuration'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::integer AS moving_duration_s,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'sumElapsedDuration'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::integer AS elapsed_duration_s,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'sumDuration'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::integer AS duration_s,
            (max(round((
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'sumDistance'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END * (1000)::numeric), 0)))::bigint AS distance_mm,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directLatitude'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 6)))::numeric(9,6) AS latitude,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directLongitude'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 6)))::numeric(9,6) AS longitude,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directHeartRate'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::integer AS heartrate_bpm,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directDoubleCadence'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::smallint AS cadence_spm,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directAirTemperature'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::smallint AS air_temp_c,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directElevation'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 1)))::numeric(7,1) AS elevation_m,
            (max(round((
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directSpeed'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END * (1000)::numeric), 0)))::bigint AS speed_mmps,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directVerticalSpeed'::text) THEN
                    CASE
                        WHEN ((activity_metrics_matched.metric_value)::numeric >= 100.0) THEN (2)::numeric
                        WHEN ((activity_metrics_matched.metric_value)::numeric <= '-100.0'::numeric) THEN ('-2'::integer)::numeric
                        ELSE (activity_metrics_matched.metric_value)::numeric
                    END
                    ELSE NULL::numeric
                END, 1)))::numeric(4,1) AS vertical_speed_mps,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directVerticalOscillation'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 2)))::numeric(5,2) AS vert_oscillation_cm,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directStrideLength'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 1)))::numeric(4,1) AS stride_length_cm,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directVerticalRatio'::text) THEN
                    CASE
                        WHEN ((activity_metrics_matched.metric_value)::numeric > (100)::numeric) THEN (99)::numeric
                        WHEN ((activity_metrics_matched.metric_value)::numeric < (100)::numeric) THEN (- (99)::numeric)
                        ELSE (activity_metrics_matched.metric_value)::numeric
                    END
                    ELSE NULL::numeric
                END, 2)))::numeric(4,2) AS vert_ratio,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directGroundContactTime'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 0)))::smallint AS ground_contact_time_ms,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directGroundContactBalanceLeft'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE NULL::numeric
                END, 2)))::numeric(4,2) AS ground_contact_balance_left,
            (max(round(
                CASE
                    WHEN (activity_metrics_matched.metric_name = 'directPerformanceCondition'::text) THEN (activity_metrics_matched.metric_value)::numeric
                    ELSE (0)::numeric
                END, 0)))::smallint AS performance_condition
           FROM staging.activity_metrics_matched
          GROUP BY activity_metrics_matched.activity_id, activity_metrics_matched.activity_type_id, activity_metrics_matched.activity_detail_index) subquery
  ORDER BY activity_id DESC, elapsed_duration_s;


--
-- Name: COLUMN vw_activity_flatten.activity_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.activity_id IS 'Column activity_id.';


--
-- Name: COLUMN vw_activity_flatten.activity_type_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.activity_type_id IS 'Column activity_type_id.';


--
-- Name: COLUMN vw_activity_flatten.activity_detail_index; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.activity_detail_index IS 'Column activity_detail_index.';


--
-- Name: COLUMN vw_activity_flatten.ts_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.ts_utc IS 'Column ts_utc.';


--
-- Name: COLUMN vw_activity_flatten.moving_duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.moving_duration_s IS 'Column moving_duration_s.';


--
-- Name: COLUMN vw_activity_flatten.elapsed_duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.elapsed_duration_s IS 'Column elapsed_duration_s.';


--
-- Name: COLUMN vw_activity_flatten.duration_s; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.duration_s IS 'Column duration_s.';


--
-- Name: COLUMN vw_activity_flatten.distance_mm; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.distance_mm IS 'Column distance_mm.';


--
-- Name: COLUMN vw_activity_flatten.latitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.latitude IS 'Column latitude.';


--
-- Name: COLUMN vw_activity_flatten.longitude; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.longitude IS 'Column longitude.';


--
-- Name: COLUMN vw_activity_flatten.heartrate_bpm; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.heartrate_bpm IS 'Column heartrate_bpm.';


--
-- Name: COLUMN vw_activity_flatten.cadence_spm; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.cadence_spm IS 'Column cadence_spm.';


--
-- Name: COLUMN vw_activity_flatten.air_temp_c; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.air_temp_c IS 'Column air_temp_c.';


--
-- Name: COLUMN vw_activity_flatten.elevation_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.elevation_m IS 'Column elevation_m.';


--
-- Name: COLUMN vw_activity_flatten.speed_mmps; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.speed_mmps IS 'Column speed_mmps.';


--
-- Name: COLUMN vw_activity_flatten.vertical_speed_mps; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.vertical_speed_mps IS 'Column vertical_speed_mps.';


--
-- Name: COLUMN vw_activity_flatten.vert_oscillation_cm; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.vert_oscillation_cm IS 'Column vert_oscillation_cm.';


--
-- Name: COLUMN vw_activity_flatten.stride_length_cm; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.stride_length_cm IS 'Column stride_length_cm.';


--
-- Name: COLUMN vw_activity_flatten.vert_ratio; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.vert_ratio IS 'Column vert_ratio.';


--
-- Name: COLUMN vw_activity_flatten.ground_contact_time_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.ground_contact_time_ms IS 'Column ground_contact_time_ms.';


--
-- Name: COLUMN vw_activity_flatten.ground_contact_balance_left; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.ground_contact_balance_left IS 'Column ground_contact_balance_left.';


--
-- Name: COLUMN vw_activity_flatten.performance_condition; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.performance_condition IS 'Column performance_condition.';


--
-- Name: COLUMN vw_activity_flatten.loc_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.loc_point IS 'Column loc_point.';


--
-- Name: COLUMN vw_activity_flatten.elevation_smooth_m; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_activity_flatten.elevation_smooth_m IS 'Column elevation_smooth_m.';


--
-- Name: vw_album_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_album_inserts AS
 WITH base AS (
         SELECT DISTINCT stg_playlist_details.album_id,
            stg_playlist_details.album_name,
            stg_playlist_details.fetched_at_utc
           FROM staging.stg_playlist_details
        UNION ALL
         SELECT DISTINCT stg_listen_history.album_id,
            stg_listen_history.album_name,
            stg_listen_history.played_at_utc AS fetched_at_utc
           FROM staging.stg_listen_history
        ), r1 AS (
         SELECT base.album_id,
            base.album_name,
            base.fetched_at_utc,
            row_number() OVER (PARTITION BY base.album_id ORDER BY base.fetched_at_utc DESC) AS duplicate_finder
           FROM base
        ), cleaned AS (
         SELECT r1.album_id,
            r1.album_name,
            public.clean_track_display(r1.album_name) AS album_name_clean,
            r1.fetched_at_utc
           FROM r1
          WHERE (r1.duplicate_finder = 1)
        )
 SELECT album_id,
    album_name,
    album_name_clean,
    public.clean_album_match(album_name_clean) AS album_name_search,
    fetched_at_utc AS updated_at_utc
   FROM cleaned;


--
-- Name: COLUMN vw_album_inserts.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_album_inserts.album_id IS 'Column album_id.';


--
-- Name: COLUMN vw_album_inserts.album_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_album_inserts.album_name IS 'Column album_name.';


--
-- Name: COLUMN vw_album_inserts.album_name_clean; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_album_inserts.album_name_clean IS 'Column album_name_clean.';


--
-- Name: COLUMN vw_album_inserts.album_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_album_inserts.album_name_search IS 'Column album_name_search.';


--
-- Name: COLUMN vw_album_inserts.updated_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_album_inserts.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: vw_artist_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_artist_inserts AS
 WITH base AS (
         SELECT DISTINCT stg_playlist_details.artist_id,
            stg_playlist_details.artist_name,
            stg_playlist_details.fetched_at_utc
           FROM staging.stg_playlist_details
        UNION ALL
         SELECT DISTINCT stg_listen_history.artist_id,
            stg_listen_history.artist_name,
            stg_listen_history.played_at_utc AS fetched_at_utc
           FROM staging.stg_listen_history
        ), r1 AS (
         SELECT base.artist_id,
            base.artist_name,
            base.fetched_at_utc,
            row_number() OVER (PARTITION BY base.artist_id ORDER BY base.fetched_at_utc DESC) AS duplicate_finder
           FROM base
        ), cleaned AS (
         SELECT r1.artist_id,
            r1.artist_name,
            public.clean_artist_match(r1.artist_name) AS artist_name_search,
            r1.fetched_at_utc
           FROM r1
          WHERE (r1.duplicate_finder = 1)
        )
 SELECT artist_id,
    artist_name,
    artist_name_search,
    fetched_at_utc AS updated_at_utc
   FROM cleaned;


--
-- Name: COLUMN vw_artist_inserts.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_artist_inserts.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN vw_artist_inserts.artist_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_artist_inserts.artist_name IS 'Column artist_name.';


--
-- Name: COLUMN vw_artist_inserts.artist_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_artist_inserts.artist_name_search IS 'Column artist_name_search.';


--
-- Name: COLUMN vw_artist_inserts.updated_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_artist_inserts.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: vw_elevation_neighbors; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_neighbors AS
 WITH pass1 AS (
         SELECT temp_elevation_smoothing_m.meter,
            temp_elevation_smoothing_m.elevation_m_smooth AS curr_elev,
            COALESCE(lag(temp_elevation_smoothing_m.elevation_m_smooth, 1) OVER w, temp_elevation_smoothing_m.elevation_m_smooth) AS prior_elev,
            COALESCE(lead(temp_elevation_smoothing_m.elevation_m_smooth, 1) OVER w, temp_elevation_smoothing_m.elevation_m_smooth) AS next_elev
           FROM staging.temp_elevation_smoothing_m
          WINDOW w AS (ORDER BY temp_elevation_smoothing_m.meter)
        ), pass2 AS (
         SELECT pass1.meter,
            pass1.curr_elev,
            pass1.prior_elev,
            pass1.next_elev,
            (pass1.curr_elev - pass1.prior_elev) AS inc_delta,
            (pass1.prior_elev - pass1.curr_elev) AS out_delta
           FROM pass1
        )
 SELECT meter,
    curr_elev,
    prior_elev,
    next_elev,
    inc_delta,
    out_delta,
    ((abs(inc_delta) < 1.0) AND (abs(out_delta) < 1.0)) AS valid_point,
    ((abs(inc_delta) > 4.0) AND (abs(out_delta) > 4.0)) AS extreme_point
   FROM pass2;


--
-- Name: COLUMN vw_elevation_neighbors.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_neighbors.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_neighbors.prior_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.prior_elev IS 'Column prior_elev.';


--
-- Name: COLUMN vw_elevation_neighbors.next_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.next_elev IS 'Column next_elev.';


--
-- Name: COLUMN vw_elevation_neighbors.inc_delta; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.inc_delta IS 'Column inc_delta.';


--
-- Name: COLUMN vw_elevation_neighbors.out_delta; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.out_delta IS 'Column out_delta.';


--
-- Name: COLUMN vw_elevation_neighbors.valid_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.valid_point IS 'Column valid_point.';


--
-- Name: COLUMN vw_elevation_neighbors.extreme_point; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_neighbors.extreme_point IS 'Column extreme_point.';


--
-- Name: vw_elevation_backward; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_backward AS
 SELECT meter,
    curr_elev,
        CASE
            WHEN (next_elev > curr_elev) THEN (next_elev - (2)::numeric)
            ELSE (next_elev + (2)::numeric)
        END AS new_elev
   FROM staging.vw_elevation_neighbors s1
  WHERE (valid_point = false);


--
-- Name: COLUMN vw_elevation_backward.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_backward.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_backward.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_backward.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_backward.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_backward.new_elev IS 'Column new_elev.';


--
-- Name: vw_elevation_bumps; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_bumps AS
 SELECT s1.meter,
    s1.curr_elev,
    avg(s2.curr_elev) AS new_elev
   FROM (staging.vw_elevation_neighbors s1
     JOIN staging.vw_elevation_neighbors s2 ON ((s2.valid_point AND (abs((s2.meter - s1.meter)) < 500))))
  WHERE s1.extreme_point
  GROUP BY s1.meter, s1.curr_elev;


--
-- Name: COLUMN vw_elevation_bumps.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_bumps.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_bumps.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_bumps.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_bumps.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_bumps.new_elev IS 'Column new_elev.';


--
-- Name: vw_elevation_forward; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_forward AS
 SELECT meter,
    curr_elev,
        CASE
            WHEN (prior_elev > curr_elev) THEN (prior_elev - 0.35)
            ELSE (next_elev + 0.35)
        END AS new_elev
   FROM staging.vw_elevation_neighbors s1
  WHERE (valid_point = false);


--
-- Name: COLUMN vw_elevation_forward.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_forward.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_forward.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_forward.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_forward.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_forward.new_elev IS 'Column new_elev.';


--
-- Name: vw_elevation_median; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_median AS
 SELECT s1.meter,
    s1.curr_elev,
    percentile_disc((0.5)::double precision) WITHIN GROUP (ORDER BY s2.curr_elev) AS new_elev
   FROM (staging.vw_elevation_neighbors s1
     JOIN staging.vw_elevation_neighbors s2 ON ((s2.valid_point AND (abs((s1.meter - s2.meter)) <= 45))))
  WHERE (s1.valid_point = false)
  GROUP BY s1.meter, s1.curr_elev;


--
-- Name: COLUMN vw_elevation_median.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_median.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_median.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_median.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_median.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_median.new_elev IS 'Column new_elev.';


--
-- Name: vw_elevation_medians; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_medians AS
 WITH neighbors AS (
         SELECT temp_elevation_smoothing_m.meter,
            temp_elevation_smoothing_m.elevation_m_smooth AS curr_elev,
            COALESCE(lag(temp_elevation_smoothing_m.elevation_m_smooth, 1) OVER w, temp_elevation_smoothing_m.elevation_m_smooth) AS prior_elev,
            COALESCE(lead(temp_elevation_smoothing_m.elevation_m_smooth, 1) OVER w, temp_elevation_smoothing_m.elevation_m_smooth) AS next_elev
           FROM staging.temp_elevation_smoothing_m
          WINDOW w AS (ORDER BY temp_elevation_smoothing_m.meter)
        ), altered AS (
         SELECT neighbors.meter,
            neighbors.curr_elev,
            ((abs((neighbors.curr_elev - neighbors.prior_elev)) < (2)::numeric) AND (abs((neighbors.next_elev - neighbors.curr_elev)) < (2)::numeric)) AS valid_point
           FROM neighbors
        ), wa AS (
         SELECT sub_1.meter,
                CASE
                    WHEN sub_1.valid_point THEN sub_1.curr_elev
                    ELSE sub_1.weighted_elev
                END AS curr_elev
           FROM ( SELECT sub_2.meter,
                    sub_2.curr_elev,
                    sub_2.valid_point,
                    (sum((sub_2.joined_elev * (sub_2.weight)::numeric)) / (sum(sub_2.weight))::numeric) AS weighted_elev
                   FROM ( SELECT s1.meter,
                            s1.valid_point,
                            s1.curr_elev,
                            s2.curr_elev AS joined_elev,
                            (30 - abs((s1.meter - s2.meter))) AS weight
                           FROM (altered s1
                             JOIN altered s2 ON ((s2.valid_point AND (abs((s1.meter - s2.meter)) <= 150))))) sub_2
                  GROUP BY sub_2.meter, sub_2.curr_elev, sub_2.valid_point) sub_1
        ), wa_neighbor AS (
         SELECT wa.meter,
            wa.curr_elev,
            COALESCE(lag(wa.curr_elev, 1) OVER w, (1)::numeric) AS prior_elev,
            COALESCE(lead(wa.curr_elev, 1) OVER w, (1)::numeric) AS next_elev
           FROM wa
          WINDOW w AS (ORDER BY wa.meter)
        ), valid_pass_2 AS (
         SELECT wa_neighbor.meter,
            wa_neighbor.curr_elev,
            ((abs((wa_neighbor.curr_elev - wa_neighbor.prior_elev)) < (2)::numeric) AND (abs((wa_neighbor.next_elev - wa_neighbor.curr_elev)) < (2)::numeric)) AS valid_point
           FROM wa_neighbor
        )
 SELECT meter,
        CASE
            WHEN valid_point THEN curr_elev
            ELSE median_elev
        END AS curr_elev
   FROM ( SELECT s1.meter,
            s1.valid_point,
            s1.curr_elev,
            percentile_disc((0.5)::double precision) WITHIN GROUP (ORDER BY s2.curr_elev) AS median_elev
           FROM (valid_pass_2 s1
             JOIN valid_pass_2 s2 ON ((s2.valid_point AND (abs((s1.meter - s2.meter)) <= 30))))
          GROUP BY s1.meter, s1.curr_elev, s1.valid_point) sub
  ORDER BY meter;


--
-- Name: COLUMN vw_elevation_medians.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_medians.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_medians.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_medians.curr_elev IS 'Column curr_elev.';


--
-- Name: vw_elevation_weighted_avg; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_elevation_weighted_avg AS
 SELECT s1.meter,
    s1.curr_elev,
    ((sum(((s2.curr_elev)::double precision * power(((45 - abs((s1.meter - s2.meter))))::double precision, (2)::double precision))) / sum(power(((45 - abs((s1.meter - s2.meter))))::double precision, (2)::double precision))))::numeric AS new_elev
   FROM (staging.vw_elevation_neighbors s1
     JOIN staging.vw_elevation_neighbors s2 ON ((s2.valid_point AND (abs((s1.meter - s2.meter)) <= 50))))
  GROUP BY s1.meter, s1.curr_elev;


--
-- Name: COLUMN vw_elevation_weighted_avg.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_weighted_avg.meter IS 'Column meter.';


--
-- Name: COLUMN vw_elevation_weighted_avg.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_weighted_avg.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_elevation_weighted_avg.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_elevation_weighted_avg.new_elev IS 'Column new_elev.';


--
-- Name: vw_large_pull; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_large_pull AS
 WITH bad_points AS (
         SELECT s1.meter,
            s1.curr_elev
           FROM staging.vw_elevation_neighbors s1
          WHERE (s1.valid_point = false)
        )
 SELECT bp.meter,
    bp.curr_elev,
    avg((((n.prior_elev + n.next_elev) + n.curr_elev) / (3)::numeric)) AS new_elev
   FROM (staging.vw_elevation_neighbors n
     JOIN bad_points bp ON ((((n.meter - bp.meter) >= 1) AND ((n.meter - bp.meter) <= 10))))
  GROUP BY bp.meter, bp.curr_elev;


--
-- Name: COLUMN vw_large_pull.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_large_pull.meter IS 'Column meter.';


--
-- Name: COLUMN vw_large_pull.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_large_pull.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_large_pull.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_large_pull.new_elev IS 'Column new_elev.';


--
-- Name: vw_listening_history_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_listening_history_inserts AS
 SELECT DISTINCT played_at_utc,
    incoming_isrc AS isrc,
        CASE
            WHEN (context_type = 'playlist'::text) THEN split_part(context_uri, 'spotify:playlist:'::text, 2)
            ELSE NULL::text
        END AS playlist_id
   FROM staging.stg_listen_history
  ORDER BY played_at_utc;


--
-- Name: COLUMN vw_listening_history_inserts.played_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_listening_history_inserts.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN vw_listening_history_inserts.isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_listening_history_inserts.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_listening_history_inserts.playlist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_listening_history_inserts.playlist_id IS 'Column playlist_id.';


--
-- Name: vw_now_playing_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_now_playing_inserts AS
 SELECT DISTINCT played_at_utc,
    incoming_isrc AS isrc,
        CASE
            WHEN (context_type = 'playlist'::text) THEN split_part(context_uri, 'spotify:playlist:'::text, 2)
            ELSE NULL::text
        END AS playlist_id
   FROM staging.stg_now_playing
  ORDER BY played_at_utc;


--
-- Name: COLUMN vw_now_playing_inserts.played_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_now_playing_inserts.played_at_utc IS 'Column played_at_utc.';


--
-- Name: COLUMN vw_now_playing_inserts.isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_now_playing_inserts.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_now_playing_inserts.playlist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_now_playing_inserts.playlist_id IS 'Column playlist_id.';


--
-- Name: vw_playlist_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_playlist_inserts AS
 WITH t1 AS (
         SELECT d.playlist_id,
            COALESCE(s.mapped_isrc, d.incoming_isrc) AS isrc,
            d.track_order,
            row_number() OVER (PARTITION BY d.playlist_id, COALESCE(s.mapped_isrc, d.incoming_isrc) ORDER BY d.track_order) AS duplicate_row
           FROM (staging.stg_playlist_details d
             LEFT JOIN music.isrc_swaps s ON ((s.original_isrc = d.incoming_isrc)))
        )
 SELECT playlist_id,
    isrc,
    row_number() OVER (PARTITION BY playlist_id ORDER BY t1.track_order) AS track_order
   FROM t1
  WHERE (duplicate_row = 1);


--
-- Name: COLUMN vw_playlist_inserts.playlist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_playlist_inserts.playlist_id IS 'Column playlist_id.';


--
-- Name: COLUMN vw_playlist_inserts.isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_playlist_inserts.isrc IS 'Column isrc.';


--
-- Name: COLUMN vw_playlist_inserts.track_order; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_playlist_inserts.track_order IS 'Column track_order.';


--
-- Name: vw_small_pull; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_small_pull AS
 WITH bad_points AS (
         SELECT s1.meter,
            s1.curr_elev
           FROM staging.vw_elevation_neighbors s1
          WHERE (s1.valid_point = false)
        )
 SELECT bp.meter,
    bp.curr_elev,
    avg((((n.prior_elev + n.next_elev) + n.curr_elev) / (3)::numeric)) AS new_elev
   FROM (staging.vw_elevation_neighbors n
     JOIN bad_points bp ON ((((bp.meter - n.meter) >= 1) AND ((bp.meter - n.meter) <= 10))))
  GROUP BY bp.meter, bp.curr_elev;


--
-- Name: COLUMN vw_small_pull.meter; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_small_pull.meter IS 'Column meter.';


--
-- Name: COLUMN vw_small_pull.curr_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_small_pull.curr_elev IS 'Column curr_elev.';


--
-- Name: COLUMN vw_small_pull.new_elev; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_small_pull.new_elev IS 'Column new_elev.';


--
-- Name: vw_track_inserts; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_track_inserts AS
 WITH base AS (
         SELECT DISTINCT stg_playlist_details.track_id,
            stg_playlist_details.track_name,
            stg_playlist_details.incoming_isrc,
            stg_playlist_details.artist_id,
            stg_playlist_details.artist_name,
            stg_playlist_details.album_id,
            stg_playlist_details.track_release,
            stg_playlist_details.track_release_precision,
            stg_playlist_details.track_markets,
            stg_playlist_details.popularity,
            stg_playlist_details.duration_ms,
            stg_playlist_details.fetched_at_utc
           FROM staging.stg_playlist_details
        ), r1 AS (
         SELECT base.track_id,
            base.track_name,
            base.incoming_isrc,
            base.artist_id,
            base.artist_name,
            base.album_id,
            base.track_release,
            base.track_release_precision,
            base.track_markets,
            base.popularity,
            base.duration_ms,
            base.fetched_at_utc,
            row_number() OVER (PARTITION BY base.track_id ORDER BY base.fetched_at_utc DESC) AS duplicate_finder
           FROM base
        ), cleaned AS (
         SELECT r1.track_id,
            r1.track_name,
            public.clean_track_display(r1.track_name) AS track_name_clean,
            r1.incoming_isrc,
            r1.artist_id,
            public.clean_artist_display(r1.track_name, r1.artist_name) AS artist_display_name,
            r1.album_id,
            (
                CASE
                    WHEN (r1.track_release_precision = 'year'::text) THEN (r1.track_release || '-01-01'::text)
                    WHEN (r1.track_release_precision = 'month'::text) THEN (r1.track_release || '-01'::text)
                    ELSE r1.track_release
                END)::date AS release_date,
            (r1.track_markets ~~ '%US%'::text) AS available_in_us,
            COALESCE(cardinality((r1.track_markets)::text[]), 0) AS total_markets,
            r1.popularity,
            r1.duration_ms,
            r1.fetched_at_utc
           FROM r1
          WHERE (r1.duplicate_finder = 1)
        )
 SELECT track_id,
    track_name,
    track_name_clean,
    public.clean_track_match(track_name_clean) AS track_name_search,
    incoming_isrc AS track_isrc,
    artist_id,
    artist_display_name,
    album_id,
    release_date,
    available_in_us,
    total_markets,
    popularity,
    duration_ms,
    fetched_at_utc AS updated_at_utc
   FROM cleaned;


--
-- Name: COLUMN vw_track_inserts.track_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_track_inserts.track_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.track_name IS 'Column track_name.';


--
-- Name: COLUMN vw_track_inserts.track_name_clean; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN vw_track_inserts.track_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.track_name_search IS 'Column track_name_search.';


--
-- Name: COLUMN vw_track_inserts.track_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.track_isrc IS 'Column track_isrc.';


--
-- Name: COLUMN vw_track_inserts.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN vw_track_inserts.artist_display_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.artist_display_name IS 'Column artist_display_name.';


--
-- Name: COLUMN vw_track_inserts.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.album_id IS 'Column album_id.';


--
-- Name: COLUMN vw_track_inserts.release_date; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.release_date IS 'Column release_date.';


--
-- Name: COLUMN vw_track_inserts.available_in_us; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.available_in_us IS 'Column available_in_us.';


--
-- Name: COLUMN vw_track_inserts.total_markets; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.total_markets IS 'Column total_markets.';


--
-- Name: COLUMN vw_track_inserts.popularity; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.popularity IS 'Column popularity.';


--
-- Name: COLUMN vw_track_inserts.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN vw_track_inserts.updated_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: vw_track_inserts_from_listen_history; Type: VIEW; Schema: staging; Owner: -
--

CREATE VIEW staging.vw_track_inserts_from_listen_history AS
 WITH base AS (
         SELECT DISTINCT stg_listen_history.track_id,
            stg_listen_history.track_name,
            stg_listen_history.incoming_isrc,
            stg_listen_history.artist_id,
            stg_listen_history.artist_name,
            stg_listen_history.album_id,
            stg_listen_history.track_release,
            stg_listen_history.track_release_precision,
            stg_listen_history.available_market_count AS total_markets,
            stg_listen_history.available_in_usa AS available_in_us,
            stg_listen_history.popularity,
            stg_listen_history.duration_ms,
            stg_listen_history.played_at_utc AS fetched_at_utc
           FROM staging.stg_listen_history
        ), r1 AS (
         SELECT base.track_id,
            base.track_name,
            base.incoming_isrc,
            base.artist_id,
            base.artist_name,
            base.album_id,
            base.track_release,
            base.track_release_precision,
            base.total_markets,
            base.available_in_us,
            base.popularity,
            base.duration_ms,
            base.fetched_at_utc,
            row_number() OVER (PARTITION BY base.track_id ORDER BY base.fetched_at_utc DESC) AS duplicate_finder
           FROM base
        ), cleaned AS (
         SELECT r1.track_id,
            r1.track_name,
            public.clean_track_display(r1.track_name) AS track_name_clean,
            r1.incoming_isrc,
            r1.artist_id,
            public.clean_artist_display(r1.track_name, r1.artist_name) AS artist_display_name,
            r1.album_id,
            (
                CASE
                    WHEN (r1.track_release_precision = 'year'::text) THEN (r1.track_release || '-01-01'::text)
                    WHEN (r1.track_release_precision = 'month'::text) THEN (r1.track_release || '-01'::text)
                    ELSE r1.track_release
                END)::date AS release_date,
            r1.available_in_us,
            r1.total_markets,
            r1.popularity,
            r1.duration_ms,
            r1.fetched_at_utc
           FROM r1
          WHERE (r1.duplicate_finder = 1)
        )
 SELECT track_id,
    track_name,
    track_name_clean,
    public.clean_track_match(track_name_clean) AS track_name_search,
    incoming_isrc AS track_isrc,
    artist_id,
    artist_display_name,
    album_id,
    release_date,
    available_in_us,
    total_markets,
    popularity,
    duration_ms,
    fetched_at_utc AS updated_at_utc
   FROM cleaned;


--
-- Name: COLUMN vw_track_inserts_from_listen_history.track_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.track_id IS 'Column track_id.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.track_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.track_name IS 'Column track_name.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.track_name_clean; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.track_name_clean IS 'Column track_name_clean.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.track_name_search; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.track_name_search IS 'Column track_name_search.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.track_isrc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.track_isrc IS 'Column track_isrc.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.artist_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.artist_id IS 'Column artist_id.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.artist_display_name; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.artist_display_name IS 'Column artist_display_name.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.album_id; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.album_id IS 'Column album_id.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.release_date; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.release_date IS 'Column release_date.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.available_in_us; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.available_in_us IS 'Column available_in_us.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.total_markets; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.total_markets IS 'Column total_markets.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.popularity; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.popularity IS 'Column popularity.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.duration_ms; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.duration_ms IS 'Column duration_ms.';


--
-- Name: COLUMN vw_track_inserts_from_listen_history.updated_at_utc; Type: COMMENT; Schema: staging; Owner: -
--

COMMENT ON COLUMN staging.vw_track_inserts_from_listen_history.updated_at_utc IS 'Column updated_at_utc.';


--
-- Name: acceptable_failures; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.acceptable_failures (
    acceptable_failures smallint
);


--
-- Name: TABLE acceptable_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.acceptable_failures IS 'Table acceptable_failures.';


--
-- Name: COLUMN acceptable_failures.acceptable_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.acceptable_failures.acceptable_failures IS 'Column acceptable_failures.';


--
-- Name: fact_configuration; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.fact_configuration (
    fact_id integer NOT NULL,
    staging_id integer,
    task_id integer,
    fact_name text,
    data_type text,
    extraction_sql text,
    infer_values boolean DEFAULT false,
    forecast_values boolean DEFAULT false,
    is_unique_constraint boolean DEFAULT false,
    interpolation_ts boolean DEFAULT false,
    interpolation_destination_table text,
    interpolate_values boolean DEFAULT false,
    value_recency timestamp with time zone
);


--
-- Name: TABLE fact_configuration; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.fact_configuration IS 'Table fact_configuration.';


--
-- Name: COLUMN fact_configuration.fact_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.fact_id IS 'Column fact_id.';


--
-- Name: COLUMN fact_configuration.staging_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.staging_id IS 'Column staging_id.';


--
-- Name: COLUMN fact_configuration.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.task_id IS 'Column task_id.';


--
-- Name: COLUMN fact_configuration.fact_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.fact_name IS 'Column fact_name.';


--
-- Name: COLUMN fact_configuration.data_type; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.data_type IS 'Column data_type.';


--
-- Name: COLUMN fact_configuration.extraction_sql; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.extraction_sql IS 'Column extraction_sql.';


--
-- Name: COLUMN fact_configuration.infer_values; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.infer_values IS 'Column infer_values.';


--
-- Name: COLUMN fact_configuration.forecast_values; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.forecast_values IS 'Column forecast_values.';


--
-- Name: COLUMN fact_configuration.is_unique_constraint; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.is_unique_constraint IS 'Column is_unique_constraint.';


--
-- Name: COLUMN fact_configuration.interpolation_ts; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.interpolation_ts IS 'Column interpolation_ts.';


--
-- Name: COLUMN fact_configuration.interpolation_destination_table; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.interpolation_destination_table IS 'Column interpolation_destination_table.';


--
-- Name: COLUMN fact_configuration.interpolate_values; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.interpolate_values IS 'Column interpolate_values.';


--
-- Name: COLUMN fact_configuration.value_recency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.fact_configuration.value_recency IS 'Column value_recency.';


--
-- Name: fact_configuration_fact_id_seq; Type: SEQUENCE; Schema: tasks; Owner: -
--

CREATE SEQUENCE tasks.fact_configuration_fact_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_configuration_fact_id_seq; Type: SEQUENCE OWNED BY; Schema: tasks; Owner: -
--

ALTER SEQUENCE tasks.fact_configuration_fact_id_seq OWNED BY tasks.fact_configuration.fact_id;


--
-- Name: staging_configuration; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.staging_configuration (
    staging_id integer NOT NULL,
    task_id integer,
    staging_name text,
    staging_description text,
    destination_table text,
    cross_join_condition text,
    filter_condition text,
    source_table text DEFAULT 'staging.api_imports'::text
);


--
-- Name: TABLE staging_configuration; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.staging_configuration IS 'Table staging_configuration.';


--
-- Name: COLUMN staging_configuration.staging_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.staging_id IS 'Column staging_id.';


--
-- Name: COLUMN staging_configuration.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.task_id IS 'Column task_id.';


--
-- Name: COLUMN staging_configuration.staging_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.staging_name IS 'Column staging_name.';


--
-- Name: COLUMN staging_configuration.staging_description; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.staging_description IS 'Column staging_description.';


--
-- Name: COLUMN staging_configuration.destination_table; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.destination_table IS 'Column destination_table.';


--
-- Name: COLUMN staging_configuration.cross_join_condition; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.cross_join_condition IS 'Column cross_join_condition.';


--
-- Name: COLUMN staging_configuration.filter_condition; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.filter_condition IS 'Column filter_condition.';


--
-- Name: COLUMN staging_configuration.source_table; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.staging_configuration.source_table IS 'Column source_table.';


--
-- Name: staging_configuration_staging_id_seq; Type: SEQUENCE; Schema: tasks; Owner: -
--

CREATE SEQUENCE tasks.staging_configuration_staging_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: staging_configuration_staging_id_seq; Type: SEQUENCE OWNED BY; Schema: tasks; Owner: -
--

ALTER SEQUENCE tasks.staging_configuration_staging_id_seq OWNED BY tasks.staging_configuration.staging_id;


--
-- Name: task_config; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.task_config (
    task_name text NOT NULL,
    task_description text,
    task_priority integer DEFAULT 999,
    task_frequency text DEFAULT 'daily'::text,
    task_interval numeric(3,1) DEFAULT 23,
    api_function text,
    api_service_name text,
    api_loop_type text,
    api_post_processing text,
    python_function text,
    last_calendar_field text,
    updated_through_date date,
    api_parameters text
);


--
-- Name: TABLE task_config; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.task_config IS 'Table task_config.';


--
-- Name: COLUMN task_config.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.task_name IS 'Column task_name.';


--
-- Name: COLUMN task_config.task_description; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.task_description IS 'Column task_description.';


--
-- Name: COLUMN task_config.task_priority; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.task_priority IS 'Column task_priority.';


--
-- Name: COLUMN task_config.task_frequency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.task_frequency IS 'Column task_frequency.';


--
-- Name: COLUMN task_config.task_interval; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.task_interval IS 'Column task_interval.';


--
-- Name: COLUMN task_config.api_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.api_function IS 'Column api_function.';


--
-- Name: COLUMN task_config.api_service_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN task_config.api_loop_type; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.api_loop_type IS 'Column api_loop_type.';


--
-- Name: COLUMN task_config.api_post_processing; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.api_post_processing IS 'Column api_post_processing.';


--
-- Name: COLUMN task_config.python_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.python_function IS 'Column python_function.';


--
-- Name: COLUMN task_config.last_calendar_field; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.last_calendar_field IS 'Column last_calendar_field.';


--
-- Name: COLUMN task_config.updated_through_date; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.updated_through_date IS 'Column updated_through_date.';


--
-- Name: COLUMN task_config.api_parameters; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_config.api_parameters IS 'Column api_parameters.';


--
-- Name: task_configuration; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.task_configuration (
    task_id integer NOT NULL,
    task_name text DEFAULT 'placeholder'::text,
    last_executed_utc timestamp with time zone DEFAULT '1980-07-18 07:00:00+00'::timestamp with time zone,
    last_succeeded_utc timestamp with time zone DEFAULT '1980-07-18 07:00:00+00'::timestamp with time zone,
    last_failed_utc timestamp with time zone,
    next_planned_execution_utc timestamp with time zone DEFAULT '1980-07-18 07:00:00+00'::timestamp with time zone,
    last_failure_message text,
    display_icon text DEFAULT 'task'::text,
    task_description text,
    task_priority integer DEFAULT 999,
    task_frequency text DEFAULT 'Inactive'::text,
    task_start_hour integer DEFAULT 6,
    task_stop_hour integer DEFAULT 22,
    task_interval integer DEFAULT 8,
    api_service_name text DEFAULT 'N/A'::text,
    api_function_name text DEFAULT 'N/A'::text,
    python_execution_function text,
    consecutive_failures smallint DEFAULT 0
);


--
-- Name: TABLE task_configuration; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.task_configuration IS 'Table task_configuration.';


--
-- Name: COLUMN task_configuration.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_id IS 'Column task_id.';


--
-- Name: COLUMN task_configuration.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_name IS 'Column task_name.';


--
-- Name: COLUMN task_configuration.last_executed_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.last_executed_utc IS 'Column last_executed_utc.';


--
-- Name: COLUMN task_configuration.last_succeeded_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.last_succeeded_utc IS 'Column last_succeeded_utc.';


--
-- Name: COLUMN task_configuration.last_failed_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.last_failed_utc IS 'Column last_failed_utc.';


--
-- Name: COLUMN task_configuration.next_planned_execution_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.next_planned_execution_utc IS 'Column next_planned_execution_utc.';


--
-- Name: COLUMN task_configuration.last_failure_message; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.last_failure_message IS 'Column last_failure_message.';


--
-- Name: COLUMN task_configuration.display_icon; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.display_icon IS 'Column display_icon.';


--
-- Name: COLUMN task_configuration.task_description; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_description IS 'Column task_description.';


--
-- Name: COLUMN task_configuration.task_priority; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_priority IS 'Column task_priority.';


--
-- Name: COLUMN task_configuration.task_frequency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_frequency IS 'Column task_frequency.';


--
-- Name: COLUMN task_configuration.task_start_hour; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_start_hour IS 'Column task_start_hour.';


--
-- Name: COLUMN task_configuration.task_stop_hour; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_stop_hour IS 'Column task_stop_hour.';


--
-- Name: COLUMN task_configuration.task_interval; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.task_interval IS 'Column task_interval.';


--
-- Name: COLUMN task_configuration.api_service_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN task_configuration.api_function_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.api_function_name IS 'Column api_function_name.';


--
-- Name: COLUMN task_configuration.python_execution_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.python_execution_function IS 'Column python_execution_function.';


--
-- Name: COLUMN task_configuration.consecutive_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_configuration.consecutive_failures IS 'Column consecutive_failures.';


--
-- Name: task_configuration_task_id_seq; Type: SEQUENCE; Schema: tasks; Owner: -
--

CREATE SEQUENCE tasks.task_configuration_task_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_configuration_task_id_seq; Type: SEQUENCE OWNED BY; Schema: tasks; Owner: -
--

ALTER SEQUENCE tasks.task_configuration_task_id_seq OWNED BY tasks.task_configuration.task_id;


--
-- Name: task_executions; Type: TABLE; Schema: tasks; Owner: -
--

CREATE TABLE tasks.task_executions (
    execution_id integer NOT NULL,
    task_id integer NOT NULL,
    task_name character varying(255) NOT NULL,
    status character varying(50) DEFAULT 'running'::character varying,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone,
    result jsonb,
    error_message text
);


--
-- Name: TABLE task_executions; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON TABLE tasks.task_executions IS 'Table task_executions.';


--
-- Name: COLUMN task_executions.execution_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.execution_id IS 'Column execution_id.';


--
-- Name: COLUMN task_executions.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.task_id IS 'Column task_id.';


--
-- Name: COLUMN task_executions.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.task_name IS 'Column task_name.';


--
-- Name: COLUMN task_executions.status; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.status IS 'Column status.';


--
-- Name: COLUMN task_executions.started_at; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.started_at IS 'Column started_at.';


--
-- Name: COLUMN task_executions.completed_at; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.completed_at IS 'Column completed_at.';


--
-- Name: COLUMN task_executions.result; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.result IS 'Column result.';


--
-- Name: COLUMN task_executions.error_message; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.task_executions.error_message IS 'Column error_message.';


--
-- Name: task_executions_execution_id_seq; Type: SEQUENCE; Schema: tasks; Owner: -
--

CREATE SEQUENCE tasks.task_executions_execution_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_executions_execution_id_seq; Type: SEQUENCE OWNED BY; Schema: tasks; Owner: -
--

ALTER SEQUENCE tasks.task_executions_execution_id_seq OWNED BY tasks.task_executions.execution_id;


--
-- Name: vw_task_execution; Type: VIEW; Schema: tasks; Owner: -
--

CREATE VIEW tasks.vw_task_execution AS
 WITH ranked_logs AS (
         SELECT task_executions.task_name,
            task_executions.event_time_utc,
            public.utc_now() AS curr_ts,
            row_number() OVER (PARTITION BY task_executions.task_name ORDER BY task_executions.event_time_utc DESC) AS row_order,
            count(task_executions.event_time_utc) OVER (PARTITION BY task_executions.task_name) AS total_attempts,
            max(task_executions.event_time_utc) OVER (PARTITION BY task_executions.task_name) AS last_execution,
            max(
                CASE
                    WHEN (task_executions.failure_type IS NULL) THEN task_executions.event_time_utc
                    ELSE NULL::timestamp with time zone
                END) OVER (PARTITION BY task_executions.task_name) AS last_success,
            max(
                CASE
                    WHEN (task_executions.failure_type IS NOT NULL) THEN task_executions.event_time_utc
                    ELSE NULL::timestamp with time zone
                END) OVER (PARTITION BY task_executions.task_name) AS last_failure,
            task_executions.failure_type,
                CASE
                    WHEN (task_executions.failure_type IS NULL) THEN 1
                    ELSE 0
                END AS success_count,
                CASE
                    WHEN (task_executions.failure_type IS NOT NULL) THEN 1
                    ELSE 0
                END AS failure_count
           FROM logging.task_executions
        ), streak_counter AS (
         SELECT ranked_logs.task_name,
            ranked_logs.row_order,
            ranked_logs.curr_ts,
            ranked_logs.total_attempts,
            ranked_logs.last_execution,
            ranked_logs.last_success,
            ranked_logs.last_failure,
            ranked_logs.success_count,
            ranked_logs.failure_count,
            sum(
                CASE
                    WHEN ((ranked_logs.last_success > ranked_logs.last_failure) AND (ranked_logs.event_time_utc > ranked_logs.last_failure)) THEN ranked_logs.success_count
                    ELSE 0
                END) OVER (PARTITION BY ranked_logs.task_name) AS streak_success,
            sum(
                CASE
                    WHEN ((ranked_logs.last_failure > ranked_logs.last_success) AND (ranked_logs.event_time_utc > ranked_logs.last_success)) THEN ranked_logs.failure_count
                    ELSE 0
                END) OVER (PARTITION BY ranked_logs.task_name) AS streak_failure
           FROM ranked_logs
        ), task_metas AS (
         SELECT streak_counter.task_name,
            streak_counter.total_attempts,
            streak_counter.last_success,
            streak_counter.last_execution,
            EXTRACT(minutes FROM age(streak_counter.curr_ts, streak_counter.last_execution)) AS execution_delta_m,
            EXTRACT(minutes FROM age(streak_counter.curr_ts, streak_counter.last_success)) AS success_delta_m,
            EXTRACT(minutes FROM age(streak_counter.curr_ts, streak_counter.last_failure)) AS failure_delta_m,
                CASE
                    WHEN (streak_counter.last_execution IS NULL) THEN (0)::bigint
                    ELSE COALESCE(streak_counter.streak_success, (1)::bigint)
                END AS consecutive_successes,
                CASE
                    WHEN (streak_counter.last_execution IS NULL) THEN (0)::bigint
                    ELSE COALESCE(streak_counter.streak_failure, (1)::bigint)
                END AS consecutive_failures
           FROM streak_counter
          WHERE (streak_counter.row_order = 1)
        ), all_task_metas AS (
         SELECT t.task_name,
            t.task_description,
            t.task_frequency,
            t.task_interval,
            t.task_priority,
            t.api_function,
            t.api_service_name,
            t.api_loop_type,
            t.api_post_processing,
            t.api_parameters,
            sl.api_service_function,
            t.python_function,
            t.last_calendar_field,
                CASE
                    WHEN ((COALESCE(t.api_function, 'NULL'::text) <> ALL (ARRAY['Day'::text, 'Range'::text])) IS NULL) THEN public.utc_now()
                    ELSE COALESCE((((t.updated_through_date)::timestamp with time zone AT TIME ZONE 'UTC'::text))::timestamp with time zone, '2020-01-01 00:00:00+00'::timestamp with time zone)
                END AS updated_through_utc,
            COALESCE(tm.total_attempts, (0)::bigint) AS total_attempts,
            COALESCE(tm.last_success, '2020-01-01 00:00:00+00'::timestamp with time zone) AS last_success_utc,
            COALESCE(tm.last_execution, '2020-01-01 00:00:00+00'::timestamp with time zone) AS last_execution_utc,
            COALESCE(tm.consecutive_successes, (0)::bigint) AS consecutive_successes,
            COALESCE(tm.consecutive_failures, (0)::bigint) AS consecutive_failures
           FROM ((tasks.task_config t
             LEFT JOIN task_metas tm ON ((tm.task_name = t.task_name)))
             LEFT JOIN api_services.api_service_list sl ON ((sl.api_service_name = t.api_service_name)))
        ), final_query AS (
         SELECT all_task_metas.task_name,
            all_task_metas.task_description,
            all_task_metas.task_frequency,
            all_task_metas.task_interval,
            all_task_metas.task_priority,
            all_task_metas.api_function,
            all_task_metas.api_service_name,
            all_task_metas.api_loop_type,
            all_task_metas.api_post_processing,
            all_task_metas.api_parameters,
            all_task_metas.api_service_function,
            all_task_metas.python_function,
            all_task_metas.last_calendar_field,
            all_task_metas.total_attempts,
            all_task_metas.consecutive_successes,
            all_task_metas.consecutive_failures,
            all_task_metas.last_execution_utc,
            all_task_metas.last_success_utc,
            all_task_metas.updated_through_utc,
            public.requires_catchup(all_task_metas.updated_through_utc, all_task_metas.task_frequency, all_task_metas.api_loop_type) AS data_requires_catchup,
            public.calculate_next_execution_utc(all_task_metas.last_success_utc, all_task_metas.task_frequency, all_task_metas.task_interval) AS next_execution_utc,
            ((all_task_metas.task_frequency <> 'Retired'::text) AND (public.requires_catchup(all_task_metas.updated_through_utc, all_task_metas.task_frequency, all_task_metas.api_loop_type) OR (public.calculate_next_execution_utc(all_task_metas.last_success_utc, all_task_metas.task_frequency, all_task_metas.task_interval) < now()))) AS do_execute,
                CASE
                    WHEN ((EXTRACT(epoch FROM (public.utc_now() - all_task_metas.last_execution_utc)) / (60)::numeric) < (1)::numeric) THEN 'recency'::text
                    WHEN (all_task_metas.consecutive_failures > 5) THEN 'failures'::text
                    ELSE 'timing'::text
                END AS execution_logic
           FROM all_task_metas
        )
 SELECT task_name,
    task_description,
    task_frequency,
    task_interval,
    task_priority,
    api_function,
    api_service_name,
    api_loop_type,
    api_post_processing,
    api_parameters,
    api_service_function,
    python_function,
    last_calendar_field,
    total_attempts,
    consecutive_successes,
    consecutive_failures,
    last_execution_utc,
    last_success_utc,
    updated_through_utc,
    data_requires_catchup,
    next_execution_utc,
    do_execute,
    execution_logic
   FROM final_query fq
  ORDER BY COALESCE(next_execution_utc, (now() + ((7 || 'days'::text))::interval)), data_requires_catchup DESC, do_execute DESC, (task_frequency <> 'Retired'::text) DESC, task_frequency, task_priority, task_interval;


--
-- Name: COLUMN vw_task_execution.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.task_name IS 'Column task_name.';


--
-- Name: COLUMN vw_task_execution.task_description; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.task_description IS 'Column task_description.';


--
-- Name: COLUMN vw_task_execution.task_frequency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.task_frequency IS 'Column task_frequency.';


--
-- Name: COLUMN vw_task_execution.task_interval; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.task_interval IS 'Column task_interval.';


--
-- Name: COLUMN vw_task_execution.task_priority; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.task_priority IS 'Column task_priority.';


--
-- Name: COLUMN vw_task_execution.api_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_function IS 'Column api_function.';


--
-- Name: COLUMN vw_task_execution.api_service_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN vw_task_execution.api_loop_type; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_loop_type IS 'Column api_loop_type.';


--
-- Name: COLUMN vw_task_execution.api_post_processing; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_post_processing IS 'Column api_post_processing.';


--
-- Name: COLUMN vw_task_execution.api_parameters; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_parameters IS 'Column api_parameters.';


--
-- Name: COLUMN vw_task_execution.api_service_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.api_service_function IS 'Column api_service_function.';


--
-- Name: COLUMN vw_task_execution.python_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.python_function IS 'Column python_function.';


--
-- Name: COLUMN vw_task_execution.last_calendar_field; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.last_calendar_field IS 'Column last_calendar_field.';


--
-- Name: COLUMN vw_task_execution.total_attempts; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.total_attempts IS 'Column total_attempts.';


--
-- Name: COLUMN vw_task_execution.consecutive_successes; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.consecutive_successes IS 'Column consecutive_successes.';


--
-- Name: COLUMN vw_task_execution.consecutive_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.consecutive_failures IS 'Column consecutive_failures.';


--
-- Name: COLUMN vw_task_execution.last_execution_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.last_execution_utc IS 'Column last_execution_utc.';


--
-- Name: COLUMN vw_task_execution.last_success_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.last_success_utc IS 'Column last_success_utc.';


--
-- Name: COLUMN vw_task_execution.updated_through_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.updated_through_utc IS 'Column updated_through_utc.';


--
-- Name: COLUMN vw_task_execution.data_requires_catchup; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.data_requires_catchup IS 'Column data_requires_catchup.';


--
-- Name: COLUMN vw_task_execution.next_execution_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.next_execution_utc IS 'Column next_execution_utc.';


--
-- Name: COLUMN vw_task_execution.do_execute; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.do_execute IS 'Column do_execute.';


--
-- Name: COLUMN vw_task_execution.execution_logic; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_execution.execution_logic IS 'Column execution_logic.';


--
-- Name: vw_task_info; Type: VIEW; Schema: tasks; Owner: -
--

CREATE VIEW tasks.vw_task_info AS
 WITH svcs AS (
         SELECT fl.friendly_name,
            fl.api_service_name,
            fl.api_function_name,
            fl.loop_strategy,
            fl.iter_path_keys,
            fl.iter_query_keys,
            fl.static_path_params,
            fl.static_query_params,
            fl.python_extraction_function,
            fl.flatten_sproc,
            fl.value_recency,
            ((COALESCE(fl.value_recency, '2025-07-18 07:00:00+00'::timestamp with time zone))::date < ((CURRENT_TIMESTAMP - '1 day'::interval))::date) AS catchup_required,
            sl.api_service_function AS python_login_function,
            sl.api_credential_requirements,
            (COALESCE(sl.rate_limit_cleared_utc, CURRENT_TIMESTAMP) > CURRENT_TIMESTAMP) AS api_under_rate_limits
           FROM (api_services.function_library fl
             JOIN api_services.api_service_list sl ON ((sl.api_service_name = fl.api_service_name)))
        )
 SELECT DISTINCT t.task_id,
    t.task_name,
    t.task_frequency,
    t.task_interval,
    t.task_start_hour,
    t.task_stop_hour,
    COALESCE(t.next_planned_execution_utc, '1980-07-18 07:00:00+00'::timestamp with time zone) AS next_planned_execution_utc,
    ((((t.task_frequency = 'Hourly'::text) AND (EXTRACT(hour FROM CURRENT_TIMESTAMP) >= (t.task_start_hour)::numeric) AND (EXTRACT(hour FROM CURRENT_TIMESTAMP) <= (t.task_stop_hour)::numeric)) OR (t.task_frequency <> ALL (ARRAY['Inactive'::text, 'Hourly'::text]))) AND (COALESCE(t.next_planned_execution_utc, '1980-07-18 07:00:00+00'::timestamp with time zone) < CURRENT_TIMESTAMP) AND (COALESCE((t.consecutive_failures)::integer, 0) < ( SELECT acceptable_failures.acceptable_failures
           FROM tasks.acceptable_failures))) AS should_execute,
    COALESCE((t.consecutive_failures)::integer, 0) AS consecutive_failures,
    svcs.api_service_name,
    svcs.api_function_name,
    svcs.loop_strategy,
    svcs.iter_path_keys,
    svcs.iter_query_keys,
    svcs.static_path_params,
    svcs.static_query_params,
    svcs.friendly_name,
    (COALESCE(svcs.api_service_name, 'N/A'::text) <> ALL (ARRAY['None'::text, 'N/A'::text, ''::text])) AS run_extract,
    svcs.python_login_function,
    svcs.python_extraction_function,
    (((((svcs.flatten_sproc || '('::text) || t.task_id) || ', '''::text) || svcs.friendly_name) || ''')'::text) AS flatten_sproc,
    COALESCE(svcs.value_recency, '2025-07-18 07:00:00+00'::timestamp with time zone) AS value_recency,
    t.python_execution_function,
    (COALESCE(t.python_execution_function, 'N/A'::text) <> ALL (ARRAY['None'::text, 'N/A'::text, ''::text])) AS run_python,
    COALESCE(svcs.api_under_rate_limits, false) AS api_under_rate_limits
   FROM (tasks.task_configuration t
     LEFT JOIN svcs ON ((svcs.friendly_name = t.api_function_name)));


--
-- Name: COLUMN vw_task_info.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_id IS 'Column task_id.';


--
-- Name: COLUMN vw_task_info.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_name IS 'Column task_name.';


--
-- Name: COLUMN vw_task_info.task_frequency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_frequency IS 'Column task_frequency.';


--
-- Name: COLUMN vw_task_info.task_interval; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_interval IS 'Column task_interval.';


--
-- Name: COLUMN vw_task_info.task_start_hour; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_start_hour IS 'Column task_start_hour.';


--
-- Name: COLUMN vw_task_info.task_stop_hour; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.task_stop_hour IS 'Column task_stop_hour.';


--
-- Name: COLUMN vw_task_info.next_planned_execution_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.next_planned_execution_utc IS 'Column next_planned_execution_utc.';


--
-- Name: COLUMN vw_task_info.should_execute; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.should_execute IS 'Column should_execute.';


--
-- Name: COLUMN vw_task_info.consecutive_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.consecutive_failures IS 'Column consecutive_failures.';


--
-- Name: COLUMN vw_task_info.api_service_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.api_service_name IS 'Column api_service_name.';


--
-- Name: COLUMN vw_task_info.api_function_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.api_function_name IS 'Column api_function_name.';


--
-- Name: COLUMN vw_task_info.loop_strategy; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.loop_strategy IS 'Column loop_strategy.';


--
-- Name: COLUMN vw_task_info.iter_path_keys; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.iter_path_keys IS 'Column iter_path_keys.';


--
-- Name: COLUMN vw_task_info.iter_query_keys; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.iter_query_keys IS 'Column iter_query_keys.';


--
-- Name: COLUMN vw_task_info.static_path_params; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.static_path_params IS 'Column static_path_params.';


--
-- Name: COLUMN vw_task_info.static_query_params; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.static_query_params IS 'Column static_query_params.';


--
-- Name: COLUMN vw_task_info.friendly_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.friendly_name IS 'Column friendly_name.';


--
-- Name: COLUMN vw_task_info.run_extract; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.run_extract IS 'Column run_extract.';


--
-- Name: COLUMN vw_task_info.python_login_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.python_login_function IS 'Column python_login_function.';


--
-- Name: COLUMN vw_task_info.python_extraction_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.python_extraction_function IS 'Column python_extraction_function.';


--
-- Name: COLUMN vw_task_info.flatten_sproc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.flatten_sproc IS 'Column flatten_sproc.';


--
-- Name: COLUMN vw_task_info.value_recency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.value_recency IS 'Column value_recency.';


--
-- Name: COLUMN vw_task_info.python_execution_function; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.python_execution_function IS 'Column python_execution_function.';


--
-- Name: COLUMN vw_task_info.run_python; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.run_python IS 'Column run_python.';


--
-- Name: COLUMN vw_task_info.api_under_rate_limits; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_info.api_under_rate_limits IS 'Column api_under_rate_limits.';


--
-- Name: vw_task_performance; Type: VIEW; Schema: tasks; Owner: -
--

CREATE VIEW tasks.vw_task_performance AS
 WITH raw_app_timing AS (
         SELECT application_events.task_id,
            (application_events.event_time_utc)::date AS event_date,
                CASE
                    WHEN (application_events.data_event = 'Complete'::text) THEN application_events.event_time_utc
                    ELSE NULL::timestamp with time zone
                END AS complete_time_utc,
                CASE
                    WHEN (application_events.data_event = 'Login'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS login_ms,
                CASE
                    WHEN (application_events.data_event = 'Extract'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS extract_ms,
                CASE
                    WHEN (application_events.data_event = 'Load'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS load_ms,
                CASE
                    WHEN (application_events.data_event = 'Flatten'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS flatten_ms,
                CASE
                    WHEN (application_events.data_event = 'Parsing'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS parse_ms,
                CASE
                    WHEN (application_events.data_event = 'Interpolation'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS interpolation_ms,
                CASE
                    WHEN (application_events.data_event = 'Forecasting'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS forecasting_ms,
                CASE
                    WHEN (application_events.data_event = 'Python'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS python_ms,
                CASE
                    WHEN (application_events.data_event <> ALL (ARRAY['Login'::text, 'Extract'::text, 'Load'::text, 'Flatten'::text, 'Parsing'::text, 'Interpolation'::text, 'Forecasting'::text, 'Complete'::text])) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS admin_ms,
                CASE
                    WHEN (application_events.data_event = 'Complete'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS total_ms,
                CASE
                    WHEN (application_events.error_text IS NULL) THEN 0
                    ELSE 1
                END AS is_failure
           FROM logging.application_events
          WHERE ((application_events.task_id IS NOT NULL) AND (application_events.event_time_utc >= (CURRENT_TIMESTAMP - '28 days'::interval)))
        )
 SELECT task_id,
    event_date,
    avg(login_ms) AS login_ms,
    avg(extract_ms) AS extract_ms,
    avg(load_ms) AS load_ms,
    avg(flatten_ms) AS flatten_ms,
    avg(parse_ms) AS parse_ms,
    avg(interpolation_ms) AS interpolation_ms,
    avg(forecasting_ms) AS forecasting_ms,
    avg(python_ms) AS python_ms,
    avg(admin_ms) AS admin_ms,
    avg(total_ms) AS total_ms,
    (sum(is_failure) >= 1) AS is_failure
   FROM raw_app_timing
  GROUP BY task_id, event_date
  ORDER BY event_date DESC;


--
-- Name: COLUMN vw_task_performance.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.task_id IS 'Column task_id.';


--
-- Name: COLUMN vw_task_performance.event_date; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.event_date IS 'Column event_date.';


--
-- Name: COLUMN vw_task_performance.login_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.login_ms IS 'Column login_ms.';


--
-- Name: COLUMN vw_task_performance.extract_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.extract_ms IS 'Column extract_ms.';


--
-- Name: COLUMN vw_task_performance.load_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.load_ms IS 'Column load_ms.';


--
-- Name: COLUMN vw_task_performance.flatten_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.flatten_ms IS 'Column flatten_ms.';


--
-- Name: COLUMN vw_task_performance.parse_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.parse_ms IS 'Column parse_ms.';


--
-- Name: COLUMN vw_task_performance.interpolation_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.interpolation_ms IS 'Column interpolation_ms.';


--
-- Name: COLUMN vw_task_performance.forecasting_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.forecasting_ms IS 'Column forecasting_ms.';


--
-- Name: COLUMN vw_task_performance.python_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.python_ms IS 'Column python_ms.';


--
-- Name: COLUMN vw_task_performance.admin_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.admin_ms IS 'Column admin_ms.';


--
-- Name: COLUMN vw_task_performance.total_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.total_ms IS 'Column total_ms.';


--
-- Name: COLUMN vw_task_performance.is_failure; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_performance.is_failure IS 'Column is_failure.';


--
-- Name: vw_task_summary_chart; Type: VIEW; Schema: tasks; Owner: -
--

CREATE VIEW tasks.vw_task_summary_chart AS
 WITH te AS (
         SELECT tc.task_id,
            tc.task_frequency,
            tc.consecutive_failures,
            tc.task_name,
            tc.last_executed_utc,
            tc.last_failed_utc,
            tc.next_planned_execution_utc,
            fl.value_recency,
                CASE
                    WHEN (tc.last_failed_utc = tc.last_executed_utc) THEN ((tc.consecutive_failures || ' - '::text) || tc.last_failure_message)
                    ELSE NULL::text
                END AS last_failure_message,
            (tc.last_failed_utc = tc.last_executed_utc) AS is_active_failure
           FROM (tasks.task_configuration tc
             LEFT JOIN api_services.function_library fl ON ((tc.api_function_name = fl.friendly_name)))
        ), raw_executions AS (
         SELECT application_events.task_id,
            application_events.event_time_utc,
                CASE
                    WHEN (application_events.data_event = 'Complete'::text) THEN application_events.event_time_utc
                    ELSE NULL::timestamp with time zone
                END AS complete_time_utc,
                CASE
                    WHEN (application_events.data_event = 'Login'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS login_ms,
                CASE
                    WHEN (application_events.data_event = 'Extract'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS extract_ms,
                CASE
                    WHEN (application_events.data_event = 'Load'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS load_ms,
                CASE
                    WHEN (application_events.data_event = 'Flatten'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS flatten_ms,
                CASE
                    WHEN (application_events.data_event = 'Parsing'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS parse_ms,
                CASE
                    WHEN (application_events.data_event = 'Interpolation'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS interpolation_ms,
                CASE
                    WHEN (application_events.data_event = 'Forecasting'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS forecasting_ms,
                CASE
                    WHEN (application_events.data_event = 'Python'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS python_ms,
                CASE
                    WHEN (application_events.data_event <> ALL (ARRAY['Login'::text, 'Extract'::text, 'Load'::text, 'Flatten'::text, 'Parsing'::text, 'Interpolation'::text, 'Forecasting'::text, 'Complete'::text])) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS admin_ms,
                CASE
                    WHEN (application_events.data_event = 'Complete'::text) THEN application_events.execution_time_ms
                    ELSE NULL::integer
                END AS total_ms,
            (application_events.error_text IS NULL) AS is_success
           FROM logging.application_events
          WHERE ((application_events.task_id IS NOT NULL) AND (application_events.event_time_utc >= (CURRENT_TIMESTAMP - '21 days'::interval)))
        ), el7 AS (
         SELECT raw_executions.task_id,
            count(DISTINCT raw_executions.event_time_utc) AS execution_count,
            count(DISTINCT
                CASE
                    WHEN raw_executions.is_success THEN raw_executions.complete_time_utc
                    ELSE NULL::timestamp with time zone
                END) AS success_count,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.login_ms)::double precision)))::numeric, 2) AS login_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.extract_ms)::double precision)))::numeric, 2) AS extract_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.flatten_ms)::double precision)))::numeric, 2) AS flatten_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.parse_ms)::double precision)))::numeric, 2) AS parse_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.interpolation_ms)::double precision)))::numeric, 2) AS interpolation_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.forecasting_ms)::double precision)))::numeric, 2) AS forecasting_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.python_ms)::double precision)))::numeric, 2) AS admin_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.admin_ms)::double precision)))::numeric, 2) AS python_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.total_ms)::double precision)))::numeric, 2) AS total_ms
           FROM raw_executions
          WHERE (raw_executions.event_time_utc >= (CURRENT_TIMESTAMP - '7 days'::interval))
          GROUP BY raw_executions.task_id
        ), el14 AS (
         SELECT raw_executions.task_id,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.login_ms)::double precision)))::numeric, 2) AS login_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.extract_ms)::double precision)))::numeric, 2) AS extract_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.flatten_ms)::double precision)))::numeric, 2) AS flatten_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.parse_ms)::double precision)))::numeric, 2) AS parse_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.interpolation_ms)::double precision)))::numeric, 2) AS interpolation_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.forecasting_ms)::double precision)))::numeric, 2) AS forecasting_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.python_ms)::double precision)))::numeric, 2) AS python_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.admin_ms)::double precision)))::numeric, 2) AS admin_ms,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((raw_executions.total_ms)::double precision)))::numeric, 2) AS total_ms
           FROM raw_executions
          WHERE (raw_executions.event_time_utc < (CURRENT_TIMESTAMP - '7 days'::interval))
          GROUP BY raw_executions.task_id
        ), sd AS (
         SELECT raw_executions.task_id,
            (round(stddev((raw_executions.login_ms)::numeric), 0))::integer AS login_ms,
            (round(stddev((raw_executions.extract_ms)::numeric), 0))::integer AS extract_ms,
            (round(stddev((raw_executions.flatten_ms)::numeric), 0))::integer AS flatten_ms,
            (round(stddev((raw_executions.parse_ms)::numeric), 0))::integer AS parse_ms,
            (round(stddev((raw_executions.interpolation_ms)::numeric), 0))::integer AS interpolation_ms,
            (round(stddev((raw_executions.forecasting_ms)::numeric), 0))::integer AS forecasting_ms,
            (round(stddev((raw_executions.python_ms)::numeric), 0))::integer AS python_ms,
            (round(stddev((raw_executions.admin_ms)::numeric), 0))::integer AS admin_ms
           FROM raw_executions
          WHERE (raw_executions.event_time_utc < (CURRENT_TIMESTAMP - '7 days'::interval))
          GROUP BY raw_executions.task_id
        ), final_query AS (
         SELECT te.task_id,
            te.task_name,
            te.task_frequency,
            te.consecutive_failures,
            public.time_ago(te.last_executed_utc) AS time_ago_execution,
            public.time_ago(te.next_planned_execution_utc) AS time_ago_next,
            public.time_ago(te.value_recency) AS time_ago_value,
            round((EXTRACT(epoch FROM (te.last_executed_utc - CURRENT_TIMESTAMP)) / (60)::numeric), 1) AS execution_minutes_ago,
            round((EXTRACT(epoch FROM (te.value_recency - CURRENT_TIMESTAMP)) / (60)::numeric), 1) AS value_recency_minutes_ago,
            round((EXTRACT(epoch FROM (te.next_planned_execution_utc - CURRENT_TIMESTAMP)) / (60)::numeric), 1) AS next_planned_execution_minutes,
            te.value_recency,
            te.last_executed_utc,
            te.last_failed_utc,
            te.next_planned_execution_utc,
            "left"(te.last_failure_message, 30) AS last_failure_message,
            te.is_active_failure,
            el7.execution_count,
            el7.success_count,
            round((((el7.success_count)::numeric / (NULLIF(el7.execution_count, 0))::numeric) * (100)::numeric), 2) AS success_percentage,
            max(el7.execution_count) OVER () AS max_executions,
            el7.login_ms,
            el7.extract_ms,
            el7.flatten_ms,
            el7.parse_ms,
            el7.interpolation_ms,
            el7.forecasting_ms,
            el7.python_ms,
            el7.admin_ms,
            el7.total_ms,
            ((((((
                CASE
                    WHEN (el7.login_ms > (el14.login_ms + (sd.login_ms)::numeric)) THEN 'Login '::text
                    ELSE ''::text
                END ||
                CASE
                    WHEN (el7.extract_ms > (el14.extract_ms + (sd.extract_ms)::numeric)) THEN 'Extract '::text
                    ELSE ''::text
                END) ||
                CASE
                    WHEN (el7.flatten_ms > (el14.flatten_ms + (sd.flatten_ms)::numeric)) THEN 'Flatten '::text
                    ELSE ''::text
                END) ||
                CASE
                    WHEN (el7.parse_ms > (el14.parse_ms + (sd.parse_ms)::numeric)) THEN 'Parse '::text
                    ELSE ''::text
                END) ||
                CASE
                    WHEN (el7.interpolation_ms > (el14.interpolation_ms + (sd.interpolation_ms)::numeric)) THEN 'Interpolation '::text
                    ELSE ''::text
                END) ||
                CASE
                    WHEN (el7.forecasting_ms > (el14.forecasting_ms + (sd.forecasting_ms)::numeric)) THEN 'Forecasting '::text
                    ELSE ''::text
                END) ||
                CASE
                    WHEN (el7.admin_ms > (el14.admin_ms + (sd.admin_ms)::numeric)) THEN 'Login '::text
                    ELSE ''::text
                END) AS increasing_steps
           FROM (((te
             LEFT JOIN el7 ON ((el7.task_id = te.task_id)))
             LEFT JOIN el14 ON ((el14.task_id = te.task_id)))
             LEFT JOIN sd ON ((sd.task_id = te.task_id)))
        )
 SELECT task_id,
    task_name,
    task_frequency,
    consecutive_failures,
    time_ago_execution,
    time_ago_next,
    time_ago_value,
    execution_minutes_ago,
    value_recency_minutes_ago,
    next_planned_execution_minutes,
    value_recency,
    last_executed_utc,
    last_failed_utc,
    next_planned_execution_utc,
    last_failure_message,
    is_active_failure,
    execution_count,
    success_count,
    success_percentage,
    max_executions,
    login_ms,
    extract_ms,
    flatten_ms,
    parse_ms,
    interpolation_ms,
    forecasting_ms,
    python_ms,
    admin_ms,
    total_ms,
    increasing_steps
   FROM final_query
  ORDER BY is_active_failure DESC, next_planned_execution_utc, last_executed_utc DESC, last_failed_utc DESC;


--
-- Name: COLUMN vw_task_summary_chart.task_id; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.task_id IS 'Column task_id.';


--
-- Name: COLUMN vw_task_summary_chart.task_name; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.task_name IS 'Column task_name.';


--
-- Name: COLUMN vw_task_summary_chart.task_frequency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.task_frequency IS 'Column task_frequency.';


--
-- Name: COLUMN vw_task_summary_chart.consecutive_failures; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.consecutive_failures IS 'Column consecutive_failures.';


--
-- Name: COLUMN vw_task_summary_chart.time_ago_execution; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.time_ago_execution IS 'Column time_ago_execution.';


--
-- Name: COLUMN vw_task_summary_chart.time_ago_next; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.time_ago_next IS 'Column time_ago_next.';


--
-- Name: COLUMN vw_task_summary_chart.time_ago_value; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.time_ago_value IS 'Column time_ago_value.';


--
-- Name: COLUMN vw_task_summary_chart.execution_minutes_ago; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.execution_minutes_ago IS 'Column execution_minutes_ago.';


--
-- Name: COLUMN vw_task_summary_chart.value_recency_minutes_ago; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.value_recency_minutes_ago IS 'Column value_recency_minutes_ago.';


--
-- Name: COLUMN vw_task_summary_chart.next_planned_execution_minutes; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.next_planned_execution_minutes IS 'Column next_planned_execution_minutes.';


--
-- Name: COLUMN vw_task_summary_chart.value_recency; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.value_recency IS 'Column value_recency.';


--
-- Name: COLUMN vw_task_summary_chart.last_executed_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.last_executed_utc IS 'Column last_executed_utc.';


--
-- Name: COLUMN vw_task_summary_chart.last_failed_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.last_failed_utc IS 'Column last_failed_utc.';


--
-- Name: COLUMN vw_task_summary_chart.next_planned_execution_utc; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.next_planned_execution_utc IS 'Column next_planned_execution_utc.';


--
-- Name: COLUMN vw_task_summary_chart.last_failure_message; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.last_failure_message IS 'Column last_failure_message.';


--
-- Name: COLUMN vw_task_summary_chart.is_active_failure; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.is_active_failure IS 'Column is_active_failure.';


--
-- Name: COLUMN vw_task_summary_chart.execution_count; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.execution_count IS 'Column execution_count.';


--
-- Name: COLUMN vw_task_summary_chart.success_count; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.success_count IS 'Column success_count.';


--
-- Name: COLUMN vw_task_summary_chart.success_percentage; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.success_percentage IS 'Column success_percentage.';


--
-- Name: COLUMN vw_task_summary_chart.max_executions; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.max_executions IS 'Column max_executions.';


--
-- Name: COLUMN vw_task_summary_chart.login_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.login_ms IS 'Column login_ms.';


--
-- Name: COLUMN vw_task_summary_chart.extract_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.extract_ms IS 'Column extract_ms.';


--
-- Name: COLUMN vw_task_summary_chart.flatten_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.flatten_ms IS 'Column flatten_ms.';


--
-- Name: COLUMN vw_task_summary_chart.parse_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.parse_ms IS 'Column parse_ms.';


--
-- Name: COLUMN vw_task_summary_chart.interpolation_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.interpolation_ms IS 'Column interpolation_ms.';


--
-- Name: COLUMN vw_task_summary_chart.forecasting_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.forecasting_ms IS 'Column forecasting_ms.';


--
-- Name: COLUMN vw_task_summary_chart.python_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.python_ms IS 'Column python_ms.';


--
-- Name: COLUMN vw_task_summary_chart.admin_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.admin_ms IS 'Column admin_ms.';


--
-- Name: COLUMN vw_task_summary_chart.total_ms; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.total_ms IS 'Column total_ms.';


--
-- Name: COLUMN vw_task_summary_chart.increasing_steps; Type: COMMENT; Schema: tasks; Owner: -
--

COMMENT ON COLUMN tasks.vw_task_summary_chart.increasing_steps IS 'Column increasing_steps.';


--
-- Name: game_events; Type: TABLE; Schema: volleyball; Owner: -
--

CREATE TABLE volleyball.game_events (
    event_id integer NOT NULL,
    game_id integer NOT NULL,
    kind text NOT NULL,
    message text,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT game_events_kind_check CHECK ((kind = ANY (ARRAY['timeout'::text, 'message'::text]))),
    CONSTRAINT game_events_kind_message CHECK ((((kind = 'timeout'::text) AND (message IS NULL)) OR ((kind = 'message'::text) AND (message IS NOT NULL) AND (btrim(message) <> ''::text))))
);


--
-- Name: TABLE game_events; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON TABLE volleyball.game_events IS 'Table game_events.';


--
-- Name: COLUMN game_events.event_id; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.event_id IS 'Column event_id.';


--
-- Name: COLUMN game_events.game_id; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.game_id IS 'Column game_id.';


--
-- Name: COLUMN game_events.kind; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.kind IS 'Column kind.';


--
-- Name: COLUMN game_events.message; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.message IS 'Column message.';


--
-- Name: COLUMN game_events.recorded_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.recorded_at IS 'Column recorded_at.';


--
-- Name: COLUMN game_events.created_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.game_events.created_at IS 'Column created_at.';


--
-- Name: game_events_event_id_seq; Type: SEQUENCE; Schema: volleyball; Owner: -
--

CREATE SEQUENCE volleyball.game_events_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: game_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: volleyball; Owner: -
--

ALTER SEQUENCE volleyball.game_events_event_id_seq OWNED BY volleyball.game_events.event_id;


--
-- Name: games; Type: TABLE; Schema: volleyball; Owner: -
--

CREATE TABLE volleyball.games (
    game_id integer NOT NULL,
    team_b_name text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    label text,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    partner_number integer,
    partner_name text,
    first_entry_at timestamp with time zone
);


--
-- Name: TABLE games; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON TABLE volleyball.games IS 'Table games.';


--
-- Name: COLUMN games.game_id; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.game_id IS 'Column game_id.';


--
-- Name: COLUMN games.team_b_name; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.team_b_name IS 'Column team_b_name.';


--
-- Name: COLUMN games.status; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.status IS 'Column status.';


--
-- Name: COLUMN games.started_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.started_at IS 'Column started_at.';


--
-- Name: COLUMN games.completed_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.completed_at IS 'Column completed_at.';


--
-- Name: COLUMN games.label; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.label IS 'Column label.';


--
-- Name: COLUMN games.notes; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.notes IS 'Column notes.';


--
-- Name: COLUMN games.created_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.created_at IS 'Column created_at.';


--
-- Name: COLUMN games.partner_number; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.partner_number IS 'Column partner_number.';


--
-- Name: COLUMN games.partner_name; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.partner_name IS 'Column partner_name.';


--
-- Name: COLUMN games.first_entry_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.games.first_entry_at IS 'Column first_entry_at.';


--
-- Name: games_game_id_seq; Type: SEQUENCE; Schema: volleyball; Owner: -
--

CREATE SEQUENCE volleyball.games_game_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: games_game_id_seq; Type: SEQUENCE OWNED BY; Schema: volleyball; Owner: -
--

ALTER SEQUENCE volleyball.games_game_id_seq OWNED BY volleyball.games.game_id;


--
-- Name: points; Type: TABLE; Schema: volleyball; Owner: -
--

CREATE TABLE volleyball.points (
    point_id integer NOT NULL,
    game_id integer NOT NULL,
    scoring_team text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    event_type text,
    CONSTRAINT points_scoring_team_check CHECK ((scoring_team = ANY (ARRAY['SR'::text, 'OPPONENT'::text])))
);


--
-- Name: TABLE points; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON TABLE volleyball.points IS 'Table points.';


--
-- Name: COLUMN points.point_id; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.point_id IS 'Column point_id.';


--
-- Name: COLUMN points.game_id; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.game_id IS 'Column game_id.';


--
-- Name: COLUMN points.scoring_team; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.scoring_team IS 'Column scoring_team.';


--
-- Name: COLUMN points.recorded_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.recorded_at IS 'Column recorded_at.';


--
-- Name: COLUMN points.created_at; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.created_at IS 'Column created_at.';


--
-- Name: COLUMN points.event_type; Type: COMMENT; Schema: volleyball; Owner: -
--

COMMENT ON COLUMN volleyball.points.event_type IS 'Column event_type.';


--
-- Name: points_point_id_seq; Type: SEQUENCE; Schema: volleyball; Owner: -
--

CREATE SEQUENCE volleyball.points_point_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: points_point_id_seq; Type: SEQUENCE OWNED BY; Schema: volleyball; Owner: -
--

ALTER SEQUENCE volleyball.points_point_id_seq OWNED BY volleyball.points.point_id;


--
-- Name: courses course_id; Type: DEFAULT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.courses ALTER COLUMN course_id SET DEFAULT nextval('activities.courses_course_id_seq'::regclass);


--
-- Name: overlap_queue queue_id; Type: DEFAULT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.overlap_queue ALTER COLUMN queue_id SET DEFAULT nextval('activities.overlap_queue_queue_id_seq'::regclass);


--
-- Name: segments segment_id; Type: DEFAULT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segments ALTER COLUMN segment_id SET DEFAULT nextval('activities.segments_segment_id_seq'::regclass);


--
-- Name: api_tokens id; Type: DEFAULT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.api_tokens ALTER COLUMN id SET DEFAULT nextval('api_services.api_tokens_id_seq'::regclass);


--
-- Name: spotify_auth_metadata id; Type: DEFAULT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.spotify_auth_metadata ALTER COLUMN id SET DEFAULT nextval('api_services.spotify_auth_metadata_id_seq'::regclass);


--
-- Name: exercise_attempts attempt_id; Type: DEFAULT; Schema: exercises; Owner: -
--

ALTER TABLE ONLY exercises.exercise_attempts ALTER COLUMN attempt_id SET DEFAULT nextval('exercises.exercise_attempts_attempt_id_seq'::regclass);


--
-- Name: exercise_timers exercise_id; Type: DEFAULT; Schema: exercises; Owner: -
--

ALTER TABLE ONLY exercises.exercise_timers ALTER COLUMN exercise_id SET DEFAULT nextval('exercises.exercise_timers_exercise_id_seq'::regclass);


--
-- Name: diary_entries entry_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.diary_entries ALTER COLUMN entry_id SET DEFAULT nextval('food.diary_entries_entry_id_seq'::regclass);


--
-- Name: foods food_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.foods ALTER COLUMN food_id SET DEFAULT nextval('food.foods_food_id_seq'::regclass);


--
-- Name: recipe_ingredients ingredient_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_ingredients ALTER COLUMN ingredient_id SET DEFAULT nextval('food.recipe_ingredients_ingredient_id_seq'::regclass);


--
-- Name: recipe_steps step_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_steps ALTER COLUMN step_id SET DEFAULT nextval('food.recipe_steps_step_id_seq'::regclass);


--
-- Name: recipes recipe_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipes ALTER COLUMN recipe_id SET DEFAULT nextval('food.recipes_recipe_id_seq'::regclass);


--
-- Name: tri_tip_events tri_tip_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.tri_tip_events ALTER COLUMN tri_tip_id SET DEFAULT nextval('food.tri_tip_events_tri_tip_id_seq'::regclass);


--
-- Name: tri_tip_readings reading_id; Type: DEFAULT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.tri_tip_readings ALTER COLUMN reading_id SET DEFAULT nextval('food.tri_tip_readings_reading_id_seq'::regclass);


--
-- Name: health_checks check_id; Type: DEFAULT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.health_checks ALTER COLUMN check_id SET DEFAULT nextval('logging.health_checks_check_id_seq'::regclass);


--
-- Name: task_executions record_id; Type: DEFAULT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.task_executions ALTER COLUMN record_id SET DEFAULT nextval('logging.task_executions_record_id_seq'::regclass);


--
-- Name: api_imports load_id; Type: DEFAULT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.api_imports ALTER COLUMN load_id SET DEFAULT nextval('staging.api_imports_load_id_seq'::regclass);


--
-- Name: fact_configuration fact_id; Type: DEFAULT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.fact_configuration ALTER COLUMN fact_id SET DEFAULT nextval('tasks.fact_configuration_fact_id_seq'::regclass);


--
-- Name: staging_configuration staging_id; Type: DEFAULT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.staging_configuration ALTER COLUMN staging_id SET DEFAULT nextval('tasks.staging_configuration_staging_id_seq'::regclass);


--
-- Name: task_configuration task_id; Type: DEFAULT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_configuration ALTER COLUMN task_id SET DEFAULT nextval('tasks.task_configuration_task_id_seq'::regclass);


--
-- Name: task_executions execution_id; Type: DEFAULT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_executions ALTER COLUMN execution_id SET DEFAULT nextval('tasks.task_executions_execution_id_seq'::regclass);


--
-- Name: game_events event_id; Type: DEFAULT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.game_events ALTER COLUMN event_id SET DEFAULT nextval('volleyball.game_events_event_id_seq'::regclass);


--
-- Name: games game_id; Type: DEFAULT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.games ALTER COLUMN game_id SET DEFAULT nextval('volleyball.games_game_id_seq'::regclass);


--
-- Name: points point_id; Type: DEFAULT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.points ALTER COLUMN point_id SET DEFAULT nextval('volleyball.points_point_id_seq'::regclass);


--
-- Name: activities activities_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.activities
    ADD CONSTRAINT activities_pkey PRIMARY KEY (activity_id);


--
-- Name: activity_details_distance activity_details_distance_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.activity_details_distance
    ADD CONSTRAINT activity_details_distance_pkey PRIMARY KEY (activity_id, distance_m);


--
-- Name: activity_details activity_details_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.activity_details
    ADD CONSTRAINT activity_details_pkey PRIMARY KEY (activity_id, ts_utc);


--
-- Name: activity_processing_queue activity_processing_queue_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.activity_processing_queue
    ADD CONSTRAINT activity_processing_queue_pkey PRIMARY KEY (activity_id);


--
-- Name: courses courses_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.courses
    ADD CONSTRAINT courses_pkey PRIMARY KEY (course_id);


--
-- Name: distinct_segments distinct_segments_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.distinct_segments
    ADD CONSTRAINT distinct_segments_pkey PRIMARY KEY (segment_id, match_seg_id);


--
-- Name: elevation_reference elevation_reference_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.elevation_reference
    ADD CONSTRAINT elevation_reference_pkey PRIMARY KEY (grid_5m_00_id);


--
-- Name: metric_list metric_list_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.metric_list
    ADD CONSTRAINT metric_list_pkey PRIMARY KEY (activity_type_id, metric_name);


--
-- Name: overlap_queue overlap_queue_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.overlap_queue
    ADD CONSTRAINT overlap_queue_pkey PRIMARY KEY (queue_id);


--
-- Name: race_predictions_raw race_predictions_raw_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.race_predictions_raw
    ADD CONSTRAINT race_predictions_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: segment_matches segment_matches_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segment_matches
    ADD CONSTRAINT segment_matches_pkey PRIMARY KEY (segment_id, activity_id);


--
-- Name: segments_details segments_details_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segments_details
    ADD CONSTRAINT segments_details_pkey PRIMARY KEY (segment_id, activity_id, activity_start_point, activity_end_point);


--
-- Name: segments segments_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segments
    ADD CONSTRAINT segments_pkey PRIMARY KEY (segment_id);


--
-- Name: steps_raw steps_raw_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.steps_raw
    ADD CONSTRAINT steps_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: training_cycle_dates training_cycle_dates_pkey; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.training_cycle_dates
    ADD CONSTRAINT training_cycle_dates_pkey PRIMARY KEY (cycle_start_utc);


--
-- Name: segment_matches uc_segment_match_act_seg_ids; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segment_matches
    ADD CONSTRAINT uc_segment_match_act_seg_ids UNIQUE (segment_id, activity_id);


--
-- Name: overlap_queue unique_overlap_pair; Type: CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.overlap_queue
    ADD CONSTRAINT unique_overlap_pair UNIQUE (segment_id_a, segment_id_b);


--
-- Name: api_service_list api_service_list_pkey; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.api_service_list
    ADD CONSTRAINT api_service_list_pkey PRIMARY KEY (api_service_name);


--
-- Name: api_tokens api_tokens_pkey; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.api_tokens
    ADD CONSTRAINT api_tokens_pkey PRIMARY KEY (id);


--
-- Name: credentials credentials_pkey; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (api_service_name);


--
-- Name: function_library function_library_pkey; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.function_library
    ADD CONSTRAINT function_library_pkey PRIMARY KEY (friendly_name);


--
-- Name: spotify_auth_metadata spotify_auth_metadata_pkey; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.spotify_auth_metadata
    ADD CONSTRAINT spotify_auth_metadata_pkey PRIMARY KEY (id);


--
-- Name: spotify_auth_metadata spotify_auth_metadata_refresh_token_hash_key; Type: CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.spotify_auth_metadata
    ADD CONSTRAINT spotify_auth_metadata_refresh_token_hash_key UNIQUE (refresh_token_hash);


--
-- Name: exercise_attempts exercise_attempts_pkey; Type: CONSTRAINT; Schema: exercises; Owner: -
--

ALTER TABLE ONLY exercises.exercise_attempts
    ADD CONSTRAINT exercise_attempts_pkey PRIMARY KEY (attempt_id);


--
-- Name: exercise_timers exercise_timers_pkey; Type: CONSTRAINT; Schema: exercises; Owner: -
--

ALTER TABLE ONLY exercises.exercise_timers
    ADD CONSTRAINT exercise_timers_pkey PRIMARY KEY (exercise_id);


--
-- Name: diary_entries diary_entries_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.diary_entries
    ADD CONSTRAINT diary_entries_pkey PRIMARY KEY (entry_id);


--
-- Name: foods foods_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.foods
    ADD CONSTRAINT foods_pkey PRIMARY KEY (food_id);


--
-- Name: recipe_cook_states recipe_cook_states_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_cook_states
    ADD CONSTRAINT recipe_cook_states_pkey PRIMARY KEY (recipe_id);


--
-- Name: recipe_ingredients recipe_ingredients_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_ingredients
    ADD CONSTRAINT recipe_ingredients_pkey PRIMARY KEY (recipe_id, ingredient_id);


--
-- Name: recipe_step_foods recipe_step_foods_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_step_foods
    ADD CONSTRAINT recipe_step_foods_pkey PRIMARY KEY (recipe_id, step_id, food_id, unit);


--
-- Name: recipe_steps recipe_steps_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_steps
    ADD CONSTRAINT recipe_steps_pkey PRIMARY KEY (step_id);


--
-- Name: recipe_steps recipe_steps_recipe_id_step_no_key; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_steps
    ADD CONSTRAINT recipe_steps_recipe_id_step_no_key UNIQUE (recipe_id, step_no);


--
-- Name: recipes recipes_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipes
    ADD CONSTRAINT recipes_pkey PRIMARY KEY (recipe_id);


--
-- Name: tri_tip_events tri_tip_events_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.tri_tip_events
    ADD CONSTRAINT tri_tip_events_pkey PRIMARY KEY (tri_tip_id);


--
-- Name: tri_tip_readings tri_tip_readings_pkey; Type: CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.tri_tip_readings
    ADD CONSTRAINT tri_tip_readings_pkey PRIMARY KEY (reading_id);


--
-- Name: body_battery_raw body_battery_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.body_battery_raw
    ADD CONSTRAINT body_battery_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: body_composition_raw body_composition_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.body_composition_raw
    ADD CONSTRAINT body_composition_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: body_dimensions body_dimensions_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.body_dimensions
    ADD CONSTRAINT body_dimensions_pkey PRIMARY KEY (captured_at_utc);


--
-- Name: fitness_age_raw fitness_age_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.fitness_age_raw
    ADD CONSTRAINT fitness_age_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: heartrate_raw heartrate_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.heartrate_raw
    ADD CONSTRAINT heartrate_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: hrv_raw hrv_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.hrv_raw
    ADD CONSTRAINT hrv_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: photo_metadata photo_metadata_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.photo_metadata
    ADD CONSTRAINT photo_metadata_pkey PRIMARY KEY (captured_at_utc);


--
-- Name: respiration_raw respiration_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.respiration_raw
    ADD CONSTRAINT respiration_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: sleep_movement_raw sleep_movement_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.sleep_movement_raw
    ADD CONSTRAINT sleep_movement_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: sleep_totals sleep_totals_pk; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.sleep_totals
    ADD CONSTRAINT sleep_totals_pk PRIMARY KEY (sleep_start_utc);


--
-- Name: spo2_raw spo2_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.spo2_raw
    ADD CONSTRAINT spo2_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: stress_raw stress_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.stress_raw
    ADD CONSTRAINT stress_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: training_status_raw training_status_raw_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.training_status_raw
    ADD CONSTRAINT training_status_raw_pkey PRIMARY KEY (ts_utc);


--
-- Name: weight_target weight_target_pkey; Type: CONSTRAINT; Schema: health; Owner: -
--

ALTER TABLE ONLY health.weight_target
    ADD CONSTRAINT weight_target_pkey PRIMARY KEY (ts_utc);


--
-- Name: api_logins api_logins_pkey; Type: CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.api_logins
    ADD CONSTRAINT api_logins_pkey PRIMARY KEY (event_time_utc);


--
-- Name: application_events application_events_pkey; Type: CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.application_events
    ADD CONSTRAINT application_events_pkey PRIMARY KEY (event_time_utc);


--
-- Name: health_checks health_checks_pkey; Type: CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.health_checks
    ADD CONSTRAINT health_checks_pkey PRIMARY KEY (check_id);


--
-- Name: music_db_info music_db_info_pkey; Type: CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.music_db_info
    ADD CONSTRAINT music_db_info_pkey PRIMARY KEY (ts_utc);


--
-- Name: task_executions task_executions_pkey; Type: CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.task_executions
    ADD CONSTRAINT task_executions_pkey PRIMARY KEY (record_id);


--
-- Name: all_albums all_albums_pkey; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.all_albums
    ADD CONSTRAINT all_albums_pkey PRIMARY KEY (album_id);


--
-- Name: all_artists all_artists_pkey; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.all_artists
    ADD CONSTRAINT all_artists_pkey PRIMARY KEY (artist_id);


--
-- Name: all_tracks all_tracks_pkey; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.all_tracks
    ADD CONSTRAINT all_tracks_pkey PRIMARY KEY (track_id);


--
-- Name: ratings isrc_ratings; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.ratings
    ADD CONSTRAINT isrc_ratings PRIMARY KEY (playlist_id, isrc);


--
-- Name: isrc_swaps isrc_swaps_pkey; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.isrc_swaps
    ADD CONSTRAINT isrc_swaps_pkey PRIMARY KEY (original_isrc);


--
-- Name: listening_history listening_history_pk; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.listening_history
    ADD CONSTRAINT listening_history_pk PRIMARY KEY (isrc, played_at_utc);


--
-- Name: playlist_relationships pk_parent_child_playlist; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.playlist_relationships
    ADD CONSTRAINT pk_parent_child_playlist PRIMARY KEY (parent_playlist_id, child_playlist_id);


--
-- Name: ratings_history pk_rating_history; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.ratings_history
    ADD CONSTRAINT pk_rating_history PRIMARY KEY (rating_time_utc, playlist_id, isrc);


--
-- Name: playlist_config playlist_config_pkey; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.playlist_config
    ADD CONSTRAINT playlist_config_pkey PRIMARY KEY (playlist_id);


--
-- Name: isrc_non_swaps uq_original_mapped_isrc; Type: CONSTRAINT; Schema: music; Owner: -
--

ALTER TABLE ONLY music.isrc_non_swaps
    ADD CONSTRAINT uq_original_mapped_isrc UNIQUE (original_isrc, mapped_isrc);


--
-- Name: dim_date dim_date_date_utc_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_date
    ADD CONSTRAINT dim_date_date_utc_key UNIQUE (date_utc);


--
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_key);


--
-- Name: dim_hour dim_hour_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_hour
    ADD CONSTRAINT dim_hour_pkey PRIMARY KEY (hour_key);


--
-- Name: dim_hour dim_hour_ts_utc_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_hour
    ADD CONSTRAINT dim_hour_ts_utc_key UNIQUE (ts_utc);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: activity_metric_descriptors activity_metric_descriptors_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.activity_metric_descriptors
    ADD CONSTRAINT activity_metric_descriptors_pkey PRIMARY KEY (activity_id, metric_idx);


--
-- Name: activity_metrics_flat activity_metrics_flat_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.activity_metrics_flat
    ADD CONSTRAINT activity_metrics_flat_pkey PRIMARY KEY (activity_id, activity_detail_index, metric_index);


--
-- Name: activity_payload activity_payload_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.activity_payload
    ADD CONSTRAINT activity_payload_pkey PRIMARY KEY (activity_id);


--
-- Name: api_imports api_imports_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.api_imports
    ADD CONSTRAINT api_imports_pkey PRIMARY KEY (load_id);


--
-- Name: bounds bounds_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.bounds
    ADD CONSTRAINT bounds_pkey PRIMARY KEY (id);


--
-- Name: interpolation_anchors interpolation_anchors_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.interpolation_anchors
    ADD CONSTRAINT interpolation_anchors_pkey PRIMARY KEY (ts_utc);


--
-- Name: interpolation_load_values interpolation_load_values_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.interpolation_load_values
    ADD CONSTRAINT interpolation_load_values_pkey PRIMARY KEY (ts_utc);


--
-- Name: isrc_dupe_seeds isrc_dupe_seeds_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.isrc_dupe_seeds
    ADD CONSTRAINT isrc_dupe_seeds_pkey PRIMARY KEY (isrc);


--
-- Name: json_examples json_examples_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.json_examples
    ADD CONSTRAINT json_examples_pkey PRIMARY KEY (raw_table_name);


--
-- Name: stg_listen_history stg_listen_history_pk; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.stg_listen_history
    ADD CONSTRAINT stg_listen_history_pk PRIMARY KEY (track_id, played_at_utc);


--
-- Name: stg_now_playing stg_now_playing_pk; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.stg_now_playing
    ADD CONSTRAINT stg_now_playing_pk PRIMARY KEY (track_id, played_at_utc);


--
-- Name: temp_activity_metas temp_activity_metas_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.temp_activity_metas
    ADD CONSTRAINT temp_activity_metas_pkey PRIMARY KEY (activity_id);


--
-- Name: temp_elevation_deltas temp_elevation_deltas_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.temp_elevation_deltas
    ADD CONSTRAINT temp_elevation_deltas_pkey PRIMARY KEY (activity_id);


--
-- Name: temp_elevation_medians temp_elevation_medians_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.temp_elevation_medians
    ADD CONSTRAINT temp_elevation_medians_pkey PRIMARY KEY (loc_point);


--
-- Name: temp_elevation_smoothing_m temp_elevation_smoothing_m_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.temp_elevation_smoothing_m
    ADD CONSTRAINT temp_elevation_smoothing_m_pkey PRIMARY KEY (meter);


--
-- Name: temp_elevation_smoothing temp_elevation_smoothing_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.temp_elevation_smoothing
    ADD CONSTRAINT temp_elevation_smoothing_pkey PRIMARY KEY (elapsed_duration_s);


--
-- Name: fact_configuration fact_configuration_pkey; Type: CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.fact_configuration
    ADD CONSTRAINT fact_configuration_pkey PRIMARY KEY (fact_id);


--
-- Name: staging_configuration staging_configuration_pkey; Type: CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.staging_configuration
    ADD CONSTRAINT staging_configuration_pkey PRIMARY KEY (staging_id);


--
-- Name: task_config task_config_pkey; Type: CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_config
    ADD CONSTRAINT task_config_pkey PRIMARY KEY (task_name);


--
-- Name: task_configuration task_configuration_pkey; Type: CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_configuration
    ADD CONSTRAINT task_configuration_pkey PRIMARY KEY (task_id);


--
-- Name: task_executions task_executions_pkey; Type: CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_executions
    ADD CONSTRAINT task_executions_pkey PRIMARY KEY (execution_id);


--
-- Name: game_events game_events_pkey; Type: CONSTRAINT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.game_events
    ADD CONSTRAINT game_events_pkey PRIMARY KEY (event_id);


--
-- Name: games games_pkey; Type: CONSTRAINT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);


--
-- Name: points points_pkey; Type: CONSTRAINT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.points
    ADD CONSTRAINT points_pkey PRIMARY KEY (point_id);


--
-- Name: activity_details_activity_id_distance_mm_idx; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX activity_details_activity_id_distance_mm_idx ON activities.activity_details USING btree (activity_id, distance_mm);


--
-- Name: activity_details_activity_id_distance_mm_idx_dist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX activity_details_activity_id_distance_mm_idx_dist ON activities.activity_details_distance USING btree (activity_id, distance_m);


--
-- Name: idx_act_detail_lat_lon; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_act_detail_lat_lon ON activities.activity_details USING btree (latitude, longitude);


--
-- Name: idx_act_detail_lat_lon_dist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_act_detail_lat_lon_dist ON activities.activity_details_distance USING btree (latitude, longitude);


--
-- Name: idx_activities_path_gist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_activities_path_gist ON activities.activities USING gist (activity_path);


--
-- Name: idx_activities_range; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_activities_range ON activities.activities USING gist (tstzrange(start_time_utc, end_time_utc));


--
-- Name: idx_courses_end_point; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_courses_end_point ON activities.courses USING gist (public.st_endpoint(course_path));


--
-- Name: idx_courses_path; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_courses_path ON activities.courses USING gist (course_path);


--
-- Name: idx_courses_start_point; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_courses_start_point ON activities.courses USING gist (public.st_startpoint(course_path));


--
-- Name: idx_elevation_ref_gist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_elevation_ref_gist ON activities.elevation_reference USING gist (grid_5m_00_id);


--
-- Name: idx_large_table_loc_point; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_large_table_loc_point ON activities.activity_details USING gist (loc_point);


--
-- Name: idx_large_table_loc_point_dist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_large_table_loc_point_dist ON activities.activity_details_distance USING gist (loc_point);


--
-- Name: idx_loc_point_grid; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_loc_point_grid ON activities.activity_details USING gist (grid_5m_00_id);


--
-- Name: idx_loc_point_grid_dist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_loc_point_grid_dist ON activities.activity_details_distance USING gist (grid_5m_00_id);


--
-- Name: idx_segment_activities_path_gist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_segment_activities_path_gist ON activities.segment_matches USING gist (activity_path);


--
-- Name: idx_segments_path_gist; Type: INDEX; Schema: activities; Owner: -
--

CREATE INDEX idx_segments_path_gist ON activities.segments USING gist (segment_path);


--
-- Name: api_tokens_service_name_is_active_idx; Type: INDEX; Schema: api_services; Owner: -
--

CREATE INDEX api_tokens_service_name_is_active_idx ON api_services.api_tokens USING btree (service_name, is_active);


--
-- Name: idx_exercise_timers_name; Type: INDEX; Schema: exercises; Owner: -
--

CREATE UNIQUE INDEX idx_exercise_timers_name ON exercises.exercise_timers USING btree (lower(name));


--
-- Name: ix_diary_day; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_diary_day ON food.diary_entries USING btree (logged_at DESC, entry_id DESC);


--
-- Name: ix_diary_logged_day_kcal; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_diary_logged_day_kcal ON food.diary_entries USING btree ((((logged_at AT TIME ZONE 'UTC'::text))::date), kcal) WHERE (kcal IS NOT NULL);


--
-- Name: ix_foods_barcode; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_foods_barcode ON food.foods USING btree (barcode) WHERE (barcode IS NOT NULL);


--
-- Name: ix_foods_name_trgm; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_foods_name_trgm ON food.foods USING gin (name public.gin_trgm_ops);


--
-- Name: ix_foods_needs_review; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_foods_needs_review ON food.foods USING btree (needs_review) WHERE needs_review;


--
-- Name: ix_foods_source_alive; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_foods_source_alive ON food.foods USING btree (source) WHERE (NOT is_deleted);


--
-- Name: ix_recipes_alive_rank; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_recipes_alive_rank ON food.recipes USING btree (times_prepared DESC, title) WHERE (NOT is_deleted);


--
-- Name: ix_recipes_cat_alive; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_recipes_cat_alive ON food.recipes USING btree (category) WHERE (NOT is_deleted);


--
-- Name: ix_recipes_food; Type: INDEX; Schema: food; Owner: -
--

CREATE INDEX ix_recipes_food ON food.recipes USING btree (food_id) WHERE (food_id IS NOT NULL);


--
-- Name: heartrate_raw_hr_date_activity_label_heartrate_bpm_idx; Type: INDEX; Schema: health; Owner: -
--

CREATE INDEX heartrate_raw_hr_date_activity_label_heartrate_bpm_idx ON health.heartrate_raw USING btree (hr_date, activity_label, heartrate_bpm);


--
-- Name: heartrate_raw_hr_date_idx; Type: INDEX; Schema: health; Owner: -
--

CREATE INDEX heartrate_raw_hr_date_idx ON health.heartrate_raw USING btree (hr_date);


--
-- Name: idx_heartrate_raw_ts_utc_date; Type: INDEX; Schema: health; Owner: -
--

CREATE INDEX idx_heartrate_raw_ts_utc_date ON health.heartrate_raw USING btree ((((ts_utc AT TIME ZONE 'UTC'::text))::date), ts_utc);


--
-- Name: idx_sleep_end; Type: INDEX; Schema: health; Owner: -
--

CREATE INDEX idx_sleep_end ON health.sleep_totals USING btree (sleep_end_utc DESC);


--
-- Name: idx_sleep_totals_range; Type: INDEX; Schema: health; Owner: -
--

CREATE INDEX idx_sleep_totals_range ON health.sleep_totals USING gist (tstzrange(sleep_start_utc, sleep_end_utc));


--
-- Name: idx_health_checks_time; Type: INDEX; Schema: logging; Owner: -
--

CREATE INDEX idx_health_checks_time ON logging.health_checks USING btree (check_time_utc DESC);


--
-- Name: trgm_idx_album_name; Type: INDEX; Schema: music; Owner: -
--

CREATE INDEX trgm_idx_album_name ON music.all_albums USING gin (album_name_search public.gin_trgm_ops);


--
-- Name: trgm_idx_artist_name; Type: INDEX; Schema: music; Owner: -
--

CREATE INDEX trgm_idx_artist_name ON music.all_artists USING gin (artist_name_search public.gin_trgm_ops);


--
-- Name: trgm_idx_track_name; Type: INDEX; Schema: music; Owner: -
--

CREATE INDEX trgm_idx_track_name ON music.all_tracks USING gin (track_name_search public.gin_trgm_ops);


--
-- Name: idx_activity_metrics_activity; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_activity_metrics_activity ON staging.activity_metrics_flat USING btree (activity_id);


--
-- Name: idx_isrc_seeds_duration; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_isrc_seeds_duration ON staging.isrc_dupe_seeds USING btree (duration_ms);

ALTER TABLE staging.isrc_dupe_seeds CLUSTER ON idx_isrc_seeds_duration;


--
-- Name: idx_isrc_seeds_duration_gist; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_isrc_seeds_duration_gist ON staging.isrc_dupe_seeds USING gist (duration_ms);


--
-- Name: idx_sm_activity_details_lat_lon; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_sm_activity_details_lat_lon ON staging.temp_activity_matched_locations USING btree (latitude, longitude);


--
-- Name: idx_sm_activity_details_medians; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_sm_activity_details_medians ON staging.temp_elevation_medians USING btree (loc_point);


--
-- Name: idx_sm_activity_details_p2p; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_sm_activity_details_p2p ON staging.temp_activity_matched_locations USING btree (loc_point, loc_point_matched);


--
-- Name: trgm_idx_pair_track1; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX trgm_idx_pair_track1 ON staging.isrc_dupe_pairs USING gin (track1 public.gin_trgm_ops);


--
-- Name: trgm_idx_pair_track2; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX trgm_idx_pair_track2 ON staging.isrc_dupe_pairs USING gin (track2 public.gin_trgm_ops);


--
-- Name: trgm_idx_track_seeds; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX trgm_idx_track_seeds ON staging.isrc_dupe_seeds USING gin (track_name_search public.gin_trgm_ops);


--
-- Name: ix_game_events_game_recorded; Type: INDEX; Schema: volleyball; Owner: -
--

CREATE INDEX ix_game_events_game_recorded ON volleyball.game_events USING btree (game_id, recorded_at DESC);


--
-- Name: vw_activity_summary_segment_creation _RETURN; Type: RULE; Schema: activities; Owner: -
--

CREATE OR REPLACE VIEW activities.vw_activity_summary_segment_creation AS
 WITH calcs AS (
         SELECT a.activity_id,
            a.activity_type_name,
            a.start_time_utc,
            public.m2mi(a.distance_m) AS distance_mi,
            a.distance_m,
            min(
                CASE
                    WHEN s.is_course THEN s.segment_id
                    ELSE NULL::integer
                END) AS course_id,
            count(DISTINCT
                CASE
                    WHEN (NOT s.is_course) THEN s.segment_id
                    ELSE NULL::integer
                END) AS child_segments,
            count(DISTINCT
                CASE
                    WHEN (sm.match_confirmed AND s2.is_course) THEN sm.segment_id
                    ELSE NULL::integer
                END) AS matched_courses,
            count(DISTINCT
                CASE
                    WHEN (sm.match_confirmed AND (NOT s2.is_course)) THEN sm.segment_id
                    ELSE NULL::integer
                END) AS matched_segments
           FROM ((((activities.activities a
             JOIN ( SELECT DISTINCT activity_details_distance.activity_id
                   FROM activities.activity_details_distance) ad ON ((ad.activity_id = a.activity_id)))
             LEFT JOIN activities.segments s ON ((s.activity_reference_id = a.activity_id)))
             LEFT JOIN activities.segment_matches sm ON ((sm.activity_id = a.activity_id)))
             LEFT JOIN activities.segments s2 ON ((s2.segment_id = sm.segment_id)))
          GROUP BY a.activity_id, a.start_time_utc, a.distance_m
        )
 SELECT calcs.activity_id,
    calcs.activity_type_name,
    calcs.start_time_utc,
    calcs.distance_mi,
    calcs.distance_m,
    COALESCE(segments.segment_name, ''::text) AS child_courses,
    calcs.child_segments,
    calcs.matched_courses,
    calcs.matched_segments
   FROM (calcs
     LEFT JOIN activities.segments ON ((calcs.course_id = segments.segment_id)))
  ORDER BY calcs.start_time_utc DESC;


--
-- Name: vw_segments_effort_stats _RETURN; Type: RULE; Schema: activities; Owner: -
--

CREATE OR REPLACE VIEW activities.vw_segments_effort_stats AS
 WITH segs AS (
         SELECT s.segment_id,
            s.segment_name,
            s.is_course,
            a.activity_type_name,
            public.m2mi(s.distance_m) AS distance_mi,
            s.elevation_gain,
            (max(a.start_time_utc))::date AS last_effort,
            count(DISTINCT sm.activity_id) AS matched_activity_count
           FROM ((activities.segments s
             LEFT JOIN activities.segment_matches sm ON ((sm.segment_id = s.segment_id)))
             LEFT JOIN activities.activities a ON ((a.activity_id = sm.activity_id)))
          GROUP BY s.segment_id, s.segment_name, a.activity_type_name
        )
 SELECT segment_id,
    segment_name,
    is_course,
    activity_type_name,
    distance_mi,
    elevation_gain,
    last_effort,
    matched_activity_count
   FROM segs
  ORDER BY is_course DESC, last_effort DESC;


--
-- Name: segment_match_exclusions segment_match_exclusions_activity_id_fkey; Type: FK CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segment_match_exclusions
    ADD CONSTRAINT segment_match_exclusions_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES activities.activities(activity_id);


--
-- Name: segment_match_exclusions segment_match_exclusions_segment_id_fkey; Type: FK CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segment_match_exclusions
    ADD CONSTRAINT segment_match_exclusions_segment_id_fkey FOREIGN KEY (segment_id) REFERENCES activities.segments(segment_id);


--
-- Name: segment_matches segment_matches_activity_id_fkey; Type: FK CONSTRAINT; Schema: activities; Owner: -
--

ALTER TABLE ONLY activities.segment_matches
    ADD CONSTRAINT segment_matches_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES activities.activities(activity_id);


--
-- Name: function_library fk_svc_function_to_service; Type: FK CONSTRAINT; Schema: api_services; Owner: -
--

ALTER TABLE ONLY api_services.function_library
    ADD CONSTRAINT fk_svc_function_to_service FOREIGN KEY (api_service_name) REFERENCES api_services.api_service_list(api_service_name);


--
-- Name: exercise_attempts exercise_attempts_exercise_id_fkey; Type: FK CONSTRAINT; Schema: exercises; Owner: -
--

ALTER TABLE ONLY exercises.exercise_attempts
    ADD CONSTRAINT exercise_attempts_exercise_id_fkey FOREIGN KEY (exercise_id) REFERENCES exercises.exercise_timers(exercise_id) ON DELETE RESTRICT;


--
-- Name: diary_entries diary_entries_food_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.diary_entries
    ADD CONSTRAINT diary_entries_food_id_fkey FOREIGN KEY (food_id) REFERENCES food.foods(food_id);


--
-- Name: recipe_cook_states recipe_cook_states_recipe_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_cook_states
    ADD CONSTRAINT recipe_cook_states_recipe_id_fkey FOREIGN KEY (recipe_id) REFERENCES food.recipes(recipe_id) ON DELETE CASCADE;


--
-- Name: recipe_ingredients recipe_ingredients_food_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_ingredients
    ADD CONSTRAINT recipe_ingredients_food_id_fkey FOREIGN KEY (food_id) REFERENCES food.foods(food_id);


--
-- Name: recipe_ingredients recipe_ingredients_recipe_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_ingredients
    ADD CONSTRAINT recipe_ingredients_recipe_id_fkey FOREIGN KEY (recipe_id) REFERENCES food.recipes(recipe_id) ON DELETE CASCADE;


--
-- Name: recipe_step_foods recipe_step_foods_food_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_step_foods
    ADD CONSTRAINT recipe_step_foods_food_id_fkey FOREIGN KEY (food_id) REFERENCES food.foods(food_id);


--
-- Name: recipe_step_foods recipe_step_foods_recipe_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_step_foods
    ADD CONSTRAINT recipe_step_foods_recipe_id_fkey FOREIGN KEY (recipe_id) REFERENCES food.recipes(recipe_id) ON DELETE CASCADE;


--
-- Name: recipe_step_foods recipe_step_foods_step_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_step_foods
    ADD CONSTRAINT recipe_step_foods_step_id_fkey FOREIGN KEY (step_id) REFERENCES food.recipe_steps(step_id) ON DELETE CASCADE;


--
-- Name: recipe_steps recipe_steps_recipe_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipe_steps
    ADD CONSTRAINT recipe_steps_recipe_id_fkey FOREIGN KEY (recipe_id) REFERENCES food.recipes(recipe_id) ON DELETE CASCADE;


--
-- Name: recipes recipes_food_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.recipes
    ADD CONSTRAINT recipes_food_id_fkey FOREIGN KEY (food_id) REFERENCES food.foods(food_id);


--
-- Name: tri_tip_readings tri_tip_readings_tri_tip_id_fkey; Type: FK CONSTRAINT; Schema: food; Owner: -
--

ALTER TABLE ONLY food.tri_tip_readings
    ADD CONSTRAINT tri_tip_readings_tri_tip_id_fkey FOREIGN KEY (tri_tip_id) REFERENCES food.tri_tip_events(tri_tip_id) ON DELETE CASCADE;


--
-- Name: api_logins api_logins_api_service_name_fkey; Type: FK CONSTRAINT; Schema: logging; Owner: -
--

ALTER TABLE ONLY logging.api_logins
    ADD CONSTRAINT api_logins_api_service_name_fkey FOREIGN KEY (api_service_name) REFERENCES api_services.api_service_list(api_service_name);


--
-- Name: dim_hour dim_hour_date_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_hour
    ADD CONSTRAINT dim_hour_date_key_fkey FOREIGN KEY (date_key) REFERENCES public.dim_date(date_key);


--
-- Name: activity_metrics_matched activity_metrics_matched_activity_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.activity_metrics_matched
    ADD CONSTRAINT activity_metrics_matched_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES staging.activity_payload(activity_id) ON DELETE CASCADE;


--
-- Name: fact_configuration fk_fact_to_staging; Type: FK CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.fact_configuration
    ADD CONSTRAINT fk_fact_to_staging FOREIGN KEY (staging_id) REFERENCES tasks.staging_configuration(staging_id);


--
-- Name: fact_configuration fk_fact_to_task; Type: FK CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.fact_configuration
    ADD CONSTRAINT fk_fact_to_task FOREIGN KEY (task_id) REFERENCES tasks.task_configuration(task_id);


--
-- Name: staging_configuration fk_staging_to_task; Type: FK CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.staging_configuration
    ADD CONSTRAINT fk_staging_to_task FOREIGN KEY (task_id) REFERENCES tasks.task_configuration(task_id);


--
-- Name: task_config task_config_api_service_name_fkey; Type: FK CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_config
    ADD CONSTRAINT task_config_api_service_name_fkey FOREIGN KEY (api_service_name) REFERENCES api_services.api_service_list(api_service_name);


--
-- Name: task_executions task_executions_task_id_fkey; Type: FK CONSTRAINT; Schema: tasks; Owner: -
--

ALTER TABLE ONLY tasks.task_executions
    ADD CONSTRAINT task_executions_task_id_fkey FOREIGN KEY (task_id) REFERENCES tasks.task_configuration(task_id);


--
-- Name: game_events game_events_game_id_fkey; Type: FK CONSTRAINT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.game_events
    ADD CONSTRAINT game_events_game_id_fkey FOREIGN KEY (game_id) REFERENCES volleyball.games(game_id) ON DELETE CASCADE;


--
-- Name: points points_game_id_fkey; Type: FK CONSTRAINT; Schema: volleyball; Owner: -
--

ALTER TABLE ONLY volleyball.points
    ADD CONSTRAINT points_game_id_fkey FOREIGN KEY (game_id) REFERENCES volleyball.games(game_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict XSiVRBwh1qBezwGFiMIIybVuaBZClxbUeUtnlxGeofW81alcgWzLZwBIE5rGc5P

