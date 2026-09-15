/**
 * Leaderboard Types
 *
 * Cross-surface data contract for the Leaderboards feature (009-002).
 * Shared between the frontend (React) and backend (FastAPI Pydantic models).
 *
 * GET  /api/activities/leaderboard/segments?name=&kind=&min_distance=&activity_type=
 * GET  /api/activities/leaderboard/:segmentId
 * POST /api/activities/leaderboard/visuals
 */

/** Course/segment kind filter: null = both, 'course' = is_course, 'segment' = NOT is_course. */
export type SegmentKindFilter = 'course' | 'segment' | null;

/** Activity-type filter for the segment list (legacy options; null = any). */
export type SegmentActivityTypeFilter =
  | 'Run'
  | 'Trail Run'
  | 'Hike'
  | 'Walk'
  | 'Bike'
  | 'Ski'
  | null;

/** Query params for GET /api/activities/leaderboard/segments. */
export interface SegmentListQuery {
  /** Case-insensitive substring match on segment_name; empty/undefined = any. */
  name?: string;
  /** Course/segment kind; null/undefined = both. */
  kind: SegmentKindFilter;
  /** Minimum distance in miles; 0/undefined = any. */
  min_distance: number;
  /** Activity-type pattern; null/undefined = any. */
  activity_type: SegmentActivityTypeFilter;
}

/** One row of the filterable segment/course list (vw_segments_effort_stats). */
export interface SegmentListRow {
  /** Segment identifier. */
  segment_id: number;
  /** Segment (or course) name. */
  segment_name: string;
  /** True when this row is a course. */
  is_course: boolean;
  /** Activity type of the segment's matched activities. */
  activity_type_name: string | null;
  /** Segment distance in miles. */
  distance_mi: number;
  /** Elevation gain in meters. */
  elevation_gain: number | null;
  /** Last effort start date (ISO 8601 date); null when never attempted. */
  last_effort: string | null;
  /** Count of matched activities (attempts). */
  matched_activity_count: number;
}

/** Leaderboard range selector mapped to the corresponding rank column. */
export type LeaderboardRange = 'All Time' | 'Last 365' | 'Current Cycle' | 'Most Recent';

/** Rank columns of vw_segment_leaderboard keyed by range. */
export const LEADERBOARD_RANGE_COLUMN: Record<LeaderboardRange, string> = {
  'All Time': 'all_time_rank',
  'Last 365': 'last_365_rank',
  'Current Cycle': 'current_cycle_rank',
  'Most Recent': 'recency_rank',
};

/** Leaderboard type selector (legacy col_selector column sets). */
export type LeaderboardType = 'Basic' | 'Fitness' | 'Advanced' | 'Environment' | 'Preparedness';

/** A single ranked effort on a segment, with fitness/preparedness context. */
export interface LeaderboardEffort {
  /** Effort activity id. */
  activity_id: number;
  /** Effort start point within the activity (meters). */
  activity_start_point: number;
  /** Effort end point within the activity (meters). */
  activity_end_point: number;
  /** Active rank in the default range (all_time_rank; ranks re-filter client-side). */
  rank: number;
  /** All-time rank (fastest = 1). */
  all_time_rank: number | null;
  /** Last-365-day rank. */
  last_365_rank: number | null;
  /** Current training-cycle rank. */
  current_cycle_rank: number | null;
  /** Recency rank (1 = most recent). */
  recency_rank: number | null;
  /** Effort start timestamp (ISO 8601). */
  start_time_utc: string;
  /** Effort duration in seconds. */
  elapsed_duration_s: number;
  /** Seconds behind the fastest effort in the default (all-time) ranking. */
  gap_s: number;
  /** Formatted pace text (m:ss/mi). */
  pace_str: string | null;
  /** Average heart rate (bpm). */
  avg_hr: number | null;
  /** Maximum heart rate (bpm). */
  max_hr: number | null;
  /** VO2 max at the time of the effort. */
  vo2_max_value: number | null;
  /** Resting heart rate while asleep (bpm). */
  resting_hr_asleep: number | null;
  /** Resting heart rate while awake (bpm). */
  resting_hr_awake: number | null;
  /** Acute training load. */
  training_load_acute: number | null;
  /** Training load percent. */
  training_load_pct: number | null;
  /** Weight at the time of the effort (lb). */
  weight_lb: number | null;
  /** Body-fat percent. */
  fat_pct: number | null;
  /** Muscle percent. */
  muscle_pct: number | null;
  /** Average cadence (spm). */
  avg_cadence: number | null;
  /** Average vertical oscillation (cm). */
  avg_vert_osc: number | null;
  /** Average vertical ratio. */
  avg_vert_ratio: number | null;
  /** Average ground contact time (ms). */
  avg_gct: number | null;
  /** Average stride length (cm). */
  avg_stride_length: number | null;
  /** Average air temperature (F). */
  avg_temp: number | null;
  /** Altitude acclimation (m). */
  altitude_acclimation_m: number | null;
  /** Heat acclimation percent. */
  heat_acclimation_pct: number | null;
  /** Sleep hours before the effort. */
  sleep_hours: number | null;
  /** Sleep score. */
  sleep_score: number | null;
  /** Awake hours before the effort. */
  awake_hours: number | null;
  /** Average performance condition. */
  avg_perf: number | null;
  /** Average ground contact balance left percent. */
  avg_balance: number | null;
}

/** A segment's full effort leaderboard (all rank columns; range filtering is client-side). */
export interface Leaderboard {
  /** The segment this leaderboard belongs to. */
  segment_id: number;
  /** Segment (or course) name. */
  segment_name: string;
  /** True when the target is a course. */
  is_course: boolean;
  /** Ranked efforts (rows without a rank in the active range are dropped client-side). */
  efforts: LeaderboardEffort[];
}

/** One selected effort for the visuals staging write. */
export interface LeaderboardEffortSelection {
  /** Effort activity id. */
  activity_id: number;
  /** Effort start point within the activity (meters). */
  start_point: number;
  /** Effort end point within the activity (meters). */
  end_point: number;
}

/** Request body for POST /api/activities/leaderboard/visuals. */
export interface LeaderboardVisualsRequest {
  /** The segment the compared efforts belong to. */
  segment_id: number;
  /** Selected efforts (1..N). */
  efforts: LeaderboardEffortSelection[];
}

/** Numeric telemetry series samples for one compared effort (canonical axes). */
export interface SegmentVisualSeriesPoint {
  /** Canonical elapsed time within the effort (seconds, aligned across efforts). */
  elapsed_s: number;
  /** Canonical distance within the effort (meters). */
  distance_m: number;
  /** Smoothed elevation (m). */
  elevation_m: number | null;
  /** Heart rate (bpm). */
  heartrate_bpm: number | null;
  /** Cadence (spm). */
  cadence_spm: number | null;
  /** Speed (mm/s). */
  speed_mmps: number | null;
  /** Vertical ratio. */
  vert_ratio: number | null;
  /** Vertical oscillation (cm). */
  vert_oscillation_cm: number | null;
  /** Air temperature (F). */
  air_temp_c: number | null;
  /** Stride length (cm). */
  stride_length_cm: number | null;
  /** Ground contact time (ms). */
  ground_contact_time_ms: number | null;
}

/** One compared effort's replay path + telemetry series. */
export interface SegmentVisualEffort {
  /** Comparison label (activity date, suffixed for same-day repeats). */
  id_val: string;
  /** Comparison rank (0 = most recent effort). */
  meta_rank: number;
  /** Replay path as [lon, lat] pairs ordered by elapsed time. */
  path_coords: number[][];
  /** Downsampled telemetry series aligned to canonical elapsed time. */
  series: SegmentVisualSeriesPoint[];
}

/** Generated visuals for the selected efforts (staging read via vw_activity_segments_aggregated). */
export interface SegmentVisuals {
  /** The segment the compared efforts belong to. */
  segment_id: number;
  /** Segment (or course) name. */
  segment_name: string;
  /** One entry per selected effort. */
  efforts: SegmentVisualEffort[];
}

/** Client-safe map configuration for the visuals replay (009-002 T08). */
export interface MapConfig {
  /**
   * Mapbox access token used to build Mapbox-style tile URLs. Null when no
   * Mapbox credential is configured (UI then offers only free OSM/Carto tile
   * styles plus the blank vector style — AC-13).
   */
  mapboxToken: string | null;
}
