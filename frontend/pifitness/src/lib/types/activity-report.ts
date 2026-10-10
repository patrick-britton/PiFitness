/**
 * Activity Report Types
 *
 * Cross-surface data contract for the Recent Activity Report feature (009-001,
 * reworked by 004-006). Shared between the frontend (React) and backend
 * (FastAPI Pydantic models in backend/schemas/activity_schemas.py).
 *
 * GET /api/activities/report?activity_id=<id>?
 * Optional id; default = newest qualifying activity (run / trail run / walk /
 * hike, >1000 m, non-null activity_path — 004-006 resolved OQ-1 B).
 */

/** Metrics displayed in the report's summary header for a single activity. */
export interface ActivityReportHeader {
  /** UTC start timestamp (ISO 8601); the UI renders it in local time. */
  start_utc: string;
  /** Activity distance in miles. */
  distance_mi: number;
  /** Formatted distance from vw_activity_header_stats (e.g. '4.6 mi'). */
  distance_display: string;
  /** Total elevation gain in meters (from activities.activities). */
  elevation_m_gain: number | null;
  /** Total duration in seconds (per OQ-4: activity_time_s from vw_activity_summary). */
  total_time_s: number;
  /** Formatted total time as h:mm:ss.ms. */
  total_time_text: string;
  /** Duration display from vw_activity_header_stats (e.g. '46:45.7'). */
  total_time_display: string;
  /** Formatted pace as m:ss.ms/mi. */
  pace_text: string;
  /** Pace display from vw_activity_header_stats (e.g. '10:09.9/mi'). */
  pace_display: string;
  /** Median heart rate, when available. */
  hr_median: number | null;
  /** Maximum heart rate, when available. */
  hr_max: number | null;
  /** Activity name from activities.activities (title fallback when not a course). */
  activity_name: string | null;
  /** Raw activity type name (e.g. 'running', 'trail_running', 'hiking', 'walking'). */
  activity_type_name: string;
  /** Total activity distance in METERS (exact; x-axis max for the HR/elevation charts). */
  distance_m: number;
}

/** A course or crossed segment with its comparison against prior/best efforts. */
export interface ActivityReportSegment {
  /** Segment identifier. */
  segment_id: number;
  /** Segment (or course) name. */
  name: string;
  /** True when this row is the course (is_course = true in the leaderboard view). */
  is_course: boolean;
  /** Overall rank (fastest = 1), when defined. */
  all_time_rank: number | null;
  /** Total number of attempts on this segment/course (the "B" in "A/B"). */
  total_attempts: number;
  /** Seconds faster (negative) or slower (positive) than the prior attempt. */
  prior_delta_s: number | null;
  /** Seconds faster (negative) or slower (positive) than the best-ever attempt. */
  best_delta_s: number | null;
}

/** A single chart sample. x-axis is cumulative distance in meters (004-006). */
export interface ActivityElevationPoint {
  /** Cumulative distance at the sample (meters). */
  distance_m: number;
  /** Average elevation for the sample, in meters. */
  elevation_m: number;
}

/** A single heart-rate chart sample. x-axis is cumulative distance in meters (004-006). */
export interface ActivityHeartratePoint {
  /** Cumulative distance at the sample (meters). */
  distance_m: number;
  /** Average heart rate for the sample, in bpm; null when no HR data. */
  heartrate_bpm: number | null;
}

/** Chart series served with the report (009-006, reworked 004-006). */
export interface ActivityReportCharts {
  elevation: ActivityElevationPoint[];
  heartrate: ActivityHeartratePoint[];
}

/** Prev/next navigation across the qualifying-activity set (004-006). */
export interface ActivityReportNav {
  /** Prior (older) qualifying activity id, or null when on the oldest. */
  prev_activity_id: number | null;
  /** Next (newer) qualifying activity id, or null when on the newest. */
  next_activity_id: number | null;
}

/** One running-efficiency metric: current attempt vs prior-10 course average. */
export interface ActivityEfficiencyMetric {
  /** Current attempt's value, when recorded. */
  current: number | null;
  /** Mean over the prior 10 attempts at the same course, when any exist. */
  prior10_avg: number | null;
}

/**
 * Running-efficiency bullet-chart data (004-006). Present only for run /
 * trail-run efforts on a course; null otherwise (replaces
 * header.show_efficiency_placeholder).
 */
export interface ActivityReportEfficiency {
  avg_cadence: ActivityEfficiencyMetric;
  avg_stride_length: ActivityEfficiencyMetric;
  avg_gct: ActivityEfficiencyMetric;
  avg_vert_ratio: ActivityEfficiencyMetric;
}

/** Full report for a single activity. */
export interface ActivityReport {
  /** The resolved activity id (footer caption). */
  activity_id: number;
  /** Prev/next navigation across the qualifying-activity set. */
  nav: ActivityReportNav;
  /** Summary header metrics. */
  header: ActivityReportHeader;
  /** The matched course row, when the activity is a course effort; else null. */
  course: ActivityReportSegment | null;
  /** Crossed non-course segments (excludes the course row itself). */
  segments: ActivityReportSegment[];
  /** True when the report has any course or segment rows (drives FR-5 nav). */
  has_segments: boolean;
  /** Efficiency bullet data for run/trail-run course efforts; else null. */
  efficiency: ActivityReportEfficiency | null;
  /** Chart series (elevation, heart rate) keyed by cumulative distance. */
  charts: ActivityReportCharts;
  /** Simplified course path as [lon, lat] pairs (bare shape; no basemap). */
  course_path: number[][];
}