/**
 * Activity Report Types
 *
 * Cross-surface data contract for the Recent Activity Report feature (009-001).
 * Shared between the frontend (React) and backend (FastAPI Pydantic models).
 *
 * GET /api/activities/report?activity_type=Run|Walk
 */

/** Activity-type selector for the report target selection. */
export type ActivityReportType = 'Run' | 'Walk';

/** Metrics displayed in the report's summary header for a single activity. */
export interface ActivityReportHeader {
  /** UTC start timestamp (ISO 8601); the UI renders it in local time. */
  start_utc: string;
  /** Activity distance in miles. */
  distance_mi: number;
  /** Total duration in seconds (per OQ-4: activity_time_s from vw_activity_summary). */
  total_time_s: number;
  /** Formatted total time as h:mm:ss.ms. */
  total_time_text: string;
  /** Formatted pace as m:ss.ms/mi. */
  pace_text: string;
  /** Median heart rate, when available. */
  hr_median: number | null;
  /** 75th-percentile heart rate, when available. */
  hr_p75: number | null;
  /** Maximum heart rate, when available. */
  hr_max: number | null;
  /** True for Run/Trail activities -> show the running-efficiency placeholder. */
  show_efficiency_placeholder: boolean;
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

/** Full report for a single activity. */
export interface ActivityReport {
  /** The resolved activity id (footer caption). */
  activity_id: number;
  /** Which activity-type selection produced this report. */
  activity_type: ActivityReportType;
  /** Summary header metrics. */
  header: ActivityReportHeader;
  /** The matched course row, when the activity is a course effort; else null. */
  course: ActivityReportSegment | null;
  /** Crossed non-course segments (excludes the course row itself). */
  segments: ActivityReportSegment[];
  /** True when the report has any course or segment rows (drives FR-5 nav). */
  has_segments: boolean;
}