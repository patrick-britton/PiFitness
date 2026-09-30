/**
 * Segment Management Types (009-003)
 */

/** Query params for GET /api/segments/activities. */
export interface ActivitiesQuery {
  activity_type?: string;
}

/** One activity in the source-activity selection list (FR-1). */
export interface ActivitySummary {
  activity_id: number;
  activity_type_name: string;
  start_time_utc: string;
  distance_m: number;
  child_segment_count: number;
  matched_count: number;
}

/** Response for GET /api/segments/activities. */
export interface ActivitiesResponse {
  data: ActivitySummary[];
  count: number;
}

/** Query params for GET /api/segments/:id/route. */
export interface RouteQuery {
  start_m: number;
  end_m: number;
}

/** Route geometry and elevation data for map and profile rendering (FR-2, FR-4). */
export interface ActivityRoute {
  path_coords: number[][];
  elevations: number[];
  /** Per-point elapsed seconds, 1:1 with path_coords (009-003 T38, OQ-6). */
  elapsed_s: number[];
  distance_m: number;
}

/** Response for GET /api/segments/:id/route. */
export interface RouteResponse {
  data: ActivityRoute;
}

/** Request body for POST /api/segments/create (FR-5). */
export interface CreateSegmentRequest {
  activity_id: number;
  start_m: number;
  end_m: number;
  name: string;
  is_course: boolean;
  map_style: string;
}

/** Response for POST /api/segments/create. */
export interface CreateSegmentResponse {
  segment_id: number;
}

/**
 * Picker list contract (FR-7) — one declaration shared with 009-002
 * (T14 debt consolidation): both endpoints read the same
 * `vw_segments_effort_stats` rows, so the types live in
 * `lib/types/leaderboards.ts` and are re-exported here; the two surfaces
 * cannot drift. `SegmentPicker` already passes the full leaderboards
 * `SegmentListQuery` shape (required kind/min_distance/activity_type keys)
 * to `API.segments.getSegmentsList`.
 */
import type { SegmentListQuery, SegmentListRow } from './leaderboards';

export type { SegmentListQuery, SegmentListRow };

/** Response for GET /api/segments/list. */
export interface SegmentListResponse {
  data: SegmentListRow[];
  count: number;
}

/** Geometry deviation metrics for candidate efforts (FR-10). */
export interface GeometryDeviation {
  distance_deviation_m: number;
  polygon_deviation: number;
  hausdorff_deviation: number | null;
  frechet_deviation: number | null;
}

/** One candidate effort for matching (FR-9, FR-10). */
export interface CandidateEffort {
  activity_id: number;
  /** Matched effort window inside the candidate activity, in meters (T06b/OQ-3). */
  best_start_dist: number;
  best_end_dist: number;
  confidence: number;
  deviation: GeometryDeviation;
  start_time_utc: string;
  distance_m: number;
  elevation_gain: number | null;
  confidence_scaled: number;
  distance_deviation_scaled: number;
  polygon_deviation_scaled: number;
  hausdorff_deviation_scaled: number | null;
  frechet_deviation_scaled: number | null;
}

/** Response for GET /api/segments/:id/matches/candidates. */
export interface CandidatesResponse {
  data: CandidateEffort[];
  count: number;
  existing_match_count: number;
}

/** Request body for POST /api/segments/:id/matches/confirm. */
export interface ConfirmMatchRequest {
  activity_id: number;
}

/** Request body for POST /api/segments/:id/matches/reject. */
export interface RejectMatchRequest {
  activity_id: number;
}

/** Response for confirm/reject operations. */
export interface MatchOperationResponse {
  message: string;
  segment: {
    segment_id: number;
    segment_name: string;
    matched_count: number;
  };
}

/** Request body for POST /api/segments/:id/matches/bulk-confirm (FR-11). */
export interface BulkConfirmRequest {
  confirm_all: boolean;
}

/** Request body for POST /api/segments/:id/matches/bulk-reject (009-003 T31). */
export interface BulkRejectRequest {
  confidence_over?: number;
}

/** Response for POST /api/segments/:id/matches/bulk-reject (009-003 T31). */
export interface BulkRejectResponse {
  rejected: number;
}

/** Request body for POST /api/segments/:id/scoring/hausdorff (FR-12). */
export interface HausdorffScoringRequest {
  run_scoring: boolean;
}

/** Request body for POST /api/segments/:id/scoring/frechet (FR-12). */
export interface FrechetScoringRequest {
  run_scoring: boolean;
}

/** Response for scoring operations. */
export interface ScoringResponse {
  message: string;
  candidates: CandidateEffort[];
}

/** Response for DELETE /api/segments/:id (FR-13). */
export interface DeleteSegmentResponse {
  message: string;
  segment_id: number;
}

/** Request body for POST /api/segments/reset (FR-14). */
export interface ResetMatchTablesRequest {
  confirm_reset: boolean;
}

/** Response for POST /api/segments/reset. */
export interface ResetResponse {
  message: string;
  truncated_count: number;
}

/** Request body for PATCH /api/segments/:id/name (FR-16). */
export interface RenameSegmentRequest {
  new_name: string;
}

/** Response for PATCH /api/segments/:id/name. */
export interface RenameResponse {
  segment_id: number;
  segment_name: string;
}

/** Query params for GET /api/segments/courses (FR-15). */
export interface CoursesQuery {
  name?: string;
  min_distance?: number;
  max_distance?: number;
  page?: number;
  page_size?: number;
}

/** One course in the course review list (FR-15). */
export interface CourseInfo {
  course_id: number;
  course_name: string;
  distance_mi: number;
  elevation_gain: number | null;
  last_run: string | null;
  attempt_count: number;
  is_new: boolean;
  /**
   * Reference-geometry pointers (T02c/OQ-4): a course's own GPS path is
   * fetched as `getRoute(activity_reference_id, {start_m: reference_start_point,
   * end_m: reference_end_point})`. Null when the course row is unknown.
   */
  activity_reference_id?: number | null;
  reference_start_point?: number | null;
  reference_end_point?: number | null;
}

/** Response for GET /api/segments/courses. */
export interface CoursesResponse {
  data: CourseInfo[];
  total_count: number;
  page: number;
  page_size: number;
  total_pages: number;
}

/** Map style configuration. */
export interface MapStyle {
  id: string;
  name: string;
  requires_satellite_token: boolean;
}

/** Response for GET /api/segments/map-styles (FR-3). */
export interface MapStylesResponse {
  data: MapStyle[];
  satellite_available: boolean;
}
