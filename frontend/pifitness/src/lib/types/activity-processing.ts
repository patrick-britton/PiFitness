/**
 * Activity Processing Types
 * 
 * Cross-surface data contract for the Activity Processing & Playlist Shuffle feature.
 * Shared between frontend (React) and backend (FastAPI Pydantic models).
 * 
 * POST /api/activities/process returns NDJSON stream
 */

/**
 * Request body for POST /api/activities/process
 * 'last_walk' processes the most recent Walk; 'last_run' the most recent Run.
 */
export interface ProcessActivityRequest {
  /** Selection mode. */
  mode: 'last_walk' | 'last_run';
  /** Music option. Required (one of the three) when mode === 'last_run'; omitted for 'last_walk'. */
  music?: 'running' | 'jogging' | 'no_music';
}

/**
 * Response body for POST /api/activities/process
 */
export interface ProcessActivityResponse {
  /** True if all steps completed successfully */
  success: boolean;
  /** Ordered array of step results */
  steps: ProcessStepResult[];
  /** Overall error message if a step failed fatally */
  error?: string;
}

/**
 * Result of a single processing step
 */
export interface ProcessStepResult {
  /** Step identifier */
  step_id: ProcessStepId;
  /** Step status */
  status: 'complete' | 'error' | 'skipped' | 'running' | 'pending';
    /** Elapsed time in milliseconds */
  elapsed_ms: number;
  /** ISO 8601 timestamp when the step started (from start event; used for live timer) */
  started_at?: string;
  /** Error message if status is 'error' */
  error?: string;
  /** Optional result data from the step */
  result?: ProcessStepResultData;
}

/**
 * Step identifier constants
 */
export type ProcessStepId =
  | 'sync_activities'
  | 'resolve_activity'
  | 'sync_details'
  | 'match_segments'
  | 'insert_heartrate'
  | 'assign_elevation_reference_time'
  | 'smooth_elevation_spikes_by_time'
  | 'smooth_elevation_python_time'
  | 'update_elevation_reference_by_time'
  | 'resample_activity_to_distance'
  | 'smooth_elevation_spikes_by_distance'
  | 'smooth_elevation_python_distance'
  | 'smooth_elevation_python_reference'
  | 'update_elevation_reference_by_distance'
  | 'build_activity_path'
  | 'segment_match_segments'
  | 'segment_pair_generation'
  | 'segment_polygon_match'
  | 'segment_mass_confirm_1'
  | 'segment_hausdorff_match'
  | 'segment_mass_confirm_2'
  | 'segment_frechet_match'
  | 'segment_mass_confirm_3'
  | 'segment_update_details'
  | 'lookup_playlist'
  | 'insert_history'
  | 'query_isrc_stats'
  | 'send_to_spotify'
  | 'verify_spotify'
  | 'report_shuffle';

/**
 * Step result data (populated on completion of relevant steps)
 */
export interface ProcessStepResultData {
  /** Number of songs heard (from lookup_playlist) */
  song_count?: number;
  /** Number of songs sent to Spotify (from report_shuffle) */
  songs_sent?: number;
  /** First song heard (from lookup_playlist) */
  first_song?: string;
  /** Last song heard (from lookup_playlist) */
  last_song?: string;
  /** Whether the playlist was successfully shuffled (from report_shuffle) */
  playlist_shuffled?: boolean;
  /** The Spotify playlist ID (from lookup_playlist) */
  playlist_id?: string;
}

/**
 * Streaming event emitted when a step begins executing.
 * Each step emits one of these (status 'running') before its terminal event.
 */
export interface ProcessStepStartEvent {
  step_id: ProcessStepId;
  status: 'running';
  /** ISO 8601 timestamp when the step started */
  started_at: string;
}

/**
 * Streaming NDJSON event for a single step completion.
 * Each line in the NDJSON stream is one of these (except the terminal event).
 */
export interface ProcessStepEvent {
  step_id: ProcessStepId;
  status: 'complete' | 'error' | 'skipped';
  elapsed_ms: number;
  error?: string;
  result?: ProcessStepResultData;
}

/**
 * End-of-run summary embedded in the terminal event on success.
 * Null values mean the corresponding steps were skipped / not applicable.
 */
export interface ProcessSummaryData {
  /** Total execution time of the whole run in milliseconds (FR-14) */
  total_elapsed_ms?: number;
  /** Playlist was shuffled (null when playlist steps were skipped) */
  playlist_shuffled?: boolean | null;
  /** Number of segments matched for the processed activity (null when segment steps were skipped) */
  segments_matched?: number | null;
  /** Count of matched courses (null when segment steps were skipped) (FR-14) */
  courses_matched?: number | null;
  /** Whether any matched segment is a course (null when segment steps were skipped) */
  course_found?: boolean | null;
  /** Name of the matched course, when course_found is true */
  course_name?: string | null;
  /** Activity the summary refers to (the resolved activity id) */
  activity_id?: number | null;
}

/**
 * Terminal event marking the end of the NDJSON stream.
 */
export interface ProcessCompleteEvent {
  complete: true;
  success: boolean;
  error?: string;
  summary?: ProcessSummaryData;
}

/**
 * Union type for all NDJSON stream events.
 */
export type NdjsonEvent = ProcessStepStartEvent | ProcessStepEvent | ProcessCompleteEvent;

/**
 * Human-readable labels for each step, used by the StepChecklist component
 */
export const STEP_LABELS: Record<ProcessStepId, string> = {
  sync_activities: 'Syncing Activities',
  sync_details: 'Syncing Activity Details',
  match_segments: 'Matching Segments',
  insert_heartrate: 'Inserting Heart Rate Data',
  assign_elevation_reference_time: 'Assigning Elevation Reference (time)',
  smooth_elevation_spikes_by_time: 'Smoothing Elevation Spikes (time)',
  smooth_elevation_python_time: 'Applying Savitzky-Golay Smoothing (time)',
  update_elevation_reference_by_time: 'Updating Elevation Reference (time)',
  resample_activity_to_distance: 'Resampling Activity to Distance',
  smooth_elevation_spikes_by_distance: 'Smoothing Elevation Spikes (distance)',
  smooth_elevation_python_distance: 'Applying Savitzky-Golay Smoothing (distance)',
  smooth_elevation_python_reference: 'Applying Savitzky-Golay Smoothing (reference)',
  update_elevation_reference_by_distance: 'Updating Elevation Reference (distance)',
  build_activity_path: 'Building Activity Path',
  segment_match_segments: 'Matching Segments',
  segment_pair_generation: 'Generating Segment Pairs',
  segment_polygon_match: 'Polygon Matching',
  segment_mass_confirm_1: 'Mass Confirmation (pass 1)',
  segment_hausdorff_match: 'Hausdorff Matching',
  segment_mass_confirm_2: 'Mass Confirmation (pass 2)',
  segment_frechet_match: 'Frechet Matching',
  segment_mass_confirm_3: 'Mass Confirmation (pass 3)',
  segment_update_details: 'Updating Segment Details',
  resolve_activity: 'Resolving Activity',
  lookup_playlist: 'Looking Up Playlist',
  insert_history: 'Inserting Listening History',
  query_isrc_stats: 'Reading Playlist Order',
  send_to_spotify: 'Sending to Spotify',
  verify_spotify: 'Verifying on Spotify',
  report_shuffle: 'Sending New Order to Spotify',
};

/**
 * Ordered list of all step IDs (execution order)
 */
export const STEP_ORDER: readonly ProcessStepId[] = [
  'sync_activities',
  'resolve_activity',
  // Playlist-shuffle sequence (only for last_run + non-no_music), per AC-5
  'lookup_playlist',
  'insert_history',
  'query_isrc_stats',
  'send_to_spotify',
  'verify_spotify',
  'report_shuffle',
  // Activity post-processing on the resolved activity
  'sync_details',
  'insert_heartrate',
  'assign_elevation_reference_time',
  'smooth_elevation_spikes_by_time',
  'smooth_elevation_python_time',
  'update_elevation_reference_by_time',
  'resample_activity_to_distance',
  'smooth_elevation_spikes_by_distance',
  'smooth_elevation_python_distance',
  'smooth_elevation_python_reference',
  'update_elevation_reference_by_distance',
  'build_activity_path',
  'segment_match_segments',
  'segment_pair_generation',
  'segment_polygon_match',
  'segment_mass_confirm_1',
  'segment_hausdorff_match',
  'segment_mass_confirm_2',
  'segment_frechet_match',
  'segment_mass_confirm_3',
  'segment_update_details',
];
