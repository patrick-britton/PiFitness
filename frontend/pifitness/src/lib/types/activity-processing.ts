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
 */
export interface ProcessActivityRequest {
  /** Playlist name selection. 'Manual Processing' shows datetime inputs. */
  playlist_name?: 'Running' | 'Jogging' | 'No Playlist' | 'Manual Processing';
  /** ISO 8601 datetime — required when playlist_name is 'Manual Processing' */
  manual_start_utc?: string;
  /** ISO 8601 datetime — required when playlist_name is 'Manual Processing' */
  manual_end_utc?: string;
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
  | 'auto_shuffle'
  | 'cleanup';

/**
 * Step result data (populated on completion of relevant steps)
 */
export interface ProcessStepResultData {
  /** Number of songs heard (from lookup_playlist) */
  song_count?: number;
  /** First song heard (from lookup_playlist) */
  first_song?: string;
  /** Last song heard (from lookup_playlist) */
  last_song?: string;
  /** Whether the playlist was successfully shuffled (from auto_shuffle) */
  playlist_shuffled?: boolean;
  /** The Spotify playlist ID (from lookup_playlist) */
  playlist_id?: string;
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
 * End-of-run summary embedded in the terminal event on success (AC-9).
 * Null values mean the corresponding steps were skipped / not applicable.
 */
export interface ProcessSummaryData {
  /** Playlist was shuffled (null when playlist steps were skipped) */
  playlist_shuffled?: boolean | null;
  /** Number of segments matched for the processed activity (null when segment steps were skipped) */
  segments_matched?: number | null;
  /** Whether any matched segment is a course (null when segment steps were skipped) */
  course_found?: boolean | null;
  /** Name of the matched course, when course_found is true */
  course_name?: string | null;
  /** Activity the summary refers to (the fake activity id in Manual Processing) */
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
export type NdjsonEvent = ProcessStepEvent | ProcessCompleteEvent;

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
  lookup_playlist: 'Looking Up Playlist',
  insert_history: 'Inserting Listening History',
  auto_shuffle: 'Reshuffling Playlist',
  cleanup: 'Cleanup',
};

/**
 * Ordered list of all step IDs (execution order)
 */
export const STEP_ORDER: readonly ProcessStepId[] = [
  'sync_activities',
  'sync_details',
  'match_segments',
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
  'lookup_playlist',
  'insert_history',
  'auto_shuffle',
  'cleanup',
];
