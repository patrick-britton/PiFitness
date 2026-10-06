/**
 * PiFitness API Client
 * Environment-aware fetch wrapper with comprehensive error handling
 */

import {
  ProcessActivityRequest,
  ProcessStepStartEvent,
  ProcessStepEvent,
  ProcessCompleteEvent,
} from './types/activity-processing';
import {
  ActivityReport,
  ActivityReportType,
} from './types/activity-report';
import {
  Leaderboard,
  LeaderboardEffortSelection,
  LeaderboardVisualsRequest,
  MapConfig,
  SegmentListQuery,
  SegmentListRow,
  SegmentVisuals,
} from './types/leaderboards';
import {
  TriTipEvent,
  TriTipReading,
  TriTipEventDetail,
  TriTipActiveResponse,
  TriTipInitiateRequest,
  TriTipPlaceRequest,
  TriTipReadingRequest,
} from './types/tri-tip';
import {
  VolleyballGame,
  VolleyballPoint,
  VolleyballActiveResponse,
  VolleyballHistoryResponse,
  VolleyballCreateGameRequest,
  VolleyballAddPointRequest,
  VolleyballTagEventRequest,
  VolleyballScoringTeam,
} from './types/volleyball';
import {
  ExerciseTimer,
  ExerciseAttempt,
  ExerciseTimerSummary,
  ExerciseListResponse,
  ExerciseDetailResponse,
  ExerciseCreateRequest,
  ExerciseUpdateRequest,
  ExerciseAttemptCreateRequest,
} from './types/exercises';
import {
  ActivitiesQuery,
  ActivitiesResponse,
  RouteQuery,
  RouteResponse,
  CreateSegmentRequest,
  CreateSegmentResponse,
  MapStylesResponse,
  CandidatesResponse,
  ConfirmMatchRequest,
  RejectMatchRequest,
  MatchOperationResponse,
  BulkConfirmRequest,
  BulkRejectRequest,
  BulkRejectResponse,
  HausdorffScoringRequest,
  FrechetScoringRequest,
  ScoringResponse,
  DeleteSegmentResponse,
  ResetMatchTablesRequest,
  ResetResponse,
  RenameSegmentRequest,
  RenameResponse,
  CoursesQuery,
  CoursesResponse,
} from './types/segment-management';
import {
  FoodLookupResponse,
} from './types/food-lookup';
import {
  UsualFood,
  DayEntry,
  DiaryStats,
  DiaryUpdateRequest,
  LogEntryRequest,
  LogApiEntryRequest,
  CombinedSearchResult,
  NutrientSnapshot,
  RecipeSummary,
  FoodListResponse,
  FoodRow,
} from './types/food-contract';
import {
  NowPlayingResponse,
  MusicActionResponse,
  MusicAddTargetsResponse,
  RecentPlaysResponse,
  ServiceStatus,
  MusicRatingsEligibleCountResponse,
  MatchupResponse,
  ScoreRequest,
  ScoreResponse,
  MusicRatingsEligiblePlaylistsResponse,
  ShufflePlaylistsResponse,
  ShuffleData,
  ShuffleConfigBody,
  ShuffleFlagsBody,
  ShufflePreviewResponse,
  ShuffleSendRequest,
  ShuffleSendResponse,
  IsrcDupeCountResponse,
  IsrcDupeMatchResponse,
  IsrcDupeDecisionRequest,
  IsrcDupeDecisionResponse,
} from './types/music';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

/**
 * Typed API fetch function with error handling
 * @param path API endpoint path
 * @param options Fetch options
 * @returns Promise with typed response
 */
export async function fetchAPI<T>(path: string, options?: RequestInit): Promise<T> {
  try {
    const url = `${API_BASE}${path}`;
    const response = await fetch(url, {
      headers: {
        "Content-Type": "application/json",
        ...options?.headers,
      },
      ...options,
    });

    if (!response.ok) {
      const errorText = await response.text();
      const error = new Error(errorText || `API request failed with status ${response.status}`);
      console.error(`[${new Date().toISOString()}] API Error ${response.status}: ${path}`, error);
      throw error;
    }

    return response.json() as Promise<T>;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Network/API Error: ${path}`, error);
    throw error;
  }
}

/**
 * API response wrapper types
 */
export interface ApiListResponse<T> {
  data: T[];
  count: number;
}

export interface ApiStatusResponse {
  status: string;
  message?: string;
  result?: any;
}

/**
 * Read an NDJSON stream from a Response body, calling onStep for each event.
 * Returns a promise that resolves with the terminal ProcessCompleteEvent.
 */
export async function readNdjsonStream(
  response: Response,
  onStep: (event: ProcessStepStartEvent | ProcessStepEvent) => void
): Promise<ProcessCompleteEvent> {
  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error("Response body is not readable");
  }

  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // Process complete lines from the buffer
      const lines = buffer.split("\n");
      // Keep the last (potentially incomplete) line in the buffer
      buffer = lines.pop() || "";

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        const event = JSON.parse(trimmed);

        // Check if this is the terminal event
        if (event.complete === true) {
          return event as ProcessCompleteEvent;
        }

                // Otherwise it's a step start (running) or completion event
        onStep(event as ProcessStepStartEvent | ProcessStepEvent);
      }
    }

    // If we exit the loop without a terminal event, something went wrong
    throw new Error("Stream ended without terminal event");
  } finally {
    reader.releaseLock();
  }
}

/**
 * API Client with module-specific endpoints
 */
export const API = {
  /**
   * Admin API endpoints — full coverage matching SP-02 design
   */
  admin: {
    // Task Monitoring
    getTasks: () => fetchAPI<ApiListResponse<any>>("/api/admin/tasks"),
    getTaskSchedule: () => fetchAPI<ApiListResponse<any>>("/api/admin/tasks/schedule"),
    getTaskNames: () => fetchAPI<ApiListResponse<string>>("/api/admin/tasks/names"),
    executeTask: (taskName: string) => fetchAPI<ApiStatusResponse>(`/api/admin/tasks/${encodeURIComponent(taskName)}/execute`, { method: "POST" }),
    executeTaskV2: (taskName: string, taskId?: number) => fetchAPI<{ execution_id: number; status: string; message: string }>(`/api/admin/tasks/v2/${encodeURIComponent(taskName)}/execute`, { 
      method: "POST", 
      body: taskId !== undefined ? JSON.stringify({ task_id: taskId }) : undefined
    }),
    updateTaskConfig: (taskId: number, config: {
      task_frequency: string;
      description?: string;
      display_icon?: string;
      priority?: number;
      hours?: number;
      interval_minutes?: number;
      stop_hour?: number;
      api_function?: string;
      python_function?: string;
    }) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/tasks/${taskId}`, {
        method: "PUT",
        body: JSON.stringify(config),
      }),
    deleteTaskConfig: (taskId: number) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/tasks/schedule/${taskId}`, { method: "DELETE" }),
    createTask: (task: {
      task_name: string;
      description?: string;
      task_frequency?: string;
      display_icon?: string;
      priority?: number;
      hours?: number;
      interval_minutes?: number;
      api_function?: string;
      python_function?: string;
    }) =>
      fetchAPI<ApiStatusResponse>("/api/admin/tasks", {
        method: "POST",
        body: JSON.stringify(task),
      }),
    getTaskConfig: (taskId: number) =>
      fetchAPI<{ data: any }>(`/api/admin/tasks/${taskId}/config`),
    getTaskLogs: (taskId: number, limit?: number) => {
      const params = limit ? `?limit=${limit}` : "";
      return fetchAPI<ApiListResponse<any>>(`/api/admin/tasks/${taskId}/logs${params}`);
    },
    getTaskPerformance: (taskId: number) =>
      fetchAPI<ApiListResponse<any>>(`/api/admin/tasks/${taskId}/performance`),
    getTaskExecutionStatus: (executionId: number) =>
      fetchAPI<any>(`/api/admin/tasks/executions/${executionId}`),

    // Fact Configuration
    upsertFactConfig: (config: { fact_id?: number | null; task_id: number; staging_id: number; is_active: boolean; custom_params?: any }) =>
      fetchAPI<ApiStatusResponse>("/api/admin/tasks/facts", {
        method: "POST",
        body: JSON.stringify(config),
      }),
    deleteFactConfig: (factId: number) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/tasks/facts/${factId}`, { method: "DELETE" }),

    // Event History
    getEvents: (params?: { search?: string; errors_only?: boolean; ignore_skips?: boolean; event_type?: string; limit?: number }) => {
      const searchParams = new URLSearchParams();
      if (params?.search) searchParams.append("search", params.search);
      if (params?.errors_only) searchParams.append("errors_only", "true");
      if (params?.ignore_skips) searchParams.append("ignore_skips", "true");
      if (params?.event_type) searchParams.append("event_type", params.event_type);
      if (params?.limit) searchParams.append("limit", params.limit.toString());
      const qs = searchParams.toString();
      return fetchAPI<ApiListResponse<any>>(`/api/admin/events${qs ? `?${qs}` : ""}`);
    },

    // Database Sessions
    getDBSessions: () => fetchAPI<ApiListResponse<any>>("/api/admin/db-sessions"),
    killDBSession: (pid: number) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/db-sessions/${pid}`, { method: "DELETE" }),

    // API Services
    getServices: () => fetchAPI<ApiListResponse<any>>("/api/admin/services"),
    addService: (serviceName: string, credentialRequirements?: string) =>
      fetchAPI<ApiStatusResponse>("/api/admin/services", {
        method: "POST",
        body: JSON.stringify({ 
          service_name: serviceName, 
          credential_requirements: credentialRequirements 
        }),
      }),
    updateService: (serviceName: string, credentialRequirements: string) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/services/${encodeURIComponent(serviceName)}`, {
        method: "PUT",
        body: JSON.stringify({ credential_requirements: credentialRequirements }),
      }),
    deleteService: (serviceName: string) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/services/${encodeURIComponent(serviceName)}`, { method: "DELETE" }),

    // Function Library
    getFunctions: () => fetchAPI<ApiListResponse<any>>("/api/admin/functions"),
    addFunction: (entry: { friendly_name: string; api_service_name: string; python_extraction_function: string; description?: string }) =>
      fetchAPI<ApiStatusResponse>("/api/admin/functions", {
        method: "POST",
        body: JSON.stringify(entry),
      }),
    updateFunction: (friendlyName: string, entry: { friendly_name: string; api_service_name: string; python_extraction_function: string; description?: string }) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/functions/${encodeURIComponent(friendlyName)}`, {
        method: "PUT",
        body: JSON.stringify(entry),
      }),
    deleteFunction: (friendlyName: string) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/functions/${encodeURIComponent(friendlyName)}`, { method: "DELETE" }),

    // Credentials
    getCredentialRequirements: () => fetchAPI<ApiListResponse<any>>("/api/admin/credentials/requirements"),
    upsertCredentials: (serviceName: string, rawCredentialsJson: string) =>
      fetchAPI<ApiStatusResponse>("/api/admin/credentials", {
        method: "POST",
        body: JSON.stringify({ api_service_name: serviceName, raw_credentials_json_string: rawCredentialsJson }),
      }),
    deleteCredentials: (serviceName: string) =>
      fetchAPI<ApiStatusResponse>(`/api/admin/credentials/${encodeURIComponent(serviceName)}`, { method: "DELETE" }),

    // Log Table Viewer
    getLogTables: () => fetchAPI<ApiListResponse<string>>("/api/admin/logs/tables"),
    getLogData: (tableName: string, limit?: number) => {
      const params = limit ? `?limit=${limit}` : "";
      return fetchAPI<ApiListResponse<any>>(`/api/admin/logs/data/${encodeURIComponent(tableName)}${params}`);
    },

    // DB Info (Charting)
    getTaskSummaryChart: () => fetchAPI<ApiListResponse<any>>("/api/admin/db-info/task-summary"),
    getDbSizeChart: () => fetchAPI<ApiListResponse<any>>("/api/admin/db-info/db-size-chart"),
    getDbSizeBreakdown: () => fetchAPI<ApiListResponse<any>>("/api/admin/db-info/db-size-breakdown"),
  },

  /**
   * Health API endpoints
   */
  health: {
    getHeartRate: (startDate?: string, endDate?: string, limit?: number) => {
      const params = new URLSearchParams();
      if (startDate) params.append("start_date", startDate);
      if (endDate) params.append("end_date", endDate);
      if (limit) params.append("limit", limit.toString());
      return fetchAPI<any[]>(`/api/health/heartrate?${params.toString()}`);
    },
    getSleepData: (startDate?: string, endDate?: string) => {
      const params = new URLSearchParams();
      if (startDate) params.append("start_date", startDate);
      if (endDate) params.append("end_date", endDate);
      return fetchAPI<any[]>(`/api/health/sleep?${params.toString()}`);
    },
    getWeightTargets: () => fetchAPI<any[]>("/api/health/weight-targets"),
  },

    /**
   * Music API endpoints
   */
  music: {
    getPlaylists: () => fetchAPI<any[]>("/api/music/playlists"),
    getPlaylistTracks: (playlistId: string) => fetchAPI<any[]>(`/api/music/playlists/${playlistId}/tracks`),
    getRatings: () => fetchAPI<MusicRatingsEligiblePlaylistsResponse>("/api/music/ratings"),
    getRatingsEligibleCount: () => fetchAPI<MusicRatingsEligibleCountResponse>("/api/music/ratings/eligible-count"),
    getRecentPlays: (limit?: number) =>
      fetchAPI<RecentPlaysResponse>(
        `/api/music/recent-plays${limit !== undefined ? `?limit=${limit}` : ""}`
      ),
    getServiceStatus: () => fetchAPI<ServiceStatus>("/api/music/service-status"),
    getNowPlaying: () => fetchAPI<NowPlayingResponse>("/api/music/now-playing"),
    skipTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/skip", { method: "POST" }),
    promoteTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/promote", { method: "POST" }),
    softRejectTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/soft-reject", { method: "POST" }),
    hardRejectTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/hard-reject", { method: "POST" }),
    removeTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/remove", { method: "POST" }),
    rankUpTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/rank-up", { method: "POST" }),
    rankDownTrack: () =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/rank-down", { method: "POST" }),
    addToPlaylist: (playlistId: string) =>
      fetchAPI<MusicActionResponse>("/api/music/now-playing/add-to-playlist", {
        method: "POST",
        body: JSON.stringify({ playlist_id: playlistId }),
      }),
    getAddTargets: () => fetchAPI<MusicAddTargetsResponse>("/api/music/now-playing/add-targets"),
    getAlbumArt: (albumId: string) =>
      fetchAPI<Blob>(`/api/music/album-art/${albumId}`),
    shuffle: (config: any) => fetchAPI<any>("/api/music/shuffle", {
      method: "POST",
      body: JSON.stringify(config),
    }),
    recordRating: (ratingData: any) => fetchAPI<any>("/api/music/ratings", {
      method: "POST",
      body: JSON.stringify(ratingData),
    }),
    getMatchup: (playlistId?: string) =>
      fetchAPI<MatchupResponse>(
        `/api/music/ratings/matchup${playlistId ? `?playlist_id=${encodeURIComponent(playlistId)}` : ""}`
      ),
    scoreMatchup: (request: ScoreRequest) =>
      fetchAPI<ScoreResponse>(`/api/music/ratings/matchup/score?${new URLSearchParams({
        playlist_id: request.playlist_id,
        isrc: request.isrc,
        isrc_vs: request.isrc_vs,
        margin: String(request.margin),
      }).toString()}`, {
        method: "POST",
      }),
    getShufflePlaylists: () =>
      fetchAPI<ShufflePlaylistsResponse>("/api/music/shuffle/playlists"),
    getShuffleData: (playlistId: string) =>
      fetchAPI<ShuffleData>(
        `/api/music/shuffle?playlist_id=${encodeURIComponent(playlistId)}`
      ),
    getShufflePreview: (playlistId: string, body: ShuffleConfigBody) =>
      fetchAPI<ShufflePreviewResponse>(
        `/api/music/shuffle/preview?playlist_id=${encodeURIComponent(playlistId)}`,
        { method: "POST", body: JSON.stringify(body) }
      ),
    reconcileShuffleConfig: (playlistId: string, body: ShuffleConfigBody) =>
      fetchAPI<{ ok: boolean; message: string }>(
        `/api/music/shuffle/config?playlist_id=${encodeURIComponent(playlistId)}`,
        { method: "POST", body: JSON.stringify(body) }
      ),
    reconcileShuffleFlags: (playlistId: string, body: ShuffleFlagsBody) =>
      fetchAPI<{ ok: boolean; message: string }>(
        `/api/music/shuffle/flags?playlist_id=${encodeURIComponent(playlistId)}`,
        { method: "POST", body: JSON.stringify(body) }
      ),
    sendShuffle: (request: ShuffleSendRequest) =>
      fetchAPI<ShuffleSendResponse>("/api/music/shuffle/send", {
        method: "POST",
        body: JSON.stringify(request),
      }),
    getIsrcDupeCount: () =>
      fetchAPI<IsrcDupeCountResponse>("/api/music/isrc-dupes/count"),
    getIsrcDupeMatch: () =>
      fetchAPI<IsrcDupeMatchResponse>("/api/music/isrc-dupes/match"),
    decideIsrcDupe: (request: IsrcDupeDecisionRequest) =>
      fetchAPI<IsrcDupeDecisionResponse>("/api/music/isrc-dupes/decision", {
        method: "POST",
        body: JSON.stringify(request),
      }),
    rescanIsrcDupes: () =>
      fetchAPI<IsrcDupeDecisionResponse>("/api/music/isrc-dupes/rescan", {
        method: "POST",
      }),
  },

  /**
   * Auth API endpoints
   */
  auth: {
    getStatus: () => fetchAPI<{ services: Record<string, any> }>("/api/auth/status"),
    refreshSpotify: () => fetchAPI<ApiStatusResponse>("/api/auth/spotify/refresh", { method: "POST" }),
    getSpotifyAuthUrl: () => fetchAPI<{ auth_url: string; redirect_uri: string }>("/api/auth/spotify/auth-url"),
    spotifyCallback: (redirectUrl: string) =>
      fetchAPI<ApiStatusResponse>("/api/auth/spotify/callback", {
        method: "POST",
        body: JSON.stringify({ redirect_url: redirectUrl }),
      }),
    testSpotify: () => fetchAPI<ApiStatusResponse>("/api/auth/spotify/test", { method: "POST" }),
    testGarmin: () => fetchAPI<ApiStatusResponse>("/api/auth/garmin/test", { method: "POST" }),
    getHealth: () => fetchAPI<Record<string, any>>("/api/auth/health"),
  },

  /**
   * Tri-tip Timer API endpoints
   */
  triTip: {
    getEvents: () => fetchAPI<ApiListResponse<TriTipEvent>>("/api/food/tri-tip"),
    getActive: () => fetchAPI<TriTipActiveResponse>("/api/food/tri-tip/active"),
    getEvent: (id: number) => fetchAPI<TriTipEventDetail>(`/api/food/tri-tip/${id}`),
    initiate: (req: TriTipInitiateRequest) =>
      fetchAPI<TriTipEvent>("/api/food/tri-tip", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    placeMeat: (id: number, req: TriTipPlaceRequest) =>
      fetchAPI<TriTipEvent>(`/api/food/tri-tip/${id}/place`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    addReading: (id: number, req: TriTipReadingRequest) =>
      fetchAPI<TriTipReading>(`/api/food/tri-tip/${id}/readings`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    complete: (id: number) =>
      fetchAPI<TriTipEvent>(`/api/food/tri-tip/${id}/complete`, { method: "POST" }),
    abandon: (id: number) =>
      fetchAPI<{ success: boolean }>(`/api/food/tri-tip/${id}`, { method: "DELETE" }),
  },

  /**
   * Volleyball Scorekeeping API endpoints (Activities -> Beach)
   */
  volleyball: {
    getHistory: () => fetchAPI<VolleyballHistoryResponse>("/api/sports/volleyball"),
    getActive: () => fetchAPI<VolleyballActiveResponse>("/api/sports/volleyball/active"),
    createGame: (req: VolleyballCreateGameRequest) =>
      fetchAPI<VolleyballGame>("/api/sports/volleyball", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    addPoint: (id: number, req: VolleyballAddPointRequest) =>
      fetchAPI<VolleyballPoint>(`/api/sports/volleyball/${id}/points`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    removeLastPoint: (id: number, scoringTeam: VolleyballScoringTeam) =>
      fetchAPI<{ success: boolean }>(
        `/api/sports/volleyball/${id}/points/${scoringTeam}`,
        { method: "DELETE" }
      ),
    tagLastEvent: (id: number, req: VolleyballTagEventRequest) =>
      fetchAPI<VolleyballPoint>(
        `/api/sports/volleyball/${id}/points/latest/event`,
        { method: "POST", body: JSON.stringify(req) }
      ),
    endGame: (id: number) =>
      fetchAPI<VolleyballGame>(`/api/sports/volleyball/${id}/end`, { method: "POST" }),
    abandonGame: (id: number) =>
      fetchAPI<{ success: boolean }>(`/api/sports/volleyball/${id}`, { method: "DELETE" }),
  },

  /**
   * Exercise Timer API endpoints (Exercises -> Timer Activation / Timer Creation)
   */
  exercises: {
    listSummaries: () => fetchAPI<ExerciseListResponse>("/api/exercises"),
    getDetail: (id: number) => fetchAPI<ExerciseDetailResponse>(`/api/exercises/${id}`),
    create: (req: ExerciseCreateRequest) =>
      fetchAPI<ExerciseTimer>("/api/exercises", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    update: (id: number, req: ExerciseUpdateRequest) =>
      fetchAPI<ExerciseTimer>(`/api/exercises/${id}`, {
        method: "PUT",
        body: JSON.stringify(req),
      }),
    remove: (id: number) =>
      fetchAPI<{ success: boolean }>(`/api/exercises/${id}`, { method: "DELETE" }),
    saveAttempt: (id: number, req: ExerciseAttemptCreateRequest) =>
      fetchAPI<ExerciseAttempt>(`/api/exercises/${id}/attempts`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
  },

  /**
   * Activities API endpoints
   */
  activities: {
    getActivities: () => fetchAPI<any[]>("/api/activities"),
    getActivity: (activityId: string) => fetchAPI<any>(`/api/activities/${activityId}`),
    getSegments: () => fetchAPI<any[]>("/api/segments"),
    getSegmentMatches: (segmentId: string) => fetchAPI<any[]>(`/api/segments/${segmentId}/matches`),
    /**
     * Fetch the Recent Activity Report for the most recent Run or Walk activity (009-001).
     */
    getReport: (activityType: ActivityReportType) =>
      fetchAPI<ActivityReport>(`/api/activities/report?activity_type=${activityType}`),
    /**
     * Fetch the filterable segment/course list for the Leaderboards page (009-002).
     */
    getLeaderboardSegments: (query: SegmentListQuery) => {
      const params = new URLSearchParams();
      if (query.name) params.set("name", query.name);
      if (query.kind) params.set("kind", query.kind);
      if (query.min_distance > 0) params.set("min_distance", String(query.min_distance));
      if (query.activity_type) params.set("activity_type", query.activity_type);
      const qs = params.toString();
      return fetchAPI<SegmentListRow[]>(
        `/api/activities/leaderboard/segments${qs ? `?${qs}` : ""}`
      );
    },
    /**
     * Fetch the full effort leaderboard for one segment/course (009-002).
     */
    getLeaderboard: (segmentId: number) =>
      fetchAPI<Leaderboard>(`/api/activities/leaderboard/${segmentId}`),
    /**
     * Stage the selected efforts and fetch the generated replay + telemetry visuals (009-002).
     */
    postLeaderboardVisuals: (request: LeaderboardVisualsRequest) =>
      fetchAPI<SegmentVisuals>("/api/activities/leaderboard/visuals", {
        method: "POST",
        body: JSON.stringify(request),
      }),
    /**
     * Process an activity via NDJSON streaming.
     * Calls onStep for each step-completion event.
     * Returns a promise that resolves with the terminal event on stream completion.
     */
    processActivity: async (
      request: ProcessActivityRequest,
      onStep: (event: ProcessStepStartEvent | ProcessStepEvent) => void
    ): Promise<ProcessCompleteEvent> => {
      const url = `${API_BASE}/api/activities/process`;
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `API request failed with status ${response.status}`);
      }

      return readNdjsonStream(response, onStep);
    },
  },

  /**
   * Segment Management API endpoints (009-003).
   */
  segments: {
    /**
     * Source-activity selection list for Create Segment mode (FR-1).
     * Server-side `activity_type` filter + limit/offset paging.
     */
    getActivities: (query?: ActivitiesQuery & { limit?: number; offset?: number }) => {
      const params = new URLSearchParams();
      if (query?.activity_type) params.set("activity_type", query.activity_type);
      if (query?.limit != null) params.set("limit", String(query.limit));
      if (query?.offset != null) params.set("offset", String(query.offset));
      const qs = params.toString();
      return fetchAPI<ActivitiesResponse>(`/api/segments/activities${qs ? `?${qs}` : ""}`);
    },
    /**
     * Activity route + elevation, optionally trimmed by the start/end gates in
     * meters (FR-2/FR-4). Omitted gates return the full activity range.
     */
    getRoute: (activityId: number, query?: Partial<RouteQuery>) => {
      const params = new URLSearchParams();
      if (query?.start_m != null) params.set("start_m", String(query.start_m));
      if (query?.end_m != null) params.set("end_m", String(query.end_m));
      const qs = params.toString();
      return fetchAPI<RouteResponse>(`/api/segments/${activityId}/route${qs ? `?${qs}` : ""}`);
    },
    /** Create the trimmed range as a course or a segment (FR-5). */
    createSegment: (req: CreateSegmentRequest) =>
      fetchAPI<CreateSegmentResponse>("/api/segments/create", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /**
     * Basemap catalogue + satellite availability for the map style selector
     * (FR-3); token-gated styles appear only when the token is configured.
     */
    getMapStyles: () => fetchAPI<MapStylesResponse>("/api/segments/map-styles"),
    /** Filterable segment/course list for Match Activities picker (FR-7). */
    getSegmentsList: (query?: SegmentListQuery) => {
      const params = new URLSearchParams();
      if (query?.name != null) params.set("name", query.name);
      if (query?.min_distance != null) params.set("min_distance", String(query.min_distance));
      if (query?.kind != null) params.set("kind", query.kind);
      if (query?.activity_type != null) params.set("activity_type", query.activity_type);
      const qs = params.toString();
      return fetchAPI<ApiListResponse<SegmentListRow>>(`/api/segments/list${qs ? `?${qs}` : ""}`);
    },
        /** Trigger the find-matches pipeline (FR-9, OQ-1). */
    findMatches: (segmentId: number) =>
      fetchAPI<ScoringResponse>(`/api/segments/${segmentId}/matches/find`, { method: "POST" }),
    /**
     * Run ONE step of the find-matches pipeline (T20). The UI calls steps
     * 1..4 in sequence to show per-step progress; same SPs as the blocking
     * call above.
     */
    findMatchesStep: (segmentId: number, step: number) =>
      fetchAPI<{ message: string }>(`/api/segments/${segmentId}/matches/find/${step}`, {
        method: "POST",
      }),
    /** Existing matches + candidates for a segment (FR-9, FR-10). */
    getCandidates: (segmentId: number) =>
      fetchAPI<CandidatesResponse>(`/api/segments/${segmentId}/matches/candidates`),
    /** Confirm one candidate effort (FR-11). */
    confirmMatch: (segmentId: number, req: ConfirmMatchRequest) =>
      fetchAPI<MatchOperationResponse>(`/api/segments/${segmentId}/matches/confirm`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Reject one candidate effort (FR-11). */
    rejectMatch: (segmentId: number, req: RejectMatchRequest) =>
      fetchAPI<MatchOperationResponse>(`/api/segments/${segmentId}/matches/reject`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Mass-approve remaining candidates (FR-11). */
    bulkConfirm: (segmentId: number, req: BulkConfirmRequest) =>
      fetchAPI<MatchOperationResponse>(`/api/segments/${segmentId}/matches/bulk-confirm`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Mass-reject remaining candidates, optionally only confidence > threshold (009-003 T31). */
    bulkReject: (segmentId: number, req: BulkRejectRequest = {}) =>
      fetchAPI<BulkRejectResponse>(`/api/segments/${segmentId}/matches/bulk-reject`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Run Hausdorff scoring on candidates (FR-12). */
    hausdorffScoring: (segmentId: number, req: HausdorffScoringRequest) =>
      fetchAPI<ScoringResponse>(`/api/segments/${segmentId}/scoring/hausdorff`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Run Fréchet scoring on candidates (FR-12). */
    frechetScoring: (segmentId: number, req: FrechetScoringRequest) =>
      fetchAPI<ScoringResponse>(`/api/segments/${segmentId}/scoring/frechet`, {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Delete a segment and its matches/details (FR-13). */
    deleteSegment: (segmentId: number) =>
      fetchAPI<DeleteSegmentResponse>(`/api/segments/${segmentId}`, { method: "DELETE" }),
    /** Truncate match tables and reset segments identity (FR-14). */
    resetMatchTables: (req: ResetMatchTablesRequest) =>
      fetchAPI<ResetResponse>("/api/segments/reset", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    /** Rename a course (FR-16). */
    renameSegment: (segmentId: number, req: RenameSegmentRequest) =>
      fetchAPI<RenameResponse>(`/api/segments/${segmentId}/name`, {
        method: "PATCH",
        body: JSON.stringify(req),
      }),
    /** Paginated course review list with name/length filters (FR-15). */
    getCourses: (query?: CoursesQuery) => {
      const params = new URLSearchParams();
      if (query?.name != null) params.set("name", query.name);
      if (query?.min_distance != null) params.set("min_distance", String(query.min_distance));
      if (query?.max_distance != null) params.set("max_distance", String(query.max_distance));
      if (query?.page != null) params.set("page", String(query.page));
      if (query?.page_size != null) params.set("page_size", String(query.page_size));
      const qs = params.toString();
      return fetchAPI<CoursesResponse>(`/api/segments/courses${qs ? `?${qs}` : ""}`);
    },
  },

  /**
   * Food API raw-JSON diagnostic proxy (010-001, dev-only viewer).
   * Returns the upstream body verbatim; non-2xx upstream still arrives as
   * HTTP 200 with `upstreamStatus` set. Local proxy failures throw (the
   * fetchAPI error path) with the raw local-error body as the message.
   */
  foodLookup: {
    searchUsda: (query: string, pageSize = 10, dataType?: string) => {
      const params = new URLSearchParams();
      params.set('query', query);
      params.set('pageSize', String(pageSize));
      if (dataType?.trim()) params.set('dataType', dataType.trim());
      return fetchAPI<FoodLookupResponse>(`/api/food/lookup/usda?${params.toString()}`);
    },
    lookupOff: (barcode: string) => {
      const params = new URLSearchParams();
      params.set('barcode', barcode);
      return fetchAPI<FoodLookupResponse>(`/api/food/lookup/off?${params.toString()}`);
    },
  },

  /**
   * Food skeleton reads (010-002 T05) — contract shapes from /api/food.
   */
  food: {
    /** Time-of-day weighted usual foods. */
    usuals: (at?: string) =>
      fetchAPI<UsualFood[]>(
        `/api/food/usuals${at ? `?at=${encodeURIComponent(at)}` : ''}`,
      ),
    /** One day's snapshot history as DayEntry[]. */
    diary: (day: string) =>
      fetchAPI<DayEntry[]>(`/api/food/diary?day=${encodeURIComponent(day)}`),
    /** Three-bar header aggregates for a device-local day (010-003). */
    diaryStats: (day: string) =>
      fetchAPI<DiaryStats>(
        `/api/food/diary/stats?day=${encodeURIComponent(day)}`,
      ),
    /** Edit an entry; a new logged_at moves it to that day (010-003 OQ-5). */
    updateDiaryEntry: (entryId: number, body: DiaryUpdateRequest) =>
      fetchAPI<DayEntry>(`/api/food/diary/${entryId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    /** Delete an entry (snapshot history removed; the food row is untouched). */
    deleteDiaryEntry: (entryId: number) =>
      fetchAPI<{ success: boolean; entry_id: number }>(
        `/api/food/diary/${entryId}`,
        { method: 'DELETE' },
      ),
    /** Recipe Box list as RecipeSummary[]. */
    recipes: () => fetchAPI<RecipeSummary[]>('/api/food/recipes'),
    /** Top-10 time-of-day suggestions (010-003 T06). */
    suggest: (at?: string) =>
      fetchAPI<UsualFood[]>(
        `/api/food/suggest${at ? `?at=${encodeURIComponent(at)}` : ''}`,
      ),
    /** Tiered combined search; backend trims to ≤30, show-more is client-side (010-003 T06). */
    combinedSearch: (q: string, limit = 30) =>
      fetchAPI<CombinedSearchResult[]>(
        `/api/food/search?q=${encodeURIComponent(q)}&limit=${limit}`,
      ),
    /** Log a saved food; server snapshots name + nutrients (010-003 T06). */
    logEntry: (body: LogEntryRequest) =>
      fetchAPI<DayEntry>('/api/food/diary', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    /** Persist-on-log an API hit into foods.foods then log it (010-003 T06). */
    logApiEntry: (body: LogApiEntryRequest) =>
      fetchAPI<DayEntry>('/api/food/diary/from-api', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    /** Alive food row by barcode for the scan-first DB check (010-003 T08). */
    byBarcode: (barcode: string) =>
      fetchAPI<{
        found: boolean; barcode: string; food_id?: number | null;
        name: string; source?: string; fdc_id?: number | null;
        nutrients?: NutrientSnapshot | null;
        /** T10 (OQ-8): row serving for scan-DB popup default/panel. */
        serving_size?: number | null;
        serving_unit?: string | null;
        serving_text?: string | null;
      }>(`/api/food/by-barcode?barcode=${encodeURIComponent(barcode)}`),
    /** Food Database page as FoodListResponse (server-side ranked search). */
    foods: (query?: { q?: string; limit?: number }) => {
      const params = new URLSearchParams();
      if (query?.q != null && query.q !== '') params.set('q', query.q);
      if (query?.limit != null) params.set('limit', String(query.limit));
      const qs = params.toString();
      return fetchAPI<FoodListResponse>(`/api/food/foods${qs ? `?${qs}` : ''}`);
    },
    /** One alive food row — hydrates suggestion-pick panels (010-003 T10). */
    foodById: (foodId: number) => fetchAPI<FoodRow>(`/api/food/foods/${foodId}`),
  },

  /**
   * App-wide configuration endpoints (009-002 T08).
   */
  config: {
    /**
     * Client-safe map configuration — exposes the Mapbox token so the
     * Leaderboards visuals replay can build Mapbox-style tile URLs and gate
     * the style dropdown on token presence (AC-13).
     */
    getMapConfig: () => fetchAPI<MapConfig>("/api/config/map"),
  },
};