/**
 * PiFitness API Client
 * Environment-aware fetch wrapper with comprehensive error handling
 */

import {
  ProcessActivityRequest,
  ProcessStepEvent,
  ProcessCompleteEvent,
} from './types/activity-processing';
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
  onStep: (event: ProcessStepEvent) => void
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

        // Otherwise it's a step event
        onStep(event as ProcessStepEvent);
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
    getRatings: () => fetchAPI<any[]>("/api/music/ratings"),
    getRatingsEligibleCount: () => fetchAPI<number>("/api/music/ratings/eligible-count"),
    getRecentPlays: () => fetchAPI<any[]>("/api/music/recent-plays"),
    shuffle: (config: any) => fetchAPI<any>("/api/music/shuffle", {
      method: "POST",
      body: JSON.stringify(config),
    }),
    recordRating: (ratingData: any) => fetchAPI<any>("/api/music/ratings", {
      method: "POST",
      body: JSON.stringify(ratingData),
    }),
    getNowPlaying: () => fetchAPI<any>("/api/music/now-playing"),
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
     * Process an activity via NDJSON streaming.
     * Calls onStep for each step-completion event.
     * Returns a promise that resolves with the terminal event on stream completion.
     */
    processActivity: async (
      request: ProcessActivityRequest,
      onStep: (event: ProcessStepEvent) => void
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
};