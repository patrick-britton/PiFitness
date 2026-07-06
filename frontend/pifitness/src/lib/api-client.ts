/**
 * PiFitness API Client
 * Environment-aware fetch wrapper with comprehensive error handling
 */

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
    executeTaskV2: (taskName: string) => fetchAPI<ApiStatusResponse>(`/api/admin/tasks/v2/${encodeURIComponent(taskName)}/execute`, { method: "POST" }),
    updateTaskConfig: (taskId: number, config: {
      task_frequency: string;
      description?: string;
      display_icon?: string;
      priority?: number;
      hours?: number;
      interval_minutes?: number;
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
    addService: (serviceName: string) =>
      fetchAPI<ApiStatusResponse>("/api/admin/services", {
        method: "POST",
        body: JSON.stringify({ service_name: serviceName }),
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
   * Activities API endpoints
   */
  activities: {
    getActivities: () => fetchAPI<any[]>("/api/activities"),
    getActivity: (activityId: string) => fetchAPI<any>(`/api/activities/${activityId}`),
    getSegments: () => fetchAPI<any[]>("/api/segments"),
    getSegmentMatches: (segmentId: string) => fetchAPI<any[]>(`/api/segments/${segmentId}/matches`),
  },
};
