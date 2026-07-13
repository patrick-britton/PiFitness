/**
 * Admin React Query Hooks
 * Provides React Query hooks for all admin API endpoints with
 * automatic caching, background refetch, and mutation invalidation.
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API, ApiListResponse, ApiStatusResponse } from "../lib/api-client";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const adminKeys = {
  all: ["admin"] as const,
  tasks: () => [...adminKeys.all, "tasks"] as const,
  taskSchedule: () => [...adminKeys.all, "tasks", "schedule"] as const,
  taskNames: () => [...adminKeys.all, "tasks", "names"] as const,
  taskPerformance: (taskId: number) => [...adminKeys.all, "tasks", taskId, "performance"] as const,
  events: (filters?: Record<string, unknown>) =>
    [...adminKeys.all, "events", filters] as const,
  dbSessions: () => [...adminKeys.all, "db-sessions"] as const,
  services: () => [...adminKeys.all, "services"] as const,
  functions: () => [...adminKeys.all, "functions"] as const,
  credentialRequirements: () => [...adminKeys.all, "credentials", "requirements"] as const,
  logTables: () => [...adminKeys.all, "logs", "tables"] as const,
  logData: (tableName: string) => [...adminKeys.all, "logs", "data", tableName] as const,
  taskSummaryChart: () => [...adminKeys.all, "db-info", "task-summary"] as const,
  dbSizeChart: () => [...adminKeys.all, "db-info", "db-size-chart"] as const,
  dbSizeBreakdown: () => [...adminKeys.all, "db-info", "db-size-breakdown"] as const,
};

// ---------------------------------------------------------------------------
// Task Monitoring Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch task execution history.
 * Auto-refreshes every 10 seconds for near-real-time monitoring.
 */
export function useTasks() {
  return useQuery({
    queryKey: adminKeys.tasks(),
    queryFn: () => API.admin.getTasks(),
    refetchInterval: 10_000,
  });
}

/**
 * Hook to fetch task scheduling configuration.
 */
export function useTaskSchedule() {
  return useQuery({
    queryKey: adminKeys.taskSchedule(),
    queryFn: () => API.admin.getTaskSchedule(),
    refetchInterval: 30_000,
  });
}

/**
 * Hook to fetch distinct task names.
 */
export function useTaskNames() {
  return useQuery({
    queryKey: adminKeys.taskNames(),
    queryFn: () => API.admin.getTaskNames(),
    staleTime: 60_000,
  });
}

/**
 * Mutation to execute a task by name.
 */
export function useExecuteTask() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (taskName: string) => API.admin.executeTask(taskName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
    },
  });
}

/**
 * Mutation to execute a task by name using the enhanced v2 execution engine.
 */
export function useExecuteTaskV2() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ taskName, taskId }: { taskName: string; taskId?: number }) => API.admin.executeTaskV2(taskName, taskId),
    onSuccess: (data) => {
      // Don't invalidate yet - the task is still running
      // The polling hook will handle updates when the task completes
    },
  });
}

/**
 * Hook to poll for task execution status.
 * Auto-refreshes every 2 seconds while task is running, stops when complete.
 */
export function useTaskExecution(executionId: number | null) {
  return useQuery({
    queryKey: [...adminKeys.all, "task-executions", executionId],
    queryFn: () => API.admin.getTaskExecutionStatus(executionId as number),
    enabled: !!executionId,
    refetchInterval: (query) => {
      // Stop polling if no executionId or if execution is complete
      if (!executionId) return false;
      
      // If we got a 404 or error, stop polling
      if (query.state.error) {
        return false;
      }
      
      const data = query.state.data;
      if (data && (data.status === 'success' || data.status === 'failed')) {
        return false; // Stop polling
      }
      return 2000; // Poll every 2 seconds while running
    },
    staleTime: 1000,
    retry: false, // Don't retry on 404
  });
}

/**
 * Mutation to update a task configuration.
 */
export function useUpdateTaskConfig() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ taskId, config }: { 
      taskId: number; 
  config: {
    task_frequency: string;
    description?: string;
    display_icon?: string;
    priority?: number;
    hours?: number;
    interval_minutes?: number;
    stop_hour?: number;
    api_function?: string;
    python_function?: string;
  }
    }) =>
      API.admin.updateTaskConfig(taskId, config),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.taskSchedule() });
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
      queryClient.invalidateQueries({ queryKey: adminKeys.taskSummaryChart() });
    },
  });
}

/**
 * Mutation to delete a task configuration.
 */
export function useDeleteTaskConfig() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (taskId: number) => API.admin.deleteTaskConfig(taskId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.taskSchedule() });
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
      queryClient.invalidateQueries({ queryKey: adminKeys.taskNames() });
    },
  });
}

  /**
   * Mutation to create a new task.
   */
  export function useCreateTask() {
    const queryClient = useQueryClient();
    return useMutation({
      mutationFn: (task: { 
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
        API.admin.createTask(task),
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
        queryClient.invalidateQueries({ queryKey: adminKeys.taskSchedule() });
        queryClient.invalidateQueries({ queryKey: adminKeys.taskNames() });
        queryClient.invalidateQueries({ queryKey: adminKeys.taskSummaryChart() });
      },
    });
  }

/**
 * Query to fetch full task configuration for a specific task (for edit dialog).
 */
export function useTaskConfig(taskId: number) {
  return useQuery({
    queryKey: [...adminKeys.all, "tasks", taskId, "config"] as const,
    queryFn: () => API.admin.getTaskConfig(taskId),
    enabled: !!taskId,
    staleTime: 60_000,
  });
}

/**
 * Query to fetch execution logs for a specific task.
 */
export function useTaskLogs(taskId: number) {
  return useQuery({
    queryKey: [...adminKeys.all, "tasks", taskId, "logs"] as const,
    queryFn: () => API.admin.getTaskLogs(taskId),
    enabled: !!taskId,
    staleTime: 30_000,
  });
}

/**
 * Query to fetch task performance data for a specific task.
 */
export function useTaskPerformance(taskId: number | null) {
  return useQuery({
    queryKey: adminKeys.taskPerformance(taskId as number),
    queryFn: () => API.admin.getTaskPerformance(taskId as number),
    enabled: !!taskId,
    staleTime: 30_000,
  });
}

/**
 * Mutation to upsert a fact configuration.
 */
export function useUpsertFactConfig() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (config: { fact_id?: number | null; task_id: number; staging_id: number; is_active: boolean; custom_params?: any }) =>
      API.admin.upsertFactConfig(config),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
    },
  });
}

/**
 * Mutation to delete a fact configuration.
 */
export function useDeleteFactConfig() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (factId: number) => API.admin.deleteFactConfig(factId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
    },
  });
}

// ---------------------------------------------------------------------------
// Event History Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch event history with optional filters.
 */
export function useEvents(filters?: { search?: string; errors_only?: boolean; ignore_skips?: boolean; event_type?: string; limit?: number }) {
  return useQuery({
    queryKey: adminKeys.events(filters as Record<string, unknown>),
    queryFn: () => API.admin.getEvents(filters),
    staleTime: 10_000,
  });
}

// ---------------------------------------------------------------------------
// Database Session Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch active DB sessions.
 * Auto-refreshes every 5 seconds for real-time monitoring.
 */
export function useDBSessions() {
  return useQuery({
    queryKey: adminKeys.dbSessions(),
    queryFn: () => API.admin.getDBSessions(),
    refetchInterval: 5_000,
  });
}

/**
 * Mutation to kill a database session by PID.
 */
export function useKillDBSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (pid: number) => API.admin.killDBSession(pid),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.dbSessions() });
    },
  });
}

// ---------------------------------------------------------------------------
// API Service Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch API services list.
 */
export function useServices() {
  return useQuery({
    queryKey: adminKeys.services(),
    queryFn: () => API.admin.getServices(),
    staleTime: 30_000,
  });
}

/**
 * Mutation to add a new API service.
 */
export function useAddService() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (serviceName: string) => API.admin.addService(serviceName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.services() });
    },
  });
}

/**
 * Mutation to delete an API service.
 */
export function useDeleteService() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (serviceName: string) => API.admin.deleteService(serviceName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.services() });
    },
  });
}

// ---------------------------------------------------------------------------
// Function Library Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch function library entries.
 */
export function useFunctions() {
  return useQuery({
    queryKey: adminKeys.functions(),
    queryFn: () => API.admin.getFunctions(),
    staleTime: 30_000,
  });
}

/**
 * Mutation to add a function library entry.
 */
export function useAddFunction() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (entry: { friendly_name: string; api_service_name: string; python_extraction_function: string; description?: string }) =>
      API.admin.addFunction(entry),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.functions() });
    },
  });
}

/**
 * Mutation to update a function library entry.
 */
export function useUpdateFunction() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ friendlyName, entry }: { friendlyName: string; entry: { friendly_name: string; api_service_name: string; python_extraction_function: string; description?: string } }) =>
      API.admin.updateFunction(friendlyName, entry),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.functions() });
    },
  });
}

/**
 * Mutation to delete a function library entry.
 */
export function useDeleteFunction() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (friendlyName: string) => API.admin.deleteFunction(friendlyName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.functions() });
    },
  });
}

// ---------------------------------------------------------------------------
// Credential Management Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch credential requirements.
 */
export function useCredentialRequirements() {
  return useQuery({
    queryKey: adminKeys.credentialRequirements(),
    queryFn: () => API.admin.getCredentialRequirements(),
    staleTime: 60_000,
  });
}

/**
 * Mutation to upsert credentials.
 */
export function useUpsertCredentials() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ serviceName, rawCredentialsJson }: { serviceName: string; rawCredentialsJson: string }) =>
      API.admin.upsertCredentials(serviceName, rawCredentialsJson),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.credentialRequirements() });
    },
  });
}

/**
 * Mutation to delete credentials.
 */
export function useDeleteCredentials() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (serviceName: string) => API.admin.deleteCredentials(serviceName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.credentialRequirements() });
    },
  });
}

// ---------------------------------------------------------------------------
// DB Info (Charting) Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch task summary chart data for DB Info.
 */
export function useTaskSummaryChart() {
  return useQuery({
    queryKey: adminKeys.taskSummaryChart(),
    queryFn: () => API.admin.getTaskSummaryChart(),
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch historical DB size chart data.
 */
export function useDbSizeChart() {
  return useQuery({
    queryKey: adminKeys.dbSizeChart(),
    queryFn: () => API.admin.getDbSizeChart(),
    staleTime: 60_000,
  });
}

/**
 * Hook to fetch current DB size breakdown data.
 */
export function useDbSizeBreakdown() {
  return useQuery({
    queryKey: adminKeys.dbSizeBreakdown(),
    queryFn: () => API.admin.getDbSizeBreakdown(),
    staleTime: 60_000,
  });
}

// ---------------------------------------------------------------------------
// Log Table Viewer Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch list of log tables.
 */
export function useLogTables() {
  return useQuery({
    queryKey: adminKeys.logTables(),
    queryFn: () => API.admin.getLogTables(),
    staleTime: 60_000,
  });
}

/**
 * Hook to fetch log data for a specific table.
 */
export function useLogData(tableName: string, limit?: number) {
  return useQuery({
    queryKey: adminKeys.logData(tableName),
    queryFn: () => API.admin.getLogData(tableName, limit),
    enabled: !!tableName,
  });
}