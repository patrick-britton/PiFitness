/**
 * Enhanced Task Management Component
 * Replaces Task Execution History with Task Summary view and adds full CRUD functionality.
 * Auto-refreshes every 10 seconds via React Query.
 */

'use client';

import { useState, useMemo, useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTasks, useTaskSchedule, useTaskNames, useExecuteTask, useExecuteTaskV2, useUpdateTaskConfig, useDeleteTaskConfig, useTaskSummaryChart, useCreateTask, useTaskLogs, useTaskConfig, useTaskExecution, adminKeys } from '@/hooks/useAdmin';
import TaskLogView from './TaskLogView';
import TaskPerformanceChart from './TaskPerformanceChart';

/**
 * Status badge component with color coding
 */
function StatusBadge({ status }: { status: string }) {
  const lowerStatus = (status || '').toLowerCase();
  let colorClass = 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';

  if (lowerStatus.includes('success') || lowerStatus.includes('completed')) {
    colorClass = 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
  } else if (lowerStatus.includes('error') || lowerStatus.includes('failed')) {
    colorClass = 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300';
  } else if (lowerStatus.includes('running') || lowerStatus.includes('executing') || lowerStatus.includes('in_progress')) {
    colorClass = 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300 animate-pulse';
  } else if (lowerStatus.includes('warning')) {
    colorClass = 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300';
  }

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colorClass}`}>
      {status || 'unknown'}
    </span>
  );
}

/**
 * Edit Task Config Dialog with Delete button and embedded logs
 */
function EditTaskDialog({
  task,
  onClose,
  onDelete,
}: {
  task: { 
    task_id: number; 
    task_name: string; 
    task_frequency: string;
    description?: string;
    display_icon?: string;
    priority?: number;
    hours?: number;
    interval_minutes?: number;
    api_function?: string;
    python_function?: string;
  };
  onClose: () => void;
  onDelete?: () => void;
}) {
  const [frequency, setFrequency] = useState(task.task_frequency || '');
  const [description, setDescription] = useState(task.description || '');
  const [displayIcon, setDisplayIcon] = useState(task.display_icon || '⚙️');
  const [priority, setPriority] = useState(task.priority ?? 0);
  const [hours, setHours] = useState(task.hours ?? 0);
  const [intervalMinutes, setIntervalMinutes] = useState(task.interval_minutes ?? 0);
  const [apiFunction, setApiFunction] = useState(task.api_function || '');
  const [pythonFunction, setPythonFunction] = useState(task.python_function || '');
  const updateConfig = useUpdateTaskConfig();

  // Fetch logs for this task
  const { data: logsData } = useTaskLogs(task.task_id);
  const logs = logsData?.data || [];

  const handleSave = () => {
    updateConfig.mutate(
      { 
        taskId: task.task_id, 
        config: { 
          task_frequency: frequency,
          description: description || undefined,
          display_icon: displayIcon || undefined,
          priority: priority,
          hours: hours,
          interval_minutes: intervalMinutes,
          api_function: apiFunction || undefined,
          python_function: pythonFunction || undefined,
        } 
      },
      { onSuccess: () => onClose() }
    );
  };

  const handleDeleteClick = () => {
    if (onDelete) {
      onDelete();
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-4xl mx-4 max-h-[90vh] flex flex-col">
         <div className="flex justify-between items-start mb-4">
           <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
             {task.task_name} <span className="text-gray-500 dark:text-gray-400 italic text-base font-normal">task_id={task.task_id}</span>
           </h3>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="flex-1 overflow-y-auto">
          <div className="space-y-4 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Frequency
              </label>
              <div className="flex flex-wrap gap-2">
                {['Hourly', 'Daily', 'Weekly', 'Monthly', 'Inactive'].map((freq) => (
                  <button
                    key={freq}
                    type="button"
                    onClick={() => setFrequency(freq)}
                    className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
                      frequency === freq
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                    }`}
                  >
                    {freq}
                  </button>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Description
              </label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={3}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Display Icon
                </label>
                <input
                  type="text"
                  value={displayIcon}
                  onChange={(e) => setDisplayIcon(e.target.value)}
                  placeholder="⚙️"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Priority
                </label>
                <input
                  type="number"
                  value={priority}
                  onChange={(e) => setPriority(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Hours
                </label>
                <input
                  type="number"
                  value={hours}
                  onChange={(e) => setHours(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Interval (min)
                </label>
                <input
                  type="number"
                  value={intervalMinutes}
                  onChange={(e) => setIntervalMinutes(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  API Function
                </label>
                <input
                  type="text"
                  value={apiFunction}
                  onChange={(e) => setApiFunction(e.target.value)}
                  placeholder="e.g. get_activities"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Python Function
                </label>
                <input
                  type="text"
                  value={pythonFunction}
                  onChange={(e) => setPythonFunction(e.target.value)}
                  placeholder="e.g. process_data"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>

            {/* Task Performance Chart */}
            <TaskPerformanceChart taskId={task.task_id} />

            {/* Recent Execution Logs */}
            <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
              <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
                Recent Execution Logs
              </h4>
              {logs.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-gray-400">No execution logs found.</p>
              ) : (
                <div className="overflow-x-auto max-h-64 overflow-y-auto">
                  <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                    <thead className="bg-gray-50 dark:bg-gray-800">
                      <tr>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                          Timestamp
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                          Event
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                          Status
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                      {logs.slice(0, 10).map((log: any, idx: number) => (
                        <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                          <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                            {log.event_time_utc ? new Date(log.event_time_utc).toLocaleString() : '-'}
                          </td>
                          <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300">
                            {log.event_description || '-'}
                          </td>
                          <td className="px-3 py-2">
                            <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                              log.error_text
                                ? 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
                                : 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300'
                            }`}>
                              {log.error_text ? 'Failed' : 'Success'}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          <div className="flex justify-between gap-3 pt-4 border-t border-gray-200 dark:border-gray-700">
            <div>
              {onDelete && (
                <button
                  onClick={handleDeleteClick}
                  className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700"
                >
                  Delete Configuration
                </button>
              )}
            </div>
            <div className="flex gap-3">
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
              >
                Cancel
              </button>
              <button
                onClick={handleSave}
                disabled={updateConfig.isPending}
                className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                {updateConfig.isPending ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Enhanced Task Management Component
 * Replaces Task Execution History with Task Summary view and adds full CRUD functionality
 */
export default function TaskList() {
  const { data: summaryData, isLoading: summaryLoading, error: summaryError } = useTaskSummaryChart();
  const { data: scheduleData } = useTaskSchedule();
  const { data: taskNamesData } = useTaskNames();
  const executeTask = useExecuteTaskV2(); // Updated to use v2 execution engine
  const deleteTaskConfig = useDeleteTaskConfig();
  const [editingTask, setEditingTask] = useState<any>(null);
  const [editingTaskId, setEditingTaskId] = useState<number | null>(null);
  const [deletingTaskId, setDeletingTaskId] = useState<number | null>(null);
  const [executeStatus, setExecuteStatus] = useState<string | null>(null);
  const [executingTaskName, setExecutingTaskName] = useState<string | null>(null);
  const [currentExecutionId, setCurrentExecutionId] = useState<number | null>(null);
  const [executeProgress, setExecuteProgress] = useState<number>(0);
  const [lastExecutionFailed, setLastExecutionFailed] = useState<boolean>(false);
  const [lastExecutionError, setLastExecutionError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [viewingLogsTaskId, setViewingLogs] = useState<number | null>(null);
  const [showFailureModal, setShowFailureModal] = useState<boolean>(false);
  const createTask = useCreateTask();
  const queryClient = useQueryClient();

  // Poll for task execution status
  const { data: executionStatus } = useTaskExecution(currentExecutionId);

  // Build maps for easier data access
  // NOTE: tasks.task_config view uses task_name as key (no task_id column)
  const scheduleMap = useMemo(() => {
    const map = new Map<string, any>();
    if (scheduleData?.data) {
      scheduleData.data.forEach((s: any) => {
        map.set(s.task_name, s);
      });
    }
    return map;
  }, [scheduleData]);

  const taskNameToIdMap = useMemo(() => {
    const map = new Map<string, number>();
    if (scheduleData?.data) {
      scheduleData.data.forEach((s: any) => {
        map.set(s.task_name, s.task_name);
      });
    }
    return map;
  }, [scheduleData]);

  // Normalize task summary data (from TaskSummary component)
  // Moved to top level to ensure consistent hook ordering
  const summaryRows = useMemo(() => {
    const rows = summaryData?.data ?? [];
    return rows.map((r) => {
      const executionMinutesAgo = r.execution_minutes_ago ?? r.execution_minutes;
      const nextPlannedMinutes = r.next_planned_execution_minutes ?? r.next_minutes;
      const executionCount = r.execution_count ?? r.last_week_count ?? r.week_count ?? r.count;
      const successCount = r.success_count ?? r.successes ?? r.successful_count ?? r.successes_count;
      const successPercentage = r.success_percentage ?? r.success_rate ?? r.success_pct;

      let computedSuccessPercentage: number | null = null;
      if (successPercentage !== null && Number.isFinite(successPercentage)) {
        computedSuccessPercentage = successPercentage;
      } else if (successCount !== null && executionCount !== null && executionCount > 0) {
        computedSuccessPercentage = (successCount / executionCount) * 100;
      }

      const timeAgoExecution = executionMinutesAgo !== null ? -executionMinutesAgo * 60 : null;
      const timeAgoNext = nextPlannedMinutes !== null ? -nextPlannedMinutes * 60 : null;

      const totalDurationMs = [
        r.login_ms, r.extract_ms, r.load_ms, r.flatten_ms, r.parse_ms,
        r.interpolation_ms, r.forecasting_ms, r.python_ms, r.admin_ms
      ].reduce((sum, val) => sum + (val ? Number(val) : 0), 0);

      return {
        task_name: String(r.task_name ?? r.name ?? 'Unknown'),
        task_id: r.task_id,
        task_frequency: r.task_frequency ?? null,
        consecutive_failures: r.consecutive_failures ?? 0,
        is_active_failure: !!r.is_active_failure,
        last_executed_utc: r.last_executed_utc ?? null,
        last_execution_utc: r.last_execution_utc ?? null,
        last_executed: r.last_executed ?? null,
        next_planned_execution_utc: r.next_planned_execution_utc ?? null,
        next_execution_utc: r.next_execution_utc ?? null,
        next_planned: r.next_planned ?? null,
        __timeAgoExecution: timeAgoExecution,
        __timeAgoNext: timeAgoNext,
        __executionCount: executionCount,
        __successPercentage: computedSuccessPercentage,
        __successCount: successCount,
        __totalDurationMs: totalDurationMs,
      };
    });
  }, [summaryData]);

  // Real-time progress bar timer using historical duration as estimate
  const progressIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const progressStartRef = useRef<number>(0);

  // Get estimated duration for the currently executing task
  const estimatedDurationMs = useMemo(() => {
    if (!executingTaskName) return 30000;
    const row = summaryRows.find(r => r.task_name === executingTaskName);
    return row?.__totalDurationMs || 30000;
  }, [summaryRows, executingTaskName]);

  // Timer effect: advances progress from 0% to 90% over the estimated duration
  useEffect(() => {
    if (executeTask.isPending && executingTaskName) {
      progressStartRef.current = Date.now();
      const step = 200; // update every 200ms
      const totalSteps = estimatedDurationMs / step;
      let currentStep = 0;

      progressIntervalRef.current = setInterval(() => {
        currentStep++;
        const elapsed = Date.now() - progressStartRef.current;
        // Use a non-linear curve: fast at start, slows toward 90%
        const progress = Math.min(90, (elapsed / estimatedDurationMs) * 100);
        setExecuteProgress(Math.round(progress));
      }, step);
    } else {
      if (progressIntervalRef.current) {
        clearInterval(progressIntervalRef.current);
        progressIntervalRef.current = null;
      }
    }
    return () => {
      if (progressIntervalRef.current) {
        clearInterval(progressIntervalRef.current);
        progressIntervalRef.current = null;
      }
    };
  }, [executeTask.isPending, executingTaskName, estimatedDurationMs]);

  // Handle task execution
  const handleExecute = (taskName: string, taskId?: number) => {
    setExecutingTaskName(taskName);
    setCurrentExecutionId(null);
    setExecuteProgress(0);
    setShowFailureModal(false);
    setExecuteStatus(`Starting ${taskName}...`);
    executeTask.mutate({ taskName, taskId }, {
      onSuccess: (data) => {
        // data should contain execution_id
        const executionId = (data as any).execution_id;
        if (executionId) {
          setCurrentExecutionId(executionId);
          setExecuteStatus(`Executing ${taskName}...`);
        } else {
          // Fallback for non-async response
          setExecutingTaskName(null);
          setExecuteProgress(100);
          setExecuteStatus(`${taskName}: ${(data as any).message || 'completed'}`);
          setLastExecutionFailed(false);
          setLastExecutionError(null);
          setTimeout(() => setExecuteStatus(null), 3000);
        }
      },
      onError: (err) => {
        setExecutingTaskName(null);
        setExecuteProgress(0);
        const errorMsg = `Error: ${err}`;
        setExecuteStatus(errorMsg);
        setLastExecutionFailed(true);
        setLastExecutionError(errorMsg);
        setShowFailureModal(true);
      },
    });
  };

  // Handle execution status updates from polling
  useEffect(() => {
    if (!executionStatus) return;

    // Handle 404 errors from polling - execution record not found yet
    if ((executionStatus as any).status === 404 || (executionStatus as any).detail?.includes('not found')) {
      // Don't treat as failure - just ignore and keep polling
      return;
    }

    if (executionStatus.status === 'success') {
      setExecutingTaskName(null);
      setCurrentExecutionId(null);
      setExecuteProgress(100);
      setExecuteStatus(`${executionStatus.task_name}: completed successfully`);
      setLastExecutionFailed(false);
      setLastExecutionError(null);
      setTimeout(() => setExecuteStatus(null), 3000);
      
      // Invalidate queries to refresh data
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
      queryClient.invalidateQueries({ queryKey: adminKeys.taskSummaryChart() });
    } else if (executionStatus.status === 'failed') {
      setExecutingTaskName(null);
      setCurrentExecutionId(null);
      setExecuteProgress(0);
      const errorMsg = `Failed: ${executionStatus.error_message || 'Task execution failed'}`;
      setExecuteStatus(errorMsg);
      setLastExecutionFailed(true);
      setLastExecutionError(errorMsg);
      setShowFailureModal(true);
      
      // Invalidate queries to refresh data
      queryClient.invalidateQueries({ queryKey: adminKeys.tasks() });
      queryClient.invalidateQueries({ queryKey: adminKeys.taskSummaryChart() });
    } else if (executionStatus.status === 'running') {
      // Still running, update status with timing estimate if available
      const elapsed = executionStatus.started_at ? 
        Math.floor((Date.now() - new Date(executionStatus.started_at).getTime()) / 1000) : 0;
      setExecuteStatus(`Executing ${executionStatus.task_name}... (${elapsed}s)`);
    }
  }, [executionStatus, queryClient]);

  // Handle opening edit dialog - fetch full config from API
  const handleEdit = (taskId: number) => {
    setEditingTaskId(taskId);
  };

  // Handle task deletion
  const handleDelete = (taskId: number) => {
    setDeletingTaskId(taskId);
  };

  const confirmDelete = () => {
    if (deletingTaskId !== null) {
      deleteTaskConfig.mutate(deletingTaskId, {
        onSuccess: () => {
          // FIRST: Close dialogs to unmount EditingTaskLoader and cancel its subscriptions
          setEditingTaskId(null);
          setEditingTask(null);
          setDeletingTaskId(null);
          // THEN: Refresh task list on next tick (after React unmounts loader)
          setTimeout(() => {
            queryClient.invalidateQueries({ queryKey: adminKeys.taskSummaryChart() });
            queryClient.invalidateQueries({ queryKey: adminKeys.taskSchedule() });
            queryClient.invalidateQueries({ queryKey: adminKeys.taskNames() });
          }, 0);
          // Show success message
          setExecuteStatus(`Task configuration deleted successfully`);
          setTimeout(() => setExecuteStatus(null), 5000);
        },
        onError: (err) => {
          setDeletingTaskId(null);
          setExecuteStatus(`Failed to delete task: ${err}`);
          setTimeout(() => setExecuteStatus(null), 5000);
        },
      });
    }
  };

  // Format functions from TaskSummary component
  function formatRelativeTime(seconds: number | null): string {
    if (seconds === null || seconds === undefined) return '-';
    const absSeconds = Math.abs(seconds);
    const prefix = seconds < 0 ? 'in ' : '';
    const suffix = seconds > 0 ? ' ago' : '';
    if (absSeconds < 60) return `${prefix}just now${suffix}`;
    if (absSeconds < 3600) return `${prefix}${Math.floor(absSeconds / 60)} min${suffix}`;
    if (absSeconds < 86400) return `${prefix}${Math.floor(absSeconds / 3600)} hours${suffix}`;
    return `${prefix}${Math.floor(absSeconds / 86400)} days${suffix}`;
  }

  function formatDuration(totalMs: number | null): string {
    if (!totalMs && totalMs !== 0) return '-';
    if (totalMs < 1000) return `${Math.round(totalMs)}ms`;
    const seconds = totalMs / 1000;
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = seconds / 60;
    return `${minutes.toFixed(1)} min`;
  }

  function getStatusBadge(isFailure: boolean) {
    const label = isFailure ? 'Error' : 'Success';
    const colorClass = isFailure
      ? 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
      : 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
    return (
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colorClass}`}>
        {label}
      </span>
    );
  }

  function getTimeChipColor(seconds: number | null) {
    if (seconds === null || seconds === undefined) return 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';
    if (seconds < 12 * 3600) return 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
    if (seconds > 48 * 3600) return 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300';
    return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300';
  }

  /**
   * Get the next execution chip with conditional logic for inactive/failing tasks
   */
  function getNextExecutionChip(row: any) {
    // Priority 1: Inactive tasks with failures
    if (row.consecutive_failures >= 5 && row.task_frequency === 'Inactive') {
      return {
        colorClass: 'bg-red-900 text-red-100 dark:bg-red-800 dark:text-red-100',
        text: 'Inactive - Failing'
      };
    }

    // Priority 2: Inactive tasks
    if (row.task_frequency === 'Inactive') {
      return {
        colorClass: 'bg-gray-300 text-gray-700 dark:bg-gray-600 dark:text-gray-300',
        text: 'Inactive'
      };
    }

    // Priority 3: Failing tasks (active but failing)
    if (row.consecutive_failures >= 5) {
      return {
        colorClass: 'bg-red-900 text-red-100 dark:bg-red-800 dark:text-red-100',
        text: 'Failing'
      };
    }

    // Priority 4: Normal timing-based logic
    const timeAgoNext = row.__timeAgoNext;
    if (timeAgoNext === null || timeAgoNext === undefined) {
      return {
        colorClass: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
        text: '-'
      };
    }

    return {
      colorClass: getTimeChipColor(timeAgoNext),
      text: formatRelativeTime(timeAgoNext)
    };
  }

  function MiniBar({ value, max, colorClass = 'bg-blue-600 dark:bg-blue-400', showLabel = true, suffix = '' }: { value: number | null; max: number; colorClass?: string; showLabel?: boolean; suffix?: string }) {
    const safeValue = value != null && Number.isFinite(value) ? value : 0;
    const safeMax = Number.isFinite(max) && max > 0 ? max : 1;
    const percent = Math.round((safeValue / safeMax) * 100);
    return (
      <div className="flex items-center gap-2">
        <div className="flex-1 h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div className={`h-2 ${colorClass} rounded-full`} style={{ width: `${percent}%` }} />
        </div>
        {showLabel && (
          <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
            {suffix === '%' ? `${percent}%` : `${Math.round(safeValue)}${suffix}`}
          </span>
        )}
      </div>
    );
  }

  // Loading and error states
  if (summaryLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (summaryError) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300">Failed to load task summary: {String(summaryError)}</p>
      </div>
    );
  }

  const maxExecutionCount = Math.max(0, ...summaryRows.map((r) => r.__executionCount ?? 0));
  const maxDuration = Math.max(0, ...summaryRows.map((r) => r.__totalDurationMs ?? 0));
  const taskNames = taskNamesData?.data || [];

  return (
    <div className="space-y-6">
      {/* Header with title and actions */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Task Management</h2>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Monitor, configure, and execute background tasks
          </p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          {executeStatus && (
            <span className={`text-sm ${executeStatus.includes('Error') ? 'text-red-600' : 'text-green-600'} animate-pulse`}>
              {executeStatus}
            </span>
          )}
          <button
            onClick={() => setShowCreateForm(true)}
            className="px-4 py-2 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700"
          >
            Add New Task
          </button>
        </div>
      </div>

      {/* Execution Progress Overlay (Popup Modal) */}
      {(executeTask.isPending && executingTaskName) && !showFailureModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
            <div className="flex items-center gap-3 mb-4">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
              <div>
                <h3 className="text-sm font-semibold text-gray-900 dark:text-white">Executing Task</h3>
                <p className="text-xs text-gray-500 dark:text-gray-400">{executingTaskName}</p>
              </div>
            </div>
            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3 overflow-hidden mb-2">
              <div
                className="h-3 bg-blue-600 rounded-full transition-all duration-200 ease-out"
                style={{ width: `${executeProgress}%` }}
              />
            </div>
            <div className="flex justify-between items-center">
              <span className="text-xs text-gray-500 dark:text-gray-400">In progress...</span>
              <span className="text-xs font-medium text-blue-600 dark:text-blue-400">{executeProgress}%</span>
            </div>
          </div>
        </div>
      )}

      {/* Execution Failure Modal */}
      {showFailureModal && lastExecutionFailed && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-lg mx-4">
            <div className="flex items-center gap-3 mb-4">
              <div className="rounded-full h-6 w-6 bg-red-600 flex items-center justify-center">
                <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </div>
              <div>
                <h3 className="text-sm font-semibold text-red-600 dark:text-red-400">Task Execution Failed</h3>
                <p className="text-xs text-gray-500 dark:text-gray-400">{executingTaskName || 'Unknown Task'}</p>
              </div>
            </div>
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-3 mb-4">
              <p className="text-sm text-red-700 dark:text-red-300 whitespace-pre-wrap">{lastExecutionError}</p>
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-4">
              The task status below will update shortly to reflect the failure.
            </div>
            <div className="flex justify-end">
              <button
                onClick={() => setShowFailureModal(false)}
                className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
              >
                Acknowledge
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Task Summary Table (replaces old Task Execution History) */}
      {summaryRows.length === 0 ? (
        <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
          <p className="text-gray-500 dark:text-gray-400">No task summary data available.</p>
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Task Name</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Execution</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Next Scheduled</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Week Count</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Success %</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Duration</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
              {summaryRows.map((row) => {
                const duration = row.__totalDurationMs ?? 0;
                const lastExecutedUtc = row.last_executed_utc ?? row.last_execution_utc ?? row.last_executed ?? null;
                const nextPlannedExecutionUtc = row.next_planned_execution_utc ?? row.next_execution_utc ?? row.next_planned ?? null;

                const timeAgoExecution = row.__timeAgoExecution;
                const timeAgoNext = row.__timeAgoNext;
                const lastExecutionChipClass = getTimeChipColor(timeAgoExecution);
                const nextExecutionChipClass = getTimeChipColor(timeAgoNext);

                const schedule = scheduleMap.get(row.task_name);

                return (
                  <tr key={row.task_name} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-white whitespace-nowrap">
                      {schedule?.display_icon ? <span className="mr-1.5">{schedule.display_icon}</span> : ''}
                      {row.task_name}
                    </td>
                    <td className="px-4 py-3">{getStatusBadge(!!row.is_active_failure)}</td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                      <span
                        title={lastExecutedUtc ? new Date(lastExecutedUtc).toLocaleString('en-US', { timeZone: 'America/Los_Angeles', dateStyle: 'medium', timeStyle: 'short' }) : '-'}
                        className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${lastExecutionChipClass}`}
                      >
                        {formatRelativeTime(timeAgoExecution)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                      <span
                        title={nextPlannedExecutionUtc ? new Date(nextPlannedExecutionUtc).toLocaleString('en-US', { timeZone: 'America/Los_Angeles', dateStyle: 'medium', timeStyle: 'short' }) : '-'}
                        className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getNextExecutionChip(row).colorClass}`}
                      >
                        {getNextExecutionChip(row).text}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 min-w-[160px]">
                      <MiniBar value={row.__executionCount} max={maxExecutionCount} colorClass="bg-gray-600 dark:bg-gray-400" suffix="" />
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 min-w-[160px]">
                      <MiniBar value={row.__successPercentage ?? 0} max={100} colorClass="bg-gray-600 dark:bg-gray-400" suffix="%" />
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 min-w-[180px]">
                      <MiniBar value={duration} max={maxDuration} colorClass="bg-gray-600 dark:bg-gray-400" suffix="" showLabel={false} />
                      <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">{formatDuration(duration)}</span>
                    </td>
                    <td className="px-4 py-3 text-right text-sm">
                      <div className="flex flex-col sm:flex-row gap-2 justify-end">
                        <button
                          onClick={() => handleExecute(row.task_name, row.task_id)}
                          className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 dark:focus:ring-offset-gray-900 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          Execute Now
                        </button>
                        <button
                          onClick={() => handleEdit(row.task_id)}
                          className="inline-flex items-center px-3 py-1.5 border border-gray-300 dark:border-gray-600 text-xs font-medium rounded-md text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700"
                        >
                          Edit
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Edit Dialog - fetches full config from API */}
      {editingTaskId !== null && deletingTaskId === null && (
        <EditingTaskLoader 
          taskId={editingTaskId} 
          onClose={() => { setEditingTaskId(null); setEditingTask(null); }}
          onDelete={() => handleDelete(editingTaskId)}
        />
      )}

      {/* Legacy edit dialog (kept for backward compat) */}
      {editingTask && !editingTaskId && (
        <EditTaskDialog task={editingTask} onClose={() => setEditingTask(null)} />
      )}

      {/* Delete Confirmation Dialog */}
      {deletingTaskId !== null && (
        <DeleteConfirmDialog
          taskId={deletingTaskId}
          onConfirm={confirmDelete}
          onCancel={() => setDeletingTaskId(null)}
        />
      )}

      {/* Task Log View */}
      {viewingLogsTaskId !== null && (
        <TaskLogView
          taskId={viewingLogsTaskId}
          onClose={() => setViewingLogs(null)}
        />
      )}

      {/* Create Task Form */}
      {showCreateForm && (
        <CreateTaskForm
          existingTaskNames={taskNames}
          onClose={() => setShowCreateForm(false)}
          createTask={createTask}
        />
      )}
    </div>
  );
}

/**
 * EditingTaskLoader - Fetches full task config from API and renders EditTaskDialog
 * This ensures ALL fields are pre-populated when editing.
 */
function EditingTaskLoader({
  taskId,
  onClose,
  onDelete,
}: {
  taskId: number;
  onClose: () => void;
  onDelete?: () => void;
}) {
  const { data: configData, isLoading, error } = useTaskConfig(taskId);

  if (isLoading) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto" />
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-2 text-center">Loading task configuration...</p>
        </div>
      </div>
    );
  }

  if (error || !configData?.data) {
    // Check if this is a 404 Not Found error (task was deleted)
    const errorMessage = error instanceof Error ? error.message : String(error);
    const isNotFound = errorMessage.includes('"status":404') || errorMessage.includes('"status": 404');
    
    if (isNotFound) {
      // Task was deleted - silently close by signaling parent
      return null;
    }
    
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 max-w-md mx-4">
          <h3 className="text-lg font-semibold text-red-600 dark:text-red-400 mb-2">Failed to Load Task</h3>
          <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
            {error ? String(error) : 'Task configuration not found'}
          </p>
          <div className="flex justify-end">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    );
  }

  return <EditTaskDialog task={configData.data} onClose={onClose} onDelete={onDelete} />;
}

/**
 * Delete Confirmation Dialog
 */
function DeleteConfirmDialog({
  taskId,
  onConfirm,
  onCancel,
}: {
  taskId: number;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
          Delete Task Configuration?
        </h3>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
          This will remove the task configuration but preserve execution history.
          The task can be recreated later if needed.
        </p>
        <div className="flex justify-end gap-3">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700"
          >
            Delete Configuration
          </button>
        </div>
      </div>
    </div>
  );
}

/**
 * Create Task Form (placeholder - disabled until backend endpoint available)
 */
function CreateTaskForm({
  existingTaskNames,
  onClose,
  createTask,
}: {
  existingTaskNames: string[];
  onClose: () => void;
  createTask: ReturnType<typeof useCreateTask>;
}) {
  const [taskName, setTaskName] = useState('');
  const [description, setDescription] = useState('');
  const [frequency, setFrequency] = useState('Inactive');
  const [displayIcon, setDisplayIcon] = useState('⚙️');
  const [priority, setPriority] = useState(0);
  const [hours, setHours] = useState(0);
  const [intervalMinutes, setIntervalMinutes] = useState(0);
  const [apiFunction, setApiFunction] = useState('');
  const [pythonFunction, setPythonFunction] = useState('');
  const createTaskMutation = createTask;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!taskName.trim()) return;

    createTaskMutation.mutate(
      {
        task_name: taskName,
        description: description || undefined,
        task_frequency: frequency,
        display_icon: displayIcon,
        priority: priority,
        hours: hours,
        interval_minutes: intervalMinutes,
        api_function: apiFunction || undefined,
        python_function: pythonFunction || undefined,
      },
      {
        onSuccess: () => {
          setTaskName('');
          setDescription('');
          setFrequency('daily');
          setDisplayIcon('⚙️');
          setPriority(0);
          setHours(0);
          setIntervalMinutes(0);
          setApiFunction('');
          setPythonFunction('');
          onClose();
        },
      }
    );
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-2xl mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Create New Task
        </h3>

        <form onSubmit={handleSubmit}>
          <div className="space-y-4 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Task Name *
              </label>
              <input
                type="text"
                value={taskName}
                onChange={(e) => setTaskName(e.target.value)}
                placeholder="e.g. process_garmin_data, generate_reports"
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Description
              </label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Describe what this task does"
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm min-h-[80px]"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Frequency
              </label>
              <div className="flex flex-wrap gap-2">
                {['Hourly', 'Daily', 'Weekly', 'Monthly', 'Inactive'].map((freq) => (
                  <button
                    key={freq.toLowerCase()}
                    type="button"
                    onClick={() => setFrequency(freq.toLowerCase())}
                    className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
                      frequency === freq.toLowerCase()
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                    }`}
                  >
                    {freq}
                  </button>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Display Icon
                </label>
                <input
                  type="text"
                  value={displayIcon}
                  onChange={(e) => setDisplayIcon(e.target.value)}
                  placeholder="⚙️"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Priority
                </label>
                <input
                  type="number"
                  value={priority}
                  onChange={(e) => setPriority(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Priority
                </label>
                <input
                  type="number"
                  value={priority}
                  onChange={(e) => setPriority(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Hours
                </label>
                <input
                  type="number"
                  value={hours}
                  onChange={(e) => setHours(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Interval (min)
                </label>
                <input
                  type="number"
                  value={intervalMinutes}
                  onChange={(e) => setIntervalMinutes(parseInt(e.target.value) || 0)}
                  min="0"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  API Function
                </label>
                <input
                  type="text"
                  value={apiFunction}
                  onChange={(e) => setApiFunction(e.target.value)}
                  placeholder="e.g. get_activities"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Python Function
                </label>
                <input
                  type="text"
                  value={pythonFunction}
                  onChange={(e) => setPythonFunction(e.target.value)}
                  placeholder="e.g. process_data"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>
            </div>
          </div>

          {createTaskMutation.isError && (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-3 mb-4">
              <p className="text-sm text-red-700 dark:text-red-300">
                Failed to create task: {String(createTaskMutation.error)}
              </p>
            </div>
          )}

          <div className="flex justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              disabled={createTaskMutation.isPending}
              className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={createTaskMutation.isPending || !taskName.trim()}
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {createTaskMutation.isPending ? 'Creating...' : 'Create Task'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
