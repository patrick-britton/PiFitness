/**
 * Task Summary Component
 * Displays task execution summary with status, timing, and execute capability.
 * Uses data from tasks.vw_task_summary_chart.
 *
 * Defensive mapping handles schema variations in the underlying view.
 */

'use client';

import { useMemo } from 'react';
import { useTaskSummaryChart, useExecuteTask } from '@/hooks/useAdmin';

type TaskSummaryRow = Record<string, any>;

interface AugmentedRow {
  task_name: string;
  is_active_failure: boolean;
  last_executed_utc?: string | null;
  last_execution_utc?: string | null;
  last_executed?: string | null;
  next_planned_execution_utc?: string | null;
  next_execution_utc?: string | null;
  next_planned?: string | null;
  __timeAgoExecution: number | null;
  __timeAgoNext: number | null;
  __executionCount: number | null;
  __successPercentage: number | null;
  __successCount: number | null;
  __totalDurationMs: number | null;
}

function toNumber(value: any): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === 'number' ? value : Number.parseFloat(value);
  return Number.isFinite(n) ? n : null;
}

function formatRelativeTime(seconds: number | null): string {
  if (seconds === null || seconds === undefined) return '-';
  if (seconds < 0) return 'overdue';
  if (seconds < 60) return 'just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} hours ago`;
  return `${Math.floor(seconds / 86400)} days ago`;
}

function formatExactTime(iso: string | null): string {
  if (!iso) return '-';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return String(iso);
  }
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

export default function TaskSummary() {
  const { data, isLoading, error, refetch } = useTaskSummaryChart();
  const executeTask = useExecuteTask();

  const rows: TaskSummaryRow[] = useMemo(() => data?.data ?? [], [data]);

  const normalized: AugmentedRow[] = useMemo(() => {
    return rows.map((r) => {
      const executionMinutesAgo = toNumber(r.execution_minutes_ago ?? r.execution_minutes);
      const nextPlannedMinutes = toNumber(r.next_planned_execution_minutes ?? r.next_minutes);
      const executionCount = toNumber(r.execution_count ?? r.last_week_count ?? r.week_count ?? r.count);
      const successCount = toNumber(r.success_count ?? r.successes ?? r.successful_count ?? r.successes_count);
      const successPercentage = toNumber(r.success_percentage ?? r.success_rate ?? r.success_pct);

      let computedSuccessPercentage: number | null = null;
      if (successPercentage !== null && Number.isFinite(successPercentage)) {
        computedSuccessPercentage = successPercentage;
      } else if (successCount !== null && executionCount !== null && executionCount > 0) {
        computedSuccessPercentage = (successCount / executionCount) * 100;
      }

      const timeAgoExecution = executionMinutesAgo !== null ? Math.abs(executionMinutesAgo) * 60 : null;
      const timeAgoNext = nextPlannedMinutes !== null ? Math.abs(nextPlannedMinutes) * 60 : null;

      const totalDurationMs = [
        r.login_ms, r.extract_ms, r.load_ms, r.flatten_ms, r.parse_ms,
        r.interpolation_ms, r.forecasting_ms, r.python_ms, r.admin_ms
      ].reduce((sum, val) => sum + (toNumber(val) ?? 0), 0);

      return {
        task_name: String(r.task_name ?? r.name ?? 'Unknown'),
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
  }, [rows]);

  const maxExecutionCount = Math.max(0, ...normalized.map((r) => r.__executionCount ?? 0));
  const maxDuration = Math.max(0, ...normalized.map((r) => r.__totalDurationMs ?? 0));

  const handleExecute = async (taskName: string) => {
    try {
      await executeTask.mutateAsync(taskName);
      await refetch();
    } catch (err) {
      console.error('Failed to execute task', taskName, err);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300">Failed to load task summary: {String(error)}</p>
      </div>
    );
  }

  if (!normalized.length) {
    return (
      <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
        <p className="text-gray-500 dark:text-gray-400">No task summary data available.</p>
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Task Summary</h2>
        <span className="text-sm text-gray-500 dark:text-gray-400">{normalized.length} task{normalized.length !== 1 ? 's' : ''}</span>
      </div>

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
            {normalized.map((row) => {
              const duration = row.__totalDurationMs ?? 0;
              const lastExecutedUtc = row.last_executed_utc ?? row.last_execution_utc ?? row.last_executed ?? null;
              const nextPlannedExecutionUtc = row.next_planned_execution_utc ?? row.next_execution_utc ?? row.next_planned ?? null;

              const timeAgoExecution = row.__timeAgoExecution;
              const timeAgoNext = row.__timeAgoNext;
              const lastExecutionChipClass = getTimeChipColor(timeAgoExecution);
              const nextExecutionChipClass = getTimeChipColor(timeAgoNext);

              return (
                <tr key={row.task_name} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                  <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-white whitespace-nowrap">{row.task_name}</td>
                  <td className="px-4 py-3">{getStatusBadge(!!row.is_active_failure)}</td>
                  <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                    <span
                      title={formatExactTime(lastExecutedUtc)}
                      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${lastExecutionChipClass}`}
                    >
                      {formatRelativeTime(timeAgoExecution)}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                    <span
                      title={formatExactTime(nextPlannedExecutionUtc)}
                      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${nextExecutionChipClass}`}
                    >
                      {formatRelativeTime(timeAgoNext)}
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
                    <button
                      onClick={() => handleExecute(row.task_name)}
                      disabled={executeTask.isPending}
                      className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 dark:focus:ring-offset-gray-900 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Execute Now
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
