/**
 * Task Log View Component
 * Displays execution logs for a specific task in a simplified table format.
 * Based on EventHistory but filtered to a single task with no additional filters.
 */

'use client';

import { useTaskLogs } from '@/hooks/useAdmin';

interface TaskLogViewProps {
  taskId: number;
  onClose: () => void;
}

export default function TaskLogView({ taskId, onClose }: TaskLogViewProps) {
  const { data: logsData, isLoading, error } = useTaskLogs(taskId);
  const logs = logsData?.data || [];

  // Map DB column names from logging.task_executions to display fields
  // DB columns: extract_time_ms, load_time_ms, transform_time_ms,
  //             interpolation_time_ms, forecast_time_ms, error_text, failure_type, event_time_utc
  const mapLogRow = (log: any) => {
    const isError = !!log.failure_type || !!log.error_text;
    const timestamp = log.event_time_utc || log.timestamp || log.start_time || '-';
    const durationMs = [
      log.extract_time_ms, log.load_time_ms, log.transform_time_ms,
      log.interpolation_time_ms, log.forecast_time_ms
    ].reduce((sum: number, val: any) => sum + (val ? Number(val) : 0), 0);
    return { ...log, _isError: isError, _timestamp: timestamp, _durationMs: durationMs };
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center bg-black/40 pt-12 px-4">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-4xl mx-4 max-h-[80vh] flex flex-col">
        <div className="flex justify-between items-start mb-4">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Task Execution Logs
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Showing execution history for task ID: {taskId}
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="flex-1 overflow-auto">
          {isLoading && (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
            </div>
          )}

          {error && (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
              <p className="text-red-700 dark:text-red-300">Failed to load logs: {String(error)}</p>
            </div>
          )}

          {!isLoading && !error && logs.length === 0 && (
            <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
              <p className="text-gray-500 dark:text-gray-400">No execution logs found for this task.</p>
            </div>
          )}

          {!isLoading && !error && logs.length > 0 && (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Timestamp
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Task Name
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Duration
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Error/Notes
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                  {logs.map((log: any, idx: number) => {
                    const mapped = mapLogRow(log);
                    return (
                      <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                          {mapped._timestamp ? new Date(mapped._timestamp).toLocaleString() : '-'}
                        </td>
                        <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-white">
                          {log.task_name || '-'}
                        </td>
                        <td className="px-4 py-3">
                          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                            mapped._isError
                              ? 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
                              : 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300'
                          }`}>
                            {mapped._isError ? 'Failed' : 'Success'}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {mapped._durationMs > 0 ? `${(mapped._durationMs / 1000).toFixed(1)}s` : '-'}
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {log.error_text || log.failure_type || '-'}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="flex justify-end gap-3 mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
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