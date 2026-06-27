/**
 * TaskList Component
 * Displays task execution history with status badges, execute button, and edit dialog.
 * Auto-refreshes every 10 seconds via React Query.
 */

'use client';

import { useState } from 'react';
import { useTasks, useTaskSchedule, useExecuteTask, useUpdateTaskConfig } from '@/hooks/useAdmin';

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
 * Edit Task Config Dialog
 */
function EditTaskDialog({
  task,
  onClose,
}: {
  task: { task_id: number; task_name: string; is_active: boolean; task_frequency: string };
  onClose: () => void;
}) {
  const [isActive, setIsActive] = useState(task.is_active);
  const [frequency, setFrequency] = useState(task.task_frequency || '');
  const updateConfig = useUpdateTaskConfig();

  const handleSave = () => {
    updateConfig.mutate(
      { taskId: task.task_id, config: { is_active: isActive, task_frequency: frequency } },
      { onSuccess: () => onClose() }
    );
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Edit Task: {task.task_name}
        </h3>

        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Active
            </label>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={isActive}
                onChange={(e) => setIsActive(e.target.checked)}
                className="sr-only peer"
              />
              <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600" />
            </label>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Frequency
            </label>
            <input
              type="text"
              value={frequency}
              onChange={(e) => setFrequency(e.target.value)}
              placeholder="e.g. hourly, daily, cron expression"
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
            />
          </div>
        </div>

        <div className="flex justify-end gap-3 mt-6">
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
  );
}

/**
 * TaskList Component
 */
export default function TaskList() {
  const { data: tasksData, isLoading: tasksLoading, error: tasksError } = useTasks();
  const { data: scheduleData } = useTaskSchedule();
  const executeTask = useExecuteTask();
  const [editingTask, setEditingTask] = useState<any>(null);
  const [executeStatus, setExecuteStatus] = useState<string | null>(null);

  // Build a map of task_id -> schedule config
  const scheduleMap = new Map<number, any>();
  if (scheduleData?.data) {
    scheduleData.data.forEach((s: any) => {
      scheduleMap.set(s.task_id, s);
    });
  }

  const handleExecute = (taskName: string) => {
    setExecuteStatus(`Triggering ${taskName}...`);
    executeTask.mutate(taskName, {
      onSuccess: (data) => {
        setExecuteStatus(`${taskName}: ${data.message || 'executed'}`);
        setTimeout(() => setExecuteStatus(null), 3000);
      },
      onError: (err) => {
        setExecuteStatus(`Error: ${err}`);
        setTimeout(() => setExecuteStatus(null), 5000);
      },
    });
  };

  if (tasksLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (tasksError) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300">Failed to load tasks: {String(tasksError)}</p>
      </div>
    );
  }

  const tasks = tasksData?.data || [];

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Task Execution History</h2>
        {executeStatus && (
          <span className="text-sm text-blue-600 dark:text-blue-400 animate-pulse">{executeStatus}</span>
        )}
      </div>

      {tasks.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">No task execution records found.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Task</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Time</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Duration</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
              {tasks.map((task: any, idx: number) => {
                const schedule = scheduleMap.get(task.task_id);
                const totalMs =
                  (task.extract_time_ms || 0) +
                  (task.transform_time_ms || 0) +
                  (task.load_time_ms || 0) +
                  (task.forecast_time_ms || 0) +
                  (task.interpolation_time_ms || 0);

                return (
                  <tr key={task.event_time_utc ? `${task.event_time_utc}-${idx}` : idx} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-white">
                      {task.task_name}
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={task.error_text ? 'error' : 'success'} />
                      {task.error_text && (
                        <p className="text-xs text-red-500 mt-1 truncate max-w-[200px]" title={task.error_text}>
                          {task.error_text}
                        </p>
                      )}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400">
                      {task.event_time_utc ? new Date(task.event_time_utc).toLocaleString() : '-'}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400">
                      {totalMs > 0 ? `${(totalMs / 1000).toFixed(1)}s` : '-'}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-2">
                        <button
                          onClick={() => handleExecute(task.task_name)}
                          disabled={executeTask.isPending}
                          className="px-3 py-1 text-xs font-medium text-white bg-blue-600 rounded hover:bg-blue-700 disabled:opacity-50"
                        >
                          Execute
                        </button>
                        {schedule && (
                          <button
                            onClick={() => setEditingTask({
                              task_id: task.task_id,
                              task_name: task.task_name,
                              is_active: schedule.is_active ?? true,
                              task_frequency: schedule.task_frequency || '',
                            })}
                            className="px-3 py-1 text-xs font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                          >
                            Edit
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Edit Dialog */}
      {editingTask && (
        <EditTaskDialog task={editingTask} onClose={() => setEditingTask(null)} />
      )}
    </div>
  );
}