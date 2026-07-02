/**
 * Active Queries View Component
 * Displays active PostgreSQL database connections with session info and kill-switch.
 * Auto-refreshes every 5 seconds via React Query.
 */

'use client';

import { useState } from 'react';
import { useDBSessions, useKillDBSession } from '@/hooks/useAdmin';

/**
 * Format run length (PostgreSQL interval) to human-readable string
 */
function formatRunLength(runLength: any): string {
  if (!runLength) return '-';
  // Handle both seconds (number) and PostgreSQL interval strings
  if (typeof runLength === 'number') {
    return `${runLength.toFixed(1)}s`;
  }
  if (typeof runLength === 'string') {
    // Parse PostgreSQL interval like "00:03:45.123456"
    const match = runLength.match(/(?:(\d+)\s+days?\s+)?(\d+):(\d+):([\d.]+)/);
    if (match) {
      const [, days, hours, minutes, secs] = match;
      const parts: string[] = [];
      if (days) parts.push(`${days}d`);
      if (hours && hours !== '00') parts.push(`${hours}h`);
      if (minutes && minutes !== '00') parts.push(`${minutes}m`);
      parts.push(`${parseFloat(secs).toFixed(0)}s`);
      return parts.join(' ');
    }
    return runLength;
  }
  return String(runLength);
}

/**
 * Truncate a query string to a reasonable length for display
 */
function truncateQuery(query: string, maxLen = 120): string {
  if (!query) return '-';
  return query.length > maxLen ? query.substring(0, maxLen) + '...' : query;
}

/**
 * Color-coded row based on run length and state
 */
function getRowStyle(state: string, runLength: any): string {
  const base = 'hover:bg-gray-50 dark:hover:bg-gray-800/50';

  if (state === 'active') {
    const seconds = typeof runLength === 'number' ? runLength : 0;
    if (seconds > 60) return `${base} bg-red-50 dark:bg-red-900/10`; // > 1 min
    if (seconds > 30) return `${base} bg-yellow-50 dark:bg-yellow-900/10`; // > 30s
    return `${base} bg-blue-50/30 dark:bg-blue-900/5`; // active but short
  }

  if (state === 'idle in transaction') {
    return `${base} bg-yellow-50/50 dark:bg-yellow-900/10`;
  }

  return base;
}

/**
 * Kill Confirmation Dialog
 */
function KillConfirmDialog({
  pid,
  query,
  onConfirm,
  onCancel,
}: {
  pid: number;
  query: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
          Kill Database Session?
        </h3>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
          This will terminate PID <strong>{pid}</strong> running:
        </p>
        <pre className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-xs text-gray-800 dark:text-gray-200 max-h-24 overflow-auto mb-6">
          {truncateQuery(query, 400)}
        </pre>
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
            Kill Session
          </button>
        </div>
      </div>
    </div>
  );
}

/**
 * Active Queries View Component
 */
export default function ActiveQueriesView() {
  const { data, isLoading, error } = useDBSessions();
  const killSession = useKillDBSession();
  const [confirmPid, setConfirmPid] = useState<number | null>(null);
  const [killMessage, setKillMessage] = useState<string | null>(null);

  const handleKill = (pid: number) => {
    killSession.mutate(pid, {
      onSuccess: (result) => {
        setKillMessage(`Session ${pid} terminated successfully`);
        setConfirmPid(null);
        setTimeout(() => setKillMessage(null), 3000);
      },
      onError: (err) => {
        setKillMessage(`Failed to kill ${pid}: ${err}`);
        setConfirmPid(null);
        setTimeout(() => setKillMessage(null), 5000);
      },
    });
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
        <p className="text-red-700 dark:text-red-300">Failed to load active queries: {String(error)}</p>
      </div>
    );
  }

  const sessions = data?.data || [];

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
          Active Database Queries
        </h2>
        <div className="flex items-center gap-3">
          {killMessage && (
            <span className={`text-sm ${killMessage.includes('Failed') ? 'text-red-600' : 'text-green-600'} animate-pulse`}>
              {killMessage}
            </span>
          )}
          <span className="text-xs text-gray-500 dark:text-gray-400">
            {sessions.length} session{sessions.length !== 1 ? 's' : ''} | auto-refresh 5s
          </span>
        </div>
      </div>

      {sessions.length === 0 ? (
        <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-6 text-center">
          <p className="text-green-700 dark:text-green-300 font-medium">No active queries</p>
          <p className="text-sm text-green-600 dark:text-green-400 mt-1">All sessions appear idle or complete.</p>
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">PID</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">State</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Query</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Run Length</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
              {sessions.map((session: any) => {
                const confirmPidForThis = confirmPid === session.pid;
                return (
                  <tr
                    key={session.pid}
                    className={getRowStyle(session.state, session.run_length)}
                  >
                    <td className="px-4 py-3 text-sm font-mono font-medium text-gray-900 dark:text-white">
                      {session.pid}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        session.state === 'active'
                          ? 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300'
                          : session.state === 'idle in transaction'
                          ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300'
                          : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'
                      }`}>
                        {session.state || 'unknown'}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 max-w-md">
                      <code className="text-xs">{truncateQuery(session.query)}</code>
                      {session.query && session.query.length > 120 && (
                        <span className="text-xs text-gray-400 ml-1" title={session.query}>
                          [...]
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 font-mono">
                      {formatRunLength(session.run_length)}
                    </td>
                    <td className="px-4 py-3">
                      <button
                        onClick={() => setConfirmPid(session.pid)}
                        disabled={killSession.isPending && confirmPidForThis}
                        className="px-3 py-1 text-xs font-medium text-white bg-red-600 rounded hover:bg-red-700 disabled:opacity-50"
                      >
                        Kill
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Kill Confirmation Dialog */}
      {confirmPid !== null && (() => {
        const session = sessions.find((s: any) => s.pid === confirmPid);
        return (
          <KillConfirmDialog
            pid={confirmPid}
            query={session?.query || ''}
            onConfirm={() => handleKill(confirmPid)}
            onCancel={() => setConfirmPid(null)}
          />
        );
      })()}
    </div>
  );
}
