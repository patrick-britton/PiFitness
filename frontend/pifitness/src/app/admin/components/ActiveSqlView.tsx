/**
 * Active SQL View Component
 * Displays active queries from pg_stat_activity with kill session capability.
 * To be fully implemented in Phase 6.
 */

'use client';

export default function ActiveSqlView() {
  return (
    <div className="p-4 bg-white dark:bg-gray-800 rounded-lg shadow-sm">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">Active SQL</h3>
      <p className="text-sm text-gray-500 dark:text-gray-400">
        Active database queries and session management — coming soon.
      </p>
    </div>
  );
}