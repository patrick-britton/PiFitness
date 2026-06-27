/**
 * EventHistory Component
 * Searchable and filterable event log viewer using vw_all_event_history.
 * Supports free text search, errors-only toggle, skip-row filter, and event type dropdown.
 */

'use client';

import { useState, useCallback } from 'react';
import { useEvents } from '@/hooks/useAdmin';

/**
 * Format a timestamp to a readable date/time string
 */
function formatTimestamp(ts: any): string {
  if (!ts) return '-';
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return String(ts);
  }
}

/**
 * Event type color coding
 */
function getEventTypeBadge(eventType: string) {
  const lower = (eventType || '').toLowerCase();
  let colorClass = 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';

  if (lower.includes('error') || lower.includes('fail')) {
    colorClass = 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300';
  } else if (lower.includes('success') || lower.includes('complete')) {
    colorClass = 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
  } else if (lower.includes('start') || lower.includes('begin')) {
    colorClass = 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300';
  } else if (lower.includes('warning')) {
    colorClass = 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300';
  } else if (lower.includes('info')) {
    colorClass = 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900 dark:text-indigo-300';
  }

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colorClass}`}>
      {eventType || 'unknown'}
    </span>
  );
}

/**
 * EventHistory Component
 */
export default function EventHistory() {
  const [search, setSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [errorsOnly, setErrorsOnly] = useState(false);
  const [ignoreSkips, setIgnoreSkips] = useState(false);
  const [eventType, setEventType] = useState('');
  const [limit, setLimit] = useState(250);

  // Debounce search input (300ms)
  const handleSearchChange = useCallback((value: string) => {
    setSearch(value);
    const timer = setTimeout(() => {
      setDebouncedSearch(value);
    }, 300);
    return () => clearTimeout(timer);
  }, []);

  const { data, isLoading, error } = useEvents({
    search: debouncedSearch || undefined,
    errors_only: errorsOnly || undefined,
    ignore_skips: ignoreSkips || undefined,
    event_type: eventType || undefined,
    limit,
  });

  // Extract unique event types from the data
  const eventTypes = new Set<string>();
  if (data?.data) {
    data.data.forEach((e: any) => {
      if (e.event_type) eventTypes.add(e.event_type);
    });
  }

  return (
    <div>
      <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Event History Search
      </h2>

      {/* Filters */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 mb-4">
        {/* Search */}
        <div className="sm:col-span-2">
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Search
          </label>
          <input
            type="text"
            value={search}
            onChange={(e) => handleSearchChange(e.target.value)}
            placeholder="Search event type, description, error text..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
          />
        </div>

        {/* Event Type */}
        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Event Type
          </label>
          <select
            value={eventType}
            onChange={(e) => setEventType(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
          >
            <option value="">All Types</option>
            {Array.from(eventTypes).sort().map((et) => (
              <option key={et} value={et}>{et}</option>
            ))}
          </select>
        </div>

        {/* Errors Only */}
        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Errors Only
          </label>
          <label className="relative inline-flex items-center cursor-pointer mt-1.5">
            <input
              type="checkbox"
              checked={errorsOnly}
              onChange={(e) => setErrorsOnly(e.target.checked)}
              className="sr-only peer"
            />
            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-red-600" />
          </label>
        </div>

        {/* Ignore Skips */}
        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Ignore Skip Rows
          </label>
          <label className="relative inline-flex items-center cursor-pointer mt-1.5">
            <input
              type="checkbox"
              checked={ignoreSkips}
              onChange={(e) => setIgnoreSkips(e.target.checked)}
              className="sr-only peer"
            />
            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600" />
          </label>
        </div>
      </div>

      {/* Results */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
        </div>
      ) : error ? (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-red-700 dark:text-red-300">Failed to load events: {String(error)}</p>
        </div>
      ) : (
        <div>
          <p className="text-sm text-gray-500 dark:text-gray-400 mb-2">
            {data?.count || 0} event{data?.count !== 1 ? 's' : ''} found
          </p>

          {(!data?.data || data.data.length === 0) ? (
            <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
              <p className="text-gray-500 dark:text-gray-400">No events match the current filters.</p>
            </div>
          ) : (
            <div className="overflow-x-auto max-h-[60vh] overflow-y-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0 z-10">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Time</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Type</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Description</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Details</th>
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                  {data.data.map((event: any, idx: number) => (
                    <tr key={event.event_time_utc || idx} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
                        {formatTimestamp(event.event_time_utc)}
                      </td>
                      <td className="px-4 py-2">
                        {getEventTypeBadge(event.event_type)}
                      </td>
                      <td className="px-4 py-2 text-sm text-gray-700 dark:text-gray-300 max-w-sm">
                        <p className="line-clamp-2" title={event.description}>
                          {event.description || '-'}
                        </p>
                      </td>
                      <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-400 max-w-xs">
                        {event.error_text && (
                          <p className="text-red-500 line-clamp-2" title={event.error_text}>
                            Error: {event.error_text}
                          </p>
                        )}
                        {event.details && !event.error_text && (
                          <p className="line-clamp-2" title={event.details}>
                            {event.details}
                          </p>
                        )}
                        {!event.error_text && !event.details && (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}