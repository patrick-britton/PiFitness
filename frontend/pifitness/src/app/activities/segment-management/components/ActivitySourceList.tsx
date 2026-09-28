'use client';

import { useCallback, useEffect, useState } from 'react';
import { API } from '@/lib/api-client';
import { ActivitySummary } from '@/lib/types/segment-management';
import { formatIsoDate, formatMiles } from '@/lib/date-format';
import ActivityTable from './ActivityTable';

/**
 * Source-activity selection screen for Create Segment mode (009-003 T08, FR-1).
 *
 * Lists activities from `GET /api/segments/activities` (one row per activity,
 * newest first) with type, start time, distance, and the two derived counts:
 * child segments/courses created FROM the activity, and matched efforts that
 * belong to it. A server-side activity-type filter and limit/offset paging keep
 * the Pi's work bounded; the list is one page at a time with an explicit
 * "Load more" rather than an unbounded query.
 *
 * Loading / empty / error states are explicit. Desktop and landscape render a
 * table (landscape scrolls horizontally); portrait renders stacked cards with
 * full-width touch targets.
 */

const PAGE_SIZE = 50;

const TYPE_OPTIONS: { value: string | null; label: string }[] = [
  { value: null, label: 'Any' },
  { value: 'Run', label: 'Run' },
  { value: 'Trail Run', label: 'Trail Run' },
  { value: 'Hike', label: 'Hike' },
  { value: 'Walk', label: 'Walk' },
  { value: 'Bike', label: 'Bike' },
  { value: 'Ski', label: 'Ski' },
];

interface ActivitySourceListProps {
  /** Called with the chosen activity. */
  onSelect: (activity: ActivitySummary) => void;
}

export default function ActivitySourceList({ onSelect }: ActivitySourceListProps) {
  const [activityType, setActivityType] = useState<string | null>(null);
  const [rows, setRows] = useState<ActivitySummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);

  const load = useCallback(async (type: string | null, offset: number) => {
    if (offset === 0) setLoading(true);
    else setLoadingMore(true);
    try {
      const res = await API.segments.getActivities({
        activity_type: type ?? undefined,
        limit: PAGE_SIZE,
        offset,
      });
      const page = res.data ?? [];
      setRows((prev) => (offset === 0 ? page : [...prev, ...page]));
      setHasMore(page.length === PAGE_SIZE);
      setError(null);
    } catch {
      setError('Could not load activities.');
      if (offset === 0) setRows([]);
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, []);

  useEffect(() => {
    load(activityType, 0);
  }, [activityType, load]);

  return (
    <div className="space-y-3">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">Select an activity to map from:</p>
        <div
          className="flex flex-wrap gap-1 rounded-md bg-gray-100 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 p-1"
          role="group"
          aria-label="Filter activities by activity type"
        >
          {TYPE_OPTIONS.map((opt) => (
            <button
              key={opt.label}
              type="button"
              aria-pressed={activityType === opt.value}
              onClick={() => setActivityType(opt.value)}
              className={`px-3 py-1.5 text-xs font-medium rounded ${
                activityType === opt.value
                  ? 'bg-blue-600 text-white'
                  : 'text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {loading && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          Loading activities…
        </p>
      )}

      {!loading && error && (
        <div className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <p role="alert" className="text-sm text-red-600 dark:text-red-400 mb-2">{error}</p>
          <button
            type="button"
            onClick={() => load(activityType, 0)}
            className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Retry
          </button>
        </div>
      )}

      {!loading && !error && rows.length === 0 && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          No matching activities found.
        </p>
      )}

      {!loading && !error && rows.length > 0 && (
        <>
          <ActivityTable rows={rows} onSelect={onSelect} />
          <div className="flex items-center justify-between gap-3 px-1">
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Showing {rows.length} activit{rows.length === 1 ? 'y' : 'ies'}
            </p>
            {hasMore && (
              <button
                type="button"
                onClick={() => load(activityType, rows.length)}
                disabled={loadingMore}
                className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {loadingMore ? 'Loading…' : 'Load more'}
              </button>
            )}
          </div>
        </>
      )}
    </div>
  );
}