'use client';

import { useCallback, useEffect, useState } from 'react';
import { API } from '@/lib/api-client';
import {
  SegmentListRow,
  SegmentKindFilter,
  SegmentActivityTypeFilter,
} from '@/lib/types/leaderboards';
import SegmentPickerTable from './SegmentPickerTable';

const KIND_OPTIONS: { value: SegmentKindFilter; label: string }[] = [
  { value: null, label: 'All' },
  { value: 'course', label: 'Course' },
  { value: 'segment', label: 'Segment' },
];

const TYPE_OPTIONS: { value: SegmentActivityTypeFilter; label: string }[] = [
  { value: null, label: 'Any' },
  { value: 'Run', label: 'Run' },
  { value: 'Trail Run', label: 'Trail Run' },
  { value: 'Hike', label: 'Hike' },
  { value: 'Walk', label: 'Walk' },
  { value: 'Bike', label: 'Bike' },
  { value: 'Ski', label: 'Ski' },
];

interface SegmentPickerProps {
  onSelect: (segment: SegmentListRow) => void;
}

export default function SegmentPicker({ onSelect }: SegmentPickerProps) {
  const [nameFilter, setNameFilter] = useState('');
  const [minDistance, setMinDistance] = useState(0);
  const [kind, setKind] = useState<SegmentKindFilter>(null);
  const [activityType, setActivityType] = useState<SegmentActivityTypeFilter>(null);
  const [rows, setRows] = useState<SegmentListRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // The list endpoint returns the full filtered set (no limit/offset params):
  // one fetch per filter change, no "load more" paging.
  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await API.segments.getSegmentsList({
        name: nameFilter || '',
        kind,
        min_distance: minDistance,
        activity_type: activityType,
      });
      setRows(res.data ?? []);
      setError(null);
    } catch {
      setError('Could not load segments.');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [nameFilter, minDistance, kind, activityType]);

  useEffect(() => {
    load();
  }, [load]);

  const clearFilters = () => {
    setNameFilter('');
    setMinDistance(0);
    setKind(null);
    setActivityType(null);
    };

  return (
    <div className="space-y-3">
      {/* Filter bar (FR-7) */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3 space-y-3">
        <div>
          <label
            htmlFor="seg-name-filter"
            className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
          >
            Segment name
          </label>
          <input
            id="seg-name-filter"
            type="text"
            value={nameFilter}
            onChange={(e) => setNameFilter(e.target.value)}
            placeholder="Search by name…"
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="space-y-1.5">
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Distance (miles, ≥)
          </label>
          <input
            type="number"
            min={0}
            step={0.1}
            value={minDistance}
            onChange={(e) => setMinDistance(Math.max(0, Number(e.target.value) || 0))}
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Type
          </label>
          <div
                        className="flex flex-wrap gap-1 rounded-md bg-gray-100 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 p-1"
            role="group"
            aria-label="Filter segments by kind"
          >
            {KIND_OPTIONS.map((opt) => (
              <button
                key={opt.label}
                type="button"
                aria-pressed={kind === opt.value}
                onClick={() => setKind(opt.value)}
                className={`px-3 py-1.5 text-xs font-medium rounded transition-colors ${
                  kind === opt.value
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Activity type
          </label>
          <div
                        className="flex flex-wrap gap-1 rounded-md bg-gray-100 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 p-1"
            role="group"
            aria-label="Filter segments by activity type"
          >
            {TYPE_OPTIONS.map((opt) => (
              <button
                key={opt.label}
                type="button"
                aria-pressed={activityType === opt.value}
                onClick={() => setActivityType(opt.value)}
                className={`px-3 py-1.5 text-xs font-medium rounded transition-colors ${
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

        <button
          type="button"
          onClick={clearFilters}
          className="px-3 py-1.5 text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          Clear filters
        </button>
      </div>

      {loading && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          Loading segments…
        </p>
      )}

      {!loading && error && (
        <div className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <p role="alert" className="text-sm text-red-600 dark:text-red-400 mb-2">
            {error}
          </p>
          <button
            type="button"
            onClick={() => load()}
            className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Retry
          </button>
        </div>
      )}

      {!loading && !error && rows.length === 0 && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          No segments match your filters.
        </p>
      )}

      {!loading && !error && rows.length > 0 && (
        <>
          <SegmentPickerTable rows={rows} onSelect={onSelect} />
          <p className="text-xs text-gray-500 dark:text-gray-400 px-1">
            Showing {rows.length} segment{rows.length === 1 ? '' : 's'}
          </p>
        </>
      )}
    </div>
  );
}