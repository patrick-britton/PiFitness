'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { API } from '@/lib/api-client';
import {
  SegmentListQuery,
  SegmentListRow,
  SegmentKindFilter,
  SegmentActivityTypeFilter,
} from '@/lib/types/leaderboards';
import LeaderboardTable from './components/LeaderboardTable';

/**
 * Leaderboards page (009-002, T06).
 *
 * Four filter controls (name substring, course/segment kind, minimum distance,
 * activity type) drive a server-filtered fetch. The result list renders
 * name, last-effort date, distance, matched-attempt count, and elevation gain
 * with progress-scaled bars (each bar scaled to the filtered set min/max over
 * the full cell width). Columns are sortable (Name, Last Effort, Type,
 * Distance, Attempts, Elevation) via asc/desc toggles with `aria-sort`.
 * Single-row selection; empty-state shows an informational message and offers
 * no leaderboard controls. Reset clears all filters back to defaults.
 *
 * Layout-aware for desktop / portrait / landscape and themed via Tailwind
 * dark: tokens (design-system.md). No hardcoded colors/breakpoints/sizes.
 */

const DEFAULT_QUERY: SegmentListQuery = {
  name: '',
  kind: null,
  min_distance: 0,
  activity_type: null,
};

const KIND_OPTIONS: { value: SegmentKindFilter; label: string }[] = [
  { value: null, label: 'Both' },
  { value: 'course', label: 'Course' },
  { value: 'segment', label: 'Segment' },
];

const ACTIVITY_TYPE_OPTIONS: { value: SegmentActivityTypeFilter; label: string }[] = [
  { value: null, label: 'Any' },
  { value: 'Run', label: 'Run' },
  { value: 'Trail Run', label: 'Trail Run' },
  { value: 'Hike', label: 'Hike' },
  { value: 'Walk', label: 'Walk' },
  { value: 'Bike', label: 'Bike' },
  { value: 'Ski', label: 'Ski' },
];

/** Format an ISO-8601 date as `d-mmm-yyyy`; falls back to "-" when missing. */
function formatDate(iso: string | null): string {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '-';
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

/**
 * Scale a value into [floor, 100]% across the data range [min, max].
 * Returns 100 when the range is flat/degenerate so a single-row result still
 * renders a full bar rather than an empty one.
 */
function scale(value: number, min: number, max: number, floor = 8): number {
  if (!Number.isFinite(value) || !Number.isFinite(min) || !Number.isFinite(max) || max <= min) return 100;
  const clamped = Math.min(Math.max(value, min), max);
  return Math.round(floor + ((clamped - min) / (max - min)) * (100 - floor));
}

/** Columns of the segment list that can be sorted. */
type SortKey =
  | 'segment_name'
  | 'last_effort'
  | 'activity_type_name'
  | 'distance_mi'
  | 'matched_activity_count'
  | 'elevation_gain';

type SortDir = 'asc' | 'desc';

/** Sortable column header: toggles asc/desc and exposes aria-sort. */
function SortableTh({
  label,
  col,
  sortKey,
  sortDir,
  onSort,
  className,
}: {
  label: string;
  col: SortKey;
  sortKey: SortKey | null;
  sortDir: SortDir;
  onSort: (col: SortKey) => void;
  className?: string;
}) {
  const active = sortKey === col;
  return (
    <th
      className={className}
      aria-sort={active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'}
    >
      <button
        type="button"
        onClick={() => onSort(col)}
        className="inline-flex items-center gap-1 font-medium hover:text-gray-700 dark:hover:text-gray-200"
      >
        {label}
        <span
          aria-hidden="true"
          className={active ? 'text-gray-900 dark:text-white' : 'text-gray-300 dark:text-gray-600'}
        >
          {active ? (sortDir === 'asc' ? '\u25B2' : '\u25BC') : '\u2195'}
        </span>
      </button>
    </th>
  );
}
export default function LeaderboardsPage() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const [query, setQuery] = useState<SegmentListQuery>(DEFAULT_QUERY);
  const [rows, setRows] = useState<SegmentListRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [showLeaderboard, setShowLeaderboard] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);
  const [sort, setSort] = useState<{ key: SortKey | null; dir: SortDir }>({ key: null, dir: 'asc' });

  const fetchSegments = useCallback(async (q: SegmentListQuery) => {
    setLoading(true);
    setError(null);
    try {
      const data = await API.activities.getLeaderboardSegments(q);
      setRows(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load segments');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSegments(DEFAULT_QUERY);
    setHasSearched(true);
  }, [fetchSegments]);

  const handleApply = useCallback(() => {
    setSelectedId(null);
    fetchSegments(query);
  }, [query, fetchSegments]);

  const handleReset = useCallback(() => {
    setQuery(DEFAULT_QUERY);
    setSelectedId(null);
    setShowLeaderboard(false);
    fetchSegments(DEFAULT_QUERY);
  }, [fetchSegments]);

  const handleSelect = useCallback((segmentId: number) => {
    setSelectedId((prev) => (prev === segmentId ? null : segmentId));
  }, []);

  /** Toggle sort: same column flips direction, new column starts ascending. */
  const handleSort = useCallback((col: SortKey) => {
    setSort((prev) =>
      prev.key === col ? { key: col, dir: prev.dir === 'asc' ? 'desc' : 'asc' } : { key: col, dir: 'asc' }
    );
  }, []);

  const extents = useMemo(() => {
    const dists = rows.map((r) => r.distance_mi);
    const attempts = rows.map((r) => r.matched_activity_count);
    const elevations = rows.map((r) => r.elevation_gain).filter((v): v is number => v != null);
    const numOrZero = (v: number) => (Number.isFinite(v) ? v : 0);
    const minOf = (arr: number[]) => (arr.length ? Math.min(...arr) : 0);
    const maxOf = (arr: number[]) => (arr.length ? Math.max(...arr) : 0);
    return {
      dist: { min: minOf(dists), max: maxOf(dists) },
      attempts: { min: minOf(attempts), max: maxOf(attempts) },
      elev: { min: minOf(elevations), max: maxOf(elevations), hasData: elevations.length > 0 },
      numOrZero,
    };
  }, [rows]);

  // Client-side sort of the fetched rows; nulls sort last; server order when no sort key.
  const sortedRows = useMemo(() => {
    if (!sort.key) return rows;
    const dir = sort.dir === 'asc' ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = a[sort.key as SortKey];
      const bv = b[sort.key as SortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
  }, [rows, sort]);

  const selectedRow = rows.find((r) => r.segment_id === selectedId) || null;

  const inputCls =
    'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
  const primaryBtn =
    'px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors';
  const secondaryBtn =
    'px-4 py-2 text-sm font-medium rounded-md bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors';

  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <div
          className={
            isDesktop ? 'grid grid-cols-4 gap-4' : isLandscape ? 'grid grid-cols-2 gap-3' : 'grid grid-cols-1 gap-3'
          }
        >
          <div>
            <label htmlFor="lb-name" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Name
            </label>
            <input
              id="lb-name"
              type="text"
              placeholder="Substring..."
              value={query.name || ''}
              onChange={(e) => setQuery((q) => ({ ...q, name: e.target.value }))}
              className={inputCls}
            />
          </div>
          <div>
            <label htmlFor="lb-kind" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Kind
            </label>
            <select
              id="lb-kind"
              value={query.kind ?? ''}
              onChange={(e) => setQuery((q) => ({ ...q, kind: (e.target.value || null) as SegmentKindFilter }))}
              className={inputCls}
            >
              {KIND_OPTIONS.map((opt) => (
                <option key={String(opt.value)} value={opt.value ?? ''}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="lb-min-dist" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Min Distance (mi)
            </label>
            <input
              id="lb-min-dist"
              type="number"
              min={0}
              step={0.1}
              value={query.min_distance}
              onChange={(e) => setQuery((q) => ({ ...q, min_distance: Math.max(0, Number(e.target.value) || 0) }))}
              className={inputCls}
            />
          </div>
          <div>
            <label htmlFor="lb-act" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Activity Type
            </label>
            <select
              id="lb-act"
              value={query.activity_type ?? ''}
              onChange={(e) =>
                setQuery((q) => ({ ...q, activity_type: (e.target.value || null) as SegmentActivityTypeFilter }))
              }
              className={inputCls}
            >
              {ACTIVITY_TYPE_OPTIONS.map((opt) => (
                <option key={String(opt.value)} value={opt.value ?? ''}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div className="flex items-center gap-3 mt-4">
          <button type="button" onClick={handleApply} className={primaryBtn}>
            Apply Filters
          </button>
          <button type="button" onClick={handleReset} className={secondaryBtn}>
            Reset
          </button>
        </div>
      </div>

      {loading && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">Loading segments...</p>
        </div>
      )}

      {error && !loading && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm font-medium text-red-800 dark:text-red-200">Error loading segments</p>
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</p>
        </div>
      )}

      {!loading && !error && hasSearched && rows.length === 0 && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">No segments match the current filters.</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">Adjust the filters above and apply again.</p>
        </div>
      )}

      {!loading && !error && rows.length > 0 && (
        <div className="space-y-3">
          <p className="text-xs text-gray-500 dark:text-gray-400">
            {rows.length} segment{rows.length === 1 ? '' : 's'} found
          </p>

          {isDesktop && (
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-xs font-medium text-gray-500 dark:text-gray-400">
                    <th className="px-4 py-2 w-8" aria-label="Select" />
                    <SortableTh label="Name" col="segment_name" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2" />
                    <SortableTh label="Last Effort" col="last_effort" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2" />
                    <SortableTh label="Type" col="activity_type_name" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2" />
                    <SortableTh label="Distance (mi)" col="distance_mi" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
                    <SortableTh label="Attempts" col="matched_activity_count" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
                    <SortableTh label="Elevation (m)" col="elevation_gain" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.map((row) => {
                    const sel = selectedId === row.segment_id;
                    return (
                      <tr
                        key={row.segment_id}
                        onClick={() => handleSelect(row.segment_id)}
                        className={`border-b border-gray-100 dark:border-gray-700 cursor-pointer transition-colors ${
                          sel ? 'bg-blue-50 dark:bg-blue-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'
                        }`}
                      >
                        <td className="px-4 py-2">
                          <input
                            type="radio"
                            name="segment-select"
                            checked={sel}
                            onChange={() => handleSelect(row.segment_id)}
                            aria-label={`Select ${row.segment_name}`}
                            className="accent-blue-600"
                          />
                        </td>
                        <td className="px-4 py-2 font-medium text-gray-900 dark:text-white">{row.segment_name}</td>
                        <td className="px-4 py-2 text-gray-600 dark:text-gray-300">{formatDate(row.last_effort)}</td>
                        <td className="px-4 py-2 text-gray-600 dark:text-gray-300">
                          {row.activity_type_name ?? '-'}
                        </td>
                        <td className="px-4 py-2">
                          <ProgressBar
                            pct={scale(row.distance_mi, extents.dist.min, extents.dist.max)}
                            label={row.distance_mi.toFixed(2)}
                            color="var(--chart-1)"
                          />
                        </td>
                        <td className="px-4 py-2">
                          <ProgressBar
                            pct={scale(row.matched_activity_count, extents.attempts.min, extents.attempts.max)}
                            label={String(row.matched_activity_count)}
                            color="var(--chart-2)"
                          />
                        </td>
                        <td className="px-4 py-2">
                          {extents.elev.hasData ? (
                            <ProgressBar
                              pct={scale(extents.numOrZero(row.elevation_gain ?? 0), extents.elev.min, extents.elev.max)}
                              label={row.elevation_gain != null ? `${row.elevation_gain}m` : '-'}
                              color="var(--chart-3)"
                            />
                          ) : (
                            <span className="text-gray-400 dark:text-gray-500">-</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {!isDesktop && (
            <div className="space-y-2">
              {sortedRows.map((row) => {
                const sel = selectedId === row.segment_id;
                return (
                  <button
                    key={row.segment_id}
                    type="button"
                    onClick={() => handleSelect(row.segment_id)}
                    className={`w-full text-left bg-white dark:bg-gray-800 border rounded-md p-3 transition-colors ${
                      sel
                        ? 'border-blue-500 dark:border-blue-400'
                        : 'border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600'
                    }`}
                    aria-pressed={sel}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-sm font-semibold text-gray-900 dark:text-white truncate">
                        {row.segment_name}
                      </p>
                      <span className="shrink-0 text-xs font-medium text-gray-500 dark:text-gray-400">
                        {row.is_course ? 'Course' : 'Segment'}
                      </span>
                    </div>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                      {formatDate(row.last_effort)} &middot; {row.activity_type_name ?? '-'}
                    </p>
                    <div className="mt-2 space-y-1.5">
                      <ProgressBar
                        pct={scale(row.distance_mi, extents.dist.min, extents.dist.max)}
                        label={`${row.distance_mi.toFixed(2)} mi`}
                        color="var(--chart-1)"
                      />
                      <ProgressBar
                        pct={scale(row.matched_activity_count, extents.attempts.min, extents.attempts.max)}
                        label={`${row.matched_activity_count} attempts`}
                        color="var(--chart-2)"
                      />
                      {extents.elev.hasData && (
                        <ProgressBar
                          pct={scale(extents.numOrZero(row.elevation_gain ?? 0), extents.elev.min, extents.elev.max)}
                          label={row.elevation_gain != null ? `${row.elevation_gain}m` : '-'}
                          color="var(--chart-3)"
                        />
                      )}
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      )}

      {selectedRow && !showLeaderboard && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm text-gray-600 dark:text-gray-300">
              Selected: <span className="font-medium">{selectedRow.segment_name}</span>
            </p>
            <button
              type="button"
              onClick={() => setShowLeaderboard(true)}
              className={primaryBtn}
            >
              Generate Leaderboard
            </button>
          </div>
        </div>
      )}

      {selectedRow && showLeaderboard && (
        <div>
          <div className="flex items-center gap-3 mb-3">
            <button
              type="button"
              onClick={() => setShowLeaderboard(false)}
              className={secondaryBtn}
            >
              Back to List
            </button>
          </div>
          <LeaderboardTable segmentId={selectedRow.segment_id} segmentName={selectedRow.segment_name} />
        </div>
      )}
    </div>
  );
}

/**
 * A progress-scaled bar: width % from the filtered set, themed via a chart
 * token. The track spans the full cell width with the value above it — a
 * side-by-side label left the track too narrow for differences to read.
 */
function ProgressBar({ pct, label, color }: { pct: number; label: string; color: string }) {
  return (
    <div title={label}>
      <div className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">{label}</div>
      <div
        className="mt-1 h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden"
        role="progressbar"
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label={label}
      >
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  );
}


