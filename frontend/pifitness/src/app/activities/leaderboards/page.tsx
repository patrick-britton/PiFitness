'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { API } from '@/lib/api-client';
import { scale } from '@/lib/bar-geometry';
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
  { value: 'course', label: 'Course' },
  { value: 'segment', label: 'Segment' },
  { value: null, label: 'Both' },
];

/** Raw DB activity-type strings are snake_case (`trail_running`, `hiking`);
 *  prettify for display per OQ-3 (`Trail Running`, `Hiking`). Display-only —
 *  the canonical filter key is sent to the backend. */
function prettyType(raw: string): string {
  return raw
    .split('_')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(' ');
}

/** Map a raw `activity_type_name` to the canonical backend filter key whose
 *  `_ACTIVITY_TYPE_PATTERNS` ILIKE semantics already match that raw form.
 *  Returns null when no canonical key matches (surfaced explicitly, never guessed). */
function canonicalTypeKey(raw: string): SegmentActivityTypeFilter {
  const v = raw.toLowerCase();
  if (v.includes('trail') && v.includes('run')) return 'Trail Run';
  if (v.includes('run') && !v.includes('trail')) return 'Run';
  if (v.includes('hik')) return 'Hike';
  if (v.includes('walk')) return 'Walk';
  if (v.includes('bik')) return 'Bike';
  if (v.includes('ski')) return 'Ski';
  return null;
}

/** Format an ISO-8601 date as `d-mmm-yyyy`; falls back to "-" when missing. */
function formatDate(iso: string | null): string {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '-';
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

/**
 * Segmented single-select toggle (009-009 FR-1, AC-1): a pill group used for
 * Kind (no visible header by design) and Activity Type. Themed via Tailwind
 * dark tokens; `aria-pressed` carries state for assistive tech.
 */
function ToggleGroup({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string | null;
  options: { value: string | null; label: string }[];
  onChange: (v: string | null) => void;
}) {
  return (
    <div
      className="flex rounded-md bg-gray-100 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 overflow-hidden"
      role="group"
      aria-label={label}
    >
      {options.map((opt) => {
        const active = (opt.value ?? null) === (value ?? null);
        return (
          <button
            key={opt.label}
            type="button"
            aria-pressed={active}
            onClick={() => onChange(opt.value ?? null)}
            className={`flex-1 px-2.5 py-1.5 text-xs font-medium whitespace-nowrap transition-colors ${
              active
                ? 'bg-blue-600 text-white'
                : 'text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600'
            }`}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Min-distance stepper (009-009 FR-1): ± buttons step exactly 1 mile while
 * preserving decimals (0.25 +1 → 1.25); direct decimal entry stays supported.
 * Native number spinners are hidden — a native `step=1` input would round
 * 0.25 up to 1 on increment.
 */
function MinDistanceStepper({
  value,
  inputCls,
  onChange,
}: {
  value: number;
  inputCls: string;
  onChange: (v: number) => void;
}) {
  const step = (delta: number) => onChange(Math.max(0, Math.round((value + delta) * 100) / 100));
  const btn =
    'w-7 flex-1 text-[10px] leading-none font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500';
  return (
    <div className="flex">
      <input
        id="lb-min-dist"
        type="number"
        min={0}
        step="any"
        value={value}
        onChange={(e) => onChange(Math.max(0, Number(e.target.value) || 0))}
        className={`${inputCls} rounded-r-none border-r-0 [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none`}
      />
      <div className="flex flex-col">
        <button
          type="button"
          onClick={() => step(1)}
          aria-label="Increase minimum distance by 1 mile"
          className={`${btn} rounded-tr-md border border-gray-300 dark:border-gray-600`}
        >
          ▲
        </button>
        <button
          type="button"
          onClick={() => step(-1)}
          aria-label="Decrease minimum distance by 1 mile"
          className={`${btn} rounded-br-md border border-t-0 border-gray-300 dark:border-gray-600`}
        >
          ▼
        </button>
      </div>
    </div>
  );
}

/** Columns of the segment list that can be sorted (Type column dropped, 009-009 T03). */
type SortKey =
  | 'segment_name'
  | 'last_effort'
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
  /** Activity types present in the (unfiltered) data — drives the type toggle (009-009 FR-1). */
  const [availableTypes, setAvailableTypes] = useState<string[]>([]);

  const fetchSegments = useCallback(async (q: SegmentListQuery) => {
    setLoading(true);
    setError(null);
    try {
      const data = await API.activities.getLeaderboardSegments(q);
      setRows(data);
      // Cache the type list from any unfiltered-by-type fetch so the toggle
      // only offers types that actually have segments (Bike/Ski stay hidden
      // until they exist) — no extra backend endpoint (009-009 key decision).
      if (!q.activity_type) {
        setAvailableTypes(
          Array.from(new Set(data.map((r) => r.activity_type_name).filter((t): t is string => !!t))).sort()
        );
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load segments');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  // Auto-apply (009-009 FR-1, AC-2): every filter change immediately re-queries
  // the list server-side — no Apply button. Text edits debounce (400ms) so the
  // Pi absorbs typing; toggles and stepper clicks fire on the next tick.
  const prevQueryRef = useRef<SegmentListQuery | null>(null);
  useEffect(() => {
    const prev = prevQueryRef.current;
    prevQueryRef.current = query;
    const nameOnlyChanged =
      prev != null &&
      (prev.name || '') !== (query.name || '') &&
      prev.kind === query.kind &&
      prev.min_distance === query.min_distance &&
      prev.activity_type === query.activity_type;
    const timer = setTimeout(
      () => {
        setSelectedId(null);
        setHasSearched(true);
        fetchSegments(query);
      },
      nameOnlyChanged ? 400 : 0
    );
    return () => clearTimeout(timer);
  }, [query, fetchSegments]);

  const handleReset = useCallback(() => {
    setQuery(DEFAULT_QUERY);
    setSelectedId(null);
    setShowLeaderboard(false);
  }, []);

  const handleSelect = useCallback((segmentId: number) => {
    setSelectedId(segmentId);
    // Auto-load (AC-5): selection immediately populates the leaderboard —
    // no separate Generate click.
    setShowLeaderboard(true);
  }, []);

  // "x" on the segment header (AC-5/AC-10): restores filters + list and drops
  // the leaderboard, race view, and all activity selections (LeaderboardTable
  // unmounts, so its internal state resets with it).
  const clearSelectedSegment = useCallback(() => {
    setSelectedId(null);
    setShowLeaderboard(false);
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
    const maxOf = (arr: number[]) => (arr.length ? Math.max(...arr) : 0);
    return {
      // Zero-based bars (AC-3): only the set max is needed.
      dist: { max: maxOf(dists) },
      attempts: { max: maxOf(attempts) },
      elev: { max: maxOf(elevations), hasData: elevations.length > 0 },
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
  // Focus mode (AC-5): a segment is selected and its leaderboard is shown —
  // filters and the segment list hide; the segment header with "x" restores.
  const focusMode = selectedRow != null && showLeaderboard;

  const inputCls =
    'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
  const primaryBtn =
    'px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors';
  const secondaryBtn =
    'px-4 py-2 text-sm font-medium rounded-md bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors';

  // Type toggle options (009-009 FR-1, T13/OQ-3): DISTINCT raw
  // `activity_type_name` values from the cached unfiltered fetch, prettified
  // for display (`trail_running` → `Trail Running`) and mapped to the
  // canonical backend key the endpoint validates. Unmapped raws surface with
  // their pretty label and a null value (never guessed) — selecting them
  // sends no filter rather than a wrong one.
  const typeOptions: { value: SegmentActivityTypeFilter; label: string }[] = [
    { value: null, label: 'Any' },
    ...availableTypes.map((raw) => ({
      value: canonicalTypeKey(raw),
      label: prettyType(raw),
    })),
  ];

  const searchBox = (
    <div>
      <label htmlFor="lb-name" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
        Search
      </label>
      <input
        id="lb-name"
        type="text"
        placeholder="segment or course name"
        value={query.name || ''}
        onChange={(e) => setQuery((q) => ({ ...q, name: e.target.value }))}
        className={inputCls}
      />
    </div>
  );

  const minDistBox = (
    <div>
      <label htmlFor="lb-min-dist" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
        Min Distance (mi)
      </label>
      <MinDistanceStepper
        value={query.min_distance}
        inputCls={inputCls}
        onChange={(v) => setQuery((q) => ({ ...q, min_distance: v }))}
      />
    </div>
  );

  const kindBox = (
    <div>
      {/* No header by design (FR-1); invisible spacer keeps pill alignment with sibling labels. */}
      <span aria-hidden="true" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 select-none">
        &nbsp;
      </span>
      <ToggleGroup
        label="Course or segment kind"
        value={query.kind}
        options={KIND_OPTIONS}
        onChange={(v) => setQuery((q) => ({ ...q, kind: v as SegmentKindFilter }))}
      />
    </div>
  );

  return (
    <div className="space-y-4">
      {!focusMode && (
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <div className="space-y-3">
          {/* Activity Type — the first filter the user engages with; top-left (009-009 FR-1). */}
          <div>
            <span id="lb-act-label" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Activity Type
            </span>
            <ToggleGroup
              label="Activity type"
              value={query.activity_type}
              options={typeOptions}
              onChange={(v) => setQuery((q) => ({ ...q, activity_type: v as SegmentActivityTypeFilter }))}
            />
          </div>

          {isDesktop ? (
            <div className="grid grid-cols-[2fr_1fr_1fr] gap-4">
              {searchBox}
              {minDistBox}
              {kindBox}
            </div>
          ) : isLandscape ? (
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-3">
                {searchBox}
                {minDistBox}
              </div>
              {kindBox}
            </div>
          ) : (
            <div className="space-y-3">
              {/* Portrait: Search and Min Distance share one line (≈2/3 vs ≈1/3 width). */}
              <div className="flex items-end gap-3">
                <div className="flex-[2] min-w-0">{searchBox}</div>
                <div className="flex-1 min-w-0">{minDistBox}</div>
              </div>
              {kindBox}
            </div>
          )}
        </div>
        <div className="flex items-center gap-3 mt-4">
          <button type="button" onClick={handleReset} className={secondaryBtn}>
            Reset
          </button>
        </div>
      </div>
      )}

      {!focusMode && loading && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">Loading segments...</p>
        </div>
      )}

      {!focusMode && error && !loading && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm font-medium text-red-800 dark:text-red-200">Error loading segments</p>
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</p>
        </div>
      )}

      {!focusMode && !loading && !error && hasSearched && rows.length === 0 && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">No segments match the current filters.</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">Adjust the filters above.</p>
        </div>
      )}

      {!focusMode && !loading && !error && rows.length > 0 && (
        <div className="space-y-3">
          <p className="text-xs text-gray-500 dark:text-gray-400">
            {rows.length} segment{rows.length === 1 ? '' : 's'} found
          </p>

          {/* One responsive sortable table for all layouts (009-009 T03/AC-4;
              the mobile card display prevented header sorting and was removed). */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-xs font-medium text-gray-500 dark:text-gray-400">
                    <th className="px-4 py-2 w-8" aria-label="Select" />
                    <SortableTh label="Name" col="segment_name" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2" />
                    <SortableTh label="Last Effort" col="last_effort" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2" />
                    <SortableTh label="Distance (mi)" col="distance_mi" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
                    <SortableTh label="Attempts" col="matched_activity_count" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
                    <SortableTh label="Elevation Gain" col="elevation_gain" sortKey={sort.key} sortDir={sort.dir} onSort={handleSort} className="px-4 py-2 w-32" />
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
                        <td className="px-4 py-2">
                          <ProgressBar
                            pct={scale(row.distance_mi, extents.dist.max)}
                            label={`${row.distance_mi.toFixed(1)} mi`}
                            color="rgb(var(--chart-1))"
                          />
                        </td>
                        <td className="px-4 py-2">
                          <ProgressBar
                            pct={scale(row.matched_activity_count, extents.attempts.max)}
                            label={String(row.matched_activity_count)}
                            color="rgb(var(--chart-2))"
                          />
                        </td>
                        <td className="px-4 py-2">
                          {extents.elev.hasData ? (
                            <ProgressBar
                              pct={scale(extents.numOrZero(row.elevation_gain ?? 0), extents.elev.max)}
                              label={row.elevation_gain != null ? `${Math.round(row.elevation_gain)}m` : '-'}
                              color="rgb(var(--chart-3))"
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
        </div>
      )}

      {/* Focus view (AC-5/AC-10): segment header with an "x" that restores the
          filters + list; the leaderboard auto-loads and Generate Race hides it. */}
      {selectedRow && (
        <div>
          <div className="flex items-center justify-between gap-3 mb-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
            <p className="text-sm font-semibold text-gray-900 dark:text-white truncate">
              {selectedRow.segment_name}
            </p>
            <button
              type="button"
              onClick={clearSelectedSegment}
              aria-label={`Clear ${selectedRow.segment_name} selection and restore the segment list`}
              className="shrink-0 w-8 h-8 flex items-center justify-center text-lg font-medium rounded-md text-gray-500 dark:text-gray-400 border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-700 dark:hover:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              ×
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


