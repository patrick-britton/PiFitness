'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { API } from '@/lib/api-client';
import {
  Leaderboard,
  LeaderboardEffort,
  LeaderboardRange,
  LeaderboardType,
  LeaderboardEffortSelection,
  SegmentVisuals,
  LEADERBOARD_RANGE_COLUMN,
} from '@/lib/types/leaderboards';
import { inRange } from '@/lib/effort-buckets';
import LeaderboardVisuals from './LeaderboardVisuals';
import EffortLollipop from './EffortLollipop';
import { deltaOf, deltaGeometry } from '@/lib/bar-geometry';

/**
 * LeaderboardTable (009-002, T07).
 * Per-segment leaderboard: range selector (drops rows w/o active rank),
 * type selector (swaps metric sets), progress-scaled metric columns,
 * rank/date/gap, multi-row selection, refresh. Dual-theme via chart tokens.
 */

interface MetricColumn {
  key: string;
  label: string;
  getValue: (e: LeaderboardEffort) => number | null;
  format: (e: LeaderboardEffort) => string;
  colorVar: string;
}

/** Chart-token colors for bar fills. The tokens hold bare RGB channels
 *  (design-system.md), so every CSS use site wraps them — a bare
 *  `var(--chart-N)` is not a valid color (Bug 009-009-T14-1). */
const CHART_COLORS = [
  'rgb(var(--chart-1))', 'rgb(var(--chart-2))', 'rgb(var(--chart-3))',
  'rgb(var(--chart-4))', 'rgb(var(--chart-5))', 'rgb(var(--chart-6))',
];

/** Default-mode racer options (009-009 T05, FR-4). */
type RaceMode = 'default' | 'custom';
type RaceOption = 'prior' | 'cycle' | 'y365' | 'alltime';
const RACE_OPTIONS: { key: RaceOption; label: string }[] = [
  { key: 'prior', label: 'Prior Attempt' },
  { key: 'cycle', label: 'Best of Cycle' },
  { key: 'y365', label: 'Best of Last 365' },
  { key: 'alltime', label: 'Best All Time' },
];

interface Racer {
  effort: LeaderboardEffort;
  label: string;
}

/**
 * Resolve Default-mode racers (009-009 T05, FR-4/AC-7; T17 membership). The most
 * recent effort always races. Each selected "best" option resolves to rank 1
 * withIN its window — 'Best of Last 365' / 'Best of Cycle' are scoped by
 * `cycle_name` (OQ-4; the rank columns are window-scoped ordering values, so
 * rank 1 is the window's best) — demoting to rank 2 when rank 1 IS the most
 * recent effort. Overlapping selections resolve to one activity labeled by the
 * hierarchy Best All Time > Best of Last 365 > Best of Cycle > Prior Attempt
 * (processed in that priority order; once an activity is chosen, later options
 * skip it).
 */
export function resolveDefaultRacers(efforts: LeaderboardEffort[], opts: Set<RaceOption>): Racer[] {
  const mostRecent = efforts.find((e) => e.recency_rank === 1) ?? null;
  if (!mostRecent) return [];
  const racers: Racer[] = [{ effort: mostRecent, label: 'Most Recent' }];
  const chosen = new Set<number>([mostRecent.activity_id]);
  const pick = (
    scopeRank: keyof LeaderboardEffort,
    label: string,
    member?: (e: LeaderboardEffort) => boolean,
  ): void => {
    for (const rank of [1, 2]) {
      const cand = efforts.find(
        (e) => (member ? member(e) : true) && (e[scopeRank] as number | null) === rank
      );
      if (cand && !chosen.has(cand.activity_id)) {
        chosen.add(cand.activity_id);
        racers.push({ effort: cand, label });
        return;
      }
    }
  };
  if (opts.has('alltime')) pick('all_time_rank', 'Best All Time');
  if (opts.has('y365')) pick('last_365_rank', 'Best of Last 365', (e) => inRange(e, 'Last 365'));
  if (opts.has('cycle')) pick('current_cycle_rank', 'Best of Cycle', (e) => inRange(e, 'Current Cycle'));
  if (opts.has('prior')) pick('recency_rank', 'Prior Attempt');
  return racers;
}

/** Type-specific metric sets mirroring legacy col_selector. */
const METRIC_SETS: Record<LeaderboardType, MetricColumn[]> = {
  Basic: [
    { key: 'pace', label: 'Pace', getValue: (e) => e.elapsed_duration_s, format: (e) => e.pace_str ?? '—', colorVar: CHART_COLORS[0] },
    { key: 'gap', label: 'Gap', getValue: (e) => e.gap_s, format: (e) => `${e.gap_s}s`, colorVar: CHART_COLORS[1] },
    { key: 'avg_hr', label: 'Avg HR', getValue: (e) => e.avg_hr, format: (e) => (e.avg_hr != null ? `${e.avg_hr}` : '—'), colorVar: CHART_COLORS[2] },
    { key: 'weight', label: 'Weight', getValue: (e) => e.weight_lb, format: (e) => (e.weight_lb != null ? `${e.weight_lb} lb` : '—'), colorVar: CHART_COLORS[3] },
  ],
  Fitness: [
    { key: 'vo2', label: 'VO2', getValue: (e) => e.vo2_max_value, format: (e) => (e.vo2_max_value != null ? `${e.vo2_max_value}` : '—'), colorVar: CHART_COLORS[0] },
    { key: 'resting_hr', label: 'Resting HR', getValue: (e) => e.resting_hr_asleep, format: (e) => (e.resting_hr_asleep != null ? `${e.resting_hr_asleep}` : '—'), colorVar: CHART_COLORS[1] },
    { key: 'acute_load', label: 'Acute Load', getValue: (e) => e.training_load_acute, format: (e) => (e.training_load_acute != null ? `${e.training_load_acute}` : '—'), colorVar: CHART_COLORS[2] },
    { key: 'weight', label: 'Weight', getValue: (e) => e.weight_lb, format: (e) => (e.weight_lb != null ? `${e.weight_lb} lb` : '—'), colorVar: CHART_COLORS[3] },
    { key: 'fat_pct', label: 'Fat %', getValue: (e) => e.fat_pct, format: (e) => (e.fat_pct != null ? `${(e.fat_pct * 100).toFixed(0)}%` : '—'), colorVar: CHART_COLORS[4] },
    { key: 'muscle_pct', label: 'Muscle %', getValue: (e) => e.muscle_pct, format: (e) => (e.muscle_pct != null ? `${(e.muscle_pct * 100).toFixed(0)}%` : '—'), colorVar: CHART_COLORS[5] },
  ],
  Advanced: [
    { key: 'cadence', label: 'Cadence', getValue: (e) => e.avg_cadence, format: (e) => (e.avg_cadence != null ? `${e.avg_cadence}` : '—'), colorVar: CHART_COLORS[0] },
    { key: 'vert_osc', label: 'Vert Osc', getValue: (e) => e.avg_vert_osc, format: (e) => (e.avg_vert_osc != null ? `${e.avg_vert_osc}` : '—'), colorVar: CHART_COLORS[1] },
    { key: 'gct', label: 'GCT', getValue: (e) => e.avg_gct, format: (e) => (e.avg_gct != null ? `${e.avg_gct}` : '—'), colorVar: CHART_COLORS[2] },
    { key: 'stride', label: 'Stride', getValue: (e) => e.avg_stride_length, format: (e) => (e.avg_stride_length != null ? `${e.avg_stride_length}` : '—'), colorVar: CHART_COLORS[3] },
  ],
  Environment: [
    { key: 'temp', label: 'Temp', getValue: (e) => e.avg_temp, format: (e) => (e.avg_temp != null ? `${e.avg_temp}F` : '—'), colorVar: CHART_COLORS[0] },
    { key: 'altitude', label: 'Altitude Accl', getValue: (e) => e.altitude_acclimation_m, format: (e) => (e.altitude_acclimation_m != null ? `${e.altitude_acclimation_m}m` : '—'), colorVar: CHART_COLORS[1] },
  ],
  Preparedness: [
    { key: 'acute_load', label: 'Acute Load', getValue: (e) => e.training_load_acute, format: (e) => (e.training_load_acute != null ? `${e.training_load_acute}` : '—'), colorVar: CHART_COLORS[0] },
    { key: 'load_pct', label: 'Load %', getValue: (e) => e.training_load_pct, format: (e) => (e.training_load_pct != null ? `${e.training_load_pct.toFixed(0)}%` : '—'), colorVar: CHART_COLORS[1] },
    { key: 'sleep_hours', label: 'Sleep Hrs', getValue: (e) => e.sleep_hours, format: (e) => (e.sleep_hours != null ? `${e.sleep_hours}` : '—'), colorVar: CHART_COLORS[2] },
    { key: 'sleep_score', label: 'Sleep Score', getValue: (e) => e.sleep_score, format: (e) => (e.sleep_score != null ? `${e.sleep_score}` : '—'), colorVar: CHART_COLORS[3] },
    { key: 'awake', label: 'Awake Hrs', getValue: (e) => e.awake_hours, format: (e) => (e.awake_hours != null ? `${e.awake_hours}` : '—'), colorVar: CHART_COLORS[4] },
  ],
};

const RANGE_OPTIONS: LeaderboardRange[] = ['All Time', 'Last 365', 'Current Cycle', 'Most Recent'];
const TYPE_OPTIONS: LeaderboardType[] = ['Basic', 'Fitness', 'Advanced', 'Environment', 'Preparedness'];

/** Signed-seconds delta label, e.g. "+12s" / "-80s" / "0s" (AC-9). */
function formatDeltaS(delta: number): string {
  if (delta === 0) return '0s';
  return `${delta > 0 ? '+' : ''}${Number(delta.toFixed(1))}s`;
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  // d-mmm-yyyy — time of day is irrelevant (FR-4/AC-9).
  return `${d.getDate()}-${months[d.getMonth()]}-${d.getFullYear()}`;
}

interface Props {
  segmentId: number;
  segmentName: string;
  /**
   * Embedded host mode (009-009 T01): the host renders its own segment header,
   * so the component suppresses its built-in name/effort-count header. Future
   * hosts (e.g. the recent-activity report) pass `embedded` for reuse.
   */
  embedded?: boolean;
  /**
   * Host hook invoked when the component clears its per-segment state because
   * the host swapped to a different segmentId (never on initial mount).
   */
  onClear?: () => void;
}

export default function LeaderboardTable({ segmentId, segmentName, embedded = false, onClear }: Props) {
  const [data, setData] = useState<Leaderboard | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [range, setRange] = useState<LeaderboardRange>('All Time');
  const [type, setType] = useState<LeaderboardType>('Basic');
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [visuals, setVisuals] = useState<SegmentVisuals | null>(null);
  const [visualsLoading, setVisualsLoading] = useState(false);
  const [visualsError, setVisualsError] = useState<string | null>(null);
  const [raceMode, setRaceMode] = useState<RaceMode>('default');
  const [raceOptions, setRaceOptions] = useState<Set<RaceOption>>(new Set());
  const visualsKey = useRef(0);

  const fetchLeaderboard = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await API.activities.getLeaderboard(segmentId);
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load leaderboard');
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [segmentId]);

  useEffect(() => {
    fetchLeaderboard();
  }, [fetchLeaderboard]);

  // Reuse contract (009-009 T01): swapping segments must never leak activity
  // selections, visuals, or range/type state across segments — and the host is
  // notified (never on initial mount).
  const mountedRef = useRef(false);
  useEffect(() => {
    setSelected(new Set());
    setVisuals(null);
    setVisualsError(null);
    setRaceMode('default');
    setRaceOptions(new Set());
    if (mountedRef.current) onClear?.();
    mountedRef.current = true;
    // onClear intentionally excluded: host callbacks are stable per host mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [segmentId]);

  // Active rank column for the selected range.
  const rankCol = LEADERBOARD_RANGE_COLUMN[range] as keyof LeaderboardEffort;

  // Range window membership (009-009 T17/OQ-4): the cycle/365 windows keep only
  // efforts inside them (`cycle_name`); 'Most Recent' and 'All Time' are the
  // whole set, re-sorted below. The rank columns are ORDERING values only —
  // every rank column is populated for every row, so they cannot express
  // membership (Bug 009-009-T17-1).
  const visibleEfforts = useMemo(() => {
    if (!data) return [];
    return data.efforts.filter((e) => inRange(e, range));
  }, [data, range]);

  // Rank-sorted view (AC-9): the active range's #1-ranked effort is always
  // the first row, and the entire table re-sorts when the Range changes.
  const sortedEfforts = useMemo(
    () =>
      [...visibleEfforts].sort(
        (a, b) => ((a[rankCol] as number) ?? Infinity) - ((b[rankCol] as number) ?? Infinity)
      ),
    [visibleEfforts, rankCol]
  );
  // Baseline for the diverging bars: the active range's #1-ranked effort.
  const baseline = sortedEfforts[0] ?? null;

  // Max |delta| from the baseline per column — the symmetric scale for the
  // diverging bars (0 axis centered; signed bars left/right of it).
  const gapMaxAbs = useMemo(() => {
    if (!baseline) return 0;
    return Math.max(
      0,
      ...visibleEfforts.map((e) => Math.abs(baseline.elapsed_duration_s - e.elapsed_duration_s))
    );
  }, [visibleEfforts, baseline]);

  const metricDeltas = useMemo(() => {
    const out: Record<string, number> = {};
    for (const col of METRIC_SETS[type]) {
      const bv = baseline ? col.getValue(baseline) : null;
      let maxAbs = 0;
      if (bv != null) {
        for (const e of visibleEfforts) {
          const v = col.getValue(e);
          if (v != null) maxAbs = Math.max(maxAbs, Math.abs(v - bv));
        }
      }
      out[col.key] = maxAbs;
    }
    return out;
  }, [visibleEfforts, type, baseline]);

  const toggleSelect = useCallback((activityId: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(activityId)) {
        next.delete(activityId);
      } else {
        next.add(activityId);
      }
      return next;
    });
  }, []);

  const selectedEfforts = useMemo<LeaderboardEffortSelection[]>(() => {
    if (!data) return [];
    return data.efforts
      .filter((e) => selected.has(e.activity_id))
      .map((e) => ({
        activity_id: e.activity_id,
        start_point: e.activity_start_point,
        end_point: e.activity_end_point,
      }));
  }, [data, selected]);

  // Default-mode racer resolution (009-009 T05): pure client-side over the
  // leaderboard payload; no re-fetch per toggle.
  const defaultRacers = useMemo<Racer[]>(
    () => (data ? resolveDefaultRacers(data.efforts, raceOptions) : []),
    [data, raceOptions]
  );

  // The effort set POSTed to the visuals endpoint: resolved racers in Default
  // mode, the checked rows in Custom mode.
  const raceEffortSelections = useMemo<LeaderboardEffortSelection[]>(() => {
    if (raceMode === 'default') {
      return defaultRacers.map((r) => ({
        activity_id: r.effort.activity_id,
        start_point: r.effort.activity_start_point,
        end_point: r.effort.activity_end_point,
      }));
    }
    return selectedEfforts;
  }, [raceMode, defaultRacers, selectedEfforts]);

  /** Stage the race efforts and generate the replay + telemetry visuals. */
  const generateVisuals = useCallback(async () => {
    if (raceEffortSelections.length === 0) return;
    setVisualsLoading(true);
    setVisualsError(null);
    try {
      const result = await API.activities.postLeaderboardVisuals({
        segment_id: segmentId,
        efforts: raceEffortSelections,
      });
      visualsKey.current += 1;
      setVisuals(result);
    } catch (err) {
      setVisualsError(err instanceof Error ? err.message : 'Failed to generate visuals');
      setVisuals(null);
    } finally {
      setVisualsLoading(false);
    }
  }, [segmentId, raceEffortSelections]);

  const selectAll = useCallback(() => {
    setSelected(new Set(visibleEfforts.map((e) => e.activity_id)));
  }, [visibleEfforts]);

  const clearSelection = useCallback(() => {
    setSelected(new Set());
  }, []);

  const metrics = METRIC_SETS[type];
  // Per-row selection UI (Select All / checkboxes) is Custom mode only (T05).
  const canSelect = raceMode === 'custom';

  return (
    <div className="space-y-4">
      {/* Controls: range + type + refresh */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          {!embedded && (
            <div>
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">{segmentName}</h3>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                {visibleEfforts.length} effort{visibleEfforts.length === 1 ? '' : 's'}
              </p>
            </div>
          )}
          <div className="flex flex-wrap items-center gap-3">
            <div>
              <label htmlFor="lb-range" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Range</label>
              <select
                id="lb-range"
                value={range}
                onChange={(e) => { setRange(e.target.value as LeaderboardRange); setSelected(new Set()); }}
                className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {RANGE_OPTIONS.map((r) => (<option key={r} value={r}>{r}</option>))}
              </select>
            </div>
            <div>
              <label htmlFor="lb-type" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Type</label>
              <select
                id="lb-type"
                value={type}
                onChange={(e) => setType(e.target.value as LeaderboardType)}
                className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {TYPE_OPTIONS.map((t) => (<option key={t} value={t}>{t}</option>))}
              </select>
            </div>
            <div className="flex items-end">
              <button
                type="button"
                onClick={fetchLeaderboard}
                disabled={loading}
                className="px-3 py-1.5 text-sm font-medium rounded-md bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
                aria-label="Refresh leaderboard"
              >
                {loading ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          </div>
        </div>
      </div>

      {loading && !data && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">Loading leaderboard...</p>
        </div>
      )}

      {error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm font-medium text-red-800 dark:text-red-200">Error loading leaderboard</p>
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</p>
          <button type="button" onClick={fetchLeaderboard} className="mt-2 px-3 py-1.5 text-sm font-medium rounded-md bg-red-600 text-white hover:bg-red-700">Retry</button>
        </div>
      )}

      {!loading && !data && !error && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">No leaderboard data for this segment.</p>
        </div>
      )}
{/* Data loaded but the active range dropped every effort (no rank in range). */}
      {data && visibleEfforts.length === 0 && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">No efforts have a rank in the <span className="font-medium">{range}</span> range.</p>
        </div>
      )}

      {/* Lollipop distribution (009-009 T04, AC-6): ALL efforts, recency-bucket
          colored, independent of the active Range (OQ-2). */}
      {!visuals && data && <EffortLollipop data={data} />}

      {/* Default / Custom race selection (009-009 T05, AC-7/AC-8) */}
      {!visuals && data && visibleEfforts.length > 0 && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
          <div className="flex items-center gap-2" role="group" aria-label="Race selection mode">
            {(['default', 'custom'] as const).map((m) => (
              <button
                key={m}
                type="button"
                aria-pressed={raceMode === m}
                onClick={() => setRaceMode(m)}
                className={`px-3 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                  raceMode === m
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700'
                }`}
              >
                {m === 'default' ? 'Default' : 'Custom'}
              </button>
            ))}
          </div>

          {raceMode === 'default' && (
            <div className="mt-3 space-y-2">
              <div className="flex flex-wrap gap-2" role="group" aria-label="Default racers">
                {RACE_OPTIONS.map((o) => {
                  const on = raceOptions.has(o.key);
                  return (
                    <button
                      key={o.key}
                      type="button"
                      aria-pressed={on}
                      onClick={() =>
                        setRaceOptions((prev) => {
                          const next = new Set(prev);
                          if (next.has(o.key)) next.delete(o.key);
                          else next.add(o.key);
                          return next;
                        })
                      }
                      className={`px-3 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                        on
                          ? 'bg-blue-600 text-white border-blue-600'
                          : 'text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700'
                      }`}
                    >
                      {o.label}
                    </button>
                  );
                })}
              </div>
              {raceOptions.size > 0 && (
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <p className="text-xs text-gray-600 dark:text-gray-300">
                    Racers: <span className="font-medium">{defaultRacers.map((r) => r.label).join(' · ')}</span>
                  </p>
                  <button
                    type="button"
                    onClick={generateVisuals}
                    disabled={visualsLoading}
                    className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors disabled:opacity-50"
                  >
                    {visualsLoading ? 'Generating…' : 'Generate Race'}
                  </button>
                </div>
              )}
            </div>
          )}

          {raceMode === 'custom' && selected.size > 0 && (
            <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
              <p className="text-xs text-gray-600 dark:text-gray-300">
                {selected.size} effort{selected.size === 1 ? '' : 's'} selected
              </p>
              <button
                type="button"
                onClick={generateVisuals}
                disabled={visualsLoading}
                className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors disabled:opacity-50"
              >
                {visualsLoading ? 'Generating…' : 'Generate Race'}
              </button>
            </div>
          )}
        </div>
      )}

      {/* Desktop table */}
      {!visuals && data && visibleEfforts.length > 0 && (
        <div className="hidden lg:block bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md overflow-x-auto">
          {canSelect && (
            <div className="flex items-center gap-3 px-4 py-2 border-b border-gray-200 dark:border-gray-700">
              <button type="button" onClick={selectAll} className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:underline">Select All</button>
              <button type="button" onClick={clearSelection} className="text-xs font-medium text-gray-500 dark:text-gray-400 hover:underline">Clear</button>
              <span className="text-xs text-gray-500 dark:text-gray-400">{selected.size} selected</span>
            </div>
          )}
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-xs font-medium text-gray-500 dark:text-gray-400">
                {canSelect && <th className="px-3 py-2 w-8" aria-label="Select" />}
                <th className="px-3 py-2">Rank</th>
                <th className="px-3 py-2">Date</th>
                <th className="px-3 py-2">Gap</th>
                {metrics.map((m) => (<th key={m.key} className="px-3 py-2">{m.label}</th>))}
              </tr>
            </thead>
            <tbody>
              {sortedEfforts.map((effort) => {
                const isSel = selected.has(effort.activity_id);
                const gapDelta = baseline ? baseline.elapsed_duration_s - effort.elapsed_duration_s : null;
                return (
                  <tr key={effort.activity_id} className={`border-b border-gray-100 dark:border-gray-700 transition-colors ${isSel ? 'bg-blue-50 dark:bg-blue-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'}`}>
                    {canSelect && (
                      <td className="px-3 py-2"><input type="checkbox" checked={isSel} onChange={() => toggleSelect(effort.activity_id)} aria-label={`Select effort ${effort.activity_id}`} className="accent-blue-600" /></td>
                    )}
                    <td className="px-3 py-2 font-medium text-gray-900 dark:text-white">{(effort[rankCol] as number | null) ?? '—'}</td>
                    <td className="px-3 py-2 text-gray-600 dark:text-gray-300">{formatDate(effort.start_time_utc)}</td>
                    <td className="px-3 py-2 min-w-[110px]">
                      <DeltaBar delta={gapDelta} maxAbs={gapMaxAbs} label={gapDelta != null ? formatDeltaS(gapDelta) : '—'} color="rgb(var(--chart-2))" />
                    </td>
                    {metrics.map((m) => {
                      const d = baseline ? deltaOf(m.getValue(effort), m.getValue(baseline)) : null;
                      return (
                        <td key={m.key} className="px-3 py-2 min-w-[120px]">
                          <DeltaBar delta={d} maxAbs={metricDeltas[m.key] ?? 0} label={m.format(effort)} color={m.colorVar} />
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Portrait / Landscape: mobile cards */}
      {!visuals && data && visibleEfforts.length > 0 && (
        <div className="lg:hidden space-y-2">
          {sortedEfforts.map((effort) => {
            const isSel = selected.has(effort.activity_id);
            const gapDelta = baseline ? baseline.elapsed_duration_s - effort.elapsed_duration_s : null;
            return (
              <div
                key={effort.activity_id}
                className={`bg-white dark:bg-gray-800 border rounded-md p-3 transition-colors ${
                  isSel ? 'border-blue-500 dark:border-blue-400' : 'border-gray-200 dark:border-gray-700'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  {canSelect && (
                    <input type="checkbox" checked={isSel} onChange={() => toggleSelect(effort.activity_id)} aria-label={`Select effort ${effort.activity_id}`} className="accent-blue-600" />
                  )}
                  <span className="text-sm font-medium text-gray-900 dark:text-white">Rank {(effort[rankCol] as number | null) ?? '—'}</span>
                  <span className="text-xs text-gray-500 dark:text-gray-400">{formatDate(effort.start_time_utc)}</span>
                </div>
                <div className="mt-1">
                  <DeltaBar delta={gapDelta} maxAbs={gapMaxAbs} label={gapDelta != null ? `Gap ${formatDeltaS(gapDelta)}` : 'Gap —'} color="rgb(var(--chart-2))" />
                </div>
                <div className="mt-2 space-y-1.5">
                  {metrics.map((m) => {
                    const d = baseline ? deltaOf(m.getValue(effort), m.getValue(baseline)) : null;
                    return <DeltaBar key={m.key} delta={d} maxAbs={metricDeltas[m.key] ?? 0} label={`${m.label}: ${m.format(effort)}`} color={m.colorVar} />;
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {visualsError && !visuals && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm font-medium text-red-800 dark:text-red-200">Failed to generate visuals</p>
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{visualsError}</p>
        </div>
      )}

      {/* Rendered visuals */}
      {visuals && (
        <div className="space-y-3">
          <div className="flex items-center gap-3 rounded-md bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 p-3">
            <button
              type="button"
              onClick={() => { setVisuals(null); setVisualsError(null); setSelected(new Set()); }}
              className="px-3 py-1.5 text-sm font-medium rounded-md bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Back to Leaderboard
            </button>
            <span className="text-sm text-gray-600 dark:text-gray-300">
              {visuals.efforts.length} effort{visuals.efforts.length === 1 ? '' : 's'} selected
            </span>
          </div>
          <LeaderboardVisuals key={visualsKey.current} visuals={visuals} />
        </div>
      )}
    </div>
  );
}

/**
 * Diverging baseline bar (009-009 T06, AC-9): the active range's #1-ranked
 * effort is the 0 axis (center of the track); deltas slower/larger than the
 * baseline extend right, faster/lower extend left — per the description's
 * example (Most Recent 200s/241.9 lb baseline; the faster 120s attempt shows
 * −80s extending left of 0). The label above shows the row's own value; the
 * bar shows the signed delta vs the baseline (no bar when unrecorded).
 */
function DeltaBar({ delta, maxAbs, label, color }: { delta: number | null; maxAbs: number; label: string; color: string }) {
  const { width, side } = deltaGeometry(delta, maxAbs);
  return (
    <div title={label}>
      <div className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">{label}</div>
      <div
        className="relative mt-1 h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden"
        role="progressbar"
        aria-valuemin={-100}
        aria-valuemax={100}
        aria-valuenow={delta != null && maxAbs > 0 ? Math.round((delta / maxAbs) * 100) : 0}
        aria-label={label}
      >
        <div className="absolute inset-y-0 left-1/2 w-px bg-gray-400 dark:bg-gray-500" aria-hidden="true" />
        {width > 0 && (
          <div
            className="absolute inset-y-0 transition-all"
            style={
              side === 'right'
                ? { left: '50%', width: `${width}%`, backgroundColor: color }
                : { right: '50%', width: `${width}%`, backgroundColor: color }
            }
          />
        )}
      </div>
    </div>
  );
}


