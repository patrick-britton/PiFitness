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
import LeaderboardVisuals from './LeaderboardVisuals';

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

const CHART_COLORS = ['var(--chart-1)', 'var(--chart-2)', 'var(--chart-3)', 'var(--chart-4)', 'var(--chart-5)', 'var(--chart-6)'];

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

/**
 * Scale a value into [floor, 100]% across the true data range [min, max].
 * Returns 100 when the range is flat/degenerate so a single-effort set still
 * renders a full bar rather than an empty one.
 */
function scale(value: number, min: number, max: number, floor = 8): number {
  if (!Number.isFinite(value) || !Number.isFinite(min) || !Number.isFinite(max) || max <= min) return 100;
  const clamped = Math.min(Math.max(value, min), max);
  return Math.round(floor + ((clamped - min) / (max - min)) * (100 - floor));
}

function formatGap(gapS: number): string {
  if (gapS === 0) return '0s';
  return `${gapS > 0 ? '+' : ''}${gapS.toFixed(1)}s`;
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  let hours = d.getHours();
  const ampm = hours >= 12 ? 'pm' : 'am';
  hours = hours % 12;
  if (hours === 0) hours = 12;
  return `${d.getDate()} ${months[d.getMonth()]} ${hours}:${String(d.getMinutes()).padStart(2, '0')} ${ampm}`;
}
interface Props {
  segmentId: number;
  segmentName: string;
}

export default function LeaderboardTable({ segmentId, segmentName }: Props) {
  const [data, setData] = useState<Leaderboard | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [range, setRange] = useState<LeaderboardRange>('All Time');
  const [type, setType] = useState<LeaderboardType>('Basic');
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [visuals, setVisuals] = useState<SegmentVisuals | null>(null);
  const [visualsLoading, setVisualsLoading] = useState(false);
  const [visualsError, setVisualsError] = useState<string | null>(null);
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

  // Active rank column for the selected range.
  const rankCol = LEADERBOARD_RANGE_COLUMN[range] as keyof LeaderboardEffort;

  // Filter: drop efforts without a rank in the active range.
  const visibleEfforts = useMemo(() => {
    if (!data) return [];
    return data.efforts.filter((e) => {
      const r = e[rankCol];
      return r != null;
    });
  }, [data, rankCol]);

  // Extent (min/max) for each metric across the visible set.
  const metricExtents = useMemo(() => {
    const cols = METRIC_SETS[type];
    const extents: Record<string, { min: number; max: number }> = {};
    for (const col of cols) {
      const vals = visibleEfforts.map((e) => col.getValue(e)).filter((v): v is number => v != null && Number.isFinite(v));
      // Scale to the TRUE data range. Forcing 0 into the range flattened
      // high-baseline metrics (durations, HR, cadence, weight) into identical
      // full-width bars; the true range keeps relative differences visible.
      extents[col.key] = vals.length ? { min: Math.min(...vals), max: Math.max(...vals) } : { min: 0, max: 0 };
    }
    return extents;
  }, [visibleEfforts, type]);

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

  /** Stage the selected efforts and generate the replay + telemetry visuals. */
  const generateVisuals = useCallback(async () => {
    if (selected.size === 0) return;
    setVisualsLoading(true);
    setVisualsError(null);
    try {
      const result = await API.activities.postLeaderboardVisuals({
        segment_id: segmentId,
        efforts: selectedEfforts,
      });
      visualsKey.current += 1;
      setVisuals(result);
    } catch (err) {
      setVisualsError(err instanceof Error ? err.message : 'Failed to generate visuals');
      setVisuals(null);
    } finally {
      setVisualsLoading(false);
    }
  }, [segmentId, selectedEfforts]);

  const selectAll = useCallback(() => {
    setSelected(new Set(visibleEfforts.map((e) => e.activity_id)));
  }, [visibleEfforts]);

  const clearSelection = useCallback(() => {
    setSelected(new Set());
  }, []);

  const metrics = METRIC_SETS[type];

  return (
    <div className="space-y-4">
      {/* Controls: range + type + refresh */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">{segmentName}</h3>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              {visibleEfforts.length} effort{visibleEfforts.length === 1 ? '' : 's'}
            </p>
          </div>
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

      {/* Desktop table */}
      {!visuals && data && visibleEfforts.length > 0 && (
        <div className="hidden lg:block bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md overflow-x-auto">
          <div className="flex items-center gap-3 px-4 py-2 border-b border-gray-200 dark:border-gray-700">
            <button type="button" onClick={selectAll} className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:underline">Select All</button>
            <button type="button" onClick={clearSelection} className="text-xs font-medium text-gray-500 dark:text-gray-400 hover:underline">Clear</button>
            <span className="text-xs text-gray-500 dark:text-gray-400">{selected.size} selected</span>
          </div>
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-xs font-medium text-gray-500 dark:text-gray-400">
                <th className="px-3 py-2 w-8" aria-label="Select" />
                <th className="px-3 py-2">Rank</th>
                <th className="px-3 py-2">Date</th>
                <th className="px-3 py-2">Gap</th>
                {metrics.map((m) => (<th key={m.key} className="px-3 py-2">{m.label}</th>))}
              </tr>
            </thead>
            <tbody>
              {visibleEfforts.map((effort) => {
                const isSel = selected.has(effort.activity_id);
                return (
                  <tr key={effort.activity_id} className={`border-b border-gray-100 dark:border-gray-700 transition-colors ${isSel ? 'bg-blue-50 dark:bg-blue-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'}`}>
                    <td className="px-3 py-2"><input type="checkbox" checked={isSel} onChange={() => toggleSelect(effort.activity_id)} aria-label={`Select effort ${effort.activity_id}`} className="accent-blue-600" /></td>
                    <td className="px-3 py-2 font-medium text-gray-900 dark:text-white">{(effort[rankCol] as number | null) ?? '—'}</td>
                    <td className="px-3 py-2 text-gray-600 dark:text-gray-300">{formatDate(effort.start_time_utc)}</td>
                    <td className="px-3 py-2 text-gray-600 dark:text-gray-300">{formatGap(effort.gap_s)}</td>
                    {metrics.map((m) => {
                      const ext = metricExtents[m.key];
                      return (
                        <td key={m.key} className="px-3 py-2 min-w-[120px]">
                          <MetricBar pct={scale(m.getValue(effort) ?? 0, ext.min, ext.max)} label={m.format(effort)} color={m.colorVar} />
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
          {visibleEfforts.map((effort) => {
            const isSel = selected.has(effort.activity_id);
            return (
              <div
                key={effort.activity_id}
                className={`bg-white dark:bg-gray-800 border rounded-md p-3 transition-colors ${
                  isSel ? 'border-blue-500 dark:border-blue-400' : 'border-gray-200 dark:border-gray-700'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <input type="checkbox" checked={isSel} onChange={() => toggleSelect(effort.activity_id)} aria-label={`Select effort ${effort.activity_id}`} className="accent-blue-600" />
                  <span className="text-sm font-medium text-gray-900 dark:text-white">Rank {(effort[rankCol] as number | null) ?? '—'}</span>
                  <span className="text-xs text-gray-500 dark:text-gray-400">{formatDate(effort.start_time_utc)}</span>
                </div>
                <div className="mt-1 text-xs text-gray-600 dark:text-gray-300">Gap {formatGap(effort.gap_s)}</div>
                <div className="mt-2 space-y-1.5">
                  {metrics.map((m) => {
                    const ext = metricExtents[m.key];
                    return <MetricBar key={m.key} pct={scale(m.getValue(effort) ?? 0, ext.min, ext.max)} label={`${m.label}: ${m.format(effort)}`} color={m.colorVar} />;
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Generate Visuals control */}
      {!visuals && selected.size > 0 && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm text-gray-600 dark:text-gray-300">
              {selected.size} effort{selected.size === 1 ? '' : 's'} selected
            </p>
            <button
              type="button"
              onClick={generateVisuals}
              disabled={visualsLoading}
              className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors disabled:opacity-50"
            >
              {visualsLoading ? 'Generating…' : 'Generate Visuals'}
            </button>
          </div>
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
 * Progress-scaled bar themed via a chart-token CSS variable. The track spans
 * the full cell width with the value above it, so small relative differences
 * remain legible (a side-by-side label left the track too narrow to read).
 */
function MetricBar({ pct, label, color }: { pct: number; label: string; color: string }) {
  return (
    <div title={label}>
      <div className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">{label}</div>
      <div className="mt-1 h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100} aria-label={label}>
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  );
}


