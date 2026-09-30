'use client';

import { memo } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { CandidateEffort } from '@/lib/types/segment-management';
import { formatIsoDate, formatMiles } from '@/lib/date-format';

/**
 * Candidate-effort list body for Match Activities mode (009-003 T09, FR-10/FR-11).
 *
 * Three layouts, same split as the other tables in this module: desktop and
 * landscape render a table (landscape scrolls horizontally), portrait renders
 * stacked cards whose actions sit inside the thumb reach.
 *
 * The five metrics render progress-scaled (value / largest value in THIS
 * candidate set — the legacy ProgressColumn semantics, computed server-side as
 * `*_scaled`) next to their raw number, so a bar is never the only carrier of
 * the value. Confirm/Reject act on a single candidate (FR-11).
 */

interface CandidateTableProps {
  rows: CandidateEffort[];
  /** Candidate whose effort route is currently drawn on the map. */
  selectedActivityId: number | null;
  /** True while a finalize/scoring action runs: actions are disabled. */
  busy: boolean;
  /** Draw this candidate's effort route on the map (FR-10 single-row selection). */
  onSelect: (row: CandidateEffort) => void;
  /** Finalize as a match (FR-11). */
  onConfirm: (row: CandidateEffort) => void;
  /** Finalize as rejected (FR-11). */
  onReject: (row: CandidateEffort) => void;
}

/** One progress-scaled metric: bar plus the readable value. */
function Metric({
  label,
  scaled,
  value,
  unit = '',
}: {
  label: string;
  scaled: number | null;
  value: number | null;
  unit?: string;
}) {
  const pct = Math.round(Math.min(Math.max(scaled ?? 0, 0), 1) * 100);
  return (
    <div>
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs text-gray-500 dark:text-gray-400">{label}</span>
        <span className="text-xs tabular-nums text-gray-700 dark:text-gray-200">
          {value == null ? '—' : `${value.toFixed(2)}${unit}`}
        </span>
      </div>
      <div className="mt-1 h-1.5 w-full rounded-full bg-gray-200 dark:bg-gray-700" aria-hidden="true">
        <div
          className="h-full rounded-full bg-blue-600 dark:bg-blue-400"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

function Metrics({ row }: { row: CandidateEffort }) {
  return (
    <div className="space-y-2">
      <Metric label="Confidence" scaled={row.confidence_scaled} value={row.confidence} />
      <Metric
        label="Distance deviation"
        scaled={row.distance_deviation_scaled}
        value={row.deviation.distance_deviation_m}
        unit=" m"
      />
      <Metric
        label="Polygon deviation"
        scaled={row.polygon_deviation_scaled}
        value={row.deviation.polygon_deviation}
      />
      <Metric
        label="Hausdorff deviation"
        scaled={row.hausdorff_deviation_scaled}
        value={row.deviation.hausdorff_deviation}
      />
      <Metric
        label="Fréchet deviation"
        scaled={row.frechet_deviation_scaled}
        value={row.deviation.frechet_deviation}
      />
    </div>
  );
}

const HEADERS = ['Activity', 'Confidence', 'Distance', 'Polygon', 'Hausdorff', 'Fréchet', ''];

function CandidateTable({
  rows,
  selectedActivityId,
  busy,
  onSelect,
  onConfirm,
  onReject,
}: CandidateTableProps) {
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';

  if (isPortrait) {
    return (
      <ul className="space-y-2">
        {rows.map((row) => {
          const selected = row.activity_id === selectedActivityId;
          return (
            <li
              key={row.activity_id}
              className={`rounded-md border bg-white dark:bg-gray-800 p-3 ${
                selected
                  ? 'border-blue-400 dark:border-blue-500'
                  : 'border-gray-200 dark:border-gray-700'
              }`}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-semibold text-gray-900 dark:text-white tabular-nums">
                  #{row.activity_id}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
                  {formatIsoDate(row.start_time_utc)} · {formatMiles(row.distance_m)} mi
                </span>
              </div>
              {row.elevation_gain != null && (
                <p className="mt-1 text-xs text-gray-600 dark:text-gray-400">
                  {row.elevation_gain} m elevation gain
                </p>
              )}
              <div className="mt-3">
                <Metrics row={row} />
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => onSelect(row)}
                  aria-label={`Show route for candidate ${row.activity_id}`}
                  className="px-3 py-2 min-h-[44px] inline-flex items-center text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {selected ? 'Route shown' : 'Show route'}
                </button>
                <button
                  type="button"
                  onClick={() => onConfirm(row)}
                  disabled={busy}
                  aria-label={`Confirm candidate ${row.activity_id}`}
                  className="px-3 py-2 min-h-[44px] inline-flex items-center text-xs font-medium rounded-md bg-green-700 text-white hover:bg-green-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  Confirm
                </button>
                <button
                  type="button"
                  onClick={() => onReject(row)}
                  disabled={busy}
                  aria-label={`Reject candidate ${row.activity_id}`}
                  className="px-3 py-2 min-h-[44px] inline-flex items-center text-xs font-medium rounded-md bg-red-700 text-white hover:bg-red-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  Reject
                </button>
              </div>
            </li>
          );
        })}
      </ul>
    );
  }

  return (
    <div className="overflow-x-auto rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
      <table className="min-w-full text-sm">
        <caption className="sr-only">
          Candidate efforts for this segment or course, with confidence and geometry deviation
          metrics scaled to the largest value in this candidate set, and confirm/reject controls
        </caption>
        <thead className="bg-gray-50 dark:bg-gray-700/50">
          <tr>
            {HEADERS.map((h, i) => (
              <th
                key={h || 'actions'}
                scope="col"
                className="px-3 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-300 whitespace-nowrap"
              >
                {h || <span className="sr-only">Actions</span>}
                {i > 0 && i < HEADERS.length - 1 ? <span className="sr-only"> (scaled to set max)</span> : null}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
          {rows.map((row) => {
            const selected = row.activity_id === selectedActivityId;
            return (
              <tr
                key={row.activity_id}
                className={
                  selected
                    ? 'bg-blue-50 dark:bg-blue-900/20'
                    : 'hover:bg-gray-50 dark:hover:bg-gray-700/60'
                }
              >
                <td className="px-3 py-2 align-top">
                  <button
                    type="button"
                    onClick={() => onSelect(row)}
                    aria-label={`Show route for candidate ${row.activity_id}`}
                    className="rounded text-left focus:outline-none focus:ring-2 focus:ring-blue-500 min-h-[44px] inline-flex flex-col justify-center px-1"
                  >
                    <span className="block font-medium text-gray-900 dark:text-white tabular-nums">
                      #{row.activity_id}
                    </span>
                    <span className="block text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {formatIsoDate(row.start_time_utc)} · {formatMiles(row.distance_m)} mi
                    </span>
                  </button>
                </td>
                <td className="px-3 py-2 align-top min-w-[110px]">
                  <Metric
                    label="of set max"
                    scaled={row.confidence_scaled}
                    value={row.confidence}
                  />
                </td>
                <td className="px-3 py-2 align-top min-w-[110px]">
                  <Metric
                    label="meters"
                    scaled={row.distance_deviation_scaled}
                    value={row.deviation.distance_deviation_m}
                  />
                </td>
                <td className="px-3 py-2 align-top min-w-[110px]">
                  <Metric
                    label="ratio"
                    scaled={row.polygon_deviation_scaled}
                    value={row.deviation.polygon_deviation}
                  />
                </td>
                <td className="px-3 py-2 align-top min-w-[110px]">
                  <Metric
                    label="meters"
                    scaled={row.hausdorff_deviation_scaled}
                    value={row.deviation.hausdorff_deviation}
                  />
                </td>
                <td className="px-3 py-2 align-top min-w-[110px]">
                  <Metric
                    label="meters"
                    scaled={row.frechet_deviation_scaled}
                    value={row.deviation.frechet_deviation}
                  />
                </td>
                <td className="px-3 py-2 align-top text-right">
                  <div className="flex items-center justify-end gap-2">
                    <button
                      type="button"
                      onClick={() => onConfirm(row)}
                      disabled={busy}
                      aria-label={`Confirm candidate ${row.activity_id}`}
                      className="px-2 py-1 min-h-[44px] min-w-[44px] inline-flex items-center justify-center text-xs font-medium rounded-md bg-green-700 text-white hover:bg-green-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      Confirm
                    </button>
                    <button
                      type="button"
                      onClick={() => onReject(row)}
                      disabled={busy}
                      aria-label={`Reject candidate ${row.activity_id}`}
                      className="px-2 py-1 min-h-[44px] min-w-[44px] inline-flex items-center justify-center text-xs font-medium rounded-md bg-red-700 text-white hover:bg-red-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      Reject
                    </button>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Memoised (009-003 T42, fixes Bug 009-003-41-1): the Match Activities session
 * re-renders on every pick change (a map point or a plot distance), and the
 * candidate list can hold hundreds of rows, each with five metrics and three
 * buttons. Its props — `rows`, `selectedActivityId`, `busy` and the three
 * callbacks, which `MatchSession` now keeps stable — do not change on a pick, so
 * the memo stops a hover from re-rendering the list at all. Selection, candidate
 * refreshes and busy transitions still re-render it because those props change.
 */
export default memo(CandidateTable);
