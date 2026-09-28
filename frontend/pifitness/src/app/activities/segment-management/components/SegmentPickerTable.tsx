'use client';

import { useViewportStore } from '@/stores/viewportStore';
import { SegmentListRow } from '@/lib/types/leaderboards';
import { formatIsoDate } from '@/lib/date-format';

/** Segment/course list body for Match Activities picker (009-003 T09, FR-7).
 *
 * Three layouts: desktop and landscape render a table (landscape scrolls it
 * horizontally), portrait renders stacked cards so the whole row is a thumb-
 * reachable target. Each row lists name, last-effort date, matched attempts,
 * distance, and elevation.
 */
interface SegmentPickerTableProps {
  rows: SegmentListRow[];
  onSelect: (segment: SegmentListRow) => void;
}

export default function SegmentPickerTable({ rows, onSelect }: SegmentPickerTableProps) {
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';

  if (isPortrait) {
    return (
      <ul className="space-y-2">
        {rows.map((row) => (
          <li key={row.segment_id}>
            <button
              type="button"
              onClick={() => onSelect(row)}
              aria-label={`Select segment ${row.segment_name}`}
              className="w-full text-left rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-3 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-semibold text-gray-900 dark:text-white truncate">
                  {row.segment_name}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
                  #{row.segment_id}
                </span>
              </div>
              <div className="mt-1 text-xs text-gray-600 dark:text-gray-400">
                                  {row.is_course ? 'Course' : 'Segment'} · {row.distance_mi.toFixed(2)} mi
                {row.elevation_gain != null ? ` · ${row.elevation_gain} m elev` : ''}
              </div>
              <div className="mt-1 text-xs text-gray-600 dark:text-gray-400">
                Last effort: {row.last_effort ? formatIsoDate(row.last_effort) : 'Never'} · Matched: {row.matched_activity_count}
              </div>
            </button>
          </li>
        ))}
      </ul>
    );
  }

  return (
    <div className="overflow-x-auto rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
      <table className="min-w-full text-sm">
        <caption className="sr-only">
          Segments and courses available for matching, with last effort date, matched count, distance, and elevation
        </caption>
        <thead className="bg-gray-50 dark:bg-gray-700/50">
          <tr>
            <th scope="col" className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-300">
              Name
            </th>
            <th scope="col" className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-300">
              Type
            </th>
            <th scope="col" className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-300">
              Last Effort
            </th>
            <th scope="col" className="px-4 py-2 text-right text-xs font-semibold text-gray-600 dark:text-gray-300">
              Distance
            </th>
            <th scope="col" className="px-4 py-2 text-right text-xs font-semibold text-gray-600 dark:text-gray-300">
              Elevation
            </th>
            <th scope="col" className="px-4 py-2 text-right text-xs font-semibold text-gray-600 dark:text-gray-300">
              Matched
            </th>
            <th scope="col" className="px-4 py-2 text-right text-xs font-semibold text-gray-600 dark:text-gray-300">
              <span className="sr-only">Select</span>
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
          {rows.map((row) => (
            <tr key={row.segment_id} className="hover:bg-gray-50 dark:hover:bg-gray-700/60">
              <td className="px-4 py-2 text-gray-900 dark:text-white font-medium truncate max-w-[180px]">
                {row.segment_name}
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300">
                {row.is_course ? 'Course' : 'Segment'}
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap">
                {row.last_effort ? formatIsoDate(row.last_effort) : '—'}
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums text-right">
                                {row.distance_mi.toFixed(2)} mi
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums text-right">
                {row.elevation_gain != null ? `${row.elevation_gain} m` : '—'}
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums text-right">
                {row.matched_activity_count}
              </td>
              <td className="px-4 py-2 text-right">
                <button
                  type="button"
                  onClick={() => onSelect(row)}
                  aria-label={`Select segment ${row.segment_name}`}
                  className="px-3 py-1.5 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  Select
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}