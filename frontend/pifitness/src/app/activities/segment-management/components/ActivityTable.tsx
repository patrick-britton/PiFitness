'use client';

import { useViewportStore } from '@/stores/viewportStore';
import { ActivitySummary } from '@/lib/types/segment-management';
import { formatIsoDate, formatMiles } from '@/lib/date-format';

/**
 * Source-activity list body for Create Segment mode (009-003 T08, AC-1).
 *
 * Three layouts: desktop and landscape render a table (landscape scrolls it
 * horizontally and keeps the chrome minimal), portrait renders stacked cards so
 * the whole row is a thumb-reachable target. Each row lists the activity type,
 * start time, distance, child segment/course count, and matched-effort count.
 */

interface ActivityTableProps {
  rows: ActivitySummary[];
  onSelect: (activity: ActivitySummary) => void;
}

const HEADERS = ['ID#', 'Type', 'Start', 'Distance', 'Child Segments/Courses', 'Matched Efforts'];

export default function ActivityTable({ rows, onSelect }: ActivityTableProps) {
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';

  if (isPortrait) {
    return (
      <ul className="space-y-2">
        {rows.map((row) => (
          <li key={row.activity_id}>
            <button
              type="button"
              onClick={() => onSelect(row)}
              aria-label={`Select activity ${row.activity_id} from ${formatIsoDate(row.start_time_utc)}`}
              className="w-full text-left rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-3 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-semibold text-gray-900 dark:text-white">
                  {row.activity_type_name}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
                  #{row.activity_id}
                </span>
              </div>
              <div className="mt-1 text-xs text-gray-600 dark:text-gray-400">
                {formatIsoDate(row.start_time_utc)} · {formatMiles(row.distance_m)} mi
              </div>
              <div className="mt-1 text-xs text-gray-600 dark:text-gray-400">
                Child segments/courses: {row.child_segment_count} · Matched efforts: {row.matched_count}
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
          Activities available as a segment source, with child segment/course and matched-effort counts
        </caption>
        <thead className="bg-gray-50 dark:bg-gray-700/50">
          <tr>
            {HEADERS.map((h) => (
              <th
                key={h}
                scope="col"
                className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-300 whitespace-nowrap"
              >
                {h}
              </th>
            ))}
            <th scope="col" className="px-4 py-2 text-right text-xs font-semibold text-gray-600 dark:text-gray-300">
              <span className="sr-only">Select</span>
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
          {rows.map((row) => (
            <tr key={row.activity_id} className="hover:bg-gray-50 dark:hover:bg-gray-700/60">
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums">{row.activity_id}</td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap">{row.activity_type_name}</td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap">
                {formatIsoDate(row.start_time_utc)}
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums whitespace-nowrap">
                {formatMiles(row.distance_m)} mi
              </td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums">{row.child_segment_count}</td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300 tabular-nums">{row.matched_count}</td>
              <td className="px-4 py-2 text-right">
                <button
                  type="button"
                  onClick={() => onSelect(row)}
                  aria-label={`Select activity ${row.activity_id}`}
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