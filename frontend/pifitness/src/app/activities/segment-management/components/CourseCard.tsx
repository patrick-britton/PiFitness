'use client';

import { CourseInfo } from '@/lib/types/segment-management';
import { formatIsoDate } from '@/lib/date-format';

/** Course metadata header (FR-15: name, last run, distance, attempts). */
export default function CourseCard({ course }: { course: CourseInfo }) {
  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
      <p className="text-sm font-semibold text-gray-900 dark:text-white">
        {course.course_name}
        {course.is_new && (
          <span className="ml-2 rounded-full bg-blue-100 dark:bg-blue-900/40 px-2 py-0.5 text-xs font-medium text-blue-700 dark:text-blue-300">
            New
          </span>
        )}
      </p>
      <p className="mt-1 text-xs text-gray-600 dark:text-gray-400">
        Last run: {course.last_run ? formatIsoDate(course.last_run) : 'Never'}
      </p>
      <p className="text-xs text-gray-600 dark:text-gray-400 tabular-nums">
        Dist/Attempts: {course.distance_mi.toFixed(1)} mi | {course.attempt_count}
      </p>
    </div>
  );
}
