'use client';

interface CourseFiltersProps {
  nameFilter: string;
  minDistance: string;
  maxDistance: string;
  onNameChange: (v: string) => void;
  onMinChange: (v: string) => void;
  onMaxChange: (v: string) => void;
  onClear: () => void;
}

/** Name + length-range filters (FR-15, legacy render_course_list semantics). */
export default function CourseFilters({
  nameFilter,
  minDistance,
  maxDistance,
  onNameChange,
  onMinChange,
  onMaxChange,
  onClear,
}: CourseFiltersProps) {
  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3 space-y-3">
      <div>
        <label
          htmlFor="course-name-filter"
          className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
        >
          Name search
        </label>
        <input
          id="course-name-filter"
          type="text"
          value={nameFilter}
          onChange={(e) => onNameChange(e.target.value)}
          placeholder="Search by name…"
          className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label
            htmlFor="course-length-min"
            className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
          >
            Length min (mi)
          </label>
          <input
            id="course-length-min"
            type="number"
            min={0}
            step="any"
            value={minDistance}
            onChange={(e) => onMinChange(e.target.value)}
            placeholder="0"
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label
            htmlFor="course-length-max"
            className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
          >
            Length max (mi)
          </label>
          <input
            id="course-length-max"
            type="number"
            min={0}
            step="any"
            value={maxDistance}
            onChange={(e) => onMaxChange(e.target.value)}
            placeholder="Any"
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>
      <button
        type="button"
        onClick={onClear}
        className="px-3 py-1.5 text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Clear filters
      </button>
    </div>
  );
}
