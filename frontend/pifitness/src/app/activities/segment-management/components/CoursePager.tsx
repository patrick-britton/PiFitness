'use client';

interface CoursePagerProps {
  page: number;
  totalPages: number;
  onPrior: () => void;
  onNext: () => void;
}

/** Wrap-around Prior/Next pager (legacy Prior/Next semantics, FR-15). */
export default function CoursePager({ page, totalPages, onPrior, onNext }: CoursePagerProps) {
  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        onClick={onPrior}
        disabled={totalPages <= 1}
        aria-label="Show prior course"
        className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Prior
      </button>
      <button
        type="button"
        onClick={onNext}
        disabled={totalPages <= 1}
        aria-label="Show next course"
        className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Next
      </button>
      <p className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
        {page} of {totalPages}
      </p>
    </div>
  );
}
