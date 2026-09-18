/**
 * IsrcDupeHomeBanner — Home page pending-duplicates banner (008-005, T06).
 *
 * Client component: reads the shared pair count via useIsrcDupeCount and
 * renders nothing when the queue is empty (FR-7/FR-9: banner only when N > 0).
 */

'use client';

import { useIsrcDupeCount } from '@/hooks/useMusic';

export default function IsrcDupeHomeBanner() {
  const { data } = useIsrcDupeCount();
  const count = data?.count ?? 0;

  if (count <= 0) return null;

  return (
    <div className="rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 px-4 py-3">
      <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
        {count} potential duplicate isrcs found
      </p>
    </div>
  );
}
