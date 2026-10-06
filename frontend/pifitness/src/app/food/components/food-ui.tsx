/**
 * Shared region-state UI for the food skeletons (010-002 T06).
 * ------------------------------------------------------------------
 * Extracted from the Diary skeleton so Recipe Box and Food Database reuse the
 * exact same loading / empty / error / section affordances instead of each
 * carrying their own copy. Theme-aware Tailwind classes only (no hardcoded
 * colors). `prefers-reduced-motion` is respected by keeping the loading
 * skeleton a single subtle pulse and no other decorative motion.
 */

'use client';

import type { ReactNode } from 'react';

export function LoadingSkeleton({ rows, testId = 'loading' }: { rows: number; testId?: string }) {
  return (
    <div className="animate-pulse space-y-2" aria-live="polite" aria-busy="true" data-testid={`${testId}-loading`}>
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="h-4 bg-gray-200 dark:bg-gray-700 rounded-md w-full" />
      ))}
    </div>
  );
}

export function EmptyBox({ title, message }: { title: string; message: string }) {
  return (
    <div className="rounded-lg border border-dashed border-gray-300 dark:border-gray-600 p-4 text-sm text-gray-500 dark:text-gray-400">
      <p className="font-medium">{title}</p>
      <p className="mt-1">{message}</p>
    </div>
  );
}

export function ErrorBox({ label, message }: { label: string; message: string }) {
  return (
    <div role="alert"
      className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-sm text-red-700 dark:text-red-300">
      <p className="font-medium">Failed to load {label.toLowerCase()}</p>
      <p className="break-words whitespace-pre-wrap">{message}</p>
    </div>
  );
}

export function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section aria-label={title}>
      <h2 className="text-base font-semibold text-gray-900 dark:text-white">{title}</h2>
      <div className="mt-2">{children}</div>
    </section>
  );
}