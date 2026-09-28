'use client';

import { useEffect, useId } from 'react';

/**
 * Confirmation dialog for destructive actions (009-003 T09 delete segment;
 * reused by T11's match-table reset per OQ-2).
 *
 * Dependency-free modal following the module's established dialog pattern
 * (`exercises/TimerCreation`): a fixed overlay with `role="dialog"`,
 * `aria-modal`, and a labelled heading, Escape to cancel, and explicit
 * Cancel/Confirm buttons. Deliberately NOT a native `window.confirm`: the
 * confirm step has to be testable, styled by the theme, and readable on a
 * phone.
 *
 * Limitation (documented, not hidden): focus is not trapped inside the dialog
 * — with only two controls and Escape handling that stays usable by keyboard,
 * and it avoids pulling in a focus-trap dependency for two buttons.
 */

interface ConfirmDialogProps {
  /** Dialog heading, also the accessible label. */
  title: string;
  /** What the action does, stated plainly (what is destroyed, what cannot be undone). */
  message: string;
  /** Confirm button label when idle. */
  confirmLabel: string;
  /** Confirm button label while the action is in flight. */
  busyLabel?: string;
  /** True while the action is running (buttons disabled). */
  busy?: boolean;
  /** Action failure, shown inside the dialog. */
  error?: string | null;
  /** Dismiss without acting. */
  onCancel: () => void;
  /** Run the action. */
  onConfirm: () => void;
}

export default function ConfirmDialog({
  title,
  message,
  confirmLabel,
  busyLabel = 'Working…',
  busy = false,
  error = null,
  onCancel,
  onConfirm,
}: ConfirmDialogProps) {
  const titleId = useId();

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !busy) onCancel();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [busy, onCancel]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50"
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
    >
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg max-w-md w-full p-6">
        <h2 id={titleId} className="text-lg font-semibold text-gray-900 dark:text-white">
          {title}
        </h2>
        <p className="text-sm text-gray-600 dark:text-gray-300 mt-2">{message}</p>

        {error && (
          <div className="mt-4 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
            <p role="alert">{error}</p>
          </div>
        )}

        <div className="mt-6 flex justify-end gap-3">
          <button
            type="button"
            onClick={onCancel}
            disabled={busy}
            className="inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700/50 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={busy}
            className="inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {busy ? busyLabel : confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}