'use client';

import { useState } from 'react';
import { API } from '@/lib/api-client';
import ConfirmDialog from './ConfirmDialog';

/**
 * Destructive match-table reset (009-003 T11, FR-14; OQ-2 confirmation dialog).
 *
 * Deliberate activation is the dialog itself: the closed control is a single
 * red-outlined button, and the POST only fires from inside the open dialog's
 * Confirm. After a successful reset the course list is reloaded from page 1
 * (identity restart recycles ids, so any held page is stale) and the caller
 * is told to refresh dependent views via `onReset`.
 */

interface ResetMatchTablesProps {
  /** Reload dependent views after a successful reset. */
  onReset: () => void;
}

export default function ResetMatchTables({ onReset }: ResetMatchTablesProps) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const reset = async () => {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await API.segments.resetMatchTables({ confirm_reset: true });
      setMessage(res.message || 'Match tables reset.');
      setOpen(false);
      onReset();
    } catch {
      setError('Failed to reset the match tables.');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-2">
      <button
        type="button"
        onClick={() => {
          setError(null);
          setOpen(true);
        }}
        aria-label="Reset match tables"
        className="px-3 py-2 text-sm font-medium rounded-md border border-red-300 dark:border-red-700 text-red-700 dark:text-red-300 hover:bg-red-50 dark:hover:bg-red-900/20 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Reset match tables…
      </button>
      {message && (
        <p role="status" className="text-sm text-green-700 dark:text-green-400">
          {message}
        </p>
      )}
      {open && (
        <ConfirmDialog
          title="Reset match tables?"
          message="This truncates the matching data (segment matches, exclusions, distinct segments, segment details) and resets the segments table. This cannot be undone."
          confirmLabel="Reset match tables"
          busyLabel="Resetting…"
          busy={busy}
          error={error}
          onCancel={() => {
            if (!busy) setOpen(false);
          }}
          onConfirm={reset}
        />
      )}
    </div>
  );
}
