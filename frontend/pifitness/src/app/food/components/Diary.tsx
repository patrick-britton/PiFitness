/**
 * Food Diary (010-003 T05).
 *
 * Diary-home lands the module on today: a three-bar calorie header (today /
 * trailing 24h / 7-day average, scaled to the largest of the three), day
 * arrows, and the day's entries newest-first as name+kcal cards. Tapping a
 * card opens an edit popup (qty, FDA-label unit, calendar+clock datetime,
 * Delete) with a live calorie figure. The plus-circle Add button swaps the
 * diary for add-entry mode until the red cancel returns to it.
 *
 * Writes round-trip to the backend (PATCH/DELETE; device-local `day`). Delete
 * follows the evergreen undo-vs-confirm rule: the entry vanishes at once
 * behind an Undo banner and only commits after the undo window elapses. Three
 * layouts via layoutVariant; dual theme via Tailwind dark: classes.
 */

'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import { previewKcal } from '../../../lib/food-units';
import type { DayEntry, DiaryStats } from '../../../lib/types/food-contract';
import { EmptyBox, ErrorBox, LoadingSkeleton } from './food-ui';
import AddEntry from './AddEntry';
import AmountPicker, { type AmountUnit } from './AmountPicker';

/** How long a removed entry stays undoable before the delete write commits. */
export const UNDO_WINDOW_MS = 5000;

type AsyncRegion<T> =
  | { status: 'loading' }
  | { status: 'ready'; items: T }
  | { status: 'error'; error: string };

function errMsg(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

/** Local calendar day as YYYY-MM-DD (matches the diary `day` contract param). */
function todayISO(): string {
  const d = new Date();
  const local = new Date(d.getTime() - d.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 10);
}

/** Shift a YYYY-MM-DD string by whole days without timezone drift. */
function shiftDay(iso: string, delta: number): string {
  const [y, m, d] = iso.split('-').map(Number);
  const base = new Date(y, m - 1, d);
  base.setDate(base.getDate() + delta);
  const local = new Date(base.getTime() - base.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 10);
}

/** Human day label; the current device-local day reads "Today". */
function dayLabel(iso: string): string {
  if (iso === todayISO()) return 'Today';
  const [y, m, d] = iso.split('-').map(Number);
  return new Date(y, m - 1, d).toLocaleDateString(undefined, {
    weekday: 'short', month: 'short', day: 'numeric',
  });
}

/** ISO instant -> value for a `datetime-local` input (local wall clock). */
function toLocalInput(iso: string): string {
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return '';
  const local = new Date(dt.getTime() - dt.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

/** `datetime-local` value -> ISO instant (empty -> undefined = unchanged). */
function fromLocalInput(value: string): string | undefined {
  return value ? new Date(value).toISOString() : undefined;
}

/** Three horizontal bars scaled to the largest of the three figures. */
function StatBars({ stats }: { stats: DiaryStats }) {
  const rows = [
    { label: 'Today', value: stats.today_kcal },
    { label: 'Last 24h', value: stats.last_24h_kcal },
    { label: '7-day avg', value: stats.avg_7d_kcal },
  ];
  const max = stats.scale_max ?? Math.max(0, ...rows.map((r) => r.value ?? 0));
  return (
    <div className="space-y-2" aria-label="Calorie bars">
      {rows.map((r) => {
        const pct = max > 0 && r.value != null ? Math.min(100, (r.value / max) * 100) : 0;
        const shown = r.value != null ? Math.round(r.value) : 0;
        return (
          <div key={r.label}>
            <div className="flex items-baseline justify-between text-xs text-gray-600 dark:text-gray-300">
              <span>{r.label}</span>
              <span className="font-medium text-gray-900 dark:text-white">
                {r.value != null ? `${shown} kcal` : '—'}
              </span>
            </div>
            <div className="mt-1 h-2 overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700"
              role="img" aria-label={`${r.label}: ${shown} kcal`}>
              <div className="h-full rounded-full bg-blue-600 dark:bg-blue-400" style={{ width: `${pct}%` }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

/** Edit an existing entry: qty, unit, calendar+clock datetime, Delete. */
function EditPopup({
  entry, onSaved, onDelete, onCancel,
}: {
  entry: DayEntry;
  onSaved: () => void;
  onDelete: (entry: DayEntry) => void;
  onCancel: () => void;
}) {
  const [qty, setQty] = useState(entry.qty);
  const [unit, setUnit] = useState<AmountUnit>((entry.unit as AmountUnit) ?? 'g');
  const [when, setWhen] = useState(toLocalInput(entry.logged_at));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Live kcal: FDA table first; a `serving` entry scales by its own ratio.
  const preview =
    previewKcal(entry.nutrients_snapshot.kcal_per_100g, qty, unit) ??
    (entry.unit === unit && entry.kcal != null && entry.qty > 0
      ? (entry.kcal * qty) / entry.qty
      : null);

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await API.food.updateDiaryEntry(entry.entry_id, {
        qty, unit, logged_at: fromLocalInput(when),
      });
      onSaved();
    } catch (e) {
      setError(errMsg(e));
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/40 p-4 sm:items-center"
      role="presentation">
      <div role="dialog" aria-modal="true" aria-label={`Edit ${entry.name_snapshot}`}
        className="w-full max-w-md rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 shadow-xl"
        onClick={(e) => e.stopPropagation()}>
        <h2 className="text-base font-semibold text-gray-900 dark:text-white">{entry.name_snapshot}</h2>

        <div className="mt-3">
          <AmountPicker key={entry.entry_id}
            value={{ qty: entry.qty, unit: (entry.unit as AmountUnit) ?? 'g' }}
            extraUnit={entry.unit ?? null}
            onChange={(a) => { setQty(a.qty); setUnit(a.unit); }} />
        </div>

        <label htmlFor="edit-when" className="mt-3 block text-xs font-medium text-gray-600 dark:text-gray-300">
          Date &amp; time
        </label>
        <input id="edit-when" type="datetime-local" value={when}
          onChange={(e) => setWhen(e.target.value)}
          className="mt-1 w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white" />

        <p role="status" className="mt-3 text-sm text-gray-700 dark:text-gray-300">
          {preview != null ? `≈ ${Math.round(preview)} kcal` : 'kcal unavailable'}
        </p>
        {error && <p role="alert" className="mt-2 text-sm text-red-700 dark:text-red-300">{error}</p>}

        <div className="mt-4 flex items-center justify-between gap-2">
          <button type="button" onClick={() => onDelete(entry)}
            className="rounded-md border border-red-300 dark:border-red-700 px-3 py-2 min-h-[44px] text-sm text-red-700 dark:text-red-300">
            Delete
          </button>
          <div className="flex items-center gap-2">
            <button type="button" onClick={onCancel}
              className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
              Cancel
            </button>
            <button type="button" onClick={() => void save()} disabled={saving}
              className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-60">
              {saving ? 'Saving…' : 'Save'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function Diary({ undoWindowMs = UNDO_WINDOW_MS }: { undoWindowMs?: number }) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const [day, setDay] = useState<string>(todayISO());
  const [mode, setMode] = useState<'diary' | 'add'>('diary');
  const [stats, setStats] = useState<AsyncRegion<DiaryStats>>({ status: 'loading' });
  const [entries, setEntries] = useState<AsyncRegion<DayEntry[]>>({ status: 'loading' });
  const [edit, setEdit] = useState<DayEntry | null>(null);
  const [pendingDelete, setPendingDelete] = useState<DayEntry | null>(null);

  const dayRef = useRef(day);
  const pendingRef = useRef<DayEntry | null>(null);
  const undoTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const loadStats = useCallback(async (d: string, silent = false) => {
    if (!silent) setStats({ status: 'loading' });
    try {
      setStats({ status: 'ready', items: await API.food.diaryStats(d) });
    } catch (error) {
      setStats({ status: 'error', error: errMsg(error) });
    }
  }, []);

  const loadEntries = useCallback(async (d: string, silent = false) => {
    if (!silent) setEntries({ status: 'loading' });
    try {
      setEntries({ status: 'ready', items: await API.food.diary(d) });
    } catch (error) {
      setEntries({ status: 'error', error: errMsg(error) });
    }
  }, []);

  const refresh = useCallback(() => {
    const d = dayRef.current;
    void loadStats(d, true);
    void loadEntries(d, true);
  }, [loadStats, loadEntries]);

  useEffect(() => {
    dayRef.current = day;
    void loadStats(day);
    void loadEntries(day);
  }, [day, loadStats, loadEntries]);

  const clearUndo = useCallback(() => {
    if (undoTimer.current) { clearTimeout(undoTimer.current); undoTimer.current = null; }
  }, []);

  const commitPending = useCallback(() => {
    clearUndo();
    const p = pendingRef.current;
    pendingRef.current = null;
    setPendingDelete(null);
    if (p) void API.food.deleteDiaryEntry(p.entry_id).catch(() => {}).finally(refresh);
  }, [clearUndo, refresh]);

  // Commit (never lose) a pending delete if the tab unmounts mid-window.
  useEffect(() => () => {
    const p = pendingRef.current;
    if (p) {
      if (undoTimer.current) clearTimeout(undoTimer.current);
      void API.food.deleteDiaryEntry(p.entry_id).catch(() => {});
    }
  }, []);

  const scheduleDelete = useCallback((entry: DayEntry) => {
    if (pendingRef.current) commitPending();
    setEdit(null);
    pendingRef.current = entry;
    setPendingDelete(entry);
    undoTimer.current = setTimeout(commitPending, undoWindowMs);
  }, [commitPending, undoWindowMs]);

  const undoDelete = useCallback(() => {
    clearUndo();
    pendingRef.current = null;
    setPendingDelete(null);
  }, [clearUndo]);

  const visible = entries.status === 'ready'
    ? entries.items.filter((e) => e.entry_id !== pendingDelete?.entry_id)
    : [];

  const renderStats = () => {
    if (stats.status === 'loading') return <LoadingSkeleton rows={3} testId="diary-stats" />;
    if (stats.status === 'error') return <ErrorBox label="Calorie summary" message={stats.error} />;
    return <StatBars stats={stats.items} />;
  };

  const renderEntries = () => {
    if (entries.status === 'loading') return <LoadingSkeleton rows={4} testId="diary-entries" />;
    if (entries.status === 'error') return <ErrorBox label="Diary entries" message={entries.error} />;
    if (visible.length === 0) return <EmptyBox title="Diary entries" message="No foods logged for this day" />;
    return (
      <ul className="space-y-1.5" aria-label="Diary entries">
        {visible.map((e) => (
          <li key={e.entry_id}>
            <button type="button" onClick={() => setEdit(e)}
              className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-700/50">
              <span className="text-gray-900 dark:text-white">{e.name_snapshot}</span>
              <span className="text-xs text-gray-500 dark:text-gray-400">
                {e.kcal != null ? `${Math.round(e.kcal)} kcal` : 'kcal n/a'}
              </span>
            </button>
          </li>
        ))}
      </ul>
    );
  };

  const header = (
    <div className="flex items-center justify-between gap-2">
      <button type="button" aria-label="Previous day" onClick={() => setDay(shiftDay(day, -1))}
        className="flex h-11 w-11 items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-lg text-gray-700 dark:text-gray-300">
        <span aria-hidden>‹</span>
      </button>
      <span aria-live="polite" className="text-sm font-medium text-gray-900 dark:text-white">{dayLabel(day)}</span>
      <button type="button" aria-label="Next day" disabled={day >= todayISO()}
        onClick={() => setDay(shiftDay(day, 1))}
        className="flex h-11 w-11 items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-lg text-gray-700 dark:text-gray-300 disabled:opacity-40">
        <span aria-hidden>›</span>
      </button>
      <button type="button" aria-label="Add entry" onClick={() => setMode('add')}
        className="flex h-11 w-11 items-center justify-center rounded-full bg-blue-600 hover:bg-blue-700 text-white">
        <span aria-hidden className="text-2xl leading-none">+</span>
      </button>
    </div>
  );

  if (mode === 'add') {
    return <AddEntry onDone={() => { setMode('diary'); refresh(); }} />;
  }

  const split = isDesktop || isLandscape;
  return (
    <div className="space-y-4">
      {header}
      <div className={split ? 'grid grid-cols-2 gap-4' : 'space-y-4'}>
        <section aria-label="Calorie summary"
          className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-3">
          {renderStats()}
        </section>
        <section aria-label="Diary entries"
          className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-3">
          {renderEntries()}
        </section>
      </div>

      {pendingDelete && (
        <div role="status"
          className="flex items-center justify-between rounded-md bg-amber-50 dark:bg-amber-900/20 px-3 py-2 text-sm text-amber-800 dark:text-amber-200">
          <span>1 entry removed</span>
          <button type="button" onClick={undoDelete} className="font-medium underline">Undo</button>
        </div>
      )}

      {edit && (
        <EditPopup entry={edit}
          onSaved={() => { setEdit(null); refresh(); }}
          onDelete={scheduleDelete}
          onCancel={() => setEdit(null)} />
      )}
    </div>
  );
}