/**
 * PlaylistShuffleView — Music module: Playlist Config tab content (008-003)
 *
 * Selection grid of configured parent playlists. Selecting exactly one row
 * reveals the four tuning inputs (weights + minutes) defaulted to that
 * playlist's saved config. The live preview and send flow are added by T06/T07.
 *
 * Reads:
 *   - GET /api/music/shuffle/playlists (selection grid, FR-1)
 *   - GET /api/music/shuffle?playlist_id= (saved config, FR-2)
 */

'use client';

import { useEffect, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import {
  ShufflePlaylistRow,
  ShuffleConfig,
  ShuffleFlagsBody,
  ShufflePreviewRow,
  PlaylistTypeOption,
} from '@/lib/types/music';

const PLAYLIST_TYPE_OPTIONS: PlaylistTypeOption[] = ['Parents', 'Seeds', 'Other'];
const DEFAULT_PLAYLIST_TYPE: PlaylistTypeOption = 'Parents';
const PLAYLIST_HEADER_SYNC_TASK = 'Playlist Header Sync';

/** Compute the max track count for bar scaling. */
function maxTrackCount(rows: ShufflePlaylistRow[]): number {
  return rows.reduce((max, r) => Math.max(max, r.track_count ?? 0), 0);
}

function PlaylistRow({
  row,
  maxCount,
  selected,
  onSelect,
}: {
  row: ShufflePlaylistRow;
  maxCount: number;
  selected: boolean;
  onSelect: (playlistId: string) => void;
}) {
  const pct = maxCount > 0 ? ((row.track_count ?? 0) / maxCount) * 100 : 0;
  return (
    <button
      type="button"
      onClick={() => onSelect(row.playlist_id)}
      aria-pressed={selected}
      className={`text-left rounded-lg border p-3 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
        selected
          ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20 dark:border-blue-400'
          : 'border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 hover:border-gray-300 dark:hover:border-gray-600'
      }`}
    >
      <p className="font-medium text-gray-900 dark:text-white truncate">
        {row.playlist_name}
      </p>
      <div
        className="mt-2 h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden"
        role="progressbar"
        aria-valuenow={row.track_count ?? 0}
        aria-valuemin={0}
        aria-valuemax={maxCount}
      >
        <div
          className="h-full rounded-full bg-blue-500 dark:bg-blue-400"
          style={{ width: `${pct}%` }}
        />
      </div>
      <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
        {row.track_count ?? 0} tracks
      </p>
      {row.playlist_type && (
        <span className="mt-2 inline-block rounded-full bg-gray-100 dark:bg-gray-700 px-2 py-0.5 text-xs font-medium text-gray-600 dark:text-gray-300">
          {row.playlist_type}
        </span>
      )}
    </button>
  );
}

function NumberInput({
  label,
  value,
  min,
  max,
  hint,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  hint?: string;
  onChange: (value: number) => void;
}) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
        {label}
      </label>
      <input
        type="number"
        value={value}
        min={min}
        max={max}
        onChange={(e) => {
          const v = parseInt(e.target.value, 10);
          if (!isNaN(v)) onChange(v);
        }}
        className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
      />
      {hint && (
        <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">{hint}</p>
      )}
    </div>
  );
}

function CheckboxField({
  label,
  checked,
  disabled,
  onChange,
}: {
  label: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label
      className={`inline-flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-300 ${
        disabled ? 'opacity-60 cursor-not-allowed' : 'cursor-pointer'
      }`}
    >
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
        className="h-4 w-4 rounded border-gray-300 dark:border-gray-600 accent-blue-600"
      />
      {label}
    </label>
  );
}


export default function PlaylistShuffleView() {
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';
  const queryClient = useQueryClient();
  const [selectedPlaylistId, setSelectedPlaylistId] = useState<string | null>(null);
  const [localConfig, setLocalConfig] = useState<ShuffleConfig | null>(null);
  const [typeFilter, setTypeFilter] = useState<PlaylistTypeOption>(DEFAULT_PLAYLIST_TYPE);
  const playlistsQuery = useQuery({ queryKey: ['music', 'shuffle', 'playlists'], queryFn: () => API.music.getShufflePlaylists() });
  const shuffleDataQuery = useQuery({ queryKey: ['music', 'shuffle', 'data', selectedPlaylistId], queryFn: () => API.music.getShuffleData(selectedPlaylistId!), enabled: !!selectedPlaylistId });
  useEffect(() => { if (shuffleDataQuery.data?.config) { setLocalConfig(shuffleDataQuery.data.config); } }, [shuffleDataQuery.data]);
  const [previewRows, setPreviewRows] = useState<ShufflePreviewRow[]>([]);
  const [previewTargetId, setPreviewTargetId] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const reconcileMutation = useMutation({ mutationFn: (cfg: ShuffleConfig) => API.music.reconcileShuffleConfig(selectedPlaylistId!, cfg), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['music', 'shuffle', 'data', selectedPlaylistId] }); } });
  const flagsMutation = useMutation({
    mutationFn: (flags: ShuffleFlagsBody) => API.music.reconcileShuffleFlags(selectedPlaylistId!, flags),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['music', 'shuffle', 'data', selectedPlaylistId] });
    },
  });
  const sendMutation = useMutation({
    mutationFn: () => API.music.sendShuffle({ playlistId: selectedPlaylistId!, ratingsWeight: localConfig!.ratingsWeight, recencyWeight: localConfig!.recencyWeight, randomWeight: localConfig!.randomWeight, minutesToSync: localConfig!.minutesToSync }),
    onSuccess: () => {
      const sentCount = previewRows.length;
      setSelectedPlaylistId(null);
      setLocalConfig(null);
      setPreviewRows([]);
      setPreviewTargetId(null);
      setTypeFilter(DEFAULT_PLAYLIST_TYPE);
      setSuccessMessage(`${sentCount} songs sent to Spotify`);
    },
  });
  const handleSend = () => { if (!selectedPlaylistId || !localConfig || previewRows.length === 0) return; sendMutation.mutate(); };
  // Decaying success message (~3.5s)
  useEffect(() => {
    if (!successMessage) return;
    const t = setTimeout(() => setSuccessMessage(null), 3500);
    return () => clearTimeout(t);
  }, [successMessage]);
// --- Sync with Spotify (Playlist Header Sync force-run, FR from 008-002) ---
  const [syncExecutionId, setSyncExecutionId] = useState<number | null>(null);
  const [syncRunning, setSyncRunning] = useState(false);
  const [syncStatusMessage, setSyncStatusMessage] = useState<string | null>(null);
  const [syncFailed, setSyncFailed] = useState(false);
  const [syncElapsedSec, setSyncElapsedSec] = useState(0);

  const executeSyncMutation = useMutation({
    mutationFn: () => API.admin.executeTaskV2(PLAYLIST_HEADER_SYNC_TASK),
    onMutate: () => {
      setSyncRunning(true);
      setSyncFailed(false);
      setSyncStatusMessage(null);
      setSyncElapsedSec(0);
      setSyncExecutionId(null);
    },
    onSuccess: (data) => {
      const executionId = (data as any).execution_id;
      if (executionId) {
        setSyncExecutionId(executionId);
      } else {
        // Non-async fallback: task completed synchronously
        setSyncRunning(false);
        setSyncFailed(false);
        setSyncStatusMessage(`Sync completed`);
        setTimeout(() => setSyncStatusMessage(null), 5000);
        queryClient.invalidateQueries({ queryKey: ['music', 'shuffle', 'playlists'] });
      }
    },
    onError: (err) => {
      setSyncRunning(false);
      setSyncFailed(true);
      setSyncStatusMessage(`Sync failed: ${err}`);
      setTimeout(() => setSyncStatusMessage(null), 6000);
    },
  });

  // Poll async execution status while the task runs
  const syncStatusQuery = useQuery({
    queryKey: ['music', 'shuffle', 'sync-status', syncExecutionId],
    queryFn: () => API.admin.getTaskExecutionStatus(syncExecutionId as number),
    enabled: !!syncExecutionId,
    refetchInterval: (query) => {
      if (!syncExecutionId) return false;
      const data = query.state.data;
      if (data && (data.status === 'success' || data.status === 'failed')) return false;
      return 1500;
    },
    retry: false,
  });

  useEffect(() => {
    const s = syncStatusQuery.data;
    if (!s) return;
    if (s.status === 'success') {
      setSyncRunning(false);
      setSyncExecutionId(null);
      setSyncFailed(false);
      setSyncStatusMessage('Sync with Spotify completed successfully');
      queryClient.invalidateQueries({ queryKey: ['music', 'shuffle', 'playlists'] });
      setTimeout(() => setSyncStatusMessage(null), 5000);
    } else if (s.status === 'failed') {
      setSyncRunning(false);
      setSyncExecutionId(null);
      setSyncFailed(true);
      setSyncStatusMessage(`Sync failed: ${(s as any).error_message || 'Task execution failed'}`);
      setTimeout(() => setSyncStatusMessage(null), 6000);
    } else if (s.status === 'running') {
      setSyncRunning(true);
    }
  }, [syncStatusQuery.data, queryClient]);

  // Tick elapsed time every second while a sync is running
  useEffect(() => {
    if (!syncRunning) return;
    const t = setInterval(() => setSyncElapsedSec((prev) => prev + 1), 1000);
    return () => clearInterval(t);
  }, [syncRunning]);

  const handleSync = () => {
    if (syncRunning) return;
    executeSyncMutation.mutate();
  };
// Debounced preview + reconcile on any tuning-input change (~200ms)
  useEffect(() => {
    if (!selectedPlaylistId || !localConfig) {
      setPreviewRows([]);
      setPreviewTargetId(null);
      return;
    }
    let cancelled = false;
    setPreviewLoading(true);
    const handle = setTimeout(async () => {
      try {
        const [previewRes] = await Promise.all([
          API.music.getShufflePreview(selectedPlaylistId, localConfig),
          API.music.reconcileShuffleConfig(selectedPlaylistId, localConfig),
        ]);
        if (cancelled) return;
        setPreviewRows(previewRes.rows);
        setPreviewTargetId(previewRes.rows[0]?.targetPlaylistId ?? null);
      } catch {
        if (!cancelled) {
          setPreviewRows([]);
          setPreviewTargetId(null);
        }
      } finally {
        if (!cancelled) setPreviewLoading(false);
      }
    }, 200);
    return () => {
      cancelled = true;
      clearTimeout(handle);
    };
  }, [selectedPlaylistId, localConfig]);

  const handleSelectRow = (playlistId: string) => {
    setSelectedPlaylistId((prev) => {
      const next = prev === playlistId ? null : playlistId;
      if (next !== prev) {
        setLocalConfig(null);
        setPreviewRows([]);
        setPreviewTargetId(null);
      }
      return next;
    });
  };

  const handleTypeFilterChange = (option: PlaylistTypeOption) => {
    setTypeFilter(option);
    setSelectedPlaylistId(null);
    setLocalConfig(null);
    setPreviewRows([]);
    setPreviewTargetId(null);
  };

  const handleConfigChange = (field: keyof ShuffleConfig, value: number) => {
    setLocalConfig((prev) => (prev ? { ...prev, [field]: value } : prev));
  };

  const handleFlagChange = (field: keyof ShuffleFlagsBody, value: boolean) => {
    if (!selectedPlaylistId) return;
    // Optimistically update the local config so the checkbox reflects instantly;
    // persist in the background and reconcile from the server on success.
    setLocalConfig((prev) => (prev ? { ...prev, [field]: value } : prev));
    const flags: ShuffleFlagsBody = {
      autoShuffle: field === 'autoShuffle' ? value : (localConfig?.autoShuffle ?? false),
      manualShuffle: field === 'manualShuffle' ? value : (localConfig?.manualShuffle ?? false),
      makeRecs: field === 'makeRecs' ? value : (localConfig?.makeRecs ?? false),
      seedsOnly: field === 'seedsOnly' ? value : (localConfig?.seedsOnly ?? false),
    };
    flagsMutation.mutate(flags);
  };
const allRows = playlistsQuery.data?.data ?? [];
  const rows = allRows.filter((r) => (r.playlist_type ?? 'Parents') === typeFilter);
  const maxCount = maxTrackCount(rows);

  if (playlistsQuery.isLoading) {
    return (
      <div className="p-8 text-center">
        <p className="text-gray-500 dark:text-gray-400">Loading playlists…</p>
      </div>
    );
  }

  if (playlistsQuery.isSuccess && allRows.length === 0) {
    return (
      <div className="p-8 text-center">
        <p className="text-gray-500 dark:text-gray-400">Sync playlists to configure</p>
      </div>
    );
  }

  const previewEmpty = !previewLoading && previewRows.length === 0 && !!selectedPlaylistId;

  return (
    <div className="space-y-6">
      {successMessage && (
        <div className="rounded-md border border-emerald-300 dark:border-emerald-700 bg-emerald-50 dark:bg-emerald-900/20 px-4 py-3 text-sm font-medium text-emerald-700 dark:text-emerald-300 transition-opacity duration-500">
          {successMessage}
        </div>
      )}
      {/* Type filter tabs + Sync with Spotify */}
      <section aria-label="Playlist type filter">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex gap-2">
            {PLAYLIST_TYPE_OPTIONS.map((option) => (
              <button
                key={option}
                type="button"
                onClick={() => handleTypeFilterChange(option)}
                aria-pressed={typeFilter === option}
                className={`rounded-md px-4 py-2 text-sm font-medium transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
                  typeFilter === option
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-200 dark:hover:bg-gray-600'
                }`}
              >
                {option}
              </button>
            ))}
          </div>

          {syncStatusMessage && (
            <span
              className={`inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium ${
                syncFailed
                  ? 'bg-red-50 text-red-700 dark:bg-red-900/20 dark:text-red-300'
                  : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/20 dark:text-emerald-300'
              }`}
            >
              {syncStatusMessage}
            </span>
          )}

          <button
            type="button"
            onClick={handleSync}
            disabled={syncRunning}
            className="inline-flex items-center gap-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-blue-50 disabled:opacity-60 disabled:cursor-wait focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
          >
            {syncRunning ? (
              <>
                <svg
                  className="animate-spin h-4 w-4"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                >
                  <circle
                    className="opacity-25"
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    strokeWidth="4"
                  />
                  <path
                    className="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"
                  />
                </svg>
                Syncing… ({syncElapsedSec}s)
              </>
            ) : (
              <>
                <svg
                  className="h-4 w-4"
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  aria-hidden="true"
                >
                  <path
                    d="M17 7C16 6.8 14.8 6.6 13.6 6.4 12.2 6.4 11 6.6 9.6 7 8.3 7.6 7.2 8.5 6.2 9.7 5.3 11 4.6 12 4.2 13.2 4.1 14.4 4.4 15.4 5 16.5 5.9 17.2 6.9 18 7.6 20 6.4 21 5.4 21.2 4.5 21.4 3.7 21.5 3 21.3 2.4 21 1.9 20.5 1.5 19.8 1.1 19 0.9 18.2 0.8 17.2 1 16.4 1.4 15.8 2 15 2.9 14.4 4 14 5 14 6 14 7 14 8 14 9 14 10 14 11 14 12 14 13 14 14 14 15 14 16 14 17 14 17.5 14 18 14.9 18.5 16 19 17.5 19.5 19 20 20.5 20 22 19 22.6 17.5 23 15.5 23.4 12 23.6 8 23.9 4.5 24 2 24 0.8 24 0 24 0 24 0 24 0 24 0 24 0 23.5 1 22.8 2.4 21.8 4.3 20.2 6.3 18 8.3 15.2 10.4 12 12 9.3 13.5 6.6 15 4.4 16.3 3 17.5 2.3 18.6 2 19.8 2 21 2.8 23.4 4 24"
                  />
                </svg>
                Sync with Spotify
              </>
            )}
          </button>
        </div>
      </section>

      {/* Selection Grid */}
      <section aria-label="Playlist selection">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">Select a Playlist</h2>
        <div className={`grid gap-3 ${isPortrait ? 'grid-cols-1' : 'grid-cols-2 lg:grid-cols-3'}`}>
          {rows.map((row) => (
            <PlaylistRow
              key={row.playlist_id}
              row={row}
              maxCount={maxCount}
              selected={selectedPlaylistId === row.playlist_id}
              onSelect={handleSelectRow}
            />
          ))}
        </div>
      </section>

{/* Playlist Options (checkbox flags) */}
      {selectedPlaylistId && (
        <section
          aria-label="Playlist options"
          className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4"
        >
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            Playlist Options
          </h2>
          {shuffleDataQuery.isLoading || !localConfig ? (
            <p className="text-gray-500 dark:text-gray-400">Loading config…</p>
          ) : (
            <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
              <CheckboxField
                label="Auto Shuffle"
                checked={localConfig.autoShuffle}
                onChange={(v) => handleFlagChange('autoShuffle', v)}
              />
              <CheckboxField
                label="Manual Shuffle"
                checked={localConfig.manualShuffle}
                onChange={(v) => handleFlagChange('manualShuffle', v)}
              />
              <CheckboxField
                label="Generate Rec"
                checked={localConfig.makeRecs}
                onChange={(v) => handleFlagChange('makeRecs', v)}
              />
              <CheckboxField
                label="Seeds Only"
                checked={localConfig.seedsOnly}
                onChange={(v) => handleFlagChange('seedsOnly', v)}
              />
            </div>
          )}
        </section>
      )}

      {/* Tuning Inputs */}
      {/* Tuning Inputs */}
      {selectedPlaylistId && (
        <section aria-label="Tuning inputs" className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Tuning</h2>
          {shuffleDataQuery.isLoading || !localConfig ? (
            <p className="text-gray-500 dark:text-gray-400">Loading config…</p>
          ) : (
            <div className={`grid gap-4 ${isPortrait ? 'grid-cols-1' : 'grid-cols-2'}`}>
              <NumberInput label="Ratings Weight" value={localConfig.ratingsWeight} min={0} max={50} onChange={(v) => handleConfigChange('ratingsWeight', v)} />
              <NumberInput label="Recency Weight" value={localConfig.recencyWeight} min={0} max={50} onChange={(v) => handleConfigChange('recencyWeight', v)} />
              <NumberInput label="Random Weight" value={localConfig.randomWeight} min={0} max={50} onChange={(v) => handleConfigChange('randomWeight', v)} />
              <NumberInput label="Minutes to Sync" value={localConfig.minutesToSync} min={30} max={9999} hint="9999 = no duration limit" onChange={(v) => handleConfigChange('minutesToSync', v)} />
            </div>
          )}
        </section>
      )}
{/* Send to Spotify (FR-5/FR-6/FR-7) */}
      {selectedPlaylistId && (
        <div className="space-y-2 pt-2">
          {sendMutation.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">Failed to send to Spotify. Please try again.</p>
          )}
          <div className="flex justify-end">
            <button
              type="button"
              onClick={handleSend}
              disabled={previewEmpty || previewLoading || sendMutation.isPending}
              className="inline-flex items-center gap-2 rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            >
              {sendMutation.isPending ? (
                <>
                  <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" aria-hidden="true">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
                  </svg>
                  Sending…
                </>
              ) : (
                <>
                  Send to Spotify
                  {previewTargetId && (<span className="font-mono text-xs text-blue-100">{previewTargetId}</span>)}
                </>
              )}
            </button>
          </div>
        </div>
      )}

      {/* Live Preview (FR-3/FR-4) */}
      {selectedPlaylistId && (
        <section aria-label="Shuffle preview" className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Preview</h2>
            {previewTargetId && (<span className="text-xs font-mono text-gray-500 dark:text-gray-400">target: {previewTargetId}</span>)}
          </div>
          {previewLoading && previewRows.length === 0 && (
            <p className="text-gray-500 dark:text-gray-400">Computing preview…</p>
          )}
          {previewRows.length > 0 && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    <th className="py-2 pr-3 w-10">#</th>
                    <th className="py-2 pr-3">Song</th>
                    <th className="py-2 pr-3 w-20">Last Heard</th>
                    <th className="py-2 pr-3 w-20">Rating</th>
                    <th className="py-2 pr-3 w-20">Random</th>
                    <th className="py-2 pr-3 w-20">Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {previewRows.map((r) => {
                    const durMax = r.durationBarMax || 1;
                    const durPct = Math.min(100, Math.round(((r.duration_s || 0) / durMax) * 100));
                    return (
                      <tr key={r.isrc || r.newPosition} className="border-b border-gray-100 dark:border-gray-700/50 last:border-0">
                        <td className="py-2 pr-3 text-gray-500 dark:text-gray-400 font-mono">{r.newPosition + 1}</td>
                        <td className="py-2 pr-3 text-gray-900 dark:text-white font-medium">{r.trackArtist || '—'}</td>
                        <td className="py-2 pr-3"><ProgressBar value={r.recency_pct} color="bg-emerald-500 dark:bg-emerald-400" /></td>
                        <td className="py-2 pr-3"><ProgressBar value={r.ratings_pct} color="bg-blue-500 dark:bg-blue-400" /></td>
                        <td className="py-2 pr-3"><ProgressBar value={r.random_pct} color="bg-purple-500 dark:bg-purple-400" /></td>
                        <td className="py-2 pr-3">
                          <div className="flex items-center gap-2">
                            <div className="flex-1"><ProgressBar value={durPct / 100} color="bg-amber-500 dark:bg-amber-400" /></div>
                            <span className="text-xs text-gray-500 dark:text-gray-400 font-mono w-10 text-right">{formatDuration(r.duration_s || 0)}</span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          {previewEmpty && <p className="text-gray-500 dark:text-gray-400">No Songs found on this playlist</p>}
        </section>
      )}
    </div>
  );
}

function ProgressBar({ value, color }: { value: number | null; color: string }) {
  const pct = Math.max(0, Math.min(1, value || 0)) * 100;
  return (
    <div className="h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
      <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

function formatDuration(totalSeconds: number): string {
  if (!totalSeconds || totalSeconds < 0) return '0:00';
  const m = Math.floor(totalSeconds / 60);
  const s = Math.floor(totalSeconds % 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}
