'use client';

import { useState, useCallback } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { useUIStore } from '@/stores/uiStore';
import { API } from '@/lib/api-client';
import {
  ProcessActivityRequest,
  ProcessStepResult,
  ProcessStepEvent,
  ProcessStepStartEvent,
  ProcessSummaryData,
  STEP_ORDER,
} from '@/lib/types/activity-processing';
import StepChecklist from './StepChecklist';

type Mode = ProcessActivityRequest['mode'];
type Music = NonNullable<ProcessActivityRequest['music']>;

const MUSIC_OPTIONS: { id: Music; label: string }[] = [
  { id: 'running', label: 'Running' },
  { id: 'jogging', label: 'Jogging' },
  { id: 'no_music', label: 'No Music' },
];

export default function ActivityProcessingPage() {
  const { layoutVariant } = useViewportStore();
  const { setActiveSubPage } = useUIStore();
  const isLandscape = layoutVariant === 'landscape';

  const [mode, setMode] = useState<Mode>('last_run');
  const [music, setMusic] = useState<Music | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingStart, setLoadingStart] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [steps, setSteps] = useState<ProcessStepResult[]>([]);
  const [summary, setSummary] = useState<ProcessSummaryData | null>(null);
  const [submitted, setSubmitted] = useState(false);

  const canSubmit = mode === 'last_walk' || music !== null;

  const handleSubmit = useCallback(async () => {
    if (!canSubmit) return;
    setLoading(true);
    setError(null);
    setSteps([]);
    setSummary(null);
    setSubmitted(true);

    const startTime = Date.now();
    setLoadingStart(startTime);

    try {
      const request: ProcessActivityRequest =
        mode === 'last_run' && music != null
          ? { mode, music }
          : { mode };

      // Build initial pending steps
      setSteps(STEP_ORDER.map((stepId) => ({
        step_id: stepId,
                status: 'pending' as const,
        elapsed_ms: 0,
      })));

      // onStep — called for each running start event and each terminal step event via NDJSON
      const onStep = (event: ProcessStepStartEvent | ProcessStepEvent) => {
        setSteps((prev) => {
          const existing = prev.find((s) => s.step_id === event.step_id);
          const next: ProcessStepResult = {
            step_id: event.step_id,
            status: event.status,
            elapsed_ms: event.status === 'running' ? 0 : event.elapsed_ms,
            started_at:
              event.status === 'running'
                ? (event as ProcessStepStartEvent).started_at
                : existing?.started_at,
            error: 'error' in event ? event.error : undefined,
            result: 'result' in event ? event.result : undefined,
          };
          const idx = prev.findIndex((s) => s.step_id === event.step_id);
          if (idx === -1) return [...prev, next];
          const updated = [...prev];
          updated[idx] = next;
          return updated;
        });
      };

      const terminal = await API.activities.processActivity(request, onStep);

      setSummary(terminal.summary ?? null);

      if (!terminal.success && terminal.error) {
        setError(terminal.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Processing failed');
    } finally {
      setLoading(false);
    }
  }, [mode, music, canSubmit]);

  const handleReset = useCallback(() => {
    setMode('last_run');
    setMusic(null);
    setError(null);
    setSteps([]);
    setSummary(null);
    setSubmitted(false);
    setLoadingStart(null);
  }, []);

  // Navigate to the existing Recent Activity Report placeholder sub-page (FR-15).
  // The page itself is not built; this switches the Activities module to its
  // 'recent-activity' placeholder tab.
  const handleOpenRecentActivity = useCallback(() => {
    setActiveSubPage('recent-activity');
  }, [setActiveSubPage]);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Activity Processing</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Process a new activity and optionally reshuffle your playlist.
        </p>
      </div>

      {/* Hide selection while processing so the pinned current step stays at the top (AC-10) */}
      {!loading && (!submitted || steps.length > 0) ? (
        <div className={`space-y-4 ${isLandscape ? 'flex flex-row gap-4 space-y-0' : ''}`}>
          {/* Mode Selection */}
          <div className={isLandscape ? 'w-64 flex-shrink-0' : ''}>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Process Option
            </label>
            <div className={`grid ${isLandscape ? 'grid-cols-1' : 'grid-cols-2'} gap-2`}>
              {(['last_walk', 'last_run'] as const).map((m) => (
                <button
                  key={m}
                  onClick={() => setMode(m)}
                  className={`px-4 py-2 rounded-md text-sm font-medium border transition-colors ${
                    mode === m
                      ? 'bg-blue-600 text-white border-blue-600'
                      : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                  }`}
                >
                  {m === 'last_walk' ? 'Last Walk' : 'Last Run'}
                </button>
              ))}
            </div>
          </div>

          {/* Music Selection (Last Run only) */}
          {mode === 'last_run' && (
            <div className={`space-y-3 ${isLandscape ? 'flex-1' : ''}`}>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Music
              </label>
              <div className={`grid ${isLandscape ? 'grid-cols-1' : 'grid-cols-3'} gap-2`}>
                {MUSIC_OPTIONS.map((opt) => (
                  <button
                    key={opt.id}
                    onClick={() => setMusic(opt.id)}
                    className={`px-4 py-2 rounded-md text-sm font-medium border transition-colors ${
                      music === opt.id
                        ? 'bg-blue-600 text-white border-blue-600'
                        : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                    }`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : null}

      {/* Actions */}
      {!loading && (
        <div className="flex items-center gap-3">
          <button
            onClick={handleSubmit}
            disabled={!canSubmit}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {submitted && steps.length === 0 ? 'Run Again' : 'Start Processing'}
          </button>

          {(submitted || steps.length > 0) && (
            <button
              onClick={handleReset}
              className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700"
            >
              Reset
            </button>
          )}
        </div>
      )}

      {/* End-of-run confirmation (FR-14) — shown at the TOP of the results area (Bug T10-1) */}
      {!loading && summary && (
        <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
          <p className="text-sm font-semibold text-green-900 dark:text-green-100 mb-2">Processing complete</p>
          <div className="text-sm text-green-700 dark:text-green-300 space-y-1">
            {summary.total_elapsed_ms != null && (
              <p>
                Total time:{' '}
                {summary.total_elapsed_ms.toLocaleString()} ms
              </p>
            )}

            {/* Shuffle status shown only when the shuffle sequence actually ran (FR-14) */}
            {summary.playlist_shuffled != null && (
              <p>
                Playlist shuffled:{' '}
                {summary.playlist_shuffled ? 'Yes' : 'No'}
              </p>
            )}

            <p>
              Segments matched:{' '}
              {summary.segments_matched == null ? '—' : summary.segments_matched.toLocaleString()}
            </p>
            {summary.courses_matched != null && (
              <p>
                Courses matched:{' '}
                {summary.courses_matched.toLocaleString()}
              </p>
            )}
          </div>

          {/* Recent Activity Report navigation (FR-15) */}
          <button
            type="button"
            onClick={handleOpenRecentActivity}
            className="mt-3 px-4 py-2 bg-white dark:bg-gray-800 border border-green-300 dark:border-green-700 text-green-800 dark:text-green-200 rounded-md hover:bg-green-50 dark:hover:bg-green-900/30"
          >
            View Recent Activity Report
          </button>
        </div>
      )}

      {/* Loading / Results — Show StepChecklist whenever steps exist */}
      {(loading || steps.length > 0) && (
        <div className="space-y-3">
          {loading && steps.length === 0 && (
            <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Processing activity...</p>
          )}
          <StepChecklist steps={steps} loadingStart={loading ? loadingStart : null} />
        </div>
      )}

      {/* Error */}
      {error && !loading && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm text-red-700 dark:text-red-300">Error: {error}</p>
        </div>
      )}
    </div>
  );
}