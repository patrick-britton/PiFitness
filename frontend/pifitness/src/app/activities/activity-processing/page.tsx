'use client';

import { useState, useCallback } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { API } from '@/lib/api-client';
import { ProcessActivityRequest, ProcessStepResult, ProcessStepEvent, STEP_ORDER } from '@/lib/types/activity-processing';
import StepChecklist from './StepChecklist';

export default function ActivityProcessingPage() {
  const { layoutVariant } = useViewportStore();
  const isLandscape = layoutVariant === 'landscape';

  const [playlist, setPlaylist] = useState<ProcessActivityRequest['playlist_name']>('Running');
  const [manualStart, setManualStart] = useState('');
  const [manualEnd, setManualEnd] = useState('');
  const [loading, setLoading] = useState(false);
  const [loadingStart, setLoadingStart] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [steps, setSteps] = useState<ProcessStepResult[]>([]);
  const [submitted, setSubmitted] = useState(false);

  const isManual = playlist === 'Manual Processing';

  const handleSubmit = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSteps([]);
    setSubmitted(true);

    const startTime = Date.now();
    setLoadingStart(startTime);

    try {
      const request: ProcessActivityRequest = {
        playlist_name: playlist,
      };

      if (isManual) {
        if (!manualStart || !manualEnd) {
          throw new Error('Manual Processing requires both start and end datetime.');
        }
        request.manual_start_utc = manualStart;
        request.manual_end_utc = manualEnd;
      }

      // Build initial pending steps
      setSteps(STEP_ORDER.map((stepId) => ({
        step_id: stepId,
        status: 'pending' as const,
        elapsed_ms: 0,
      })));

      // onStep callback — called each time a step-completion event arrives via NDJSON
      const onStep = (event: ProcessStepEvent) => {
        setSteps((prev) => {
          const idx = prev.findIndex((s) => s.step_id === event.step_id);
          if (idx === -1) {
            // Step not yet in list — append it
            return [...prev, {
              step_id: event.step_id,
              status: event.status,
              elapsed_ms: event.elapsed_ms,
              error: event.error,
              result: event.result,
            }];
          }
          // Replace existing step
          const updated = [...prev];
          updated[idx] = {
            step_id: event.step_id,
            status: event.status,
            elapsed_ms: event.elapsed_ms,
            error: event.error,
            result: event.result,
          };
          return updated;
        });
      };

      const terminal = await API.activities.processActivity(request, onStep);

      if (!terminal.success && terminal.error) {
        setError(terminal.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Processing failed');
    } finally {
      setLoading(false);
    }
  }, [playlist, manualStart, manualEnd, isManual]);

  const handleReset = useCallback(() => {
    setPlaylist('Running');
    setManualStart('');
    setManualEnd('');
    setError(null);
    setSteps([]);
    setSubmitted(false);
    setLoadingStart(null);
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Activity Processing</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Process a new activity and optionally reshuffle your playlist.
        </p>
      </div>

      {!submitted || steps.length > 0 ? (
        <div className={`space-y-4 ${isLandscape ? 'flex flex-row gap-4 space-y-0' : ''}`}>
          {/* Playlist Selection */}
          <div className={isLandscape ? 'w-64 flex-shrink-0' : ''}>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Playlist Option
            </label>
            <div className={`grid ${isLandscape ? 'grid-cols-1' : 'grid-cols-2 sm:grid-cols-4'} gap-2`}>
              {(['Running', 'Jogging', 'No Playlist', 'Manual Processing'] as const).map((option) => (
                <button
                  key={option}
                  onClick={() => setPlaylist(option)}
                  className={`px-4 py-2 rounded-md text-sm font-medium border transition-colors ${
                    playlist === option
                      ? 'bg-blue-600 text-white border-blue-600'
                      : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                  }`}
                >
                  {option}
                </button>
              ))}
            </div>
          </div>

          {/* Manual datetime inputs */}
          {isManual && (
            <div className={`space-y-3 ${isLandscape ? 'flex-1' : ''}`}>
              <div>
                <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Start Time (UTC ISO 8601)
                </label>
                <input
                  type="text"
                  value={manualStart}
                  onChange={(e) => setManualStart(e.target.value)}
                  placeholder="2026-07-18T08:00:00Z"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white text-sm"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">
                  End Time (UTC ISO 8601)
                </label>
                <input
                  type="text"
                  value={manualEnd}
                  onChange={(e) => setManualEnd(e.target.value)}
                  placeholder="2026-07-18T09:00:00Z"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white text-sm"
                />
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
            disabled={isManual && (!manualStart || !manualEnd)}
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