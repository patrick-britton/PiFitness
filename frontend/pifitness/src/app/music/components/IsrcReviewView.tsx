/**
 * IsrcReviewView — Music module: ISRC Review tab content (008-005)
 *
 * One pair at a time: overall score plus track/artist/album/duration lines.
 * Identical values render with a match marker; differing values render as
 * score + both values. Preferred merge-target ISRC is labeled (OQ-3).
 *
 * Reads: GET count (FR-1), GET match (FR-2). Writes: POST decision
 * (FR-3/FR-4/FR-5), POST rescan (FR-6, ~25 s).
 */

'use client';

import { useState } from 'react';
import { IsrcDupeMatch } from '@/lib/types/music';
import { useViewportStore } from '@/stores/viewportStore';
import {
  useIsrcDupeCount,
  useIsrcDupeMatch,
  useDecideIsrcDupe,
  useRescanIsrcDupes,
} from '@/hooks/useMusic';

/** One comparison line: match marker when identical, else score + values. */
function CompareLine({
  label,
  value1,
  value2,
  scorePct,
  durationScore,
}: {
  label: string;
  value1: string | null;
  value2: string | null;
  scorePct?: number | null;
  durationScore?: number | null;
}) {
  const identical = (value1 ?? '') === (value2 ?? '');
  return (
    <p className="text-sm text-gray-900 dark:text-white">
      <span className="font-semibold text-blue-600 dark:text-blue-400">{label}: </span>
      {identical ? (
        <span>
          <span aria-hidden="true">✓ </span>
          <strong>{value1 ?? '—'}</strong>
          <span aria-hidden="true"> ✓</span>
        </span>
      ) : durationScore !== undefined ? (
        <span><strong>{durationScore}</strong></span>
      ) : (
        <span>
          <strong>{scorePct ?? 0}%</strong>{' : '}
          <strong>{value1 ?? '—'}</strong>{' vs '}
          <strong>{value2 ?? '—'}</strong>
        </span>
      )}
    </p>
  );
}

function MatchupDetail({ match }: { match: IsrcDupeMatch }) {
  const toPct = (v: number | null) =>
    v === null || v === undefined ? null : Math.round(Number(v) * 100);
  const preferredIsFirst = match.preferredIsrc === match.isrc1;
  return (
    <div className="space-y-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
      <p className="text-sm text-gray-900 dark:text-white">
        <span className="font-semibold text-blue-600 dark:text-blue-400">Overall: </span>
        <strong>{match.matchScore}</strong>
      </p>
      <CompareLine label="Track" value1={match.trackName1} value2={match.trackName2} scorePct={toPct(match.trackScore)} />
      <CompareLine label="Artist" value1={match.artistName1} value2={match.artistName2} scorePct={toPct(match.artistScore)} />
      <CompareLine label="Album" value1={match.albumName1} value2={match.albumName2} scorePct={toPct(match.albumScore)} />
      <CompareLine label="Duration" value1={String(match.duration1 ?? '')} value2={String(match.duration2 ?? '')} durationScore={match.durationScore} />
      <p className="text-xs text-gray-500 dark:text-gray-400">
        Preferred ISRC: <span className="font-mono font-semibold">{match.preferredIsrc}</span>
        {preferredIsFirst ? ' (first)' : ' (second)'}
      </p>
      <p className="text-xs text-gray-500 dark:text-gray-400 font-mono">
        {match.isrc1} vs {match.isrc2}
      </p>
    </div>
  );
}


export default function IsrcReviewView() {
  const { data: countData, isLoading: countLoading, isError: countError } =
    useIsrcDupeCount();
  const {
    data: matchData,
    isLoading: matchLoading,
    isError: matchError,
    refetch: refetchMatch,
  } = useIsrcDupeMatch((countData?.count ?? 0) > 0);
  const decideMutation = useDecideIsrcDupe();
  const rescanMutation = useRescanIsrcDupes();
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';
  const [toast, setToast] = useState<string | null>(null);

  const count = countData?.count ?? 0;
  const match = matchData?.match ?? null;

  const showToast = (msg: string) => {
    setToast(msg);
    window.setTimeout(() => setToast(null), 3000);
  };

  const handleDecision = (accept: boolean) => {
    if (!match) return;
    decideMutation.mutate(
      {
        isrc1: match.isrc1,
        isrc2: match.isrc2,
        preferredIsrc: match.preferredIsrc,
        accept,
      },
      {
        onSuccess: (res) => {
          showToast(res.message);
          refetchMatch();
        },
      },
    );
  };

  if (countLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-gray-500 dark:text-gray-400">Loading…</p>
      </div>
    );
  }

  if (countError) {
    return (
      <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 px-4 py-3">
        <p className="text-sm text-red-800 dark:text-red-200">
          Could not load duplicate ISRCs.
        </p>
      </div>
    );
  }

  if (count === 0) {
    return (
      <div className="space-y-4">
        <div className="rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 px-4 py-3">
          <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
            No duplicate isrcs found
          </p>
        </div>
        <button
          onClick={() => rescanMutation.mutate()}
          disabled={rescanMutation.isPending}
          className="inline-flex items-center justify-center rounded-md bg-blue-600 px-4 py-2 min-h-[44px] text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-wait focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
        >
          {rescanMutation.isPending ? 'Takes about 25 seconds…' : 'Look for new matches'}
        </button>
        {rescanMutation.isError && (
          <p className="text-sm text-red-600 dark:text-red-400">
            Re-scan failed. Please try again.
          </p>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 px-4 py-3">
        <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
          {count} potential duplicate isrcs found
        </p>
      </div>

      {matchLoading && (
        <div className="p-8 text-center">
          <p className="text-gray-500 dark:text-gray-400">Loading matchup…</p>
        </div>
      )}

      {matchError && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 px-4 py-3">
          <p className="text-sm text-red-800 dark:text-red-200">
            Could not load the next pair.
          </p>
        </div>
      )}

      {match && !matchLoading && (
        <div className={isPortrait ? 'space-y-4' : isLandscape ? 'grid grid-cols-2 gap-3' : 'max-w-2xl space-y-4'}>
          <MatchupDetail match={match} />
          <div className="flex gap-3">
            <button
              onClick={() => handleDecision(true)}
              disabled={decideMutation.isPending}
              className="inline-flex items-center justify-center rounded-md bg-green-600 px-6 py-2 min-h-[44px] min-w-[44px] text-sm font-bold text-white hover:bg-green-700 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-green-500"
            >
              ACCEPT
            </button>
            <button
              onClick={() => handleDecision(false)}
              disabled={decideMutation.isPending}
              className="inline-flex items-center justify-center rounded-md bg-red-600 px-6 py-2 min-h-[44px] min-w-[44px] text-sm font-bold text-white hover:bg-red-700 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500"
            >
              REJECT
            </button>
          </div>
        </div>
      )}

      {decideMutation.isError && (
        <p className="text-sm text-red-600 dark:text-red-400">
          Decision failed. Please try again.
        </p>
      )}

      {toast && (
        <div
          role="status"
          aria-live="polite"
          className="rounded-md bg-gray-900 dark:bg-gray-100 px-4 py-2 text-sm text-white dark:text-gray-900"
        >
          {toast}
        </div>
      )}
    </div>
  );
}
