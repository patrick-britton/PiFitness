/**
 * Tri-tip Timer Component
 *
 * Container for the Tri-tip Timer feature (Food -> Tri-tip Timer submodule).
 *
 * Reads the current backend state via useTriTipActive (OQ-3: returning to the
 * page restores live in-progress state) and branches on it:
 *   - no in-progress event  -> InitiationForm (weight + shape)
 *   - 'initiated' event     -> summary + fallback ETA (AC-10) + Place Meat prompt
 *   - 'active' event        -> active grilling dashboard (T07) + chart (T08)
 *
 * Three layouts: desktop / portrait / landscape.
 */

'use client';

import { useViewportStore } from '../../../stores/viewportStore';
import { useTriTipActive } from '../../../hooks/useTriTip';
import InitiationForm from './InitiationForm';
import ActiveEventView from './ActiveEventView';

export default function TriTipTimer() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isPortrait = layoutVariant === 'portrait';

  const { data, isLoading, isError, error } = useTriTipActive();

  // Loading state (skeleton, no layout shift).
  if (isLoading) {
    return (
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded-md w-48" />
        <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded-lg" />
      </div>
    );
  }

  if (isError) {
    const msg =
      error instanceof Error ? error.message : 'Could not load tri-tip state.';
    return (
      <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-red-700 dark:text-red-300">
        <p className="font-medium">Error loading tri-tip timer</p>
        <p className="text-sm mt-1">{msg}</p>
      </div>
    );
  }

  const event = data?.event ?? null;

  // No in-progress event -> initiation form.
  if (!event) {
    return (
      <div className={isDesktop ? 'max-w-2xl mx-auto' : ''}>
        <InitiationForm />
      </div>
    );
  }

  // In-progress event exists (initiated or active).
  return (
    <div className="space-y-4">
      <ActiveEventView event={event} />
    </div>
  );
}