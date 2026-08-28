/**
 * Beach Volleyball Scorekeeping Page
 *
 * Container for the Volleyball Scorekeeping feature (Activities -> Beach
 * submodule, last tab). Renders the BeachVolleyball component, which reads
 * the current backend state via React Query hooks and branches between
 * pre-game (start form + history), active scoreboard, and history states.
 *
 * Contract: frontend/pifitness/src/lib/types/volleyball.ts
 */
'use client';

import BeachVolleyball from './components/BeachVolleyball';

export default function BeachPage() {
  return <BeachVolleyball />;
}
