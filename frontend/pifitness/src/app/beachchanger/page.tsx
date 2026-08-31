/**
 * Unlisted Beach Volleyball Viewer — fixed URL /beachchanger (006-002)
 * ('gamechanger', but for beach volleyball). Intentionally NOT added to any
 * navigation, menu, or sub-page config (AC-3): the only way here is typing
 * or bookmarking the URL.
 *
 * Read-only by construction (AC-2): this route imports only the viewer
 * component below — no mutation hooks, no scorekeeper components — so no
 * control capable of mutating volleyball.games / volleyball.points is
 * shipped to this route.
 */
'use client';

import BeachViewer from './components/BeachViewer';

export default function BeachViewerPage() {
  return <BeachViewer />;
}
