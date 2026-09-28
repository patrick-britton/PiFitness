'use client';

import { useState } from 'react';
import { SegmentListRow } from '@/lib/types/leaderboards';
import SegmentPicker from './SegmentPicker';
import MatchSession from './MatchSession';

/** Match Activities mode (009-003 T09, FR-7..FR-13).
 *
 * Two screens, never both: the filterable segment/course picker, then the
 * match-session editor for the chosen segment. Selecting a segment hides the
 * picker (the 009-009 leaderboard pattern); "Choose different segment" returns
 * to the picker.
 */
export default function MatchActivitiesMode() {
  const [segment, setSegment] = useState<SegmentListRow | null>(null);

  if (!segment) return <SegmentPicker onSelect={setSegment} />;

  return <MatchSession segment={segment} onChooseDifferentSegment={() => setSegment(null)} />;
}