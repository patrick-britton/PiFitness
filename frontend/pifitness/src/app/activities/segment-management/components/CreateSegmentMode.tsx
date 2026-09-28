'use client';

import { useState } from 'react';
import { ActivitySummary } from '@/lib/types/segment-management';
import ActivitySourceList from './ActivitySourceList';
import SegmentTrimEditor from './SegmentTrimEditor';

/**
 * Create Segment mode (009-003 T08, FR-1..FR-6).
 *
 * Two screens, never both: the source-activity list, then the trim editor for
 * the chosen activity. Selecting an activity hides the list (the 009-009
 * leaderboard pattern, which keeps the working surface uncluttered on phones);
 * "Choose new activity" returns to the list.
 */
export default function CreateSegmentMode() {
  const [activity, setActivity] = useState<ActivitySummary | null>(null);

  if (!activity) return <ActivitySourceList onSelect={setActivity} />;

  return (
    <SegmentTrimEditor
      activity={activity}
      onChooseNewActivity={() => setActivity(null)}
    />
  );
}