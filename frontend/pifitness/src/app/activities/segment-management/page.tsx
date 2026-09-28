'use client';

import { useState } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import CreateSegmentMode from './components/CreateSegmentMode';
import MatchActivitiesMode from './components/MatchActivitiesMode';
import CourseReviewMode from './components/CourseReviewMode';

/**
 * Segment Management page (009-003).
 *
 * Hosts the feature's three modes: Create Segment (T08), Match Activities
 * (T09), and Course Review (T10). Mode navigation follows the design system's
 * three-state pattern — desktop tabs with labels, a portrait dropdown, and a
 * compact scrolling tab strip in landscape.
 */

type ModeId = 'create' | 'match' | 'courses';

const MODES: { id: ModeId; label: string }[] = [
  { id: 'create', label: 'Create Segment' },
  { id: 'match', label: 'Match Activities' },
  { id: 'courses', label: 'Course Review' },
];

export default function SegmentManagementPage() {
  const [mode, setMode] = useState<ModeId>('create');
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isPortrait = layoutVariant === 'portrait';

  return (
    <div className="space-y-4">
      {isPortrait ? (
        <div>
          <label
            htmlFor="seg-mode-select"
            className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
          >
            Mode
          </label>
          <select
            id="seg-mode-select"
            value={mode}
            onChange={(e) => setMode(e.target.value as ModeId)}
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {MODES.map((m) => (
              <option key={m.id} value={m.id}>
                {m.label}
              </option>
            ))}
          </select>
        </div>
      ) : (
        <nav
          className={`flex border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 rounded-t-md ${
            isDesktop ? 'gap-1' : 'overflow-x-auto gap-0'
          }`}
          aria-label="Segment Management modes"
        >
          {MODES.map((m) => (
            <button
              key={m.id}
              type="button"
              onClick={() => setMode(m.id)}
              aria-current={mode === m.id ? 'page' : undefined}
              title={m.label}
              className={`px-3 py-2.5 text-sm font-medium border-b-2 whitespace-nowrap transition-colors ${
                isDesktop ? 'sm:px-4' : ''
              } ${
                mode === m.id
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-600'
              }`}
            >
              {isDesktop ? m.label : m.label.split(' ')[0]}
            </button>
          ))}
        </nav>
      )}

      {mode === 'create' && <CreateSegmentMode />}

      {mode === 'match' && <MatchActivitiesMode />}

      {mode === 'courses' && <CourseReviewMode />}
    </div>
  );
}