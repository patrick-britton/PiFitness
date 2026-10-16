/**
 * VolleyballMomentumBar
 * Consecutive-point streak meter (006-003, FR-5, AC-6).
 *
 * Renders between the partner line and the score boxes on both surfaces
 * (scorekeeper + viewer). Derives the trailing same-team streak from `points`;
 * hidden below a 2-point streak; fills center->left (SR) / center->right
 * (opponent), capped at 5 points wide with a fire emoji at the outer end at
 * >= 5; the label on the opposite side keeps counting past 5. Theme-aware
 * team colors; thin bar (height ≈ label font) with a text-sm italic label
 * the same size as the 'Playing with' line.
 *
 * Geometry (human corrections 2026-10-16):
 *   - 'Momentum-o-meter' title centered over the row.
 *   - The row is two fixed halves (`flex` + `w-1/2` each), so the bar's inner
 *     edge sits exactly at page center — directly above the dash between the
 *     two score boxes — no matter which side the streak is on or how wide
 *     the label is.
 *   - The fill lives in the half's track (flex-1 within the half, i.e. the
 *     "entire width minus emoji"); the fire owns a fixed `w-6` slot at the
 *     half's outer end OUTSIDE the fill — never overlapping it, and the
 *     track width stays constant whether the fire shows or not.
 */
'use client';

import { useEffect, useState } from 'react';
import { VolleyballPoint } from '../../../../lib/types/volleyball';

interface VolleyballMomentumBarProps {
  /** Points in recorded order (ascending); the trailing run computes the streak. */
  points: VolleyballPoint[];
  /** Opponent team name, used in the "by {opponent}" label. */
  opponentName: string;
}

export default function VolleyballMomentumBar({
  points,
  opponentName,
}: VolleyballMomentumBarProps) {
  // Recompute colors when the theme class toggles (same pattern as the
  // scoreboards, which read dark mode off the <html> class).
  const [isDark, setIsDark] = useState(false);
  useEffect(() => {
    const update = () =>
      setIsDark(
        typeof document !== 'undefined' &&
          document.documentElement.classList.contains('dark')
      );
    update();
    const observer = new MutationObserver(update);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });
    return () => observer.disconnect();
  }, []);

  // ------------------------------------------------------------------ streak
  let streak = 0;
  let streakTeam: 'SR' | 'OPPONENT' | null = null;
  for (let i = points.length - 1; i >= 0; i -= 1) {
    if (streak === 0) {
      streakTeam = points[i].scoring_team;
      streak = 1;
    } else if (points[i].scoring_team === streakTeam) {
      streak += 1;
    } else {
      break;
    }
  }

  if (streak < 2 || !streakTeam) return null;

  const isSR = streakTeam === 'SR';
  const streakColor = isSR
    ? isDark
      ? '#750530'
      : '#050C46'
    : isDark
      ? '#374151'
      : '#9ca3af';
  const widthPct = Math.min(streak, 5) * 20; // 20% per point, capped at 100%

  return (
    <div>
      <p className="text-center text-sm font-semibold text-gray-900 dark:text-white">
        Momentum-o-meter
      </p>
      <div className="flex items-center gap-1">
        {isSR ? (
          <>
            {/* Fixed left half: bar track spans the whole half (inner edge =
                page center, above the score-box dash). The fill grows from the
                inner edge outward; the fire slot is reserved at the outer end,
                outside the fill. Label owns the fixed right half. */}
            <div className="w-1/2 flex h-2 items-center justify-end gap-1">
              <div className="relative h-full flex-1">
                <div
                  className="absolute right-0 bottom-0 h-full"
                  style={{
                    backgroundColor: streakColor,
                    width: `${widthPct}%`,
                  }}
                />
              </div>
              <span aria-hidden="true" className="w-6 shrink-0 text-center text-sm leading-none">
                {streak >= 5 ? '🔥' : ''}
              </span>
            </div>
            <p className="w-1/2 text-sm italic text-gray-600 dark:text-gray-300">
              {streak} points in a row by Scripps
            </p>
          </>
        ) : (
          <>
            <p className="w-1/2 text-right text-sm italic text-gray-600 dark:text-gray-300">
              {streak} points in a row by {opponentName}
            </p>
            {/* Fixed right half, mirrored: inner edge = page center. */}
            <div className="w-1/2 flex h-2 items-center justify-start gap-1">
              <span aria-hidden="true" className="w-6 shrink-0 text-center text-sm leading-none">
                {streak >= 5 ? '🔥' : ''}
              </span>
              <div className="relative h-full flex-1">
                <div
                  className="absolute left-0 bottom-0 h-full"
                  style={{
                    backgroundColor: streakColor,
                    width: `${widthPct}%`,
                  }}
                />
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
