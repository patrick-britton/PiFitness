/**
 * Effort recency buckets for the Leaderboards page (009-009 T17 / OQ-4).
 *
 * Membership is DB-computed: `activities.vw_segment_leaderboard.cycle_name`
 * ('Current Cycle' | 'Last 365' | 'All Time'), verified live 2026-09-16.
 * The four rank columns are window-scoped ORDERING values and are populated for
 * every row (live check: 2192/2192 non-null), so the previous `!= null` chain
 * marked every non-recent effort "Training Cycle" (Bug 009-009-T17-1).
 *
 * Pure and dependency-free (no React, no DOM, no Chart.js) so
 * `scripts/verify-bars.mjs` can assert the derivation instead of a device.
 */
import type { LeaderboardEffort, LeaderboardRange } from './types/leaderboards';

/** Lollipop buckets — one chart dataset each (keys match the `--lb-*` tokens). */
export type EffortBucket = 'recent' | 'cycle' | '365' | 'alltime';

/**
 * Bucket for one effort. Precedence (human, OQ-4): Most Recent > Current Cycle >
 * Last 365 > All Time. 'Most Recent' is the segment's single latest effort by
 * date (`recency_rank === 1`) — the view carries no such label. An absent or
 * unexpected label falls to 'alltime' (never guessed into a window).
 */
export function bucketOf(effort: LeaderboardEffort): EffortBucket {
  if (effort.recency_rank === 1) return 'recent';
  if (effort.cycle_name === 'Current Cycle') return 'cycle';
  if (effort.cycle_name === 'Last 365') return '365';
  return 'alltime';
}

/**
 * Range-window MEMBERSHIP (ordering stays with the range's rank column).
 * 'Most Recent' and 'All Time' are whole-set views — the table re-sorts and
 * re-baselines them. 'Current Cycle' keeps just in-cycle efforts; 'Last 365'
 * keeps in-cycle efforts too, because they are also within 365 days (the view's
 * label is exclusive, the range window is not). An absent label is outside both
 * windows.
 */
export function inRange(effort: LeaderboardEffort, range: LeaderboardRange): boolean {
  if (range === 'Most Recent' || range === 'All Time') return true;
  if (range === 'Current Cycle') return effort.cycle_name === 'Current Cycle';
  return effort.cycle_name === 'Current Cycle' || effort.cycle_name === 'Last 365';
}