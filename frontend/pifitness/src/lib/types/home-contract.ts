/**
 * Home Module Contract (010-006 T01).
 *
 * Cross-surface contract for the home screen quick-add buttons:
 *   - "Log coffee (12 fl oz)" -> food_id 2
 *   - "Log Joy Juice (1 serving)" -> food_id 52
 *
 * Each button POSTs a diary entry for today via the existing log entry path.
 * The backend consumes `QuickLogRequest` and returns a `DayEntry` (the
 * snapshot entry just created), reusing the food contract types so there is
 * exactly one place a diary log request/response is defined.
 *
 * Per OQ-1 (reviewed), the food_id is hard-coded per button; no name-resolution
 * endpoint is needed for two known items.
 */

import type { DayEntry, LogEntryRequest } from './food-contract';

/** Request body the home page sends to log a quick-add food today. */
export interface QuickLogRequest {
  food_id: number;
  qty: number;
  unit: string;
}

/** Response returned after a quick-log: the snapshot entry just created. */
export type QuickLogResponse = DayEntry;

/** Hard-coded quick-add button definitions resolved at review time (OQ-1). */
export const QUICK_LOG_BUTTONS: {
  readonly id: string;
  readonly label: string;
  readonly food_id: number;
  readonly qty: number;
  readonly unit: string;
}[] = [
  { id: 'log-coffee', label: 'Coffee', food_id: 2, qty: 12, unit: 'fl oz' },
  { id: 'log-joy-juice', label: 'Smoothie', food_id: 52, qty: 1, unit: 'serving' },
];

/** Build a `LogEntryRequest` for posting to POST /api/food/diary. */
export function toLogEntryRequest(
  button: (typeof QUICK_LOG_BUTTONS)[number]
): LogEntryRequest {
  return {
    food_id: button.food_id,
    qty: button.qty,
    unit: button.unit,
    logged_at: undefined,
  };
}
