/**
 * Tri-tip Timer Types
 *
 * Cross-surface data contract for the Tri-tip Timer feature.
 * Shared between frontend (React) and backend (FastAPI Pydantic models).
 *
 * Maps directly to the pre-built backend tables:
 *   food.tri_tip_events     (event lifecycle)
 *   food.tri_tip_readings   (grill + internal temperature readings)
 *
 * Endpoints (backend/api/tri_tip.py, prefix /api/food/tri-tip):
 *   GET    /api/food/tri-tip          -> list events
 *   GET    /api/food/tri-tip/active   -> current in-progress event + prediction + references
 *   GET    /api/food/tri-tip/{id}     -> single event with readings
 *   POST   /api/food/tri-tip          -> initiate event (weight + shape)
 *   POST   /api/food/tri-tip/{id}/place    -> place meat (creates first reading @ 38F)
 *   POST   /api/food/tri-tip/{id}/readings -> record a reading while active
 *   POST   /api/food/tri-tip/{id}/complete -> pull meat (status -> complete)
 *   DELETE /api/food/tri-tip/{id}     -> abandon (removes event + cascade readings)
 */

/** Lifecycle status of a tri-tip event. */
export type TriTipStatus = 'initiated' | 'active' | 'complete';

/** Supported meat shapes, chosen at initiation (OQ: three shapes). */
export type TriTipShape = 'Short+Fat' | 'Long+Skinny' | 'Typical';

/**
 * A tri-tip grilling event.
 * Maps to food.tri_tip_events.
 */
export interface TriTipEvent {
  /** Primary key (SERIAL). */
  tri_tip_id: number;
  /** Weight in pounds. */
  weight_lbs: number;
  /** Meat shape at initiation. */
  shape: TriTipShape;
  /** Target internal temperature in °F (default 125.0). */
  target_internal_temp_f: number;
  /** Lifecycle status. */
  status: TriTipStatus;
  /** Set by the app to MIN(recorded_at) of this event's readings (not now()). */
  started_at: string | null;
  /** Set by the app to MAX(recorded_at) of this event's readings (not now()). */
  completed_at: string | null;
  /** Optional label. */
  label?: string | null;
  /** Optional notes. */
  notes?: string | null;
  /** Row creation time. */
  created_at: string;
}

/**
 * A single grill/thermometer reading.
 * Maps to food.tri_tip_readings.
 */
export interface TriTipReading {
  /** Primary key (SERIAL). */
  reading_id: number;
  /** Parent event id. */
  tri_tip_id: number;
  /** Server-assigned timestamp (DEFAULT now()). */
  recorded_at: string;
  /** Grill temperature in °F. */
  grill_temp_f: number;
  /** Internal meat temperature in °F. */
  internal_temp_f: number;
  /** Optional note. */
  note?: string | null;
  /** Row creation time. */
  created_at: string;
}

/** A single point on the predicted temperature curve. */
export interface TriTipCurvePoint {
  /** Minutes elapsed since the event's first reading (t₀ = MIN recorded_at). */
  grill_min: number;
  /** Predicted internal temperature in °F. */
  internal_temp_f: number;
}

/**
 * Prediction / ETA model (OQ-1).
 * Present from initiation (shape-aware fallback, ~15 min/lb when no readings
 * exist) and revised as readings arrive. The shape-aware model also learns
 * from prior events' readings.
 */
export interface TriTipPrediction {
  /** ISO time-of-day the meat is projected to reach the target internal temp (125 °F). */
  projected_done_at: string;
  /** Relative number of minutes between now and projected_done_at. */
  minutes_remaining: number;
  /** Predicted curve from t₀ to (and past) the target temp. */
  curve: TriTipCurvePoint[];
}

/** An event enriched with its readings. */
export interface TriTipEventDetail {
  /** The event record. */
  event: TriTipEvent;
  /** The event's readings. */
  readings: TriTipReading[];
}

/**
 * A prior completed event normalized to its own t₀, used as gray chart
 * reference background (OQ-2). Each reading is relabeled with the minutes
 * elapsed since that event's started_at so curves overlay regardless of
 * clock time.
 */
export interface TriTipReferenceEvent {
  /** The prior completed event. */
  event: TriTipEvent;
  /** Readings normalized to grill-minutes elapsed since this event's t₀. */
  readings: (TriTipReading & { grill_min: number })[];
}

/**
 * Response for GET /api/food/tri-tip/active.
 * Holds the current in-progress event (initiated or active), its readings and
 * prediction, prior completed events as chart references, and (OQ-3) the event
 * that is blocking a new initiate when applicable.
 */
export interface TriTipActiveResponse {
  /** The current in-progress event, or null when none exists. */
  event: TriTipEvent | null;
  /** Readings for the current in-progress event. */
  readings: TriTipReading[];
  /** Prediction for the in-progress event (present from initiation). */
  prediction: TriTipPrediction | null;
  /** Prior completed events as gray chart references (OQ-2). */
  references: TriTipReferenceEvent[];
  /** Set when another initiate was refused (block-and-guide, OQ-3). */
  blocked_by: TriTipEvent | null;
}

/** Response for GET /api/food/tri-tip (events list). */
export interface TriTipListResponse {
  data: TriTipEvent[];
  count: number;
}

/** Request body for POST /api/food/tri-tip (initiate an event). */
export interface TriTipInitiateRequest {
  /** Weight in pounds (CHECK weight_lbs > 0). */
  weight_lbs: number;
  /** One meat shape. */
  shape: TriTipShape;
}

/** Request body for POST /api/food/tri-tip/{id}/place. */
export interface TriTipPlaceRequest {
  /** Grill temperature in °F. Internal temp is fixed at 38.0 by the backend. */
  grill_temp_f: number;
}

/** Request body for POST /api/food/tri-tip/{id}/readings. */
export interface TriTipReadingRequest {
  /** Grill temperature in °F. */
  grill_temp_f: number;
  /** Internal meat temperature in °F. */
  internal_temp_f: number;
  /** Optional note. */
  note?: string;
}