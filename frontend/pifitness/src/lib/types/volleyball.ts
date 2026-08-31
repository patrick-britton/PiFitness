/**
 * Volleyball Scorekeeping Types
 *
 * Cross-surface data contract for the Beach Volleyball scorekeeping feature.
 * Shared between frontend (React) and backend (FastAPI Pydantic models).
 *
 * Maps directly to the pre-built backend tables (no migrations; designs 006-001 + 006-002):
 *   volleyball.games   (game lifecycle; Team A is always "SR"; 006-002 adds
 *                       partner_number/partner_name for match context)
 *   volleyball.points  (one row per scored point — event sourcing;
 *                       score is derived by counting rows, never stored;
 *                       006-002 adds event_type for notable-play tags)
 *
 * Endpoints (backend/api/volleyball.py, prefix /api/sports/volleyball):
 *   GET    /api/sports/volleyball                      -> game history (completed_at desc, w/ final scores)
 *   GET    /api/sports/volleyball/active               -> active game + points + derived score, or game: null
 *   POST   /api/sports/volleyball                      -> create game (opponent name + SR partner); 409 if one is active
 *   POST   /api/sports/volleyball/{id}/points          -> add a point for a team
 *   DELETE /api/sports/volleyball/{id}/points/{team}   -> remove that team's most recent point
 *   POST   /api/sports/volleyball/{id}/points/latest/event -> tag the most recent point (either team) with an event_type (006-002)
 *   POST   /api/sports/volleyball/{id}/end             -> end game (completed_at = MAX(recorded_at))
 *   DELETE /api/sports/volleyball/{id}                 -> abandon game (cascade deletes its points)
 */

/** Lifecycle status of a volleyball game. */
export type VolleyballGameStatus = 'active' | 'completed';

/** Scoring team discriminator (CHECK in volleyball.points). */
export type VolleyballScoringTeam = 'SR' | 'OPPONENT';

/** Notable-play event types taggable on a point (006-002). */
export type VolleyballEventType = 'Ace' | 'Block' | 'Spike' | 'Dive';

/**
 * A volleyball game.
 * Maps to volleyball.games. `started_at` is set by the app to
 * MIN(recorded_at) of the game's points (null until the first point);
 * `completed_at` to MAX(recorded_at) when ended — never button-press time.
 */
export interface VolleyballGame {
  /** Primary key (SERIAL). */
  game_id: number;
  /** Opponent team name (Team B). Team A is always "SR" and is not stored. */
  team_b_name: string;
  /** Lifecycle status. */
  status: VolleyballGameStatus;
  /** MIN(recorded_at) of this game's points, or null before the first point. */
  started_at: string | null;
  /** MAX(recorded_at) of this game's points once completed. */
  completed_at: string | null;
  /** SR side's partner jersey number for this match (006-002). Nullable in the
   *  DB for pre-006-002 rows; required by the API at creation going forward. */
  partner_number: number | null;
  /** SR side's partner name for this match (006-002; optional). */
  partner_name: string | null;
  /** Optional label (unused in v1). */
  label?: string | null;
  /** Optional notes (unused in v1). */
  notes?: string | null;
  /** Row creation time. */
  created_at: string;
}

/**
 * A single scored point.
 * Maps to volleyball.points. The running score is derived by counting
 * these rows per team (event sourcing) — no score value is ever stored.
 */
export interface VolleyballPoint {
  /** Primary key (SERIAL). */
  point_id: number;
  /** Parent game id. */
  game_id: number;
  /** Which team scored. */
  scoring_team: VolleyballScoringTeam;
  /** Notable-play tag on this point (006-002): 'Ace' | 'Block' | 'Spike' | 'Dive', or null when untagged. */
  event_type: VolleyballEventType | null;
  /** Server-assigned timestamp (DEFAULT now()). */
  recorded_at: string;
  /** Row creation time. */
  created_at: string;
}

/** Derived cumulative score for a game (COUNT of points per team). */
export interface VolleyballScore {
  /** Points scored by Team A ("SR"). */
  sr: number;
  /** Points scored by Team B (the opponent). */
  opponent: number;
}

/** A game enriched with its points and derived score. */
export interface VolleyballGameDetail {
  /** The game record. */
  game: VolleyballGame;
  /** The game's points in recorded order (recorded_at asc). */
  points: VolleyballPoint[];
  /** Score derived by counting points per team. */
  score: VolleyballScore;
}

/** A completed game with its final derived score (history list). */
export interface VolleyballHistoryGame {
  /** The game record. */
  game: VolleyballGame;
  /** Final score derived by counting points per team. */
  score: VolleyballScore;
}

/**
 * Response for GET /api/sports/volleyball/active.
 * `game` is null when no game is active.
 */
export interface VolleyballActiveResponse {
  /** The active game, or null when none exists. */
  game: VolleyballGameDetail | null;
}

/** Response for GET /api/sports/volleyball (game history). */
export interface VolleyballHistoryResponse {
  /** Completed games, sorted descending by completed_at. */
  games: VolleyballHistoryGame[];
}

/** Request body for POST /api/sports/volleyball (create a game). */
export interface VolleyballCreateGameRequest {
  /** Opponent team name (Team B). Team A is always "SR" and is not prompted. */
  team_b_name: string;
  /** SR side's partner jersey number (mandatory at creation; 006-002). */
  partner_number: number;
  /** SR side's partner name (optional; 006-002). */
  partner_name?: string | null;
}

/**
 * Response for POST /api/sports/volleyball when the single-active guard
 * refuses creation. HTTP 409; `blocked_by` describes the active game.
 */
export interface VolleyballBlockedResponse {
  /** Detail message explaining the refusal. */
  detail: string;
  /** The active game blocking creation. */
  blocked_by: VolleyballGame;
}

/** Request body for POST /api/sports/volleyball/{id}/points. */
export interface VolleyballAddPointRequest {
  /** Which team scored. */
  scoring_team: VolleyballScoringTeam;
  /** Optional notable-play tag written onto THIS point at creation
   *  (006-002; Bug T08-3: the UI holds a selected event and writes it with
   *  the next point). Null/omitted records an untagged point. */
  event_type?: VolleyballEventType | null;
}

/** Request body for POST /api/sports/volleyball/{id}/points/latest/event (006-002). */
export interface VolleyballTagEventRequest {
  /** The notable-play tag to write onto the most recently recorded point. */
  event_type: VolleyballEventType;
}
