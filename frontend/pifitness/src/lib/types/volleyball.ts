/**
 * Volleyball Scorekeeping Types
 *
 * Cross-surface data contract for the Beach Volleyball scorekeeping feature.
 * Shared between frontend (React) and backend (FastAPI Pydantic models).
 *
 * Maps directly to the pre-built backend tables (no migrations; design 006-001):
 *   volleyball.games   (game lifecycle; Team A is always "SR")
 *   volleyball.points  (one row per scored point — event sourcing;
 *                       score is derived by counting rows, never stored)
 *
 * Endpoints (backend/api/volleyball.py, prefix /api/sports/volleyball):
 *   GET    /api/sports/volleyball                      -> game history (completed_at desc, w/ final scores)
 *   GET    /api/sports/volleyball/active               -> active game + points + derived score, or game: null
 *   POST   /api/sports/volleyball                      -> create game (opponent name only); 409 if one is active
 *   POST   /api/sports/volleyball/{id}/points          -> add a point for a team
 *   DELETE /api/sports/volleyball/{id}/points/{team}   -> remove that team's most recent point
 *   POST   /api/sports/volleyball/{id}/end             -> end game (completed_at = MAX(recorded_at))
 *   DELETE /api/sports/volleyball/{id}                 -> abandon game (cascade deletes its points)
 */

/** Lifecycle status of a volleyball game. */
export type VolleyballGameStatus = 'active' | 'completed';

/** Scoring team discriminator (CHECK in volleyball.points). */
export type VolleyballScoringTeam = 'SR' | 'OPPONENT';

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
}
