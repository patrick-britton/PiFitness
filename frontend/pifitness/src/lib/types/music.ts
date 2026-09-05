/**
 * Music Module Types — Now Playing & Recently Heard
 *
 * Cross-surface data contract for feature 008-001.
 * Shared between frontend (React) and backend (FastAPI/Pydantic).
 *
 * No new schema: reads reuse canonical `music.*` views/tables
 * (vw_track_id_to_isrc, vw_best_track_id, all_tracks, all_albums,
 * playlist_relationships, ratings, track_recommendations, playlist_config,
 * playlist_isrcs, vw_recent_plays). Writes touch only the existing staging path
 * (api_imports / stg_now_playing via flatten_now_playing) and existing music.*
 * tables (playlist_isrcs, track_recommendations,
 * playlist_recommendation_exclusions, ratings, ratings_history).
 *
 * Endpoint inventory (backend/api/music.py, prefix /api/music):
 *   GET  /now-playing
 *        -> { playing, rateLimited, track: NowPlayingTrack | null, refreshedAt }
 *   POST /now-playing/skip            -> { ok, message }
 *   POST /now-playing/promote         -> { ok, message }
 *   POST /now-playing/soft-reject     -> { ok, message }
 *   POST /now-playing/hard-reject     -> { ok, message }
 *   POST /now-playing/remove          -> { ok, message }
 *   POST /now-playing/rank-up         -> { ok, message }
 *   POST /now-playing/rank-down       -> { ok, message }
 *   POST /now-playing/add-to-playlist -> { ok, message }  body { playlist_id }
 *   GET  /now-playing/add-targets     -> { playlists, eligible }
 *   GET  /recent-plays?limit=         -> { plays, scale }
 *   GET  /album-art/{album_id}        -> JPEG (OQ-1: local disk cache keyed by
 *                                         album id; downloaded from Spotify on
 *                                         first need)
 *   GET  /service-status              -> { spotify: { rateLimited, rateLimitClearedUtc } }
 *   GET  /ratings/eligible-count     -> { count }   (pre-existing; FR-10
 *                                         landing)
 *   GET  /ratings/matchup?playlist_id= -> { ok, primary, challenger }
 *   POST /ratings/matchup/score        -> { ok, next, scores }
 *                                         query { playlist_id, isrc, isrc_vs, margin }
 */

/**
 * Playback context relationship of the current track.
 * - `'regular'`     — playing from the track's own playlist (or a child that
 *                     resolves through `playlist_relationships` to a parent).
 * - `'recommendation'` — playing from a recommendation set; Promote / Soft
 *                     Reject / Hard Reject apply, on the PARENT playlist.
 * - `null`          — no playlist context ("not from playlist").
 */
export type MusicContextRelationshipType = 'regular' | 'recommendation' | null;

/** Where the displayed rating comes from. */
export type MusicRatingSource = 'ratings' | 'predicted' | 'baseline';

/** Playback context for the current track, derived from canonical data. */
export interface NowPlayingContext {
  /** Whether playback context is a Spotify playlist at all. */
  isPlaylist: boolean;
  /** The context playlist's Spotify id, or null when not from a playlist. */
  playlistId: string | null;
  /** The context playlist's display name, or null when not applicable. */
  playlistName: string | null;
  /** 'regular' | 'recommendation' when on a playlist, null otherwise. */
  relationshipType: MusicContextRelationshipType;
  /**
   * Parent playlist id when the context playlist is a child in a local
   * playlist family (drives which controls appear); null otherwise.
   */
  parentPlaylistId: string | null;
  /**
   * Parent playlist display name (when on a recommendation or family child);
   * null otherwise. Drives the context line label.
   */
  parentPlaylistName: string | null;
}

/** Rating value shown for the current track. */
export interface NowPlayingRating {
  /** ELO-style rating value (predicted for recommendations). */
  value: number;
  /** 'ratings' from music.ratings, 'predicted' from track_recommendations,
   *  'baseline' when unrated and displayed at the 1500 baseline. */
  source: MusicRatingSource;
}

/** Optional progression info for the playing track (if the API provides it). */
export interface NowPlayingCompletion {
  /** Seconds elapsed in the track so far. */
  doneInS: number;
  /** ISO timestamp when the track completes. */
  completeAtTS: string;
}

/**
 * The currently playing (or most recently resolved) track for Now Playing.
 * Album art is served from the app's album-art endpoint; the browser never
 * reads local disk paths (OQ-1).
 */
export interface NowPlayingTrack {
  /** Spotify track id from playback. */
  trackId: string;
  /** Canonical best-track id (music.vw_best_track_id + all_tracks). */
  bestTrackId: string;
  /** Resolved ISRC (music.vw_track_id_to_isrc). */
  isrc: string;
  /** Cleaned track name. */
  trackName: string;
  /** Artist display name. */
  artistName: string;
  /** Album display name. */
  albumName: string;
  /** Spotify album id (keys the album-art cache). */
  albumId: string;
  /** Browser URL to the album-art endpoint for this album. */
  albumArtUrl: string;
  /** Playback context and playlist-family resolution. */
  context: NowPlayingContext;
  /** Rating to display (regular / predicted / baseline). */
  rating: NowPlayingRating;
  /** Track progression, when available. */
  completion: NowPlayingCompletion | null;
}

/**
 * Response for GET /api/music/now-playing.
 * `track` is null when nothing is playing or recording failed before
 * resolution (FR-2 → the small "No songs currently playing" state).
 */
export interface NowPlayingResponse {
  /** Whether a track is playing / resolved. */
  playing: boolean;
  /** True when the Spotify service is rate-limited (FR-9). */
  rateLimited: boolean;
  /** The resolved current track, or null. */
  track: NowPlayingTrack | null;
  /** ISO timestamp of when the API computed this snapshot. */
  refreshedAt: string;
}

// ---------------------------------------------------------------------------
// Now Playing actions
// ---------------------------------------------------------------------------

/** Common response for every Now Playing action POST. */
export interface MusicActionResponse {
  ok: boolean;
  message: string;
}

/** Request body for POST /api/music/now-playing/add-to-playlist. */
export interface MusicAddToPlaylistRequest {
  /** The target playlist's Spotify id. */
  playlist_id: string;
}

/** A playlist offered as an add-to target (config-driven eligibility). */
export interface MusicPlaylistTarget {
  playlist_id: string;
  playlist_name: string;
}

/**
 * Response for GET /api/music/now-playing/add-targets.
 * `eligible` is false when no configured playlist qualifies, and `playlists`
 * is empty — the UI shows an error instead of a target list (FR-6).
 */
export interface MusicAddTargetsResponse {
  playlists: MusicPlaylistTarget[];
  eligible: boolean;
}

// ---------------------------------------------------------------------------
// Recently Heard
// ---------------------------------------------------------------------------

/**
 * One row of the Recently Heard list (music.vw_recent_plays).
 * Read-only; the Recently Heard view performs no writes (FR-8).
 */
export interface RecentPlayRow {
  /** Track ISRC. */
  isrc: string;
  /** ISO timestamp of the most recent play. */
  lastPlayedAtUtc: string;
  /** Cleaned track name (rendered bold on its own line). */
  trackName: string;
  /** Artist display name (second line, subdued). */
  artistName: string;
  /** Playlist name the track played from, or null (second line, italic). */
  playlistName: string | null;
  /** ELO rating (1500 baseline when unrated). */
  rating: number;
  /** Distinct plays in the last 30 days. */
  playcountLast30: number;
  /** Total distinct plays. */
  playcountTotal: number;
}

/**
 * Scale anchors computed over the CURRENT result set (FR-7), used to render
 * the rating and play-count bars relative to the loaded rows.
 */
export interface RecentPlaysScale {
   /**
    * Lowest rating present in the returned set. Rows are rated values or the
    * 1500 baseline; a rated row may sit below 1500, so this can be < 1500.
    */
  minRating: number;
  /** Highest rating present in the returned set. */
  maxRating: number;
  /** Max playcount_last_30 in the returned set. */
  maxPlaycountLast30: number;
  /** Max playcount_total in the returned set. */
  maxPlaycountTotal: number;
}

/** Response for GET /api/music/recent-plays?limit=. */
export interface RecentPlaysResponse {
  /** The most-recent-first rows (count === the validated limit). */
  plays: RecentPlayRow[];
  /** Per-set scale bounds for the returned rows. */
  scale: RecentPlaysScale;
}

// ---------------------------------------------------------------------------
// Service status / landing
// ---------------------------------------------------------------------------

/** Response for GET /api/music/service-status (FR-9 signal). */
export interface ServiceStatus {
  spotify: {
    /** True while the Spotify service is rate-limited. */
    rateLimited: boolean;
    /** ISO timestamp when the rate-limit is expected to clear, or null. */
    rateLimitClearedUtc: string | null;
  };
}

/** Response for GET /api/music/ratings/eligible-count (FR-10 landing). */
export interface MusicRatingsEligibleCountResponse {
  count: number;
}

/** Response for GET /api/music/ratings (eligible-playlists chooser, FR-2).
 *  `data` maps playlist_name (count-in-label, e.g. "Car (5)") -> playlist_id. */
export interface MusicRatingsEligiblePlaylistsResponse {
  data: Record<string, string>;
  count: number;
}

// ---------------------------------------------------------------------------
// Track Ratings — Matchup (008-004)
// ---------------------------------------------------------------------------

/** One side of a rating matchup. */
export interface MatchupTrack {
  isrc: string;
  playlistId: string;
  playlistName: string;
  trackName: string;
  artistName: string;
  albumId: string;
  albumArtUrl: string | null;
  score: number;
}

/** Response for GET /api/music/ratings/matchup. `primary` is null when no
 *  rateable tracks exist. `challenger` is null when the playlist has only
 *  one track total (no challenger exists). */
export interface MatchupResponse {
  primary: MatchupTrack | null;
  challenger: MatchupTrack | null;
}

/** Request query for POST /api/music/ratings/matchup/score. */
export interface ScoreRequest {
  playlist_id: string;
  isrc: string;
  isrc_vs: string;
  /** -5..-1 or +1..+5 (no zero/draw). Positive = primary wins. */
  margin: number;
}

/** Response for POST /api/music/ratings/matchup/score. */
export interface ScoreResponse {
  ok: boolean;
  next: MatchupResponse | null;
  scores: {
    ok: boolean;
    isrc: string;
    isrcVs: string;
    homeNewElo: number;
    awayNewElo: number;
  };
}

// ---------------------------------------------------------------------------
// Playlist Shuffle (008-003)
// ---------------------------------------------------------------------------

/** One row of the selection grid (GET /api/music/shuffle/playlists). */
export interface ShufflePlaylistRow {
  playlist_id: string;
  playlist_name: string;
  track_count: number | null;
  ratings_weight: number | null;
  recency_weight: number | null;
  randomness_weight: number | null;
  minutes_to_sync: number | null;
  playlist_type: string | null;
}

/** Playlist type filter options (derived from music.vw_playlist_config.playlist_type). */
export type PlaylistTypeOption = 'Parents' | 'Seeds' | 'Other';

/** Response for GET /api/music/shuffle/playlists. */
export interface ShufflePlaylistsResponse {
  data: ShufflePlaylistRow[];
  count: number;
}

/** Saved tuning config for a selected playlist (GET /api/music/shuffle). */
export interface ShuffleConfig {
  ratingsWeight: number;
  recencyWeight: number;
  randomWeight: number;
  minutesToSync: number;
  autoShuffle: boolean;
  manualShuffle: boolean;
  makeRecs: boolean;
  seedsOnly: boolean;
}

/** Raw per-track stats row for a selected playlist (GET /api/music/shuffle). */
export interface ShuffleTrackRow {
  playlist_id: string;
  target_playlist_id: string;
  isrc: string;
  track_id: string;
  track_artist: string;
  duration_s: number;
  recency_pct: number;
  ratings_pct: number;
  random_pct: number;
}

/** Response for GET /api/music/shuffle?playlist_id= (FR-1/FR-2). */
export interface ShuffleData {
  playlistId: string;
  targetPlaylistId: string | null;
  config: ShuffleConfig;
  rows: ShuffleTrackRow[];
  count: number;
}

/** Body for POST /api/music/shuffle/preview and /shuffle/config. */
export interface ShuffleConfigBody {
  ratingsWeight: number;
  recencyWeight: number;
  randomWeight: number;
  minutesToSync: number;
}

/** Body for POST /api/music/shuffle/flags (boolean checkbox reconcile). */
export interface ShuffleFlagsBody {
  autoShuffle: boolean;
  manualShuffle: boolean;
  makeRecs: boolean;
  seedsOnly: boolean;
}

/** One ordered preview row (POST /api/music/shuffle/preview). Fields
 *  `isrc`, `trackId`, `targetPlaylistId` are carried but hidden in the UI. */
export interface ShufflePreviewRow {
  newPosition: number;
  trackArtist: string | null;
  recency_pct: number | null;
  ratings_pct: number | null;
  random_pct: number | null;
  duration_s: number | null;
  durationBarMax: number;
  isrc: string | null;
  trackId: string | null;
  targetPlaylistId: string | null;
}

/** Response for POST /api/music/shuffle/preview. */
export interface ShufflePreviewResponse {
  rows: ShufflePreviewRow[];
  count: number;
}

/** Request body for POST /api/music/shuffle/send (FR-5/FR-6). */
export interface ShuffleSendRequest {
  playlistId: string;
  ratingsWeight: number;
  recencyWeight: number;
  randomWeight: number;
  minutesToSync: number;
}

/** Response for POST /api/music/shuffle/send. */
export interface ShuffleSendResponse {
  ok: boolean;
  message: string;
}