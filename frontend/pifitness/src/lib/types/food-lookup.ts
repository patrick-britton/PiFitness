/** Food API raw-JSON lookup contract (010-001, dev-only diagnostic).
 * Backend passes the upstream body through verbatim; the frontend
 * pretty-prints it without extraction, renaming, or filtering.
 */

export interface FoodLookupResponse {
  upstreamStatus: number;
  /** Upstream JSON body verbatim (or {_nonJsonBody} wrapper when upstream was not JSON). */
  body: unknown;
  /** USDA only: X-RateLimit-Remaining header value, in-band (no DB logging). */
  rateLimitRemaining?: string | null;
}

export interface FoodLookupLocalError {
  detail: string;
  status: number;
  type: 'UpstreamError' | 'ValidationError' | 'ConfigError';
}
