# Garmin Login Flow — Actual Implementation

## TL;DR

Every Garmin login path runs through the same three-layer protection to
prevent over-login (Garmin's SSO will 429 after too many auth attempts):

1. **DB session cache** (`api_services.api_tokens`) — cross-process, cross-machine
2. **HTTP token refresh** — pure HTTP, no browser
3. **In-memory client reuse** (300s window) — process-local

Fresh Playwright login only fires when all three are exhausted (typically:
DI and IT refresh tokens both expired — usually days to weeks after last
successful login).

## Entry points

- **`garmin_login(headless=False)`** in `provider.py:139` — the
  primary entry point. Called by `get_garmin_client()` and by
  `GarminProvider.refresh()`.
- **`get_garmin_client(incoming_token=None)`** in `provider.py:90` —
  higher-level wrapper. Returns a `{"client": GarminClient, "token": str,
  "token_age": float}` dict.
- **`GarminClient.request_json(host, path, params)`** in `client.py` —
  the actual API call path. Uses `auth.ensure_authenticated()` on every
  request, which re-runs the three-layer protection.

## Session lifecycle

### Layer 1 — DB session cache (`api_services.api_tokens`)

`garmin_login()` at `provider.py`:

1. Load email/password via `garmin_creds()` → `load_api_credentials('Garmin')`
2. Load previous session via `load_token_from_db('Garmin')` (`provider.py:166`)
3. Write session to local cache file at
   `{LOCAL_STORAGE_PATH}/garmin/native-oauth2.json` (`provider.py:170`)
4. Call `GarminClient.from_credentials(...).auth.ensure_authenticated()` (`:190`)
5. Save refreshed/new session back via `save_token_to_db('Garmin', ...)` (`:194`)

**This is why any machine that ever logged in shares its session with every
other machine — including the headless Pi.** Without this layer, every task
run on every machine would trigger a Playwright login and Garmin would 429.

### Layer 2 — HTTP token refresh (no browser)

Inside `AuthManager.ensure_authenticated()` (in `auth.py`):

- Valid DI access token → return session as-is
- DI access expired but DI refresh valid → `refresh_di_token_slot()`
- IT access expired but IT refresh valid → `refresh_it_token_slot()`
- Both expired → `create_browser_session()` (Layer 3)

Both `refresh_di_token_slot` and `refresh_it_token_slot` are pure `httpx`
POST calls. **No Playwright required as long as refresh tokens are valid.**

Refresh token lifetimes (from Garmin's DI/IT responses, typically):
- `expires_in`: 3600s (access token)
- `refresh_token_expires_in`: 86400s (refresh token) — 24h

The `save_token_to_db` code extracts the **earlier** of DI/IT refresh
expiry and stores it in `refresh_token_expires_at_utc` — so monitoring
can alert before both are dead.

### Layer 3 — Fresh Playwright login

Inside `AuthManager.create_browser_session()` (`auth.py:360`):

1. `require_credentials()` — raise `MissingCredentialsError` if none
2. **`login_via_browser(...)`** (Playwright) — submits credentials to
   Garmin SSO, captures the `serviceTicketId` from the login API response
3. HTTP POST to `DI_TOKEN_URL` with service ticket → DI token
4. HTTP POST to `IT_TOKEN_URL` with DI access token → IT token
5. Return `NativeOAuth2Session(di, it)`

**`create_native_session` was misnamed.** The old name said "native" but
the code uses Playwright. Renamed to `create_browser_session` in INT-17.
## DB cache population

The `api_services.api_tokens` table is populated by `save_token_to_db`
after a successful auth flow. `garmin_login()` writes to it after
`AuthManager.ensure_authenticated()` completes (provider.py:177). This is
what lets other machines pick up the same session without re-authenticating.

**The INT-13-live test does not populate the DB cache.** The test at
`tests/test_garmin_login_flow.py::test_real_garmin_login_returns_usable_api_response`
calls `AuthManager.create_browser_session()` directly, which exercises the
Playwright login path but bypasses the surrounding `garmin_login()` wrapper
that performs the DB write. A fresh Garmin session will be absent from
`api_services.api_tokens` until a production entry point completes.

**Silent-failure contract.** `save_token_to_db` raises `RuntimeError` if the
underlying `qec` call returns an error list (INT-19b). Before INT-19b the
failure was silent, which is why an empty token table went unnoticed across
several blocks.
