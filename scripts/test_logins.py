"""
PiFitness Login Test Suite — Phase 5
=====================================

Independent, easy-to-use tests for Spotify and Garmin login health.
Can be run on both Windows (dev) and Raspberry Pi (production).

Usage:
    python scripts/test_logins.py              # Run all tests
    python scripts/test_logins.py --spotify     # Spotify only
    python scripts/test_logins.py --garmin      # Garmin only
    python scripts/test_logins.py --status      # Quick status check only
    python scripts/test_logins.py --verbose     # Detailed output

Exit codes:
    0 = All tests passed
    1 = Some tests failed
    2 = Fatal error (can't run tests)
"""

import argparse
import sys
import os
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Test Result Tracking
# ---------------------------------------------------------------------------

class TestResults:
    def __init__(self):
        self.passed = []
        self.failed = []
        self.skipped = []
        self.warnings = []

    def add_pass(self, test_name, detail=""):
        self.passed.append((test_name, detail))
        print(f"  ✅ PASS: {test_name}")

    def add_fail(self, test_name, detail=""):
        self.failed.append((test_name, detail))
        print(f"  ❌ FAIL: {test_name} — {detail}")

    def add_skip(self, test_name, reason=""):
        self.skipped.append((test_name, reason))
        print(f"  ⏭️  SKIP: {test_name} — {reason}")

    def add_warning(self, test_name, detail=""):
        self.warnings.append((test_name, detail))
        print(f"  ⚠️  WARN: {test_name} — {detail}")

    def summary(self):
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        print(f"  ✅ Passed:  {len(self.passed)}")
        print(f"  ❌ Failed:  {len(self.failed)}")
        print(f"  ⏭️  Skipped: {len(self.skipped)}")
        print(f"  ⚠️  Warnings: {len(self.warnings)}")
        print("=" * 60)

        if self.failed:
            print("\nFAILED TESTS:")
            for name, detail in self.failed:
                print(f"  ❌ {name}: {detail}")

        if self.warnings:
            print("\nWARNINGS:")
            for name, detail in self.warnings:
                print(f"  ⚠️  {name}: {detail}")

        return len(self.failed) == 0


# ---------------------------------------------------------------------------
# Database Helpers
# ---------------------------------------------------------------------------

def db_query(sql, params=None):
    """Run a SQL query and return results as list of dicts."""
    from backend_functions.database_functions import sql_to_dict, one_sql_result
    try:
        if sql.strip().upper().startswith("SELECT") and "COUNT" not in sql.upper() and "MAX(" not in sql.upper():
            return sql_to_dict(sql, params) if params else sql_to_dict(sql)
        else:
            return one_sql_result(sql, params) if params else one_sql_result(sql)
    except Exception as e:
        return f"DB_ERROR: {e}"


def get_last_spotify_login_event():
    """Return the most recent Spotify login event from api_logins."""
    from backend_functions.database_functions import sql_to_dict
    sql = """SELECT event_time_utc, event_name, error_text 
             FROM logging.api_logins 
             WHERE api_service_name = 'Spotify' 
             ORDER BY event_time_utc DESC LIMIT 1"""
    rows = sql_to_dict(sql)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Test: Database Connectivity
# ---------------------------------------------------------------------------

def test_db_connectivity(results):
    """Verify we can connect to the database."""
    print("\n--- Database Connectivity ---")
    try:
        from backend_functions.database_functions import get_conn
        conn = get_conn()
        conn.close()
        results.add_pass("Database connection", "Connected successfully")
    except Exception as e:
        results.add_fail("Database connection", str(e))


# ---------------------------------------------------------------------------
# Test: Spotify Login Status
# ---------------------------------------------------------------------------

def test_spotify_rate_limit_status(results):
    """Check if Spotify is currently under rate limitations."""
    print("\n--- Spotify Rate Limit Status ---")
    try:
        from backend_functions.service_logins import sql_rate_limited
        is_limited = sql_rate_limited()
        if is_limited:
            sql = "SELECT rate_limit_cleared_utc FROM api_services.api_service_list WHERE api_service_name = 'Spotify'"
            from backend_functions.database_functions import one_sql_result
            cleared_utc = one_sql_result(sql)
            results.add_warning("Rate limit active", f"Cleared at: {cleared_utc}")
        else:
            results.add_pass("Rate limit status", "Not currently rate limited")
    except Exception as e:
        results.add_fail("Rate limit status check", str(e))


def test_spotify_last_login(results):
    """Check when Spotify was last successfully logged in."""
    print("\n--- Spotify Last Login ---")
    try:
        from backend_functions.database_functions import sql_to_dict
        sql = """SELECT event_time_utc, event_name, error_text 
                 FROM logging.api_logins 
                 WHERE api_service_name = 'Spotify' 
                 ORDER BY event_time_utc DESC LIMIT 5"""
        rows = sql_to_dict(sql)
        if rows:
            print(f"  Last 5 Spotify login events:")
            for r in rows:
                err = f" — ERROR: {r['error_text']}" if r.get('error_text') else ""
                print(f"    {r['event_time_utc']}: {r['event_name']}{err}")
            results.add_pass("Last login query", f"Most recent: {rows[0]['event_time_utc']}")
        else:
            results.add_warning("Last login query", "No login events found in api_logins")
    except Exception as e:
        results.add_fail("Last login query", str(e))


def test_spotify_token_acquisition(results, verbose=False):
    """Attempt to acquire a Spotify token and report result.
    
    When --verbose is used, this test demonstrates the 'reuse-first-then-reacquire'
    methodology step by step: it checks whether the token was reused from the DB,
    refreshed via a refresh token, or freshly acquired via full OAuth.
    """
    print("\n--- Spotify Token Acquisition ---")
    try:
        from backend_functions.service_logins import get_spotify_token, load_token_from_db
        from backend_functions.database_functions import sql_to_dict

        # Step 1: Check if a token exists in the DB
        db_token = load_token_from_db('Spotify')
        if verbose:
            if db_token:
                has_refresh = "refresh_token" in db_token
                has_access = "access_token" in db_token
                print(f"    [VERBOSE] Step 1: Checking DB for stored token... Found!")
                print(f"    [VERBOSE]   - access_token present: {has_access}")
                print(f"    [VERBOSE]   - refresh_token present: {has_refresh}")
            else:
                print(f"    [VERBOSE] Step 1: Checking DB for stored token... Not found")

        # Step 2: Acquire token (this will try DB reuse first, then fall through)
        token = get_spotify_token()
        if token and token.get("token"):
            token_preview = token["token"][:20] + "..."
            results.add_pass("Token acquisition", f"Token obtained: {token_preview}")
            if verbose:
                print(f"    [VERBOSE] Step 2: get_spotify_token() returned:")
                print(f"    [VERBOSE]   - token: {token_preview}")
                print(f"    [VERBOSE]   - token_age: {time.time() - token['token_age']:.0f}s ago (since validation/creation)")
                print(f"    [VERBOSE]   - client present: {token.get('client') is not None}")

            # Step 3: Determine what actually happened by checking the log
            if verbose:
                print(f"    [VERBOSE] Step 3: Checking api_logins to determine what get_spotify_token() did...")
                last_event = get_last_spotify_login_event()
                if last_event:
                    event_name = last_event['event_name']
                    event_time = last_event['event_time_utc']
                    print(f"    [VERBOSE]   Most recent login event: '{event_name}' at {event_time}")
                    if event_name == 'Token reuse from DB':
                        print(f"    [VERBOSE]   ✅ Token was REUSED from database (no OAuth call needed)")
                    elif 'refreshed' in event_name.lower():
                        print(f"    [VERBOSE]   ✅ Token was REFRESHED via stored refresh token (no full OAuth)")
                    elif 'New Token' in event_name:
                        print(f"    [VERBOSE]   ⚠️  Token was FRESHLY ACQUIRED via OAuth (reuse path was unavailable)")
                    else:
                        print(f"    [VERBOSE]   ℹ️  Event: {event_name}")
                else:
                    print(f"    [VERBOSE]   No login events found in api_logins")
        else:
            results.add_fail("Token acquisition", "get_spotify_token() returned None or empty token")
    except Exception as e:
        results.add_fail("Token acquisition", f"Exception: {e}")


def test_spotify_token_validation(results):
    """Test token reuse logic by calling get_spotify_client with a valid token."""
    print("\n--- Spotify Token Validation ---")
    try:
        from backend_functions.service_logins import get_spotify_client, get_spotify_token
        # First get a fresh token
        fresh_token = get_spotify_token()
        if not fresh_token:
            results.add_skip("Token validation", "Cannot test — no fresh token available")
            return

        # Now pass it to get_spotify_client for validation/reuse
        result = get_spotify_client(fresh_token)
        if result and result.get("client"):
            results.add_pass("Token validation", "Client validated and returned")
        elif result and result.get("token"):
            results.add_pass("Token validation", "Token returned (no client — expected for Spotify)")
        else:
            results.add_fail("Token validation", "get_spotify_client() returned invalid result")
    except Exception as e:
        results.add_fail("Token validation", f"Exception: {e}")


def test_spotify_reuse_demonstration(results, verbose=False):
    """Demonstrate the 'reuse-first-then-reacquire' methodology by calling
    get_spotify_token() twice in sequence, showing that the second call
    reuses the token saved by the first call."""
    print("\n--- Spotify Reuse Methodology Demonstration ---")
    if not verbose:
        print("  (Use --verbose for detailed step-by-step demonstration)")
        results.add_skip("Reuse demo", "Skipped — use --verbose to enable")
        return

    try:
        from backend_functions.service_logins import get_spotify_token, load_token_from_db
        from backend_functions.database_functions import sql_to_dict

        print("  ╔══════════════════════════════════════════════════════════╗")
        print("  ║     TOKEN REUSE METHODOLOGY DEMONSTRATION               ║")
        print("  ║                                                        ║")
        print("  ║  Methodology:                                           ║")
        print("  ║  1. Check DB for stored token (reuse if valid)          ║")
        print("  ║  2. If valid, try to refresh via refresh_token          ║")
        print("  ║  3. Only fall through to full OAuth if 1 & 2 fail       ║")
        print("  ╚══════════════════════════════════════════════════════════╝")
        print()

        # --- Call 1 ---
        print("  ─── Call #1: get_spotify_token() ───")
        db_token_before = load_token_from_db('Spotify')
        if db_token_before:
            print(f"    DB token found: access_token={'Yes' if 'access_token' in db_token_before else 'No'}, "
                  f"refresh_token={'Yes' if 'refresh_token' in db_token_before else 'No'}")
        else:
            print("    DB token: Not found")

        last_event_before = get_last_spotify_login_event()
        print(f"    Last login event before call: {last_event_before['event_name'] if last_event_before else 'None'}")

        token1 = get_spotify_token()

        last_event_after = get_last_spotify_login_event()
        print(f"    Last login event AFTER  call: {last_event_after['event_name'] if last_event_after else 'None'}")
        if token1:
            print(f"    Result: Token {'acquired' if token1.get('token') else 'FAILED'}, "
                  f"age={time.time() - token1['token_age']:.0f}s")
            # Interpret
            event_name = last_event_after['event_name'] if last_event_after else ''
            if event_name == 'Token reuse from DB':
                print(f"    ➡️  REUSE: Token was reused from database — no OAuth call needed")
            elif 'refreshed via DB' in event_name:
                print(f"    ➡️  REFRESH: Token was refreshed via stored refresh token — quick, no user interaction")
            elif 'New Token' in event_name or 'login with New Token' in event_name:
                print(f"    ➡️  ACQUIRE: Token was freshly acquired via OAuth (reuse/refresh paths unavailable)")
            else:
                print(f"    ➡️  METHOD: {event_name}")
        else:
            print(f"    Result: get_spotify_token() returned None")
            results.add_warning("Reuse demo call #1", "First token acquisition returned None")
            return
        print()

        # --- Call 2 (should reuse) ---
        print("  ─── Call #2: get_spotify_token() (should reuse from DB) ───")

        last_event_before2 = get_last_spotify_login_event()

        token2 = get_spotify_token()

        last_event_after2 = get_last_spotify_login_event()
        print(f"    Last login event BEFORE call #2: {last_event_before2['event_name'] if last_event_before2 else 'None'}")
        print(f"    Last login event AFTER  call #2: {last_event_after2['event_name'] if last_event_after2 else 'None'}")

        if token2:
            event_name2 = last_event_after2['event_name'] if last_event_after2 else ''
            print(f"    Result: Token {'acquired' if token2.get('token') else 'FAILED'}")
            
            if event_name2 == 'Token reuse from DB':
                print(f"    ✅ VERIFIED: Second call reused token from database")
                print()
                print(f"  ╔══════════════════════════════════════════════════════════╗")
                print(f"  ║  ✅  REUSE-FIRST METHODOLOGY CONFIRMED                  ║")
                print(f"  ║                                                        ║")
                print(f"  ║  The second call to get_spotify_token() successfully    ║")
                print(f"  ║  reused the DB-stored token without making a new        ║")
                print(f"  ║  OAuth request.                                         ║")
                print(f"  ╚══════════════════════════════════════════════════════════╝")
                results.add_pass("Reuse methodology", "Second call reused token from DB (confirmed via api_logins)")
            elif 'refreshed' in event_name2.lower():
                print(f"    ⚠️  VERIFIED: Second call refreshed via DB refresh token")
                print(f"    (This is still reuse-first — no full OAuth was needed)")
                results.add_pass("Reuse methodology", "Second call refreshed via refresh token (no full OAuth)")
            else:
                print(f"    ⚠️  Note: event_name='{event_name2}' — unexpected for call #2")
                results.add_warning("Reuse methodology", f"Call #2 resulted in {event_name2}")
        else:
            print(f"    Result: get_spotify_token() returned None")
            results.add_fail("Reuse demo call #2", "Second token acquisition returned None")
        
        print()

    except Exception as e:
        results.add_fail("Reuse demonstration", f"Exception: {e}")
        import traceback
        if verbose:
            traceback.print_exc()


def test_spotify_rate_limit_test(results):
    """Run the rate_limit_test to check if Spotify API is responsive."""
    print("\n--- Spotify Rate Limit Test ---")
    try:
        from backend_functions.service_logins import rate_limit_test
        is_limited, seconds = rate_limit_test()
        if is_limited:
            results.add_warning("Rate limit test", f"Rate limited! Retry-After: {seconds}s")
        else:
            results.add_pass("Rate limit test", "API responded — not rate limited")
    except Exception as e:
        results.add_fail("Rate limit test", f"Exception: {e}")


# ---------------------------------------------------------------------------
# Test: Garmin Login Status
# ---------------------------------------------------------------------------

def test_garmin_last_login(results):
    """Check when Garmin was last successfully logged in."""
    print("\n--- Garmin Last Login ---")
    try:
        from backend_functions.database_functions import sql_to_dict
        sql = """SELECT event_time_utc, event_name, error_text 
                 FROM logging.api_logins 
                 WHERE api_service_name = 'Garmin' 
                 ORDER BY event_time_utc DESC LIMIT 5"""
        rows = sql_to_dict(sql)
        if rows:
            print(f"  Last 5 Garmin login events:")
            for r in rows:
                err = f" — ERROR: {r['error_text']}" if r.get('error_text') else ""
                print(f"    {r['event_time_utc']}: {r['event_name']}{err}")
            results.add_pass("Last login query", f"Most recent: {rows[0]['event_time_utc']}")
        else:
            results.add_warning("Last login query", "No login events found in api_logins")
    except Exception as e:
        results.add_fail("Last login query", str(e))


def test_garmin_credentials(results):
    """Check if Garmin credentials are available in the database."""
    print("\n--- Garmin Credentials ---")
    try:
        from backend_functions.service_logins import garmin_creds
        email, password = garmin_creds()
        if email and password:
            masked_email = email[:3] + "***@" + email.split("@")[1] if "@" in email else "***"
            results.add_pass("Credentials available", f"Email: {masked_email}")
        else:
            results.add_fail("Credentials available", "Email or password is None")
    except Exception as e:
        results.add_fail("Credentials available", str(e))


def test_garmin_pirate_path(results):
    """Check if the pirate-garmin_clone source path exists and is importable."""
    print("\n--- Pirate-Garmin Path Check ---")
    project_root = Path(__file__).parent.parent
    pirate_src_path = project_root / "pirate-garmin_clone" / "src"

    if pirate_src_path.exists():
        results.add_pass("Source path exists", str(pirate_src_path))
    else:
        results.add_fail("Source path exists", f"Not found: {pirate_src_path}")
        return

    # Try importing
    try:
        sys.path.insert(0, str(pirate_src_path))
        from pirate_garmin.cli import app
        results.add_pass("Pirate-garmin import", "Successfully imported pirate_garmin.cli.app")
    except Exception as e:
        results.add_fail("Pirate-garmin import", str(e))


def test_garmin_token_cache(results):
    """Check if Garmin token cache exists and when it was last modified."""
    print("\n--- Garmin Token Cache ---")
    try:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        cache_loc = os.getenv("LOCAL_STORAGE_PATH")
        if not cache_loc:
            results.add_skip("Token cache", "LOCAL_STORAGE_PATH not set in .env")
            return

        cache_path = Path(cache_loc)
        if not cache_path.exists():
            results.add_warning("Token cache", f"Cache directory not found: {cache_loc}")
            return

        # Check for garmin-related files in the cache directory
        garmin_files = list(cache_path.glob("*garmin*")) + list(cache_path.glob("*.garmin*"))
        if garmin_files:
            for f in garmin_files:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
                print(f"    {f.name}: modified {mtime.isoformat()} ({age_hours:.1f}h ago)")
            results.add_pass("Token cache", f"{len(garmin_files)} Garmin cache file(s) found")
        else:
            results.add_warning("Token cache", "No Garmin cache files found in LOCAL_STORAGE_PATH")
    except Exception as e:
        results.add_fail("Token cache", str(e))


def test_garmin_pirate_login_dry_run(results, verbose=False):
    """
    Dry-run test: check if pirate-garmin login CLI can be invoked.
    Does NOT actually log in — just verifies the CLI is reachable.
    """
    print("\n--- Pirate-Garmin CLI Dry Run ---")
    try:
        project_root = Path(__file__).parent.parent
        pirate_src_path = project_root / "pirate-garmin_clone" / "src"
        sys.path.insert(0, str(pirate_src_path))

        from pirate_garmin.cli import app
        from typer.testing import CliRunner

        runner = CliRunner()
        # Use --help to verify CLI is functional without actually logging in
        result = runner.invoke(app, ["--help"])

        if result.exit_code == 0:
            results.add_pass("CLI reachable", "pirate-garmin --help returned exit code 0")
            if verbose:
                print(f"    Output preview: {result.stdout[:200]}")
        else:
            results.add_fail("CLI reachable", f"Exit code: {result.exit_code}, Error: {result.stderr}")
    except Exception as e:
        results.add_fail("CLI dry run", str(e))


def test_token_expiration_tracking(results, verbose=False):
    """Test that token expiration values are being tracked correctly.
    
    For Spotify: Checks that _migration.spotify_auth_metadata has expiry data.
    For Garmin: Checks that _migration.api_tokens has refresh_token_expires_at_utc.
    """
    print("\n--- Token Expiration Tracking ---")
    try:
        from backend_functions.service_logins import get_auth_status
        
        status = get_auth_status()
        
        # Check Spotify expiry
        spotify_expires = status.get("services", {}).get("Spotify", {}).get("token_expires_utc")
        if spotify_expires:
            try:
                expires_dt = datetime.fromisoformat(spotify_expires.replace("Z", "+00:00"))
                days_until_expiry = (expires_dt - datetime.now(timezone.utc)).days
                if days_until_expiry < 0:
                    results.add_warning("Spotify expiry", f"Token expired {abs(days_until_expiry)} days ago")
                else:
                    results.add_pass("Spotify expiry", f"{days_until_expiry} days until refresh token expiry (6 months)")
            except Exception as e:
                results.add_warning("Spotify expiry parse", str(e))
        else:
            results.add_warning("Spotify expiry", "No expiry data found in _migration.spotify_auth_metadata")
        
        # Check Garmin expiry
        garmin_expires = status.get("services", {}).get("Garmin", {}).get("token_expires_utc")
        if garmin_expires:
            try:
                expires_dt = datetime.fromisoformat(garmin_expires.replace("Z", "+00:00"))
                days_until_expiry = (expires_dt - datetime.now(timezone.utc)).days
                if days_until_expiry < 0:
                    results.add_warning("Garmin expiry", f"Token expired {abs(days_until_expiry)} days ago")
                else:
                    results.add_pass("Garmin expiry", f"{days_until_expiry} days until refresh token expiry (from native-oauth2)")
            except Exception as e:
                results.add_warning("Garmin expiry parse", str(e))
        else:
            results.add_warning("Garmin expiry", "No expiry data found in _migration.api_tokens")
    except Exception as e:
        results.add_fail("Token expiration tracking", str(e))


# ---------------------------------------------------------------------------
# Test: Task Executioner Login Dispatch
# ---------------------------------------------------------------------------

def test_task_login_configuration(results):
    """Verify that all tasks have valid login function configurations."""
    print("\n--- Task Login Configuration ---")
    try:
        from backend_functions.database_functions import sql_to_dict
        sql = """SELECT task_id, task_name, api_service_name, python_login_function
                 FROM tasks.vw_task_info
                 WHERE run_extract = true
                 ORDER BY api_service_name, task_id"""
        tasks = sql_to_dict(sql)

        issues = []
        for t in tasks:
            login_func = t.get('python_login_function')
            if not login_func:
                issues.append(f"Task #{t['task_id']} ({t['task_name']}): missing python_login_function")
            elif '.' not in login_func:
                issues.append(f"Task #{t['task_id']} ({t['task_name']}): invalid format: {login_func}")

        if issues:
            for i in issues:
                results.add_warning("Task config", i)
        else:
            results.add_pass("Task config", f"All {len(tasks)} extract tasks have valid login functions")
    except Exception as e:
        results.add_fail("Task config check", str(e))


# ---------------------------------------------------------------------------
# Main Test Runner
# ---------------------------------------------------------------------------

def run_all_tests(args):
    results = TestResults()

    print("=" * 60)
    print("PiFitness Login Test Suite")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # Always test DB connectivity first
    test_db_connectivity(results)

    # --- Spotify Tests ---
    if args.spotify or args.all:
        print("\n" + "=" * 60)
        print("SPOTIFY TESTS")
        print("=" * 60)
        test_spotify_rate_limit_status(results)
        test_spotify_last_login(results)
        test_spotify_token_acquisition(results, args.verbose)
        test_spotify_token_validation(results)
        test_spotify_reuse_demonstration(results, args.verbose)
        if not args.status:
            test_spotify_rate_limit_test(results)

    # --- Garmin Tests ---
    if args.garmin or args.all:
        print("\n" + "=" * 60)
        print("GARMIN TESTS")
        print("=" * 60)
        test_garmin_last_login(results)
        test_garmin_credentials(results)
        test_garmin_pirate_path(results)
        test_garmin_token_cache(results)
        if not args.status:
            test_garmin_pirate_login_dry_run(results, args.verbose)

# --- Cross-cutting Tests ---
    if args.all or args.status:
        print("\n" + "=" * 60)
        print("CROSS-CUTTING TESTS")
        print("=" * 60)
        test_task_login_configuration(results)
        test_token_expiration_tracking(results, args.verbose)

    # --- Summary ---
    success = results.summary()
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(
        description="PiFitness Login Test Suite — Test Spotify & Garmin login health"
    )
    parser.add_argument("--spotify", action="store_true", help="Run Spotify tests only")
    parser.add_argument("--garmin", action="store_true", help="Run Garmin tests only")
    parser.add_argument("--status", action="store_true", help="Quick status check only (no API calls)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Detailed output")
    parser.add_argument("--all", action="store_true", help="Run all tests (default)")

    args = parser.parse_args()

    # Default: run all if no specific service selected
    if not args.spotify and not args.garmin:
        args.all = True

    sys.exit(run_all_tests(args))


if __name__ == "__main__":
    main()