import importlib
import os
import time

import requests
import spotipy
from dotenv import load_dotenv
from garminconnect import Garmin, GarminConnectAuthenticationError, GarminConnectTooManyRequestsError, \
    GarminConnectConnectionError
from spotipy import SpotifyOAuth, SpotifyException
from pathlib import Path
from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import one_sql_result, get_conn, qec
from backend_functions.logging_functions import log_api_event, log_app_event, start_timer, elapsed_ms

load_dotenv()


def load_api_credentials(service=None):
    # loads and decrypts credentials for a specific service
    if not service:
        print('No service provided')
        return None

    t_sql = f"""
        SELECT api_credentials FROM api_services.credentials 
        WHERE api_service_name = '{service}';"""

    result = one_sql_result(t_sql)

    if result:
        return decrypt_dict(result)
    else:
        print(f'No results returned for service: {service}')
        return None


def spotify_creds():
    creds = load_api_credentials('Spotify')
    if 'client_id' in creds:
        cid = creds['client_id']
    else:
        cid = None

    if 'client_secret' in creds:
        csec = creds['client_secret']
    else:
        csec = None

    if 'redirect_uri' in creds:
        uri = creds['redirect_uri']
    else:
        uri = None
    return cid, csec, uri


def garmin_creds():
    # Loads the decrypted garmin credentials from the database and returns email & password
    creds = load_api_credentials('Garmin')
    if 'email' in creds:
        e = creds['email']
    else:
        e= None

    if 'password' in creds:
        p = creds['password']
    else:
        p = None
    return e, p


def get_spotify_client(incoming_token=None):
    # tests validity of incoming token and returns a client & token
    if incoming_token is None:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token, no preexisting provided', token_age=0)
        return new_spotify_token

    if "token" not in incoming_token:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from malformed dictionary (token)', token_age=0)
        return new_spotify_token

    if "token_age" not in incoming_token:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from malformed dictionary (token age)', token_age=0)
        return new_spotify_token

    if incoming_token.get("client") is None:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from missing/none client', token_age=0)
        return new_spotify_token


    token_age = time.time() - incoming_token["token_age"]
    max_age = 1800 # seconds
    if token_age > max_age: #test token validity
        try:
            cl = incoming_token.get("client")
            cl.current_user()
            log_api_event('Spotify', 'Token reuse: client still active', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_spotify_token=spotify_rate_limit_detection(log_msg=f'New Token from expired: {e}', token_age=token_age)
            return new_spotify_token
    else:
        try:
            cl = incoming_token.get("client")
            cl.current_user()
            log_api_event('Spotify', 'Token reuse: client still active', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_spotify_token=spotify_rate_limit_detection(log_msg=f'New Token, non-timing error: {e}', token_age=token_age)
            return new_spotify_token



def sql_rate_limited():
    test_sql = """SELECT COALESCE(CURRENT_TIMESTAMP < rate_limit_cleared_utc, 1=0) as rate_limited
                FROM api_services.api_service_list
                WHERE api_service_name = 'Spotify'"""
    return one_sql_result(test_sql)

def log_rate_limitation():
    log_api_event(service='Spotify', event='Under rate limitations', token_age=0)
    return

def spotify_rate_limit_detection(log_msg, token_age):
    test_sql = """SELECT COALESCE(CURRENT_TIMESTAMP < rate_limit_cleared_utc, 1=0) as rate_limited
                FROM api_services.api_service_list
                WHERE api_service_name = 'Spotify'"""
    is_rate_limited = sql_rate_limited()
    new_spotify_token = get_spotify_token()
    # SQL knows I'm already rate-limited
    if is_rate_limited:
        new_spotify_token = insert_client(new_spotify_token, None)
        log_rate_limitation()
        return new_spotify_token

    # Test if any new rate limitations are in effect
    is_rate_limited, sleep_interval = rate_limit_test(new_spotify_token)
    if is_rate_limited:
        new_spotify_token = insert_client(new_spotify_token, None)
        update_sql = f""""UPDATE api_services.api_service_list 
                        SET rate_limit_detected_utc = CURRENT_TIMESTAMP,
                        rate_limit_cleared_utc = CURRENT_TIMESTAMP + Interval '%s seconds'
                        WHERE api_service_name = 'Spotify';
                        """
        params = [sleep_interval,]
        qec(update_sql, params)
        log_api_event(service='Spotify', event='New rate limitations detected', token_age=0)
        return new_spotify_token

    # Otherwise return a new token.
    new_spotify_token = insert_client(new_spotify_token, sp_client(new_spotify_token))
    log_api_event(service='Spotify', event=log_msg, token_age=token_age)
    return new_spotify_token
    return

def sp_client(t):
    c= spotipy.Spotify(
        auth=t["token"],
        retries=0,
        status_retries=0,
        requests_timeout=5,
    )
    return c


def rate_limit_test(sp_token=None):
    # 1. Get the token from your existing client

    # Build token if not passed
    if not sp_token:
        sp_token = get_spotify_token()

    token = sp_token["token"]


    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 2. Make the raw call (using the same endpoint Spotipy uses)
    # Using a fake playlist or a known one. A GET request is safer/cheaper than replace_items.
    playlist_id = '0OGtAcLTRWGdO4S8tuudyD'
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    try:
        # We send an empty list to 'replace'—this is the same as your sp.playlist_replace_items
        # Spotify's API uses PUT for replacing all items.
        response = requests.put(url, headers=headers, json={"uris": []}, timeout=10)

        # 3. Capture the 429 and the Retry-After header
        if response.status_code == 429:
            # requests.headers is case-insensitive
            retry_after = response.headers.get("Retry-After")

            if retry_after:
                return True, int(retry_after)

            # If 429 exists but header is missing, Spotify is being non-compliant.
            return True, 120

            # If 200, 201, or 404/403 (e.g. invalid playlist ID), you are NOT rate limited
        return False, None

    except Exception as e:
        print(f"Network error: {e}")
        return True, 60

def get_spotify_token():
    # Retrieve the Spotify credentials from the database
    # Attempt a login with the appropriate scopes
    # Load the login state to session state
    t0=start_timer()
    client_id, client_secret, redirect_uri = spotify_creds()

    if not all([client_id, client_secret, redirect_uri]):
        log_app_event(cat='API Login Failure', desc="Missing Spotify Credentials", exec_time=elapsed_ms(t0))
        return None

    # Declare the scope
    scope_list = ['user-read-recently-played',
                  'user-library-read',
                  'user-modify-playback-state',
                  'playlist-read-private',
                  'playlist-read-collaborative',
                  'playlist-modify-private',
                  'playlist-modify-public',
                  'playlist-read-private playlist-read-collaborative',
                  "user-library-modify",
                  'user-read-playback-state',
                  'user-read-recently-played']
    scope = ''
    for scope_type in scope_list:
        scope = scope + scope_type + ' '
    scope = scope.strip()

    cache_loc = Path(os.getenv("LOCAL_STORAGE_PATH"))

    # Create the auth manager
    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
        cache_path=os.path.join(cache_loc, ".spotify_cache")
    )

    try:
        token_info = auth_manager.get_access_token(as_dict=True)
        access_token = token_info["access_token"]
        login_time = time.time()
        log_api_event(service='Spotify', event='login with New Token')
        final_token = {"client": None,
                       "token": access_token,
                       "token_age": login_time}
        return final_token
    except Exception as e:
        log_api_event(service='Spotify', event='token acquisition failure', err=e)
        return None


def garmin_login():
    # This function will retrieve garmin credentials and attempt to login.
    # If the credentials do not exist or the login attempt fails, user will be reprompted to enter credentials.

    sql = """SELECT seconds_since_last_event FROM logging.vw_last_login 
            WHERE api_service_name='Garmin'"""
    ll_diff = one_sql_result(sql) or 0

    max_delay = 0
    # Ensure we're not logging in too frequently
    if ll_diff < max_delay:
        print(f"Garmin forced wait of {max_delay - ll_diff} seconds")
        time.sleep(max_delay-ll_diff)

    email, password = garmin_creds()


    if email is None or password is None:
        print('Garmin login aborted due to missing email or password')
        return False

    try:
        client = Garmin(email, password)
        client.login()
        log_api_event(service='Garmin', event='Official login')
        return client
    except GarminConnectAuthenticationError as e:
        log_api_event(service='Garmin', event='login failure, authentication', err=e)
        return None
    except GarminConnectTooManyRequestsError as e:
        log_api_event(service='Garmin', event='login failure, too many requests', err=e)
        return None
    except GarminConnectConnectionError as e:
        log_api_event(service='Garmin', event='login failure, connection', err=e)
        return None
    except Exception as e:
        log_api_event(service='Garmin', event='login failure, uncaught', err=e)
        return None


def get_garmin_client(incoming_token=None):
    # tests validity of incoming token and returns a client & token
    if incoming_token is None:
        new_token = {"client": garmin_login(),
                     "token": None,
                     "token_age": time.time()}
        log_api_event('Garmin', 'New Token from none', token_age=0)
        return new_token

    if "client" not in incoming_token or "token_age" not in incoming_token:
        new_token = {"client": garmin_login(),
                     "token": None,
                     "token_age": time.time()}
        log_api_event('Garmin', 'New Token from malformed dictionary', token_age=0)
        return new_token

    if incoming_token.get("client") is None:
        new_token = {"client": garmin_login(),
                     "token": None,
                     "token_age": time.time()}
        log_api_event('Garmin', 'Client was in token, but was None', token_age=0)
        return new_token

    token_age = time.time() - incoming_token["token_age"]
    max_age = 300  # seconds
    if token_age > max_age:  # test token validity
        try:
            cl = incoming_token.get("client")
            cl.get_full_name()
            log_api_event('Garmin', 'Token reuse: check and pass', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_token = {"client": garmin_login(),
                         "token": None,
                         "token_age": time.time()}
            log_api_event('Garmin', 'New Token from expired', token_age=token_age)
            return new_token
    else:
        log_api_event('Garmin', 'Token reuse: recency skip', token_age=token_age)
        return incoming_token


def insert_client(incoming_dict, client):
    outgoing_dict = {"client": client,
                     "token": incoming_dict.get("token"),
                     "token_age": incoming_dict.get("token_age")}
    return outgoing_dict


def test_login(service_name):
    # Pulls the testable function from database and attempts login
    test_sql = f"""SELECT api_service_function from api_services.api_service_list
                WHERE api_service_name = '{service_name}'; """

    test_str = one_sql_result(test_sql)
    try:
        module_name, test_name = test_str.rsplit('.', 1)
    except Exception as e:
        return 'Service Invalid'
    module = importlib.import_module(module_name)
    svc_function = getattr(module, test_name)
    client = svc_function()
    return client is not None

def get_service_list(append_option=None):
    # Returns the known api services as a list
    sql="SELECT api_service_name from api_services.api_service_list"
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    service_list = [row[0] for row in cursor.fetchall()]
    if append_option:
        service_list.append(append_option)
    return service_list


