import importlib
import os
import time
import spotipy
from dotenv import load_dotenv
from garminconnect import Garmin, GarminConnectAuthenticationError, GarminConnectTooManyRequestsError, \
    GarminConnectConnectionError
from spotipy import SpotifyOAuth
from pathlib import Path
from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import one_sql_result
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
        new_spotify_token = get_spotify_token()
        new_spotify_token = insert_client(new_spotify_token, spotipy.Spotify(auth=new_spotify_token["token"]))
        return new_spotify_token

    if "token" not in incoming_token or "token_time" not in incoming_token:
        new_spotify_token = get_spotify_token()
        new_spotify_token = insert_client(new_spotify_token, spotipy.Spotify(auth=new_spotify_token["token"]))
        return new_spotify_token

    if incoming_token.get("client") is None:
        new_spotify_token = get_spotify_token()
        new_spotify_token = insert_client(new_spotify_token, spotipy.Spotify(auth=new_spotify_token["token"]))
        return new_spotify_token


    token_age = time.time() - incoming_token["token_time"]
    max_age = 1800 # seconds
    if token_age > max_age: #test token validity
        try:
            cl = incoming_token.get("client")
            cl.current_user()
            log_api_event('Spotify', 'token age check passed', token_age=token_age)
            return incoming_token
        except Exception as e:
            # dbf.log_entry(cat="API Login", desc=f"Token invalid @ {round(token_age / 60, 0)}m old.")
            new_spotify_token = get_spotify_token()
            new_spotify_token = insert_client(new_spotify_token, spotipy.Spotify(auth=new_spotify_token["token"]))
            log_api_event('Spotify', 'token age check failed', token_age=token_age)
            return new_spotify_token
    else:
        log_api_event('Spotify', 'token age check bypassed', token_age=token_age)
        return incoming_token


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

    cache_loc = Path(os.getenv("LOCAL_STORAGE_PATH")).read_text().strip()

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
        log_api_event(service='Spotify', event='token acquired')
        final_token = {"client": None,
                       "token": access_token,
                       "token_time": login_time}
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
        log_api_event(service='Garmin', event='login')
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

        return new_token

    if "client" not in incoming_token or "token_time" not in incoming_token:
        new_token = {"client": garmin_login(),
                     "token": None,
                     "token_age": time.time()}
        return new_token

    if incoming_token.get("client") is None:
        new_token = {"client": garmin_login(),
                     "token": None,
                     "token_age": time.time()}
        return new_token

    token_age = time.time() - incoming_token["token_time"]
    max_age = 300  # seconds
    if token_age > max_age:  # test token validity
        try:
            cl = incoming_token.get("client")
            cl.get_full_name()
            log_api_event('Garmin', 'token age check passed', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_token = {"client": garmin_login(),
                         "token": None,
                         "token_age": time.time()}
            log_api_event('Garmin', 'token age check failed', token_age=token_age)
            return new_token
    else:
        log_api_event('Garmin', 'token age check bypassed', token_age=token_age)
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
    module_name, test_name = test_str.get("api_service_function").rsplit('.', 1)
    module = importlib.import_module(module_name)
    svc_function = getattr(module, test_name)
    client = svc_function()
    if client:
        return 'Success!'
    else:
        return 'Failure!'
