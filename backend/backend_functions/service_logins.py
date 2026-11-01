import importlib

from dotenv import load_dotenv
from backend_functions.database_functions import one_sql_result, get_conn, qec

load_dotenv()

from pifitness.integrations._common import (
    load_api_credentials,
    load_token_from_db,
    save_token_to_db,
)

from pifitness.integrations.garmin.provider import (
    garmin_creds,
    garmin_login,
    get_garmin_client,
)

from pifitness.integrations.spotify.provider import (
    spotify_creds,
    get_spotify_client,
    sql_rate_limited,
    check_rate_limit_cached,
    log_rate_limitation,
    spotify_rate_limit_detection,
    sp_client,
    rate_limit_test,
    get_spotify_token,
    store_spotify_auth_metadata,
    clear_spotify_cache,
    get_spotify_token_expiry,
    insert_client,
)


from pifitness.auth.status import (
    get_auth_status,
)

def mapbox_token():
    creds = load_api_credentials('Mapbox')
    return creds.get('token')


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


# Import Garmin auth classes from the pirate-garmin_clone library
