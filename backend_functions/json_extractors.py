import ast
import hashlib
import re
import time
from datetime import date, timedelta, datetime
import pytz

from backend_functions.database_functions import sql_to_dict, qec, sql_to_list, start_timer, elapsed_ms
from backend_functions.helper_functions import get_sync_dates, random_sleep, safe_parse
from backend_functions.logging_functions import log_app_event
from backend_functions.music_functions import get_playlist_list
from backend_functions.service_logins import get_spotify_client


def extract_json_limit_50(client=None, td=None):
    curr_ts = int(datetime.now(pytz.UTC).timestamp() * 1000)
    args= {
        'limit': 50,
        'before': curr_ts
    }
    return extract_with_args(client, td, args)

def extract_json_limit_offset(client=None, td=None):
    function = td.get('api_function_name')
    offset = 0
    limit = 50
    all_json = []
    while True:
        # Ensure we're being good API Citizens
        if offset != 0:
            time.sleep(1)
        args = {'limit': limit, 'offset': offset}
        raw_json = getattr(client, function)(**args)
        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            break

        if raw_json.get('next'):
            offset += 50
        else:
            break
    return all_json


def extract_json_playlist_details(client=None, td=None, list_id=None):

    # Connects to Spotify API and downloads all tracks
    # Uploads JSON to DB, which is then processed via stored procedure.

    # Monitor performance, start the timer

    if not client:
        # get_spotify_client() returns a client *dict* ({'client': <spotipy>, ...})
        # while this extractor needs the raw spotipy client. (Before 000-001 T08
        # this assigned the function object itself, so the no-client path raised
        # AttributeError: 'function' object has no attribute 'playlist_items'.)
        client = (get_spotify_client() or {}).get('client')

    if not client:
        log_app_event(cat='Playlist Sync',
                      desc='Aborting playlist detail extraction — no usable Spotify client',
                      err='get_spotify_client() returned no client',
                      data_event='ValidatePayload')
        return []

    task_name = 'Playlist Detail Sync'

    # Put the single (or multiple) playlist into a list
    if not list_id:
        playlists = get_playlist_list()
    else:
        playlists = [list_id]

    # Ensure we actually have playlists
    if not playlists:
        log_app_event(cat='Playlist Sync',
                      desc='Aborting playlist detail extraction — no playlists to sync',
                      err='get_playlist_list() returned no playlist ids',
                      data_event='ValidatePayload')
        return []

    log_app_event(cat='Playlist Sync',
                  desc=f"{len(playlists)} playlists found")

    # Initialize Results
    all_items = []

        # Iterate through list of playlists
    for l in playlists:
        if l != playlists[0]:
            time.sleep(2)  # Sleep for 10 seconds between playlists

        results = client.playlist_items(playlist_id=l, additional_types=['track'])

        # Get the next page of results
        while results:
            all_items.append(results)
            results = client.next(results)
            time.sleep(1)

    # 000-001 AC-8 (T08): normalize paging hrefs before the staging load.
    # Spotify's playlist_items paging href uses "/items?offset=.." for page 2+,
    # but staging.flatten_playlist_details derives playlist_id as
    # split_part(split_part(href,'/playlists/',2),'/tracks',1) — so an
    # un-normalized href persists "{id}/items?offset=..&limit=.." into
    # music.playlist_isrcs.playlist_id (Bug 000-001-T01-1). This mirrors the
    # rewrite in music_functions.playlist_to_db (004-004 Bug T10-7).
    for page in all_items:
        if isinstance(page, dict):
            href = page.get('href') or ''
            if '/items?' in href:
                page['href'] = href.replace('/items?', '/tracks?', 1)

    return all_items


PLAYLIST_ID_RE = re.compile(r'^[A-Za-z0-9]{22}$')


def derive_playlist_id_from_href(href):
    """Derive a playlist id the same way the live flatten SPROC does.

    staging.flatten_playlist_details computes
    ``split_part(split_part(href, '/playlists/', 2), '/tracks', 1)``; with an
    un-normalized paging href (``.../playlists/{id}/items?offset=50&limit=50``)
    that yields ``{id}/items?offset=50&limit=50`` and pollutes
    ``music.playlist_isrcs.playlist_id`` (Bug 000-001-T01-1). This helper mirrors
    the SPROC derivation and trims any query string, so a caller can tell whether
    the payload would persist a bare 22-char id.

    Returns the derived token, or None when the href carries no '/playlists/'.
    """
    if not href or '/playlists/' not in href:
        return None
    derived = href.split('/playlists/', 1)[1].split('/tracks', 1)[0]
    return derived.split('?', 1)[0]


def validate_playlist_payload(json_data, td):
    """Fail-loud guard (AC-8) for playlist sync payloads.

    Called between extraction and the staging load by the live task runner
    (ultimate_task_executioner_v2.extract_load_flatten) and by
    music_functions.playlist_to_db. Aborts (returns False + logs) when the
    payload is empty, has a truncated page chain (paging not exhausted), or
    yields a non-22-char playlist id — so no malformed row reaches staging and
    no downstream delete can run.

    Args:
        json_data: list of page dicts from the extractor
        td: task dictionary (for task_id/task_name logging context)

    Returns:
        True if the payload is valid and safe to load, False to abort.
    """
    task_id = td.get('task_id')
    task_name = td.get('task_name')
    api_function_name = td.get('api_function_name', '')

    def _abort(desc, err):
        log_app_event(
            cat=f"Task #{task_id}: {task_name}",
            desc=desc,
            err=err,
            task_id=task_id,
            data_event='ValidatePayload'
        )
        return False

    # --- Empty payload: abort with no deletes ---
    if not json_data:
        return _abort(
            "Aborting playlist sync — empty payload",
            f"api_function_name={api_function_name}; no data returned from API"
        )

    # --- Normalise to list ---
    if isinstance(json_data, dict):
        json_data = [json_data]

    if not isinstance(json_data, (list, tuple)):
        return _abort(
            "Aborting playlist sync — unexpected payload type",
            f"api_function_name={api_function_name}; type={type(json_data).__name__}"
        )

    # --- Validate page ids (detail payloads) and item ids (header payloads) ---
    # Detail payloads ('playlist_items'): the page href is the only source of the
    # playlist id the flatten SPROC persists, so it must resolve to a bare
    # 22-char id. Header payloads ('current_user_playlists'): pages point at the
    # user's playlist collection ('/users/{id}/playlists?offset=..'), so the
    # playlist ids live on the items.
    is_header_payload = api_function_name == 'current_user_playlists'

    non_empty_pages = 0
    derived_ids = []
    for page in json_data:
        items = page.get('items', []) if isinstance(page, dict) else []
        if items:
            non_empty_pages += 1

        page_href = page.get('href', '') if isinstance(page, dict) else ''
        derived = derive_playlist_id_from_href(page_href)

        if not is_header_payload and (derived is None or not PLAYLIST_ID_RE.match(derived)):
            return _abort(
                "Aborting playlist sync — non-22-char playlist_id",
                (f"api_function_name={api_function_name}; href={page_href[:120]}; "
                 f"derived_id={str(derived)[:40]}")
            )
        derived_ids.append(derived)

        if is_header_payload:
            for item in items:
                if isinstance(item, dict):
                    item_id = item.get('id')
                    if item_id and not PLAYLIST_ID_RE.match(str(item_id)):
                        return _abort(
                            "Aborting playlist sync — non-22-char playlist_id in items",
                            f"api_function_name={api_function_name}; item_id={str(item_id)[:30]}"
                        )

    # --- Paging must be exhausted for every playlist chain ---
    # A page that advertises a 'next' href is only fine when its successor page
    # was already fetched (intermediate pages of a paged payload keep their
    # pointer). A page whose successor is missing, or belongs to a different
    # playlist, means extraction stopped early — abort (no load, no deletes).
    for i, page in enumerate(json_data):
        next_url = page.get('next') if isinstance(page, dict) else None
        if next_url and (i + 1 >= len(json_data) or derived_ids[i + 1] != derived_ids[i]):
            return _abort(
                f"Aborting playlist sync — incomplete paging (page {i} has next={str(next_url)[:80]})",
                f"api_function_name={api_function_name}; pages not exhausted"
            )

    # --- At least one page must carry items ---
    if non_empty_pages == 0:
        return _abort(
            "Aborting playlist sync — all pages empty",
            f"api_function_name={api_function_name}; {len(json_data)} page(s) with 0 items"
        )

    return True




def extract_with_args(client=None, td=None, args=None):
    function = td.get('api_function_name')
    if args:
        return getattr(client, function)(**args)
    else:
        return getattr(client, function)()

def extract_json_range(client=None, td=None, daily=False):
    function = td.get('api_function_name')

    date_list = get_sync_dates(td.get('value_recency'), 'range')
    all_json = []
    for date_val in date_list:
        # pause for 2 seconds during each loop
        if date_val != date_list[0]:
            time.sleep(2)

        # If I can pull a range of values, the result will be a tuple.
        if not isinstance(date_val, (list, tuple)) or len(date_val) != 2:
            d1, d2 = default_range()
        else:
            d1, d2 = date_val

        if d1 is None or d2 is None:
            d1, d2 = default_range()

        args = [str(d1), str(d2)]
        if daily:
            args.append('daily')

        raw_json = getattr(client, function)(*args)

        # Append the results
        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            break

    return all_json


def extract_json_range_daily(client=None, td=None):
    return extract_json_range(client, td, daily=True)


def extract_json_day(client=None, td=None):
    function = td.get('api_function_name')
    date_list = get_sync_dates(td.get('value_recency'), 'single_day')
    all_json = []
    for date_val in date_list:
        # pause for 2 seconds during each loop
        if date_val != date_list[0]:
            time.sleep(2)

        args = [date_val,]
        raw_json = getattr(client, function)(*args)

        # Append the results
        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            break

    return all_json


def default_range():
    d2 = date.today()
    d1 = d2 - timedelta(days=1)
    return d1, d2


def to_params(param_list=None, search_val=None, replace_val=None, return_type='list'):
    if isinstance(param_list, list):
        temp_list = param_list
    else:
        temp_list = [param.strip() for param in param_list.split(',')]

    rb_list = []
    for p in temp_list:
        if search_val in p:
            rb_list.append(p.replace(search_val, str(replace_val)))
        else:
            rb_list.append(p)

    if return_type == 'list':
        return rb_list
    elif return_type == 'dict':
        return dict(p.split("=", 1) for p in rb_list)
    else:
        return ", ".join(rb_list)


def extract_json_activity_details(client=None, td=None, aid=None):

    # Connects to Spotify API and downloads all tracks
    # Uploads JSON to DB, which is then processed via stored procedure.

    # Monitor performance, start the timer

    task_name = 'Running Detail Sync'

    # Put the single (or multiple) playlist into a list
    if not aid:
        activities = sql_to_dict(query_str="SELECT * FROM activities.vw_activity_ids_to_sync")
    else:
        activities = [{'activity_id': aid, 'max_points': 99999}]

    # Ensure we actually have playlists
    if not activities:
        return client

    log_app_event(cat='Activity Detail Sync',
                  desc=f"{len(activities)} activities to download")

    # Initialize Results
    all_items = []

    # Iterate through list of playlists
    for a in activities:
        if a != activities[0]:
            time.sleep(2)  # Sleep for 10 seconds between playlists
        print(f'Syncing activity: {a.get('activity_id')}')
        results = client.get_activity_details(activity_id=int(a.get('activity_id')),
                                                                maxchart=int(a.get('max_points')),
                                                                maxpoly=int(a.get('max_points')))

        # Get the next page of results
        if results:
            all_items.append(results)
            print('Info Appended.')

    return all_items


def extract_json_isrc_search(client=None, td=None, aid=None):
    # Takes 50 isrcs at a time and pulls any and all track information
    batch_size = 50
    isrc_list = sql_to_list(f"SELECT track_isrc FROM music.vw_track_id_finder LIMIT {batch_size}")
    results = []
    getnum = 0
    for isrc in isrc_list:
        if isrc == isrc_list[0]:
            time.sleep(1)
        print(f'Searching for isrc: {isrc}')
        getnum += 1
        seen_hashes = set()
        offset = 0
        limit = 50
        market_val = None
        isrc_results = []  # Track results for this specific ISRC

        while True:
            try:
                query = f"isrc:{isrc}"
                # Perform the search for this specific ISRC
                response = client.search(q=query, type='track', limit=limit, offset=offset, market=market_val)
                if not response:
                    print(f'No response, breaking {isrc}')
                    break

                tracks_data = response.get('tracks', {})
                tracks = tracks_data.get('items', [])
                num_returned = len(tracks)

                batch_hash = hashlib.md5(str([t['id'] for t in tracks]).encode()).hexdigest() if tracks else None
                if batch_hash in seen_hashes:
                    print(f"Duplicate batch detected at offset {offset}, breaking.")
                    break
                if batch_hash:
                    seen_hashes.add(batch_hash)

                if tracks:
                    isrc_results.append(tracks)
                else:
                    break

                # did we find less track than the limit?
                if num_returned < limit:
                    print('Acceptable break: under limit')
                    break

                offset += limit
                if offset >= 1000 and market_val is None:
                    seen_hashes = set()
                    offset = 0
                    limit = 50
                    market_val = 'US'
                    print(f"Reset batch, searching again for just US")
                elif offset >= 1000:
                    print(f'US search results also exceed 1000')
                    break

            except Exception as e:
                print(f'Error searching ISRC: {isrc} err={e}')
                break

        # Handle fallback scenarios
        if isrc_results:
            results.extend(isrc_results)
            qec(f"""UPDATE music.all_tracks set id_synced_at_utc = CURRENT_TIMESTAMP where track_isrc = '{isrc}'""")

    return results


def get_pirate_data(client, endpoint, path_params=None, query_params=None):
    """Fetch data from Garmin via pirate-garmin's native Android API.
    
    Uses the passed-in GarminClient directly instead of spinning up a
    separate CLI invocation. The client handles auth, token refresh,
    and 401 retry internally.
    
    Args:
        client: GarminClient instance from backend_functions.pirate_garmin_auth (authenticated)
        endpoint: Endpoint key string (e.g. 'usersummary.daily')
        path_params: Dict of path placeholder values
        query_params: Dict of query parameter values
    
    Returns:
        tuple: (response_json, None) on success, (None, error_msg) on failure
    """
    from backend_functions.pirate_garmin_endpoints import resolve_endpoint, render_endpoint

    try:
        endpoint_def = resolve_endpoint(endpoint)
    except KeyError as e:
        return None, str(e)

    # Build path and query params
    resolved_path = endpoint_def.path
    if path_params:
        for key, val in path_params.items():
            placeholder = "{" + key + "}"
            if placeholder in resolved_path:
                resolved_path = resolved_path.replace(placeholder, str(val))
            else:
                # Raise error for unknown placeholders
                pass

    # Merge defaults with query params
    all_params = dict(endpoint_def.defaults)
    if query_params:
        all_params.update(query_params)

    try:
        result = client.request_json(
            host=endpoint_def.host,
            path=resolved_path,
            params=all_params if all_params else None,
        )
        return result, None
    except Exception as e:
        return None, str(e)

def gen_activity_list(aid=None):
    if not aid:
        return sql_to_list(query_str="SELECT DISTINCT activity_id FROM activities.vw_activity_ids_to_sync")
    else:
        return [aid]


def extract_pirate_universal(client=None, td=None, aid=None):
    """
    A single, metadata-driven orchestrator for all pirate-garmin endpoints.
    """
    t0 = start_timer()
    endpoint = td.get('api_function_name')
    loop_strategy = td.get('loop_strategy')
    all_json = []

    # 1. Initialize the correct iterator based on metadata
    if loop_strategy == 'single_day':
        iter_list = get_sync_dates(td.get('value_recency'), 'single_day')
    elif loop_strategy == 'range':
        iter_list = get_sync_dates(td.get('value_recency'), 'range')
    elif loop_strategy == 'activity':
        iter_list = gen_activity_list(aid)
    elif loop_strategy == 'single_run':
        iter_list = [None]  # Executes the loop exactly once without variables
    else:
        print(f"Unknown loop strategy: {loop_strategy}")
        return []

    # 2. Extract routing instructions
    path_keys = safe_parse(td.get('iter_path_keys'), [])
    query_keys = safe_parse(td.get('iter_query_keys'), [])
    static_path = safe_parse(td.get('static_path_params'), {})
    static_query = safe_parse(td.get('static_query_params'), {})

    # 3. Execution Loop
    for idx, val in enumerate(iter_list):
        if idx > 0:
            random_sleep(500,5000)

        # Clone the static parameters so we don't overwrite the original dictionary
        current_path = static_path.copy()
        current_query = static_query.copy()

        if val is not None:
            # Ensure the loop variable is iterable (handles single strings vs tuples)
            vals = val if isinstance(val, tuple) else (val,)

            # Map the loop values to their designated path or query keys
            for i, key in enumerate(path_keys):
                if i < len(vals): current_path[key] = vals[i]

            for i, key in enumerate(query_keys):
                if i < len(vals): current_query[key] = vals[i]

        # Call get_pirate_data with the authenticated GarminClient
        # The client is passed from extract_load_flatten via the executioner's
        # single login call — no separate auth needed here
        raw_json, error = get_pirate_data(
            client=client,
            endpoint=endpoint,
            path_params=current_path if current_path else None,
            query_params=current_query if current_query else None
        )

        # Handle Error / Logging Protocol
        if error:
            log_app_event(
                cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                desc=f"Extraction Failure for : {val}",
                exec_time=elapsed_ms(t0),
                task_id=td.get('task_id'),
                data_event='Extraction Failure'
            )
            print(f"Error on {endpoint} [{val}]: {error}")
            continue

        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            log_app_event(
                cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                desc=f"Unexpected Payload for : {val}",
                exec_time=elapsed_ms(t0),
                task_id=td.get('task_id'),
                data_event=f'Unexpected response {raw_json}'
            )
            print(f"Bad JSON for {val}: {raw_json}")
        else:
            print(f"No JSON returned for {val}")

    return all_json
