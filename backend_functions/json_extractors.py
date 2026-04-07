import hashlib
import time
from datetime import date, timedelta, datetime
import pytz

from backend_functions.database_functions import sql_to_dict, qec, sql_to_list, start_timer, elapsed_ms
from backend_functions.helper_functions import get_sync_dates
from backend_functions.logging_functions import log_app_event
from backend_functions.music_functions import get_playlist_list


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

    task_name = 'Playlist Detail Sync'

    # Put the single (or multiple) playlist into a list
    if not list_id:
        playlists = get_playlist_list()
    else:
        playlists = [list_id]

    # Ensure we actually have playlists
    if not playlists:
        return client

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
    return all_items


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


def get_pirate_data(endpoint, path_params=None, query_params=None):
    from pirate_garmin.cli import app
    from typer.testing import CliRunner
    import json

    runner = CliRunner()
    args = ["get", endpoint]

    # 1. Handle Path Parameters (CRITICAL for dayview)
    if path_params:
        for key, val in path_params.items():
            args.extend(["--path", f"{key}={val}"])

    # 2. Handle Query Parameters (For filters/ranges)
    if query_params:
        for key, val in query_params.items():
            args.extend(["--query", f"{key}={val}"])

    result = runner.invoke(app, args)

    if result.exit_code == 0:
        return json.loads(result.stdout), None
    else:
        print(f"Error: {result}")
        return None, str(result.stderr)


def extract_pirate_daily(client=None, td=None):
    t0 = start_timer()
    endpoint = td.get('api_function_name')
    date_list = get_sync_dates(td.get('value_recency'), 'single_day')
    all_json = []
    for date_val in date_list:
        # Pause for multiple iterations
        if date_val != date_list[0]:
            time.sleep(2)

        # Get the data
        raw_json, error = get_pirate_data(endpoint=endpoint,
                                        path_params={"date": date_val},
                                        query_params=None)

        if error:
            log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                          desc=f"Extraction Failure for : {date_val}",
                          exec_time=elapsed_ms(t0),
                          task_id=td.get('task_id'),
                          data_event='Extraction Failure')
            print(f"Error: {error}")
            continue

        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                          desc=f"Extraction Failure for : {date_val}",
                          exec_time=elapsed_ms(t0),
                          task_id=td.get('task_id'),
                          data_event=f'Unexpected response {raw_json}')
            print(f"Bad JSON for {date_val}: {raw_json}")
        else:
            print(f"No JSON for {date_val}")


    return all_json


def extract_pirate_activity(client=None, td=None, aid=None):
    t0 = start_timer()
    endpoint = td.get('api_function_name')
    if not aid:
        activities = sql_to_dict(query_str="SELECT * FROM activities.vw_activity_ids_to_sync")
    else:
        activities = [{'activity_id': aid, 'max_points': 99999}]
    all_json = []
    for a in activities:
        # Pause for multiple iterations
        if a != activities[0]:
            time.sleep(2)

        # Get the data
        raw_json, error = get_pirate_data(endpoint=endpoint,
                                        path_params={"activityId": a.get('activity_id'),
                                                     "maxChartSize": a.get('max_points'),
                                                     "maxPolyLineSize": a.get('max_points')},
                                        query_params=None)

        if error:
            log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                          desc=f"Extraction Failure for Activity : {a.get('activity_id')}",
                          exec_time=elapsed_ms(t0),
                          task_id=td.get('task_id'),
                          data_event='Extraction Failure')
            print(f"Error: {error}")
            continue

        if isinstance(raw_json, dict):
            all_json.append(raw_json)
        elif isinstance(raw_json, list):
            all_json.extend(raw_json)
        elif raw_json is not None:
            log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                          desc=f"Extraction Failure for Activity: {a.get('activity_id')}",
                          exec_time=elapsed_ms(t0),
                          task_id=td.get('task_id'),
                          data_event=f'Unexpected response {raw_json}')
            print(f"Bad JSON for {a.get('activity_id')}: {raw_json}")
        else:
            print(f"No JSON for {a.get('activity_id')}")


    return all_json

def extract_pirate_activity_summary(client=None, td=None, aid=None):
    t0 = start_timer()
    endpoint = td.get('api_function_name')

    all_json = []


    # Get the data

    raw_json, error = get_pirate_data(endpoint=endpoint,
                                    path_params={"start": 0,
                                                 "limit": 100},
                                    query_params=None)


    if error:
        log_app_event(cat=f"Task #{td.get('task_id')}: {td.get('task_name')}",
                      desc=f"Extraction Failure pulling activity summary",
                      exec_time=elapsed_ms(t0),
                      task_id=td.get('task_id'),
                      data_event='Extraction Failure')
        print(f"Error: {error}")


    return raw_json

