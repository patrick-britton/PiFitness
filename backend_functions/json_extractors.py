import time
from datetime import date, timedelta, datetime
import pytz
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