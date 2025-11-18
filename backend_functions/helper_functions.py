from datetime import date, datetime, timedelta

def reverse_key_lookup(d, value):
    # Returns the key value for a dictionary when passed a list item
    matches = [k for k, v in d.items() if v == value]
    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0]
    else:
        return matches


def list_to_dict_by_key(list_of_dicts, primary_key):
    # Transforms a list of dictionaries into a single dictionary by using a specified primary key
    return {dict(item)[primary_key]: dict(item) for item in list_of_dicts}


def get_sync_dates(meta_sync_val=None, meta_sync_type=None, max_range_days=7):
    # returns either a list of dates or a list of date pairs.

    is_range = meta_sync_type == 'date_range'

    # Convert meta_sync to a date object
    if meta_sync_val is None:
        last_sync_date = date.today()
    elif isinstance(meta_sync_val, str):
        last_sync_date = datetime.fromisoformat(meta_sync_val).date()
    elif isinstance(meta_sync_val, (date, datetime)):
        last_sync_date = meta_sync_val if isinstance(meta_sync_val, date) else meta_sync_val.date()
    else:
        raise ValueError(f"Cannot interpret meta_sync value: {meta_sync_val}")

    today_date = date.today()
    dates = []

    if last_sync_date > today_date:
        return []

    if not is_range:
        # Return list of individual dates
        curr_date = last_sync_date
        while curr_date <= today_date:
            dates.append(curr_date.strftime('%Y-%m-%d'))
            curr_date += timedelta(days=1)
    else:
        # Return list of (start_date, end_date) ranges
        curr_start = last_sync_date
        while curr_start <= today_date:
            curr_end = min(curr_start + timedelta(days=max_range_days - 1), today_date)
            dates.append((curr_start.strftime('%Y-%m-%d'), curr_end.strftime('%Y-%m-%d')))
            curr_start = curr_end + timedelta(days=1)

    return dates


def set_keys_to_none(d, key_list):
    # Sets the keys to None in the dictionary when not in the key_list
    new_dict = {}
    for key in key_list:
        if key in d:
            new_dict[key] = d[key]
        else:
            new_dict[key] = None

    for key in d:
        if key not in new_dict:
            new_dict[key] = None
    return new_dict



