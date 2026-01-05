from datetime import date, datetime, timedelta, timezone
import pandas as pd
import numpy as np


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

    is_range = meta_sync_type == 'Range'

    # Convert meta_sync to a date object
    if meta_sync_val is None:
        last_sync_date = date.today()
    elif isinstance(meta_sync_val, str):
        last_sync_date = datetime.fromisoformat(meta_sync_val).date()
    elif isinstance(meta_sync_val, datetime):
        # Explicitly handle datetime first (before date check)
        last_sync_date = meta_sync_val.date()
    elif isinstance(meta_sync_val, date):
        last_sync_date = meta_sync_val
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


    if meta_sync_type=='Day':
        dates = dates[:21]
    else:
        dates = dates[:7]
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


def get_last_date(date_list):
    if not date_list:
        return None  # or raise an error

    last_item = date_list[-1]

    # Case 1: last item is a pair/tuple/list → return second element
    if isinstance(last_item, (tuple, list)) and len(last_item) >= 2:
        return last_item[-1]

    # Case 2: last item is a single date value
    return last_item


def col_value(df, col, return_type):
    # returns the specified values of a column if it exists in the dataframe
    defaults = {
        'min': 0,
        'max': 1,
    }

    # Check if column exists
    if col not in df.columns:
        return defaults.get(return_type, 0)

    # Check if column is empty
    if df[col].empty or len(df[col].dropna()) == 0:
        return defaults.get(return_type, 0)

    try:
        if return_type == 'min':
            value = df[col].min()
        elif return_type == 'max':
            value = df[col].max()
        else:
            return 0

        # Handle NaN, infinity, and None
        if pd.isna(value) or np.isinf(value) or value is None:
            return defaults.get(return_type, 0)

        # Convert to float and round to 2 decimal places
        value = float(value)
        value = round(value, 2)

        # Final safety check for JSON serialization
        if not np.isfinite(value):
            return defaults.get(return_type, 0)

        return value

    except (ValueError, TypeError, AttributeError):
        # Catch any conversion errors
        return defaults.get(return_type, 0)


def format_time_ago(timestamp):
    """Convert timestamp to human-readable time ago format"""
    if pd.isna(timestamp):
        return ''

    # Ensure timestamp is timezone-aware UTC
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    # Calculate difference
    now = datetime.now(timezone.utc)
    delta = now - timestamp
    total_seconds = int(delta.total_seconds())

    # Handle future dates
    if total_seconds < 0:
        total_seconds = abs(total_seconds)
        prefix = '+'
    else:
        prefix = ''

    # Calculate appropriate unit
    if total_seconds < 60:
        return f"{prefix}{total_seconds}s"
    elif total_seconds < 120 * 60:  # Less than 120 minutes
        minutes = total_seconds // 60
        return f"{prefix}{minutes}m"
    elif total_seconds < 48 * 3600:  # Less than 48 hours
        hours = total_seconds // 3600
        return f"{prefix}{hours}h"
    elif total_seconds < 30 * 86400:  # Less than 30 days
        days = total_seconds // 86400
        return f"{prefix}{days}d"
    else:  # 30+ days
        months = total_seconds // (30 * 86400)
        return f"{prefix}{months}mo"


def add_time_ago_column(df, timestamp_col, new_col_name='time_ago'):

    if timestamp_col in df.columns:
        df[new_col_name] = df[timestamp_col].apply(format_time_ago)
    else:
        df[new_col_name] = None

    return df


def convert_to_json_serializable(x):
    if isinstance(x, (np.int64, np.int32, np.int16, np.int8)):
        return int(x)
    elif isinstance(x, (np.float64, np.float32)):
        return float(x)
    elif isinstance(x, np.bool_):
        return bool(x)
    return x