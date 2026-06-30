"""
High-performance data manipulation helpers replacing pandas operations.
All functions work with native Python data structures (lists, dicts).

Design Principles:
- No pandas dependencies
- Work with list[dict[str, Any]] exclusively
- Explicit NaN/None handling via is_none_or_nan()
- Type-safe conversions via safe_float()/safe_int()
- Clear, readable function names that describe the operation
"""

import math
from datetime import datetime, date
from typing import Any, List, Dict, Optional, Callable, Tuple


def is_none_or_nan(value: Any) -> bool:
    """
    Check if value is None, NaN, or infinite.
    Centralized null checking for consistency across all helpers.
    """
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    return False


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Convert value to float with NaN/inf protection.
    Replaces: pd.to_numeric(errors='coerce')
    """
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    Convert value to int with error handling.
    Handles numpy int64, float strings, None, etc.
    """
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def col_min_max(
    data: List[Dict[str, Any]],
    col: str,
    operation: str = 'min',
    default: float = 0.0
) -> float:
    """
    Calculate min or max of a column across list of dicts.
    Replaces: df[col].min() / df[col].max()

    Args:
        data: List of dictionaries
        col: Column name to aggregate
        operation: 'min' or 'max'
        default: Value to return if column is empty/missing

    Returns:
        Aggregated value or default

    Example:
        max_elev = col_min_max(activities, 'elevation', operation='max')
    """
    # Extract valid numeric values
    values = []
    for d in data:
        if col not in d:
            continue
        val = d[col]
        if is_none_or_nan(val):
            continue
        try:
            values.append(float(val))
        except (ValueError, TypeError):
            continue

    if not values:
        return default

    try:
        if operation == 'min':
            return min(values)
        elif operation == 'max':
            return max(values)
        else:
            raise ValueError(f"Unsupported operation: {operation}. Use 'min' or 'max'.")
    except (ValueError, TypeError):
        return default


def filter_by_condition(
    data: List[Dict[str, Any]],
    col: str,
    condition: Callable[[Any], bool]
) -> List[Dict[str, Any]]:
    """
    Filter data by custom condition on column.
    Replaces: df[df['col'] > x]

    Args:
        data: List of dictionaries
        col: Column name to filter on
        condition: Lambda/function that takes column value and returns bool

    Returns:
        Filtered list of dictionaries

    Example:
        fast_runs = filter_by_condition(activities, 'pace', lambda p: p < 5.0)
    """
    return [d for d in data if col in d and not is_none_or_nan(d[col]) and condition(d[col])]


def filter_equal(data: List[Dict[str, Any]], col: str, value: Any) -> List[Dict[str, Any]]:
    """
    Filter rows where column equals value.
    Replaces: df[df['col'] == value]
    """
    return [d for d in data if col in d and d[col] == value]


def filter_not_equal(data: List[Dict[str, Any]], col: str, value: Any) -> List[Dict[str, Any]]:
    """Filter rows where column does not equal value"""
    return [d for d in data if col not in d or d[col] != value]


def filter_in(data: List[Dict[str, Any]], col: str, values: List[Any]) -> List[Dict[str, Any]]:
    """Filter rows where column value is in provided list"""
    return [d for d in data if col in d and d[col] in values]


def fill_none(
    data: List[Dict[str, Any]],
    col: str,
    fill_value: Any = 0
) -> List[Dict[str, Any]]:
    """
    Replace None/NaN values in specified column.
    Replaces: df['col'].fillna(fill_value)

    Args:
        data: List of dictionaries
        col: Column name to fill
        fill_value: Value to use for None/NaN

    Returns:
        New list of dictionaries with filled values
    """
    return [
        {**d, col: fill_value if is_none_or_nan(d.get(col)) else d[col]}
        for d in data
    ]


def fill_nones(
    data: List[Dict[str, Any]],
    columns: List[str],
    fill_value: Any = 0
) -> List[Dict[str, Any]]:
    """
    Replace None/NaN values in multiple columns.
    Replaces: df[['col1', 'col2']].fillna(fill_value)
    """
    result = data
    for col in columns:
        result = fill_none(result, col, fill_value)
    return result


def group_by(
    data: List[Dict[str, Any]],
    key_col: str
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    Group data by column value.
    Replaces: df.groupby('col')

    Args:
        data: List of dictionaries
        key_col: Column to group by

    Returns:
        Dictionary mapping group values to lists of rows

    Example:
        by_type = group_by(activities, 'activity_type')
        runs = by_type.get('run', [])
    """
    result: Dict[Any, List[Dict[str, Any]]] = {}
    for d in data:
        key = d.get(key_col)
        if key not in result:
            result[key] = []
        result[key].append(d)
    return result


def aggregate_sum(
    data: List[Dict[str, Any]],
    group_col: str,
    value_col: str
) -> Dict[Any, float]:
    """
    Sum values by group.
    Replaces: df.groupby('group')['value'].sum()

    Returns:
        Dict mapping group values to summed values
    """
    result: Dict[Any, float] = {}
    for d in data:
        if group_col not in d or value_col not in d:
            continue
        val = d[value_col]
        if is_none_or_nan(val):
            continue
        try:
            key = d[group_col]
            val_float = float(val)
            result[key] = result.get(key, 0.0) + val_float
        except (ValueError, TypeError):
            continue
    return result


def aggregate_avg(
    data: List[Dict[str, Any]],
    group_col: str,
    value_col: str
) -> Dict[Any, float]:
    """
    Average values by group.
    Replaces: df.groupby('group')['value'].mean()
    """
    sums: Dict[Any, float] = {}
    counts: Dict[Any, int] = {}
    
    for d in data:
        if group_col not in d or value_col not in d:
            continue
        val = d[value_col]
        if is_none_or_nan(val):
            continue
        try:
            key = d[group_col]
            val_float = float(val)
            sums[key] = sums.get(key, 0.0) + val_float
            counts[key] = counts.get(key, 0) + 1
        except (ValueError, TypeError):
            continue
    
    return {k: sums[k] / counts[k] for k in sums if counts.get(k, 0) > 0}


def cumulative_sum(
    data: List[Dict[str, Any]],
    value_col: str,
    result_col: str = 'cumulative'
) -> List[Dict[str, Any]]:
    """
    Calculate cumulative sum of a column.
    Replaces: df['col'].cumsum()

    Args:
        data: List of dictionaries (assumes order is meaningful)
        value_col: Column to sum
        result_col: Name for cumulative column

    Returns:
        New list of dictionaries with cumulative values
    """
    result = []
    running_total = 0.0
    
    for d in data:
        new_row = d.copy()
        if value_col in d and not is_none_or_nan(d[value_col]):
            try:
                running_total += float(d[value_col])
            except (ValueError, TypeError):
                pass
        new_row[result_col] = running_total
        result.append(new_row)
    
    return result


def sort_by(
    data: List[Dict[str, Any]],
    col: str,
    reverse: bool = False,
    numeric: bool = True
) -> List[Dict[str, Any]]:
    """
    Sort data by column.
    Replaces: df.sort_values('col', ascending=...)

    Args:
        data: List of dictionaries
        col: Column to sort by
        reverse: True for descending order
        numeric: Treat values as numeric (True) or string (False)

    Returns:
        Sorted list of dictionaries
    """
    def sort_key(d: Dict[str, Any]) -> Tuple[int, Any]:
        val = d.get(col)
        if val is None or is_none_or_nan(val):
            # Push None/NaN to end
            return (1, 0) if not reverse else (0, 0)
        if numeric:
            try:
                return (0, float(val))
            except (ValueError, TypeError):
                return (0, str(val))
        else:
            return (0, str(val))
    
    return sorted(data, key=sort_key, reverse=reverse)


def sort_by_multiple(
    data: List[Dict[str, Any]],
    sort_spec: List[Tuple[str, bool]]
) -> List[Dict[str, Any]]:
    """
    Sort by multiple columns.
    Replaces: df.sort_values(['col1', 'col2'], ascending=[True, False])

    Args:
        data: List of dictionaries
        sort_spec: List of (column_name, reverse) tuples

    Returns:
        Sorted list
    """
    def multi_key(d: Dict[str, Any]) -> Tuple:
        keys = []
        for col, reverse in sort_spec:
            val = d.get(col)
            if val is None or is_none_or_nan(val):
                keys.append((1, 0) if not reverse else (0, 0))
            else:
                try:
                    keys.append((0, float(val)))
                except (ValueError, TypeError):
                    keys.append((0, str(val)))
        return tuple(keys)
    
    return sorted(data, key=multi_key)


def column_values(data: List[Dict[str, Any]], col: str) -> List[Any]:
    """
    Extract column values as list.
    Replaces: df['col'].tolist() or df['col'].to_list()
    """
    return [d.get(col) for d in data]


def unique_values(data: List[Dict[str, Any]], col: str) -> List[Any]:
    """
    Get unique values for column preserving order.
    Replaces: df['col'].unique()

    Returns:
        List of unique values in order of first appearance
    """
    seen: set = set()
    result: List[Any] = []
    for d in data:
        val = d.get(col)
        if val is not None and not is_none_or_nan(val):
            # Use string representation for hashability
            val_str = str(val)
            if val_str not in seen:
                seen.add(val_str)
                result.append(val)
    return result


def value_counts(
    data: List[Dict[str, Any]],
    col: str,
    normalize: bool = False
) -> Dict[Any, int]:
    """
    Count occurrences of each value.
    Replaces: df['col'].value_counts()

    Returns:
        Dict mapping values to counts (descending order of frequency)
    """
    counts: Dict[Any, int] = {}
    for d in data:
        val = d.get(col)
        if val is not None and not is_none_or_nan(val):
            counts[val] = counts.get(val, 0) + 1
    
    # Sort by count descending
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


def head(data: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, Any]]:
    """Get first n rows. Replaces: df.head(n)"""
    return data[:n]


def tail(data: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, Any]]:
    """Get last n rows. Replaces: df.tail(n)"""
    return data[-n:]


def drop_duplicates(
    data: List[Dict[str, Any]],
    subset: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Remove duplicate rows.
    Replaces: df.drop_duplicates(subset=['col1', 'col2'])

    Args:
        data: List of dictionaries
        subset: Columns to consider for uniqueness (None = all columns)

    Returns:
        Deduplicated list
    """
    seen: set = set()
    result: List[Dict[str, Any]] = []
    
    for d in data:
        # Create tuple of values to check
        if subset:
            key_tuple = tuple(d.get(c) for c in subset)
        else:
            key_tuple = tuple(sorted(d.items()))
        
        if key_tuple not in seen:
            seen.add(key_tuple)
            result.append(d)
    
    return result


def rename_columns(
    data: List[Dict[str, Any]],
    column_map: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Rename columns.
    Replaces: df.rename(columns={'old': 'new'})

    Returns:
        New list with renamed keys
    """
    result = []
    for d in data:
        new_row = {}
        for k, v in d.items():
            new_key = column_map.get(k, k)
            new_row[new_key] = v
        result.append(new_row)
    return result


def select_columns(
    data: List[Dict[str, Any]],
    columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Select specific columns.
    Replaces: df[['col1', 'col2', 'col3']]

    Returns:
        New list containing only specified columns
    """
    return [
        {col: d.get(col) for col in columns}
        for d in data
    ]


def add_column(
    data: List[Dict[str, Any]],
    col_name: str,
    values: List[Any]
) -> List[Dict[str, Any]]:
    """
    Add new column from list of values.
    Replaces: df['new_col'] = values

    Returns:
        New list with added column
    """
    if len(values) != len(data):
        raise ValueError(
            f"Length mismatch: data has {len(data)} rows, values has {len(values)} elements"
        )
    
    result = []
    for d, val in zip(data, values):
        new_row = {**d, col_name: val}
        result.append(new_row)
    return result


def interpolate_timeseries(
    time_data: List[float],
    value_data: List[float],
    target_times: List[float]
) -> List[Tuple[float, float]]:
    """
    Linear interpolation for 1D time series.
    Replaces: np.interp() for simple use cases

    Args:
        time_data: Known time points
        value_data: Known values at those times
        target_times: Times to interpolate at

    Returns:
        List of (time, interpolated_value) tuples

    Example:
        # Get elevation at specific distances
        elevations = interpolate_timeseries(distances, elevations, target_distances)
    """
    if not time_data or not value_data:
        return [(t, 0.0) for t in target_times]
    
    if len(time_data) != len(value_data):
        raise ValueError("time_data and value_data must have same length")
    
    result = []
    data_idx = 0
    
    for target_time in target_times:
        # Advance index while next point is before target
        while (data_idx < len(time_data) - 1 and 
               time_data[data_idx + 1] < target_time):
            data_idx += 1
        
        # Handle edge cases
        if data_idx >= len(time_data) - 1:
            # Beyond last known point - extrapolate with last value
            result.append((target_time, value_data[-1]))
        elif time_data[data_idx] > target_time:
            # Before first known point - extrapolate with first value
            result.append((target_time, value_data[0]))
        else:
            # Interpolate between two points
            t0, t1 = time_data[data_idx], time_data[data_idx + 1]
            v0, v1 = value_data[data_idx], value_data[data_idx + 1]
            
            if t1 == t0:
                interpolated = v0
            else:
                ratio = (target_time - t0) / (t1 - t0)
                interpolated = v0 + ratio * (v1 - v0)
            
            result.append((target_time, interpolated))
    
    return result


def date_from_iso(
    data: List[Dict[str, Any]],
    source_col: str,
    target_col: str,
    format: str = 'date'
) -> List[Dict[str, Any]]:
    """
    Convert ISO date strings to date objects.
    Replaces: pd.to_datetime(df['col']).dt.date

    Args:
        data: List of dictionaries
        source_col: Column containing ISO date string
        target_col: Column to create with date object
        format: 'date' for date object, 'datetime' for datetime object

    Returns:
        New list with converted dates
    """
    result = []
    for d in data:
        new_row = d.copy()
        if source_col in d and d[source_col]:
            try:
                dt = datetime.fromisoformat(str(d[source_col]))
                if format == 'date':
                    new_row[target_col] = dt.date()
                else:
                    new_row[target_col] = dt
            except (ValueError, TypeError):
                new_row[target_col] = None
        else:
            new_row[target_col] = None
        result.append(new_row)
    return result


def to_json_serializable(value: Any) -> Any:
    """
    Convert value to JSON-serializable type.
    Handles numpy types, datetime objects, None, etc.
    Replaces: custom pandas/numpy type conversion logic

    Args:
        value: Any value that might not be JSON-serializable

    Returns:
        JSON-safe value
    """
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (int, str, bool)):
        return value
    # Try to convert numpy types
    try:
        import numpy as np
        if isinstance(value, (np.integer, np.floating)):
            return float(value) if isinstance(value, np.floating) else int(value)
        if isinstance(value, np.ndarray):
            return value.tolist()
    except ImportError:
        pass
    # Last resort: string conversion
    return str(value)


def to_json_serializable_list(
    data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Convert entire list of dicts to JSON-serializable.
    Replaces: df.applymap(to_json_serializable)

    Returns:
        New list with all values converted
    """
    return [
        {k: to_json_serializable(v) for k, v in d.items()}
        for d in data
    ]


def dict_list_to_csv(data: List[Dict[str, Any]]) -> str:
    """
    Convert list of dicts to CSV string.
    Simple CSV export without pandas.
    """
    if not data:
        return ""
    
    headers = list(data[0].keys())
    lines = [",".join(headers)]
    
    for row in data:
        values = []
        for h in headers:
            val = row.get(h)
            if val is None:
                values.append("")
            elif isinstance(val, str):
                # Escape quotes and wrap in quotes if contains comma
                escaped = val.replace('"', '""')
                if ',' in escaped or '"' in escaped or '\n' in escaped:
                    values.append(f'"{escaped}"')
                else:
                    values.append(escaped)
            else:
                values.append(str(val))
        lines.append(",".join(values))
    
    return "\n".join(lines)


def first_value(data: List[Dict[str, Any]], col: str, default: Any = None) -> Any:
    """
    Get first non-null value from column.
    Useful for getting single values from query results.
    """
    for d in data:
        if col in d and not is_none_or_nan(d[col]):
            return d[col]
    return default


# Convenience aliases for common operations
# These provide shorter names for frequently used patterns

def min_val(data: List[Dict[str, Any]], col: str, default: float = 0.0) -> float:
    """Shorthand for col_min_max(data, col, 'min', default)"""
    return col_min_max(data, col, operation='min', default=default)


def max_val(data: List[Dict[str, Any]], col: str, default: float = 0.0) -> float:
    """Shorthand for col_min_max(data, col, 'max', default)"""
    return col_min_max(data, col, operation='max', default=default)


def sum_col(data: List[Dict[str, Any]], col: str, default: float = 0.0) -> float:
    """
    Sum all values in column, skipping None/NaN.
    Replaces: df['col'].sum()
    """
    total = 0.0
    for d in data:
        if col in d and not is_none_or_nan(d[col]):
            try:
                total += float(d[col])
            except (ValueError, TypeError):
                pass
    return total if total != 0.0 else default


def avg_col(data: List[Dict[str, Any]], col: str, default: float = 0.0) -> float:
    """
    Average of column values, skipping None/NaN.
    Replaces: df['col'].mean()
    """
    values = []
    for d in data:
        if col in d and not is_none_or_nan(d[col]):
            try:
                values.append(float(d[col]))
            except (ValueError, TypeError):
                pass
    
    if not values:
        return default
    return sum(values) / len(values)