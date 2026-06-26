"""
Health Query Functions
======================

Database query functions extracted from frontend_functions/health_module.py.
These functions return plain Python data structures with no Streamlit dependencies.
"""

from typing import List, Dict, Any, Optional, Union, Sequence
from datetime import datetime, date
from backend_functions.database_functions import qec, sql_to_dict
from backend.schemas.health_schemas import HeartRate, SleepData

def get_weight_targets() -> Sequence[Dict[str, Any]]:
    """
    Retrieve weight targets from the database.

    Returns:
        List[Dict[str, Any]]: List of weight target records with ts_utc and weight_lb fields.
        Each record contains the timestamp and weight in pounds.
    """
    tgt_sql = '''
    SELECT ts_utc, round(weight_total_g*0.00220462,1) as weight_lb
    FROM health.weight_target
    ORDER BY ts_utc DESC
    '''
    result = sql_to_dict(tgt_sql)
    return result if result else []

def add_weight_target(date: str, weight_lb: float) -> Union[str, List[str]]:
    """
    Add a new weight target to the database.

    Args:
        date (str): Date string in format 'YYYY-MM-DD'
        weight_lb (float): Weight in pounds

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    # Convert pounds to grams for database storage
    weight_g = int(round(weight_lb / 0.00220462, 0))

    sql = '''
    INSERT INTO health.weight_target (ts_utc, weight_total_g)
    VALUES (%s::TIMESTAMPTZ, %s)
    ON CONFLICT (ts_utc) DO UPDATE SET
    weight_total_g = EXCLUDED.weight_total_g
    '''
    result = qec(sql, [date, weight_g])
    return result if result else "Success"

def get_weight_viz_data(
    xaxis: str,
    xlimit: str,
    yaxis: List[str],
    history_limit: int
) -> Sequence[Dict[str, Any]]:
    """
    Retrieve weight visualization data based on user-selected parameters.

    Args:
        xaxis (str): X-axis column name (e.g., 'day_of_year', 'dm30')
        xlimit (str): X-axis limit column name (e.g., 'relative_year', 'relative_30')
        yaxis (List[str]): List of Y-axis column names to retrieve
        history_limit (int): Number of periods to include

    Returns:
        List[Dict[str, Any]]: Weight visualization data with selected columns
    """
    # Build dynamic SQL query
    columns = [xaxis, xlimit] + yaxis
    column_list = ', '.join(columns)

    sql = f'''
    SELECT {column_list}
    FROM health.vw_weight_viz
    WHERE {xlimit} > -{history_limit}
    ORDER BY {xlimit} ASC
    '''
    result = sql_to_dict(sql)
    return result if result else []

def add_photo_metadata(photo_type: str, file_name: str) -> Union[str, List[str]]:
    """
    Add photo metadata to the database.

    Args:
        photo_type (str): Type of photo ('front' or 'side')
        file_name (str): Name of the photo file

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    ins_sql = '''
    INSERT INTO health.photo_metadata (photo_type, file_name)
    VALUES (%s, %s)
    '''
    result = qec(ins_sql, [photo_type, file_name])
    return result if result else "Success"

def add_body_dimensions(
    butt_cm: float,
    waist_cm: float,
    stomach_cm: float,
    chest_cm: float,
    neck_cm: float
) -> Union[str, List[str]]:
    """
    Add body dimension measurements to the database.

    Args:
        butt_cm (float): Butt circumference in centimeters
        waist_cm (float): Waist circumference in centimeters
        stomach_cm (float): Stomach circumference in centimeters
        chest_cm (float): Chest circumference in centimeters
        neck_cm (float): Neck circumference in centimeters

    Returns:
        Union[str, List[str]]: Result message from the database operation
    """
    ins_sql = '''
    INSERT INTO health.body_dimensions(butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm)
    VALUES (%s, %s, %s, %s, %s)
    '''
    result = qec(ins_sql, [butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm])
    return result if result else "Success"

# ---------------------------------------------------------------------------
# Heart Rate
# ---------------------------------------------------------------------------

def get_heart_rate_timeseries(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 1000,
) -> Sequence[Dict[str, Any]]:
    """
    Retrieve heart rate time series data from the database.

    Args:
        start_date (Optional[date]): Filter by start date (inclusive).
        end_date (Optional[date]): Filter by end date (inclusive).
        limit (int): Maximum number of data points to return.

    Returns:
        Sequence[Dict[str, Any]]: List of heart rate records with
        ts_utc, heartrate_bpm, activity_label, and hr_date fields.
    """
    sql = "SELECT ts_utc, heartrate_bpm, activity_label, hr_date FROM health.heartrate_raw"
    params = []
    conditions = []

    if start_date:
        conditions.append("ts_utc >= %s::DATE")
        params.append(start_date)
    if end_date:
        conditions.append("ts_utc <= %s::DATE")
        params.append(end_date)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY ts_utc DESC LIMIT %s"
    params.append(limit)

    result = sql_to_dict(sql, params)
    return result if result else []


# ---------------------------------------------------------------------------
# Sleep Data
# ---------------------------------------------------------------------------

def get_sleep_data(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Sequence[Dict[str, Any]]:
    """
    Retrieve sleep data from the database.

    Args:
        start_date (Optional[date]): Filter by sleep end date (inclusive).
        end_date (Optional[date]): Filter by sleep end date (inclusive).

    Returns:
        Sequence[Dict[str, Any]]: List of sleep records with
        sleep_end_date, sleep_start_utc, sleep_end_utc, sleep_score,
        heartrate_bpm, spo2, breaths_per_min, hrv_value, sleep_duration_s,
        rem_sleep_s, light_sleep_s, awake_sleep_s, deep_sleep_s, score_label.
    """
    sql = """
        SELECT
            sleep_end_date,
            sleep_start_utc,
            sleep_end_utc,
            sleep_score,
            heartrate_bpm,
            spo2,
            breaths_per_min,
            hrv_value,
            sleep_duration_s,
            rem_sleep_s,
            light_sleep_s,
            awake_sleep_s,
            deep_sleep_s,
            score_label
        FROM health.sleep_totals
    """
    params = []
    conditions = []

    if start_date:
        conditions.append("sleep_end_date >= %s::DATE")
        params.append(start_date)
    if end_date:
        conditions.append("sleep_end_date <= %s::DATE")
        params.append(end_date)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY sleep_end_date DESC"

    result = sql_to_dict(sql, params)
    return result if result else []
