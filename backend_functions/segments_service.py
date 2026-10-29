"""Public service functions for the segments domain.

Extracted from router modules so that tests and other callers import from a
service layer rather than reaching into router private names (PY-005).
"""

from datetime import date, datetime
from decimal import Decimal


def json_safe(value):
    """Coerce Decimal/date/datetime to JSON-safe primitives.

    Public extraction of the router-private ``_json_safe`` helper that lived
    in ``backend/api/segments.py``.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def safe_row(row: dict) -> dict:
    """Return a dict with all values coerced to JSON-safe primitives.

    Public extraction of the router-private ``_safe_row`` helper that lived
    in ``backend/api/segments.py``.
    """
    return {k: json_safe(v) for k, v in row.items()}


def with_reference_window(rows, refs, id_key="segment_id") -> list:
    """Add a segment's reference-geometry pointers to picker/course rows.

    Public extraction of the router-private ``_with_reference_window`` helper
    that lived in ``backend/api/segments.py``.

    Keeps the existing columns exactly as they are and appends the three
    reference scalars so the Match Activities / Course Review modes can
    request the segment's own GPS path. Missing refs (unknown id) become
    nulls. ``id_key`` selects the row's segment-id column
    ('segment_id' for the /segments/list picker, 'course_id' for the
    /segments/courses page).
    """
    data = []
    for row in rows:
        enriched = safe_row(row)
        info = refs.get(int(row[id_key])) or {}
        enriched["activity_reference_id"] = json_safe(info.get("activity_reference_id"))
        enriched["reference_start_point"] = json_safe(info.get("reference_start_point"))
        enriched["reference_end_point"] = json_safe(info.get("reference_end_point"))
        data.append(enriched)
    return data