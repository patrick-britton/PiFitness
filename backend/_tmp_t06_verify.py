"""T06 verification: summary fields total_elapsed_ms + courses_matched."""
import sys
sys.path.insert(0, '.')

from unittest.mock import patch
from backend.api.activities import _build_process_summary
from backend.schemas.activity_schemas import ProcessSummaryData

# Test 1: with activity_id, both fields populated
with patch("backend.api.activities.one_sql_result", return_value=7), \
     patch("backend.api.activities.sql_to_dict", return_value=[{"segment_name": "C1"}, {"segment_name": "C2"}]):
    s = _build_process_summary(shuffle_completed=True, activity_id=12345, total_elapsed_ms=4200)

assert isinstance(s, ProcessSummaryData)
assert s.total_elapsed_ms == 4200, f"Expected 4200, got {s.total_elapsed_ms}"
assert s.courses_matched == 2, f"Expected 2, got {s.courses_matched}"
assert s.segments_matched == 7
assert s.playlist_shuffled is True
assert s.course_found is True
print("Test 1 PASS: total_elapsed_ms=4200, courses_matched=2, segments_matched=7")

# Test 2: no shuffle → playlist_shuffled is None
with patch("backend.api.activities.one_sql_result", return_value=0), \
     patch("backend.api.activities.sql_to_dict", return_value=[]):
    s2 = _build_process_summary(shuffle_completed=False, activity_id=999, total_elapsed_ms=1000)
assert s2.playlist_shuffled is None
assert s2.courses_matched == 0
assert s2.segments_matched == 0
print("Test 2 PASS: no shuffle → playlist_shuffled=None, courses_matched=0")

# Test 3: no activity_id → all null
s3 = _build_process_summary(shuffle_completed=False, activity_id=None, total_elapsed_ms=500)
assert s3.total_elapsed_ms == 500
assert s3.segments_matched is None
assert s3.courses_matched is None
print("Test 3 PASS: no activity_id → segment/course fields null")

print("ALL T06 TESTS PASS")