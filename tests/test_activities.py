"""
Test Activities API Endpoints
============================

Basic smoke tests for activities API endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app)

def test_activities_list():
    """Test GET /api/activities returns successfully with schema-aware columns."""
    response = client.get("/api/activities")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["count"], int)
    if data["count"] > 0:
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0
        first_activity = data["data"][0]
        # These columns should always exist
        assert "activity_id" in first_activity
        assert "activity_name" in first_activity
        assert "start_timestamp_utc" in first_activity
        # These columns may or may not exist (schema-aware)
        if "duration_moving_s" in first_activity:
            assert isinstance(first_activity["duration_moving_s"], int)
        if "distance_m" in first_activity:
            assert isinstance(first_activity["distance_m"], (int, float))

def test_activity_by_id():
    """Test GET /api/activities/{id} returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["data"]["activity_id"] == activity_id

def test_activity_telemetry():
    """Test GET /api/activities/{id}/telemetry returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}/telemetry")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        if len(data["data"]) > 0:
            first_point = data["data"][0]
            assert "timestamp_utc" in first_point
            assert "latitude" in first_point
            assert "longitude" in first_point
            assert "elevation_m" in first_point

def test_activity_segments():
    """Test GET /api/activities/{id}/segments returns successfully."""
    # First get an activity ID
    list_response = client.get("/api/activities")
    assert list_response.status_code == 200
    list_data = list_response.json()

    if list_data["count"] > 0:
        activity_id = list_data["data"][0]["activity_id"]
        response = client.get(f"/api/activities/{activity_id}/segments")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "count" in data


def test_activity_report_contract_shape():
    """Test GET /api/activities/report returns the cross-surface ActivityReport
    shape (009-001 T02), with query helpers mocked so no DB is required."""
    import backend.api.activities as api_activities

    header = {
        "activity_id": 1234,
        "start_utc": "2026-09-01T12:00:00+00:00",
        "distance_mi": 5.0,
        "distance_display": "5.0 mi",
        "duration_display": "45:00.0",
        "pace_display": "9:00.0/mi",
        "hr_median": 150.0,
        "hr_maximum": 182.0,
        "total_time_s": 2700.0,
        "elevation_m_gain": 120.5,
    }
    chart_rows = [
        {"minute": 0, "elevation_m": 120, "heartrate_bpm": 155.0,
         "speed_mps": 3.0, "pace_sec_per_mi": 570.5, "pace_text": "9:30.5"},
        {"minute": 1, "elevation_m": 132, "heartrate_bpm": 168.0,
         "speed_mps": 3.1, "pace_sec_per_mi": 555.0, "pace_text": "9:15.0"},
    ]
    efforts = [
        {
            "segment_id": 11,
            "name": "My Course",
            "is_course": True,
            "all_time_rank": 2,
            "prior_delta_s": -12.5,
            "best_delta_s": 8.25,
            "total_attempts": 47,
        },
        {
            "segment_id": 22,
            "name": "Hill Sprint",
            "is_course": False,
            "all_time_rank": 15,
            "prior_delta_s": 3.0,
            "best_delta_s": 20.5,
            "total_attempts": 47,
        },
    ]

    import unittest.mock as mock
    with mock.patch.object(api_activities, "get_activity_report_header_view_by_type", return_value=header), \
         mock.patch.object(api_activities, "get_activity_report_charts", return_value=chart_rows), \
         mock.patch.object(api_activities, "get_activity_course_path", return_value=[[-122.42, 37.77], [-122.43, 37.78]]), \
         mock.patch.object(api_activities, "get_activity_report_efforts", return_value=efforts):
        response = client.get("/api/activities/report?activity_type=Run")
        assert response.status_code == 200
        data = response.json()
        assert data["activity_id"] == 1234
        assert data["activity_type"] == "Run"
        assert data["has_segments"] is True
        assert data["header"]["total_time_text"] == "0:45:00.000"
        assert data["header"]["pace_text"] == "9:00.00/mi"
        assert data["header"]["hr_median"] == 150.0
        assert "hr_p75" not in data["header"]
        assert data["header"]["hr_max"] == 182.0
        assert data["header"]["elevation_m_gain"] == 120.5
        assert data["header"]["show_efficiency_placeholder"] is True
        # Course mapped from the is_course row; segments exclude the course row.
        assert data["course"]["name"] == "My Course"
        assert data["course"]["all_time_rank"] == 2
        assert data["course"]["total_attempts"] == 47
        assert len(data["segments"]) == 1
        assert data["segments"][0]["name"] == "Hill Sprint"
        assert data["segments"][0]["is_course"] is False
        assert data["segments"][0]["prior_delta_s"] == 3.0
        # New 009-006 chart series + course path fields ride on the same report.
        assert data["course_path"] == [[-122.42, 37.77], [-122.43, 37.78]]
        assert data["charts"]["elevation"][0] == {"minute": 0, "elevation_m": 120.0}
        assert data["charts"]["heartrate"][1]["heartrate_bpm"] == 168.0
        assert data["charts"]["pace"][0]["pace_sec_per_mi"] == 570.5
        assert data["charts"]["pace"][0]["pace_text"] == "9:30.5"


def test_activity_report_invalid_type():
    """Test GET /api/activities/report rejects an unknown activity_type."""
    response = client.get("/api/activities/report?activity_type=Ride")
    assert response.status_code == 422


def test_leaderboard_segments_contract_shape():
    """Test GET /api/activities/leaderboard/segments returns the bare
    SegmentListRow[] contract (009-002 T03), with the query helper mocked
    so no DB is required."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    rows = [
        {
            "segment_id": 7,
            "segment_name": "Hill Sprint",
            "is_course": False,
            "activity_type_name": "running",
            "distance_mi": 0.42,
            "elevation_gain": 18.0,
            "last_effort": "2026-09-01",
            "matched_activity_count": 12,
        },
        {
            "segment_id": 8,
            "segment_name": "My Course",
            "is_course": True,
            "activity_type_name": "trail_running",
            "distance_mi": 4.6,
            "elevation_gain": None,
            "last_effort": None,
            "matched_activity_count": 0,
        },
    ]
    with mock.patch.object(api_activities, "get_leaderboard_segments", return_value=rows):
        response = client.get("/api/activities/leaderboard/segments")
        assert response.status_code == 200
        data = response.json()
        # Bare array per the SegmentListRow[] contract (fetchAPI<SegmentListRow[]>).
        assert isinstance(data, list)
        assert len(data) == 2
        first = data[0]
        for field in (
            "segment_id",
            "segment_name",
            "is_course",
            "activity_type_name",
            "distance_mi",
            "elevation_gain",
            "last_effort",
            "matched_activity_count",
        ):
            assert field in first
        assert first["segment_id"] == 7
        assert first["is_course"] is False
        assert first["matched_activity_count"] == 12
        assert data[1]["last_effort"] is None


def test_leaderboard_segments_filters_forwarded():
    """Filters (name/kind/min_distance/activity_type) reach the SQL helper;
    an empty name degrades to None (no filter)."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    with mock.patch.object(api_activities, "get_leaderboard_segments", return_value=[]) as helper:
        response = client.get(
            "/api/activities/leaderboard/segments"
            "?name=hill&kind=course&min_distance=2&activity_type=Trail%20Run"
        )
        assert response.status_code == 200
        assert response.json() == []
        helper.assert_called_once_with(
            name="hill", kind="course", min_distance=2.0, activity_type="Trail Run"
        )

    with mock.patch.object(api_activities, "get_leaderboard_segments", return_value=[]) as helper:
        response = client.get("/api/activities/leaderboard/segments?name=")
        assert response.status_code == 200
        assert response.json() == []
        helper.assert_called_once_with(
            name=None, kind=None, min_distance=0.0, activity_type=None
        )


def test_leaderboard_segments_invalid_params():
    """Unknown kind/activity_type or negative min_distance are rejected."""
    response = client.get("/api/activities/leaderboard/segments?kind=both")
    assert response.status_code == 422
    response = client.get("/api/activities/leaderboard/segments?activity_type=Ride")
    assert response.status_code == 422
    response = client.get("/api/activities/leaderboard/segments?min_distance=-1")
    assert response.status_code == 422


def test_leaderboard_contract_shape():
    """Test GET /api/activities/leaderboard/{id} returns the Leaderboard
    contract (009-002 T04), with the query helper mocked so no DB is required."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    rows = [
        {
            "segment_id": 7,
            "is_course": False,
            "segment_name": "Hill Sprint",
            "activity_id": 111,
            "activity_start_point": 100,
            "activity_end_point": 900,
            "distance_mi": 0.42,
            "start_time_utc": "2026-09-01T10:00:00+00:00",
            "all_time_rank": 1,
            "last_365_rank": 1,
            "current_cycle_rank": 1,
            "recency_rank": 3,
            "elapsed_duration_s": 120.0,
            "pace_str": "4:45/mi",
            "gap_s": 0.0,
            "rank": 1,
            "avg_hr": 150.0,
            "max_hr": 172.0,
            "vo2_max_value": 52.0,
            "resting_hr_asleep": 48.0,
            "resting_hr_awake": 55.0,
            "training_load_acute": 320.0,
            "training_load_pct": 60.0,
            "weight_lb": 160.0,
            "fat_pct": 0.18,
            "muscle_pct": 0.42,
            "avg_cadence": 178.0,
            "avg_vert_osc": 8.1,
            "avg_vert_ratio": 8.9,
            "avg_gct": 240.0,
            "avg_stride_length": 110.0,
            "avg_temp": 68.0,
            "altitude_acclimation_m": 100.0,
            "heat_acclimation_pct": 0.0,
            "sleep_hours": 7.5,
            "sleep_score": 82.0,
            "awake_hours": 1.2,
            "avg_perf": 3.0,
            "avg_balance": 50.2,
        },
        {
            "segment_id": 7,
            "is_course": False,
            "segment_name": "Hill Sprint",
            "activity_id": 222,
            "activity_start_point": 100,
            "activity_end_point": 900,
            "distance_mi": 0.42,
            "start_time_utc": "2026-08-15T10:00:00+00:00",
            "all_time_rank": 2,
            "last_365_rank": 2,
            "current_cycle_rank": 2,
            "recency_rank": 2,
            "elapsed_duration_s": 125.5,
            "pace_str": "4:58/mi",
            "gap_s": 5.5,
            "rank": 2,
            "avg_hr": 155.0,
            "max_hr": None,
            "all_time_rank_extra_ignored": True,
        },
    ]
    with mock.patch.object(api_activities, "get_segment_leaderboard", return_value=rows):
        response = client.get("/api/activities/leaderboard/7")
        assert response.status_code == 200
        data = response.json()
        assert data["segment_id"] == 7
        assert data["segment_name"] == "Hill Sprint"
        assert data["is_course"] is False
        assert len(data["efforts"]) == 2
        e0 = data["efforts"][0]
        # Core ranked fields (FR-6/FR-8): rank columns, gap, date.
        assert e0["rank"] == 1
        assert e0["all_time_rank"] == 1
        assert e0["last_365_rank"] == 1
        assert e0["current_cycle_rank"] == 1
        assert e0["recency_rank"] == 3
        assert e0["gap_s"] == 0.0
        assert e0["start_time_utc"] == "2026-09-01T10:00:00+00:00"
        assert e0["elapsed_duration_s"] == 120.0
        # Metric fields for the leaderboard-type column sets (FR-7).
        assert e0["pace_str"] == "4:45/mi"
        assert e0["avg_hr"] == 150.0
        assert e0["vo2_max_value"] == 52.0
        assert e0["resting_hr_asleep"] == 48.0
        assert e0["training_load_acute"] == 320.0
        assert e0["weight_lb"] == 160.0
        assert e0["fat_pct"] == 0.18
        assert e0["avg_cadence"] == 178.0
        assert e0["avg_gct"] == 240.0
        assert e0["avg_temp"] == 68.0
        assert e0["sleep_hours"] == 7.5
        assert e0["awake_hours"] == 1.2
        # Nulls survive as null; second row's gap is non-zero.
        assert e0["max_hr"] == 172.0
        assert data["efforts"][1]["gap_s"] == 5.5
        assert data["efforts"][1]["max_hr"] is None


def test_leaderboard_not_found():
    """A segment with no leaderboard rows returns 404."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    with mock.patch.object(api_activities, "get_segment_leaderboard", return_value=[]):
        response = client.get("/api/activities/leaderboard/999999")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# 009-002 T05: POST /api/activities/leaderboard/visuals
# ---------------------------------------------------------------------------


def test_leaderboard_visuals_contract_shape():
    """POST /api/activities/leaderboard/visuals returns the SegmentVisuals
    contract (009-002 T05): staging invoked, telemetry arrays zipped into the
    per-effort series, path_coords preserved, segment_name resolved."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    request_body = {
        "segment_id": 7,
        "efforts": [
            {"activity_id": 111, "start_point": 100, "end_point": 900},
            {"activity_id": 222, "start_point": 50, "end_point": 800},
        ],
    }

    telemetry_rows = [
        {
            "id_val": "2026-09-01",
            "meta_rank": 1,
            "path_coords": [[-73.98, 40.76], [-73.97, 40.77], [-73.96, 40.78]],
            "x_time": [0.0, 1.0, 2.0],
            "x_distance": [0.0, 4.2, 8.4],
            "y_elevation": [10.0, 12.0, 15.0],
            "y_hr": [140.0, 150.0, 160.0],
            "y_cadence": [168.0, 172.0, 176.0],
            "y_speed": [3200.0, 3300.0, 3400.0],
            "y_vert_ratio": [7.5, 8.0, 8.5],
            "y_osc": [6.0, 6.5, 7.0],
            "y_temp": [68.0, 69.0, 70.0],
            "y_stride": [105.0, 110.0, 115.0],
            "y_gct": [250.0, 245.0, 240.0],
        },
        {
            "id_val": "2026-08-15",
            "meta_rank": 2,
            "path_coords": [[-73.98, 40.76], [-73.97, 40.77]],
            "x_time": [0.0, 1.5],
            "x_distance": [0.0, 6.3],
            "y_elevation": [10.0, 14.0],
            "y_hr": [145.0, 158.0],
            "y_cadence": [170.0, 174.0],
            "y_speed": [3100.0, 3250.0],
            "y_vert_ratio": [7.8, 8.2],
            "y_osc": [6.2, 6.8],
            "y_temp": [70.0, 71.0],
            "y_stride": [107.0, 112.0],
            "y_gct": [252.0, 247.0],
        },
    ]

    with mock.patch.object(api_activities, "stage_leaderboard_visuals") as mock_stage, \
         mock.patch.object(api_activities, "get_segment_visuals_telemetry", return_value=telemetry_rows), \
         mock.patch.object(api_activities, "get_segment_name", return_value="Hill Sprint"):
        response = client.post("/api/activities/leaderboard/visuals", json=request_body)

    assert response.status_code == 200
    data = response.json()

    # Staging helper called once with segment_id + flattened effort rows.
    mock_stage.assert_called_once_with(
        7,
        [
            {"activity_id": 111, "start_point": 100, "end_point": 900},
            {"activity_id": 222, "start_point": 50, "end_point": 800},
        ],
    )

    # Envelope.
    assert data["segment_id"] == 7
    assert data["segment_name"] == "Hill Sprint"
    assert len(data["efforts"]) == 2

    # First effort: path_coords preserved, series zipped from parallel arrays.
    e0 = data["efforts"][0]
    assert e0["id_val"] == "2026-09-01"
    assert e0["meta_rank"] == 1
    assert e0["path_coords"] == [[-73.98, 40.76], [-73.97, 40.77], [-73.96, 40.78]]
    assert len(e0["series"]) == 3
    assert e0["series"][0]["elapsed_s"] == 0.0
    assert e0["series"][0]["distance_m"] == 0.0
    assert e0["series"][0]["heartrate_bpm"] == 140.0
    assert e0["series"][0]["cadence_spm"] == 168.0
    assert e0["series"][0]["speed_mmps"] == 3200.0
    assert e0["series"][2]["elapsed_s"] == 2.0
    assert e0["series"][2]["elevation_m"] == 15.0
    assert e0["series"][2]["ground_contact_time_ms"] == 240.0

    # Second effort: shorter series (2 samples), distinct id/meta_rank.
    e1 = data["efforts"][1]
    assert e1["id_val"] == "2026-08-15"
    assert e1["meta_rank"] == 2
    assert len(e1["series"]) == 2
    assert e1["series"][1]["elapsed_s"] == 1.5


def test_leaderboard_visuals_not_found():
    """A visuals request whose telemetry helper returns no rows yields 404."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    with mock.patch.object(api_activities, "stage_leaderboard_visuals"), \
         mock.patch.object(api_activities, "get_segment_visuals_telemetry", return_value=[]):
        response = client.post(
            "/api/activities/leaderboard/visuals",
            json={"segment_id": 7, "efforts": [{"activity_id": 111, "start_point": 100, "end_point": 900}]},
        )
    assert response.status_code == 404


def test_stage_leaderboard_visuals_bulk_insert():
    """stage_leaderboard_visuals TRUNCATEs then bulk-inserts all selected rows
    in a single parameterized execute_values statement (009-002 T05) and commits."""
    import unittest.mock as mock

    import backend_functions.queries.activities_queries as aq

    efforts = [
        {"activity_id": 111, "start_point": 100, "end_point": 900},
        {"activity_id": 222, "start_point": 50, "end_point": 800},
        {"activity_id": 333, "start_point": 200, "end_point": 1500},
    ]

    mock_conn = mock.MagicMock()
    mock_cur = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cur

    with mock.patch.object(aq, "get_conn", return_value=mock_conn), \
         mock.patch.object(aq, "execute_values") as mock_ev:
        aq.stage_leaderboard_visuals(7, efforts)

    # TRUNCATE executed first.
    assert mock_cur.execute.call_args_list[0][0][0] == "TRUNCATE TABLE activities.temp_activity_segment_metas;"
    # Single bulk INSERT via execute_values with all 3 rows, parameterized.
    mock_ev.assert_called_once()
    rows_arg = mock_ev.call_args[0][2]
    template_arg = mock_ev.call_args[1].get("template")
    assert rows_arg == [(111, 100, 900), (222, 50, 800), (333, 200, 1500)]
    assert template_arg == "(%s, %s, %s)"
    # Committed and connection closed.
    mock_conn.commit.assert_called_once()
    mock_cur.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_activity_report_walk_no_placeholder():
    """Test a Walk report does not show the running-efficiency placeholder."""
    import backend.api.activities as api_activities
    import unittest.mock as mock

    header = {
        "activity_id": 999,
        "start_utc": "2026-09-01T10:00:00+00:00",
        "distance_mi": 2.0,
        "distance_display": "2.0 mi",
        "duration_display": "30:00.0",
        "pace_display": "15:00.0/mi",
        "hr_median": 120.0,
        "hr_maximum": 160.0,
        "total_time_s": 1800.0,
        "elevation_m_gain": None,
    }
    with mock.patch.object(api_activities, "get_activity_report_header_view_by_type", return_value=header), \
         mock.patch.object(api_activities, "get_activity_report_charts", return_value=[]), \
         mock.patch.object(api_activities, "get_activity_course_path", return_value=[]), \
         mock.patch.object(api_activities, "get_activity_report_efforts", return_value=[]):
        response = client.get("/api/activities/report?activity_type=Walk")
        assert response.status_code == 200
        data = response.json()
        assert data["activity_type"] == "Walk"
        assert data["header"]["show_efficiency_placeholder"] is False
        assert data["course"] is None
        assert data["segments"] == []
        assert data["has_segments"] is False
        assert data["charts"]["elevation"] == []
        assert data["course_path"] == []